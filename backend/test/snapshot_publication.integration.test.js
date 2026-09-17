const assert = require('node:assert/strict');
const test = require('node:test');
const fs = require('node:fs/promises');
const path = require('node:path');
const { randomUUID } = require('node:crypto');
const { execFile } = require('node:child_process');
const { promisify } = require('node:util');
const { Pool } = require('pg');
const { publishSnapshotBatch, readSnapshotGeneration } = require('../snapshot_batch');
const { checkSnapshotConsistency } = require('../snapshot_hierarchy');
const { reaggregateSnapshotDate } = require('../snapshot_reaggregation');
const { backfillSnapshotDates } = require('../snapshot_backfill');
const { fetchStoredApiHistory } = require('../fix_git_stats');

// Explicit opt-in only: creates and drops its own random schema, never public.
test('PostgreSQL atomic snapshot publication', { skip: !process.env.SNAPSHOT_TEST_DATABASE_URL }, async t => {
    const connectionString = process.env.SNAPSHOT_TEST_DATABASE_URL;
    const schema = `snapshot_test_${randomUUID().replaceAll('-', '')}`;
    const admin = new Pool({ connectionString });
    const pool = new Pool({ connectionString, options: `-c search_path=${schema}`, max: 5 });
    t.after(async () => {
        await pool.end();
        await admin.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
        await admin.end();
    });
    await admin.query(`CREATE SCHEMA ${schema}`);
    for (const filename of ['schema.sql', 'contributors_schema.sql']) {
        const sql = await fs.readFile(path.join(__dirname, '../../db', filename), 'utf8');
        await pool.query(sql.replace(/^\\.*$/gm, ''));
    }
    // Exercise an upgrade from the old schema as well as repeat application.
    await pool.query('ALTER TABLE organizations DROP COLUMN snapshot_generation');
    const migration = await fs.readFile(path.join(__dirname, '../../db/migrations/004_snapshot_generation.sql'), 'utf8');
    await pool.query(migration);
    await pool.query(migration);
    await pool.query("INSERT INTO organizations (name) VALUES ('test-org')");
    await pool.query("INSERT INTO special_interest_groups (org_id,slug,name) VALUES (1,'one','one'),(1,'two','two')");
    await pool.query("INSERT INTO repositories (org_id,sig_id,name) VALUES (1,1,'one'),(1,2,'two')");
    const repositories = (await pool.query('SELECT id, name, sig_id FROM repositories ORDER BY id')).rows;
    const date = '2026-09-07';
    const entry = (id, count = 1) => ({ repoId: id,
        commitStats: { new_commits: count, lines_added: count * 10, lines_deleted: count,
            authorStats: count ? { alice: { commits: count, lines_added: count * 10, lines_deleted: count, github_id: 101 } } : {} },
        apiMetrics: { new_prs: count, closed_merged_prs: 0, new_issues: 0, closed_issues: 0 },
        contributorDetails: count ? [{ username: 'alice', github_id: 101,
            prs_opened: count, prs_closed: 0, issues_opened: 0, issues_closed: 0 }] : [],
    });
    const options = { pool, orgId: 1, orgName: 'test-org', repositories, snapshotDate: date };
    const publish = async (entries, extra = {}) => publishSnapshotBatch({ ...options, entries,
        expectedGeneration: await readSnapshotGeneration(pool, 1), ...extra });
    const dump = async () => {
        const result = {};
        for (const table of ['organizations', 'repo_snapshots', 'sig_snapshots', 'activity_snapshots',
            'contributors', 'contributor_repo_activities', 'contributor_daily_activities']) {
            result[table] = (await pool.query(`SELECT * FROM ${table} ORDER BY id`)).rows;
        }
        return result;
    };
    await t.test('publishes all levels and freshness together; aggregates shared authors once per org', async () => {
        let invalidated = false;
        await publish([entry(1), entry(2)], { markFresh: true, afterCommit: async () => {
            assert.equal(await readSnapshotGeneration(pool, 1), '1');
            invalidated = true;
        } });
        assert.equal(invalidated, true);
        assert.deepEqual(await checkSnapshotConsistency(pool, 1), []);
        const data = await dump();
        assert.equal(data.activity_snapshots[0].new_commits, 2);
        assert.equal(data.activity_snapshots[0].active_contributors, 2);
        assert.equal(data.contributor_daily_activities.length, 1);
        assert.equal(data.contributor_daily_activities[0].commits_count, 2);
        assert.ok(data.organizations[0].last_ingestion_completed_at);
    });
    await t.test('failure after repository writes rolls back facts, parents, freshness and generation', async () => {
        const before = await dump();
        await pool.query(`CREATE FUNCTION reject_parent() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'injected aggregate failure'; END $$`);
        await pool.query('CREATE TRIGGER fail_parent BEFORE INSERT OR UPDATE ON activity_snapshots FOR EACH ROW EXECUTE FUNCTION reject_parent()');
        let invalidated = false;
        try {
            await assert.rejects(publish([entry(1, 3), entry(2, 4)], { markFresh: true,
                afterCommit: async () => { invalidated = true; } }), /injected aggregate failure/);
        } finally {
            await pool.query('DROP TRIGGER fail_parent ON activity_snapshots');
        }
        assert.deepEqual(await dump(), before);
        assert.equal(invalidated, false);
    });
    await t.test('repeated publication replaces rather than adds facts, and zero days clear stale authors', async () => {
        await publish([entry(1), entry(2)]);
        assert.equal((await dump()).activity_snapshots[0].new_commits, 2);
        await publish([entry(1, 0), entry(2, 0)]);
        const data = await dump();
        assert.equal(data.activity_snapshots[0].new_commits, 0);
        assert.equal(data.activity_snapshots[0].active_contributors, 0);
        assert.equal(data.contributor_repo_activities.length, 0);
        assert.equal(data.contributor_daily_activities.length, 0);
        assert.deepEqual(await checkSnapshotConsistency(pool, 1), []);
    });
    await t.test('readers see the previous complete version while new repository facts are uncommitted', async () => {
        const before = await dump();
        let observed = false;
        const wrappedPool = { connect: async () => {
            const client = await pool.connect();
            return { release: () => client.release(), query: async (sql, params) => {
                if (sql.startsWith('INSERT INTO sig_snapshots')) {
                    assert.deepEqual(await dump(), before);
                    observed = true;
                }
                return client.query(sql, params);
            } };
        } };
        await publish([entry(1, 3), entry(2, 2)], { pool: wrappedPool });
        assert.equal(observed, true);
        assert.equal((await dump()).activity_snapshots[0].new_commits, 5);
    });
    await t.test('a hierarchy assertion failure rolls back the whole publication', async () => {
        const before = await dump();
        await pool.query(`CREATE FUNCTION corrupt_sig() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN NEW.new_commits := NEW.new_commits + 1; RETURN NEW; END $$`);
        await pool.query('CREATE TRIGGER corrupt_parent BEFORE INSERT OR UPDATE ON sig_snapshots FOR EACH ROW EXECUTE FUNCTION corrupt_sig()');
        try {
            await assert.rejects(publish([entry(1), entry(2)], { markFresh: true }), /Snapshot hierarchy mismatch/);
        } finally {
            await pool.query('DROP TRIGGER corrupt_parent ON sig_snapshots');
        }
        assert.deepEqual(await dump(), before);
    });
    await t.test('concurrent publishers of the same generation allow only one winner', async () => {
        const generation = await readSnapshotGeneration(pool, 1);
        const outcomes = await Promise.allSettled([1, 5].map(count => publishSnapshotBatch({ ...options,
            entries: [entry(1, count), entry(2, count)], expectedGeneration: generation })));
        assert.equal(outcomes.filter(result => result.status === 'fulfilled').length, 1);
        assert.match(outcomes.find(result => result.status === 'rejected').reason.message, /generation changed/);
        assert.deepEqual(await checkSnapshotConsistency(pool, 1), []);
    });
    await t.test('partial repair rebuilds every parent metric without changing freshness', async () => {
        const before = await dump();
        await publish([entry(1, 7)], { partial: true, repositories: repositories.slice(0, 1) });
        const after = await dump();
        assert.deepEqual(after.organizations[0].last_ingestion_completed_at, before.organizations[0].last_ingestion_completed_at);
        assert.deepEqual(after.repo_snapshots.find(row => row.repo_id === 2), before.repo_snapshots.find(row => row.repo_id === 2));
        assert.deepEqual(await checkSnapshotConsistency(pool, 1), []);
    });
    await t.test('cache failure is reported as committed, not rolled back', async () => {
        const generation = await readSnapshotGeneration(pool, 1);
        await assert.rejects(publish([entry(1), entry(2)], { afterCommit: async () => { throw new Error('Redis down'); } }),
            error => error.committed === true);
        assert.equal(await readSnapshotGeneration(pool, 1), String(BigInt(generation) + 1n));
        assert.deepEqual(await checkSnapshotConsistency(pool, 1), []);
    });
    await t.test('checker detects compensating SIG errors and reaggregation repairs them', async () => {
        const before = await dump();
        await pool.query('UPDATE sig_snapshots SET new_prs = new_prs + CASE WHEN sig_id=1 THEN 1 ELSE -1 END');
        const differences = await checkSnapshotConsistency(pool, 1, date);
        assert.equal(differences.length, 2);
        assert.ok(differences.every(row => row.level === 'sig'));
        await reaggregateSnapshotDate({ pool, orgId: 1, snapshotDate: date });
        assert.deepEqual(await checkSnapshotConsistency(pool, 1), []);
        assert.deepEqual((await dump()).organizations[0].last_ingestion_completed_at, before.organizations[0].last_ingestion_completed_at);
    });
    await t.test('backfill validates all collected dates before the first publication', async () => {
        const before = await dump();
        await assert.rejects(backfillSnapshotDates({ ...options,
            dates: [new Date(2026, 8, 8), new Date(2026, 8, 9)],
            fetchCommits: async () => new Map([['2026-09-08', entry(1).commitStats]]),
            fetchApi: async () => ({ statsMap: new Map([['2026-09-08', entry(1).apiMetrics]]),
                contributorDetailsMap: new Map([['2026-09-08', new Map()], ['2026-09-09', new Map()]]) }),
        }), /Invalid snapshot metric/);
        assert.deepEqual(await dump(), before);
    });
    await t.test('commit-only correction preserves PR facts and rebuilds all parent metrics', async () => {
        const before = await dump();
        await backfillSnapshotDates({ ...options, dates: [new Date(2026, 8, 7)],
            fetchCommits: async repo => new Map([[date, entry(repo.id, 4).commitStats]]),
            fetchApi: (repo, start, end) => fetchStoredApiHistory(pool, repo, start, end),
        });
        const after = await dump();
        assert.equal(after.activity_snapshots[0].new_commits, 8);
        assert.equal(after.activity_snapshots[0].new_prs, before.activity_snapshots[0].new_prs);
        assert.equal(after.contributor_daily_activities[0].prs_opened, before.contributor_daily_activities[0].prs_opened);
        assert.deepEqual(after.organizations[0].last_ingestion_completed_at, before.organizations[0].last_ingestion_completed_at);
        assert.deepEqual(await checkSnapshotConsistency(pool, 1), []);
    });
    await t.test('multi-chunk backfill retains committed dates and rejects a competing publication', async () => {
        const dates = Array.from({ length: 8 }, (_, i) => new Date(2026, 8, 10 + i));
        const dateKey = value => `2026-09-${String(value.getDate()).padStart(2, '0')}`;
        const history = (start, end, value) => new Map(dates.filter(d => d >= start && d <= end).map(d => [dateKey(d), value]));
        const published = [];
        await assert.rejects(backfillSnapshotDates({ ...options, dates,
            fetchCommits: async (repo, start, end) => history(start, end, entry(repo.id).commitStats),
            fetchApi: async (repo, start, end) => ({ statsMap: history(start, end, entry(repo.id).apiMetrics),
                contributorDetailsMap: history(start, end, new Map()) }),
            onPublished: async (publishedDate, generation) => {
                published.push(publishedDate);
                assert.equal(await readSnapshotGeneration(pool, 1), generation);
                if (published.length === 7) await reaggregateSnapshotDate({ pool, orgId: 1, snapshotDate: date });
            },
        }), /generation changed/);
        assert.equal(published.length, 7);
        assert.equal((await pool.query("SELECT * FROM activity_snapshots WHERE snapshot_date = '2026-09-17'")).rows.length, 0);
        assert.deepEqual(await checkSnapshotConsistency(pool, 1), []);
    });
    await t.test('read-only check CLI reports coverage, differences and missing dates', async () => {
        const url = new URL(connectionString);
        const env = { ...process.env, DB_HOST: url.hostname, DB_PORT: url.port || '5432',
            DB_NAME: decodeURIComponent(url.pathname.slice(1)), DB_USER: decodeURIComponent(url.username),
            DB_PASSWORD: decodeURIComponent(url.password), PGOPTIONS: `-c search_path=${schema}` };
        const check = day => promisify(execFile)(process.execPath, ['check_snapshot_consistency.js', day],
            { cwd: path.join(__dirname, '..'), env });
        await pool.query("UPDATE organizations SET name = 'hust-open-atom-club' WHERE id = 1");
        try {
            const before = await dump();
            const result = JSON.parse((await check(date)).stdout);
            assert.equal(result.checked_dates, 1);
            assert.deepEqual(result.differences, []);
            await assert.rejects(check('2099-01-01'), error => error.code === 1 && /No snapshots/.test(error.stderr));
            assert.deepEqual(await dump(), before);
            await pool.query('UPDATE activity_snapshots SET new_commits = new_commits + 1 WHERE snapshot_date = $1', [date]);
            await assert.rejects(check(date), error => error.code === 1 && JSON.parse(error.stdout).differences.length === 1);
        } finally {
            await pool.query("UPDATE organizations SET name = 'test-org' WHERE id = 1");
        }
    });
});
