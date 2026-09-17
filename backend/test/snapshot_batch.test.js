const assert = require('node:assert/strict');
const test = require('node:test');
const { validateBatch, collectSnapshotBatch, publishSnapshotBatch } = require('../snapshot_batch');
const { collectRepoApiStats } = require('../repo_api_ingestion');
const { installSnapshotCache } = require('../snapshot_cache');
const { fetchRepoStatsViaGraphQL } = require('../github_repo_history');

const repositories = [{ id: 1, name: 'one', sig_id: 1 }, { id: 2, name: 'two', sig_id: 2 }];
const snapshotDate = '2026-09-07';
const zero = id => ({ repoId: id,
    commitStats: { new_commits: 0, lines_added: 0, lines_deleted: 0, authorStats: {} },
    apiMetrics: { new_prs: 0, closed_merged_prs: 0, new_issues: 0, closed_issues: 0 }, contributorDetails: [] });

test('batch validation requires every repository, including zero-activity repositories', () => {
    assert.doesNotThrow(() => validateBatch(repositories, [zero(1), zero(2)], snapshotDate));
    for (const entries of [[zero(1)], [zero(1), zero(1)], [zero(1), zero(3)]]) {
        assert.throws(() => validateBatch(repositories, entries, snapshotDate));
    }
    assert.throws(() => validateBatch(repositories, [zero(1), zero(2)], '2026-02-30'), /Invalid snapshot date/);
    for (const value of [-1, 0.5, NaN, undefined, 2147483648]) {
        const entry = zero(1);
        entry.commitStats.new_commits = value;
        assert.throws(() => validateBatch(repositories, [entry, zero(2)], snapshotDate), /Invalid snapshot metric/);
    }
});

test('collection failure rejects the entire batch after in-flight work completes', async () => {
    const finished = [];
    await assert.rejects(collectSnapshotBatch({ repositories, snapshotDate,
        collectCommits: async repo => {
            if (repo.id === 1) throw new Error('GitHub unavailable');
            return zero(repo.id).commitStats;
        },
        collectApi: async repo => { finished.push(repo.id); return zero(repo.id); },
    }), AggregateError);
    assert.deepEqual(finished, [2]);
});

test('REST search truncation and incomplete-results flags are collection failures', async () => {
    for (const response of [{ items: [], total_count: 1 }, { items: [], total_count: 0, incomplete_results: true }]) {
        await assert.rejects(collectRepoApiStats({ githubRest: async () => response,
            orgName: 'org', repoName: 'repo', snapshotDate }), /Incomplete GitHub/);
    }
});

test('stale generations and changed membership abort before any snapshot writes', async () => {
    for (const staleGeneration of [true, false]) {
        const queries = [];
        let released = false;
        const client = { async query(sql) {
            queries.push(sql);
            if (sql.includes('FOR UPDATE')) return { rows: [{ id: 1, snapshot_generation: staleGeneration ? '2' : '1' }] };
            if (sql.startsWith('SELECT id, name')) return { rows: repositories.slice(0, 1) };
            return { rows: [] };
        }, release() { released = true; } };
        await assert.rejects(publishSnapshotBatch({ pool: { connect: async () => client },
            orgId: 1, orgName: 'org', repositories, entries: [zero(1), zero(2)],
            snapshotDate, expectedGeneration: '1' }), staleGeneration ? /generation changed/ : /membership changed/);
        assert.equal(queries.at(-1), 'ROLLBACK');
        assert.equal(queries.some(sql => sql.includes('INSERT')), false);
        assert.equal(released, true);
    }
});

test('partial repairs cannot mark the entire organization fresh', async () => {
    await assert.rejects(publishSnapshotBatch({ repositories, entries: [zero(1), zero(2)],
        snapshotDate, expectedGeneration: '1', partial: true, markFresh: true }), /partial batch/);
});

test('in-flight cache writes cannot populate a newer snapshot generation', async () => {
    let middleware;
    let generation = '1';
    const keys = new Map();
    const redis = { get: async key => keys.get(key), setEx: async (key, ttl, value) => keys.set(key, value) };
    installSnapshotCache({ use: (path, fn) => { middleware = fn; } }, redis,
        { query: async () => ({ rows: [{ snapshot_generation: generation }] }) }, 'org');
    let resume;
    const paused = new Promise(resolve => { resume = resolve; });
    const old = new Promise((resolve, reject) => {
        middleware({}, {}, error => {
            if (error) return reject(error);
            paused.then(async () => { await redis.setEx('summary', 10, 'old'); resolve(); }).catch(reject);
        });
    });
    await new Promise(resolve => setImmediate(resolve));
    generation = '2';
    await new Promise((resolve, reject) => middleware({}, {}, async error => {
        if (error) return reject(error);
        try { await redis.setEx('summary', 10, 'new'); resolve(); } catch (err) { reject(err); }
    }));
    resume();
    await old;
    assert.equal(keys.get('summary:snapshot-generation:1'), 'old');
    assert.equal(keys.get('summary:snapshot-generation:2'), 'new');
});

test('GraphQL collects recently closed old PRs and refuses missing repositories', async () => {
    const date = new Date(2026, 8, 7);
    await assert.rejects(fetchRepoStatsViaGraphQL('repo', date, date,
        async () => ({ repository: null }), 'org'), /not found/);
    const connection = nodes => ({ nodes, pageInfo: { hasNextPage: false, endCursor: null } });
    const result = await fetchRepoStatsViaGraphQL('repo', date, date, async query => {
        assert.match(query, /field: UPDATED_AT/);
        return { repository: { issues: connection([]), pullRequests: connection([
            { createdAt: '2020-01-01', updatedAt: '2026-09-08', closedAt: null },
            { createdAt: '2019-01-01', updatedAt: '2026-09-07', closedAt: '2026-09-07', author: { login: 'alice' } },
        ]) } };
    }, 'org');
    assert.equal(result.statsMap.get(snapshotDate).closed_merged_prs, 1);
    assert.equal(result.contributorDetailsMap.get(snapshotDate).get('alice').prs_closed, 1);
});
