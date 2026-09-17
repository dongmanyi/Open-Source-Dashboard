const { persistRepoCommitStats } = require('./commit_author_stats');
const { persistRepoApiStats } = require('./contributor_api_stats');
const { acquireContributorWriteLocks } = require('./contributor_daily_aggregation');
const { recordSuccessfulIngestion } = require('./data_freshness');
const { rebuildSnapshotHierarchy, assertSnapshotConsistency } = require('./snapshot_hierarchy');
const { runPromisesWithConcurrency } = require('./promise_concurrency');

const COMMIT_FIELDS = ['new_commits', 'lines_added', 'lines_deleted'];
const API_FIELDS = ['new_prs', 'closed_merged_prs', 'new_issues', 'closed_issues'];

function validateDate(value) {
    if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(value)
        || !Number.isFinite(Date.parse(value)) || new Date(value).toISOString().slice(0, 10) !== value) {
        throw new Error(`Invalid snapshot date: ${value}`);
    }
}

function validateMetrics(metrics, fields) {
    for (const key of fields) {
        if (!Number.isSafeInteger(metrics?.[key]) || metrics[key] < 0 || metrics[key] > 2147483647) {
            throw new Error(`Invalid snapshot metric: ${key}`);
        }
    }
}

function validateBatch(repositories, entries, snapshotDate) {
    validateDate(snapshotDate);
    const expected = new Set(repositories.map(r => r.id));
    if (!expected.size || expected.size !== repositories.length || entries.length !== expected.size) {
        throw new Error('Incomplete or duplicate repository batch');
    }
    for (const entry of entries) {
        if (!expected.delete(entry.repoId)) throw new Error('Unexpected or duplicate repository result');
        validateMetrics(entry.commitStats, COMMIT_FIELDS);
        validateMetrics(entry.apiMetrics, API_FIELDS);
        if (!entry.commitStats.authorStats || typeof entry.commitStats.authorStats !== 'object'
            || Array.isArray(entry.commitStats.authorStats) || !Array.isArray(entry.contributorDetails)) {
            throw new Error('Missing contributor facts');
        }
        for (const author of Object.values(entry.commitStats.authorStats)) {
            validateMetrics(author, ['commits', 'lines_added', 'lines_deleted']);
        }
        for (const contributor of entry.contributorDetails) {
            if (!contributor.username) throw new Error('Missing contributor username');
            validateMetrics(contributor, ['prs_opened', 'prs_closed', 'issues_opened', 'issues_closed']);
        }
    }
}

async function collectSnapshotBatch({ repositories, snapshotDate, collectCommits, collectApi, concurrency = 3 }) {
    validateDate(snapshotDate);
    if (!Number.isInteger(concurrency) || concurrency < 1) throw new Error('Invalid collection concurrency');
    const entries = await runPromisesWithConcurrency(repositories.map(repo => async () => ({
        repoId: repo.id,
        commitStats: await collectCommits(repo, snapshotDate),
        ...await collectApi(repo, snapshotDate),
    })), concurrency);
    validateBatch(repositories, entries, snapshotDate);
    return entries;
}

// Single-repository repairs publish that repository plus rebuilt parent totals.
// Full ingestion requires every currently tracked repository, including zero days.
async function publishSnapshotBatch({ pool, orgId, orgName, snapshotDate, repositories, entries,
    expectedGeneration, partial = false, markFresh = false, afterCommit = async () => {} }) {
    validateBatch(repositories, entries, snapshotDate);
    if (!/^\d+$/.test(String(expectedGeneration))) throw new Error('Missing collection generation');
    if (partial && markFresh) throw new Error('A partial batch cannot mark organization data fresh');
    const client = await pool.connect();
    try {
        await client.query('BEGIN');
        await client.query("SET LOCAL lock_timeout = '30s'");
        // Metadata sync already locks the organization row before changing SIG assignments.
        const org = await client.query('SELECT id, snapshot_generation FROM organizations WHERE id = $1 AND name = $2 FOR UPDATE', [orgId, orgName]);
        if (org.rows.length !== 1) throw new Error('Organization not found');
        if (String(org.rows[0].snapshot_generation) !== String(expectedGeneration)) {
            throw new Error('Snapshot generation changed during collection; recollect the batch');
        }
        await acquireContributorWriteLocks(client, orgId, snapshotDate);
        const current = await client.query(
            'SELECT id, name, sig_id FROM repositories WHERE org_id = $1 AND sig_id IS NOT NULL AND is_in_organization = TRUE ORDER BY id', [orgId]);
        const requested = new Map(repositories.map(r => [r.id, r]));
        const selected = partial ? current.rows.filter(r => requested.has(r.id)) : current.rows;
        if (selected.length !== repositories.length || selected.some(r => {
            const previous = requested.get(r.id);
            return !previous || previous.name !== r.name || previous.sig_id !== r.sig_id;
        })) throw new Error('Repository membership changed during collection; recollect the batch');

        // All writes use this connection. None of the repository helpers may commit it.
        for (const entry of [...entries].sort((a, b) => a.repoId - b.repoId)) {
            await persistRepoCommitStats({ client, repoId: entry.repoId, snapshotDate, commitStats: entry.commitStats });
            await persistRepoApiStats({ client, orgName, repoId: entry.repoId, snapshotDate,
                apiMetrics: entry.apiMetrics, contributorDetails: entry.contributorDetails });
        }
        await rebuildSnapshotHierarchy(client, orgId, snapshotDate);
        await assertSnapshotConsistency(client, orgId, snapshotDate);
        if (markFresh) await recordSuccessfulIngestion(client, orgId);
        await client.query('UPDATE organizations SET snapshot_generation = snapshot_generation + 1 WHERE id = $1', [orgId]);
        await client.query('COMMIT');
    } catch (error) {
        await client.query('ROLLBACK').catch(() => {});
        throw error;
    } finally {
        client.release();
    }
    // A cache failure must never be reported as a database rollback.
    try {
        await afterCommit();
    } catch (cause) {
        const error = new Error('Snapshot committed, but cache invalidation failed', { cause });
        error.committed = true;
        throw error;
    }
}

async function readSnapshotGeneration(pool, orgId) {
    const result = await pool.query('SELECT snapshot_generation FROM organizations WHERE id = $1', [orgId]);
    if (!result.rows.length) throw new Error('Organization not found');
    return String(result.rows[0].snapshot_generation);
}

async function invalidateSnapshotCaches(redisClient, pool, orgId, orgName) {
    const sigs = await pool.query('SELECT id FROM special_interest_groups WHERE org_id = $1', [orgId]);
    const patterns = [`org:${orgName}:*`, 'org:day:*', 'contributors:*', 'sigs:compare:*',
        ...sigs.rows.map(sig => `sig:${sig.id}:*`)];
    for (const pattern of patterns) {
        const keys = [];
        for await (const key of redisClient.scanIterator({ MATCH: pattern, COUNT: 100 })) keys.push(key);
        if (keys.length) await redisClient.del(keys);
    }
}

module.exports = { validateDate, validateBatch, collectSnapshotBatch, publishSnapshotBatch, invalidateSnapshotCaches, readSnapshotGeneration };
