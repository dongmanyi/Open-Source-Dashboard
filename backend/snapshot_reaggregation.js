const { validateDate } = require('./snapshot_batch');
const { acquireContributorWriteLocks } = require('./contributor_daily_aggregation');
const { rebuildSnapshotHierarchy, assertSnapshotConsistency } = require('./snapshot_hierarchy');

// Repair parents from repository facts, without marking a new GitHub ingestion.
async function reaggregateSnapshotDate({ pool, orgId, snapshotDate }) {
    validateDate(snapshotDate);
    const client = await pool.connect();
    try {
        await client.query('BEGIN');
        await client.query("SET LOCAL lock_timeout = '30s'");
        const org = await client.query('SELECT id FROM organizations WHERE id = $1 FOR UPDATE', [orgId]);
        if (!org.rows.length) throw new Error('Organization not found');
        await acquireContributorWriteLocks(client, orgId, snapshotDate);
        // Match the full publisher's current tracked set, including zero days.
        // created_at is the local registration time, not historical membership;
        // it cannot justify silently excluding a repository from coverage.
        const coverage = await client.query(`SELECT r.id, r.name, rs.repo_id AS snapshot_repo_id
            FROM repositories r LEFT JOIN repo_snapshots rs
              ON rs.repo_id = r.id AND rs.snapshot_date = $2
            WHERE r.org_id = $1 AND r.sig_id IS NOT NULL AND r.is_in_organization = TRUE
            ORDER BY r.id`, [orgId, snapshotDate]);
        if (!coverage.rows.length) throw new Error('No tracked repositories; use a full backfill');
        const missing = coverage.rows.filter(row => row.snapshot_repo_id === null);
        if (missing.length) {
            throw new Error(`Incomplete repository coverage for ${snapshotDate}: ${missing.map(row => row.name).join(', ')}; use a full backfill`);
        }
        await rebuildSnapshotHierarchy(client, orgId, snapshotDate);
        await assertSnapshotConsistency(client, orgId, snapshotDate);
        await client.query('UPDATE organizations SET snapshot_generation = snapshot_generation + 1 WHERE id = $1', [orgId]);
        await client.query('COMMIT');
    } catch (error) {
        await client.query('ROLLBACK').catch(() => {});
        throw error;
    } finally {
        client.release();
    }
}

module.exports = { reaggregateSnapshotDate };
