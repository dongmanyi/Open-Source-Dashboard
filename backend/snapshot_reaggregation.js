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
