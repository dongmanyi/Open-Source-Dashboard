const { collectSnapshotBatch, publishSnapshotBatch, readSnapshotGeneration } = require('./snapshot_batch');
const { runPromisesWithConcurrency } = require('./promise_concurrency');

function formatDate(date) {
    return [date.getFullYear(), String(date.getMonth() + 1).padStart(2, '0'),
        String(date.getDate()).padStart(2, '0')].join('-');
}

// Limit memory to seven days across repositories; each date is a separate atomic
// publication, so a failed later date retains previously committed complete days.
async function backfillSnapshotDates({ pool, orgId, orgName, repositories, dates,
    fetchCommits, fetchApi, partial = false, afterCommit, expectedGeneration, onPublished = async () => {} }) {
    if (!dates.length) return;
    let generation = expectedGeneration ?? await readSnapshotGeneration(pool, orgId);
    for (let start = 0; start < dates.length; start += 7) {
        const chunk = dates.slice(start, start + 7);
        const histories = new Map();
        await runPromisesWithConcurrency(repositories.map(repo => async () => {
            const commits = await fetchCommits(repo, chunk[0], chunk[chunk.length - 1]);
            const api = await fetchApi(repo, chunk[0], chunk[chunk.length - 1]);
            histories.set(repo.id, { commits, api });
        }), 3);
        // Validate the whole collected chunk before publishing its first day.
        const batches = [];
        for (const date of chunk) {
            const snapshotDate = formatDate(date);
            const entries = await collectSnapshotBatch({ repositories, snapshotDate,
                collectCommits: async repo => histories.get(repo.id).commits.get(snapshotDate),
                collectApi: async repo => {
                    const { statsMap, contributorDetailsMap } = histories.get(repo.id).api;
                    const stats = statsMap.get(snapshotDate);
                    if (!contributorDetailsMap.has(snapshotDate)) throw new Error('Missing contributor facts for backfill date');
                    return { apiMetrics: stats,
                        contributorDetails: Array.from(contributorDetailsMap.get(snapshotDate).values()) };
                } });
            batches.push({ snapshotDate, entries });
        }
        for (const batch of batches) {
            await publishSnapshotBatch({ pool, orgId, orgName, repositories, ...batch,
                expectedGeneration: generation, partial, afterCommit });
            generation = String(BigInt(generation) + 1n);
            await onPublished(batch.snapshotDate, generation);
        }
    }
}

module.exports = { backfillSnapshotDates };
