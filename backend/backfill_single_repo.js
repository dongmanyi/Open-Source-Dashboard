// A single-repository repair publishes repository facts and parent totals together.
const { runGraphQLBackfillForRange, getScopedProgressFile } = require('./run_graphql_backfill');
const path = require('node:path');

async function main() {
    const repoName = process.argv[2];
    if (!repoName || !/^[A-Za-z0-9_.-]+$/.test(repoName)) throw new Error('Usage: node backfill_single_repo.js <repository-name>');
    const endDate = new Date();
    endDate.setHours(0, 0, 0, 0);
    endDate.setDate(endDate.getDate() - 1);
    const startDate = new Date(endDate);
    startDate.setDate(startDate.getDate() - 29);
    const progressFile = path.join(__dirname, repoName + '_' + path.basename(getScopedProgressFile(startDate, endDate)));
    await runGraphQLBackfillForRange({ startDate, endDate, repoName, progressFile });
}

if (require.main === module) main().catch(error => {
    console.error(error);
    process.exitCode = 1;
});
