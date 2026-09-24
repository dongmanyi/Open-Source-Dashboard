/**
 * Date Range Backfill Script
 *
 * 用法:
 *   node backfill_date_range.js --date YYYY-MM-DD
 *   node backfill_date_range.js --start-date YYYY-MM-DD --end-date YYYY-MM-DD
 *
 * 可选参数:
 *   --flush-cache    兼容旧命令；每次发布后自动失效相关缓存
 *   --reset-existing 忽略已保存进度；旧数据保留到原子发布完成
 *   --help           显示帮助信息
 */

require('dotenv').config();
const path = require('path');
const {
    runGraphQLBackfillForRange,
    formatDate,
    getScopedProgressFile,
} = require('./run_graphql_backfill');

function printUsage() {
    console.log(`
Date range backfill usage:

  node backfill_date_range.js --date 2026-03-12
  node backfill_date_range.js --start-date 2026-03-12 --end-date 2026-03-14
  node backfill_date_range.js --date 2026-03-12 --flush-cache
  node backfill_date_range.js --start-date 2026-03-12 --end-date 2026-03-14 --reset-existing

Options:
  --date         Backfill a single day
  --start-date   Range start date in YYYY-MM-DD
  --end-date     Range end date in YYYY-MM-DD
  --flush-cache  Compatibility flag; relevant caches are always invalidated after publication
  --reset-existing
                 Ignore saved progress; preserve existing rows until atomic replacement
  --help         Show this help message
`);
}

function parseDateLiteral(value, flagName) {
    if (!value) {
        throw new Error(`${flagName} requires a value in YYYY-MM-DD format.`);
    }

    if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) {
        throw new Error(`${flagName} must use YYYY-MM-DD format.`);
    }

    const [year, month, day] = value.split('-').map(Number);
    const parsedDate = new Date(year, month - 1, day);

    if (formatDate(parsedDate) !== value) {
        throw new Error(`${flagName} is not a valid calendar date.`);
    }

    return parsedDate;
}

// Existing snapshots remain visible until their replacements commit.

async function main() {
    const args = process.argv.slice(2);
    let singleDateArg = null;
    let startDateArg = null;
    let endDateArg = null;
    let flushCache = false;
    let resetExisting = false;

    for (let i = 0; i < args.length; i++) {
        const arg = args[i];

        if (arg === '--help') {
            printUsage();
            return;
        }

        if (arg === '--flush-cache') {
            flushCache = true;
            continue;
        }

        if (arg === '--reset-existing') {
            resetExisting = true;
            continue;
        }

        if (arg === '--date') {
            singleDateArg = args[++i];
            continue;
        }

        if (arg === '--start-date') {
            startDateArg = args[++i];
            continue;
        }

        if (arg === '--end-date') {
            endDateArg = args[++i];
            continue;
        }

        throw new Error(`Unknown argument: ${arg}`);
    }

    if (singleDateArg && (startDateArg || endDateArg)) {
        throw new Error('Use either --date or --start-date/--end-date, not both.');
    }

    if (!singleDateArg && (!startDateArg || !endDateArg)) {
        throw new Error('You must provide either --date or both --start-date and --end-date.');
    }

    const startDate = singleDateArg
        ? parseDateLiteral(singleDateArg, '--date')
        : parseDateLiteral(startDateArg, '--start-date');
    const endDate = singleDateArg
        ? parseDateLiteral(singleDateArg, '--date')
        : parseDateLiteral(endDateArg, '--end-date');

    if (startDate > endDate) {
        throw new Error('--start-date cannot be later than --end-date.');
    }

    const progressFile = getScopedProgressFile(startDate, endDate);
    const description = startDate.getTime() === endDate.getTime()
        ? `date ${formatDate(startDate)}`
        : `date range ${formatDate(startDate)} to ${formatDate(endDate)}`;

    console.log(`Using progress file: ${path.basename(progressFile)}`);

    if (resetExisting) {
        console.log('Ignoring saved progress; existing data will be replaced atomically after collection.');
    }

    await runGraphQLBackfillForRange({
        startDate,
        endDate,
        progressFile,
        description,
        flushCache,
        resetProgress: resetExisting,
    });
}

main().catch((error) => {
    console.error(error.message);
    process.exitCode = 1;
});
