// Recollect commits while retaining existing PR/Issue facts. Publish all levels together.
require('dotenv').config();
const { Pool } = require('pg');
const Redis = require('redis');
const { fetchCommitHistoryViaGraphQL } = require('./github_commit_history');
const { DEFAULT_PROPERTY_NAME, syncRepositorySigsFromGitHub } = require('./repository_sig_sync');
const { backfillSnapshotDates } = require('./snapshot_backfill');
const { invalidateSnapshotCaches } = require('./snapshot_batch');

const formatDate = date => [date.getFullYear(), String(date.getMonth() + 1).padStart(2, '0'),
    String(date.getDate()).padStart(2, '0')].join('-');

async function fetchStoredApiHistory(pool, repo, start, end) {
    const parameters = [repo.id, formatDate(start), formatDate(end)];
    const snapshots = await pool.query(`SELECT snapshot_date, new_prs, closed_merged_prs, new_issues, closed_issues
        FROM repo_snapshots WHERE repo_id = $1 AND snapshot_date BETWEEN $2 AND $3`, parameters);
    const facts = await pool.query(`SELECT cra.snapshot_date, c.github_username AS username,
        c.github_id, c.avatar_url, cra.prs_opened, cra.prs_closed, cra.issues_opened, cra.issues_closed
        FROM contributor_repo_activities cra JOIN contributors c ON c.id = cra.contributor_id
        WHERE cra.repo_id = $1 AND cra.snapshot_date BETWEEN $2 AND $3
          AND (cra.prs_opened <> 0 OR cra.prs_closed <> 0 OR cra.issues_opened <> 0 OR cra.issues_closed <> 0)`, parameters);
    const statsMap = new Map(snapshots.rows.map(row => [formatDate(row.snapshot_date), row]));
    const contributorDetailsMap = new Map([...statsMap.keys()].map(date => [date, new Map()]));
    for (const row of facts.rows) {
        const contributors = contributorDetailsMap.get(formatDate(row.snapshot_date));
        if (!contributors) throw new Error('Contributor facts lack a repository snapshot; use a full backfill');
        contributors.set(row.username, row);
    }
    for (const date = new Date(start); date <= end; date.setDate(date.getDate() + 1)) {
        if (!statsMap.has(formatDate(date))) throw new Error('Repository snapshots missing; use a full backfill');
    }
    return { statsMap, contributorDetailsMap };
}

async function main(args = process.argv.slice(2)) {
    const days = args.length ? Number(args[0]) : 30;
    if (args.length > 1 || !Number.isSafeInteger(days) || days < 1 || days > 3650) {
        throw new Error('Usage: node fix_git_stats.js [days: 1-3650]');
    }
    const pool = new Pool({ user: process.env.DB_USER, host: process.env.DB_HOST,
        database: process.env.DB_NAME, password: process.env.DB_PASSWORD, port: process.env.DB_PORT });
    const redis = Redis.createClient({ url: process.env.REDIS_URL || 'redis://localhost:6379',
        socket: { reconnectStrategy: false } });
    redis.on('error', error => console.error('Redis:', error.message));
    const orgName = 'hust-open-atom-club';
    try {
        await redis.connect();
        await syncRepositorySigsFromGitHub({ pool, githubToken: process.env.GITHUB_TOKEN,
            orgName, propertyName: process.env.GITHUB_SIG_PROPERTY || DEFAULT_PROPERTY_NAME });
        const { rows: orgs } = await pool.query('SELECT id FROM organizations WHERE name = $1', [orgName]);
        if (!orgs.length) throw new Error('Organization not found');
        const orgId = orgs[0].id;
        const { rows: repositories } = await pool.query(
            'SELECT id, name, sig_id FROM repositories WHERE org_id = $1 AND sig_id IS NOT NULL AND is_in_organization = TRUE ORDER BY id', [orgId]);
        if (!repositories.length) throw new Error('No tracked repositories');
        const dates = [];
        for (let i = days; i >= 1; i--) {
            const date = new Date();
            date.setHours(0, 0, 0, 0);
            date.setDate(date.getDate() - i);
            dates.push(date);
        }
        await backfillSnapshotDates({ pool, orgId, orgName, repositories, dates,
            fetchCommits: (repo, start, end) => fetchCommitHistoryViaGraphQL(repo.name, start, end),
            fetchApi: (repo, start, end) => fetchStoredApiHistory(pool, repo, start, end),
            afterCommit: () => invalidateSnapshotCaches(redis, pool, orgId, orgName),
            onPublished: async date => console.log('Published corrected commit history:', date),
        });
    } finally {
        await pool.end();
        if (redis.isOpen) await redis.quit();
    }
}

if (require.main === module) main().catch(error => { console.error(error.message); process.exitCode = 1; });
module.exports = { fetchStoredApiHistory, main };
