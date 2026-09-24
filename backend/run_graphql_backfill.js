/**
 * GraphQL Backfill Script
 * 
 * 使用 GraphQL API 高效回填历史数据。
 * 按日期原子发布，采集窗口限制为七天。
 * 
 * 用法: node run_graphql_backfill.js [days] [--flush-cache]
 * 例如: node run_graphql_backfill.js 365
 * 例如: node run_graphql_backfill.js 30 --flush-cache
 */

require('dotenv').config();
const { Pool } = require('pg');
const Redis = require('redis');
const axios = require('axios');
const fs = require('fs/promises');
const path = require('path');
const {
    fetchCommitHistoryViaGraphQL: fetchCommitHistoryRangeViaGraphQL,
    fetchCommitsViaGraphQL: fetchCommitsForDayViaGraphQL,
} = require('./github_commit_history');
const {
    DEFAULT_PROPERTY_NAME,
    syncRepositorySigsFromGitHub,
} = require('./repository_sig_sync');
const { runPromisesWithConcurrency } = require('./promise_concurrency');
const {
    MAX_RATE_LIMIT_RETRIES,
    getPrimaryRateLimitWaitMs,
} = require('./github_rate_limit');
const { backfillSnapshotDates } = require('./snapshot_backfill');
const { readSnapshotGeneration, invalidateSnapshotCaches } = require('./snapshot_batch');
const {
    storeContributorActivities: persistContributorActivities,
} = require('./contributor_api_stats');

// --- Configuration ---
const GITHUB_TOKEN = process.env.GITHUB_TOKEN;
const ORG_NAME = 'hust-open-atom-club';
const PROGRESS_FILE = path.join(__dirname, 'backfill_progress.json');

// --- Optimized Concurrency Settings ---
const BASE_DELAY_MS = 100;            // Base delay between requests (reduced from 500ms)

// --- Rate Limit Tracking ---
let rateLimitRemaining = 5000;
let rateLimitResetTime = null;

// --- Database Connection ---
const pool = new Pool({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_NAME,
    password: process.env.DB_PASSWORD,
    port: process.env.DB_PORT,
});

// --- Redis Connection ---
const redisClient = Redis.createClient({
    url: process.env.REDIS_URL || 'redis://localhost:6379',
    socket: { reconnectStrategy: false },
});
redisClient.on('error', (err) => console.error('Redis Client Error', err));

// --- Utility Functions ---
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

const formatDate = (date) => {
    const year = date.getFullYear();
    const month = (date.getMonth() + 1).toString().padStart(2, '0');
    const day = date.getDate().toString().padStart(2, '0');
    return `${year}-${month}-${day}`;
};

function normalizeDate(date) {
    const normalized = new Date(date);
    normalized.setHours(0, 0, 0, 0);
    return normalized;
}

function buildDateList(startDate, endDate) {
    const allDates = [];
    const currentDate = normalizeDate(startDate);
    const lastDate = normalizeDate(endDate);

    while (currentDate <= lastDate) {
        allDates.push(new Date(currentDate));
        currentDate.setDate(currentDate.getDate() + 1);
    }

    return allDates;
}

function getScopedProgressFile(startDate, endDate) {
    const startDateStr = formatDate(startDate);
    const endDateStr = formatDate(endDate);
    const fileName = startDateStr === endDateStr
        ? `backfill_progress_${startDateStr}.json`
        : `backfill_progress_${startDateStr}_${endDateStr}.json`;
    return path.join(__dirname, fileName);
}

// --- GraphQL API with Adaptive Rate Limiting ---
async function githubGraphQL(query, variables = {}, retryCount = 0) {
    if (!GITHUB_TOKEN) {
        throw new Error("GITHUB_TOKEN is not set in environment variables.");
    }

    // Adaptive delay based on remaining rate limit
    let delayMs = BASE_DELAY_MS;
    if (rateLimitRemaining < 500) {
        // Very low - significantly slow down
        delayMs = 2000;
        console.warn(`[Rate Limit] Low remaining points (${rateLimitRemaining}), slowing down...`);
    } else if (rateLimitRemaining < 1000) {
        // Getting low - moderate slowdown
        delayMs = 500;
    } else if (rateLimitRemaining < 2000) {
        // Caution zone
        delayMs = 200;
    }

    await delay(delayMs);

    try {
        const response = await axios.post(
            'https://api.github.com/graphql',
            { query, variables },
            {
                timeout: 60000,
                headers: {
                    'Authorization': `Bearer ${GITHUB_TOKEN}`,
                    'Content-Type': 'application/json',
                }
            }
        );

        // Update rate limit tracking from response headers
        if (response.headers['x-ratelimit-remaining']) {
            rateLimitRemaining = parseInt(response.headers['x-ratelimit-remaining'], 10);
        }
        if (response.headers['x-ratelimit-reset']) {
            rateLimitResetTime = parseInt(response.headers['x-ratelimit-reset'], 10) * 1000;
        }

        if (response.data.errors) {
            const errorMessages = response.data.errors.map(e => e.message).join(', ');
            throw new Error(`GraphQL Error: ${errorMessages}`);
        }

        return response.data.data;
    } catch (error) {
        const waitTime = getPrimaryRateLimitWaitMs(error);
        if (waitTime !== null && retryCount < MAX_RATE_LIMIT_RETRIES) {
            console.warn(`[Rate Limit] Primary limit exhausted. Waiting ${Math.ceil(waitTime / 1000)} seconds...`);
            rateLimitRemaining = 0;
            await delay(waitTime);
            rateLimitRemaining = 5000;
            return githubGraphQL(query, variables, retryCount + 1);
        }
        throw error;
    }
}

// --- Fetch Repo Stats via GraphQL ---
async function fetchRepoStatsViaGraphQL(repoName, startDate, endDate, graphQLClient = githubGraphQL) {
    return require('./github_repo_history').fetchRepoStatsViaGraphQL(repoName, startDate, endDate, graphQLClient, ORG_NAME);
}

// --- Store Stats to Database ---

async function storeContributorActivities(repoId, dateStr, contributorDetails, databasePool = pool) {
    try {
        await persistContributorActivities({
            pool: databasePool,
            orgName: ORG_NAME,
            repoId,
            snapshotDate: dateStr,
            contributorDetails,
        });
    } catch (error) {
        console.error('[Contributors] Error in storeContributorActivities:', error.message);
        throw error;
    }
}

// Keep the default client here so the backfill shares its adaptive rate-limit tracking.
async function fetchCommitsViaGraphQL(repoName, targetDate, graphQLClient = githubGraphQL) {
    return fetchCommitsForDayViaGraphQL(repoName, targetDate, graphQLClient, ORG_NAME);
}

async function fetchCommitHistoryViaGraphQL(repoName, startDate, endDate, graphQLClient = githubGraphQL) {
    return fetchCommitHistoryRangeViaGraphQL(repoName, startDate, endDate, graphQLClient, ORG_NAME);
}


// --- Progress Checkpoint Functions ---
async function loadProgress(progressFile = PROGRESS_FILE) {
    try {
        const data = await fs.readFile(progressFile, 'utf8');
        return JSON.parse(data);
    } catch {
        return { completedRepos: {}, gitCompleted: false, graphqlCompleted: false };
    }
}

async function saveProgress(progress, progressFile = PROGRESS_FILE) {
    await fs.writeFile(progressFile, JSON.stringify(progress, null, 2));
}

async function clearProgress(progressFile = PROGRESS_FILE) {
    try {
        await fs.unlink(progressFile);
    } catch {
        // File doesn't exist, that's fine
    }
}

// --- Main Backfill Function (Optimized) ---
async function runGraphQLBackfillForRange({ startDate, endDate, progressFile = PROGRESS_FILE, description, repoName = null, resetProgress = false }) {
    const start = normalizeDate(startDate);
    const end = normalizeDate(endDate);
    if (!Number.isFinite(start.getTime()) || !Number.isFinite(end.getTime()) || start > end) {
        throw new Error('Invalid date range for GraphQL backfill.');
    }
    try {
        await redisClient.connect();
        await syncRepositorySigsFromGitHub({ pool, githubToken: GITHUB_TOKEN,
            orgName: ORG_NAME, propertyName: process.env.GITHUB_SIG_PROPERTY || DEFAULT_PROPERTY_NAME });
        const { rows: orgs } = await pool.query('SELECT id FROM organizations WHERE name = $1', [ORG_NAME]);
        if (!orgs.length) throw new Error('Organization not found');
        const orgId = orgs[0].id;
        const { rows: tracked } = await pool.query(
            'SELECT id, name, sig_id FROM repositories WHERE org_id = $1 AND sig_id IS NOT NULL AND is_in_organization = TRUE ORDER BY id', [orgId]);
        const repositories = repoName ? tracked.filter(r => r.name === repoName) : tracked;
        if (!repositories.length) throw new Error('No matching tracked repositories');
        await invalidateSnapshotCaches(redisClient, pool, orgId, ORG_NAME);
        // A v2 checkpoint represents committed whole dates, never per-repo writes.
        // Changed membership or another publisher invalidates the checkpoint.
        const scope = JSON.stringify({ orgId, repoName, start: formatDate(start), end: formatDate(end), tracked });
        const generation = await readSnapshotGeneration(pool, orgId);
        const saved = resetProgress ? null : await loadProgress(progressFile);
        const progress = saved?.version === 2 && saved.scope === scope && saved.generation === generation
            ? saved : { version: 2, scope, generation, publishedDates: [] };
        const dates = buildDateList(start, end).filter(date => !progress.publishedDates.includes(formatDate(date)));
        console.log('Atomic backfill:', description || scope, 'pending days:', dates.length);
        await backfillSnapshotDates({ pool, orgId, orgName: ORG_NAME, repositories, dates, partial: Boolean(repoName), expectedGeneration: generation,
            fetchCommits: (repo, first, last) => fetchCommitHistoryViaGraphQL(repo.name, first, last),
            fetchApi: (repo, first, last) => fetchRepoStatsViaGraphQL(repo.name, first, last),
            afterCommit: () => invalidateSnapshotCaches(redisClient, pool, orgId, ORG_NAME),
            onPublished: async (date, publishedGeneration) => {
                progress.publishedDates.push(date);
                progress.generation = publishedGeneration;
                await saveProgress(progress, progressFile);
                console.log('Published complete date:', date);
            },
        });
        await clearProgress(progressFile);
    } finally {
        await pool.end();
        if (redisClient.isOpen) await redisClient.quit();
    }
}

async function runGraphQLBackfill(days = 30, options = {}) {
    const today = normalizeDate(new Date());
    const startDate = new Date(today);
    startDate.setDate(today.getDate() - days);

    const endDate = new Date(today);
    endDate.setDate(today.getDate() - 1);

    return runGraphQLBackfillForRange({
        startDate,
        endDate,
        progressFile: options.progressFile || PROGRESS_FILE,
        description: `${days} days of data`,
        flushCache: options.flushCache !== undefined ? options.flushCache : false,
    });
}

// --- Run ---
if (require.main === module) {
    const args = process.argv.slice(2);
    const flushCache = args.includes('--flush-cache');
    const daysArg = args.find((arg) => /^\d+$/.test(arg));
    const days = daysArg ? parseInt(daysArg, 10) : 730;

    runGraphQLBackfill(days, { flushCache }).catch((error) => {
        console.error(error);
        process.exitCode = 1;
    });
}

module.exports = {
    runGraphQLBackfill,
    runGraphQLBackfillForRange,
    fetchRepoStatsViaGraphQL,
    fetchCommitHistoryViaGraphQL,
    fetchCommitsViaGraphQL,
    storeContributorActivities,
    runPromisesWithConcurrency,
    formatDate,
    getScopedProgressFile,
};
