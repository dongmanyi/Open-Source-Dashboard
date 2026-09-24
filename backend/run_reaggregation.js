require('dotenv').config();
const { Pool } = require('pg');
const Redis = require('redis');
const { validateDate, invalidateSnapshotCaches } = require('./snapshot_batch');
const { reaggregateSnapshotDate } = require('./snapshot_reaggregation');

async function main(args = process.argv.slice(2)) {
    if (args.length !== 2 || args[0] !== '--date') {
        throw new Error('Usage: node run_reaggregation.js --date YYYY-MM-DD');
    }
    const snapshotDate = args[1];
    validateDate(snapshotDate);
    const pool = new Pool({ user: process.env.DB_USER, host: process.env.DB_HOST,
        database: process.env.DB_NAME, password: process.env.DB_PASSWORD, port: process.env.DB_PORT });
    const redis = Redis.createClient({ url: process.env.REDIS_URL || 'redis://localhost:6379',
        socket: { reconnectStrategy: false } });
    redis.on('error', error => console.error('Redis:', error.message));
    const orgName = 'hust-open-atom-club';
    try {
        await redis.connect();
        const { rows } = await pool.query('SELECT id FROM organizations WHERE name = $1', [orgName]);
        if (!rows.length) throw new Error('Organization not found');
        await reaggregateSnapshotDate({ pool, orgId: rows[0].id, snapshotDate });
        console.log('Committed reaggregation:', snapshotDate);
        await invalidateSnapshotCaches(redis, pool, rows[0].id, orgName);
    } finally {
        await pool.end();
        if (redis.isOpen) await redis.quit();
    }
}

if (require.main === module) main().catch(error => { console.error(error.message); process.exitCode = 1; });
module.exports = { main };
