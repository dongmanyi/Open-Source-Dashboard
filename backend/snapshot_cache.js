const { AsyncLocalStorage } = require('node:async_hooks');

// All pool.query calls in an API request share a read-only MVCC snapshot. The
// generation is read inside that same transaction, including on cache hits.
function installSnapshotCache(app, redis, pool, orgName) {
    const context = new AsyncLocalStorage();
    const query = pool.query.bind(pool);
    pool.query = (...args) => {
        const request = context.getStore();
        if (!request) return query(...args);
        if (request.isClosed()) return Promise.reject(new Error('API snapshot transaction is closed'));
        return request.client.query(...args);
    };
    for (const method of ['get', 'setEx']) {
        const original = redis[method].bind(redis);
        redis[method] = (key, ...args) => {
            const generation = context.getStore()?.generation;
            return original(generation === undefined ? key : `${key}:snapshot-generation:${generation}`, ...args);
        };
    }
    app.use('/api', async (req, res, next) => {
        let client;
        let closed = false;
        let cleanupPromise;
        let connectionError;
        const onClientError = error => {
            connectionError = error;
            // A checked-out pg client is not covered by the pool's idle error
            // listener. Abort this response rather than crash the process.
            res.destroy();
        };
        const cleanup = () => {
            closed = true;
            if (!client) return Promise.resolve();
            if (!cleanupPromise) {
                cleanupPromise = (async () => {
                    let failure = connectionError;
                    try {
                        // Read-only requests never have changes to commit. Rollback
                        // also handles SQL failures caught by a route's error handler.
                        await client.query('ROLLBACK');
                    } catch (error) {
                        failure = error;
                    }
                    client.removeListener('error', onClientError);
                    client.release(failure);
                })();
            }
            return cleanupPromise;
        };
        // Register before acquiring a connection to cover disconnects while queued.
        res.once('finish', cleanup);
        res.once('close', cleanup);
        try {
            client = await pool.connect();
            client.on('error', onClientError);
            if (closed || res.destroyed) return await cleanup();
            await client.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
            if (closed || res.destroyed) return await cleanup();
            const result = await client.query('SELECT snapshot_generation FROM organizations WHERE name = $1', [orgName]);
            if (closed || res.destroyed) return await cleanup();
            context.run({ client, generation: String(result.rows[0]?.snapshot_generation ?? 'missing'),
                isClosed: () => closed }, next);
        } catch (error) {
            await cleanup();
            if (!res.destroyed) next(error);
        }
    });
}

module.exports = { installSnapshotCache };
