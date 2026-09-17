const { AsyncLocalStorage } = require('node:async_hooks');

// Bind cache reads and writes to the generation at request start. An old in-flight
// request cannot repopulate the new generation after the publisher invalidates keys.
function installSnapshotCache(app, redis, pool, orgName) {
    const context = new AsyncLocalStorage();
    for (const method of ['get', 'setEx']) {
        const original = redis[method].bind(redis);
        redis[method] = (key, ...args) => {
            const generation = context.getStore();
            return original(generation === undefined ? key : `${key}:snapshot-generation:${generation}`, ...args);
        };
    }
    app.use('/api', async (req, res, next) => {
        try {
            const result = await pool.query('SELECT snapshot_generation FROM organizations WHERE name = $1', [orgName]);
            context.run(String(result.rows[0]?.snapshot_generation ?? 'missing'), next);
        } catch (error) {
            next(error);
        }
    });
}

module.exports = { installSnapshotCache };
