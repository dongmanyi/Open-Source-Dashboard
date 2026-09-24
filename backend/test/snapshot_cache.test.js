const assert = require('node:assert/strict');
const test = require('node:test');
const { EventEmitter } = require('node:events');
const { installSnapshotCache } = require('../snapshot_cache');

function fixture({ failQuery, connect } = {}) {
    const queries = [];
    const releases = [];
    const client = Object.assign(new EventEmitter(), { query: async sql => {
        queries.push(sql);
        if (sql === failQuery) throw new Error('injected query failure');
        return { rows: [{ snapshot_generation: '7' }] };
    }, release: error => releases.push(error) });
    const pool = { query: async () => 'outside request', connect: connect || (async () => client) };
    const redis = { get: async () => null, setEx: async () => {} };
    let middleware;
    installSnapshotCache({ use: (_, handler) => { middleware = handler; } }, redis, pool, 'org');
    return { middleware, pool, queries, releases, client, response: new EventEmitter() };
}
const tick = () => new Promise(resolve => setImmediate(resolve));

test('API queries use one transaction; finish and close release it only once', async () => {
    const f = fixture();
    let lateQuery;
    await new Promise((resolve, reject) => f.middleware({}, f.response, async error => {
        if (error) return reject(error);
        try {
            await f.pool.query('SELECT route_summary');
            await f.pool.query('SELECT route_repositories');
            lateQuery = () => f.pool.query('SELECT after_response');
            f.response.emit('finish');
            f.response.emit('close');
            await assert.rejects(lateQuery(), /transaction is closed/);
            resolve();
        } catch (err) { reject(err); }
    }));
    await tick();
    assert.equal(f.queries[0], 'BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
    assert.deepEqual(f.queries.slice(2), ['SELECT route_summary', 'SELECT route_repositories', 'ROLLBACK']);
    assert.deepEqual(f.releases, [undefined]);
    assert.equal(await f.pool.query('SELECT outside'), 'outside request');
});

test('client disconnect while waiting for the pool releases the eventual connection', async () => {
    let connect;
    const pending = new Promise(resolve => { connect = resolve; });
    const f = fixture({ connect: () => pending });
    let routed = false;
    const running = f.middleware({}, f.response, () => { routed = true; });
    f.response.destroyed = true;
    f.response.emit('close');
    connect(f.client);
    await running;
    assert.equal(routed, false);
    assert.deepEqual(f.queries, ['ROLLBACK']);
    assert.equal(f.releases.length, 1);
});

test('generation read failure rolls back and forwards the error', async () => {
    const f = fixture({ failQuery: 'SELECT snapshot_generation FROM organizations WHERE name = $1' });
    let failure;
    await f.middleware({}, f.response, error => { failure = error; });
    f.response.emit('close');
    await tick();
    assert.match(failure.message, /injected query failure/);
    assert.equal(f.queries.at(-1), 'ROLLBACK');
    assert.equal(f.releases.length, 1);
});

test('rollback failure discards the connection instead of returning it as healthy', async () => {
    const f = fixture({ failQuery: 'ROLLBACK' });
    await f.middleware({}, f.response, () => f.response.emit('finish'));
    await tick();
    assert.equal(f.releases.length, 1);
    assert.match(f.releases[0].message, /injected query failure/);
});

test('checked-out client connection errors abort the response and discard the client', async () => {
    const f = fixture();
    f.response.destroy = () => { f.response.destroyed = true; f.response.emit('close'); };
    await f.middleware({}, f.response, () => {});
    const failure = new Error('connection lost');
    f.client.emit('error', failure);
    await tick();
    assert.equal(f.response.destroyed, true);
    assert.deepEqual(f.releases, [failure]);
    assert.equal(f.client.listenerCount('error'), 0);
});
