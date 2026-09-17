// These are additive snapshot fields. The public unique-contributor metric is
// computed separately and must not be compared to a sum of repository counts.
const METRICS = Object.freeze([
    'new_prs', 'closed_merged_prs', 'new_issues', 'closed_issues',
    'active_contributors', 'new_commits', 'lines_added', 'lines_deleted',
]);

async function rebuildSnapshotHierarchy(client, orgId, snapshotDate) {
    await client.query(
        `INSERT INTO sig_snapshots (sig_id, snapshot_date, ${METRICS.join(', ')})
         SELECT sig.id, $2::date,
                ${METRICS.map(k => `COALESCE(SUM(rs.${k}), 0)::integer`).join(', ')}
         FROM special_interest_groups sig
         LEFT JOIN repositories r ON r.sig_id = sig.id AND r.org_id = sig.org_id
         LEFT JOIN repo_snapshots rs ON rs.repo_id = r.id AND rs.snapshot_date = $2
         WHERE sig.org_id = $1
         GROUP BY sig.id
         ON CONFLICT (sig_id, snapshot_date) DO UPDATE SET
         ${METRICS.map(k => `${k} = EXCLUDED.${k}`).join(', ')}, created_at = NOW()`,
        [orgId, snapshotDate]
    );
    await client.query(
        `INSERT INTO activity_snapshots (org_id, snapshot_date, ${METRICS.join(', ')}, new_repos)
         SELECT $1, $2::date,
                ${METRICS.map(k => `COALESCE(SUM(ss.${k}), 0)::integer`).join(', ')}, 0
         FROM special_interest_groups sig
         LEFT JOIN sig_snapshots ss ON ss.sig_id = sig.id AND ss.snapshot_date = $2
         WHERE sig.org_id = $1
         ON CONFLICT (org_id, snapshot_date) DO UPDATE SET
         ${METRICS.map(k => `${k} = EXCLUDED.${k}`).join(', ')}, created_at = NOW()`,
        [orgId, snapshotDate]
    );
}

async function checkSnapshotConsistency(client, orgId, snapshotDate = null) {
    const result = await client.query(
        `WITH dates AS (
             SELECT rs.snapshot_date FROM repo_snapshots rs
             JOIN repositories r ON r.id = rs.repo_id
             WHERE r.org_id = $1 AND r.sig_id IS NOT NULL
             UNION SELECT ss.snapshot_date FROM sig_snapshots ss
             JOIN special_interest_groups sig ON sig.id = ss.sig_id WHERE sig.org_id = $1
             UNION SELECT snapshot_date FROM activity_snapshots WHERE org_id = $1
         ), expected_sig AS (
             SELECT sig.id AS entity_id, d.snapshot_date,
                    ${METRICS.map(k => `COALESCE(SUM(rs.${k}), 0)::bigint AS ${k}`).join(', ')}
             FROM dates d CROSS JOIN special_interest_groups sig
             LEFT JOIN repositories r ON r.sig_id = sig.id AND r.org_id = sig.org_id
             LEFT JOIN repo_snapshots rs ON rs.repo_id = r.id AND rs.snapshot_date = d.snapshot_date
             WHERE sig.org_id = $1 AND ($2::date IS NULL OR d.snapshot_date = $2)
             GROUP BY sig.id, d.snapshot_date
         ), expected_org AS (
             SELECT $1::integer AS entity_id, d.snapshot_date,
                    ${METRICS.map(k => `COALESCE(SUM(ss.${k}), 0)::bigint AS ${k}`).join(', ')}
             FROM dates d LEFT JOIN special_interest_groups sig ON sig.org_id = $1
             LEFT JOIN sig_snapshots ss ON ss.sig_id = sig.id AND ss.snapshot_date = d.snapshot_date
             WHERE $2::date IS NULL OR d.snapshot_date = $2
             GROUP BY d.snapshot_date
         )
         SELECT 'sig' AS level, e.entity_id, e.snapshot_date, to_jsonb(e) AS expected,
                to_jsonb(s) AS actual
         FROM expected_sig e LEFT JOIN sig_snapshots s
              ON s.sig_id = e.entity_id AND s.snapshot_date = e.snapshot_date
         WHERE ROW(${METRICS.map(k => `e.${k}`).join(', ')}) IS DISTINCT FROM
               ROW(${METRICS.map(k => `s.${k}`).join(', ')})
         UNION ALL
         SELECT 'organization', e.entity_id, e.snapshot_date, to_jsonb(e), to_jsonb(a)
         FROM expected_org e LEFT JOIN activity_snapshots a
              ON a.org_id = e.entity_id AND a.snapshot_date = e.snapshot_date
         WHERE ROW(${METRICS.map(k => `e.${k}`).join(', ')}) IS DISTINCT FROM
               ROW(${METRICS.map(k => `a.${k}`).join(', ')})
         ORDER BY snapshot_date, level, entity_id`,
        [orgId, snapshotDate]
    );
    return result.rows;
}

async function assertSnapshotConsistency(client, orgId, snapshotDate) {
    const differences = await checkSnapshotConsistency(client, orgId, snapshotDate);
    if (differences.length) {
        throw new Error(`Snapshot hierarchy mismatch: ${JSON.stringify(differences)}`);
    }
}

module.exports = { METRICS, rebuildSnapshotHierarchy, checkSnapshotConsistency, assertSnapshotConsistency };
