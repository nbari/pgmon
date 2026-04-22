pub const DATABASE_QUERY: &str = r"
SELECT 
    datname,
    numbackends,
    xact_commit, 
    xact_rollback, 
    CASE
        WHEN COALESCE(blks_hit, 0) + COALESCE(blks_read, 0) > 0
        THEN COALESCE(blks_hit, 0)::float8
             / (COALESCE(blks_hit, 0) + COALESCE(blks_read, 0)) * 100.0
        ELSE 100.0
    END as cache_hit_pct,
    COALESCE(temp_bytes, 0)::bigint as temp_bytes,
    COALESCE(deadlocks, 0)::bigint as deadlocks,
    stats_reset
FROM pg_stat_database
ORDER BY numbackends DESC, xact_commit DESC
";

pub const LOCKS_QUERY: &str = r"
SELECT
    COALESCE(blocking_locks.pid::text, '<unknown>') AS blocking_pid,
    COALESCE(blocked_locks.pid::text, '<unknown>') AS blocked_pid,
    COALESCE(blocked_activity.usename, '<unknown>') AS blocked_user,
    COALESCE(blocked_locks.relation::regclass::text, blocked_locks.locktype) AS locked_item,
    COALESCE(blocked_locks.mode, '<unknown>') AS waiting_mode,
    GREATEST(0, EXTRACT(EPOCH FROM (now() - blocked_activity.query_start)))::bigint AS wait_duration_s,
    COALESCE(regexp_replace(blocked_activity.query, '\s+', ' ', 'g'), '<unknown>') AS blocked_query
FROM pg_catalog.pg_locks blocked_locks
JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
JOIN pg_catalog.pg_locks blocking_locks 
    ON blocking_locks.locktype = blocked_locks.locktype
    AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
    AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
    AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
    AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
    AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
    AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
    AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
    AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
    AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
    AND blocking_locks.pid != blocked_locks.pid
WHERE NOT blocked_locks.granted
ORDER BY wait_duration_s DESC
LIMIT 500
";

pub const IO_QUERY: &str = r"
SELECT 
    backend_type,
    object,
    context,
    COALESCE(reads, 0) as count_read, 
    COALESCE(writes, 0) as count_write, 
    COALESCE(read_time, 0) as timing_read, 
    COALESCE(write_time, 0) as timing_write
FROM pg_stat_io
ORDER BY COALESCE(read_time, 0) + COALESCE(write_time, 0) DESC, backend_type ASC
LIMIT 500
";

pub const SETTINGS_QUERY: &str = r"
SELECT 
    name, 
    setting, 
    COALESCE(unit, '') as unit, 
    category,
    short_desc 
FROM pg_settings 
ORDER BY category, name
";

pub const AUTO_EXPLAIN_STATUS_QUERY: &str = r"
SELECT
    current_setting('auto_explain.log_min_duration', true) as log_min_duration,
    current_setting('auto_explain.log_analyze', true) as log_analyze,
    current_setting('auto_explain.log_buffers', true) as log_buffers,
    current_setting('auto_explain.log_format', true) as log_format
";

pub const REPLICATION_RECEIVER_QUERY: &str = r"
SELECT
    COALESCE(status, '') as status,
    COALESCE(sender_host, '') as sender_host,
    COALESCE(sender_port::text, '') as sender_port,
    COALESCE(slot_name, '') as slot_name,
    COALESCE(latest_end_lsn::text, '') as latest_end_lsn
FROM pg_stat_wal_receiver
LIMIT 1
";

pub const REPLICATION_SENDERS_QUERY: &str = r"
SELECT
    replication.pid::text,
    COALESCE(replication.usename, '') as usename,
    COALESCE(replication.application_name, '') as application_name,
    COALESCE(host(replication.client_addr), replication.client_hostname, '[local]') as client,
    COALESCE(replication.state, '') as state,
    COALESCE(replication.sync_state, '') as sync_state,
    COALESCE(slots.slot_name, '') as slot_name,
    GREATEST(COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), replication.sent_lsn), 0), 0)::bigint as sent_lag_bytes,
    GREATEST(COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), replication.write_lsn), 0), 0)::bigint as write_lag_bytes,
    GREATEST(COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), replication.flush_lsn), 0), 0)::bigint as flush_lag_bytes,
    GREATEST(COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), replication.replay_lsn), 0), 0)::bigint as replay_lag_bytes
FROM pg_stat_replication replication
LEFT JOIN pg_replication_slots slots
    ON slots.active_pid = replication.pid
ORDER BY replay_lag_bytes DESC, write_lag_bytes DESC, replication.pid ASC
";

pub const REPLICATION_SLOTS_QUERY: &str = r"
SELECT
    COALESCE(slot_name, '') as slot_name,
    COALESCE(slot_type, '') as slot_type,
    CASE WHEN active THEN 'yes' ELSE 'no' END as active,
    COALESCE(active_pid::text, '') as active_pid,
    COALESCE(restart_lsn::text, '') as restart_lsn,
    COALESCE(confirmed_flush_lsn::text, '') as confirmed_flush_lsn,
    COALESCE(wal_status, '') as wal_status
FROM pg_replication_slots
ORDER BY active DESC, slot_name ASC
";

pub const ACTIVITY_SUMMARY_QUERY: &str = r"
SELECT
    current_setting('server_version') as server_version,
    pg_postmaster_start_time() as postmaster_start,
    (SELECT COUNT(*)::bigint FROM pg_database WHERE datallowconn) as database_count,
    COALESCE(
        (SELECT SUM(pg_database_size(datname))::bigint FROM pg_database WHERE datallowconn),
        0
    ) as total_database_bytes,
    CASE
        WHEN COALESCE(SUM(blks_hit), 0) + COALESCE(SUM(blks_read), 0) > 0
        THEN COALESCE(SUM(blks_hit), 0)::float8
             / (COALESCE(SUM(blks_hit), 0) + COALESCE(SUM(blks_read), 0)) * 100.0
        ELSE 100.0
    END as cache_hit_pct,
    CASE
        WHEN COALESCE(SUM(xact_commit), 0) + COALESCE(SUM(xact_rollback), 0) > 0
        THEN COALESCE(SUM(xact_rollback), 0)::float8
             / (COALESCE(SUM(xact_commit), 0) + COALESCE(SUM(xact_rollback), 0)) * 100.0
        ELSE 0.0
    END as rollback_pct,
    COALESCE(SUM(xact_commit), 0)::bigint as total_commits,
    COALESCE(SUM(xact_rollback), 0)::bigint as total_rollbacks,
    (COALESCE(SUM(xact_commit), 0) + COALESCE(SUM(xact_rollback), 0))::bigint as total_xacts,
    COALESCE(SUM(tup_inserted), 0)::bigint as total_inserts,
    COALESCE(SUM(tup_updated), 0)::bigint as total_updates,
    COALESCE(SUM(tup_deleted), 0)::bigint as total_deletes,
    COALESCE(SUM(tup_returned), 0)::bigint as total_returned,
    COALESCE(SUM(temp_files), 0)::bigint as total_temp_files,
    COALESCE(SUM(temp_bytes), 0)::bigint as total_temp_bytes,
    (SELECT setting::bigint FROM pg_settings WHERE name = 'max_connections') as max_connections
FROM pg_stat_database
";

pub const ACTIVITY_PROCESS_QUERY: &str = r"
SELECT
    COALESCE(
        (SELECT COUNT(*)::bigint
         FROM pg_stat_activity
         WHERE backend_type IN ('background worker', 'logical replication worker', 'parallel worker')),
        0
    ) as worker_total,
    (SELECT setting::bigint FROM pg_settings WHERE name = 'max_worker_processes') as max_worker_processes,
    COALESCE(
        (SELECT COUNT(*)::bigint
         FROM pg_stat_activity
         WHERE backend_type = 'logical replication worker'),
        0
    ) as logical_workers,
    (SELECT setting::bigint FROM pg_settings WHERE name = 'max_logical_replication_workers') as max_logical_workers,
    COALESCE(
        (SELECT COUNT(*)::bigint
         FROM pg_stat_activity
         WHERE backend_type = 'parallel worker'),
        0
    ) as parallel_workers,
    (SELECT setting::bigint FROM pg_settings WHERE name = 'max_parallel_workers') as max_parallel_workers,
    COALESCE(
        (SELECT COUNT(*)::bigint
         FROM pg_stat_activity
         WHERE backend_type = 'autovacuum worker'),
        0
    ) as autovacuum_workers,
    (SELECT setting::bigint FROM pg_settings WHERE name = 'autovacuum_max_workers') as max_autovacuum_workers,
    COALESCE(
        (SELECT COUNT(*)::bigint
         FROM pg_stat_activity
         WHERE backend_type = 'walsender'),
        0
    ) as wal_senders,
    (SELECT setting::bigint FROM pg_settings WHERE name = 'max_wal_senders') as max_wal_senders,
    COALESCE((SELECT COUNT(*)::bigint FROM pg_stat_wal_receiver), 0) as wal_receivers,
    COALESCE((SELECT COUNT(*)::bigint FROM pg_replication_slots), 0) as replication_slots,
    (SELECT setting::bigint FROM pg_settings WHERE name = 'max_replication_slots') as max_replication_slots
";

pub const ACTIVITY_SESSIONS_QUERY: &str = r"
WITH activity AS (
    SELECT
        pid,
        COALESCE(backend_type, '') as backend_type,
        COALESCE(backend_xmin::text, '') as xmin,
        COALESCE(datname, '') as datname,
        COALESCE(application_name, '') as application_name,
        COALESCE(usename, '') as usename,
        COALESCE(host(client_addr), client_hostname, '[local]') as client,
        GREATEST(
            0,
            EXTRACT(
                EPOCH FROM (
                    now() - COALESCE(query_start, xact_start, state_change, backend_start)
                )
            )::bigint
        ) as duration_sec,
        COALESCE(wait_event, '') as wait_event,
        COALESCE(wait_event_type, '') as wait_event_type,
        COALESCE(state, '') as state,
        COALESCE(regexp_replace(query, '\s+', ' ', 'g'), '') as query,
        COALESCE(cardinality(pg_blocking_pids(pid)), 0)::bigint as blocked_by_count,
        pg_blocking_pids(pid) as blocking_pids
    FROM pg_stat_activity
    WHERE pid <> pg_backend_pid()
      AND backend_type IN ('client backend', 'walsender')
),
blockers AS (
    SELECT
        blocking_pid::integer as pid,
        COUNT(*)::bigint as blocked_count
    FROM activity, unnest(blocking_pids) as blocking_pid
    GROUP BY blocking_pid
)
SELECT
    activity.pid::text,
    activity.backend_type,
    activity.xmin,
    activity.datname,
    activity.application_name,
    activity.usename,
    activity.client,
    activity.duration_sec,
    CASE
        WHEN activity.wait_event <> '' THEN activity.wait_event
        WHEN activity.wait_event_type <> '' THEN activity.wait_event_type
        ELSE ''
    END as wait_info,
    activity.state,
    activity.query,
    activity.blocked_by_count,
    COALESCE(blockers.blocked_count, 0)::bigint as blocked_count
FROM activity
LEFT JOIN blockers ON activity.pid = blockers.pid
ORDER BY activity.duration_sec DESC, activity.pid ASC
";

pub const ACTIVITY_DETAIL_QUERY: &str = r"
SELECT
  pid::text,
  COALESCE(usename, '') as usename,
  COALESCE(application_name, '') as application_name,
  COALESCE(host(client_addr), '[local]') as client_addr,
  COALESCE(client_port::text, '') as client_port,
  COALESCE(backend_start::text, '') as backend_start,
  COALESCE(state, '') as state,
  COALESCE(wait_event_type, '') as wait_event_type,
  COALESCE(wait_event, '') as wait_event,
  COALESCE(xact_start::text, '') as xact_start,
  COALESCE(state_change::text, '') as state_change,
  COALESCE(query, '') as query,
  COALESCE(pg_blocking_pids(pid)::text, '{}') as blocking_pids
FROM pg_stat_activity
WHERE pid = $1
";

pub const ACTIVITY_BLOCKING_QUERY: &str = r"
SELECT
  blocked.pid::text AS blocked_pid,
  COALESCE(blocked.usename, '') AS blocked_user,
  COALESCE(blocked.state, '') AS blocked_state,
  GREATEST(0, EXTRACT(EPOCH FROM (now() - COALESCE(blocked.query_start, blocked.xact_start)))::bigint)::text AS blocked_duration,
  COALESCE(regexp_replace(blocked.query, '\s+', ' ', 'g'), '') AS blocked_query
FROM pg_stat_activity blocked
JOIN pg_locks bl ON blocked.pid = bl.pid
JOIN pg_locks kl
  ON bl.locktype = kl.locktype
 AND bl.database IS NOT DISTINCT FROM kl.database
 AND bl.relation IS NOT DISTINCT FROM kl.relation
 AND bl.page IS NOT DISTINCT FROM kl.page
 AND bl.tuple IS NOT DISTINCT FROM kl.tuple
 AND bl.virtualxid IS NOT DISTINCT FROM kl.virtualxid
 AND bl.transactionid IS NOT DISTINCT FROM kl.transactionid
 AND bl.classid IS NOT DISTINCT FROM kl.classid
 AND bl.objid IS NOT DISTINCT FROM kl.objid
 AND bl.objsubid IS NOT DISTINCT FROM kl.objsubid
 AND bl.pid <> kl.pid
JOIN pg_stat_activity blocker ON blocker.pid = kl.pid
WHERE NOT bl.granted
  AND blocker.pid = $1
";

pub const ACTIVITY_LOCKS_QUERY: &str = r"
SELECT
  l.locktype,
  l.mode,
  CASE WHEN l.granted THEN 'yes' ELSE 'no' END as granted,
  COALESCE(c.relname, l.relation::text, '') as relation
FROM pg_locks l
LEFT JOIN pg_class c ON l.relation = c.oid
WHERE l.pid = $1
ORDER BY l.granted DESC, l.locktype, l.mode
LIMIT 100
";

pub const DATABASE_TREE_QUERY: &str = r"
WITH user_tables AS (
    SELECT
        n.nspname AS schema_name,
        c.relname AS table_name,
        CASE c.relkind
            WHEN 'r' THEN 'table'
            WHEN 'p' THEN 'partitioned table'
            WHEN 'm' THEN 'materialized view'
            ELSE c.relkind::text
        END AS object_type,
        GREATEST(COALESCE(c.reltuples, 0)::bigint, 0) AS est_rows,
        pg_total_relation_size(c.oid) AS total_bytes
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r', 'p', 'm')
      AND n.nspname NOT IN ('pg_catalog', 'information_schema')
      AND n.nspname NOT LIKE 'pg_toast%'
)
SELECT
    schema_name,
    NULL::text AS table_name,
    'schema'::text AS object_type,
    COUNT(*)::bigint AS child_count,
    pg_size_pretty(COALESCE(SUM(total_bytes), 0)::bigint) AS total_size,
    0::int AS depth,
    COALESCE(SUM(total_bytes), 0)::bigint AS sort_bytes
FROM user_tables
GROUP BY schema_name
UNION ALL
SELECT
    schema_name,
    table_name,
    object_type,
    est_rows AS child_count,
    pg_size_pretty(total_bytes) AS total_size,
    1::int AS depth,
    total_bytes AS sort_bytes
FROM user_tables
ORDER BY schema_name ASC, depth ASC, sort_bytes DESC, table_name ASC NULLS LAST
";

#[cfg(test)]
mod tests {
    use super::ACTIVITY_SESSIONS_QUERY;

    #[test]
    fn test_activity_sessions_query_includes_walsender_backends() {
        assert!(
            ACTIVITY_SESSIONS_QUERY.contains("backend_type IN ('client backend', 'walsender')")
        );
    }
}
