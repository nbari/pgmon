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
    COALESCE(relation::regclass::text, locktype) as target, 
    mode, 
    COUNT(*) FILTER (WHERE NOT granted)::bigint as waiters,
    COUNT(*) FILTER (WHERE granted)::bigint as holders
FROM pg_locks
GROUP BY 1, 2
ORDER BY waiters DESC, holders DESC, target ASC, mode ASC
LIMIT 500
";

pub const IO_QUERY: &str = r"
SELECT 
    backend_type,
    COALESCE(reads, 0) as count_read, 
    COALESCE(writes, 0) as count_write, 
    COALESCE(read_time, 0) as timing_read, 
    COALESCE(write_time, 0) as timing_write
FROM pg_stat_io
ORDER BY COALESCE(read_time, 0) + COALESCE(write_time, 0) DESC, backend_type ASC
LIMIT 500
";

pub const STATEMENTS_QUERY: &str = r"
SELECT 
    query, 
    total_exec_time as total_time, 
    mean_exec_time as mean_time, 
    calls, 
    shared_blk_read_time as blk_read_time, 
    shared_blk_write_time as blk_write_time
FROM pg_stat_statements
ORDER BY total_exec_time DESC
LIMIT 500
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
        COALESCE(backend_xmin::text, '') as xmin,
        COALESCE(datname, '') as datname,
        COALESCE(application_name, '') as application_name,
        COALESCE(usename, '') as usename,
        COALESCE(client_addr::text, client_hostname, '[local]') as client,
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
      AND backend_type = 'client backend'
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
