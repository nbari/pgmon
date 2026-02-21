pub const DATABASE_QUERY: &str = r"
SELECT 
    datname,
    numbackends,
    xact_commit, 
    xact_rollback, 
    blks_read, 
    blks_hit, 
    tup_fetched,
    stats_reset
FROM pg_stat_database
ORDER BY xact_commit DESC
";

pub const LOCKS_QUERY: &str = r"
SELECT 
    relation::regclass::text, 
    mode, 
    granted, 
    pid
FROM pg_locks
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

pub const CONN_STATS_QUERY: &str = r"
SELECT
    COALESCE(state, 'background') as state,
    COUNT(*)::bigint as count
FROM pg_stat_activity
WHERE pid <> pg_backend_pid()
GROUP BY state
ORDER BY
    CASE state
        WHEN 'active'                        THEN 1
        WHEN 'idle in transaction'           THEN 2
        WHEN 'idle in transaction (aborted)' THEN 3
        WHEN 'idle'                          THEN 4
        WHEN 'fastpath function call'        THEN 5
        WHEN 'disabled'                      THEN 6
        ELSE 7
    END
";

pub const ACTIVE_QUERIES_QUERY: &str = r"
SELECT
    pid::text,
    COALESCE(usename, '') as usename,
    COALESCE(datname, '') as datname,
    GREATEST(0, EXTRACT(EPOCH FROM (now() - query_start))::bigint) as duration_sec,
    COALESCE(regexp_replace(query, '\s+', ' ', 'g'), '') as query
FROM pg_stat_activity
WHERE pid <> pg_backend_pid()
    AND state = 'active'
ORDER BY query_start ASC NULLS LAST
";

pub const PERF_STATS_QUERY: &str = r"
SELECT
    CASE
        WHEN COALESCE(SUM(blks_hit), 0) + COALESCE(SUM(blks_read), 0) > 0
        THEN COALESCE(SUM(blks_hit), 0)::float8
             / (COALESCE(SUM(blks_hit), 0) + COALESCE(SUM(blks_read), 0)) * 100.0
        ELSE 100.0
    END as cache_hit_pct,
    COALESCE(SUM(xact_commit), 0)::bigint    as total_commits,
    COALESCE(SUM(xact_rollback), 0)::bigint  as total_rollbacks,
    COALESCE(SUM(numbackends), 0)::bigint    as total_backends,
    (SELECT setting::bigint FROM pg_settings WHERE name = 'max_connections') as max_connections
FROM pg_stat_database
";
