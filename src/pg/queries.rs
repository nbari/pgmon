pub const ACTIVITY_QUERY: &str = r#"
SELECT 
    pid, 
    usename, 
    datname, 
    state, 
    query, 
    query_start, 
    application_name, 
    client_addr::text
FROM pg_stat_activity
WHERE pid <> pg_backend_pid()
ORDER BY COALESCE(now() - query_start, '0s'::interval) DESC
LIMIT 500
"#;

pub const DATABASE_QUERY: &str = r#"
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
"#;

pub const LOCKS_QUERY: &str = r#"
SELECT 
    relation::regclass::text, 
    mode, 
    granted, 
    pid
FROM pg_locks
LIMIT 500
"#;

pub const IO_QUERY: &str = r#"
SELECT 
    backend_type,
    COALESCE(reads, 0) as count_read, 
    COALESCE(writes, 0) as count_write, 
    COALESCE(read_time, 0) as timing_read, 
    COALESCE(write_time, 0) as timing_write
FROM pg_stat_io
LIMIT 500
"#;

pub const STATEMENTS_QUERY: &str = r#"
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
"#;
