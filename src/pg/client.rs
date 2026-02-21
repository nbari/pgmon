use crate::pg::queries::{
    ACTIVE_QUERIES_QUERY, CONN_STATS_QUERY, DATABASE_QUERY, IO_QUERY, LOCKS_QUERY,
    PERF_STATS_QUERY, STATEMENTS_QUERY,
};
use anyhow::{Context, Result};
use postgres::{Client, NoTls};

pub struct PgClient {
    client: Client,
}

impl PgClient {
    pub fn new(dsn: &str) -> Result<Self> {
        let client = Client::connect(dsn, NoTls)
            .with_context(|| format!("Failed to connect to Postgres with DSN: {dsn}"))?;
        Ok(Self { client })
    }

    pub fn fetch_database_stats(&mut self) -> Result<Vec<Vec<String>>> {
        let rows = self.client.query(DATABASE_QUERY, &[])?;
        Ok(rows
            .into_iter()
            .map(|row| {
                vec![
                    row.get::<_, Option<String>>(0).unwrap_or_default(),
                    row.get::<_, i32>(1).to_string(),
                    row.get::<_, i64>(2).to_string(),
                    row.get::<_, i64>(3).to_string(),
                    row.get::<_, i64>(4).to_string(),
                    row.get::<_, i64>(5).to_string(),
                    row.get::<_, i64>(6).to_string(),
                    row.get::<_, Option<chrono::DateTime<chrono::Utc>>>(7)
                        .map(|t| t.to_rfc3339())
                        .unwrap_or_default(),
                ]
            })
            .collect())
    }

    pub fn fetch_locks(&mut self) -> Result<Vec<Vec<String>>> {
        let rows = self.client.query(LOCKS_QUERY, &[])?;
        Ok(rows
            .into_iter()
            .map(|row| {
                vec![
                    row.get::<_, Option<String>>(0).unwrap_or_default(),
                    row.get::<_, Option<String>>(1).unwrap_or_default(),
                    row.get::<_, bool>(2).to_string(),
                    row.get::<_, Option<i32>>(3)
                        .map(|v| v.to_string())
                        .unwrap_or_default(),
                ]
            })
            .collect())
    }

    pub fn fetch_io_stats(&mut self) -> Result<Vec<Vec<String>>> {
        if !self.view_exists("pg_stat_io")? {
            return Ok(vec![vec![
                "pg_stat_io not available (PG 16+ required)".to_string(),
            ]]);
        }
        let rows = self.client.query(IO_QUERY, &[])?;
        Ok(rows
            .into_iter()
            .map(|row| {
                vec![
                    row.get::<_, Option<String>>(0).unwrap_or_default(),
                    row.get::<_, i64>(1).to_string(),
                    row.get::<_, i64>(2).to_string(),
                    row.get::<_, f64>(3).to_string(),
                    row.get::<_, f64>(4).to_string(),
                ]
            })
            .collect())
    }

    pub fn fetch_statements(&mut self) -> Result<Vec<Vec<String>>> {
        if !self.extension_exists("pg_stat_statements")? {
            return Ok(vec![vec!["pg_stat_statements not installed".to_string()]]);
        }
        let rows = match self.client.query(STATEMENTS_QUERY, &[]) {
            Ok(r) => r,
            Err(e) => {
                if e.to_string().contains("shared_preload_libraries") {
                    return Ok(vec![vec![
                        "pg_stat_statements library not loaded".to_string(),
                    ]]);
                }
                return Err(e.into());
            }
        };
        Ok(rows
            .into_iter()
            .map(|row| {
                vec![
                    row.get::<_, String>(0),
                    row.get::<_, f64>(1).to_string(),
                    row.get::<_, f64>(2).to_string(),
                    row.get::<_, i64>(3).to_string(),
                    row.get::<_, f64>(4).to_string(),
                    row.get::<_, f64>(5).to_string(),
                ]
            })
            .collect())
    }

    pub fn fetch_conn_stats(&mut self) -> Result<Vec<(String, i64)>> {
        let rows = self.client.query(CONN_STATS_QUERY, &[])?;
        Ok(rows
            .into_iter()
            .map(|row| (row.get::<_, String>(0), row.get::<_, i64>(1)))
            .collect())
    }

    pub fn fetch_active_queries(&mut self) -> Result<Vec<Vec<String>>> {
        let rows = self.client.query(ACTIVE_QUERIES_QUERY, &[])?;
        Ok(rows
            .into_iter()
            .map(|row| {
                let duration_secs = row.get::<_, Option<i64>>(3).unwrap_or(0).max(0);
                let h = duration_secs / 3600;
                let m = (duration_secs % 3600) / 60;
                let s = duration_secs % 60;
                vec![
                    row.get::<_, String>(0),
                    row.get::<_, String>(1),
                    row.get::<_, String>(2),
                    format!("{h:02}:{m:02}:{s:02}"),
                    row.get::<_, String>(4),
                ]
            })
            .collect())
    }

    pub fn fetch_perf_stats(&mut self) -> Result<(f64, i64, i64, i64, i64)> {
        let row = self.client.query_one(PERF_STATS_QUERY, &[])?;
        Ok((
            row.get::<_, f64>(0),
            row.get::<_, i64>(1),
            row.get::<_, i64>(2),
            row.get::<_, i64>(3),
            row.get::<_, i64>(4),
        ))
    }

    fn extension_exists(&mut self, name: &str) -> Result<bool> {
        let row = self
            .client
            .query_opt("SELECT 1 FROM pg_extension WHERE extname = $1", &[&name])?;
        Ok(row.is_some())
    }

    fn view_exists(&mut self, name: &str) -> Result<bool> {
        let row = self
            .client
            .query_opt("SELECT 1 FROM pg_views WHERE viewname = $1", &[&name])?;
        Ok(row.is_some())
    }
}
