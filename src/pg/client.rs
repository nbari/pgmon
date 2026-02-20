use crate::pg::queries::{ACTIVITY_QUERY, DATABASE_QUERY, IO_QUERY, LOCKS_QUERY, STATEMENTS_QUERY};
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

    pub fn fetch_activity(&mut self) -> Result<Vec<Vec<String>>> {
        let rows = self.client.query(ACTIVITY_QUERY, &[])?;
        Ok(rows
            .into_iter()
            .map(|row| {
                vec![
                    row.get::<_, i32>(0).to_string(),
                    row.get::<_, Option<String>>(1).unwrap_or_default(),
                    row.get::<_, Option<String>>(2).unwrap_or_default(),
                    row.get::<_, Option<String>>(3).unwrap_or_default(),
                    row.get::<_, Option<String>>(4).unwrap_or_default(),
                    row.get::<_, Option<chrono::DateTime<chrono::Utc>>>(5)
                        .map(|t| t.to_rfc3339())
                        .unwrap_or_default(),
                    row.get::<_, Option<String>>(6).unwrap_or_default(),
                    row.get::<_, Option<String>>(7).unwrap_or_default(),
                ]
            })
            .collect())
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
