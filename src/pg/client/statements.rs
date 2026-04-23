//! Statement and capability fetchers for `PgClient`.

use super::{CapabilityStatus, PgClient};
use crate::pg::queries::IO_QUERY;
use anyhow::Result;

impl PgClient {
    pub fn fetch_io_stats(&mut self) -> Result<Vec<Vec<String>>> {
        if let CapabilityStatus::Unavailable(_) = self.ensure_io_capability()? {
            return Ok(Vec::new());
        }
        let rows = self.client.query(IO_QUERY, &[])?;
        rows.into_iter()
            .map(|row| {
                Ok(vec![
                    row.try_get::<_, Option<String>>(0)?.unwrap_or_default(),
                    row.try_get::<_, Option<String>>(1)?.unwrap_or_default(),
                    row.try_get::<_, Option<String>>(2)?.unwrap_or_default(),
                    row.try_get::<_, i64>(3)?.to_string(),
                    row.try_get::<_, i64>(4)?.to_string(),
                    row.try_get::<_, f64>(5)?.to_string(),
                    row.try_get::<_, f64>(6)?.to_string(),
                ])
            })
            .collect()
    }

    pub fn fetch_statements(&mut self) -> Result<Vec<Vec<String>>> {
        let Some(query) = self.ensure_statements_query()? else {
            return Ok(Vec::new());
        };

        let rows = match self.client.query(query.as_str(), &[]) {
            Ok(r) => r,
            Err(e) => {
                if e.to_string().contains("shared_preload_libraries") {
                    self.capability_cache.statements = Some(CapabilityStatus::unavailable(
                        "pg_stat_statements extension exists, but shared_preload_libraries does not load it.",
                    ));
                    self.capability_cache.statements_query = None;
                    return Ok(Vec::new());
                }
                return Err(e.into());
            }
        };
        rows.into_iter()
            .map(|row| {
                Ok(vec![
                    row.try_get::<_, String>(6)?,
                    row.try_get::<_, String>(0)?,
                    row.try_get::<_, f64>(1)?.to_string(),
                    row.try_get::<_, f64>(2)?.to_string(),
                    row.try_get::<_, i64>(3)?.to_string(),
                    row.try_get::<_, f64>(4)?.to_string(),
                    row.try_get::<_, f64>(5)?.to_string(),
                ])
            })
            .collect()
    }

    pub(crate) fn io_capability(&self) -> CapabilityStatus {
        self.capability_cache.io.clone().unwrap_or_default()
    }

    pub(crate) fn statements_capability(&self) -> CapabilityStatus {
        self.capability_cache.statements.clone().unwrap_or_default()
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

    fn column_exists(&mut self, table_name: &str, column_name: &str) -> Result<bool> {
        let row = self.client.query_opt(
            "SELECT 1 FROM information_schema.columns WHERE table_name = $1 AND column_name = $2",
            &[&table_name, &column_name],
        )?;
        Ok(row.is_some())
    }

    fn ensure_io_capability(&mut self) -> Result<CapabilityStatus> {
        if let Some(status) = self.capability_cache.io.as_ref() {
            return Ok(status.clone());
        }

        let status = if self.view_exists("pg_stat_io")? {
            CapabilityStatus::Available
        } else {
            CapabilityStatus::unavailable(
                "pg_stat_io is not available on this server (PostgreSQL 16+ required).",
            )
        };
        self.capability_cache.io = Some(status.clone());
        Ok(status)
    }

    fn ensure_statements_query(&mut self) -> Result<Option<String>> {
        if let Some(status) = self.capability_cache.statements.as_ref()
            && matches!(status, CapabilityStatus::Unavailable(_))
        {
            return Ok(None);
        }

        if let Some(query) = self.capability_cache.statements_query.as_ref() {
            self.capability_cache.statements = Some(CapabilityStatus::Available);
            return Ok(Some(query.clone()));
        }

        if !self.extension_exists("pg_stat_statements")? {
            self.capability_cache.statements = Some(CapabilityStatus::unavailable(
                "pg_stat_statements is not installed in the current database.",
            ));
            return Ok(None);
        }

        let total_time_column = if self.column_exists("pg_stat_statements", "total_exec_time")? {
            "total_exec_time"
        } else {
            "total_time"
        };
        let mean_time_column = if self.column_exists("pg_stat_statements", "mean_exec_time")? {
            "mean_exec_time"
        } else {
            "mean_time"
        };
        let blk_read_time_expr =
            if self.column_exists("pg_stat_statements", "shared_blk_read_time")? {
                "COALESCE(s.shared_blk_read_time, 0)::float8"
            } else {
                "0::float8"
            };
        let blk_write_time_expr =
            if self.column_exists("pg_stat_statements", "shared_blk_write_time")? {
                "COALESCE(s.shared_blk_write_time, 0)::float8"
            } else {
                "0::float8"
            };

        let query = build_statements_query(
            total_time_column,
            mean_time_column,
            blk_read_time_expr,
            blk_write_time_expr,
        );
        self.capability_cache.statements_query = Some(query.clone());
        self.capability_cache.statements = Some(CapabilityStatus::Available);
        Ok(Some(query))
    }
}

fn build_statements_query(
    total_time_column: &str,
    mean_time_column: &str,
    blk_read_time_expr: &str,
    blk_write_time_expr: &str,
) -> String {
    format!(
        r"
SELECT
    COALESCE(regexp_replace(s.query, '\s+', ' ', 'g'), '') as query,
    COALESCE(s.{total_time_column}, 0)::float8 as total_time,
    COALESCE(s.{mean_time_column}, 0)::float8 as mean_time,
    COALESCE(s.calls, 0)::bigint as calls,
    {blk_read_time_expr} as blk_read_time,
    {blk_write_time_expr} as blk_write_time,
    COALESCE(d.datname, '') as datname
FROM pg_stat_statements s
LEFT JOIN pg_database d ON d.oid = s.dbid
ORDER BY s.{total_time_column} DESC
LIMIT 500
"
    )
}

#[cfg(test)]
mod tests {
    use super::build_statements_query;

    #[test]
    fn test_build_statements_query_uses_exec_time_columns() {
        let query = build_statements_query(
            "total_exec_time",
            "mean_exec_time",
            "COALESCE(s.shared_blk_read_time, 0)::float8",
            "COALESCE(s.shared_blk_write_time, 0)::float8",
        );

        assert!(query.contains("COALESCE(s.total_exec_time, 0)::float8 as total_time"));
        assert!(query.contains("COALESCE(s.mean_exec_time, 0)::float8 as mean_time"));
        assert!(query.contains("ORDER BY s.total_exec_time DESC"));
    }

    #[test]
    fn test_build_statements_query_falls_back_when_block_timing_columns_are_missing() {
        let query = build_statements_query(
            "total_exec_time",
            "mean_exec_time",
            "0::float8",
            "0::float8",
        );

        assert!(query.contains("0::float8 as blk_read_time"));
        assert!(query.contains("0::float8 as blk_write_time"));
        assert!(!query.contains("s.0::float8"));
    }
}
