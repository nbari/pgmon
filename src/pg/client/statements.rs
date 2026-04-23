//! Statement and capability fetchers for `PgClient`.

use super::{CapabilityStatus, DbResult, PgClient, PgClientConnection};
use crate::pg::queries::IO_QUERY;
use sqlx::Row;

impl PgClient {
    pub(crate) async fn fetch_io_stats(
        &self,
        connection: &mut PgClientConnection,
    ) -> DbResult<Vec<Vec<String>>> {
        if let Err(super::DbError::CapabilityMissing(_)) =
            self.ensure_io_capability(connection).await
        {
            return Ok(Vec::new());
        }

        let rows = sqlx::query(IO_QUERY)
            .fetch_all(connection.as_mut())
            .await
            .map_err(super::connect::classify_query_error)?;
        rows.into_iter()
            .map(|row| {
                Ok(vec![
                    row.try_get::<Option<String>, _>(0)?.unwrap_or_default(),
                    row.try_get::<Option<String>, _>(1)?.unwrap_or_default(),
                    row.try_get::<Option<String>, _>(2)?.unwrap_or_default(),
                    row.try_get::<i64, _>(3)?.to_string(),
                    row.try_get::<i64, _>(4)?.to_string(),
                    row.try_get::<f64, _>(5)?.to_string(),
                    row.try_get::<f64, _>(6)?.to_string(),
                ])
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(super::connect::classify_query_error)
    }

    pub(crate) async fn fetch_statements(
        &self,
        connection: &mut PgClientConnection,
    ) -> DbResult<Vec<Vec<String>>> {
        let query = match self.ensure_statements_query(connection).await {
            Ok(Some(query)) => query,
            Ok(None) | Err(super::DbError::CapabilityMissing(_)) => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };

        let rows = match sqlx::query(&query).fetch_all(connection.as_mut()).await {
            Ok(rows) => rows,
            Err(error) => {
                if error.to_string().contains("shared_preload_libraries") {
                    let mut state = self.state();
                    state.capability_cache.statements = Some(CapabilityStatus::unavailable(
                        "pg_stat_statements extension exists, but shared_preload_libraries does not load it.",
                    ));
                    state.capability_cache.statements_query = None;
                    return Ok(Vec::new());
                }
                return Err(super::connect::classify_query_error(error));
            }
        };

        rows.into_iter()
            .map(|row| {
                Ok(vec![
                    row.try_get::<String, _>(6)?,
                    row.try_get::<String, _>(0)?,
                    row.try_get::<f64, _>(1)?.to_string(),
                    row.try_get::<f64, _>(2)?.to_string(),
                    row.try_get::<i64, _>(3)?.to_string(),
                    row.try_get::<f64, _>(4)?.to_string(),
                    row.try_get::<f64, _>(5)?.to_string(),
                ])
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(super::connect::classify_query_error)
    }

    async fn extension_exists(
        &self,
        connection: &mut PgClientConnection,
        name: &str,
    ) -> DbResult<bool> {
        let row = sqlx::query("SELECT 1 FROM pg_extension WHERE extname = $1")
            .bind(name)
            .fetch_optional(connection.as_mut())
            .await
            .map_err(super::connect::classify_query_error)?;
        Ok(row.is_some())
    }

    async fn view_exists(&self, connection: &mut PgClientConnection, name: &str) -> DbResult<bool> {
        let row = sqlx::query("SELECT 1 FROM pg_views WHERE viewname = $1")
            .bind(name)
            .fetch_optional(connection.as_mut())
            .await
            .map_err(super::connect::classify_query_error)?;
        Ok(row.is_some())
    }

    async fn column_exists(
        &self,
        connection: &mut PgClientConnection,
        table_name: &str,
        column_name: &str,
    ) -> DbResult<bool> {
        let row = sqlx::query(
            "SELECT 1 FROM information_schema.columns WHERE table_name = $1 AND column_name = $2",
        )
        .bind(table_name)
        .bind(column_name)
        .fetch_optional(connection.as_mut())
        .await
        .map_err(super::connect::classify_query_error)?;
        Ok(row.is_some())
    }

    async fn ensure_io_capability(
        &self,
        connection: &mut PgClientConnection,
    ) -> DbResult<CapabilityStatus> {
        if let Some(status) = self.state().capability_cache.io.clone() {
            return match status {
                CapabilityStatus::Unavailable(_) => Err(super::DbError::CapabilityMissing(
                    "pg_stat_io is not available on this server (PostgreSQL 16+ required).",
                )),
                other => Ok(other),
            };
        }

        let status = if self.view_exists(connection, "pg_stat_io").await? {
            CapabilityStatus::Available
        } else {
            CapabilityStatus::unavailable(
                "pg_stat_io is not available on this server (PostgreSQL 16+ required).",
            )
        };
        self.state().capability_cache.io = Some(status.clone());
        match status {
            CapabilityStatus::Unavailable(_) => Err(super::DbError::CapabilityMissing(
                "pg_stat_io is not available on this server (PostgreSQL 16+ required).",
            )),
            other => Ok(other),
        }
    }

    async fn ensure_statements_query(
        &self,
        connection: &mut PgClientConnection,
    ) -> DbResult<Option<String>> {
        {
            let state = self.state();
            if let Some(status) = state.capability_cache.statements.as_ref()
                && matches!(status, CapabilityStatus::Unavailable(_))
            {
                return Err(super::DbError::CapabilityMissing(
                    "pg_stat_statements is not installed in the current database.",
                ));
            }
            if let Some(query) = state.capability_cache.statements_query.as_ref() {
                return Ok(Some(query.clone()));
            }
        }

        if !self
            .extension_exists(connection, "pg_stat_statements")
            .await?
        {
            self.state().capability_cache.statements = Some(CapabilityStatus::unavailable(
                "pg_stat_statements is not installed in the current database.",
            ));
            return Err(super::DbError::CapabilityMissing(
                "pg_stat_statements is not installed in the current database.",
            ));
        }

        let total_time_column = if self
            .column_exists(connection, "pg_stat_statements", "total_exec_time")
            .await?
        {
            "total_exec_time"
        } else {
            "total_time"
        };
        let mean_time_column = if self
            .column_exists(connection, "pg_stat_statements", "mean_exec_time")
            .await?
        {
            "mean_exec_time"
        } else {
            "mean_time"
        };
        let blk_read_time_expr = if self
            .column_exists(connection, "pg_stat_statements", "shared_blk_read_time")
            .await?
        {
            "COALESCE(s.shared_blk_read_time, 0)::float8"
        } else {
            "0::float8"
        };
        let blk_write_time_expr = if self
            .column_exists(connection, "pg_stat_statements", "shared_blk_write_time")
            .await?
        {
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
        let mut state = self.state();
        state.capability_cache.statements_query = Some(query.clone());
        state.capability_cache.statements = Some(CapabilityStatus::Available);
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
