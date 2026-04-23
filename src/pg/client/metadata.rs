//! General metadata fetchers for `PgClient`.

use super::{DbResult, PgClient, PgClientConnection};
use crate::pg::queries::{DATABASE_QUERY, DATABASE_TREE_QUERY, LOCKS_QUERY, SETTINGS_QUERY};
use sqlx::Row;

impl PgClient {
    pub(crate) async fn fetch_database_stats(
        &self,
        connection: &mut PgClientConnection,
    ) -> DbResult<Vec<Vec<String>>> {
        let rows = sqlx::query(DATABASE_QUERY)
            .fetch_all(connection.as_mut())
            .await
            .map_err(super::connect::classify_query_error)?;
        rows.into_iter()
            .map(|row| {
                Ok(vec![
                    row.try_get::<Option<String>, _>(0)?.unwrap_or_default(),
                    row.try_get::<i32, _>(1)?.to_string(),
                    row.try_get::<i64, _>(2)?.to_string(),
                    row.try_get::<i64, _>(3)?.to_string(),
                    format!("{:.1}%", row.try_get::<f64, _>(4)?),
                    row.try_get::<i64, _>(5)?.to_string(),
                    row.try_get::<i64, _>(6)?.to_string(),
                    row.try_get::<Option<chrono::DateTime<chrono::Utc>>, _>(7)?
                        .map(|timestamp| timestamp.to_rfc3339())
                        .unwrap_or_default(),
                ])
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(super::connect::classify_query_error)
    }

    pub(crate) async fn fetch_locks(
        &self,
        connection: &mut PgClientConnection,
    ) -> DbResult<Vec<Vec<String>>> {
        let rows = sqlx::query(LOCKS_QUERY)
            .fetch_all(connection.as_mut())
            .await
            .map_err(super::connect::classify_query_error)?;
        let result = rows
            .into_iter()
            .map(|row| {
                Ok(vec![
                    row.try_get::<String, _>(0)?,
                    row.try_get::<String, _>(1)?,
                    row.try_get::<String, _>(2)?,
                    row.try_get::<String, _>(3)?,
                    row.try_get::<String, _>(4)?,
                    row.try_get::<i64, _>(5)?.to_string(),
                    row.try_get::<String, _>(6)?,
                ])
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(super::connect::classify_query_error)?;

        if result.is_empty() {
            return Ok(vec![vec![
                String::new(),
                String::new(),
                String::new(),
                "No active locks found".to_string(),
                String::new(),
                String::new(),
                String::new(),
            ]]);
        }

        Ok(result)
    }

    pub(crate) async fn fetch_table_definition(
        &self,
        connection: &mut PgClientConnection,
        schema: &str,
        table: &str,
    ) -> DbResult<(Vec<Vec<String>>, Vec<String>)> {
        let col_query = r"
            SELECT
                column_name,
                data_type,
                COALESCE(character_maximum_length::text, ''),
                is_nullable,
                COALESCE(column_default, '')
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
            ORDER BY ordinal_position
        ";

        let columns = sqlx::query(col_query)
            .bind(schema)
            .bind(table)
            .fetch_all(connection.as_mut())
            .await
            .map_err(super::connect::classify_query_error)?
            .into_iter()
            .map(|row| {
                Ok(vec![
                    row.try_get::<String, _>(0)?,
                    row.try_get::<String, _>(1)?,
                    row.try_get::<String, _>(2)?,
                    row.try_get::<String, _>(3)?,
                    row.try_get::<String, _>(4)?,
                ])
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(super::connect::classify_query_error)?;

        let idx_query = r"
            SELECT pg_get_indexdef(indexrelid)
            FROM pg_index
            WHERE indrelid = ($1 || '.' || $2)::regclass
        ";

        let quoted_schema = format!("\"{schema}\"");
        let quoted_table = format!("\"{table}\"");

        let indexes = sqlx::query(idx_query)
            .bind(&quoted_schema)
            .bind(&quoted_table)
            .fetch_all(connection.as_mut())
            .await
            .map_err(super::connect::classify_query_error)?
            .into_iter()
            .map(|row| row.try_get::<String, _>(0))
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(super::connect::classify_query_error)?;

        Ok((columns, indexes))
    }

    pub(crate) async fn fetch_database_tree(
        &self,
        connection: &mut PgClientConnection,
    ) -> DbResult<Vec<Vec<String>>> {
        let rows = sqlx::query(DATABASE_TREE_QUERY)
            .fetch_all(connection.as_mut())
            .await
            .map_err(super::connect::classify_query_error)?;
        rows.into_iter()
            .map(|row| {
                let depth = row.try_get::<i32, _>(5)?;
                let label = match depth {
                    0 => row.try_get::<String, _>(0)?,
                    _ => format!(
                        "  {}",
                        row.try_get::<Option<String>, _>(1)?.unwrap_or_default()
                    ),
                };
                Ok(vec![
                    label,
                    row.try_get::<String, _>(2)?,
                    row.try_get::<i64, _>(3)?.to_string(),
                    row.try_get::<String, _>(4)?,
                    row.try_get::<String, _>(0)?,
                    row.try_get::<Option<String>, _>(1)?.unwrap_or_default(),
                    depth.to_string(),
                ])
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(super::connect::classify_query_error)
    }

    pub(crate) async fn fetch_settings(
        &self,
        connection: &mut PgClientConnection,
    ) -> DbResult<Vec<Vec<String>>> {
        let rows = sqlx::query(SETTINGS_QUERY)
            .fetch_all(connection.as_mut())
            .await
            .map_err(super::connect::classify_query_error)?;
        rows.into_iter()
            .map(|row| {
                Ok(vec![
                    row.try_get::<String, _>(0)?,
                    row.try_get::<String, _>(1)?,
                    row.try_get::<String, _>(2)?,
                    row.try_get::<String, _>(3)?,
                    row.try_get::<String, _>(4)?,
                ])
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(super::connect::classify_query_error)
    }
}
