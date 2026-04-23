//! General metadata fetchers for `PgClient`.

use super::PgClient;
use crate::pg::queries::{DATABASE_QUERY, DATABASE_TREE_QUERY, LOCKS_QUERY, SETTINGS_QUERY};
use anyhow::Result;

impl PgClient {
    pub fn fetch_database_stats(&mut self) -> Result<Vec<Vec<String>>> {
        let rows = self.client.query(DATABASE_QUERY, &[])?;
        rows.into_iter()
            .map(|row| {
                Ok(vec![
                    row.try_get::<_, Option<String>>(0)?.unwrap_or_default(),
                    row.try_get::<_, i32>(1)?.to_string(),
                    row.try_get::<_, i64>(2)?.to_string(),
                    row.try_get::<_, i64>(3)?.to_string(),
                    format!("{:.1}%", row.try_get::<_, f64>(4)?),
                    row.try_get::<_, i64>(5)?.to_string(),
                    row.try_get::<_, i64>(6)?.to_string(),
                    row.try_get::<_, Option<chrono::DateTime<chrono::Utc>>>(7)?
                        .map(|t| t.to_rfc3339())
                        .unwrap_or_default(),
                ])
            })
            .collect()
    }

    pub fn fetch_locks(&mut self) -> Result<Vec<Vec<String>>> {
        let rows = self.client.query(LOCKS_QUERY, &[])?;
        let result: Result<Vec<Vec<String>>> = rows
            .into_iter()
            .map(|row| {
                Ok(vec![
                    row.try_get::<_, String>(0)?,
                    row.try_get::<_, String>(1)?,
                    row.try_get::<_, String>(2)?,
                    row.try_get::<_, String>(3)?,
                    row.try_get::<_, String>(4)?,
                    row.try_get::<_, i64>(5)?.to_string(),
                    row.try_get::<_, String>(6)?,
                ])
            })
            .collect();

        let mut data = result?;
        if data.is_empty() {
            data.push(vec![
                String::new(),
                String::new(),
                String::new(),
                "No active locks found".to_string(),
                String::new(),
                String::new(),
                String::new(),
            ]);
        }
        Ok(data)
    }

    pub fn fetch_table_definition(
        &mut self,
        schema: &str,
        table: &str,
    ) -> Result<(Vec<Vec<String>>, Vec<String>)> {
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

        let columns = self
            .client
            .query(col_query, &[&schema, &table])?
            .into_iter()
            .map(|row| {
                Ok(vec![
                    row.try_get::<_, String>(0)?,
                    row.try_get::<_, String>(1)?,
                    row.try_get::<_, String>(2)?,
                    row.try_get::<_, String>(3)?,
                    row.try_get::<_, String>(4)?,
                ])
            })
            .collect::<Result<Vec<_>>>()?;

        let idx_query = r"
            SELECT pg_get_indexdef(indexrelid)
            FROM pg_index
            WHERE indrelid = ($1 || '.' || $2)::regclass
        ";

        let quoted_schema = format!("\"{schema}\"");
        let quoted_table = format!("\"{table}\"");

        let indexes = self
            .client
            .query(idx_query, &[&quoted_schema, &quoted_table])?
            .into_iter()
            .map(|row| row.try_get::<_, String>(0))
            .collect::<Result<Vec<_>, _>>()?;

        Ok((columns, indexes))
    }

    pub fn fetch_database_tree(&mut self) -> Result<Vec<Vec<String>>> {
        let rows = self.client.query(DATABASE_TREE_QUERY, &[])?;
        rows.into_iter()
            .map(|row| {
                let depth = row.try_get::<_, i32>(5)?;
                let label = match depth {
                    0 => row.try_get::<_, String>(0)?,
                    _ => format!(
                        "  {}",
                        row.try_get::<_, Option<String>>(1)?.unwrap_or_default()
                    ),
                };
                Ok(vec![
                    label,
                    row.try_get::<_, String>(2)?,
                    row.try_get::<_, i64>(3)?.to_string(),
                    row.try_get::<_, String>(4)?,
                    row.try_get::<_, String>(0)?,
                    row.try_get::<_, Option<String>>(1)?.unwrap_or_default(),
                    depth.to_string(),
                ])
            })
            .collect()
    }

    pub fn fetch_settings(&mut self) -> Result<Vec<Vec<String>>> {
        let rows = self.client.query(SETTINGS_QUERY, &[])?;
        rows.into_iter()
            .map(|row| {
                Ok(vec![
                    row.try_get::<_, String>(0)?,
                    row.try_get::<_, String>(1)?,
                    row.try_get::<_, String>(2)?,
                    row.try_get::<_, String>(3)?,
                    row.try_get::<_, String>(4)?,
                ])
            })
            .collect()
    }
}
