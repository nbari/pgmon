//! Explain execution helpers for `PgClient`.

use super::{ExplainMode, PgClient, validate_explain_query};
use crate::pg::client::{DbError, DbResult, PgClientConnection};
use sqlx::Row;

impl ExplainMode {
    pub(super) const fn statement_prefix(self) -> &'static str {
        match self {
            Self::Estimated => "EXPLAIN (VERBOSE, SETTINGS)",
            Self::GenericEstimated => "EXPLAIN (GENERIC_PLAN, VERBOSE, SETTINGS)",
        }
    }
}

impl PgClient {
    pub(crate) async fn fetch_explain_plan(
        &self,
        connection: &mut PgClientConnection,
        query: &str,
        mode: ExplainMode,
    ) -> DbResult<Vec<String>> {
        validate_explain_query(query, mode, Some(self.server_version_num()))
            .map_err(|error| DbError::fatal(error.to_string()))?;
        let explain_query = format!("{} {query}", mode.statement_prefix());

        match mode {
            ExplainMode::Estimated => sqlx::query(&explain_query)
                .fetch_all(connection.as_mut())
                .await
                .map_err(super::super::connect::classify_query_error)?
                .into_iter()
                .map(|row| {
                    row.try_get::<String, _>(0)
                        .map_err(super::super::connect::classify_query_error)
                })
                .collect(),
            ExplainMode::GenericEstimated => sqlx::raw_sql(&explain_query)
                .fetch_all(connection.as_mut())
                .await
                .map_err(|error| {
                    let details = error.to_string();
                    if details.contains("could not determine data type")
                        || details.contains("could not determine polymorphic type")
                    {
                        return DbError::fatal(
                            "Generic estimated plan failed because PostgreSQL could not infer one or more parameter types. Add explicit casts or replace placeholders with real literals outside pgmon.",
                        );
                    }
                    super::super::connect::classify_query_error(error)
                })?
                .into_iter()
                .map(|row| {
                    row.try_get::<String, _>(0)
                        .map_err(super::super::connect::classify_query_error)
                })
                .collect(),
        }
    }
}
