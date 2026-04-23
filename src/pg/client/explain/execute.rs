//! Explain execution helpers for `PgClient`.

use super::{ExplainMode, PgClient, validate_explain_query};
use anyhow::Result;

impl ExplainMode {
    pub(super) fn statement_prefix(self) -> &'static str {
        match self {
            Self::Estimated => "EXPLAIN (VERBOSE, SETTINGS)",
            Self::GenericEstimated => "EXPLAIN (GENERIC_PLAN, VERBOSE, SETTINGS)",
        }
    }
}

impl PgClient {
    pub fn fetch_explain_plan(&mut self, query: &str, mode: ExplainMode) -> Result<Vec<String>> {
        validate_explain_query(query, mode, Some(self.server_version_num))?;
        let explain_query = format!("{} {query}", mode.statement_prefix());
        match mode {
            ExplainMode::Estimated => {
                let rows = self.client.query(&explain_query, &[])?;
                rows.into_iter()
                    .map(|row| row.try_get::<_, String>(0).map_err(anyhow::Error::from))
                    .collect()
            }
            ExplainMode::GenericEstimated => {
                use postgres::SimpleQueryMessage;

                // Simple query protocol is used here because it doesn't try to parse parameters
                // ($1, $2) on the client side, allowing us to send the raw query string to
                // Postgres after we have verified it contains only one statement.
                let messages = match self.client.simple_query(&explain_query) {
                    Ok(messages) => messages,
                    Err(error)
                        if error.to_string().contains("could not determine data type")
                            || error
                                .to_string()
                                .contains("could not determine polymorphic type") =>
                    {
                        return Err(anyhow::anyhow!(
                            "Generic estimated plan failed because PostgreSQL could not infer one or more parameter types. Add explicit casts or replace placeholders with real literals outside pgmon."
                        ));
                    }
                    Err(error) => return Err(error.into()),
                };
                let mut plan = Vec::new();
                for msg in messages {
                    if let SimpleQueryMessage::Row(row) = msg {
                        plan.push(row.get(0).unwrap_or_default().to_string());
                    }
                }
                Ok(plan)
            }
        }
    }
}
