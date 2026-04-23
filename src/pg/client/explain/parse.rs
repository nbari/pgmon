//! Parse-time explain validation helpers.

use super::ExplainMode;
use anyhow::{Context, Result};
use pg_query::NodeRef;

const GENERIC_PLAN_MIN_SERVER_VERSION_NUM: i32 = 160_000;

pub(super) fn supports_generic_explain(server_version_num: i32) -> bool {
    server_version_num >= GENERIC_PLAN_MIN_SERVER_VERSION_NUM
}

fn parse_tree_contains_param_ref(parse_result: &pg_query::protobuf::ParseResult) -> Result<bool> {
    // `pg_query::nodes()` only walks a subset of the parse tree and skips
    // branches such as INSERT VALUES lists. Inspect the full protobuf tree so
    // parameterized DML is classified consistently.
    let json_value = serde_json::to_value(parse_result).context(
        "Explain is unavailable because pgmon could not inspect the PostgreSQL parse tree safely.",
    )?;
    Ok(json_value_contains_param_ref(&json_value))
}

fn json_value_contains_param_ref(value: &serde_json::Value) -> bool {
    let mut stack = vec![value];
    while let Some(value) = stack.pop() {
        match value {
            serde_json::Value::Object(entries) => {
                if entries.contains_key("ParamRef") {
                    return true;
                }
                stack.extend(entries.values());
            }
            serde_json::Value::Array(values) => stack.extend(values.iter()),
            serde_json::Value::Null
            | serde_json::Value::Bool(_)
            | serde_json::Value::Number(_)
            | serde_json::Value::String(_) => {}
        }
    }

    false
}

pub(crate) fn analyze_explain_query(
    query: &str,
    server_version_num: Option<i32>,
) -> Result<ExplainMode> {
    let parsed = pg_query::parse(query).map_err(|error| {
        anyhow::anyhow!(
            "Explain is only available for a single PostgreSQL statement that pgmon can parse safely: {error}"
        )
    })?;

    if parsed.protobuf.stmts.len() != 1 {
        return Err(anyhow::anyhow!(
            "Explain only supports a single SQL statement. Multiple statements are refused to avoid executing trailing SQL."
        ));
    }

    let nodes = parsed.protobuf.nodes();
    let Some((top_level_node, _, _, _)) = nodes.first() else {
        return Err(anyhow::anyhow!(
            "Explain is unavailable because PostgreSQL returned an empty parse tree for this statement."
        ));
    };
    let contains_param_ref = parse_tree_contains_param_ref(&parsed.protobuf)?;

    let explain_mode = match top_level_node {
        NodeRef::SelectStmt(_)
        | NodeRef::InsertStmt(_)
        | NodeRef::UpdateStmt(_)
        | NodeRef::DeleteStmt(_)
        | NodeRef::MergeStmt(_) => {
            if contains_param_ref {
                ExplainMode::GenericEstimated
            } else {
                ExplainMode::Estimated
            }
        }
        _ => {
            return Err(anyhow::anyhow!(
                "Explain is only available for single SELECT, INSERT, UPDATE, DELETE, or MERGE statements."
            ));
        }
    };

    if explain_mode == ExplainMode::GenericEstimated
        && let Some(server_version_num) = server_version_num
        && !supports_generic_explain(server_version_num)
    {
        return Err(anyhow::anyhow!(
            "Generic estimated plans require PostgreSQL 16+; this server is PostgreSQL {}.",
            major_server_version(server_version_num)
        ));
    }

    Ok(explain_mode)
}

pub(super) fn validate_explain_query(
    query: &str,
    mode: ExplainMode,
    server_version_num: Option<i32>,
) -> Result<()> {
    let actual_mode = analyze_explain_query(query, server_version_num)?;
    if actual_mode != mode {
        return Err(anyhow::anyhow!(
            "Explain mode changed after parsing the SQL. Reopen the query info modal and try again."
        ));
    }
    Ok(())
}

fn major_server_version(server_version_num: i32) -> i32 {
    server_version_num / 10_000
}
