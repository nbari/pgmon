use super::parse::{supports_generic_explain, validate_explain_query};
use super::{ExplainMode, PgClient, analyze_explain_query};
use crate::pg::client::connect::config_from_dsn;
use anyhow::Result;
use postgres::{Client, NoTls};
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn test_supports_generic_explain_requires_postgresql_16() {
    assert!(!supports_generic_explain(150_000));
    assert!(supports_generic_explain(160_000));
}

#[test]
fn test_analyze_explain_query_accepts_single_select() -> Result<()> {
    let mode = analyze_explain_query("SELECT 1", Some(160_000))?;

    assert_eq!(mode, ExplainMode::Estimated);
    Ok(())
}

#[test]
fn test_analyze_explain_query_detects_placeholder_params_from_parse_tree() -> Result<()> {
    let mode = analyze_explain_query("SELECT * FROM accounts WHERE id = $1", Some(160_000))?;

    assert_eq!(mode, ExplainMode::GenericEstimated);
    Ok(())
}

#[test]
fn test_analyze_explain_query_ignores_placeholder_text_in_literals_and_comments() -> Result<()> {
    let mode = analyze_explain_query(
        "SELECT '$1' AS literal /* $2 */ -- $3\nFROM pg_catalog.pg_class",
        Some(160_000),
    )?;

    assert_eq!(mode, ExplainMode::Estimated);
    Ok(())
}

#[test]
fn test_analyze_explain_query_rejects_utility_statement() {
    let result = analyze_explain_query("SET application_name = 'pgmon'", Some(160_000));

    assert!(result.is_err());
    let message = result
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default();
    assert!(message.contains("SELECT, INSERT, UPDATE, DELETE, or MERGE"));
}

#[test]
fn test_analyze_explain_query_rejects_nested_explain_statement() {
    let result = analyze_explain_query("EXPLAIN SELECT 1", Some(160_000));

    assert!(result.is_err());
    let message = result
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default();
    assert!(message.contains("SELECT, INSERT, UPDATE, DELETE, or MERGE"));
}

#[test]
fn test_analyze_explain_query_rejects_create_table_as_statement() {
    let result = analyze_explain_query(
        "CREATE TABLE explain_review AS SELECT 1 AS id",
        Some(160_000),
    );

    assert!(result.is_err());
    let message = result
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default();
    assert!(message.contains("SELECT, INSERT, UPDATE, DELETE, or MERGE"));
}

#[test]
fn test_validate_explain_query_rejects_multiple_statements() {
    let result = validate_explain_query(
        "SELECT 1; DELETE FROM accounts",
        ExplainMode::Estimated,
        Some(160_000),
    );

    assert!(result.is_err());
    let message = result
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default();
    assert!(message.contains("single SQL statement"));
}

#[test]
fn test_validate_explain_query_rejects_generic_plan_on_postgresql_15() {
    let result = validate_explain_query(
        "SELECT * FROM accounts WHERE id = $1",
        ExplainMode::GenericEstimated,
        Some(150_000),
    );

    assert!(result.is_err());
    let message = result
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default();
    assert!(message.contains("PostgreSQL 16+"));
}

#[test]
fn test_validate_explain_query_rejects_mode_mismatch() {
    let result = validate_explain_query(
        "SELECT * FROM accounts WHERE id = $1",
        ExplainMode::Estimated,
        Some(160_000),
    );

    assert!(result.is_err());
    let message = result
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default();
    assert!(message.contains("Explain mode changed"));
}

#[test]
fn test_explain_safety_live_insert_does_not_mutate_rows() -> Result<()> {
    let Some(dsn) = live_test_dsn() else {
        return Ok(());
    };
    let table_name = unique_test_table_name("insert");
    let mut control_client = connect_test_client(&dsn)?;
    control_client.batch_execute(&format!(
        "DROP TABLE IF EXISTS \"{table_name}\"; CREATE TABLE \"{table_name}\" (id integer PRIMARY KEY, value integer NOT NULL);"
    ))?;

    let mut explain_client = PgClient::new(&dsn, 5_000)?;
    let query = format!("INSERT INTO \"{table_name}\" (id, value) VALUES (1, 10)");
    let plan = explain_client.fetch_explain_plan(&query, ExplainMode::Estimated)?;

    assert!(!plan.is_empty());
    assert_eq!(table_row_count(&mut control_client, &table_name)?, 0);

    control_client.batch_execute(&format!("DROP TABLE IF EXISTS \"{table_name}\";"))?;
    Ok(())
}

#[test]
fn test_explain_safety_live_update_does_not_mutate_rows() -> Result<()> {
    let Some(dsn) = live_test_dsn() else {
        return Ok(());
    };
    let table_name = unique_test_table_name("update");
    let mut control_client = connect_test_client(&dsn)?;
    control_client.batch_execute(&format!(
        "DROP TABLE IF EXISTS \"{table_name}\"; CREATE TABLE \"{table_name}\" (id integer PRIMARY KEY, value integer NOT NULL); INSERT INTO \"{table_name}\" (id, value) VALUES (1, 10);"
    ))?;

    let mut explain_client = PgClient::new(&dsn, 5_000)?;
    let query = format!("UPDATE \"{table_name}\" SET value = 20 WHERE id = 1");
    let plan = explain_client.fetch_explain_plan(&query, ExplainMode::Estimated)?;

    assert!(!plan.is_empty());
    assert_eq!(table_value(&mut control_client, &table_name, 1)?, 10);

    control_client.batch_execute(&format!("DROP TABLE IF EXISTS \"{table_name}\";"))?;
    Ok(())
}

#[test]
fn test_explain_safety_live_delete_does_not_mutate_rows() -> Result<()> {
    let Some(dsn) = live_test_dsn() else {
        return Ok(());
    };
    let table_name = unique_test_table_name("delete");
    let mut control_client = connect_test_client(&dsn)?;
    control_client.batch_execute(&format!(
        "DROP TABLE IF EXISTS \"{table_name}\"; CREATE TABLE \"{table_name}\" (id integer PRIMARY KEY, value integer NOT NULL); INSERT INTO \"{table_name}\" (id, value) VALUES (1, 10);"
    ))?;

    let mut explain_client = PgClient::new(&dsn, 5_000)?;
    let query = format!("DELETE FROM \"{table_name}\" WHERE id = 1");
    let plan = explain_client.fetch_explain_plan(&query, ExplainMode::Estimated)?;

    assert!(!plan.is_empty());
    assert_eq!(table_row_count(&mut control_client, &table_name)?, 1);

    control_client.batch_execute(&format!("DROP TABLE IF EXISTS \"{table_name}\";"))?;
    Ok(())
}

#[test]
fn test_explain_safety_live_modifying_cte_select_does_not_mutate_rows() -> Result<()> {
    let Some(dsn) = live_test_dsn() else {
        return Ok(());
    };
    let table_name = unique_test_table_name("cte");
    let mut control_client = connect_test_client(&dsn)?;
    control_client.batch_execute(&format!(
        "DROP TABLE IF EXISTS \"{table_name}\"; CREATE TABLE \"{table_name}\" (id integer PRIMARY KEY, value integer NOT NULL); INSERT INTO \"{table_name}\" (id, value) VALUES (1, 10);"
    ))?;

    let mut explain_client = PgClient::new(&dsn, 5_000)?;
    let query = format!(
        "WITH deleted AS (DELETE FROM \"{table_name}\" WHERE id = 1 RETURNING id) SELECT * FROM deleted"
    );
    let plan = explain_client.fetch_explain_plan(&query, ExplainMode::Estimated)?;

    assert!(!plan.is_empty());
    assert_eq!(table_row_count(&mut control_client, &table_name)?, 1);

    control_client.batch_execute(&format!("DROP TABLE IF EXISTS \"{table_name}\";"))?;
    Ok(())
}

#[test]
fn test_explain_safety_live_generic_insert_does_not_mutate_rows() -> Result<()> {
    let Some(dsn) = live_test_dsn() else {
        return Ok(());
    };
    let mut control_client = connect_test_client(&dsn)?;
    let server_version_num = current_server_version_num(&mut control_client)?;
    if !supports_generic_explain(server_version_num) {
        return Ok(());
    }

    let table_name = unique_test_table_name("generic");
    control_client.batch_execute(&format!(
        "DROP TABLE IF EXISTS \"{table_name}\"; CREATE TABLE \"{table_name}\" (id integer PRIMARY KEY, value integer NOT NULL);"
    ))?;

    let mut explain_client = PgClient::new(&dsn, 5_000)?;
    let query =
        format!("INSERT INTO \"{table_name}\" (id, value) VALUES ($1::integer, $2::integer)");
    let plan = explain_client.fetch_explain_plan(&query, ExplainMode::GenericEstimated)?;

    assert!(!plan.is_empty());
    assert_eq!(table_row_count(&mut control_client, &table_name)?, 0);

    control_client.batch_execute(&format!("DROP TABLE IF EXISTS \"{table_name}\";"))?;
    Ok(())
}

fn live_test_dsn() -> Option<String> {
    std::env::var("PGMON_TEST_DSN")
        .ok()
        .filter(|dsn| !dsn.trim().is_empty())
}

fn unique_test_table_name(suffix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("pgmon_explain_safety_{suffix}_{nanos}")
}

fn connect_test_client(dsn: &str) -> Result<Client> {
    let config = config_from_dsn(dsn, 5_000)?;
    Ok(config.connect(NoTls)?)
}

fn current_server_version_num(client: &mut Client) -> Result<i32> {
    let row = client.query_one("SELECT current_setting('server_version_num')::int", &[])?;
    Ok(row.try_get::<_, i32>(0)?)
}

fn table_row_count(client: &mut Client, table_name: &str) -> Result<i64> {
    let row = client.query_one(&format!("SELECT COUNT(*) FROM \"{table_name}\""), &[])?;
    Ok(row.try_get::<_, i64>(0)?)
}

fn table_value(client: &mut Client, table_name: &str, id: i32) -> Result<i32> {
    let row = client.query_one(
        &format!("SELECT value FROM \"{table_name}\" WHERE id = $1"),
        &[&id],
    )?;
    Ok(row.try_get::<_, i32>(0)?)
}
