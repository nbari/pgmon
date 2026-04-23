use super::parse::{supports_generic_explain, validate_explain_query};
use super::{ExplainMode, analyze_explain_query};
use crate::pg::client::{DbRuntime, PgClient, PgClientConnection};
use anyhow::{Result, anyhow};
use sqlx::Row;
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
    with_live_clients(&dsn, |control_client, explain_client| async move {
        let mut control_connection = control_client.acquire().await.map_err(db_error)?;
        let mut explain_connection = explain_client.acquire().await.map_err(db_error)?;
        sqlx::raw_sql(&format!(
            "DROP TABLE IF EXISTS \"{table_name}\"; CREATE TABLE \"{table_name}\" (id integer PRIMARY KEY, value integer NOT NULL);"
        ))
        .execute(control_connection.as_mut())
        .await?;

        let query = format!("INSERT INTO \"{table_name}\" (id, value) VALUES (1, 10)");
        let plan = explain_client
            .fetch_explain_plan(&mut explain_connection, &query, ExplainMode::Estimated)
            .await
            .map_err(db_error)?;

        assert!(!plan.is_empty());
        assert_eq!(
            table_row_count(&mut control_connection, &table_name).await?,
            0
        );

        sqlx::raw_sql(&format!("DROP TABLE IF EXISTS \"{table_name}\";"))
            .execute(control_connection.as_mut())
            .await?;
        Ok(())
    })
}

#[test]
fn test_explain_safety_live_update_does_not_mutate_rows() -> Result<()> {
    let Some(dsn) = live_test_dsn() else {
        return Ok(());
    };
    let table_name = unique_test_table_name("update");
    with_live_clients(&dsn, |control_client, explain_client| async move {
        let mut control_connection = control_client.acquire().await.map_err(db_error)?;
        let mut explain_connection = explain_client.acquire().await.map_err(db_error)?;
        sqlx::raw_sql(&format!(
            "DROP TABLE IF EXISTS \"{table_name}\"; CREATE TABLE \"{table_name}\" (id integer PRIMARY KEY, value integer NOT NULL); INSERT INTO \"{table_name}\" (id, value) VALUES (1, 10);"
        ))
        .execute(control_connection.as_mut())
        .await?;

        let query = format!("UPDATE \"{table_name}\" SET value = 20 WHERE id = 1");
        let plan = explain_client
            .fetch_explain_plan(&mut explain_connection, &query, ExplainMode::Estimated)
            .await
            .map_err(db_error)?;

        assert!(!plan.is_empty());
        assert_eq!(
            table_value(&mut control_connection, &table_name, 1).await?,
            10
        );

        sqlx::raw_sql(&format!("DROP TABLE IF EXISTS \"{table_name}\";"))
            .execute(control_connection.as_mut())
            .await?;
        Ok(())
    })
}

#[test]
fn test_explain_safety_live_delete_does_not_mutate_rows() -> Result<()> {
    let Some(dsn) = live_test_dsn() else {
        return Ok(());
    };
    let table_name = unique_test_table_name("delete");
    with_live_clients(&dsn, |control_client, explain_client| async move {
        let mut control_connection = control_client.acquire().await.map_err(db_error)?;
        let mut explain_connection = explain_client.acquire().await.map_err(db_error)?;
        sqlx::raw_sql(&format!(
            "DROP TABLE IF EXISTS \"{table_name}\"; CREATE TABLE \"{table_name}\" (id integer PRIMARY KEY, value integer NOT NULL); INSERT INTO \"{table_name}\" (id, value) VALUES (1, 10);"
        ))
        .execute(control_connection.as_mut())
        .await?;

        let query = format!("DELETE FROM \"{table_name}\" WHERE id = 1");
        let plan = explain_client
            .fetch_explain_plan(&mut explain_connection, &query, ExplainMode::Estimated)
            .await
            .map_err(db_error)?;

        assert!(!plan.is_empty());
        assert_eq!(
            table_row_count(&mut control_connection, &table_name).await?,
            1
        );

        sqlx::raw_sql(&format!("DROP TABLE IF EXISTS \"{table_name}\";"))
            .execute(control_connection.as_mut())
            .await?;
        Ok(())
    })
}

#[test]
fn test_explain_safety_live_modifying_cte_select_does_not_mutate_rows() -> Result<()> {
    let Some(dsn) = live_test_dsn() else {
        return Ok(());
    };
    let table_name = unique_test_table_name("cte");
    with_live_clients(&dsn, |control_client, explain_client| async move {
        let mut control_connection = control_client.acquire().await.map_err(db_error)?;
        let mut explain_connection = explain_client.acquire().await.map_err(db_error)?;
        sqlx::raw_sql(&format!(
            "DROP TABLE IF EXISTS \"{table_name}\"; CREATE TABLE \"{table_name}\" (id integer PRIMARY KEY, value integer NOT NULL); INSERT INTO \"{table_name}\" (id, value) VALUES (1, 10);"
        ))
        .execute(control_connection.as_mut())
        .await?;

        let query = format!(
            "WITH deleted AS (DELETE FROM \"{table_name}\" WHERE id = 1 RETURNING id) SELECT * FROM deleted"
        );
        let plan = explain_client
            .fetch_explain_plan(&mut explain_connection, &query, ExplainMode::Estimated)
            .await
            .map_err(db_error)?;

        assert!(!plan.is_empty());
        assert_eq!(
            table_row_count(&mut control_connection, &table_name).await?,
            1
        );

        sqlx::raw_sql(&format!("DROP TABLE IF EXISTS \"{table_name}\";"))
            .execute(control_connection.as_mut())
            .await?;
        Ok(())
    })
}

#[test]
fn test_explain_safety_live_generic_insert_does_not_mutate_rows() -> Result<()> {
    let Some(dsn) = live_test_dsn() else {
        return Ok(());
    };
    let table_name = unique_test_table_name("generic");
    with_live_clients(&dsn, |control_client, explain_client| async move {
        let mut control_connection = control_client.acquire().await.map_err(db_error)?;
        let mut explain_connection = explain_client.acquire().await.map_err(db_error)?;
        let server_version_num = current_server_version_num(&mut control_connection).await?;
        if !supports_generic_explain(server_version_num) {
            return Ok(());
        }

        sqlx::raw_sql(&format!(
            "DROP TABLE IF EXISTS \"{table_name}\"; CREATE TABLE \"{table_name}\" (id integer PRIMARY KEY, value integer NOT NULL);"
        ))
        .execute(control_connection.as_mut())
        .await?;

        let query =
            format!("INSERT INTO \"{table_name}\" (id, value) VALUES ($1::integer, $2::integer)");
        let plan = explain_client
            .fetch_explain_plan(
                &mut explain_connection,
                &query,
                ExplainMode::GenericEstimated,
            )
            .await
            .map_err(db_error)?;

        assert!(!plan.is_empty());
        assert_eq!(
            table_row_count(&mut control_connection, &table_name).await?,
            0
        );

        sqlx::raw_sql(&format!("DROP TABLE IF EXISTS \"{table_name}\";"))
            .execute(control_connection.as_mut())
            .await?;
        Ok(())
    })
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

fn with_live_clients<F, Fut>(dsn: &str, test: F) -> Result<()>
where
    F: FnOnce(PgClient, PgClient) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let runtime = DbRuntime::new()?;
    let executor = runtime.executor();
    runtime.block_on(async move {
        let control_client = executor
            .clone()
            .client(dsn, 5_000)
            .await
            .map_err(db_error)?;
        let explain_client = executor.client(dsn, 5_000).await.map_err(db_error)?;
        test(control_client, explain_client).await
    })
}

async fn current_server_version_num(connection: &mut PgClientConnection) -> Result<i32> {
    let row = sqlx::query("SELECT current_setting('server_version_num')::int")
        .fetch_one(connection.as_mut())
        .await?;
    row.try_get::<i32, _>(0).map_err(Into::into)
}

async fn table_row_count(connection: &mut PgClientConnection, table_name: &str) -> Result<i64> {
    let row = sqlx::query(&format!("SELECT COUNT(*) FROM \"{table_name}\""))
        .fetch_one(connection.as_mut())
        .await?;
    row.try_get::<i64, _>(0).map_err(Into::into)
}

async fn table_value(
    connection: &mut PgClientConnection,
    table_name: &str,
    id: i32,
) -> Result<i32> {
    let row = sqlx::query(&format!("SELECT value FROM \"{table_name}\" WHERE id = $1"))
        .bind(id)
        .fetch_one(connection.as_mut())
        .await?;
    row.try_get::<i32, _>(0).map_err(Into::into)
}

fn db_error(error: impl std::fmt::Display) -> anyhow::Error {
    anyhow!("{error}")
}
