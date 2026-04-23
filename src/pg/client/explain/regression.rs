use super::ExplainMode;
use super::parse::validate_explain_query;

#[test]
fn test_regression_validate_explain_query_accepts_parameterized_insert_values() {
    let result = validate_explain_query(
        "INSERT INTO accounts (id, value) VALUES ($1::integer, $2::integer)",
        ExplainMode::GenericEstimated,
        Some(160_000),
    );

    assert!(result.is_ok());
}
