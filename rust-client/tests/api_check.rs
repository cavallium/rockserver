use rockserver_client::{ColumnSchema, ColumnHashType};

#[tokio::test]
async fn test_api_compilation() {
    // This test mainly verifies that the public API is accessible and compiles correctly.
    // It doesn't connect to a real server.

    // Check types
    let _schema = ColumnSchema {
        fixed_keys: vec![8],
        variable_tail_keys: vec![ColumnHashType::Xxhash32],
        has_value: true,
        merge_operator_name: Some("test".to_string()),
        merge_operator_version: Some(1),
    };
}
