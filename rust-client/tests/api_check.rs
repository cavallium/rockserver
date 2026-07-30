use rockserver_client::proto::Kv;
use rockserver_client::{ColumnHashType, ColumnSchema, RockserverClient};

#[allow(dead_code)]
async fn ensure_api_compiles(client: &RockserverClient) {
    client
        .put_ensure(0, 1, vec![vec![1]], vec![2])
        .await
        .unwrap();
    client
        .put_multi_ensure(0, 1, futures::stream::empty::<Kv>())
        .await
        .unwrap();
}

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
