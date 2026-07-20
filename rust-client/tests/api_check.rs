use rockserver_client::{ColumnHashType, ColumnSchema, RockserverStatusExt};
use tonic::{Code, Status};

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

#[test]
fn test_rockserver_status_error_parsing() {
    let status = Status::internal(
        "RocksDBError: [uid:NOT_IMPLEMENTED] Local handles are unsupported in replicated mode",
    );

    let parsed = status
        .rockserver_error()
        .expect("expected Rockserver error");
    assert_eq!("NOT_IMPLEMENTED", parsed.error_type);
    assert_eq!(
        "Local handles are unsupported in replicated mode",
        parsed.message
    );
    assert!(status.is_rockserver_error("NOT_IMPLEMENTED"));
}

#[test]
fn test_plain_tonic_status_is_not_rockserver_error() {
    let status = Status::new(Code::Internal, "plain internal error");

    assert!(status.rockserver_error().is_none());
    assert!(!status.is_rockserver_error("NOT_IMPLEMENTED"));
}
