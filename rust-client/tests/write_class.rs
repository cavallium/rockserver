use prost::Message;
use rockserver_client::proto::{
    put_batch_request, Kv, PutBatchInitialRequest, PutBatchRequest, PutRequest, WriteClass,
};

#[test]
fn legacy_protobuf_defaults_to_foreground() {
    let request = PutRequest {
        transaction_or_update_id: 0,
        column_id: 1,
        data: Some(Kv {
            keys: vec![vec![1]],
            value: vec![2],
        }),
        write_class: WriteClass::Foreground as i32,
    };
    let decoded = PutRequest::decode(request.encode_to_vec().as_slice()).unwrap();
    assert_eq!(decoded.write_class, WriteClass::Foreground as i32);
}

#[test]
fn streaming_initial_request_retains_maintenance() {
    let request = PutBatchRequest {
        put_batch_request_type: Some(put_batch_request::PutBatchRequestType::InitialRequest(
            PutBatchInitialRequest {
                column_id: 1,
                mode: 0,
                write_class: WriteClass::Maintenance as i32,
            },
        )),
    };
    let decoded = PutBatchRequest::decode(request.encode_to_vec().as_slice()).unwrap();
    let Some(put_batch_request::PutBatchRequestType::InitialRequest(initial)) =
        decoded.put_batch_request_type
    else {
        panic!("expected initial request");
    };
    assert_eq!(initial.write_class, WriteClass::Maintenance as i32);
}
