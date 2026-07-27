use prost::Message;
use rockserver_client::proto::{
	put_batch_request, Kv, PutBatchInitialRequest, PutBatchRequest, PutRequest,
	RequestContext, WorkloadProfile,
};

#[test]
fn unary_request_retains_mandatory_workload_context() {
	let context = RequestContext {
		profile: WorkloadProfile::Ingest as i32,
		deadline_epoch_millis: i64::MAX,
	};
	let request = PutRequest {
		transaction_or_update_id: 0,
		column_id: 1,
		data: Some(Kv { keys: vec![vec![1]], value: vec![2] }),
		context: Some(context.clone()),
	};
	let decoded = PutRequest::decode(request.encode_to_vec().as_slice()).unwrap();
	assert_eq!(decoded.context, Some(context));
}

#[test]
fn streaming_initial_request_retains_workload_context() {
	let context = RequestContext {
		profile: WorkloadProfile::Batch as i32,
		deadline_epoch_millis: i64::MAX,
	};
	let request = PutBatchRequest {
		put_batch_request_type: Some(put_batch_request::PutBatchRequestType::InitialRequest(
			PutBatchInitialRequest { column_id: 1, mode: 0, context: Some(context.clone()) },
		)),
	};
	let decoded = PutBatchRequest::decode(request.encode_to_vec().as_slice()).unwrap();
	let Some(put_batch_request::PutBatchRequestType::InitialRequest(initial)) =
		decoded.put_batch_request_type
	else {
		panic!("expected initial request");
	};
	assert_eq!(initial.context, Some(context));
}
