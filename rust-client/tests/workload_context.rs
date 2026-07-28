use prost::Message;
use rockserver_client::proto::{
	put_batch_request, Kv, PutBatchInitialRequest, PutBatchRequest, PutRequest,
	RequestContext, WorkloadProfile,
};

#[test]
fn unary_request_retains_mandatory_workload_context() {
	let context = RequestContext {
		profile: 4,
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
		profile: 6,
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

#[test]
fn all_workload_profile_values_are_stable() {
	assert_eq!(WorkloadProfile::Unspecified as i32, 0);
	assert_eq!(WorkloadProfile::Control as i32, 1);
	assert_eq!(WorkloadProfile::Latency as i32, 2);
	assert_eq!(WorkloadProfile::Analytical as i32, 3);
	assert_eq!(WorkloadProfile::Ingest as i32, 4);
	assert_eq!(WorkloadProfile::Cdc as i32, 5);
	assert_eq!(WorkloadProfile::Batch as i32, 6);
	assert_eq!(WorkloadProfile::PhysicalMaintenance as i32, 7);
}

#[test]
fn typed_context_constructor_rejects_protected_profiles() {
	for profile in [
		WorkloadProfile::Control,
		WorkloadProfile::Cdc,
		WorkloadProfile::PhysicalMaintenance,
	] {
		assert!(std::panic::catch_unwind(|| RequestContext::for_profile(profile, i64::MAX)).is_err());
	}
}

#[test]
fn typed_context_constructor_uses_explicit_selectable_wire_values() {
	for (profile, expected) in [
		(WorkloadProfile::Latency, 2),
		(WorkloadProfile::Analytical, 3),
		(WorkloadProfile::Ingest, 4),
		(WorkloadProfile::Batch, 6),
	] {
		let deadline = if profile == WorkloadProfile::Latency { 1 } else { i64::MAX };
		assert_eq!(RequestContext::for_profile(profile, deadline).profile, expected);
	}
}
