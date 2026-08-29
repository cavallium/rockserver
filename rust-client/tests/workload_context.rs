use prost::Message;
use rockserver_client::proto::{
	put_batch_request, Kv, PutBatchInitialRequest, PutBatchRequest, PutRequest,
	RequestContext, WorkloadProfile,
};
use rockserver_client::RawSstToken;

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
	for (wire_value, expected) in [
		(0, WorkloadProfile::Unspecified),
		(1, WorkloadProfile::Control),
		(2, WorkloadProfile::Latency),
		(3, WorkloadProfile::Analytical),
		(4, WorkloadProfile::Ingest),
		(5, WorkloadProfile::Cdc),
		(6, WorkloadProfile::Batch),
		(7, WorkloadProfile::PhysicalMaintenance),
	] {
		assert_eq!(WorkloadProfile::try_from(wire_value), Ok(expected));
	}
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
fn raw_sst_tokens_accept_only_canonical_opaque_table_names() {
	for valid in ["000001.sst", "/000001.sst", "18446744073709551615.sst"] {
		assert_eq!(RawSstToken::new(valid.to_owned()).unwrap().as_str(), valid);
	}
	for invalid in [
		"000000.sst",
		"00001.sst",
		"0000001.sst",
		"../000001.sst",
		"/000001.ldb",
		"18446744073709551616.sst",
	] {
		assert!(RawSstToken::new(invalid.to_owned()).is_err(), "accepted {invalid}");
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
