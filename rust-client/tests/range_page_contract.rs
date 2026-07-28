use prost::Message;
use rockserver_client::proto::{
    CapabilitiesResponse, GetRangePageRequest, Kv, RangeBudget, RangeKey, RangePage,
    RangeRequestType, RequestContext,
};
use rockserver_client::REQUIRED_WORKLOAD_CONTRACT_VERSION;

#[test]
fn workload_v2_and_bounded_range_capability_are_mandatory() {
    let capability = CapabilitiesResponse {
        workload_contract_version: REQUIRED_WORKLOAD_CONTRACT_VERSION,
        bounded_range: true,
    };
    let decoded = CapabilitiesResponse::decode(capability.encode_to_vec().as_slice()).unwrap();
    assert_eq!(decoded.workload_contract_version, 2);
    assert!(decoded.bounded_range);
}

#[test]
fn range_request_types_have_explicit_stable_values() {
    assert_eq!(RangeRequestType::Unspecified as i32, 0);
    assert_eq!(RangeRequestType::AllInRange as i32, 1);
    assert_eq!(RangeRequestType::AllInRangeNoCache as i32, 2);
}

#[test]
fn page_request_repeats_original_bounds_and_carries_an_exclusive_cursor() {
    let request = GetRangePageRequest {
        transaction_id: 7,
        column_id: 11,
        start_keys_inclusive: vec![vec![1]],
        end_keys_exclusive: vec![vec![9]],
        reverse: true,
        resume_after: Some(RangeKey {
            keys: vec![vec![6]],
        }),
        request_type: RangeRequestType::AllInRange as i32,
        timeout_ms: 5_000,
        budget: Some(RangeBudget {
            max_items: 4_096,
            max_bytes: 8 * 1024 * 1024,
        }),
        context: Some(RequestContext {
            profile: 2,
            deadline_epoch_millis: 123_456,
        }),
    };
    let decoded = GetRangePageRequest::decode(request.encode_to_vec().as_slice()).unwrap();
    assert_eq!(decoded.start_keys_inclusive, vec![vec![1]]);
    assert_eq!(decoded.end_keys_exclusive, vec![vec![9]]);
    assert_eq!(decoded.resume_after.unwrap().keys, vec![vec![6]]);
    assert!(decoded.reverse);
}

#[test]
fn page_response_preserves_cursor_and_has_more_without_partial_ambiguity() {
    let page = RangePage {
        items: vec![Kv {
            keys: vec![vec![3]],
            value: vec![4],
        }],
        resume_after: Some(RangeKey {
            keys: vec![vec![3]],
        }),
        has_more: true,
    };
    let decoded = RangePage::decode(page.encode_to_vec().as_slice()).unwrap();
    assert_eq!(decoded.items.len(), 1);
    assert_eq!(decoded.resume_after.unwrap().keys, vec![vec![3]]);
    assert!(decoded.has_more);
}
