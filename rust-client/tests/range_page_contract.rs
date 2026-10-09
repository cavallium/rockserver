use prost::Message;
use rockserver_client::proto::{
    CapabilitiesResponse, GetRangePageRequest, Kv, RangeBudget, RangeKey, RangePage,
    RangeRequestType, RequestContext,
};

#[test]
fn capabilities_preserve_backend_modes_and_range_limits() {
    let capability = CapabilitiesResponse {
        backend_mode: rockserver_client::BackendMode::Distributed as i32,
        raw_scan_mode: rockserver_client::RawScanMode::ImmutableExportJobs as i32,
        maximum_range_page_keys: 4_096,
        maximum_range_page_bytes: 8 * 1024 * 1024,
    };
    let decoded = CapabilitiesResponse::decode(capability.encode_to_vec().as_slice()).unwrap();
    assert_eq!(decoded, capability);
    let typed = rockserver_client::Capabilities::try_from(decoded).unwrap();
    assert_eq!(
        typed.backend_mode,
        rockserver_client::BackendMode::Distributed
    );
    assert_eq!(
        typed.raw_scan_mode,
        rockserver_client::RawScanMode::ImmutableExportJobs
    );
    assert_eq!(typed.maximum_range_page_keys, 4_096);
    assert_eq!(typed.maximum_range_page_bytes, 8 * 1024 * 1024);
}

#[test]
fn capabilities_reject_missing_unknown_modes_and_nonpositive_limits() {
    let valid = CapabilitiesResponse {
        backend_mode: rockserver_client::BackendMode::Standalone as i32,
        raw_scan_mode: rockserver_client::RawScanMode::PhysicalSst as i32,
        maximum_range_page_keys: 4_096,
        maximum_range_page_bytes: 8 * 1024 * 1024,
    };
    for value in [0, -1, 99] {
        assert!(
            rockserver_client::Capabilities::try_from(CapabilitiesResponse {
                backend_mode: value,
                ..valid.clone()
            })
            .is_err()
        );
        assert!(
            rockserver_client::Capabilities::try_from(CapabilitiesResponse {
                raw_scan_mode: value,
                ..valid.clone()
            })
            .is_err()
        );
    }
    for value in [0, -1, i32::MIN] {
        assert!(
            rockserver_client::Capabilities::try_from(CapabilitiesResponse {
                maximum_range_page_keys: value,
                ..valid.clone()
            })
            .is_err()
        );
    }
    for value in [0, -1, i64::MIN] {
        assert!(
            rockserver_client::Capabilities::try_from(CapabilitiesResponse {
                maximum_range_page_bytes: value,
                ..valid.clone()
            })
            .is_err()
        );
    }
    assert!(
        rockserver_client::Capabilities::try_from(CapabilitiesResponse {
            raw_scan_mode: rockserver_client::RawScanMode::Unsupported as i32,
            maximum_range_page_keys: i32::MAX,
            maximum_range_page_bytes: i64::MAX,
            ..valid
        })
        .is_ok()
    );
}

#[test]
fn distributed_backend_rejects_physical_sst_capabilities() {
    let error = rockserver_client::Capabilities::try_from(CapabilitiesResponse {
        backend_mode: rockserver_client::BackendMode::Distributed as i32,
        raw_scan_mode: rockserver_client::RawScanMode::PhysicalSst as i32,
        maximum_range_page_keys: 4_096,
        maximum_range_page_bytes: 8 * 1024 * 1024,
    })
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
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
        budget: Some(RangeBudget {
            max_items: 4_096,
            max_bytes: 8 * 1024 * 1024,
        }),
        context: Some(RequestContext {
            profile: 2,
            timeout_nanos: 123_456,
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
