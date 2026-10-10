use crate::proto;
use std::time::Duration;

// Re-export proto types that are part of the public API
pub use crate::proto::{
    BackendMode, CdcEvent, CdcMode, Changed, ColumnHashType, Delta, FirstAndLast, Kv, KvBatch,
    MergeBatchMode, Merged, Operation, Previous, PreviousPresence, PutBatchMode, RawScanMode,
    RequestContext, UpdateBegin, WorkloadProfile,
};

/// Backend behavior and bounded-page limits validated during the handshake.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Capabilities {
    pub backend_mode: BackendMode,
    pub raw_scan_mode: RawScanMode,
    pub cdc_mode: CdcMode,
    pub maximum_range_page_keys: i32,
    pub maximum_range_page_bytes: i64,
}

impl TryFrom<proto::CapabilitiesResponse> for Capabilities {
    type Error = tonic::Status;

    fn try_from(response: proto::CapabilitiesResponse) -> Result<Self, Self::Error> {
        let backend_mode = BackendMode::try_from(response.backend_mode)
            .map_err(|_| tonic::Status::failed_precondition("unknown backend mode"))?;
        if !matches!(
            backend_mode,
            BackendMode::Standalone | BackendMode::Distributed
        ) {
            return Err(tonic::Status::failed_precondition(
                "backend mode is required",
            ));
        }
        let raw_scan_mode = RawScanMode::try_from(response.raw_scan_mode)
            .map_err(|_| tonic::Status::failed_precondition("unknown raw scan mode"))?;
        if !matches!(
            raw_scan_mode,
            RawScanMode::Unsupported | RawScanMode::PhysicalSst | RawScanMode::ImmutableExportJobs
        ) {
            return Err(tonic::Status::failed_precondition(
                "raw scan mode is required",
            ));
        }
        if backend_mode == BackendMode::Distributed && raw_scan_mode == RawScanMode::PhysicalSst {
            return Err(tonic::Status::failed_precondition(
                "distributed backends cannot expose physical SST raw scans",
            ));
        }
        let cdc_mode = CdcMode::try_from(response.cdc_mode)
            .map_err(|_| tonic::Status::failed_precondition("unknown CDC mode"))?;
        if !matches!(
            cdc_mode,
            CdcMode::CdcUnsupported | CdcMode::Scalar | CdcMode::VectorSubscriptions
        ) || backend_mode == BackendMode::Distributed && cdc_mode == CdcMode::Scalar
            || backend_mode == BackendMode::Standalone && cdc_mode == CdcMode::VectorSubscriptions
        {
            return Err(tonic::Status::failed_precondition(
                "invalid backend CDC mode",
            ));
        }
        if response.maximum_range_page_keys <= 0 || response.maximum_range_page_bytes <= 0 {
            return Err(tonic::Status::failed_precondition(
                "bounded range page limits must be positive",
            ));
        }
        Ok(Self {
            backend_mode,
            raw_scan_mode,
            cdc_mode,
            maximum_range_page_keys: response.maximum_range_page_keys,
            maximum_range_page_bytes: response.maximum_range_page_bytes,
        })
    }
}

impl Capabilities {
    pub fn maximum_vector_cdc_records(&self) -> i32 {
        if self.cdc_mode == CdcMode::VectorSubscriptions {
            crate::cdc::MAX_RECORDS
        } else {
            0
        }
    }
    pub fn maximum_vector_cdc_source_bytes(&self) -> i32 {
        if self.cdc_mode == CdcMode::VectorSubscriptions {
            crate::cdc::MAX_POLL_BYTES
        } else {
            0
        }
    }
    pub fn maximum_vector_cdc_batch_bytes(&self) -> usize {
        if self.cdc_mode == CdcMode::VectorSubscriptions {
            crate::cdc::MAX_BATCH_BYTES
        } else {
            0
        }
    }
    pub(crate) fn with_transport_limit(
        mut self,
        maximum_message_bytes: usize,
    ) -> Result<Self, tonic::Status> {
        let maximum_page_bytes = maximum_message_bytes
            .checked_sub(64 * 1024)
            .and_then(|available| i64::try_from(available / 2).ok())
            .filter(|maximum| *maximum > 0)
            .ok_or_else(|| {
                tonic::Status::failed_precondition(
                    "transport limit must permit a positive bounded range page",
                )
            })?;
        self.maximum_range_page_bytes = self.maximum_range_page_bytes.min(maximum_page_bytes);
        Ok(self)
    }

    pub(crate) fn validate_range_budget(
        &self,
        budget: &proto::RangeBudget,
    ) -> Result<(), tonic::Status> {
        if budget.max_items <= 0 || budget.max_items > self.maximum_range_page_keys {
            return Err(tonic::Status::invalid_argument(
                "range max_items must be positive and within the backend limit",
            ));
        }
        if budget.max_bytes <= 0 || budget.max_bytes > self.maximum_range_page_bytes {
            return Err(tonic::Status::invalid_argument(
                "range max_bytes must be positive and within the backend limit",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod capability_tests {
    use super::*;

    #[test]
    fn transport_limit_clamps_page_budget_with_response_headroom() {
        let capabilities = Capabilities {
            backend_mode: BackendMode::Standalone,
            raw_scan_mode: RawScanMode::PhysicalSst,
            cdc_mode: CdcMode::Scalar,
            maximum_range_page_keys: 4_096,
            maximum_range_page_bytes: 8 * 1024 * 1024,
        };
        let clamped = capabilities.with_transport_limit(4 * 1024 * 1024).unwrap();
        assert_eq!(
            clamped.maximum_range_page_bytes,
            (4 * 1024 * 1024 - 64 * 1024) / 2
        );
        assert_eq!(
            clamped.maximum_range_page_keys,
            capabilities.maximum_range_page_keys
        );
        assert_eq!(
            capabilities.with_transport_limit(64 * 1024 * 1024).unwrap(),
            capabilities
        );
        assert_eq!(
            Capabilities {
                maximum_range_page_bytes: i64::MAX,
                ..capabilities
            }
            .with_transport_limit(64 * 1024 * 1024)
            .unwrap()
            .maximum_range_page_bytes,
            (64 * 1024 * 1024 - 64 * 1024) / 2
        );
        for maximum in [0, 64 * 1024, 64 * 1024 + 1] {
            assert_eq!(
                capabilities
                    .with_transport_limit(maximum)
                    .unwrap_err()
                    .code(),
                tonic::Code::FailedPrecondition
            );
        }
    }

    #[test]
    fn range_budget_respects_signed_limits_and_advertised_maxima() {
        let capabilities = Capabilities {
            backend_mode: BackendMode::Standalone,
            raw_scan_mode: RawScanMode::PhysicalSst,
            cdc_mode: CdcMode::Scalar,
            maximum_range_page_keys: 4_096,
            maximum_range_page_bytes: 8 * 1024 * 1024,
        };
        for (max_items, max_bytes) in [(1, 1), (4_096, 8 * 1024 * 1024)] {
            assert!(capabilities
                .validate_range_budget(&proto::RangeBudget {
                    max_items,
                    max_bytes
                })
                .is_ok());
        }
        for (max_items, max_bytes) in [
            (0, 1),
            (-1, 1),
            (i32::MIN, 1),
            (4_097, 1),
            (i32::MAX, 1),
            (1, 0),
            (1, -1),
            (1, i64::MIN),
            (1, 8 * 1024 * 1024 + 1),
            (1, i64::MAX),
        ] {
            assert_eq!(
                capabilities
                    .validate_range_budget(&proto::RangeBudget {
                        max_items,
                        max_bytes
                    })
                    .unwrap_err()
                    .code(),
                tonic::Code::InvalidArgument
            );
        }
    }
}

/// Opaque, database-scoped identity of an immutable RocksDB SST file.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RawSstToken(String);

impl RawSstToken {
    pub fn new(value: String) -> Result<Self, &'static str> {
        let without_slash = value.strip_prefix('/').unwrap_or(&value);
        let digits = without_slash
            .strip_suffix(".sst")
            .ok_or("raw SST token must end in .sst")?;
        if !(6..=20).contains(&digits.len())
            || (digits.len() > 6 && digits.starts_with('0'))
            || !digits.bytes().all(|byte| byte.is_ascii_digit())
            || digits
                .parse::<u64>()
                .ok()
                .filter(|number| *number != 0)
                .is_none()
        {
            return Err("raw SST token must be a canonical RocksDB table filename");
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

/// One resumable raw-scan event. A completion token is safe to checkpoint only
/// after the event carrying it has been consumed successfully.
#[derive(Clone, Debug, PartialEq)]
pub enum RawScanEvent {
    Batch {
        batch: KvBatch,
        completed_sst_token: Option<RawSstToken>,
    },
    SstCompleted(RawSstToken),
}

fn workload_profile_wire_value(profile: WorkloadProfile) -> i32 {
    match profile {
        WorkloadProfile::Unspecified => 0,
        WorkloadProfile::Control => 1,
        WorkloadProfile::Latency => 2,
        WorkloadProfile::Analytical => 3,
        WorkloadProfile::Ingest => 4,
        WorkloadProfile::Cdc => 5,
        WorkloadProfile::Batch => 6,
        WorkloadProfile::PhysicalMaintenance => 7,
    }
}

impl RequestContext {
    pub const NO_TIMEOUT: i64 = i64::MAX;

    /// Create a mandatory caller context. Protected profiles are rejected by Rockserver.
    pub fn for_profile(profile: WorkloadProfile, timeout: Option<Duration>) -> Self {
        let profile = workload_profile_wire_value(profile);
        assert!(
            matches!(profile, 2 | 3 | 4 | 6),
            "workload profile is owned by Rockserver"
        );
        let timeout_nanos = timeout.map_or(Self::NO_TIMEOUT, |duration| {
            assert!(!duration.is_zero(), "timeout must be positive");
            duration.as_nanos().min((i64::MAX - 1) as u128) as i64
        });
        assert!(
            profile != 2 || timeout_nanos != Self::NO_TIMEOUT,
            "LATENCY requires a finite timeout"
        );
        Self {
            profile,
            timeout_nanos,
        }
    }

    pub fn latency(timeout: Duration) -> Self {
        Self::for_profile(WorkloadProfile::Latency, Some(timeout))
    }

    pub fn analytical() -> Self {
        Self::for_profile(WorkloadProfile::Analytical, None)
    }

    pub fn analytical_with_timeout(timeout: Duration) -> Self {
        Self::for_profile(WorkloadProfile::Analytical, Some(timeout))
    }

    pub fn ingest() -> Self {
        Self::for_profile(WorkloadProfile::Ingest, None)
    }

    pub fn ingest_with_timeout(timeout: Duration) -> Self {
        Self::for_profile(WorkloadProfile::Ingest, Some(timeout))
    }

    pub fn batch() -> Self {
        Self::for_profile(WorkloadProfile::Batch, None)
    }

    pub fn batch_with_timeout(timeout: Duration) -> Self {
        Self::for_profile(WorkloadProfile::Batch, Some(timeout))
    }
}

/// Schema definition for a column in RockServer.
///
/// This struct defines the key structure (fixed and variable parts) and whether the column stores a value.
/// It also handles optional merge operator configuration.
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnSchema {
    /// List of fixed-length key definitions.
    pub fixed_keys: Vec<i32>,
    /// List of variable-length tail key definitions.
    pub variable_tail_keys: Vec<ColumnHashType>,
    /// Indicates if the column stores a value associated with the key.
    pub has_value: bool,
    /// Optional name of the merge operator to use.
    pub merge_operator_name: Option<String>,
    /// Optional version of the merge operator.
    pub merge_operator_version: Option<i64>,
}

impl From<ColumnSchema> for proto::ColumnSchema {
    fn from(val: ColumnSchema) -> Self {
        proto::ColumnSchema {
            fixed_keys: val.fixed_keys,
            variable_tail_keys: val
                .variable_tail_keys
                .into_iter()
                .map(|x| x as i32)
                .collect(),
            has_value: val.has_value,
            merge_operator_name: val.merge_operator_name,
            merge_operator_version: val.merge_operator_version,
            // JAVA-only feature, explicitly excluded in Rust client
            merge_operator_class: None,
        }
    }
}

impl From<proto::ColumnSchema> for ColumnSchema {
    fn from(val: proto::ColumnSchema) -> Self {
        ColumnSchema {
            fixed_keys: val.fixed_keys,
            variable_tail_keys: val
                .variable_tail_keys
                .into_iter()
                .filter_map(|x| ColumnHashType::try_from(x).ok())
                .collect(),
            has_value: val.has_value,
            merge_operator_name: val.merge_operator_name,
            merge_operator_version: val.merge_operator_version,
        }
    }
}

/// Represents a Column in RockServer.
#[derive(Clone, Debug, PartialEq)]
pub struct Column {
    /// The unique name of the column.
    pub name: String,
    /// The schema definition of the column.
    pub schema: Option<ColumnSchema>,
}

impl From<proto::Column> for Column {
    fn from(val: proto::Column) -> Self {
        Column {
            name: val.name,
            schema: val.schema.map(|s| s.into()),
        }
    }
}

impl From<Column> for proto::Column {
    fn from(val: Column) -> Self {
        proto::Column {
            name: val.name,
            schema: val.schema.map(|s| s.into()),
        }
    }
}

/// Configuration options for the CDC (Change Data Capture) stream.
#[derive(Clone, Debug)]
pub struct CdcStreamOptions {
    /// The sequence number to start streaming from. If None, starts from the beginning or handled by server default.
    pub from_seq: Option<i64>,
    /// The maximum number of events to fetch in a single poll request.
    pub batch_size: i64,
    /// The delay to wait when no new events are available before polling again.
    pub idle_delay: Duration,
    /// Strategy for committing processed offsets back to the server.
    pub commit_mode: CdcCommitMode,
}

/// Strategies for committing CDC offsets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CdcCommitMode {
    /// Commit the offset after every successfully processed event. Slowest but safest.
    PerEvent,
    /// Commit the offset after processing a batch of events.
    Batch,
    /// Do not commit offsets automatically.
    None,
}

/// Atomic precondition for creating or updating a CDC subscription.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CdcCreatePrecondition {
    /// Require that the subscription metadata does not exist.
    Absent,
    /// Require that the subscription exists at exactly this durable checkpoint.
    LastCommitted(i64),
}

/// Typed failure returned by checked CDC operations.
#[derive(Debug)]
pub enum CdcError {
    /// The subscription was absent, present, or at a checkpoint different from the requested precondition.
    SubscriptionChanged(tonic::Status),
    /// The subscription metadata was deleted or never created.
    SubscriptionNotFound(tonic::Status),
    /// Any other transport or server failure.
    Rpc(tonic::Status),
}

impl CdcError {
    pub fn status(&self) -> &tonic::Status {
        match self {
            Self::SubscriptionChanged(status)
            | Self::SubscriptionNotFound(status)
            | Self::Rpc(status) => status,
        }
    }

    pub fn into_status(self) -> tonic::Status {
        match self {
            Self::SubscriptionChanged(status)
            | Self::SubscriptionNotFound(status)
            | Self::Rpc(status) => status,
        }
    }
}

impl std::fmt::Display for CdcError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.status().fmt(formatter)
    }
}

impl std::error::Error for CdcError {}

impl From<tonic::Status> for CdcError {
    fn from(status: tonic::Status) -> Self {
        const SUBSCRIPTION_CHANGED: &str = "RocksDBError: [uid:CDC_SUBSCRIPTION_CHANGED]";
        const SUBSCRIPTION_NOT_FOUND: &str = "RocksDBError: [uid:CDC_SUBSCRIPTION_NOT_FOUND]";
        if status.message().starts_with(SUBSCRIPTION_CHANGED) {
            Self::SubscriptionChanged(status)
        } else if status.message().starts_with(SUBSCRIPTION_NOT_FOUND) {
            Self::SubscriptionNotFound(status)
        } else {
            Self::Rpc(status)
        }
    }
}

#[cfg(test)]
mod cdc_error_tests {
    use super::CdcError;

    #[test]
    fn classifies_subscription_not_found() {
        let status = tonic::Status::not_found(
            "RocksDBError: [uid:CDC_SUBSCRIPTION_NOT_FOUND] CDC subscription not found: missing",
        );

        assert!(matches!(
            CdcError::from(status),
            CdcError::SubscriptionNotFound(_)
        ));
    }

    #[test]
    fn leaves_unrelated_statuses_as_rpc_errors() {
        assert!(matches!(
            CdcError::from(tonic::Status::unavailable("temporarily unavailable")),
            CdcError::Rpc(_)
        ));
    }
}

impl Default for CdcStreamOptions {
    fn default() -> Self {
        Self {
            from_seq: None,
            batch_size: 1000,
            idle_delay: Duration::from_millis(500),
            commit_mode: CdcCommitMode::Batch,
        }
    }
}
