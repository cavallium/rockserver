use crate::proto;
use std::time::Duration;

// Re-export proto types that are part of the public API
pub use crate::proto::{
    Changed, ColumnHashType, Delta, FirstAndLast, Kv, KvBatch, MergeBatchMode, Merged, Operation,
    Previous, PreviousPresence, PutBatchMode, RequestContext, UpdateBegin, WorkloadProfile,
    ClientRequestId,
};

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
            workload_contract_version: crate::REQUIRED_WORKLOAD_CONTRACT_VERSION,
            timeout_nanos,
            request_id: None,
        }
    }

    /// This context with the number of a write of a client session (see
    /// `RockserverClient::open_client_session`), on a replicated server that advertises `client_sessions`.
    /// Every copy of one write must carry the same `session_id` and `sequence`: the server applies it at most
    /// once and answers every copy with the first result. `completed_below` tells the server that every write
    /// of the session numbered below it has its final answer and is never sent again. A write staged in a
    /// transaction must not be numbered. Answers to watch for: gRPC `UNKNOWN` (`OUTCOME_UNKNOWN`: the write may
    /// or may not have been applied; send the same numbered write again to learn which) and
    /// `FAILED_PRECONDITION` with `SESSION_EXPIRED` in its message (the session is gone; a first copy was not
    /// applied and can be sent again under a new session, a later copy's outcome stays unknown).
    pub fn with_request_id(mut self, session_id: i64, sequence: i64, completed_below: i64) -> Self {
        self.request_id = Some(ClientRequestId {
            session_id,
            sequence,
            completed_below,
        });
        self
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
