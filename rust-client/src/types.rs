use std::time::Duration;
use crate::proto;

// Re-export proto types that are part of the public API
pub use crate::proto::{
    ColumnHashType, Operation, PutBatchMode, MergeBatchMode,
    Kv, KvBatch, Delta, Previous, Changed, PreviousPresence, Merged, UpdateBegin,
    FirstAndLast, CdcEvent,
};

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
            variable_tail_keys: val.variable_tail_keys.into_iter().map(|x| x as i32).collect(),
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
            variable_tail_keys: val.variable_tail_keys.into_iter()
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
