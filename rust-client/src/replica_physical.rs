//! Explicit node/operator authority, disabled unless the server operator injects it.
//! The caller chooses the channel and qualified target. No workload selection, retargeting
//! or retry is performed. Timeout, cancellation and disconnect end observation only;
//! native flush/compaction may continue. Physical counts are not logical database totals.
use crate::{
    client::{await_bound_call, BoundDeadline, DeadlineInterceptor, PreparedRequest},
    proto,
};
use prost::Message;
use std::{collections::HashSet, time::Duration};
use tonic::{
    service::{interceptor::InterceptedService, Interceptor},
    transport::Channel,
    Request, Status,
};
type Result<T> = std::result::Result<T, Status>;
const MAX_FRAME: usize = 64 * 1024 * 1024;
const MAX_REQUEST: usize = 2 * 1024 * 1024;
const MAX_ITEMS: usize = 65_536;
const MAX_STRING: usize = 65_536;
const MAX_KEY_HEX: usize = 4 * 1024 * 1024;
fn invalid(message: &str) -> Status {
    Status::invalid_argument(message)
}
fn malformed(message: &str) -> Status {
    Status::data_loss(message)
}
fn check(condition: bool, message: &str) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(malformed(message))
    }
}
fn string(value: &str) -> Result<()> {
    check(value.len() <= MAX_STRING, "physical string exceeds bound")
}
fn bounded<M: Message>(value: &M, maximum: usize) -> Result<()> {
    check(
        value.encoded_len() <= maximum,
        "physical frame exceeds bound",
    )
}
fn uuid(value: &[u8]) -> Result<[u8; 16]> {
    value
        .try_into()
        .map_err(|_| malformed("physical UUID must contain 16 bytes"))
}
fn uuid_text(value: &[u8; 16]) -> String {
    let h: String = value.iter().map(|b| format!("{b:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &h[..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..]
    )
}

/// Separate operator timeout, without a customer workload profile. `i64::MAX` is unbounded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplicaPhysicalContext {
    timeout_nanos: i64,
}
impl ReplicaPhysicalContext {
    pub fn new(timeout_nanos: i64) -> Result<Self> {
        if timeout_nanos <= 0 {
            return Err(invalid("operator timeout must be positive"));
        }
        Ok(Self { timeout_nanos })
    }
    pub fn timeout_nanos(self) -> i64 {
        self.timeout_nanos
    }
    fn wire(self) -> proto::ReplicaPhysicalContext {
        proto::ReplicaPhysicalContext {
            timeout_nanos: self.timeout_nanos,
        }
    }
    fn request<T>(self, value: T) -> Result<PreparedRequest<T>> {
        let mut request = Request::new(value);
        let deadline = if self.timeout_nanos == i64::MAX {
            None
        } else {
            let timeout = Duration::from_nanos(self.timeout_nanos as u64);
            let deadline = tokio::time::Instant::now()
                .checked_add(timeout)
                .ok_or_else(|| invalid("operator deadline exceeds clock range"))?;
            request.set_timeout(timeout);
            request.extensions_mut().insert(BoundDeadline(deadline));
            Some(deadline)
        };
        Ok(PreparedRequest { request, deadline })
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ReplicaPhysicalTarget {
    pub node: [u8; 16],
    pub incarnation: [u8; 16],
    pub process: [u8; 16],
    pub pool: [u8; 16],
    pub session: [u8; 16],
    pub column: [u8; 16],
    pub native_column_id: i64,
}
impl ReplicaPhysicalTarget {
    fn valid(&self) -> bool {
        self.native_column_id >= 0
    }
    fn wire(&self) -> Result<proto::ReplicaPhysicalTarget> {
        if !self.valid() {
            return Err(invalid("negative native column identity"));
        }
        Ok(proto::ReplicaPhysicalTarget {
            node: self.node.to_vec(),
            incarnation: self.incarnation.to_vec(),
            process: self.process.to_vec(),
            pool: self.pool.to_vec(),
            session: self.session.to_vec(),
            column: self.column.to_vec(),
            native_column_id: self.native_column_id,
        })
    }
    fn decode(w: proto::ReplicaPhysicalTarget) -> Result<Self> {
        let target = Self {
            node: uuid(&w.node)?,
            incarnation: uuid(&w.incarnation)?,
            process: uuid(&w.process)?,
            pool: uuid(&w.pool)?,
            session: uuid(&w.session)?,
            column: uuid(&w.column)?,
            native_column_id: w.native_column_id,
        };
        check(target.valid(), "negative native column identity")?;
        Ok(target)
    }
    pub fn session_name(&self) -> String {
        format!("{}-{}", uuid_text(&self.session), self.native_column_id)
    }
    fn exact(w: Option<proto::ReplicaPhysicalTarget>, expected: &Self) -> Result<Self> {
        let actual = Self::decode(w.ok_or_else(|| malformed("missing physical target"))?)?;
        check(&actual == expected, "crossed physical target")?;
        Ok(actual)
    }
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReplicaPhysicalScope {
    pub pool: Option<[u8; 16]>,
    pub column: Option<[u8; 16]>,
}
impl ReplicaPhysicalScope {
    fn wire(self) -> proto::ReplicaPhysicalScope {
        proto::ReplicaPhysicalScope {
            pool: self.pool.map(|v| v.to_vec()),
            column: self.column.map(|v| v.to_vec()),
        }
    }
    fn contains(&self, target: &ReplicaPhysicalTarget) -> bool {
        self.pool.is_none_or(|v| v == target.pool) && self.column.is_none_or(|v| v == target.column)
    }
    fn targets(
        &self,
        targets: impl IntoIterator<Item = ReplicaPhysicalTarget>,
    ) -> Result<Vec<ReplicaPhysicalTarget>> {
        let mut result = Vec::new();
        let mut unique = HashSet::new();
        for target in targets {
            check(
                result.len() < MAX_ITEMS && self.contains(&target) && unique.insert(target.clone()),
                "invalid physical target scope/count/duplicate",
            )?;
            result.push(target);
        }
        Ok(result)
    }
}
/// All 29 SST property fields are preserved in `tables`, including every distribution.
#[derive(Clone, Debug)]
pub struct ReplicaPhysicalObservation {
    pub target: ReplicaPhysicalTarget,
    pub estimated_keys: i64,
    pub actual_cf_comparator: String,
    pub tables: proto::ColumnTablePropertiesResponse,
}
impl ReplicaPhysicalObservation {
    fn decode(w: proto::ReplicaPhysicalObservation) -> Result<Self> {
        let target = ReplicaPhysicalTarget::decode(
            w.target
                .ok_or_else(|| malformed("missing observation target"))?,
        )?;
        check(w.estimated_keys >= 0, "negative estimated keys")?;
        string(&w.actual_cf_comparator)?;
        let tables = w
            .tables
            .ok_or_else(|| malformed("missing physical table properties"))?;
        table_properties(&tables)?;
        Ok(Self {
            target,
            estimated_keys: w.estimated_keys,
            actual_cf_comparator: w.actual_cf_comparator,
            tables,
        })
    }
}
fn frequencies<'a>(count: i64, values: impl Iterator<Item = &'a i64>, size: usize) -> Result<()> {
    check(size <= MAX_ITEMS, "too many physical distribution entries")?;
    let mut sum = 0_i64;
    for &value in values {
        check(value > 0, "nonpositive physical distribution frequency")?;
        sum = sum
            .checked_add(value)
            .ok_or_else(|| malformed("physical distribution frequency overflow"))?;
    }
    check(
        sum == count,
        "physical distribution does not match table count",
    )
}
fn table_properties(w: &proto::ColumnTablePropertiesResponse) -> Result<()> {
    check(
        (0..=MAX_ITEMS as i64).contains(&w.table_count),
        "invalid physical table count",
    )?;
    for scalar in [
        w.data_size,
        w.index_size,
        w.index_partitions,
        w.top_level_index_size,
        w.filter_size,
        w.raw_key_size,
        w.raw_value_size,
        w.num_data_blocks,
        w.num_entries,
        w.num_deletions,
        w.num_merge_operands,
        w.num_range_deletions,
        w.slow_compression_estimated_data_size,
        w.fast_compression_estimated_data_size,
        w.oldest_creation_time,
        w.newest_creation_time,
        w.oldest_key_time,
    ] {
        check(scalar >= 0, "negative physical table property")?;
    }
    for map in [
        &w.format_versions,
        &w.fixed_key_lengths,
        &w.index_keys_are_user_keys,
        &w.index_values_are_delta_encoded,
        &w.column_family_ids,
    ] {
        check(
            map.keys().all(|&key| key >= 0),
            "negative physical distribution key",
        )?;
        frequencies(w.table_count, map.values(), map.len())?;
    }
    for map in [
        &w.filter_policies,
        &w.comparators,
        &w.merge_operators,
        &w.prefix_extractors,
        &w.property_collectors,
        &w.compressions,
    ] {
        for key in map.keys() {
            string(key)?;
        }
        frequencies(w.table_count, map.values(), map.len())?;
    }
    Ok(())
}
#[derive(Clone, Debug)]
pub struct ReplicaPhysicalMetadata {
    pub target: ReplicaPhysicalTarget,
    pub metadata: proto::GetSstMetadataResponse,
}
fn basename(value: &str) -> bool {
    let Some(stem) = value.strip_suffix(".sst") else {
        return false;
    };
    (1..=20).contains(&stem.len()) && stem.bytes().all(|b| b.is_ascii_digit())
}
fn filenames(files: &[String], nonempty: bool) -> Result<()> {
    check(
        files.len() <= MAX_ITEMS && (!nonempty || !files.is_empty()),
        "invalid SST file count",
    )?;
    let mut seen = HashSet::new();
    for file in files {
        check(
            basename(file) && seen.insert(file),
            "invalid or duplicate SST basename",
        )?;
    }
    Ok(())
}
fn hex(value: &str) -> bool {
    value.len() <= MAX_KEY_HEX
        && value.len() % 2 == 0
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
impl ReplicaPhysicalMetadata {
    fn decode(
        w: proto::ReplicaPhysicalMetadata,
        target: &ReplicaPhysicalTarget,
        level: i32,
    ) -> Result<Self> {
        bounded(&w, MAX_FRAME)?;
        let target = ReplicaPhysicalTarget::exact(w.target, target)?;
        let metadata = w
            .metadata
            .ok_or_else(|| malformed("missing SST metadata"))?;
        check(
            metadata.column_id == target.native_column_id
                && metadata.session == target.session_name(),
            "crossed SST session/native column",
        )?;
        string(&metadata.column_name)?;
        check(
            metadata.num_levels > 0
                && metadata.base_level >= 0
                && metadata.base_level < metadata.num_levels
                && !metadata.paths.is_empty()
                && metadata.paths.len() <= MAX_ITEMS
                && metadata.files.len() <= MAX_ITEMS,
            "invalid SST metadata dimensions",
        )?;
        for path in &metadata.paths {
            string(path)?;
        }
        let mut seen = HashSet::new();
        for file in &metadata.files {
            check(
                basename(&file.name)
                    && seen.insert(&file.name)
                    && file.level >= 0
                    && file.level < metadata.num_levels
                    && (level == -1 || file.level == level)
                    && file.path_id >= 0
                    && (file.path_id as usize) < metadata.paths.len()
                    && file.size_bytes >= 0
                    && hex(&file.smallest_key_hex)
                    && hex(&file.largest_key_hex)
                    && file.smallest_key_hex <= file.largest_key_hex,
                "invalid SST metadata file",
            )?;
        }
        Ok(Self { target, metadata })
    }
}
#[derive(Clone, Debug)]
pub struct ReplicaPhysicalCompaction {
    pub target: ReplicaPhysicalTarget,
    pub result: proto::CompactFilesResponse,
}
fn compaction_request(
    target: &ReplicaPhysicalTarget,
    r: &proto::CompactFilesRequest,
) -> Result<()> {
    if !target.valid()
        || r.column_id != target.native_column_id
        || r.session != target.session_name()
        || r.level < 1
        || r.output_path_id < 0
        || r.output_file_size_limit <= 0
        || r.max_input_bytes <= 0
        || !(1..=16).contains(&r.max_subcompactions)
    {
        return Err(invalid("invalid qualified selective compaction request"));
    }
    filenames(&r.files, true).map_err(|_| invalid("invalid selective compaction filenames"))?;
    bounded(r, MAX_REQUEST).map_err(|_| invalid("selective compaction request exceeds frame bound"))
}
impl ReplicaPhysicalCompaction {
    fn decode(
        w: proto::ReplicaPhysicalCompaction,
        target: &ReplicaPhysicalTarget,
        request: &proto::CompactFilesRequest,
    ) -> Result<Self> {
        bounded(&w, MAX_FRAME)?;
        let target = ReplicaPhysicalTarget::exact(w.target, target)?;
        let result = w
            .result
            .ok_or_else(|| malformed("missing physical compaction result"))?;
        check(
            result.executed == request.execute
                && result.validated_input_bytes >= 0
                && result.output_bytes >= 0
                && result.elapsed_nanos >= 0,
            "invalid physical compaction result",
        )?;
        filenames(&result.input_files, false)?;
        filenames(&result.output_files, false)?;
        if !result.executed {
            check(
                result.output_files.is_empty()
                    && result.output_bytes == 0
                    && result.input_files.iter().collect::<HashSet<_>>()
                        == request.files.iter().collect::<HashSet<_>>(),
                "crossed selective compaction dry run",
            )?;
        }
        Ok(Self { target, result })
    }
}
/// Default interceptor for operator channels without additional caller metadata.
#[derive(Clone, Copy, Debug, Default)]
pub struct ReplicaPhysicalNoopInterceptor;
impl Interceptor for ReplicaPhysicalNoopInterceptor {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>> {
        Ok(request)
    }
}
/// Combines the shared request deadline with the caller's optional channel interceptor.
#[derive(Clone, Debug)]
pub struct ReplicaPhysicalInterceptor<I> {
    caller: I,
}
impl<I: Interceptor> Interceptor for ReplicaPhysicalInterceptor<I> {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>> {
        let request = DeadlineInterceptor.call(request)?;
        self.caller.call(request)
    }
}
#[derive(Clone, Debug)]
pub struct ReplicaPhysicalClient<I = ReplicaPhysicalNoopInterceptor> {
    client: proto::replica_physical_service_client::ReplicaPhysicalServiceClient<
        InterceptedService<Channel, ReplicaPhysicalInterceptor<I>>,
    >,
    context: ReplicaPhysicalContext,
}
impl ReplicaPhysicalClient {
    /// Construction performs no connection, capability handshake or operator RPC.
    pub fn new(channel: Channel, context: ReplicaPhysicalContext) -> Self {
        Self::with_interceptor(channel, context, ReplicaPhysicalNoopInterceptor)
    }
}
macro_rules! physical_rpc {
    ($s:expr,$method:ident,$message:expr) => {{
        let message = $message;
        bounded(&message, MAX_REQUEST)
            .map_err(|_| invalid("physical request exceeds frame bound"))?;
        let prepared = $s.context.request(message)?;
        let mut client = $s.client.clone();
        let response = await_bound_call(prepared.deadline, client.$method(prepared.request))
            .await?
            .into_inner();
        bounded(&response, MAX_FRAME)?;
        response
    }};
}
impl<I: Interceptor + Clone> ReplicaPhysicalClient<I> {
    pub fn with_interceptor(
        channel: Channel,
        context: ReplicaPhysicalContext,
        interceptor: I,
    ) -> Self {
        Self{client:proto::replica_physical_service_client::ReplicaPhysicalServiceClient::with_interceptor(channel,ReplicaPhysicalInterceptor{caller:interceptor}).max_decoding_message_size(MAX_FRAME).max_encoding_message_size(MAX_REQUEST),context}
    }
    pub fn with_context(&self, context: ReplicaPhysicalContext) -> Self {
        Self {
            client: self.client.clone(),
            context,
        }
    }
    fn scope_request(&self, scope: ReplicaPhysicalScope) -> proto::ReplicaPhysicalScopeRequest {
        proto::ReplicaPhysicalScopeRequest {
            scope: Some(scope.wire()),
            context: Some(self.context.wire()),
        }
    }
    pub async fn discover(
        &self,
        scope: ReplicaPhysicalScope,
    ) -> Result<Vec<ReplicaPhysicalTarget>> {
        let response = physical_rpc!(self, discover, self.scope_request(scope));
        check(
            response.targets.len() <= MAX_ITEMS,
            "too many physical targets",
        )?;
        let targets = response
            .targets
            .into_iter()
            .map(ReplicaPhysicalTarget::decode)
            .collect::<Result<Vec<_>>>()?;
        scope.targets(targets)
    }
    pub async fn observe(
        &self,
        scope: ReplicaPhysicalScope,
    ) -> Result<Vec<ReplicaPhysicalObservation>> {
        let response = physical_rpc!(self, observe, self.scope_request(scope));
        check(
            response.observations.len() <= MAX_ITEMS,
            "too many physical observations",
        )?;
        let observations = response
            .observations
            .into_iter()
            .map(ReplicaPhysicalObservation::decode)
            .collect::<Result<Vec<_>>>()?;
        scope.targets(observations.iter().map(|v| v.target.clone()))?;
        Ok(observations)
    }
    /// Explicit operator flush. Cancellation does not prove native work has stopped.
    pub async fn flush(&self, scope: ReplicaPhysicalScope) -> Result<()> {
        physical_rpc!(self, flush, self.scope_request(scope));
        Ok(())
    }
    /// Explicit operator compaction. Cancellation does not prove native work has stopped.
    pub async fn compact(&self, scope: ReplicaPhysicalScope) -> Result<()> {
        physical_rpc!(self, compact, self.scope_request(scope));
        Ok(())
    }
    pub async fn metadata(
        &self,
        target: &ReplicaPhysicalTarget,
        level: i32,
    ) -> Result<ReplicaPhysicalMetadata> {
        if level < -1 {
            return Err(invalid("invalid SST metadata level"));
        }
        let response = physical_rpc!(
            self,
            metadata,
            proto::ReplicaPhysicalMetadataRequest {
                target: Some(target.wire()?),
                level,
                context: Some(self.context.wire())
            }
        );
        ReplicaPhysicalMetadata::decode(response, target, level)
    }
    pub async fn compact_files(
        &self,
        target: &ReplicaPhysicalTarget,
        request: &proto::CompactFilesRequest,
    ) -> Result<ReplicaPhysicalCompaction> {
        compaction_request(target, request)?;
        let response = physical_rpc!(
            self,
            compact_files,
            proto::ReplicaPhysicalCompactRequest {
                target: Some(target.wire()?),
                request: Some(request.clone()),
                context: Some(self.context.wire())
            }
        );
        ReplicaPhysicalCompaction::decode(response, target, request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn target() -> ReplicaPhysicalTarget {
        ReplicaPhysicalTarget {
            node: [1; 16],
            incarnation: [2; 16],
            process: [3; 16],
            pool: [4; 16],
            session: [5; 16],
            column: [6; 16],
            native_column_id: 7,
        }
    }
    fn tables() -> proto::ColumnTablePropertiesResponse {
        let mut w = proto::ColumnTablePropertiesResponse {
            table_count: 1,
            ..Default::default()
        };
        for map in [
            &mut w.format_versions,
            &mut w.fixed_key_lengths,
            &mut w.index_keys_are_user_keys,
            &mut w.index_values_are_delta_encoded,
            &mut w.column_family_ids,
        ] {
            map.insert(0, 1);
        }
        for map in [
            &mut w.filter_policies,
            &mut w.comparators,
            &mut w.merge_operators,
            &mut w.prefix_extractors,
            &mut w.property_collectors,
            &mut w.compressions,
        ] {
            map.insert(String::new(), 1);
        }
        w
    }
    fn metadata() -> proto::ReplicaPhysicalMetadata {
        let t = target();
        proto::ReplicaPhysicalMetadata {
            target: Some(t.wire().unwrap()),
            metadata: Some(proto::GetSstMetadataResponse {
                session: t.session_name(),
                column_id: 7,
                column_name: "column".into(),
                num_levels: 7,
                base_level: 1,
                paths: vec!["/storage/pool".into()],
                files: vec![proto::SstFileMetadata {
                    name: "12.sst".into(),
                    level: 1,
                    path_id: 0,
                    size_bytes: 42,
                    smallest_key_hex: "00".into(),
                    largest_key_hex: "ff".into(),
                    being_compacted: false,
                }],
            }),
        }
    }
    fn compact() -> proto::CompactFilesRequest {
        proto::CompactFilesRequest {
            column_id: 7,
            session: target().session_name(),
            files: vec!["12.sst".into()],
            level: 1,
            output_path_id: 0,
            output_file_size_limit: 1,
            max_input_bytes: 42,
            max_subcompactions: 1,
            execute: false,
        }
    }
    #[test]
    fn target_codec_and_scope_are_fully_qualified() {
        let t = target();
        assert_eq!(ReplicaPhysicalTarget::decode(t.wire().unwrap()).unwrap(), t);
        assert_eq!(t.session_name(), "05050505-0505-0505-0505-050505050505-7");
        let mut w = t.wire().unwrap();
        w.process.pop();
        assert!(ReplicaPhysicalTarget::decode(w).is_err());
        let mut w = t.wire().unwrap();
        w.native_column_id = -1;
        assert!(ReplicaPhysicalTarget::decode(w).is_err());
        let mut crossed = t.clone();
        crossed.incarnation[0] ^= 1;
        assert!(ReplicaPhysicalTarget::exact(Some(crossed.wire().unwrap()), &t).is_err());
        let scope = ReplicaPhysicalScope {
            pool: Some(t.pool),
            column: Some(t.column),
        };
        assert!(scope.targets(vec![t.clone()]).is_ok());
        assert!(scope.targets(vec![t.clone(), t.clone()]).is_err());
        crossed = t;
        crossed.pool[0] ^= 1;
        assert!(scope.targets(vec![crossed]).is_err());
    }
    #[test]
    fn table_properties_preserve_and_validate_all_distributions() {
        let valid = tables();
        table_properties(&valid).unwrap();
        let w = proto::ReplicaPhysicalObservation {
            target: Some(target().wire().unwrap()),
            estimated_keys: 4,
            actual_cf_comparator: "leveldb.BytewiseComparator".into(),
            tables: Some(valid.clone()),
        };
        let encoded = w.tables.as_ref().unwrap().encode_to_vec();
        let decoded = ReplicaPhysicalObservation::decode(w).unwrap();
        assert_eq!(decoded.tables.encode_to_vec(), encoded);
        let mut bad = valid.clone();
        bad.raw_value_size = -1;
        assert!(table_properties(&bad).is_err());
        let mut bad = valid.clone();
        bad.compressions.insert("zstd".into(), 0);
        assert!(table_properties(&bad).is_err());
        let mut bad = valid.clone();
        bad.column_family_ids.insert(-1, 1);
        assert!(table_properties(&bad).is_err());
        let mut bad = valid;
        bad.comparators.clear();
        assert!(table_properties(&bad).is_err());
        assert!(table_properties(&proto::ColumnTablePropertiesResponse::default()).is_ok());
    }
    #[test]
    fn metadata_requires_exact_target_session_and_native_id() {
        let valid = metadata();
        assert_eq!(
            ReplicaPhysicalMetadata::decode(valid.clone(), &target(), 1)
                .unwrap()
                .metadata
                .files
                .len(),
            1
        );
        let mut bad = valid.clone();
        bad.metadata.as_mut().unwrap().session = "stale-session".into();
        assert!(ReplicaPhysicalMetadata::decode(bad, &target(), 1).is_err());
        let mut bad = valid.clone();
        bad.metadata.as_mut().unwrap().column_id = 8;
        assert!(ReplicaPhysicalMetadata::decode(bad, &target(), 1).is_err());
        let mut bad = valid.clone();
        bad.target.as_mut().unwrap().node[0] ^= 1;
        assert!(ReplicaPhysicalMetadata::decode(bad, &target(), 1).is_err());
        let mut bad = valid.clone();
        bad.metadata.as_mut().unwrap().files[0].path_id = 1;
        assert!(ReplicaPhysicalMetadata::decode(bad, &target(), 1).is_err());
        let mut bad = valid.clone();
        bad.metadata.as_mut().unwrap().files[0].smallest_key_hex = "FF".into();
        assert!(ReplicaPhysicalMetadata::decode(bad, &target(), 1).is_err());
        let mut bad = valid.clone();
        bad.metadata.as_mut().unwrap().files[0].smallest_key_hex = "ffff".into();
        assert!(ReplicaPhysicalMetadata::decode(bad, &target(), 1).is_err());
        assert!(ReplicaPhysicalMetadata::decode(valid, &target(), 2).is_err());
    }
    #[test]
    fn selective_compaction_validates_before_copy_and_binds_dry_run() {
        let request = compact();
        compaction_request(&target(), &request).unwrap();
        let mut bad = request.clone();
        bad.files[0] = "../12.sst".into();
        assert!(compaction_request(&target(), &bad).is_err());
        let mut bad = request.clone();
        bad.files.push("12.sst".into());
        assert!(compaction_request(&target(), &bad).is_err());
        let mut bad = request.clone();
        bad.max_subcompactions = 17;
        assert!(compaction_request(&target(), &bad).is_err());
        let mut bad = request.clone();
        bad.session = "other-session".into();
        assert!(compaction_request(&target(), &bad).is_err());
        let valid = proto::ReplicaPhysicalCompaction {
            target: Some(target().wire().unwrap()),
            result: Some(proto::CompactFilesResponse {
                executed: false,
                input_files: request.files.clone(),
                output_files: vec![],
                validated_input_bytes: 42,
                output_bytes: 0,
                elapsed_nanos: 1,
            }),
        };
        ReplicaPhysicalCompaction::decode(valid.clone(), &target(), &request).unwrap();
        let mut bad = valid.clone();
        bad.result.as_mut().unwrap().input_files = vec!["13.sst".into()];
        assert!(ReplicaPhysicalCompaction::decode(bad, &target(), &request).is_err());
        let mut bad = valid;
        bad.result.as_mut().unwrap().executed = true;
        assert!(ReplicaPhysicalCompaction::decode(bad, &target(), &request).is_err());
    }
    #[test]
    fn wire_cardinalities_strings_and_request_bounds() {
        assert!(string(&"x".repeat(MAX_STRING + 1)).is_err());
        assert!(!hex("0"));
        assert!(!basename("12.sst/"));
        assert!(!basename("123456789012345678901.sst"));
        let huge = proto::ReplicaPhysicalCompactRequest {
            request: Some(proto::CompactFilesRequest {
                files: vec!["12.sst".repeat(MAX_REQUEST)],
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(bounded(&huge, MAX_REQUEST).is_err());
        assert!(frequencies(1, [1_i64].iter(), MAX_ITEMS + 1).is_err());
        assert!(frequencies(1, [-1_i64].iter(), 1).is_err());
    }
    #[tokio::test]
    async fn operator_context_has_one_deadline_without_workload_selection() {
        assert!(ReplicaPhysicalContext::new(0).is_err());
        assert!(ReplicaPhysicalContext::new(-1).is_err());
        let unbounded = ReplicaPhysicalContext::new(i64::MAX)
            .unwrap()
            .request(())
            .unwrap();
        assert!(unbounded.deadline.is_none());
        assert!(unbounded.request.metadata().get("grpc-timeout").is_none());
        let prepared = ReplicaPhysicalContext::new(1_000_000_000)
            .unwrap()
            .request(())
            .unwrap();
        assert_eq!(
            prepared.deadline,
            prepared
                .request
                .extensions()
                .get::<BoundDeadline>()
                .map(|b| b.0)
        );
        assert!(prepared.request.metadata().get("grpc-timeout").is_some());
        let mut request = Request::new(());
        request.extensions_mut().insert(BoundDeadline(
            tokio::time::Instant::now() - Duration::from_millis(1),
        ));
        let mut interceptor = ReplicaPhysicalInterceptor {
            caller: ReplicaPhysicalNoopInterceptor,
        };
        assert_eq!(
            interceptor.call(request).unwrap_err().code(),
            tonic::Code::DeadlineExceeded
        );
    }
    // Type-check all operator methods and constructors without creating a channel or running RPCs.
    #[allow(dead_code)]
    async fn public_operator_workflow(channel: Channel) -> Result<()> {
        let context = ReplicaPhysicalContext::new(1_000_000_000)?;
        let client = ReplicaPhysicalClient::new(channel.clone(), context).with_context(context);
        let authenticated =
            ReplicaPhysicalClient::with_interceptor(channel, context, |mut request: Request<()>| {
                request
                    .metadata_mut()
                    .insert("operator", "explicit".parse().unwrap());
                Ok(request)
            });
        let scope = ReplicaPhysicalScope::default();
        let _ = client.discover(scope).await?;
        let _ = authenticated.observe(scope).await?;
        client.flush(scope).await?;
        client.compact(scope).await?;
        let _ = client.metadata(&target(), -1).await?;
        let _ = client.compact_files(&target(), &compact()).await?;
        Ok(())
    }
}
