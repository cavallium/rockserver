//! Bounded immutable export jobs. IDs are network-order UUID bytes; metadata contains no paths.
use crate::{export_proto as wire, RequestContext, Result};
use prost::Message;
use std::{collections::HashSet, future::Future, time::Duration};
use tonic::service::{interceptor::InterceptedService, Interceptor};
use tonic::{transport::Channel, Request, Status};

pub const MAX_COLUMNS: usize = 256;
pub const MAX_CHUNKS: i32 = 65536;
pub const MAX_BOUND_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_MANIFEST_ITEMS: i32 = 128;
pub const MAX_MANIFEST_BYTES: i32 = 4 * 1024 * 1024;
pub const MIN_PAGE_BYTES: i32 = 64 * 1024;
pub const MAX_FILE_PAGE_BYTES: i32 = 1024 * 1024;
pub const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
pub type ExportId = [u8; 16];
pub use wire::{ExportPhase as Phase, ExportRawScanMode as RawScanMode};

#[derive(Clone, Debug)]
pub struct Limits {
    pub maximum_chunks: i32,
    pub maximum_journal_bytes: i64,
    pub maximum_disk_bytes: i64,
    pub maximum_rows: i32,
    pub maximum_payload_bytes: i64,
    pub maximum_file_bytes: i64,
    pub maximum_chunk_duration: Duration,
    pub snapshot_ttl: Duration,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            maximum_chunks: 4096,
            maximum_journal_bytes: 64 * 1024 * 1024,
            maximum_disk_bytes: 1024 * 1024 * 1024,
            maximum_rows: 65536,
            maximum_payload_bytes: 16 * 1024 * 1024,
            maximum_file_bytes: 32 * 1024 * 1024,
            maximum_chunk_duration: Duration::from_secs(30),
            snapshot_ttl: Duration::from_secs(60),
        }
    }
}
impl Limits {
    fn encode(&self) -> Result<wire::ExportLimits> {
        let nanos = self.maximum_chunk_duration.as_nanos();
        let millis = self.snapshot_ttl.as_millis();
        if !(1..=MAX_CHUNKS).contains(&self.maximum_chunks)
            || !(2 * (MAX_MANIFEST_BYTES as i64 + 4096) + 1024..=64 * 1024 * 1024)
                .contains(&self.maximum_journal_bytes)
            || self.maximum_rows < 1
            || self.maximum_payload_bytes < 1
            || self.maximum_file_bytes < 1
            || self.maximum_disk_bytes < self.maximum_file_bytes
            || nanos == 0
            || nanos > i64::MAX as u128
            || !(1..=60000).contains(&millis)
            || self.snapshot_ttl.as_nanos() != millis * 1_000_000
        {
            return Err(invalid("invalid export limits"));
        }
        Ok(wire::ExportLimits {
            maximum_chunks: self.maximum_chunks,
            maximum_journal_bytes: self.maximum_journal_bytes,
            maximum_disk_bytes: self.maximum_disk_bytes,
            maximum_rows: self.maximum_rows,
            maximum_payload_bytes: self.maximum_payload_bytes,
            maximum_file_bytes: self.maximum_file_bytes,
            maximum_chunk_duration_nanos: nanos as i64,
            snapshot_ttl_millis: millis as i64,
        })
    }
}
#[derive(Clone, Debug)]
pub struct Create {
    pub request_id: ExportId,
    pub columns: Vec<i64>,
    pub limits: Limits,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExportStatus {
    pub job_id: ExportId,
    pub phase: Phase,
    pub columns: Vec<i64>,
    pub participant_count: i32,
    pub chunk_count: i32,
    pub acknowledged_count: i32,
    pub file_bytes: i64,
    pub cleanup_pending: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ChunkId {
    pub job_id: ExportId,
    pub group_id: ExportId,
    pub range_lineage: ExportId,
    pub range_epoch: i64,
    pub snapshot_term: i64,
    pub snapshot_index: i64,
    pub ordinal: i64,
    pub content_hash: [u8; 32],
}
impl ChunkId {
    fn validate(&self) -> Result<()> {
        if self.range_epoch < 0
            || self.snapshot_term < 1
            || self.snapshot_index < 0
            || self.ordinal < 0
        {
            return Err(invalid("invalid export chunk identity"));
        }
        Ok(())
    }
    fn encode(&self) -> Result<wire::ExportChunkId> {
        self.validate()?;
        Ok(wire::ExportChunkId {
            job_id: self.job_id.to_vec(),
            group_id: self.group_id.to_vec(),
            range_lineage: self.range_lineage.to_vec(),
            range_epoch: self.range_epoch,
            snapshot_term: self.snapshot_term,
            snapshot_index: self.snapshot_index,
            ordinal: self.ordinal,
            content_hash: self.content_hash.to_vec(),
        })
    }
}
impl TryFrom<wire::ExportChunkId> for ChunkId {
    type Error = Status;
    fn try_from(v: wire::ExportChunkId) -> Result<Self> {
        let id = Self {
            job_id: uuid(v.job_id)?,
            group_id: uuid(v.group_id)?,
            range_lineage: uuid(v.range_lineage)?,
            range_epoch: v.range_epoch,
            snapshot_term: v.snapshot_term,
            snapshot_index: v.snapshot_index,
            ordinal: v.ordinal,
            content_hash: v
                .content_hash
                .try_into()
                .map_err(|_| invalid("invalid content hash"))?,
        };
        id.validate()?;
        Ok(id)
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Chunk {
    pub id: ChunkId,
    pub column_id: i64,
    pub lower_inclusive: Option<Vec<u8>>,
    pub upper_exclusive: Option<Vec<u8>>,
    pub rows: i64,
    pub payload_bytes: i64,
    pub file_bytes: i64,
    pub has_more: bool,
}
impl TryFrom<wire::ExportChunk> for Chunk {
    type Error = Status;
    fn try_from(v: wire::ExportChunk) -> Result<Self> {
        let bounds = v.lower_inclusive.as_ref().map_or(0, Vec::len)
            + v.upper_exclusive.as_ref().map_or(0, Vec::len);
        if bounds > MAX_BOUND_BYTES
            || v.rows < 0
            || v.payload_bytes < 0
            || v.file_bytes < 0
            || v.rows == 0 && (v.payload_bytes != 0 || v.file_bytes != 0 || v.has_more)
            || v.rows > 0 && v.file_bytes == 0
            || v.has_more && v.upper_exclusive.is_none()
            || matches!((&v.lower_inclusive, &v.upper_exclusive), (Some(a), Some(b)) if a >= b)
        {
            return Err(invalid("invalid export chunk"));
        }
        Ok(Self {
            id: v
                .id
                .ok_or_else(|| invalid("missing chunk identity"))?
                .try_into()?,
            column_id: v.column_id,
            lower_inclusive: v.lower_inclusive,
            upper_exclusive: v.upper_exclusive,
            rows: v.rows,
            payload_bytes: v.payload_bytes,
            file_bytes: v.file_bytes,
            has_more: v.has_more,
        })
    }
}
#[derive(Clone, Debug)]
pub struct ManifestRequest {
    pub job_id: ExportId,
    pub offset: i32,
    pub maximum_items: i32,
    pub maximum_bytes: i32,
}
impl ManifestRequest {
    fn validate(&self) -> Result<()> {
        if !(0..=MAX_CHUNKS).contains(&self.offset)
            || !(1..=MAX_MANIFEST_ITEMS).contains(&self.maximum_items)
            || !(MIN_PAGE_BYTES..=MAX_MANIFEST_BYTES).contains(&self.maximum_bytes)
        {
            return Err(invalid("invalid manifest request"));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestPage {
    pub chunks: Vec<Chunk>,
    pub next: i32,
    pub has_more: bool,
}
impl ManifestPage {
    pub fn decode(v: wire::ExportManifestPage, request: &ManifestRequest) -> Result<Self> {
        request.validate()?;
        if v.chunks.len() > request.maximum_items as usize || v.encoded_len() > MAX_FRAME_BYTES {
            return Err(invalid("oversized manifest response"));
        }
        let bytes = v.chunks.iter().fold(9usize, |sum, c| {
            sum + 153
                + c.lower_inclusive.as_ref().map_or(0, Vec::len)
                + c.upper_exclusive.as_ref().map_or(0, Vec::len)
        });
        if bytes > request.maximum_bytes as usize
            || v.next != request.offset + v.chunks.len() as i32
            || v.next > MAX_CHUNKS
            || v.has_more && (v.chunks.is_empty() || v.next == MAX_CHUNKS)
        {
            return Err(invalid("invalid manifest response bounds"));
        }
        let mut identities = HashSet::new();
        let chunks = v
            .chunks
            .into_iter()
            .map(|v| {
                let c = Chunk::try_from(v)?;
                if c.id.job_id != request.job_id || !identities.insert(c.id.clone()) {
                    return Err(invalid("wrong or duplicate manifest chunk identity"));
                }
                Ok(c)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            chunks,
            next: v.next,
            has_more: v.has_more,
        })
    }
}
#[derive(Clone, Debug)]
pub struct ReadRequest {
    pub id: ChunkId,
    pub offset: i64,
    pub maximum_bytes: i32,
}
impl ReadRequest {
    fn validate(&self) -> Result<()> {
        self.id.validate()?;
        if self.offset < 0 || !(MIN_PAGE_BYTES..=MAX_FILE_PAGE_BYTES).contains(&self.maximum_bytes)
        {
            return Err(invalid("invalid file request"));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FilePage {
    pub id: ChunkId,
    pub offset: i64,
    pub bytes: Vec<u8>,
    pub has_more: bool,
}
impl FilePage {
    pub fn decode(v: wire::ExportFilePage, request: &ReadRequest) -> Result<Self> {
        request.validate()?;
        let id = ChunkId::try_from(v.id.ok_or_else(|| invalid("missing chunk identity"))?)?;
        if id != request.id
            || v.offset != request.offset
            || v.data.len() > request.maximum_bytes as usize
            || v.offset.checked_add(v.data.len() as i64).is_none()
            || v.has_more && v.data.is_empty()
        {
            return Err(invalid("invalid file response identity or bounds"));
        }
        Ok(Self {
            id,
            offset: v.offset,
            bytes: v.data,
            has_more: v.has_more,
        })
    }
}
fn invalid(message: &'static str) -> Status {
    Status::invalid_argument(message)
}
fn uuid(v: Vec<u8>) -> Result<ExportId> {
    v.try_into()
        .map_err(|_| invalid("UUID must contain exactly 16 bytes"))
}
fn columns(mut v: Vec<i64>) -> Result<Vec<i64>> {
    if v.is_empty() || v.len() > MAX_COLUMNS {
        return Err(invalid("invalid export columns"));
    }
    v.sort_unstable();
    if v.windows(2).any(|p| p[0] == p[1]) {
        return Err(invalid("duplicate export column"));
    }
    Ok(v)
}
impl ExportStatus {
    pub fn decode(v: wire::ExportStatus, expected_job: Option<ExportId>) -> Result<Self> {
        let job_id = uuid(v.job_id)?;
        if expected_job.is_some_and(|id| id != job_id)
            || v.participant_count < 0
            || !(0..=MAX_CHUNKS).contains(&v.chunk_count)
            || !(0..=v.chunk_count).contains(&v.acknowledged_count)
            || v.file_bytes < 0
        {
            return Err(invalid("invalid export status"));
        }
        Ok(Self {
            job_id,
            phase: Phase::try_from(v.phase).map_err(|_| invalid("unknown export phase"))?,
            columns: columns(v.columns)?,
            participant_count: v.participant_count,
            chunk_count: v.chunk_count,
            acknowledged_count: v.acknowledged_count,
            file_bytes: v.file_bytes,
            cleanup_pending: v.cleanup_pending,
        })
    }
}

#[derive(Clone, Copy, Debug)]
struct Deadline(tokio::time::Instant);
#[derive(Clone, Copy, Debug)]
struct DeadlineInterceptor;
impl Interceptor for DeadlineInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>> {
        if let Some(d) = request.extensions().get::<Deadline>() {
            let remaining = d.0.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(Status::deadline_exceeded("export deadline expired"));
            }
            request.set_timeout(remaining);
        }
        Ok(request)
    }
}
type Client = wire::rocks_db_export_service_client::RocksDbExportServiceClient<
    InterceptedService<Channel, DeadlineInterceptor>,
>;
/// Export-only handle over a caller-owned channel; cloning shares the connection.
#[derive(Clone, Debug)]
pub struct ExportClient {
    client: Client,
    context: RequestContext,
}
impl ExportClient {
    pub fn new(channel: Channel, context: RequestContext) -> Self {
        Self {
            client:
                wire::rocks_db_export_service_client::RocksDbExportServiceClient::with_interceptor(
                    channel,
                    DeadlineInterceptor,
                )
                .max_decoding_message_size(MAX_FRAME_BYTES)
                .max_encoding_message_size(MAX_FRAME_BYTES),
            context,
        }
    }
    pub fn with_context(&self, context: RequestContext) -> Self {
        Self {
            client: self.client.clone(),
            context,
        }
    }
    async fn call<T, R, F, Fut>(&self, message: T, call: F) -> Result<R>
    where
        F: FnOnce(Client, Request<T>) -> Fut,
        Fut: Future<Output = Result<tonic::Response<R>>>,
    {
        if !matches!(self.context.profile, 2 | 3 | 4 | 6)
            || self.context.timeout_nanos <= 0
            || self.context.profile == 2 && self.context.timeout_nanos == i64::MAX
        {
            return Err(invalid("invalid export request context"));
        }
        let mut request = Request::new(message);
        let deadline = if self.context.timeout_nanos == i64::MAX {
            None
        } else {
            let timeout = Duration::from_nanos(self.context.timeout_nanos as u64);
            let deadline = tokio::time::Instant::now()
                .checked_add(timeout)
                .ok_or_else(|| invalid("export timeout exceeds clock range"))?;
            request.set_timeout(timeout);
            request.extensions_mut().insert(Deadline(deadline));
            Some(deadline)
        };
        let future = call(self.client.clone(), request);
        let response = if let Some(deadline) = deadline {
            tokio::time::timeout_at(deadline, future)
                .await
                .map_err(|_| Status::deadline_exceeded("export deadline expired"))??
        } else {
            future.await?
        };
        Ok(response.into_inner())
    }
    fn job(&self, id: ExportId) -> wire::ExportJobRequest {
        wire::ExportJobRequest {
            job_id: id.to_vec(),
            context: Some(self.context.clone()),
        }
    }
    pub async fn create_export(&self, request: Create) -> Result<ExportStatus> {
        let request = wire::ExportCreateRequest {
            request_id: request.request_id.to_vec(),
            columns: columns(request.columns)?,
            limits: Some(request.limits.encode()?),
            context: Some(self.context.clone()),
        };
        ExportStatus::decode(
            self.call(request, |mut c, r| async move { c.create_export(r).await })
                .await?,
            None,
        )
    }
    pub async fn recover_export(&self, id: ExportId) -> Result<ExportStatus> {
        ExportStatus::decode(
            self.call(
                self.job(id),
                |mut c, r| async move { c.recover_export(r).await },
            )
            .await?,
            Some(id),
        )
    }
    pub async fn step_export(&self, id: ExportId) -> Result<ExportStatus> {
        ExportStatus::decode(
            self.call(
                self.job(id),
                |mut c, r| async move { c.step_export(r).await },
            )
            .await?,
            Some(id),
        )
    }
    pub async fn export_status(&self, id: ExportId) -> Result<ExportStatus> {
        ExportStatus::decode(
            self.call(
                self.job(id),
                |mut c, r| async move { c.export_status(r).await },
            )
            .await?,
            Some(id),
        )
    }
    pub async fn export_manifest(&self, request: ManifestRequest) -> Result<ManifestPage> {
        request.validate()?;
        let message = wire::ExportManifestRequest {
            job_id: request.job_id.to_vec(),
            offset: request.offset,
            maximum_items: request.maximum_items,
            maximum_bytes: request.maximum_bytes,
            context: Some(self.context.clone()),
        };
        ManifestPage::decode(
            self.call(
                message,
                |mut c, r| async move { c.export_manifest(r).await },
            )
            .await?,
            &request,
        )
    }
    pub async fn read_export(&self, request: ReadRequest) -> Result<FilePage> {
        request.validate()?;
        let message = wire::ExportReadRequest {
            id: Some(request.id.encode()?),
            offset: request.offset,
            maximum_bytes: request.maximum_bytes,
            context: Some(self.context.clone()),
        };
        FilePage::decode(
            self.call(message, |mut c, r| async move { c.read_export(r).await })
                .await?,
            &request,
        )
    }
    pub async fn acknowledge_export(&self, id: ChunkId) -> Result<()> {
        self.call(
            wire::ExportAcknowledgeRequest {
                id: Some(id.encode()?),
                context: Some(self.context.clone()),
            },
            |mut c, r| async move { c.acknowledge_export(r).await },
        )
        .await
    }
    pub async fn release_export(&self, id: ExportId) -> Result<()> {
        self.call(
            self.job(id),
            |mut c, r| async move { c.release_export(r).await },
        )
        .await
    }
    pub async fn cancel_export(&self, id: ExportId) -> Result<()> {
        self.call(
            self.job(id),
            |mut c, r| async move { c.cancel_export(r).await },
        )
        .await
    }
    pub async fn capabilities(&self) -> Result<wire::ExportCapabilities> {
        let v = self
            .call(
                (),
                |mut c, r| async move { c.get_export_capabilities(r).await },
            )
            .await?;
        if v.raw_scan_mode != RawScanMode::ImmutableExportJobs as i32
            || v.maximum_columns < 1
            || v.maximum_columns > MAX_COLUMNS as i32
            || !(1..=MAX_CHUNKS).contains(&v.maximum_chunks)
            || !(1..=MAX_MANIFEST_ITEMS).contains(&v.maximum_manifest_items)
            || !(MIN_PAGE_BYTES..=MAX_MANIFEST_BYTES).contains(&v.maximum_manifest_bytes)
            || !(MIN_PAGE_BYTES..=MAX_FILE_PAGE_BYTES).contains(&v.maximum_file_page_bytes)
        {
            return Err(Status::failed_precondition("invalid export capabilities"));
        }
        Ok(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn id() -> ChunkId {
        ChunkId {
            job_id: [1; 16],
            group_id: [2; 16],
            range_lineage: [3; 16],
            range_epoch: 4,
            snapshot_term: 5,
            snapshot_index: 6,
            ordinal: 7,
            content_hash: [8; 32],
        }
    }
    fn chunk() -> wire::ExportChunk {
        wire::ExportChunk {
            id: Some(id().encode().unwrap()),
            column_id: -1,
            lower_inclusive: Some(vec![]),
            upper_exclusive: Some(vec![255]),
            rows: 1,
            payload_bytes: 2,
            file_bytes: 3,
            has_more: true,
        }
    }
    fn manifest() -> ManifestRequest {
        ManifestRequest {
            job_id: id().job_id,
            offset: 0,
            maximum_items: 2,
            maximum_bytes: MIN_PAGE_BYTES,
        }
    }
    #[test]
    fn qualified_identity_roundtrips_and_rejects_malformed_fields() {
        let original = id();
        assert_eq!(
            ChunkId::try_from(original.encode().unwrap()).unwrap(),
            original
        );
        for field in 0..4 {
            let mut wire = original.encode().unwrap();
            match field {
                0 => wire.job_id.pop(),
                1 => wire.group_id.pop(),
                2 => wire.range_lineage.pop(),
                _ => wire.content_hash.pop(),
            };
            assert!(ChunkId::try_from(wire).is_err());
        }
        let mut wire = original.encode().unwrap();
        wire.snapshot_term = 0;
        assert!(ChunkId::try_from(wire).is_err());
    }
    #[test]
    fn limits_require_bounded_journal_and_exact_millisecond_ttl() {
        assert!(Limits::default().encode().is_ok());
        let mut l = Limits::default();
        l.snapshot_ttl = Duration::from_nanos(1_000_001);
        assert!(l.encode().is_err());
        l = Limits::default();
        l.maximum_journal_bytes = 1;
        assert!(l.encode().is_err());
        l = Limits::default();
        l.maximum_chunk_duration = Duration::from_secs(u64::MAX);
        assert!(l.encode().is_err());
        assert_eq!(columns(vec![4, -1, 2]).unwrap(), vec![-1, 2, 4]);
        assert!(columns(vec![1, 1]).is_err());
    }
    #[test]
    fn manifest_checks_identity_progress_duplicates_and_budget() {
        let page = || wire::ExportManifestPage {
            chunks: vec![chunk()],
            next: 1,
            has_more: true,
        };
        let decoded = ManifestPage::decode(page(), &manifest()).unwrap();
        assert_eq!(decoded.chunks[0].lower_inclusive, Some(vec![]));
        assert_eq!(decoded.chunks[0].id, id());
        let mut v = page();
        v.next = 0;
        assert!(ManifestPage::decode(v, &manifest()).is_err());
        let mut v = page();
        v.chunks.push(chunk());
        v.next = 2;
        assert!(ManifestPage::decode(v, &manifest()).is_err());
        let mut request = manifest();
        request.job_id = [9; 16];
        assert!(ManifestPage::decode(page(), &request).is_err());
        let mut v = page();
        v.chunks[0].upper_exclusive = Some(vec![255; MIN_PAGE_BYTES as usize]);
        assert!(ManifestPage::decode(v, &manifest()).is_err());
        let v = wire::ExportManifestPage {
            chunks: vec![],
            next: 0,
            has_more: true,
        };
        assert!(ManifestPage::decode(v, &manifest()).is_err());
    }
    #[test]
    fn chunks_enforce_unsigned_bounds_and_empty_chunk_rules() {
        let mut c = chunk();
        c.lower_inclusive = Some(vec![128]);
        c.upper_exclusive = Some(vec![127]);
        assert!(Chunk::try_from(c).is_err());
        let mut c = chunk();
        c.rows = 0;
        assert!(Chunk::try_from(c).is_err());
        let mut c = chunk();
        c.upper_exclusive = None;
        assert!(Chunk::try_from(c).is_err());
        let mut c = chunk();
        c.rows = 0;
        c.payload_bytes = 0;
        c.file_bytes = 0;
        c.has_more = false;
        assert!(Chunk::try_from(c).is_ok());
    }
    #[test]
    fn file_pages_check_full_identity_offsets_budget_and_overflow() {
        let request = ReadRequest {
            id: id(),
            offset: 4,
            maximum_bytes: MIN_PAGE_BYTES,
        };
        let page = || wire::ExportFilePage {
            id: Some(id().encode().unwrap()),
            offset: 4,
            data: vec![1],
            has_more: true,
        };
        assert_eq!(FilePage::decode(page(), &request).unwrap().bytes, vec![1]);
        let mut v = page();
        v.id.as_mut().unwrap().ordinal += 1;
        assert!(FilePage::decode(v, &request).is_err());
        let mut v = page();
        v.offset += 1;
        assert!(FilePage::decode(v, &request).is_err());
        let mut v = page();
        v.data.clear();
        assert!(FilePage::decode(v, &request).is_err());
        let mut v = page();
        v.data = vec![0; MIN_PAGE_BYTES as usize + 1];
        assert!(FilePage::decode(v, &request).is_err());
        let mut v = page();
        v.offset = i64::MAX;
        let request = ReadRequest {
            offset: i64::MAX,
            ..request
        };
        assert!(FilePage::decode(v, &request).is_err());
    }
    #[test]
    fn status_rejects_wrong_job_unknown_phase_and_invalid_counts() {
        let status = || wire::ExportStatus {
            job_id: [1; 16].to_vec(),
            phase: Phase::Ready as i32,
            columns: vec![2, 1],
            participant_count: 1,
            chunk_count: 1,
            acknowledged_count: 0,
            file_bytes: 3,
            cleanup_pending: false,
        };
        assert_eq!(
            ExportStatus::decode(status(), Some([1; 16]))
                .unwrap()
                .columns,
            vec![1, 2]
        );
        assert!(ExportStatus::decode(status(), Some([2; 16])).is_err());
        let mut v = status();
        v.phase = 99;
        assert!(ExportStatus::decode(v, None).is_err());
        let mut v = status();
        v.acknowledged_count = 2;
        assert!(ExportStatus::decode(v, None).is_err());
    }
}
