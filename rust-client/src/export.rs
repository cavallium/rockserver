//! Bounded immutable export jobs. IDs are network-order UUID bytes; metadata contains no paths.
use crate::{export_proto as wire, RequestContext, Result};
use prost::Message;
use sha2::{Digest, Sha256};
use std::{collections::HashSet, future::Future, time::Duration};
use tonic::service::{interceptor::InterceptedService, Interceptor};
use tonic::{transport::Channel, Request, Status};

pub const MAX_COLUMNS: usize = 256;
pub const MAX_SNAPSHOT_TTL_MILLIS: u128 = 24 * 60 * 60 * 1000;
pub const MAX_CHUNKS: i32 = 65536;
pub const MAX_BOUND_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_MANIFEST_ITEMS: i32 = 128;
pub const MAX_MANIFEST_BYTES: i32 = 4 * 1024 * 1024;
pub const MIN_PAGE_BYTES: i32 = 64 * 1024;
pub const MAX_FILE_PAGE_BYTES: i32 = 1024 * 1024;
pub const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_SCHEMA_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_WITNESS_BYTES: i64 = 64 * 1024 * 1024;
pub type ExportId = [u8; 16];
pub use wire::{
    ExportPhase as Phase, ExportRawScanMode as RawScanMode, ExportWitnessKind as WitnessKind,
};

#[path = "aggregate_export.rs"]
mod aggregate;
pub use aggregate::*;

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
            || !(1..=MAX_SNAPSHOT_TTL_MILLIS).contains(&millis)
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
    pub definition_digest: [u8; 32],
    pub definition_bytes: i64,
    pub roster_generation: i64,
    pub domain_count: i32,
    pub capture_digest: Option<[u8; 32]>,
    pub capture_bytes: i64,
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
/// Decoded current distributed schema witness; its digest is computed from the canonical bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExportSchema {
    pub cluster_id: ExportId,
    pub database_id: ExportId,
    pub domain_id: ExportId,
    pub schema_epoch: i64,
    pub routing_prefix_bytes: i32,
    pub columns: Vec<ExportSchemaColumn>,
    pub digest: [u8; 32],
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExportSchemaColumn {
    pub column_id: ExportId,
    pub api_id: i64,
    pub name: String,
    pub physical_key_widths: Vec<i32>,
    pub variable_tail_hashes: Vec<SchemaHash>,
    pub has_value: bool,
    pub operator: Option<ExportSchemaOperator>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchemaHash {
    XxHash32,
    XxHash8,
    AllSame8,
    FixedInteger32,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExportSchemaOperator {
    pub name: String,
    pub version: i64,
    pub class_name: String,
    pub artifact_sha256: [u8; 32],
    pub value_codec_version: i32,
    pub operand_codec_version: i32,
    pub deterministic_contract: String,
    pub deterministic_contract_version: i32,
}
struct SchemaReader<'a>(&'a [u8]);
impl<'a> SchemaReader<'a> {
    fn take(&mut self, size: usize) -> Result<&'a [u8]> {
        if self.0.len() < size {
            return Err(invalid("truncated export schema"));
        }
        let (head, tail) = self.0.split_at(size);
        self.0 = tail;
        Ok(head)
    }
    fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn uuid(&mut self) -> Result<ExportId> {
        Ok(self.take(16)?.try_into().unwrap())
    }
    fn flag(&mut self) -> Result<bool> {
        match self.take(1)?[0] {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(invalid("invalid export schema flag")),
        }
    }
    fn text(&mut self) -> Result<String> {
        let length = self.i32()?;
        if !(1..=4096).contains(&length) {
            return Err(invalid("invalid export schema text length"));
        }
        std::str::from_utf8(self.take(length as usize)?)
            .map(str::to_owned)
            .map_err(|_| invalid("invalid export schema UTF-8"))
    }
}
impl ExportSchema {
    pub fn decode(encoded: &[u8]) -> Result<Self> {
        if !(76..=MAX_SCHEMA_BYTES).contains(&encoded.len()) {
            return Err(invalid("invalid export schema size"));
        }
        let mut r = SchemaReader(encoded);
        if r.i32()? != 0x44534348 || r.i32()? != 1 || r.i32()? != 1 {
            return Err(invalid("unsupported export schema or physical key codec"));
        }
        let cluster_id = r.uuid()?;
        let database_id = r.uuid()?;
        let domain_id = r.uuid()?;
        let schema_epoch = r.i64()?;
        let routing_prefix_bytes = r.i32()?;
        let count = r.i32()?;
        if schema_epoch < 0
            || !(0..=1024 * 1024).contains(&routing_prefix_bytes)
            || !(0..=256).contains(&count)
            || count as usize * 39 > r.0.len()
        {
            return Err(invalid("invalid export schema header bounds"));
        }
        let mut columns = Vec::with_capacity(count as usize);
        let mut previous = None;
        let mut ids = HashSet::new();
        let mut names = HashSet::new();
        let mut prefix = None;
        for _ in 0..count {
            let column_id = r.uuid()?;
            let api_id = r.i64()?;
            if api_id <= 0 || !ids.insert(api_id) || previous.is_some_and(|id| id >= column_id) {
                return Err(invalid("noncanonical export schema UUID or API ID"));
            }
            previous = Some(column_id);
            let name = r.text()?;
            if !names.insert(name.clone()) {
                return Err(invalid("duplicate export schema column name"));
            }
            let components = r.i32()?;
            if !(0..=256).contains(&components) || components as usize * 4 + 6 > r.0.len() {
                return Err(invalid("invalid export schema key component count"));
            }
            let mut physical_key_widths = Vec::with_capacity(components as usize);
            let mut bytes = 0i64;
            for _ in 0..components {
                let width = r.i32()?;
                bytes += width as i64;
                if width <= 0 || bytes > MAX_SCHEMA_BYTES as i64 {
                    return Err(invalid("invalid export schema key width/budget"));
                }
                physical_key_widths.push(width);
            }
            let tails = r.i32()?;
            if tails < 0 || tails > components || tails as usize + 2 > r.0.len() {
                return Err(invalid("invalid export schema variable tail count"));
            }
            let mut variable_tail_hashes = Vec::with_capacity(tails as usize);
            for k in 0..tails {
                let (hash, width) = match r.take(1)?[0] {
                    1 => (SchemaHash::XxHash32, 4),
                    2 => (SchemaHash::XxHash8, 1),
                    3 => (SchemaHash::AllSame8, 1),
                    4 => (SchemaHash::FixedInteger32, 4),
                    _ => return Err(invalid("unknown export schema hash codec")),
                };
                if physical_key_widths[(components - tails + k) as usize] != width {
                    return Err(invalid("export schema hash width differs from codec"));
                }
                variable_tail_hashes.push(hash);
            }
            let has_value = r.flag()?;
            let operator = if r.flag()? {
                let name = r.text()?;
                let version = r.i64()?;
                let class_name = r.text()?;
                let artifact_sha256 = r.take(32)?.try_into().unwrap();
                let value_codec_version = r.i32()?;
                let operand_codec_version = r.i32()?;
                let deterministic_contract = r.text()?;
                let deterministic_contract_version = r.i32()?;
                if version < 0
                    || value_codec_version < 1
                    || operand_codec_version < 1
                    || deterministic_contract_version < 1
                {
                    return Err(invalid("invalid export schema operator identity"));
                }
                Some(ExportSchemaOperator {
                    name,
                    version,
                    class_name,
                    artifact_sha256,
                    value_codec_version,
                    operand_codec_version,
                    deterministic_contract,
                    deterministic_contract_version,
                })
            } else {
                None
            };
            let mut widths = Vec::new();
            let mut bytes = 0i64;
            for width in &physical_key_widths[..(components - tails) as usize] {
                if bytes >= routing_prefix_bytes as i64 {
                    break;
                }
                widths.push(*width);
                bytes += *width as i64;
            }
            if bytes != routing_prefix_bytes as i64 || prefix.as_ref().is_some_and(|p| p != &widths)
            {
                return Err(invalid(
                    "export schema routing prefix differs from fixed component boundaries",
                ));
            }
            prefix = Some(widths);
            columns.push(ExportSchemaColumn {
                column_id,
                api_id,
                name,
                physical_key_widths,
                variable_tail_hashes,
                has_value,
                operator,
            });
        }
        if !r.0.is_empty() {
            return Err(invalid("trailing export schema bytes"));
        }
        Ok(Self {
            cluster_id,
            database_id,
            domain_id,
            schema_epoch,
            routing_prefix_bytes,
            columns,
            digest: Sha256::digest(encoded).into(),
        })
    }
}
impl ExportStatus {
    pub fn decode(v: wire::ExportStatus, expected_job: Option<ExportId>) -> Result<Self> {
        let phase = Phase::try_from(v.phase).map_err(|_| invalid("unknown export phase"))?;
        let definition_digest = content_digest(v.definition_digest)?;
        let capture_digest = if v.capture_digest.is_empty() {
            None
        } else {
            Some(content_digest(v.capture_digest)?)
        };
        if !(1..=MAX_WITNESS_BYTES).contains(&v.definition_bytes)
            || v.roster_generation < 0
            || !(1..=64).contains(&v.domain_count)
            || if capture_digest.is_some() {
                !(1..=MAX_WITNESS_BYTES).contains(&v.capture_bytes)
            } else {
                v.capture_bytes != 0
            }
        {
            return Err(invalid("invalid aggregate export witness status"));
        }
        if capture_digest.is_none()
            && matches!(phase, Phase::Exporting | Phase::Materialized | Phase::Ready)
        {
            return Err(invalid("export phase requires forced capture witness"));
        }
        let selected = columns(v.columns)?;
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
            phase,
            columns: selected,
            participant_count: v.participant_count,
            chunk_count: v.chunk_count,
            acknowledged_count: v.acknowledged_count,
            file_bytes: v.file_bytes,
            cleanup_pending: v.cleanup_pending,
            definition_digest,
            definition_bytes: v.definition_bytes,
            roster_generation: v.roster_generation,
            domain_count: v.domain_count,
            capture_digest,
            capture_bytes: v.capture_bytes,
        })
    }
}

fn content_digest(value: Vec<u8>) -> Result<[u8; 32]> {
    value
        .try_into()
        .map_err(|_| invalid("witness digest must contain 32 bytes"))
}
#[derive(Clone, Debug)]
pub struct WitnessRequest {
    pub job_id: ExportId,
    pub kind: WitnessKind,
    pub expected_digest: [u8; 32],
    pub offset: i64,
    pub maximum_bytes: i32,
}
impl WitnessRequest {
    fn validate(&self) -> Result<()> {
        if self.offset < 0
            || self.offset > MAX_WITNESS_BYTES
            || !(MIN_PAGE_BYTES..=MAX_FILE_PAGE_BYTES).contains(&self.maximum_bytes)
        {
            return Err(invalid("invalid export witness request"));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WitnessPage {
    pub job_id: ExportId,
    pub kind: WitnessKind,
    pub digest: [u8; 32],
    pub total_bytes: i64,
    pub offset: i64,
    pub data: Vec<u8>,
    pub has_more: bool,
}
impl WitnessPage {
    pub fn decode(v: wire::ExportWitnessPage, request: &WitnessRequest) -> Result<Self> {
        request.validate()?;
        let page = Self {
            job_id: uuid(v.job_id)?,
            kind: WitnessKind::try_from(v.kind)
                .map_err(|_| invalid("unknown export witness kind"))?,
            digest: content_digest(v.digest)?,
            total_bytes: v.total_bytes,
            offset: v.offset,
            data: v.data,
            has_more: v.has_more,
        };
        page.validate()?;
        if page.job_id != request.job_id
            || page.kind != request.kind
            || page.digest != request.expected_digest
            || page.offset != request.offset
            || page.data.len() > request.maximum_bytes as usize
        {
            return Err(invalid(
                "crossed export witness identity/digest/offset/budget",
            ));
        }
        Ok(page)
    }
    fn validate(&self) -> Result<()> {
        let end = self
            .offset
            .checked_add(self.data.len() as i64)
            .ok_or_else(|| invalid("export witness offset overflow"))?;
        if !(1..=MAX_WITNESS_BYTES).contains(&self.total_bytes)
            || self.offset < 0
            || self.offset > self.total_bytes
            || self.data.len() > MAX_FILE_PAGE_BYTES as usize
            || end > self.total_bytes
            || self.has_more != (end < self.total_bytes)
            || self.has_more && self.data.is_empty()
        {
            return Err(invalid("invalid export witness page bounds"));
        }
        Ok(())
    }
    /// Recompose a complete, contiguous witness and verify its qualified digest before parsing it.
    pub fn assemble(
        pages: impl IntoIterator<Item = Self>,
        job_id: ExportId,
        kind: WitnessKind,
        expected_digest: [u8; 32],
        total_bytes: i64,
    ) -> Result<Vec<u8>> {
        if !(1..=MAX_WITNESS_BYTES).contains(&total_bytes) {
            return Err(invalid("invalid aggregate witness size"));
        }
        let mut encoded = Vec::new();
        let mut complete = false;
        for page in pages {
            page.validate()?;
            if complete
                || page.job_id != job_id
                || page.kind != kind
                || page.digest != expected_digest
                || page.total_bytes != total_bytes
                || page.offset != encoded.len() as i64
            {
                return Err(invalid("noncontiguous or crossed aggregate witness"));
            }
            encoded.extend_from_slice(&page.data);
            complete = !page.has_more;
        }
        if !complete
            || encoded.len() as i64 != total_bytes
            || <[u8; 32]>::from(Sha256::digest(&encoded)) != expected_digest
        {
            return Err(invalid("incomplete or corrupted aggregate witness digest"));
        }
        Ok(encoded)
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
    pub async fn export_witness(&self, request: WitnessRequest) -> Result<WitnessPage> {
        request.validate()?;
        let message = wire::ExportWitnessRequest {
            job_id: request.job_id.to_vec(),
            kind: request.kind as i32,
            expected_digest: request.expected_digest.to_vec(),
            offset: request.offset,
            maximum_bytes: request.maximum_bytes,
            context: Some(self.context.clone()),
        };
        WitnessPage::decode(
            self.call(message, |mut c, r| async move { c.export_witness(r).await })
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
        let mut maximum = Limits::default();
        maximum.snapshot_ttl = Duration::from_millis(MAX_SNAPSHOT_TTL_MILLIS as u64);
        assert!(maximum.encode().is_ok());
        maximum.snapshot_ttl += Duration::from_millis(1);
        assert!(maximum.encode().is_err());
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
    fn schema_bytes() -> Vec<u8> {
        fn text(encoded: &mut Vec<u8>, value: &str) {
            encoded.extend_from_slice(&(value.len() as i32).to_be_bytes());
            encoded.extend_from_slice(value.as_bytes());
        }
        let mut encoded = Vec::new();
        for header in [0x44534348i32, 1, 1] {
            encoded.extend_from_slice(&header.to_be_bytes());
        }
        for uuid in [[3; 16], [4; 16], [5; 16]] {
            encoded.extend_from_slice(&uuid);
        }
        encoded.extend_from_slice(&6i64.to_be_bytes());
        encoded.extend_from_slice(&4i32.to_be_bytes());
        encoded.extend_from_slice(&2i32.to_be_bytes());
        for (api, name, operator) in [(1i64, "a", false), (2i64, "b", true)] {
            encoded.extend_from_slice(&[api as u8; 16]);
            encoded.extend_from_slice(&api.to_be_bytes());
            text(&mut encoded, name);
            for field in [2i32, 4, 4, 1] {
                encoded.extend_from_slice(&field.to_be_bytes());
            }
            encoded.extend_from_slice(&[1, 1, operator as u8]);
            if operator {
                text(&mut encoded, "merge");
                encoded.extend_from_slice(&7i64.to_be_bytes());
                text(&mut encoded, "example.Merge");
                encoded.extend_from_slice(&[8; 32]);
                encoded.extend_from_slice(&1i32.to_be_bytes());
                encoded.extend_from_slice(&2i32.to_be_bytes());
                text(&mut encoded, "deterministic");
                encoded.extend_from_slice(&3i32.to_be_bytes());
            }
        }
        encoded
    }
    fn schema_status() -> wire::ExportStatus {
        wire::ExportStatus {
            job_id: [1; 16].to_vec(),
            phase: Phase::Capturing as i32,
            columns: vec![1, 2],
            participant_count: 1,
            chunk_count: 1,
            acknowledged_count: 0,
            file_bytes: 3,
            cleanup_pending: false,
            definition_digest: Sha256::digest(schema_bytes()).to_vec(),
            definition_bytes: schema_bytes().len() as i64,
            roster_generation: 0,
            domain_count: 1,
            capture_digest: vec![],
            capture_bytes: 0,
        }
    }
    #[test]
    fn schema_component_exposes_canonical_physical_operator_identity() {
        let schema = ExportSchema::decode(&schema_bytes()).unwrap();
        assert_eq!(
            (schema.cluster_id, schema.database_id, schema.domain_id),
            ([3; 16], [4; 16], [5; 16])
        );
        assert_eq!(schema.schema_epoch, 6);
        assert_eq!(schema.routing_prefix_bytes, 4);
        assert_eq!(
            schema.digest,
            <[u8; 32]>::from(Sha256::digest(schema_bytes()))
        );
        assert_eq!(schema.columns[0].api_id, 1);
        assert_eq!(schema.columns[0].physical_key_widths, vec![4, 4]);
        assert_eq!(
            schema.columns[0].variable_tail_hashes,
            vec![SchemaHash::XxHash32]
        );
        assert!(schema.columns[0].operator.is_none());
        let operator = schema.columns[1].operator.as_ref().unwrap();
        assert_eq!(operator.name, "merge");
        assert_eq!(operator.version, 7);
        assert_eq!(operator.class_name, "example.Merge");
        assert_eq!(operator.artifact_sha256, [8; 32]);
        assert_eq!(
            (
                operator.value_codec_version,
                operator.operand_codec_version,
                operator.deterministic_contract_version
            ),
            (1, 2, 3)
        );
        assert_eq!(operator.deterministic_contract, "deterministic");
    }
    #[test]
    fn aggregate_status_roundtrip_requires_qualified_witness_metadata() {
        let wire = schema_status();
        let decoded = ExportStatus::decode(
            wire::ExportStatus::decode(wire.encode_to_vec().as_slice()).unwrap(),
            Some([1; 16]),
        )
        .unwrap();
        assert_eq!(
            decoded.definition_digest,
            <[u8; 32]>::from(Sha256::digest(schema_bytes()))
        );
        assert_eq!(decoded.definition_bytes, schema_bytes().len() as i64);
        assert_eq!(decoded.domain_count, 1);
        assert!(decoded.capture_digest.is_none());
        assert_eq!(decoded.capture_bytes, 0);
        for digest in [vec![], vec![0; 31], vec![0; 33]] {
            let mut status = schema_status();
            status.definition_digest = digest;
            assert!(ExportStatus::decode(status, None).is_err());
        }
        for bytes in [0, MAX_WITNESS_BYTES + 1] {
            let mut status = schema_status();
            status.definition_bytes = bytes;
            assert!(ExportStatus::decode(status, None).is_err());
        }
        for count in [0, 65] {
            let mut status = schema_status();
            status.domain_count = count;
            assert!(ExportStatus::decode(status, None).is_err());
        }
        let mut missing_capture = schema_status();
        missing_capture.phase = Phase::Ready as i32;
        assert!(ExportStatus::decode(missing_capture, None).is_err());
        let mut status = schema_status();
        status.roster_generation = -1;
        assert!(ExportStatus::decode(status, None).is_err());
        let mut status = schema_status();
        status.capture_bytes = 1;
        assert!(ExportStatus::decode(status, None).is_err());
        let mut status = schema_status();
        status.capture_digest = vec![0; 32];
        assert!(ExportStatus::decode(status, None).is_err());
        let mut status = schema_status();
        status.capture_digest = vec![1; 32];
        status.capture_bytes = MAX_WITNESS_BYTES;
        assert!(ExportStatus::decode(status, None).is_ok());
    }
    fn witness_request() -> WitnessRequest {
        WitnessRequest {
            job_id: [1; 16],
            kind: WitnessKind::Definition,
            expected_digest: [2; 32],
            offset: 0,
            maximum_bytes: MIN_PAGE_BYTES,
        }
    }
    fn witness_page() -> wire::ExportWitnessPage {
        wire::ExportWitnessPage {
            job_id: [1; 16].to_vec(),
            kind: WitnessKind::Definition as i32,
            digest: [2; 32].to_vec(),
            total_bytes: 3,
            offset: 0,
            data: vec![1, 2, 3],
            has_more: false,
        }
    }
    #[test]
    fn witness_page_checks_exact_job_kind_hash_offset_total_and_budget() {
        assert_eq!(
            WitnessPage::decode(witness_page(), &witness_request())
                .unwrap()
                .data,
            vec![1, 2, 3]
        );
        let mut page = witness_page();
        page.job_id = [3; 16].to_vec();
        assert!(WitnessPage::decode(page, &witness_request()).is_err());
        let mut page = witness_page();
        page.kind = WitnessKind::Captures as i32;
        assert!(WitnessPage::decode(page, &witness_request()).is_err());
        let mut page = witness_page();
        page.kind = 99;
        assert!(WitnessPage::decode(page, &witness_request()).is_err());
        let mut page = witness_page();
        page.digest = [3; 32].to_vec();
        assert!(WitnessPage::decode(page, &witness_request()).is_err());
        let mut page = witness_page();
        page.offset = 1;
        page.data.pop();
        assert!(WitnessPage::decode(page, &witness_request()).is_err());
        let mut page = witness_page();
        page.has_more = true;
        assert!(WitnessPage::decode(page, &witness_request()).is_err());
        let mut page = witness_page();
        page.data.clear();
        page.has_more = true;
        assert!(WitnessPage::decode(page, &witness_request()).is_err());
        let mut page = witness_page();
        page.total_bytes = MAX_WITNESS_BYTES + 1;
        assert!(WitnessPage::decode(page, &witness_request()).is_err());
        let mut page = witness_page();
        page.data = vec![0; MIN_PAGE_BYTES as usize + 1];
        page.total_bytes = page.data.len() as i64;
        assert!(WitnessPage::decode(page, &witness_request()).is_err());
        let mut request = witness_request();
        request.maximum_bytes = MIN_PAGE_BYTES - 1;
        assert!(WitnessPage::decode(witness_page(), &request).is_err());
    }
    #[test]
    fn witness_recomposition_verifies_continuity_and_full_digest_without_ack() {
        let bytes = schema_bytes();
        let digest: [u8; 32] = Sha256::digest(&bytes).into();
        let pages = vec![
            WitnessPage {
                job_id: [1; 16],
                kind: WitnessKind::Definition,
                digest,
                total_bytes: bytes.len() as i64,
                offset: 0,
                data: bytes[..100].to_vec(),
                has_more: true,
            },
            WitnessPage {
                job_id: [1; 16],
                kind: WitnessKind::Definition,
                digest,
                total_bytes: bytes.len() as i64,
                offset: 100,
                data: bytes[100..].to_vec(),
                has_more: false,
            },
        ];
        let recomposed = WitnessPage::assemble(
            pages.clone(),
            [1; 16],
            WitnessKind::Definition,
            digest,
            bytes.len() as i64,
        )
        .unwrap();
        assert_eq!(recomposed, bytes);
        assert!(ExportSchema::decode(&recomposed).is_ok());
        let mut corrupted = pages.clone();
        corrupted[1].data[0] ^= 1;
        assert!(WitnessPage::assemble(
            corrupted,
            [1; 16],
            WitnessKind::Definition,
            digest,
            bytes.len() as i64
        )
        .is_err());
        let mut crossed = pages.clone();
        crossed[1].offset += 1;
        assert!(WitnessPage::assemble(
            crossed,
            [1; 16],
            WitnessKind::Definition,
            digest,
            bytes.len() as i64
        )
        .is_err());
        assert!(WitnessPage::assemble(
            pages[..1].to_vec(),
            [1; 16],
            WitnessKind::Definition,
            digest,
            bytes.len() as i64
        )
        .is_err());
    }
    #[test]
    fn schema_decoder_rejects_noncanonical_ids_names_utf8_and_carrier_versions() {
        let bytes = schema_bytes();
        assert_eq!(bytes.len(), 267);
        for offset in [4, 8] {
            let mut v = bytes.clone();
            v[offset..offset + 4].copy_from_slice(&2i32.to_be_bytes());
            assert!(ExportSchema::decode(&v).is_err());
        }
        let mut v = bytes.clone();
        v[92..100].copy_from_slice(&0i64.to_be_bytes());
        assert!(ExportSchema::decode(&v).is_err());
        let mut v = bytes.clone();
        v[140..148].copy_from_slice(&1i64.to_be_bytes());
        assert!(ExportSchema::decode(&v).is_err());
        let mut v = bytes.clone();
        v[124..140].copy_from_slice(&[1; 16]);
        assert!(ExportSchema::decode(&v).is_err());
        let mut v = bytes.clone();
        v[76..92].copy_from_slice(&[0x80; 16]);
        assert!(ExportSchema::decode(&v).is_err());
        let mut v = bytes.clone();
        v[124..140].copy_from_slice(&[0x80; 16]);
        assert!(ExportSchema::decode(&v).is_ok());
        let mut v = bytes.clone();
        v[152] = b'a';
        assert!(ExportSchema::decode(&v).is_err());
        let mut v = bytes.clone();
        v[104] = 0xff;
        assert!(ExportSchema::decode(&v).is_err());
        let mut v = bytes.clone();
        v[100..104].copy_from_slice(&4097i32.to_be_bytes());
        assert!(ExportSchema::decode(&v).is_err());
        let mut v = bytes;
        v.push(0);
        assert!(ExportSchema::decode(&v).is_err());
    }
    #[test]
    fn schema_decoder_enforces_shape_prefix_hash_flags_and_operator_versions() {
        let bytes = schema_bytes();
        let mut v = bytes.clone();
        v[60..68].copy_from_slice(&(-1i64).to_be_bytes());
        assert!(ExportSchema::decode(&v).is_err());
        for (offset, value) in [
            (72, 257i32),
            (105, 257),
            (109, 0),
            (109, MAX_SCHEMA_BYTES as i32),
            (117, 3),
            (68, 3),
            (238, 0),
            (242, 0),
            (263, 0),
        ] {
            let mut v = bytes.clone();
            v[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
            assert!(ExportSchema::decode(&v).is_err());
        }
        for (offset, value) in [(121, 0u8), (121, 2), (122, 2), (123, 2)] {
            let mut v = bytes.clone();
            v[offset] = value;
            assert!(ExportSchema::decode(&v).is_err());
        }
        let mut v = bytes.clone();
        v[181..189].copy_from_slice(&(-1i64).to_be_bytes());
        assert!(ExportSchema::decode(&v).is_err());
        // Equal byte prefix length is insufficient: every column must use the same fixed component boundaries.
        let mut v = bytes;
        v[157..161].copy_from_slice(&2i32.to_be_bytes());
        assert!(ExportSchema::decode(&v).is_err());
    }
    #[test]
    fn status_rejects_wrong_job_unknown_phase_and_invalid_counts() {
        let status = || wire::ExportStatus {
            job_id: [1; 16].to_vec(),
            phase: Phase::Capturing as i32,
            columns: vec![2, 1],
            participant_count: 1,
            chunk_count: 1,
            acknowledged_count: 0,
            file_bytes: 3,
            cleanup_pending: false,
            definition_digest: Sha256::digest(schema_bytes()).to_vec(),
            definition_bytes: schema_bytes().len() as i64,
            roster_generation: 0,
            domain_count: 1,
            capture_digest: vec![],
            capture_bytes: 0,
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
