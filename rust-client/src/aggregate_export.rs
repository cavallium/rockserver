//! Canonical path-free aggregate witnesses. These codecs validate binding, not native capture authority.
use super::*;
use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DomainRoster {
    pub cluster_id: ExportId,
    pub database_id: ExportId,
    pub generation: i64,
    pub domains: Vec<ExportId>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggregateReplica {
    pub node_id: ExportId,
    pub node_incarnation: ExportId,
    pub replica_id: ExportId,
    pub voter: bool,
    pub peer_id: String,
    pub address: String,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggregateRange {
    pub range_id: ExportId,
    pub group_id: ExportId,
    pub epoch: i64,
    pub configuration_version: i64,
    pub lower: Option<Vec<u8>>,
    pub upper: Option<Vec<u8>>,
    pub topology: u8,
    pub replicas: Vec<AggregateReplica>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggregateDirectory {
    pub canonical: Vec<u8>,
    pub cluster_id: ExportId,
    pub database_id: ExportId,
    pub domain_id: ExportId,
    pub schema_epoch: i64,
    pub schema_digest: [u8; 32],
    pub generation: i64,
    pub routing_prefix_bytes: i32,
    pub columns: Vec<ExportId>,
    pub ranges: Vec<AggregateRange>,
}
#[derive(Clone, Debug)]
pub struct AggregateExportDomain {
    pub directory: AggregateDirectory,
    pub schema: ExportSchema,
    pub selected_columns: Vec<i64>,
    pub canonical_schema: Vec<u8>,
}
#[derive(Clone, Debug)]
pub struct AggregateExportDefinition {
    canonical: Vec<u8>,
    pub job_id: ExportId,
    pub request_id: ExportId,
    pub roster: DomainRoster,
    pub limits: Limits,
    pub domains: Vec<AggregateExportDomain>,
    pub digest: [u8; 32],
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CapturePosition {
    pub term: i64,
    pub index: i64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotCapture {
    pub group_id: ExportId,
    pub ready: CapturePosition,
    pub owner_incarnation: ExportId,
    pub snapshot_id: ExportId,
    pub position: CapturePosition,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CaptureCohort {
    pub id: ExportId,
    pub node_incarnation: ExportId,
    pub process_incarnation: ExportId,
    pub pool_id: ExportId,
    pub groups: Vec<ExportId>,
}
#[derive(Clone, Debug)]
pub struct AggregateExportCaptures {
    canonical: Vec<u8>,
    pub definition_digest: [u8; 32],
    pub digest: [u8; 32],
    pub captures: Vec<SnapshotCapture>,
    pub cohorts: Vec<CaptureCohort>,
}
struct Read<'a>(&'a [u8]);
impl<'a> Read<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.0.len() < n {
            return Err(invalid("truncated aggregate export witness"));
        }
        let (h, t) = self.0.split_at(n);
        self.0 = t;
        Ok(h)
    }
    fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn id(&mut self) -> Result<ExportId> {
        Ok(self.take(16)?.try_into().unwrap())
    }
    fn digest(&mut self) -> Result<[u8; 32]> {
        Ok(self.take(32)?.try_into().unwrap())
    }
    fn count(&mut self, max: usize, minbytes: usize) -> Result<usize> {
        let n = self.i32()?;
        if n < 0 || n as usize > max || n as usize * minbytes > self.0.len() {
            return Err(invalid("aggregate witness count exceeds bound"));
        }
        Ok(n as usize)
    }
    fn blob(&mut self, max: usize) -> Result<&'a [u8]> {
        let n = self.count(max, 1)?;
        self.take(n)
    }
    fn bound(&mut self) -> Result<Option<Vec<u8>>> {
        let n = self.i32()?;
        if n == -1 {
            return Ok(None);
        }
        if n < 0 || n as usize > MAX_SCHEMA_BYTES {
            return Err(invalid("invalid aggregate bound"));
        }
        Ok(Some(self.take(n as usize)?.to_vec()))
    }
    fn end(&self) -> Result<()> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(invalid("trailing aggregate witness bytes"))
        }
    }
    fn control(&mut self) -> Result<(ExportId, i64, i64)> {
        let op = (self.id()?, self.i64()?, self.i64()?);
        if op.1 < 0 || op.2 < 0 {
            return Err(invalid("negative snapshot control identity"));
        }
        Ok(op)
    }
    fn position(&mut self) -> Result<CapturePosition> {
        let p = CapturePosition {
            term: self.i64()?,
            index: self.i64()?,
        };
        if p.term < 1 || p.index < 1 {
            return Err(invalid("snapshot proof position must be positive"));
        }
        Ok(p)
    }
}
fn ordered<T: Ord>(values: &[T]) -> bool {
    values.windows(2).all(|p| p[0] < p[1])
}
fn roster(encoded: &[u8]) -> Result<DomainRoster> {
    if !(60..=1068).contains(&encoded.len()) {
        return Err(invalid("invalid roster size"));
    }
    let mut r = Read(encoded);
    let cluster_id = r.id()?;
    let database_id = r.id()?;
    let generation = r.i64()?;
    let count = r.count(64, 16)?;
    let domains = (0..count).map(|_| r.id()).collect::<Result<Vec<_>>>()?;
    r.end()?;
    if generation < 0 || domains.is_empty() || !ordered(&domains) {
        return Err(invalid("noncanonical database roster"));
    }
    Ok(DomainRoster {
        cluster_id,
        database_id,
        generation,
        domains,
    })
}
fn endpoint(address: &str) -> bool {
    if !address
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b"._-[]:".contains(&b))
    {
        return false;
    }
    let Some((host, port)) = address.rsplit_once(':') else {
        return false;
    };
    !host.is_empty()
        && (!host.contains(':') || host.starts_with('[') && host.ends_with(']'))
        && !port.is_empty()
        && port.len() <= 5
        && port.bytes().all(|b| b.is_ascii_digit())
        && port.parse::<u32>().is_ok_and(|p| (1..=65535).contains(&p))
}
fn directory(encoded: &[u8], schema: &ExportSchema) -> Result<AggregateDirectory> {
    if !(112..=1024 * 1024).contains(&encoded.len()) {
        return Err(invalid("invalid aggregate directory size"));
    }
    let mut r = Read(encoded);
    if r.i32()? != 1 {
        return Err(invalid("unsupported current directory carrier"));
    }
    let cluster_id = r.id()?;
    let database_id = r.id()?;
    let domain_id = r.id()?;
    let schema_epoch = r.i64()?;
    let schema_digest = r.digest()?;
    let generation = r.i64()?;
    let routing_prefix_bytes = r.i32()?;
    let count = r.count(256, 16)?;
    let columns = (0..count).map(|_| r.id()).collect::<Result<Vec<_>>>()?;
    if cluster_id != schema.cluster_id
        || database_id != schema.database_id
        || domain_id != schema.domain_id
        || schema_epoch != schema.schema_epoch
        || schema_digest != schema.digest
        || routing_prefix_bytes != schema.routing_prefix_bytes
        || generation < 0
        || !ordered(&columns)
        || columns
            != schema
                .columns
                .iter()
                .map(|c| c.column_id)
                .collect::<Vec<_>>()
    {
        return Err(invalid("directory differs from frozen schema witness"));
    }
    let count = r.count(1024, 61)?;
    if count == 0 {
        return Err(invalid("empty aggregate range directory"));
    }
    let mut ranges = Vec::with_capacity(count);
    let mut ids = HashSet::new();
    let mut groups = HashSet::new();
    for i in 0..count {
        let range_id = r.id()?;
        let group_id = r.id()?;
        let epoch = r.i64()?;
        let configuration_version = r.i64()?;
        let lower = r.bound()?;
        let upper = r.bound()?;
        let topology = r.take(1)?[0];
        if epoch < 0
            || configuration_version < 0
            || !ids.insert(range_id)
            || !groups.insert(group_id)
            || !(1..=3).contains(&topology)
            || (i == 0 && lower.is_some())
            || (i > 0
                && (lower.is_none()
                    || ranges
                        .last()
                        .map_or(true, |p: &AggregateRange| p.upper != lower)))
            || (i + 1 == count && upper.is_some())
            || (i + 1 < count && upper.is_none())
            || matches!((&lower,&upper),(Some(a),Some(b))if a>=b)
            || routing_prefix_bytes > 0
                && [&lower, &upper].iter().any(|b| {
                    b.as_ref()
                        .is_some_and(|b| b.len() != routing_prefix_bytes as usize)
                })
        {
            return Err(invalid("invalid aggregate range coverage"));
        }
        let count = r.count(16, 57)?;
        if count == 0 {
            return Err(invalid("empty range owner set"));
        }
        let mut replicas = Vec::with_capacity(count);
        let mut nodes = HashSet::new();
        let mut incarnations = HashSet::new();
        let mut peers = HashSet::new();
        let mut addresses = HashSet::new();
        let mut voters = 0;
        for _ in 0..count {
            let node_id = r.id()?;
            let node_incarnation = r.id()?;
            let replica_id = r.id()?;
            let role = r.take(1)?[0];
            let peer_id = std::str::from_utf8(r.blob(128)?)
                .map_err(|_| invalid("non-ASCII replica peer"))?
                .to_owned();
            let address = std::str::from_utf8(r.blob(256)?)
                .map_err(|_| invalid("non-ASCII replica address"))?
                .to_owned();
            if !nodes.insert(node_id)
                || !incarnations.insert(node_incarnation)
                || replicas
                    .last()
                    .is_some_and(|p: &AggregateReplica| p.replica_id >= replica_id)
                || !(1..=2).contains(&role)
                || peer_id.is_empty()
                || !peer_id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
                || !endpoint(&address)
                || !peers.insert(peer_id.clone())
                || !addresses.insert(address.clone())
            {
                return Err(invalid("invalid duplicate range replica/endpoint"));
            }
            let voter = role == 1;
            if voter {
                voters += 1
            }
            replicas.push(AggregateReplica {
                node_id,
                node_incarnation,
                replica_id,
                voter,
                peer_id,
                address,
            });
        }
        if !(1..=5).contains(&voters) {
            return Err(invalid("invalid range voter count"));
        }
        ranges.push(AggregateRange {
            range_id,
            group_id,
            epoch,
            configuration_version,
            lower,
            upper,
            topology,
            replicas,
        });
    }
    r.end()?;
    Ok(AggregateDirectory {
        canonical: encoded.to_vec(),
        cluster_id,
        database_id,
        domain_id,
        schema_epoch,
        schema_digest,
        generation,
        routing_prefix_bytes,
        columns,
        ranges,
    })
}
impl AggregateExportDefinition {
    pub fn canonical(&self) -> &[u8] {
        &self.canonical
    }
    pub fn decode(encoded: Vec<u8>, status: &ExportStatus) -> Result<Self> {
        if encoded.len() as i64 != status.definition_bytes
            || encoded.len() as i64 > MAX_WITNESS_BYTES
            || <[u8; 32]>::from(Sha256::digest(&encoded)) != status.definition_digest
        {
            return Err(invalid("aggregate definition digest/size changed"));
        }
        let mut r = Read(&encoded);
        if r.i32()? != 0x44455844 {
            return Err(invalid("invalid aggregate definition magic"));
        }
        let job_id = r.id()?;
        let request_id = r.id()?;
        let roster = roster(r.blob(1068)?)?;
        let maximum_chunks = r.i32()?;
        let maximum_journal_bytes = r.i64()?;
        let maximum_disk_bytes = r.i64()?;
        let maximum_rows = r.i32()?;
        let maximum_payload_bytes = r.i64()?;
        let maximum_file_bytes = r.i64()?;
        let nanos = r.i64()?;
        let millis = r.i64()?;
        if nanos < 1 || millis < 1 {
            return Err(invalid("invalid aggregate export duration"));
        }
        let limits = Limits {
            maximum_chunks,
            maximum_journal_bytes,
            maximum_disk_bytes,
            maximum_rows,
            maximum_payload_bytes,
            maximum_file_bytes,
            maximum_chunk_duration: Duration::from_nanos(nanos as u64),
            snapshot_ttl: Duration::from_millis(millis as u64),
        };
        limits.encode()?;
        let count = r.count(64, 12)?;
        let mut domains = Vec::with_capacity(count);
        let mut all_api = HashSet::new();
        let mut selected = Vec::new();
        for _ in 0..count {
            let dir = r.blob(1024 * 1024)?;
            let canonical_schema = r.blob(MAX_SCHEMA_BYTES)?.to_vec();
            let schema = ExportSchema::decode(&canonical_schema)?;
            let directory = directory(dir, &schema)?;
            if schema.cluster_id != roster.cluster_id
                || schema.database_id != roster.database_id
                || domains.last().is_some_and(|d: &AggregateExportDomain| {
                    d.directory.domain_id >= directory.domain_id
                })
                || schema.columns.iter().any(|c| !all_api.insert(c.api_id))
            {
                return Err(invalid("aggregate domain namespace/API identity changed"));
            }
            let count = r.count(MAX_COLUMNS, 8)?;
            let selected_columns = (0..count).map(|_| r.i64()).collect::<Result<Vec<_>>>()?;
            if !ordered(&selected_columns)
                || selected_columns
                    .iter()
                    .any(|id| !schema.columns.iter().any(|c| c.api_id == *id))
            {
                return Err(invalid("aggregate selection missing from schema"));
            }
            selected.extend_from_slice(&selected_columns);
            domains.push(AggregateExportDomain {
                directory,
                schema,
                selected_columns,
                canonical_schema,
            });
        }
        r.end()?;
        selected.sort_unstable();
        if job_id != status.job_id
            || roster.generation != status.roster_generation
            || count != status.domain_count as usize
            || domains
                .iter()
                .map(|d| d.directory.domain_id)
                .collect::<Vec<_>>()
                != roster.domains
            || selected != status.columns
            || selected.is_empty()
            || selected.len() > MAX_COLUMNS
        {
            return Err(invalid("aggregate definition roster/job/selection changed"));
        }
        Ok(Self {
            canonical: encoded,
            job_id,
            request_id,
            roster,
            limits,
            domains,
            digest: status.definition_digest,
        })
    }
}
#[derive(Clone, Debug)]
struct Fence {
    canonical: Vec<u8>,
    id: ExportId,
    groups: Vec<ExportId>,
}
fn fence(
    encoded: &[u8],
    domain: &AggregateExportDomain,
    covered: &mut HashSet<ExportId>,
) -> Result<Fence> {
    if !(172..=2 * 1024 * 1024 - 8192).contains(&encoded.len()) {
        return Err(invalid("invalid aggregate snapshot fence size"));
    }
    let mut r = Read(encoded);
    let cluster = r.id()?;
    let database = r.id()?;
    let dom = r.id()?;
    let id = r.id()?;
    let _home = r.id()?;
    let epoch = r.i64()?;
    let op = r.control()?;
    let generation = r.i64()?;
    let schema_epoch = r.i64()?;
    let digest = r.digest()?;
    if cluster != domain.schema.cluster_id
        || database != domain.schema.database_id
        || dom != domain.schema.domain_id
        || epoch < 0
        || generation != domain.directory.generation
        || schema_epoch != domain.schema.schema_epoch
        || digest != domain.schema.digest
    {
        return Err(invalid("snapshot fence differs from frozen domain"));
    }
    let count = r.count(64, 165)?;
    if count == 0 {
        return Err(invalid("empty snapshot fence"));
    }
    let mut groups = Vec::with_capacity(count);
    let mut ranges = HashSet::new();
    let mut controls = HashSet::from([op]);
    for _ in 0..count {
        let range_id = r.id()?;
        let group = r.id()?;
        let epoch = r.i64()?;
        let digest = r.digest()?;
        let install = r.control()?;
        let release = r.control()?;
        let range = domain
            .directory
            .ranges
            .iter()
            .find(|range| range.group_id == group)
            .ok_or_else(|| invalid("foreign snapshot range"))?;
        if groups.last().is_some_and(|id| *id >= group)
            || !covered.insert(group)
            || !ranges.insert(range_id)
            || !controls.insert(install)
            || !controls.insert(release)
            || range_id != range.range_id
            || epoch != range.epoch
            || digest != domain.schema.digest
        {
            return Err(invalid("snapshot participant/control identity changed"));
        }
        groups.push(group);
        let count = r.count(256, 25)?;
        let mut columns = Vec::with_capacity(count);
        let mut bound_bytes = 0;
        for _ in 0..count {
            let kind = r.take(1)?[0];
            let column = r.id()?;
            let lower = r.bound()?;
            let upper = r.bound()?;
            bound_bytes += lower.as_ref().map_or(0, Vec::len) + upper.as_ref().map_or(0, Vec::len);
            if kind != 2
                || lower != range.lower
                || upper != range.upper
                || bound_bytes > MAX_SCHEMA_BYTES
            {
                return Err(invalid(
                    "snapshot scope is not a full bounded column interval",
                ));
            }
            columns.push(column);
        }
        if !ordered(&columns) || columns != domain.directory.columns {
            return Err(invalid("snapshot scope column coverage changed"));
        }
    }
    r.end()?;
    Ok(Fence {
        canonical: encoded.to_vec(),
        id,
        groups,
    })
}
fn backup(encoded: &[u8], definition: &AggregateExportDefinition) -> Result<Vec<Fence>> {
    let mut r = Read(encoded);
    if r.i32()? != 0x44424244
        || r.id()? != definition.job_id
        || roster(r.blob(1068)?)? != definition.roster
    {
        return Err(invalid("capture barrier roster/job changed"));
    }
    let ranges = r.i32()?;
    let pairs = r.i32()?;
    let chunks = r.i32()?;
    let archive = r.i64()?;
    let metadata = r.i32()?;
    if !(1..=65536).contains(&ranges)
        || !(1..=65536 * 256).contains(&pairs)
        || !(1..=1048576).contains(&chunks)
        || archive < 1
        || !(1..=MAX_WITNESS_BYTES).contains(&(metadata as i64))
        || encoded.len() > metadata as usize
    {
        return Err(invalid("invalid capture barrier global limits"));
    }
    let count = r.count(64, 12)?;
    if count != definition.domains.len() {
        return Err(invalid("capture barrier domain coverage changed"));
    }
    let mut fences = Vec::new();
    let mut fence_ids = HashSet::new();
    let mut all_ranges = HashSet::new();
    let mut all_groups = HashSet::new();
    let mut all_columns = HashSet::new();
    let mut column_pairs = 0i64;
    for domain in &definition.domains {
        if r.blob(1024 * 1024)? != domain.directory.canonical
            || r.blob(MAX_SCHEMA_BYTES)? != domain.canonical_schema
        {
            return Err(invalid("capture barrier frozen vector changed"));
        }
        for range in &domain.directory.ranges {
            if !all_ranges.insert(range.range_id) || !all_groups.insert(range.group_id) {
                return Err(invalid("capture range/group shared by domains"));
            }
        }
        for column in &domain.schema.columns {
            if !all_columns.insert(column.column_id) {
                return Err(invalid("capture column shared by domains"));
            }
        }
        column_pairs +=
            domain.directory.ranges.len() as i64 * domain.directory.columns.len() as i64;
        let count = r.count(65536, 4)?;
        let mut covered = HashSet::new();
        if domain.directory.columns.is_empty() {
            if count != 0 {
                return Err(invalid("empty schema invented capture participants"));
            }
        } else if count == 0
            || domain
                .directory
                .ranges
                .iter()
                .any(|range| range.topology != 1)
        {
            return Err(invalid("capture lacks full ACTIVE domain vector"));
        }
        for _ in 0..count {
            let fence = fence(r.blob(2 * 1024 * 1024 - 8192)?, domain, &mut covered)?;
            if !fence_ids.insert(fence.id) {
                return Err(invalid("duplicate global fence identity"));
            }
            fences.push(fence);
        }
        if !domain.directory.columns.is_empty() && covered.len() != domain.directory.ranges.len() {
            return Err(invalid("capture range coverage incomplete"));
        }
    }
    r.end()?;
    if all_groups.len() > ranges as usize
        || column_pairs > pairs as i64
        || column_pairs > chunks as i64
    {
        return Err(invalid("capture vector exceeds global range/pair limits"));
    }
    Ok(fences)
}
impl AggregateExportCaptures {
    pub fn canonical(&self) -> &[u8] {
        &self.canonical
    }
    pub fn decode(
        encoded: Vec<u8>,
        definition: &AggregateExportDefinition,
        status: &ExportStatus,
    ) -> Result<Self> {
        let digest = status
            .capture_digest
            .ok_or_else(|| invalid("capture witness not forced"))?;
        if status.job_id != definition.job_id
            || status.definition_digest != definition.digest
            || encoded.len() as i64 != status.capture_bytes
            || encoded.len() as i64 > MAX_WITNESS_BYTES
            || <[u8; 32]>::from(Sha256::digest(&encoded)) != digest
        {
            return Err(invalid("capture witness digest/job/size changed"));
        }
        let mut r = Read(&encoded);
        if r.i32()? != 0x44455843 || r.digest()? != definition.digest {
            return Err(invalid("capture witness definition changed"));
        }
        let fences = backup(r.blob(MAX_WITNESS_BYTES as usize)?, definition)?;
        let expected: HashMap<_, _> = fences.iter().map(|f| (f.id, f)).collect();
        let count = r.count(65536, 4)?;
        if count != fences.len() {
            return Err(invalid("capture fence coverage changed"));
        }
        let mut captures = Vec::new();
        let mut groups = HashSet::new();
        let mut previous = None;
        for _ in 0..count {
            let encoded = r.blob(2 * 1024 * 1024)?;
            let mut progress = Read(encoded);
            let fence = progress.blob(2 * 1024 * 1024 - 8192)?;
            if fence.len() < 64 {
                return Err(invalid("truncated captured fence"));
            }
            let id: ExportId = fence[48..64].try_into().unwrap();
            let expected = expected
                .get(&id)
                .ok_or_else(|| invalid("foreign captured fence"))?;
            if previous.is_some_and(|p| p >= id) || fence != expected.canonical {
                return Err(invalid("noncanonical captured fence vector"));
            }
            previous = Some(id);
            let phase = progress.take(1)?[0];
            let consumed = progress.take(1)?[0];
            let count = progress.count(64, 17)?;
            if phase > 1
                || consumed > 1
                || phase == 0 && consumed == 1
                || count != expected.groups.len()
            {
                return Err(invalid("capture phase/participant coverage changed"));
            }
            for expected_group in &expected.groups {
                let group_id = progress.id()?;
                if group_id != *expected_group
                    || !groups.insert(group_id)
                    || progress.take(1)?[0] != 3
                {
                    return Err(invalid("capture requires complete ready/token vector"));
                }
                let ready = progress.position()?;
                let owner_incarnation = progress.id()?;
                let snapshot_id = progress.id()?;
                let position = progress.position()?;
                if position.term < ready.term
                    || position.index < ready.index
                    || position.index == ready.index && position.term != ready.term
                {
                    return Err(invalid("snapshot capture precedes ready proof"));
                }
                captures.push(SnapshotCapture {
                    group_id,
                    ready,
                    owner_incarnation,
                    snapshot_id,
                    position,
                });
            }
            progress.end()?;
        }
        let count = r.count(1024, 68)?;
        let mut cohorts = Vec::with_capacity(count);
        let mut covered = HashSet::new();
        let mut previous = None;
        for _ in 0..count {
            let id = r.id()?;
            let node_incarnation = r.id()?;
            let process_incarnation = r.id()?;
            let pool_id = r.id()?;
            let count = r.count(1024, 16)?;
            let cohort_groups = (0..count).map(|_| r.id()).collect::<Result<Vec<_>>>()?;
            if previous.is_some_and(|p| p >= id)
                || cohort_groups.is_empty()
                || !ordered(&cohort_groups)
                || cohort_groups.iter().any(|g| !covered.insert(*g))
            {
                return Err(invalid("noncanonical duplicate capture cohort/group"));
            }
            previous = Some(id);
            cohorts.push(CaptureCohort {
                id,
                node_incarnation,
                process_incarnation,
                pool_id,
                groups: cohort_groups,
            });
        }
        r.end()?;
        if covered != groups {
            return Err(invalid("capture cohort coverage differs from token vector"));
        }
        Ok(Self {
            canonical: encoded,
            definition_digest: definition.digest,
            digest,
            captures,
            cohorts,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn int(out: &mut Vec<u8>, v: i32) {
        out.extend_from_slice(&v.to_be_bytes())
    }
    fn long(out: &mut Vec<u8>, v: i64) {
        out.extend_from_slice(&v.to_be_bytes())
    }
    fn uuid(out: &mut Vec<u8>, v: u8) {
        out.extend_from_slice(&[v; 16])
    }
    fn blob(out: &mut Vec<u8>, v: &[u8]) {
        int(out, v.len() as i32);
        out.extend_from_slice(v)
    }
    fn schema(domain: u8, has_column: bool) -> Vec<u8> {
        let mut b = Vec::new();
        for v in [0x44534348, 1, 1] {
            int(&mut b, v)
        }
        for v in [3, 4, domain] {
            uuid(&mut b, v)
        }
        long(&mut b, 7);
        int(&mut b, 0);
        int(&mut b, has_column as i32);
        if has_column {
            uuid(&mut b, 9);
            long(&mut b, 1);
            blob(&mut b, b"a");
            int(&mut b, 1);
            int(&mut b, 4);
            int(&mut b, 0);
            b.extend_from_slice(&[1, 0]);
        }
        b
    }
    fn dir(domain: u8, has_column: bool) -> Vec<u8> {
        let mut b = Vec::new();
        int(&mut b, 1);
        for v in [3, 4, domain] {
            uuid(&mut b, v)
        }
        long(&mut b, 7);
        b.extend_from_slice(&Sha256::digest(schema(domain, has_column)));
        long(&mut b, 8);
        int(&mut b, 0);
        int(&mut b, has_column as i32);
        if has_column {
            uuid(&mut b, 9)
        }
        int(&mut b, 1);
        uuid(&mut b, if has_column { 11 } else { 13 });
        uuid(&mut b, if has_column { 10 } else { 12 });
        long(&mut b, 1);
        long(&mut b, 2);
        int(&mut b, -1);
        int(&mut b, -1);
        b.push(1);
        int(&mut b, 1);
        for v in [14, 15, 16] {
            uuid(&mut b, v)
        }
        b.push(1);
        blob(&mut b, b"peer");
        blob(&mut b, b"localhost:9000");
        b
    }
    fn roster_bytes() -> Vec<u8> {
        let mut b = Vec::new();
        uuid(&mut b, 3);
        uuid(&mut b, 4);
        long(&mut b, 1);
        int(&mut b, 2);
        uuid(&mut b, 5);
        uuid(&mut b, 6);
        b
    }
    fn definition_bytes() -> Vec<u8> {
        let mut b = Vec::new();
        int(&mut b, 0x44455844);
        uuid(&mut b, 1);
        uuid(&mut b, 2);
        blob(&mut b, &roster_bytes());
        let l = Limits::default().encode().unwrap();
        int(&mut b, l.maximum_chunks);
        long(&mut b, l.maximum_journal_bytes);
        long(&mut b, l.maximum_disk_bytes);
        int(&mut b, l.maximum_rows);
        long(&mut b, l.maximum_payload_bytes);
        long(&mut b, l.maximum_file_bytes);
        long(&mut b, l.maximum_chunk_duration_nanos);
        long(&mut b, l.snapshot_ttl_millis);
        int(&mut b, 2);
        for (dom, selected) in [(5, true), (6, false)] {
            blob(&mut b, &dir(dom, selected));
            blob(&mut b, &schema(dom, selected));
            int(&mut b, selected as i32);
            if selected {
                long(&mut b, 1)
            }
        }
        b
    }
    fn status() -> ExportStatus {
        let b = definition_bytes();
        ExportStatus {
            job_id: [1; 16],
            phase: Phase::Ready,
            columns: vec![1],
            participant_count: 1,
            chunk_count: 0,
            acknowledged_count: 0,
            file_bytes: 0,
            cleanup_pending: false,
            definition_digest: Sha256::digest(&b).into(),
            definition_bytes: b.len() as i64,
            roster_generation: 1,
            domain_count: 2,
            capture_digest: None,
            capture_bytes: 0,
        }
    }
    fn control(b: &mut Vec<u8>, sequence: i64) {
        uuid(b, 21);
        long(b, 1);
        long(b, sequence)
    }
    fn fence_bytes() -> Vec<u8> {
        let mut b = Vec::new();
        for v in [3, 4, 5, 20, 22] {
            uuid(&mut b, v)
        }
        long(&mut b, 1);
        control(&mut b, 0);
        long(&mut b, 8);
        long(&mut b, 7);
        b.extend_from_slice(&Sha256::digest(schema(5, true)));
        int(&mut b, 1);
        uuid(&mut b, 11);
        uuid(&mut b, 10);
        long(&mut b, 1);
        b.extend_from_slice(&Sha256::digest(schema(5, true)));
        control(&mut b, 1);
        control(&mut b, 2);
        int(&mut b, 1);
        b.push(2);
        uuid(&mut b, 9);
        int(&mut b, -1);
        int(&mut b, -1);
        b
    }
    fn barrier_bytes() -> Vec<u8> {
        let mut b = Vec::new();
        int(&mut b, 0x44424244);
        uuid(&mut b, 1);
        blob(&mut b, &roster_bytes());
        int(&mut b, 2);
        int(&mut b, 1);
        int(&mut b, 1);
        long(&mut b, 1024 * 1024 * 1024);
        int(&mut b, MAX_WITNESS_BYTES as i32);
        int(&mut b, 2);
        for (dom, selected) in [(5, true), (6, false)] {
            blob(&mut b, &dir(dom, selected));
            blob(&mut b, &schema(dom, selected));
            int(&mut b, selected as i32);
            if selected {
                blob(&mut b, &fence_bytes())
            }
        }
        b
    }
    fn progress_bytes() -> Vec<u8> {
        let mut b = Vec::new();
        blob(&mut b, &fence_bytes());
        b.extend_from_slice(&[1, 0]);
        int(&mut b, 1);
        uuid(&mut b, 10);
        b.push(3);
        long(&mut b, 2);
        long(&mut b, 3);
        uuid(&mut b, 15);
        uuid(&mut b, 23);
        long(&mut b, 2);
        long(&mut b, 4);
        b
    }
    fn captures_bytes() -> Vec<u8> {
        let mut b = Vec::new();
        int(&mut b, 0x44455843);
        b.extend_from_slice(&Sha256::digest(definition_bytes()));
        blob(&mut b, &barrier_bytes());
        int(&mut b, 1);
        blob(&mut b, &progress_bytes());
        int(&mut b, 1);
        for v in [24, 15, 25, 26] {
            uuid(&mut b, v)
        }
        int(&mut b, 1);
        uuid(&mut b, 10);
        b
    }
    fn with_capture() -> ExportStatus {
        let mut s = status();
        let b = captures_bytes();
        s.capture_digest = Some(Sha256::digest(&b).into());
        s.capture_bytes = b.len() as i64;
        s
    }
    #[test]
    fn complete_definition_and_capture_workflow_preserves_advertised_empty_domain() {
        let s = with_capture();
        let definition = AggregateExportDefinition::decode(definition_bytes(), &s).unwrap();
        assert_eq!(definition.roster.domains, vec![[5; 16], [6; 16]]);
        assert_eq!(definition.domains[0].selected_columns, vec![1]);
        assert!(definition.domains[1].schema.columns.is_empty());
        assert!(definition.domains[1].selected_columns.is_empty());
        assert_eq!(definition.limits.snapshot_ttl, Duration::from_secs(60));
        let captures = AggregateExportCaptures::decode(captures_bytes(), &definition, &s).unwrap();
        assert_eq!(captures.captures.len(), 1);
        assert_eq!(
            captures.captures[0].position,
            CapturePosition { term: 2, index: 4 }
        );
        assert_eq!(captures.cohorts[0].groups, vec![[10; 16]]);
        assert_eq!(captures.canonical(), captures_bytes());
    }
    #[test]
    fn qualified_definition_and_capture_pages_recompose_before_codec_validation() {
        let status = with_capture();
        let compose = |bytes: Vec<u8>, kind: WitnessKind, digest: [u8; 32]| {
            let total_bytes = bytes.len() as i64;
            let middle = bytes.len() / 2;
            let pages = vec![
                WitnessPage {
                    job_id: status.job_id,
                    kind,
                    digest,
                    total_bytes,
                    offset: 0,
                    data: bytes[..middle].to_vec(),
                    has_more: true,
                },
                WitnessPage {
                    job_id: status.job_id,
                    kind,
                    digest,
                    total_bytes,
                    offset: middle as i64,
                    data: bytes[middle..].to_vec(),
                    has_more: false,
                },
            ];
            WitnessPage::assemble(pages, status.job_id, kind, digest, total_bytes).unwrap()
        };
        let definition = AggregateExportDefinition::decode(
            compose(
                definition_bytes(),
                WitnessKind::Definition,
                status.definition_digest,
            ),
            &status,
        )
        .unwrap();
        let captures = AggregateExportCaptures::decode(
            compose(
                captures_bytes(),
                WitnessKind::Captures,
                status.capture_digest.unwrap(),
            ),
            &definition,
            &status,
        )
        .unwrap();
        assert_eq!(captures.definition_digest, definition.digest);
        assert_eq!(captures.captures[0].snapshot_id, [23; 16]);
    }
    #[test]
    fn definition_rejects_changed_status_roster_schema_selection_and_canonical_bytes() {
        let s = status();
        let mut wrong = s.clone();
        wrong.roster_generation += 1;
        assert!(AggregateExportDefinition::decode(definition_bytes(), &wrong).is_err());
        let mut wrong = s.clone();
        wrong.columns = vec![2];
        assert!(AggregateExportDefinition::decode(definition_bytes(), &wrong).is_err());
        let mut wrong = s.clone();
        wrong.domain_count = 1;
        assert!(AggregateExportDefinition::decode(definition_bytes(), &wrong).is_err());
        let mut corrupted = definition_bytes();
        corrupted[0] ^= 1;
        assert!(AggregateExportDefinition::decode(corrupted.clone(), &s).is_err());
        let mut altered = s.clone();
        altered.definition_digest = Sha256::digest(&corrupted).into();
        assert!(AggregateExportDefinition::decode(corrupted, &altered).is_err());
        let mut dir = dir(5, true);
        dir[60] ^= 1;
        assert!(directory(&dir, &ExportSchema::decode(&schema(5, true)).unwrap()).is_err());
        let mut noncanonical = roster_bytes();
        noncanonical[44..60].copy_from_slice(&[6; 16]);
        assert!(roster(&noncanonical).is_err());
    }
    #[test]
    fn captures_reject_missing_tokens_changed_full_scope_and_cohort_coverage() {
        let s = with_capture();
        let definition = AggregateExportDefinition::decode(definition_bytes(), &s).unwrap();
        let mut encoded = captures_bytes();
        let n = encoded.len();
        encoded[n - 1] ^= 1;
        let mut altered = s.clone();
        altered.capture_digest = Some(Sha256::digest(&encoded).into());
        assert!(AggregateExportCaptures::decode(encoded, &definition, &altered).is_err());
        assert!(AggregateExportCaptures::decode(captures_bytes(), &definition, &status()).is_err());
        let mut encoded = captures_bytes();
        encoded[4] ^= 1;
        altered.capture_digest = Some(Sha256::digest(&encoded).into());
        assert!(AggregateExportCaptures::decode(encoded, &definition, &altered).is_err());
        let mut fence = fence_bytes();
        let n = fence.len();
        fence[n - 25] = 1;
        let mut covered = HashSet::new();
        assert!(super::fence(&fence, &definition.domains[0], &mut covered).is_err());
        let mut encoded = captures_bytes();
        let progress = progress_bytes();
        let start = encoded
            .windows(progress.len())
            .position(|v| v == progress)
            .unwrap();
        encoded[start + 4 + fence_bytes().len() + 6 + 16] = 1;
        altered.capture_digest = Some(Sha256::digest(&encoded).into());
        assert!(AggregateExportCaptures::decode(encoded, &definition, &altered).is_err());
    }
}
