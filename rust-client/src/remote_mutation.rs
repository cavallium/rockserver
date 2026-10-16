//! Explicit durable mutation lifecycle. Persist the owner, request UUID and sequence before
//! prepare. Dropping a call cancels observation only: query the same identity after ambiguity.
//! No method retries, selects a sequence, replays a source, or acknowledges automatically.
use crate::{
    client::{await_bound_call, BoundDeadline, DeadlineInterceptor},
    proto, RequestContext, RockserverClient,
};
use prost::Message;
use sha2::{Digest, Sha256};
use tonic::{service::interceptor::InterceptedService, transport::Channel, Status};
type Result<T> = std::result::Result<T, Status>;
const MAX_BYTES: usize = 2 * 1024 * 1024;
const MAX_FRAME: usize = MAX_BYTES + 64 * 1024;
fn invalid(message: &str) -> Status {
    Status::invalid_argument(message)
}
fn malformed(message: &str) -> Status {
    Status::data_loss(message)
}
fn array<const N: usize>(bytes: &[u8]) -> Result<[u8; N]> {
    bytes
        .try_into()
        .map_err(|_| malformed("invalid identity/digest size"))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RemoteMutationKind {
    Point,
    Range,
    Bulk,
    Atomic,
}
impl RemoteMutationKind {
    fn name(self) -> &'static str {
        match self {
            Self::Point => "POINT",
            Self::Range => "RANGE",
            Self::Bulk => "BULK",
            Self::Atomic => "ATOMIC",
        }
    }
    fn parse(value: &str) -> Result<Self> {
        match value {
            "POINT" => Ok(Self::Point),
            "RANGE" => Ok(Self::Range),
            "BULK" => Ok(Self::Bulk),
            "ATOMIC" => Ok(Self::Atomic),
            _ => Err(malformed("unknown mutation kind")),
        }
    }
}
/// Original directory and schema authority for one domain, ordered by unsigned UUID bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteMutationDomainBinding {
    pub domain_id: [u8; 16],
    pub directory_generation: i64,
    pub directory_digest: [u8; 32],
    pub schema_epoch: i64,
    pub schema_digest: [u8; 32],
}
fn bindings_valid(domains: &[RemoteMutationDomainBinding], kind: RemoteMutationKind) -> bool {
    (1..=64).contains(&domains.len())
        && (kind == RemoteMutationKind::Atomic || domains.len() == 1)
        && domains
            .iter()
            .all(|d| d.directory_generation >= 0 && d.schema_epoch >= 0)
        && domains
            .windows(2)
            .all(|pair| pair[0].domain_id < pair[1].domain_id)
}
impl RemoteMutationDomainBinding {
    fn wire(&self) -> proto::RemoteMutationDomainBinding {
        proto::RemoteMutationDomainBinding {
            domain_id: self.domain_id.to_vec(),
            directory_generation: self.directory_generation,
            directory_digest: self.directory_digest.to_vec(),
            schema_epoch: self.schema_epoch,
            schema_digest: self.schema_digest.to_vec(),
        }
    }
    fn decode(w: proto::RemoteMutationDomainBinding) -> Result<Self> {
        Ok(Self {
            domain_id: array(&w.domain_id)?,
            directory_generation: w.directory_generation,
            directory_digest: array(&w.directory_digest)?,
            schema_epoch: w.schema_epoch,
            schema_digest: array(&w.schema_digest)?,
        })
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteMutationOwner {
    pub cluster_id: [u8; 16],
    pub database_id: [u8; 16],
    pub owner_id: [u8; 16],
    pub owner_epoch: i64,
    /// Advisory only. Persist an explicitly selected sequence before preparing.
    pub next_request_sequence: i64,
    pub kind: RemoteMutationKind,
    pub domains: Vec<RemoteMutationDomainBinding>,
}
impl RemoteMutationOwner {
    fn wire(&self) -> Result<proto::RemoteMutationAuthority> {
        if self.owner_epoch < 0
            || self.next_request_sequence < 0
            || !bindings_valid(&self.domains, self.kind)
        {
            return Err(invalid("invalid owner authority/domain vector"));
        }
        Ok(proto::RemoteMutationAuthority {
            cluster_id: self.cluster_id.to_vec(),
            database_id: self.database_id.to_vec(),
            owner_id: self.owner_id.to_vec(),
            owner_epoch: self.owner_epoch,
            next_request_sequence: self.next_request_sequence,
            kind: self.kind.name().into(),
            domains: self
                .domains
                .iter()
                .map(RemoteMutationDomainBinding::wire)
                .collect(),
        })
    }
    fn decode(w: proto::RemoteMutationAuthority) -> Result<Self> {
        if w.owner_epoch < 0 || w.next_request_sequence < 0 || !(1..=64).contains(&w.domains.len())
        {
            return Err(malformed("invalid owner authority/domain count"));
        }
        let owner = Self {
            cluster_id: array(&w.cluster_id)?,
            database_id: array(&w.database_id)?,
            owner_id: array(&w.owner_id)?,
            owner_epoch: w.owner_epoch,
            next_request_sequence: w.next_request_sequence,
            kind: RemoteMutationKind::parse(&w.kind)?,
            domains: w
                .domains
                .into_iter()
                .map(RemoteMutationDomainBinding::decode)
                .collect::<Result<_>>()?,
        };
        if !bindings_valid(&owner.domains, owner.kind) {
            return Err(malformed("noncanonical owner domain authority"));
        }
        Ok(owner)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RemoteMutationIdentity {
    pub request_id: [u8; 16],
    pub request_sequence: i64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteMutationToken(Vec<u8>);
impl RemoteMutationToken {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if !(213..=6261).contains(&bytes.len()) {
            return Err(malformed("invalid mutation token size"));
        }
        let kind = match bytes[80] {
            0 => RemoteMutationKind::Point,
            1 => RemoteMutationKind::Range,
            2 => RemoteMutationKind::Bulk,
            3 => RemoteMutationKind::Atomic,
            _ => return Err(malformed("unknown mutation token kind")),
        };
        let count = i32::from_be_bytes(array(&bytes[81..85])?);
        if !(1..=64).contains(&count)
            || bytes.len() != 117 + 96 * count as usize
            || kind != RemoteMutationKind::Atomic && count != 1
            || i64::from_be_bytes(array(&bytes[64..72])?) < 0
            || i64::from_be_bytes(array(&bytes[72..80])?) < 0
        {
            return Err(malformed("invalid mutation token identity/domain count"));
        }
        let mut previous: Option<&[u8]> = None;
        for binding in bytes[85..bytes.len() - 32].chunks_exact(96) {
            if previous.is_some_and(|id| id >= &binding[..16])
                || i64::from_be_bytes(array(&binding[16..24])?) < 0
                || i64::from_be_bytes(array(&binding[56..64])?) < 0
            {
                return Err(malformed("noncanonical token domain authority"));
            }
            previous = Some(&binding[..16]);
        }
        Ok(Self(bytes.to_vec()))
    }
    pub fn encode(&self) -> Vec<u8> {
        self.0.clone()
    }
    pub fn kind(&self) -> RemoteMutationKind {
        match self.0[80] {
            0 => RemoteMutationKind::Point,
            1 => RemoteMutationKind::Range,
            2 => RemoteMutationKind::Bulk,
            _ => RemoteMutationKind::Atomic,
        }
    }
    pub fn input_digest(&self) -> [u8; 32] {
        self.0[self.0.len() - 32..].try_into().unwrap()
    }
    fn expected(
        owner: &RemoteMutationOwner,
        id: RemoteMutationIdentity,
        digest: [u8; 32],
    ) -> Result<Self> {
        owner.wire()?;
        if id.request_sequence < 0 {
            return Err(invalid("negative request sequence"));
        }
        let mut bytes = Vec::with_capacity(117 + 96 * owner.domains.len());
        for uuid in [
            &owner.cluster_id,
            &owner.database_id,
            &owner.owner_id,
            &id.request_id,
        ] {
            bytes.extend_from_slice(uuid);
        }
        bytes.extend_from_slice(&owner.owner_epoch.to_be_bytes());
        bytes.extend_from_slice(&id.request_sequence.to_be_bytes());
        bytes.push(owner.kind as u8);
        bytes.extend_from_slice(&(owner.domains.len() as i32).to_be_bytes());
        for domain in &owner.domains {
            bytes.extend_from_slice(&domain.domain_id);
            bytes.extend_from_slice(&domain.directory_generation.to_be_bytes());
            bytes.extend_from_slice(&domain.directory_digest);
            bytes.extend_from_slice(&domain.schema_epoch.to_be_bytes());
            bytes.extend_from_slice(&domain.schema_digest);
        }
        bytes.extend_from_slice(&digest);
        Self::decode(&bytes)
    }
    fn prepared(w: proto::RemoteMutationTokenResponse, expected: Self) -> Result<Self> {
        let actual = Self::decode(&w.token)?;
        if actual != expected {
            return Err(malformed("crossed prepared mutation identity/input"));
        }
        Ok(actual)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MutationCommandStatus {
    Success,
    ResultTooLarge,
    Conflict,
    SessionExpired,
    StaleSchema,
}
impl MutationCommandStatus {
    fn parse(s: &str) -> Result<Self> {
        match s {
            "SUCCESS" => Ok(Self::Success),
            "RESULT_TOO_LARGE" => Ok(Self::ResultTooLarge),
            "CONFLICT" => Ok(Self::Conflict),
            "SESSION_EXPIRED" => Ok(Self::SessionExpired),
            "STALE_SCHEMA" => Ok(Self::StaleSchema),
            _ => Err(malformed("unknown terminal outcome")),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MutationCommandResult {
    pub status: MutationCommandStatus,
    pub values: Vec<Option<Vec<u8>>>,
}
impl MutationCommandResult {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if !(8..=MAX_BYTES).contains(&bytes.len()) {
            return Err(malformed("invalid command result size"));
        }
        let mut cursor = 0;
        fn int(bytes: &[u8], cursor: &mut usize) -> Result<i32> {
            let end = cursor
                .checked_add(4)
                .ok_or_else(|| malformed("truncated result"))?;
            let value = i32::from_be_bytes(array(
                bytes
                    .get(*cursor..end)
                    .ok_or_else(|| malformed("truncated result"))?,
            )?);
            *cursor = end;
            Ok(value)
        }
        let status = match int(bytes, &mut cursor)? {
            0 => MutationCommandStatus::Success,
            1 => MutationCommandStatus::ResultTooLarge,
            2 => MutationCommandStatus::Conflict,
            3 => MutationCommandStatus::SessionExpired,
            4 => MutationCommandStatus::StaleSchema,
            _ => return Err(malformed("unknown command result status")),
        };
        let count = int(bytes, &mut cursor)?;
        if !(0..=256).contains(&count)
            || count as usize > (bytes.len() - cursor) / 4
            || status != MutationCommandStatus::Success && count != 0
        {
            return Err(malformed("invalid command result count"));
        }
        let mut values = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let length = int(bytes, &mut cursor)?;
            if length < -1 {
                return Err(malformed("invalid result value length"));
            }
            if length == -1 {
                values.push(None);
            } else {
                let end = cursor
                    .checked_add(length as usize)
                    .ok_or_else(|| malformed("invalid value length"))?;
                values.push(Some(
                    bytes
                        .get(cursor..end)
                        .ok_or_else(|| malformed("truncated result value"))?
                        .to_vec(),
                ));
                cursor = end;
            }
        }
        if cursor != bytes.len() {
            return Err(malformed("trailing result bytes"));
        }
        Ok(Self { status, values })
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RemoteMutationState {
    Prepared,
    Pending,
    Terminal,
    Acknowledged,
    Expired,
    OwnerUnavailable,
    Unavailable,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteMutationObservation {
    pub token: RemoteMutationToken,
    pub state: RemoteMutationState,
    pub applied_mutations: i64,
    pub copied_batches: i64,
    pub retained_results: i32,
    pub outcome: Option<MutationCommandStatus>,
    pub result_digest: Option<[u8; 32]>,
}
impl RemoteMutationObservation {
    pub fn progress_known(&self) -> bool {
        self.applied_mutations >= 0 && self.copied_batches >= 0
    }
    fn decode(w: proto::RemoteMutationObservation, expected: &RemoteMutationToken) -> Result<Self> {
        bounded_reply(&w)?;
        let token = RemoteMutationToken::decode(&w.token)?;
        if &token != expected {
            return Err(malformed("crossed mutation token"));
        }
        let state = match w.state.as_str() {
            "PREPARED" => RemoteMutationState::Prepared,
            "PENDING" => RemoteMutationState::Pending,
            "TERMINAL" => RemoteMutationState::Terminal,
            "ACKNOWLEDGED" => RemoteMutationState::Acknowledged,
            "EXPIRED" => RemoteMutationState::Expired,
            "OWNER_UNAVAILABLE" => RemoteMutationState::OwnerUnavailable,
            "UNAVAILABLE" => RemoteMutationState::Unavailable,
            _ => return Err(malformed("unknown mutation state")),
        };
        let outcome = w
            .outcome
            .as_deref()
            .map(MutationCommandStatus::parse)
            .transpose()?;
        let digest = w.result_digest.as_deref().map(array).transpose()?;
        let terminal = matches!(
            state,
            RemoteMutationState::Terminal | RemoteMutationState::Acknowledged
        );
        if w.applied_mutations < -1
            || w.copied_batches < -1
            || (w.applied_mutations < 0) != (w.copied_batches < 0)
            || !(0..=256).contains(&w.retained_results)
            || terminal != outcome.is_some()
            || terminal && (w.applied_mutations < 0 || digest.is_none())
            || state == RemoteMutationState::Expired
                && (w.applied_mutations >= 0 || digest.is_some())
        {
            return Err(malformed("invalid mutation progress/outcome"));
        }
        Ok(Self {
            token,
            state,
            applied_mutations: w.applied_mutations,
            copied_batches: w.copied_batches,
            retained_results: w.retained_results,
            outcome,
            result_digest: digest,
        })
    }
}
#[derive(Clone, Debug)]
pub struct RemoteMutationPage {
    pub status: RemoteMutationObservation,
    pub offset: i32,
    pub next_offset: i32,
    pub rows: Vec<MutationCommandResult>,
    pub encoded_rows: Vec<Vec<u8>>,
    pub has_more: bool,
}
fn budget(offset: i32, items: i32, bytes: i32) -> Result<()> {
    if offset < 0 || !(1..=256).contains(&items) || !(1..=MAX_BYTES as i32).contains(&bytes) {
        Err(invalid("invalid mutation page budget"))
    } else {
        Ok(())
    }
}
impl RemoteMutationPage {
    fn decode(
        w: proto::RemoteMutationPage,
        token: &RemoteMutationToken,
        offset: i32,
        items: i32,
        bytes: i32,
    ) -> Result<Self> {
        budget(offset, items, bytes)?;
        bounded_reply(&w)?;
        let status = RemoteMutationObservation::decode(
            w.status.ok_or_else(|| malformed("missing page status"))?,
            token,
        )?;
        if w.offset != offset
            || w.rows.len() > items as usize
            || offset.checked_add(w.rows.len() as i32) != Some(w.next_offset)
            || w.next_offset > status.retained_results
            || w.has_more != (w.next_offset < status.retained_results)
            || w.rows.iter().map(Vec::len).sum::<usize>() > bytes as usize
        {
            return Err(malformed("invalid result page cursor/count/bytes"));
        }
        let rows = w
            .rows
            .iter()
            .map(|r| MutationCommandResult::decode(r))
            .collect::<Result<_>>()?;
        Ok(Self {
            status,
            offset,
            next_offset: w.next_offset,
            rows,
            encoded_rows: w.rows,
            has_more: w.has_more,
        })
    }
}
/// A logical key and nullable value; components are copied into the request before sending.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteMutationEntry {
    pub key: Vec<Vec<u8>>,
    pub value: Option<Vec<u8>>,
}
#[derive(Clone, Copy, Debug)]
pub enum PointMutationKind {
    Put,
    Delete,
    Merge,
}
impl PointMutationKind {
    fn name(self) -> &'static str {
        match self {
            Self::Put => "PUT",
            Self::Delete => "DELETE",
            Self::Merge => "MERGE",
        }
    }
}
#[derive(Clone, Copy, Debug)]
pub enum MutationCallback {
    Nothing,
    Previous,
    Exists,
    Delta,
    Changed,
    PreviousPresence,
    Merged,
}
impl MutationCallback {
    fn code(self) -> i32 {
        match self {
            Self::Nothing => 0,
            Self::Previous => 1,
            Self::Exists => 4,
            Self::Delta => 5,
            Self::Changed => 7,
            Self::PreviousPresence => 8,
            Self::Merged => 9,
        }
    }
    fn name(self) -> &'static str {
        match self {
            Self::Nothing => "NOTHING",
            Self::Previous => "PREVIOUS",
            Self::Exists => "EXISTS",
            Self::Delta => "DELTA",
            Self::Changed => "CHANGED",
            Self::PreviousPresence => "PREVIOUS_PRESENCE",
            Self::Merged => "MERGED",
        }
    }
}
fn key(w: &[Vec<u8>]) -> Result<proto::KeyTuple> {
    if w.len() > 256 || w.iter().map(|c| c.len().saturating_add(5)).sum::<usize>() > MAX_FRAME {
        return Err(invalid("key exceeds bounds"));
    }
    Ok(proto::KeyTuple { keys: w.to_vec() })
}
fn entries(w: &[RemoteMutationEntry], maximum: usize) -> Result<Vec<proto::RemoteMutationEntry>> {
    if w.len() > maximum {
        return Err(invalid("too many entries"));
    }
    let mut input_bytes = 0usize;
    for row in w {
        if row.key.len() > 256 {
            return Err(invalid("too many key components"));
        }
        for component in &row.key {
            input_bytes = input_bytes.saturating_add(component.len());
        }
        input_bytes = input_bytes.saturating_add(row.value.as_ref().map_or(0, Vec::len));
        if input_bytes > MAX_FRAME {
            return Err(invalid("mutation input exceeds frame bound"));
        }
    }
    let rows = w
        .iter()
        .map(|r| {
            Ok(proto::RemoteMutationEntry {
                key: Some(key(&r.key)?),
                value: r.value.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if rows.iter().map(|r| r.encoded_len() + 5).sum::<usize>() > MAX_FRAME {
        return Err(invalid("mutation input exceeds frame bound"));
    }
    Ok(rows)
}
fn bounded_reply<M: Message>(w: &M) -> Result<()> {
    if w.encoded_len() > MAX_FRAME {
        Err(malformed("mutation response exceeds frame bound"))
    } else {
        Ok(())
    }
}
fn length(h: &mut Sha256, n: usize) -> Result<()> {
    h.update(
        i32::try_from(n)
            .map_err(|_| invalid("input length exceeds bound"))?
            .to_be_bytes(),
    );
    Ok(())
}
fn digest_key(h: &mut Sha256, k: Option<&[Vec<u8>]>) -> Result<()> {
    if let Some(k) = k {
        length(h, k.len())?;
        for c in k {
            length(h, c.len())?;
            h.update(c);
        }
    } else {
        h.update((-1_i32).to_be_bytes());
    }
    Ok(())
}
fn digest_rows(h: &mut Sha256, rows: &[RemoteMutationEntry]) -> Result<()> {
    length(h, rows.len())?;
    for row in rows {
        digest_key(h, Some(&row.key))?;
        if let Some(v) = &row.value {
            length(h, v.len())?;
            h.update(v);
        } else {
            h.update((-1_i32).to_be_bytes());
        }
    }
    Ok(())
}
/// Canonical Java RemoteMutationInputs.batch digest; persist ordinal and these logical rows.
pub fn remote_mutation_batch_digest(
    column: i64,
    merge: bool,
    rows: &[RemoteMutationEntry],
) -> Result<[u8; 32]> {
    entries(rows, 4096)?;
    let mut h = Sha256::new();
    h.update([3]);
    h.update(column.to_be_bytes());
    h.update([merge as u8]);
    digest_rows(&mut h, rows)?;
    Ok(h.finalize().into())
}

/// Ordered atomic write. PUT may carry null for a key-only column; MERGE requires a value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AtomicMutation {
    pub column: i64,
    pub key: Vec<Vec<u8>>,
    pub kind: AtomicMutationKind,
    pub value: Option<Vec<u8>>,
    pub returns: AtomicReturnMode,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AtomicMutationKind {
    Put,
    Delete,
    Merge,
}
impl AtomicMutationKind {
    fn name(self) -> &'static str {
        match self {
            Self::Put => "PUT",
            Self::Delete => "DELETE",
            Self::Merge => "MERGE",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AtomicReturnMode {
    None,
    Previous,
    Current,
}
impl AtomicReturnMode {
    fn name(self) -> &'static str {
        match self {
            Self::None => "NONE",
            Self::Previous => "PREVIOUS",
            Self::Current => "CURRENT",
        }
    }
}
/// Equality against the original capture: null is absent; empty is present empty.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AtomicPredicate {
    pub column: i64,
    pub key: Vec<Vec<u8>>,
    pub expected: Option<Vec<u8>>,
}
fn atomic_key_size(key: &[Vec<u8>]) -> Result<usize> {
    if key.len() > 256 {
        return Err(invalid("too many atomic key components"));
    }
    key.iter().try_fold(4usize, |size, part| {
        size.checked_add(4)
            .and_then(|n| n.checked_add(part.len()))
            .filter(|&n| n <= MAX_BYTES)
            .ok_or_else(|| invalid("atomic key exceeds byte bound"))
    })
}
/// Exact canonical input bytes, checked before copying values or constructing wire entries.
pub fn remote_mutation_atomic_size(
    mutations: &[AtomicMutation],
    predicates: &[AtomicPredicate],
    ttl_millis: i64,
) -> Result<usize> {
    if !(1..=60000).contains(&ttl_millis)
        || mutations.len() > 256
        || predicates.len() > 256
        || !(1..=256).contains(&(mutations.len() + predicates.len()))
    {
        return Err(invalid("invalid atomic count or lifetime"));
    }
    let mut bytes = 17usize;
    for mutation in mutations {
        if mutation.kind == AtomicMutationKind::Delete && mutation.value.is_some()
            || mutation.kind == AtomicMutationKind::Merge && mutation.value.is_none()
        {
            return Err(invalid("invalid atomic mutation value"));
        }
        bytes = bytes
            .checked_add(14 + atomic_key_size(&mutation.key)?)
            .and_then(|n| n.checked_add(mutation.value.as_ref().map_or(0, Vec::len)))
            .filter(|&n| n <= MAX_BYTES)
            .ok_or_else(|| invalid("atomic input exceeds byte bound"))?;
    }
    for predicate in predicates {
        bytes = bytes
            .checked_add(12 + atomic_key_size(&predicate.key)?)
            .and_then(|n| n.checked_add(predicate.expected.as_ref().map_or(0, Vec::len)))
            .filter(|&n| n <= MAX_BYTES)
            .ok_or_else(|| invalid("atomic input exceeds byte bound"))?;
    }
    Ok(bytes)
}
fn digest_nullable(h: &mut Sha256, value: Option<&[u8]>) -> Result<()> {
    if let Some(value) = value {
        length(h, value.len())?;
        h.update(value);
    } else {
        h.update((-1i32).to_be_bytes());
    }
    Ok(())
}
/// Current Java RemoteMutationInputs.atomic digest. Ordered mutations/returns are preserved.
pub fn remote_mutation_atomic_digest(
    mutations: &[AtomicMutation],
    predicates: &[AtomicPredicate],
    ttl_millis: i64,
) -> Result<[u8; 32]> {
    remote_mutation_atomic_size(mutations, predicates, ttl_millis)?;
    let mut h = Sha256::new();
    h.update([4]);
    h.update(ttl_millis.to_be_bytes());
    length(&mut h, mutations.len())?;
    for mutation in mutations {
        h.update(mutation.column.to_be_bytes());
        digest_key(&mut h, Some(&mutation.key))?;
        h.update([mutation.kind as u8]);
        digest_nullable(&mut h, mutation.value.as_deref())?;
        h.update([mutation.returns as u8]);
    }
    length(&mut h, predicates.len())?;
    for predicate in predicates {
        h.update(predicate.column.to_be_bytes());
        digest_key(&mut h, Some(&predicate.key))?;
        digest_nullable(&mut h, predicate.expected.as_deref())?;
    }
    Ok(h.finalize().into())
}
/// Bind subsequent phases to the original call deadline, including readiness-queue time.
fn atomic_request_at<T: Message>(
    message: T,
    deadline: Option<tokio::time::Instant>,
) -> Result<tonic::Request<T>> {
    if message.encoded_len() > MAX_FRAME {
        return Err(invalid("mutation request exceeds frame bound"));
    }
    let mut request = tonic::Request::new(message);
    if let Some(deadline) = deadline {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(tonic::Status::deadline_exceeded(
                "atomic call original deadline expired",
            ));
        }
        request.set_timeout(remaining);
        request.extensions_mut().insert(BoundDeadline(deadline));
    }
    Ok(request)
}

#[derive(Clone, Debug)]
pub struct RemoteMutationClient {
    base: RockserverClient,
    client: proto::remote_mutation_service_client::RemoteMutationServiceClient<
        InterceptedService<Channel, DeadlineInterceptor>,
    >,
}
macro_rules! rpc {
    ($self:expr,$method:ident,$message:expr) => {{
        let message = $message;
        if message.encoded_len() > MAX_FRAME {
            return Err(invalid("mutation request exceeds frame bound"));
        }
        let prepared = $self.base.contextual_request(message)?;
        let mut client = $self.client.clone();
        await_bound_call(prepared.deadline, client.$method(prepared.request))
            .await?
            .into_inner()
    }};
}
macro_rules! token_method {
    ($name:ident) => {
        pub async fn $name(
            &self,
            token: &RemoteMutationToken,
        ) -> Result<RemoteMutationObservation> {
            let w = rpc!(
                self,
                $name,
                proto::RemoteMutationTokenRequest {
                    token: token.0.to_vec(),
                    context: Some(self.base.context.clone())
                }
            );
            RemoteMutationObservation::decode(w, token)
        }
    };
}
impl RemoteMutationClient {
    pub(crate) fn from_client(base: RockserverClient, channel: Channel) -> Self {
        Self {base,client:proto::remote_mutation_service_client::RemoteMutationServiceClient::with_interceptor(channel,DeadlineInterceptor).max_decoding_message_size(MAX_FRAME)}
    }
    pub fn with_context(&self, context: RequestContext) -> Self {
        Self {
            base: self.base.with_context(context),
            client: self.client.clone(),
        }
    }
    pub async fn owners(&self, column: i64) -> Result<Vec<RemoteMutationOwner>> {
        let w = rpc!(
            self,
            owners,
            proto::RemoteMutationOwnersRequest {
                column,
                context: Some(self.base.context.clone())
            }
        );
        bounded_reply(&w)?;
        if w.owners.len() > 4096 {
            return Err(malformed("too many owners"));
        }
        w.owners
            .into_iter()
            .map(RemoteMutationOwner::decode)
            .collect()
    }
    /// Discover durable atomic owners for an explicitly supplied set of logical columns.
    pub async fn atomic_owners(&self, columns: &[i64]) -> Result<Vec<RemoteMutationOwner>> {
        if columns.is_empty() || columns.len() > 256 {
            return Err(invalid("invalid atomic owner column count"));
        }
        let w = rpc!(
            self,
            atomic_owners,
            proto::RemoteMutationAtomicOwnersRequest {
                columns: columns.to_vec(),
                context: Some(self.base.context.clone())
            }
        );
        bounded_reply(&w)?;
        if w.owners.len() > 4096 {
            return Err(malformed("too many atomic owners"));
        }
        w.owners
            .into_iter()
            .map(|wire| {
                let owner = RemoteMutationOwner::decode(wire)?;
                if owner.kind != RemoteMutationKind::Atomic {
                    return Err(malformed("not an atomic owner"));
                }
                Ok(owner)
            })
            .collect()
    }
    /// Persist the selected owner, caller UUID and sequence before prepare. No identity is generated.
    pub async fn prepare_atomic(
        &self,
        owner: &RemoteMutationOwner,
        id: RemoteMutationIdentity,
        mutations: &[AtomicMutation],
        predicates: &[AtomicPredicate],
        ttl_millis: i64,
    ) -> Result<RemoteMutationToken> {
        let deadline = self.base.contextual_request(())?.deadline;
        self.prepare_atomic_at(owner, id, mutations, predicates, ttl_millis, deadline)
            .await
    }
    async fn prepare_atomic_at(
        &self,
        owner: &RemoteMutationOwner,
        id: RemoteMutationIdentity,
        mutations: &[AtomicMutation],
        predicates: &[AtomicPredicate],
        ttl_millis: i64,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<RemoteMutationToken> {
        if owner.kind != RemoteMutationKind::Atomic {
            return Err(invalid("not an atomic owner"));
        }
        let digest = remote_mutation_atomic_digest(mutations, predicates, ttl_millis)?;
        let expected = RemoteMutationToken::expected(owner, id, digest)?;
        let message = proto::RemoteMutationPrepareAtomicRequest {
            owner: Some(owner.wire()?),
            request_id: id.request_id.to_vec(),
            request_sequence: id.request_sequence,
            mutations: mutations
                .iter()
                .map(|m| proto::RemoteMutationAtomicWrite {
                    column: m.column,
                    key: Some(proto::KeyTuple {
                        keys: m.key.clone(),
                    }),
                    kind: m.kind.name().into(),
                    value: m.value.clone(),
                    returns: m.returns.name().into(),
                })
                .collect(),
            predicates: predicates
                .iter()
                .map(|p| proto::RemoteMutationAtomicPredicate {
                    column: p.column,
                    key: Some(proto::KeyTuple {
                        keys: p.key.clone(),
                    }),
                    expected: p.expected.clone(),
                })
                .collect(),
            ttl_millis,
            context: Some(self.base.context.clone()),
        };
        let request = atomic_request_at(message, deadline)?;
        let mut client = self.client.clone();
        let reply = await_bound_call(deadline, client.prepare_atomic(request))
            .await?
            .into_inner();
        RemoteMutationToken::prepared(reply, expected)
    }
    /// Idempotently prepare the caller's original identity and execute its validated token.
    /// Both RPCs share one original deadline. Ambiguous completion never acknowledges or retries.
    pub async fn execute_atomic(
        &self,
        owner: &RemoteMutationOwner,
        id: RemoteMutationIdentity,
        mutations: &[AtomicMutation],
        predicates: &[AtomicPredicate],
        ttl_millis: i64,
    ) -> Result<RemoteMutationObservation> {
        let deadline = self.base.contextual_request(())?.deadline;
        let token = self
            .prepare_atomic_at(owner, id, mutations, predicates, ttl_millis, deadline)
            .await?;
        let request = atomic_request_at(
            proto::RemoteMutationTokenRequest {
                token: token.encode(),
                context: Some(self.base.context.clone()),
            },
            deadline,
        )?;
        let mut client = self.client.clone();
        let reply = await_bound_call(deadline, client.execute(request))
            .await?
            .into_inner();
        RemoteMutationObservation::decode(reply, &token)
    }
    /// Caller must persist owner and identity before calling. The returned token is fully validated.
    pub async fn prepare_point(
        &self,
        owner: &RemoteMutationOwner,
        id: RemoteMutationIdentity,
        column: i64,
        rows: &[RemoteMutationEntry],
        kind: PointMutationKind,
        callback: MutationCallback,
        multi: bool,
    ) -> Result<RemoteMutationToken> {
        if owner.kind != RemoteMutationKind::Point
            || rows.is_empty()
            || !multi && rows.len() != 1
            || matches!(kind, PointMutationKind::Delete) && rows.iter().any(|r| r.value.is_some())
        {
            return Err(invalid("invalid point owner/count/values"));
        }
        let copied = entries(rows, 256)?;
        let mut h = Sha256::new();
        h.update([0]);
        h.update(column.to_be_bytes());
        h.update((kind as i32).to_be_bytes());
        h.update(callback.code().to_be_bytes());
        h.update([multi as u8]);
        digest_rows(&mut h, rows)?;
        let expected = RemoteMutationToken::expected(owner, id, h.finalize().into())?;
        let w = rpc!(
            self,
            prepare_point,
            proto::RemoteMutationPreparePointRequest {
                owner: Some(owner.wire()?),
                request_id: id.request_id.to_vec(),
                request_sequence: id.request_sequence,
                column,
                entries: copied,
                kind: kind.name().into(),
                callback: callback.name().into(),
                multi,
                values_present: !matches!(kind, PointMutationKind::Delete),
                context: Some(self.base.context.clone())
            }
        );
        RemoteMutationToken::prepared(w, expected)
    }
    pub async fn prepare_range(
        &self,
        owner: &RemoteMutationOwner,
        id: RemoteMutationIdentity,
        column: i64,
        lower: Option<&[Vec<u8>]>,
        upper: Option<&[Vec<u8>]>,
    ) -> Result<RemoteMutationToken> {
        if owner.kind != RemoteMutationKind::Range {
            return Err(invalid("not a range owner"));
        }
        let mut h = Sha256::new();
        h.update([1]);
        h.update(column.to_be_bytes());
        digest_key(&mut h, lower)?;
        digest_key(&mut h, upper)?;
        let expected = RemoteMutationToken::expected(owner, id, h.finalize().into())?;
        let w = rpc!(
            self,
            prepare_range,
            proto::RemoteMutationPrepareRangeRequest {
                owner: Some(owner.wire()?),
                request_id: id.request_id.to_vec(),
                request_sequence: id.request_sequence,
                column,
                lower: lower.map(key).transpose()?,
                upper: upper.map(key).transpose()?,
                context: Some(self.base.context.clone())
            }
        );
        RemoteMutationToken::prepared(w, expected)
    }
    pub async fn prepare_bulk(
        &self,
        owner: &RemoteMutationOwner,
        id: RemoteMutationIdentity,
        column: i64,
        merge: bool,
    ) -> Result<RemoteMutationToken> {
        if owner.kind != RemoteMutationKind::Bulk {
            return Err(invalid("not a bulk owner"));
        }
        let mut h = Sha256::new();
        h.update([2]);
        h.update(column.to_be_bytes());
        h.update([merge as u8]);
        let expected = RemoteMutationToken::expected(owner, id, h.finalize().into())?;
        let w = rpc!(
            self,
            prepare_bulk,
            proto::RemoteMutationPrepareBulkRequest {
                owner: Some(owner.wire()?),
                request_id: id.request_id.to_vec(),
                request_sequence: id.request_sequence,
                column,
                merge,
                context: Some(self.base.context.clone())
            }
        );
        RemoteMutationToken::prepared(w, expected)
    }
    token_method!(execute);
    token_method!(resume);
    token_method!(status);
    pub async fn finish_input(
        &self,
        token: &RemoteMutationToken,
    ) -> Result<RemoteMutationObservation> {
        if token.kind() != RemoteMutationKind::Bulk {
            return Err(invalid("not a bulk token"));
        }
        let w = rpc!(
            self,
            finish_input,
            proto::RemoteMutationTokenRequest {
                token: token.0.to_vec(),
                context: Some(self.base.context.clone())
            }
        );
        RemoteMutationObservation::decode(w, token)
    }
    pub async fn results(
        &self,
        token: &RemoteMutationToken,
        offset: i32,
        maximum_items: i32,
        maximum_bytes: i32,
    ) -> Result<RemoteMutationPage> {
        budget(offset, maximum_items, maximum_bytes)?;
        let w = rpc!(
            self,
            results,
            proto::RemoteMutationResultsRequest {
                token: token.0.to_vec(),
                offset,
                maximum_items,
                maximum_bytes,
                context: Some(self.base.context.clone())
            }
        );
        RemoteMutationPage::decode(w, token, offset, maximum_items, maximum_bytes)
    }
    pub async fn acknowledge(
        &self,
        token: &RemoteMutationToken,
        result_digest: [u8; 32],
    ) -> Result<RemoteMutationObservation> {
        let w = rpc!(
            self,
            acknowledge,
            proto::RemoteMutationAcknowledgeRequest {
                token: token.0.to_vec(),
                result_digest: result_digest.to_vec(),
                context: Some(self.base.context.clone())
            }
        );
        let observed = RemoteMutationObservation::decode(w, token)?;
        if observed.result_digest.is_some_and(|d| d != result_digest) {
            return Err(malformed("crossed acknowledgement digest"));
        }
        Ok(observed)
    }
    /// Re-supply the original column and merge flag; they must match the token's bulk header.
    pub async fn append_batch(
        &self,
        token: &RemoteMutationToken,
        column: i64,
        merge: bool,
        ordinal: i64,
        rows: &[RemoteMutationEntry],
    ) -> Result<RemoteMutationObservation> {
        let mut h = Sha256::new();
        h.update([2]);
        h.update(column.to_be_bytes());
        h.update([merge as u8]);
        let header: [u8; 32] = h.finalize().into();
        if token.kind() != RemoteMutationKind::Bulk || ordinal < 0 || header != token.input_digest()
        {
            return Err(invalid("invalid bulk token/header/ordinal"));
        }
        let input_digest = remote_mutation_batch_digest(column, merge, rows)?;
        let w = rpc!(
            self,
            append_batch,
            proto::RemoteMutationAppendRequest {
                token: token.0.to_vec(),
                ordinal,
                entries: entries(rows, 4096)?,
                input_digest: input_digest.to_vec(),
                context: Some(self.base.context.clone())
            }
        );
        RemoteMutationObservation::decode(w, token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn owner() -> RemoteMutationOwner {
        RemoteMutationOwner {
            cluster_id: [1; 16],
            database_id: [2; 16],
            owner_id: [4; 16],
            owner_epoch: 9,
            next_request_sequence: 999,
            kind: RemoteMutationKind::Point,
            domains: vec![RemoteMutationDomainBinding {
                domain_id: [3; 16],
                directory_generation: 10,
                directory_digest: [7; 32],
                schema_epoch: 11,
                schema_digest: [6; 32],
            }],
        }
    }
    fn token() -> RemoteMutationToken {
        RemoteMutationToken::expected(
            &owner(),
            RemoteMutationIdentity {
                request_id: [5; 16],
                request_sequence: 12,
            },
            [8; 32],
        )
        .unwrap()
    }
    fn observation(state: &str) -> proto::RemoteMutationObservation {
        proto::RemoteMutationObservation {
            token: token().encode().to_vec(),
            state: state.into(),
            applied_mutations: 0,
            copied_batches: 0,
            retained_results: 0,
            outcome: None,
            result_digest: None,
        }
    }
    #[tokio::test]
    async fn standalone_capability_fails_without_rpc() {
        let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let base = RockserverClient::new_unchecked(channel, RequestContext::batch());
        assert_eq!(
            base.remote_mutations().unwrap_err().code(),
            tonic::Code::Unimplemented
        );
    }
    #[test]
    fn current_token_encoding_and_crossed_identity() {
        let t = token();
        let encoded = t.encode();
        assert_eq!(encoded.len(), 213);
        assert_eq!(&encoded[64..72], &9i64.to_be_bytes());
        assert_eq!(&encoded[72..80], &12i64.to_be_bytes());
        assert_eq!(encoded[80], 0);
        assert_eq!(&encoded[81..85], &1i32.to_be_bytes());
        assert_eq!(&encoded[85..101], &[3; 16]);
        assert_eq!(&encoded[101..109], &10i64.to_be_bytes());
        assert_eq!(&encoded[141..149], &11i64.to_be_bytes());
        assert_eq!(RemoteMutationToken::decode(&encoded).unwrap(), t);
        for offset in [0, 32, 48, 64, 72, 85, 101, 109, 141, 149, 212] {
            let mut crossed = encoded.clone();
            crossed[offset] ^= 1;
            assert!(RemoteMutationToken::prepared(
                proto::RemoteMutationTokenResponse { token: crossed },
                t.clone()
            )
            .is_err());
        }
        let mut bad = encoded.clone();
        bad[80] = 4;
        assert!(RemoteMutationToken::decode(&bad).is_err());
        assert!(RemoteMutationToken::decode(&encoded[..212]).is_err());
        let mut bad = encoded.clone();
        bad.push(0);
        assert!(RemoteMutationToken::decode(&bad).is_err());
        let mut w = observation("PREPARED");
        w.token[160] ^= 1;
        assert!(RemoteMutationObservation::decode(w, &t).is_err());
    }
    #[test]
    fn vector_tokens_require_unsigned_sorted_unique_real_authority() {
        let mut owner = owner();
        owner.kind = RemoteMutationKind::Atomic;
        let mut second = owner.domains[0].clone();
        second.domain_id = [128; 16];
        second.directory_generation = 12;
        second.schema_epoch = 13;
        owner.domains.push(second.clone());
        let id = RemoteMutationIdentity {
            request_id: [5; 16],
            request_sequence: 12,
        };
        let t = RemoteMutationToken::expected(&owner, id, [8; 32]).unwrap();
        assert_eq!(t.encode().len(), 309);
        assert_eq!(t.kind(), RemoteMutationKind::Atomic);
        let mut bytes = t.encode();
        let first = bytes[85..181].to_vec();
        let second_bytes = bytes[181..277].to_vec();
        bytes[85..181].copy_from_slice(&second_bytes);
        bytes[181..277].copy_from_slice(&first);
        assert!(RemoteMutationToken::decode(&bytes).is_err());
        let mut bytes = t.encode();
        bytes[181..197].copy_from_slice(&first[..16]);
        assert!(RemoteMutationToken::decode(&bytes).is_err());
        let mut bytes = t.encode();
        bytes[80] = 0;
        assert!(RemoteMutationToken::decode(&bytes).is_err());
        for count in [0i32, 65] {
            let mut bytes = t.encode();
            bytes[81..85].copy_from_slice(&count.to_be_bytes());
            assert!(RemoteMutationToken::decode(&bytes).is_err());
        }
        owner.domains[0].directory_generation = -1;
        assert!(RemoteMutationToken::expected(&owner, id, [8; 32]).is_err());
        owner.domains[0].directory_generation = 10;
        owner.domains[0].schema_epoch = -1;
        assert!(RemoteMutationToken::expected(&owner, id, [8; 32]).is_err());
        owner.domains.clear();
        assert!(RemoteMutationToken::expected(&owner, id, [8; 32]).is_err());
        owner.domains = (0u8..64)
            .map(|v| {
                let mut b = second.clone();
                b.domain_id = [v; 16];
                b
            })
            .collect();
        let max = RemoteMutationToken::expected(&owner, id, [8; 32]).unwrap();
        assert_eq!(max.encode().len(), 6261);
        assert!(RemoteMutationToken::decode(&max.encode()).is_ok());
        owner.domains.push(second);
        assert!(RemoteMutationToken::expected(&owner, id, [8; 32]).is_err());
    }
    #[test]
    fn command_result_nullable_values_and_failure_are_explicit() {
        let encoded = [0, 0, 0, 0, 0, 0, 0, 2, 255, 255, 255, 255, 0, 0, 0, 1, 42];
        let decoded = MutationCommandResult::decode(&encoded).unwrap();
        assert_eq!(decoded.values, vec![None, Some(vec![42])]);
        let failed = MutationCommandResult::decode(&[0, 0, 0, 3, 0, 0, 0, 0]).unwrap();
        assert_eq!(failed.status, MutationCommandStatus::SessionExpired);
        assert!(
            MutationCommandResult::decode(&[0, 0, 0, 3, 0, 0, 0, 1, 255, 255, 255, 255]).is_err()
        );
        assert!(MutationCommandResult::decode(&[0, 0, 0, 0, 0, 0, 1, 1]).is_err());
        assert!(MutationCommandResult::decode(&[0, 0, 0, 0, 0, 0, 0, 0, 0]).is_err());
    }
    #[test]
    fn forgotten_expiry_and_terminal_expiry_are_distinct() {
        let mut expired = observation("EXPIRED");
        expired.applied_mutations = -1;
        expired.copied_batches = -1;
        let value = RemoteMutationObservation::decode(expired.clone(), &token()).unwrap();
        assert!(!value.progress_known());
        assert_eq!(value.outcome, None);
        expired.outcome = Some("SESSION_EXPIRED".into());
        assert!(RemoteMutationObservation::decode(expired, &token()).is_err());
        let mut terminal = observation("TERMINAL");
        terminal.outcome = Some("SESSION_EXPIRED".into());
        terminal.result_digest = Some(vec![0; 32]);
        let value = RemoteMutationObservation::decode(terminal.clone(), &token()).unwrap();
        assert!(value.progress_known());
        assert_eq!(value.outcome, Some(MutationCommandStatus::SessionExpired));
        terminal.result_digest = None;
        assert!(RemoteMutationObservation::decode(terminal, &token()).is_err());
    }
    #[test]
    fn pages_validate_cursor_count_budget_and_outcome() {
        let mut status = observation("PENDING");
        status.retained_results = 1;
        let valid = proto::RemoteMutationPage {
            status: Some(status),
            offset: 0,
            next_offset: 1,
            rows: vec![vec![0; 8]],
            has_more: false,
        };
        assert_eq!(
            RemoteMutationPage::decode(valid.clone(), &token(), 0, 1, 8)
                .unwrap()
                .rows
                .len(),
            1
        );
        assert!(RemoteMutationPage::decode(valid.clone(), &token(), 0, 1, 7).is_err());
        let mut wrong = valid.clone();
        wrong.next_offset = 2;
        assert!(RemoteMutationPage::decode(wrong, &token(), 0, 1, 8).is_err());
        let mut wrong = valid.clone();
        wrong.has_more = true;
        assert!(RemoteMutationPage::decode(wrong, &token(), 0, 1, 8).is_err());
        let mut wrong = valid;
        wrong.rows[0][3] = 99;
        assert!(RemoteMutationPage::decode(wrong, &token(), 0, 1, 8).is_err());
        assert!(budget(0, 257, 8).is_err());
        assert!(budget(0, 1, MAX_BYTES as i32 + 1).is_err());
    }
    #[test]
    fn canonical_batch_digest_vector() {
        let rows = vec![
            RemoteMutationEntry {
                key: vec![b"a".to_vec(), vec![]],
                value: None,
            },
            RemoteMutationEntry {
                key: vec![],
                value: Some(b"z".to_vec()),
            },
        ];
        let digest = remote_mutation_batch_digest(7, true, &rows).unwrap();
        assert_eq!(
            digest
                .iter()
                .map(|v| format!("{v:02x}"))
                .collect::<String>(),
            "d4308250383bf707413072bb6ff95e22cbc8532228d830b84675973c7ff5671e"
        );
        assert_ne!(
            digest,
            remote_mutation_batch_digest(7, false, &rows).unwrap()
        );
    }
    #[test]
    fn atomic_canonical_digest_preserves_null_empty_order_and_returns() {
        let first = AtomicMutation {
            column: 7,
            key: vec![b"a".to_vec()],
            kind: AtomicMutationKind::Put,
            value: None,
            returns: AtomicReturnMode::Previous,
        };
        let predicate = AtomicPredicate {
            column: 8,
            key: vec![b"b".to_vec()],
            expected: Some(vec![]),
        };
        assert_eq!(
            remote_mutation_atomic_size(&[first.clone()], &[predicate.clone()], 1000).unwrap(),
            61
        );
        let digest =
            remote_mutation_atomic_digest(&[first.clone()], &[predicate.clone()], 1000).unwrap();
        assert_eq!(
            digest
                .iter()
                .map(|v| format!("{v:02x}"))
                .collect::<String>(),
            "e50fb358257f477e3e6ffe6652f00f029b35d9c4c4815232fc6d3732d03b2630"
        );
        let mut empty = first.clone();
        empty.value = Some(vec![]);
        assert_ne!(
            digest,
            remote_mutation_atomic_digest(&[empty.clone()], &[predicate.clone()], 1000).unwrap()
        );
        let mut absent = predicate.clone();
        absent.expected = None;
        assert_ne!(
            digest,
            remote_mutation_atomic_digest(&[first.clone()], &[absent], 1000).unwrap()
        );
        assert_ne!(
            remote_mutation_atomic_digest(&[first.clone(), empty.clone()], &[], 1000).unwrap(),
            remote_mutation_atomic_digest(&[empty, first.clone()], &[], 1000).unwrap()
        );
        let mut current = first.clone();
        current.returns = AtomicReturnMode::Current;
        assert_ne!(
            digest,
            remote_mutation_atomic_digest(&[current], &[predicate.clone()], 1000).unwrap()
        );
        assert!(remote_mutation_atomic_digest(&[], &[predicate], 1000).is_ok());
        assert!(remote_mutation_atomic_digest(&[], &[], 1000).is_err());
        assert!(remote_mutation_atomic_digest(&[first.clone()], &[], 0).is_err());
        assert!(remote_mutation_atomic_digest(&[first.clone()], &[], 60001).is_err());
        assert!(remote_mutation_atomic_digest(&vec![first; 257], &[], 1000).is_err());
    }
    #[test]
    fn atomic_preflight_uses_exact_canonical_byte_limit_and_value_rules() {
        let mut large = AtomicMutation {
            column: 7,
            key: vec![],
            kind: AtomicMutationKind::Put,
            value: Some(vec![0; MAX_BYTES - 35]),
            returns: AtomicReturnMode::None,
        };
        assert_eq!(
            remote_mutation_atomic_size(std::slice::from_ref(&large), &[], 1).unwrap(),
            MAX_BYTES
        );
        large.value.as_mut().unwrap().push(0);
        assert!(remote_mutation_atomic_size(std::slice::from_ref(&large), &[], 1).is_err());
        large.value = None;
        large.kind = AtomicMutationKind::Merge;
        assert!(remote_mutation_atomic_size(std::slice::from_ref(&large), &[], 1).is_err());
        large.kind = AtomicMutationKind::Delete;
        large.value = Some(vec![]);
        assert!(remote_mutation_atomic_size(std::slice::from_ref(&large), &[], 1).is_err());
        large.value = None;
        large.key = vec![vec![]; 257];
        assert!(remote_mutation_atomic_size(&[large], &[], 1).is_err());
    }
    #[tokio::test]
    async fn atomic_convenience_phases_bind_the_same_original_deadline() {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
        let prepare = atomic_request_at(
            proto::RemoteMutationPrepareAtomicRequest::default(),
            Some(deadline),
        )
        .unwrap();
        tokio::task::yield_now().await;
        let execute =
            atomic_request_at(proto::RemoteMutationTokenRequest::default(), Some(deadline))
                .unwrap();
        assert_eq!(
            prepare.extensions().get::<BoundDeadline>().unwrap().0,
            deadline
        );
        assert_eq!(
            execute.extensions().get::<BoundDeadline>().unwrap().0,
            deadline
        );
        assert!(atomic_request_at(
            proto::RemoteMutationTokenRequest::default(),
            Some(tokio::time::Instant::now() - std::time::Duration::from_millis(1))
        )
        .is_err());
        assert!(
            atomic_request_at(proto::RemoteMutationTokenRequest::default(), None)
                .unwrap()
                .metadata()
                .get("grpc-timeout")
                .is_none()
        );
    }
    // Compiled API workflow fixture; never run and never opens a socket.
    #[allow(dead_code)]
    async fn explicit_public_workflow(
        base: &RockserverClient,
        owner: &RemoteMutationOwner,
        id: RemoteMutationIdentity,
        rows: &[RemoteMutationEntry],
    ) -> Result<()> {
        let client = base
            .remote_mutations()?
            .with_context(RequestContext::batch());
        let _ = client.owners(7).await?;
        let _ = client.atomic_owners(&[7, 8]).await?;
        let mutations = [AtomicMutation {
            column: 7,
            key: vec![vec![1]],
            kind: AtomicMutationKind::Put,
            value: None,
            returns: AtomicReturnMode::Previous,
        }];
        let predicates = [AtomicPredicate {
            column: 8,
            key: vec![vec![2]],
            expected: Some(vec![]),
        }];
        let _ = client
            .prepare_atomic(owner, id, &mutations, &predicates, 1000)
            .await?;
        let _ = client
            .execute_atomic(owner, id, &mutations, &predicates, 1000)
            .await?;
        let point = client
            .prepare_point(
                owner,
                id,
                7,
                rows,
                PointMutationKind::Put,
                MutationCallback::Nothing,
                true,
            )
            .await?;
        let _ = client.execute(&point).await?;
        let _ = client.resume(&point).await?;
        let _ = client.status(&point).await?;
        let _ = client.results(&point, 0, 256, MAX_BYTES as i32).await?;
        let _ = client.acknowledge(&point, [0; 32]).await?;
        let _ = client.prepare_range(owner, id, 7, None, None).await?;
        let bulk = client.prepare_bulk(owner, id, 7, false).await?;
        let _ = client.append_batch(&bulk, 7, false, 0, rows).await?;
        let _ = client.finish_input(&bulk).await?;
        Ok(())
    }
}
