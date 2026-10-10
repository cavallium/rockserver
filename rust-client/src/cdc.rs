//! Explicit vector CDC delivery and consumer acknowledgement over a caller-owned channel.
use crate::{cdc_proto as wire, RequestContext, Result};
use prost::Message;
use sha2::{Digest, Sha256};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashSet},
    future::Future,
    time::Duration,
};
use tonic::service::{interceptor::InterceptedService, Interceptor};
use tonic::{transport::Channel, Request, Status as RpcStatus};

pub type Id = [u8; 16];
pub const MAX_LINEAGES: usize = 1024;
pub const MAX_COLUMNS: usize = 256;
pub const MAX_RECORDS: i32 = 256;
pub const MIN_POLL_BYTES: i32 = 111;
pub const MAX_POLL_BYTES: i32 = 8 * 1024 * 1024;
pub const MAX_EVENTS_PER_ENVELOPE: i32 = 2048;
pub const MAX_EVENTS: usize = MAX_RECORDS as usize * MAX_EVENTS_PER_ENVELOPE as usize;
pub const MAX_BATCH_BYTES: usize = 10_551_536;
pub const MAX_FRAME_BYTES: usize = 32 * 1024 * 1024;
pub const MAX_REQUEST_BYTES: usize = 128 * 1024;
const MAX_COMMAND_BYTES: usize = 2 * 1024 * 1024;
const MAX_CHECKPOINT_BYTES: usize = 4 + 60 * MAX_LINEAGES;
const MAX_DEFINITION_BYTES: usize = 120 + 16 * MAX_COLUMNS + MAX_CHECKPOINT_BYTES;
pub use wire::{
    CdcDecisionPhase as DecisionPhase, CdcLocalPhase as LocalPhase,
    CdcMutationKind as MutationKind, CdcReturnMode as ReturnMode, CdcStatePhase as StatePhase,
    CdcTopology as Topology,
};
fn invalid(s: &'static str) -> RpcStatus {
    RpcStatus::invalid_argument(s)
}
fn required<T>(v: Option<T>) -> Result<T> {
    v.ok_or_else(|| invalid("missing CDC field"))
}
fn id(v: Vec<u8>) -> Result<Id> {
    v.try_into()
        .map_err(|_| invalid("CDC UUID must contain 16 bytes"))
}
fn digest(v: Vec<u8>) -> Result<[u8; 32]> {
    v.try_into()
        .map_err(|_| invalid("CDC digest must contain 32 bytes"))
}
fn uuid_order(a: &Id, b: &Id) -> Ordering {
    (
        i64::from_be_bytes(a[..8].try_into().unwrap()),
        i64::from_be_bytes(a[8..].try_into().unwrap()),
    )
        .cmp(&(
            i64::from_be_bytes(b[..8].try_into().unwrap()),
            i64::from_be_bytes(b[8..].try_into().unwrap()),
        ))
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct Lineage {
    pub range_id: Id,
    pub group_id: Id,
    pub range_epoch: i64,
}
impl Ord for Lineage {
    fn cmp(&self, other: &Self) -> Ordering {
        uuid_order(&self.range_id, &other.range_id)
            .then_with(|| uuid_order(&self.group_id, &other.group_id))
            .then(self.range_epoch.cmp(&other.range_epoch))
    }
}
impl PartialOrd for Lineage {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Lineage {
    fn validate(&self) -> Result<()> {
        if self.range_epoch < 0 {
            Err(invalid("negative CDC epoch"))
        } else {
            Ok(())
        }
    }
    fn encode(&self) -> Result<wire::CdcLineage> {
        self.validate()?;
        Ok(wire::CdcLineage {
            range_id: self.range_id.to_vec(),
            group_id: self.group_id.to_vec(),
            range_epoch: self.range_epoch,
        })
    }
}
impl TryFrom<wire::CdcLineage> for Lineage {
    type Error = RpcStatus;
    fn try_from(v: wire::CdcLineage) -> Result<Self> {
        let result = Self {
            range_id: id(v.range_id)?,
            group_id: id(v.group_id)?,
            range_epoch: v.range_epoch,
        };
        result.validate()?;
        Ok(result)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Position {
    pub term: i64,
    pub index: i64,
}
impl Position {
    fn validate(&self, committed: bool) -> Result<()> {
        if self.term < 0 || self.index < 0 || committed && self.term == 0 {
            Err(invalid("invalid CDC position"))
        } else {
            Ok(())
        }
    }
    fn encode(&self) -> wire::CdcPosition {
        wire::CdcPosition {
            term: self.term,
            index: self.index,
        }
    }
}
impl TryFrom<wire::CdcPosition> for Position {
    type Error = RpcStatus;
    fn try_from(v: wire::CdcPosition) -> Result<Self> {
        let p = Self {
            term: v.term,
            index: v.index,
        };
        p.validate(false)?;
        Ok(p)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Cursor {
    pub position: Position,
    pub mutation_ordinal: i32,
}
impl Cursor {
    pub fn beginning() -> Self {
        Self {
            position: Position { term: 0, index: 0 },
            mutation_ordinal: -1,
        }
    }
    pub fn after(position: Position) -> Result<Self> {
        position.validate(true)?;
        Ok(Self {
            position,
            mutation_ordinal: MAX_EVENTS_PER_ENVELOPE,
        })
    }
    fn validate(&self) -> Result<()> {
        self.position.validate(false)?;
        if *self != Self::beginning()
            && (self.position.term == 0
                || !(0..=MAX_EVENTS_PER_ENVELOPE).contains(&self.mutation_ordinal))
        {
            return Err(invalid("invalid CDC cursor"));
        }
        Ok(())
    }
    pub fn covers(&self, earlier: &Self) -> Result<bool> {
        self.validate()?;
        earlier.validate()?;
        if *earlier == Self::beginning() {
            return Ok(true);
        }
        if *self == Self::beginning() {
            return Ok(false);
        }
        if self.position.index == earlier.position.index {
            if self.position.term != earlier.position.term {
                return Err(invalid("CDC entry term changed"));
            }
            return Ok(self.mutation_ordinal >= earlier.mutation_ordinal);
        }
        if self.position.index > earlier.position.index {
            if self.position.term < earlier.position.term {
                return Err(invalid("CDC term regressed"));
            }
            return Ok(true);
        }
        if self.position.term > earlier.position.term {
            return Err(invalid("CDC term increased before earlier entry"));
        }
        Ok(false)
    }
    fn encode(&self) -> Result<wire::CdcCursor> {
        self.validate()?;
        Ok(wire::CdcCursor {
            position: Some(self.position.encode()),
            mutation_ordinal: self.mutation_ordinal,
        })
    }
}
impl TryFrom<wire::CdcCursor> for Cursor {
    type Error = RpcStatus;
    fn try_from(v: wire::CdcCursor) -> Result<Self> {
        let c = Self {
            position: required(v.position)?.try_into()?,
            mutation_ordinal: v.mutation_ordinal,
        };
        c.validate()?;
        Ok(c)
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Checkpoint(pub BTreeMap<Lineage, Cursor>);
impl Checkpoint {
    fn validate(&self) -> Result<()> {
        if self.0.is_empty() || self.0.len() > MAX_LINEAGES {
            return Err(invalid("too many CDC lineages"));
        }
        for (l, c) in &self.0 {
            l.validate()?;
            c.validate()?;
        }
        Ok(())
    }
    pub fn advance(&self, lineage: Lineage, next: Cursor) -> Result<Self> {
        self.validate()?;
        if !next.covers(
            self.0
                .get(&lineage)
                .ok_or_else(|| invalid("foreign CDC lineage"))?,
        )? {
            return Err(invalid("CDC cursor regressed"));
        }
        let mut result = self.clone();
        result.0.insert(lineage, next);
        Ok(result)
    }
    fn covers(&self, earlier: &Self) -> Result<bool> {
        if self.0.keys().ne(earlier.0.keys()) {
            return Ok(false);
        }
        for (l, c) in &self.0 {
            if !c.covers(&earlier.0[l])? {
                return Ok(false);
            }
        }
        Ok(true)
    }
    fn encode(&self) -> Result<wire::CdcCheckpoint> {
        self.validate()?;
        Ok(wire::CdcCheckpoint {
            entries: self
                .0
                .iter()
                .map(|(l, c)| {
                    Ok(wire::CdcCheckpointEntry {
                        lineage: Some(l.encode()?),
                        cursor: Some(c.encode()?),
                    })
                })
                .collect::<Result<_>>()?,
        })
    }
}
impl TryFrom<wire::CdcCheckpoint> for Checkpoint {
    type Error = RpcStatus;
    fn try_from(v: wire::CdcCheckpoint) -> Result<Self> {
        if v.entries.is_empty()
            || v.entries.len() > MAX_LINEAGES
            || v.encoded_len() > MAX_REQUEST_BYTES
        {
            return Err(invalid("too many CDC lineages"));
        }
        let mut entries = BTreeMap::new();
        for entry in v.entries {
            if entries
                .insert(
                    required(entry.lineage)?.try_into()?,
                    required(entry.cursor)?.try_into()?,
                )
                .is_some()
            {
                return Err(invalid("duplicate CDC lineage"));
            }
        }
        Ok(Self(entries))
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Definition {
    pub cluster_id: Id,
    pub database_id: Id,
    pub domain_id: Id,
    pub subscription_id: Id,
    pub incarnation_id: Id,
    pub directory_digest: [u8; 32],
    pub columns: Vec<Id>,
    pub start: Checkpoint,
}
impl Definition {
    fn validate(&self) -> Result<()> {
        self.start.validate()?;
        if self.columns.is_empty()
            || self.columns.len() > MAX_COLUMNS
            || self.start.0.is_empty()
            || self.budget_bytes() > MAX_DEFINITION_BYTES
            || self.columns.iter().collect::<HashSet<_>>().len() != self.columns.len()
        {
            return Err(invalid("invalid CDC definition"));
        }
        Ok(())
    }
    fn budget_bytes(&self) -> usize {
        120 + 16 * self.columns.len() + 4 + 60 * self.start.0.len()
    }
    fn encode(&self) -> Result<wire::CdcDefinition> {
        self.validate()?;
        let mut columns = self.columns.clone();
        columns.sort_by(uuid_order);
        Ok(wire::CdcDefinition {
            cluster_id: self.cluster_id.to_vec(),
            database_id: self.database_id.to_vec(),
            domain_id: self.domain_id.to_vec(),
            subscription_id: self.subscription_id.to_vec(),
            incarnation_id: self.incarnation_id.to_vec(),
            directory_digest: self.directory_digest.to_vec(),
            columns: columns.into_iter().map(|v| v.to_vec()).collect(),
            start: Some(self.start.encode()?),
        })
    }
}
impl TryFrom<wire::CdcDefinition> for Definition {
    type Error = RpcStatus;
    fn try_from(v: wire::CdcDefinition) -> Result<Self> {
        if v.columns.len() > MAX_COLUMNS || v.encoded_len() > MAX_REQUEST_BYTES {
            return Err(invalid("too many CDC columns"));
        }
        let mut result = Self {
            cluster_id: id(v.cluster_id)?,
            database_id: id(v.database_id)?,
            domain_id: id(v.domain_id)?,
            subscription_id: id(v.subscription_id)?,
            incarnation_id: id(v.incarnation_id)?,
            directory_digest: digest(v.directory_digest)?,
            columns: v.columns.into_iter().map(id).collect::<Result<_>>()?,
            start: required(v.start)?.try_into()?,
        };
        result.columns.sort_by(uuid_order);
        result.validate()?;
        Ok(result)
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct State {
    pub definition: Definition,
    pub phase: StatePhase,
    pub generation: i64,
    pub checkpoint: Checkpoint,
    pub delivered: Checkpoint,
}
impl TryFrom<wire::CdcState> for State {
    type Error = RpcStatus;
    fn try_from(v: wire::CdcState) -> Result<Self> {
        let result = Self {
            definition: required(v.definition)?.try_into()?,
            phase: StatePhase::try_from(v.phase).map_err(|_| invalid("unknown CDC state phase"))?,
            generation: v.generation,
            checkpoint: required(v.checkpoint)?.try_into()?,
            delivered: required(v.delivered)?.try_into()?,
        };
        let closed = result.phase == StatePhase::SubscriptionClosed;
        let initial = result.delivered == result.definition.start;
        let minimum = if result.checkpoint == result.definition.start {
            1
        } else {
            2
        } + closed as i64;
        if result.generation < 0
            || !result.checkpoint.covers(&result.definition.start)?
            || !result.delivered.covers(&result.checkpoint)?
            || if initial {
                result.generation != closed as i64
            } else {
                result.generation < minimum
            }
        {
            return Err(invalid("invalid CDC subscription progress"));
        }
        Ok(result)
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Status {
    pub state: State,
    pub phase: LocalPhase,
}
impl Status {
    pub fn decode(v: wire::CdcStatus, expected: Id) -> Result<Self> {
        if v.encoded_len() > MAX_FRAME_BYTES {
            return Err(invalid("oversized CDC status"));
        }
        let result = Self {
            state: required(v.state)?.try_into()?,
            phase: LocalPhase::try_from(v.phase).map_err(|_| invalid("unknown CDC local phase"))?,
        };
        if result.state.definition.subscription_id != expected
            || result.phase == LocalPhase::Create
                && (result.state.generation != 0
                    || result.state.phase != StatePhase::SubscriptionActive
                    || result.state.delivered != result.state.definition.start)
            || result.phase == LocalPhase::Active
                && result.state.phase != StatePhase::SubscriptionActive
            || result.phase == LocalPhase::Closed
                && result.state.phase != StatePhase::SubscriptionClosed
        {
            return Err(invalid("wrong CDC status identity or phase"));
        }
        Ok(result)
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationId {
    pub session_id: Id,
    pub epoch: i64,
    pub sequence: i64,
}
impl TryFrom<wire::CdcOperationId> for OperationId {
    type Error = RpcStatus;
    fn try_from(v: wire::CdcOperationId) -> Result<Self> {
        if v.epoch < 0 || v.sequence < 0 {
            return Err(invalid("invalid CDC operation"));
        }
        Ok(Self {
            session_id: id(v.session_id)?,
            epoch: v.epoch,
            sequence: v.sequence,
        })
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Mutation {
    pub kind: MutationKind,
    pub column_id: Id,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub return_mode: ReturnMode,
    pub member_keys: Vec<Vec<u8>>,
}
impl Mutation {
    fn budget_bytes(&self) -> usize {
        26 + self.key.len()
            + self.value.as_ref().map_or(0, Vec::len)
            + if self.member_keys.is_empty() {
                0
            } else {
                4 + self.member_keys.iter().map(|k| 2 + k.len()).sum::<usize>()
            }
    }
}
impl TryFrom<wire::CdcMutation> for Mutation {
    type Error = RpcStatus;
    fn try_from(v: wire::CdcMutation) -> Result<Self> {
        let kind =
            MutationKind::try_from(v.kind).map_err(|_| invalid("unknown CDC mutation kind"))?;
        let return_mode =
            ReturnMode::try_from(v.return_mode).map_err(|_| invalid("unknown CDC return mode"))?;
        let bucket = matches!(
            kind,
            MutationKind::MutationBucketPut
                | MutationKind::MutationBucketDelete
                | MutationKind::MutationBucketMerge
        );
        let deletion = matches!(
            kind,
            MutationKind::MutationDelete | MutationKind::MutationBucketDelete
        );
        if bucket == v.member_keys.is_empty()
            || v.member_keys.len() > 256
            || v.member_keys.iter().any(|k| k.len() > 65535)
            || deletion == v.value.is_some()
            || kind == MutationKind::MutationDeleteRange
                && (return_mode != ReturnMode::ReturnNone
                    || v.value.as_ref().is_some_and(|upper| &v.key >= upper))
        {
            return Err(invalid("invalid CDC mutation shape"));
        }
        let result = Self {
            kind,
            column_id: id(v.column_id)?,
            key: v.key,
            value: v.value,
            return_mode,
            member_keys: v.member_keys,
        };
        if result.budget_bytes() > MAX_COMMAND_BYTES {
            return Err(invalid("oversized CDC mutation"));
        }
        Ok(result)
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Event {
    pub cursor: Cursor,
    pub mutation: Mutation,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Participant {
    pub lineage: Lineage,
    pub schema_digest: [u8; 32],
    pub plan_digest: [u8; 32],
    pub prepared: Option<Position>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Decision {
    pub cluster_id: Id,
    pub database_id: Id,
    pub domain_id: Id,
    pub transaction_id: Id,
    pub operation: OperationId,
    pub schema_epoch: i64,
    pub schema_digest: [u8; 32],
    pub plan_digest: [u8; 32],
    pub phase: DecisionPhase,
    pub participants: Vec<Participant>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Finalization {
    pub home_group: Id,
    pub home_epoch: i64,
    pub participant_group: Id,
    pub recovery_operation: OperationId,
    pub decision: Decision,
}
impl Finalization {
    fn budget_bytes(&self) -> usize {
        76 + 182 + 121 * self.decision.participants.len()
    }
}
impl TryFrom<wire::CdcFinalization> for Finalization {
    type Error = RpcStatus;
    fn try_from(v: wire::CdcFinalization) -> Result<Self> {
        let d = required(v.decision)?;
        if v.home_epoch < 0
            || d.schema_epoch < 0
            || d.participants.is_empty()
            || d.participants.len() > 64
        {
            return Err(invalid("invalid CDC decision bounds"));
        }
        let mut groups = HashSet::new();
        let mut ranges = HashSet::new();
        let participants = d
            .participants
            .into_iter()
            .map(|p| {
                let lineage: Lineage = required(p.lineage)?.try_into()?;
                if !groups.insert(lineage.group_id) || !ranges.insert(lineage.range_id) {
                    return Err(invalid("duplicate transaction participant"));
                }
                let prepared = p.prepared.map(Position::try_from).transpose()?;
                if let Some(position) = prepared {
                    position.validate(true)?;
                    if position.index == 0 {
                        return Err(invalid("invalid CDC prepared index"));
                    }
                }
                if d.phase == DecisionPhase::DecisionCommit as i32 && prepared.is_none() {
                    return Err(invalid("unprepared CDC commit participant"));
                }
                Ok(Participant {
                    lineage,
                    schema_digest: digest(p.schema_digest)?,
                    plan_digest: digest(p.plan_digest)?,
                    prepared,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let result = Self {
            home_group: id(v.home_group)?,
            home_epoch: v.home_epoch,
            participant_group: id(v.participant_group)?,
            recovery_operation: required(v.recovery_operation)?.try_into()?,
            decision: Decision {
                cluster_id: id(d.cluster_id)?,
                database_id: id(d.database_id)?,
                domain_id: id(d.domain_id)?,
                transaction_id: id(d.transaction_id)?,
                operation: required(d.operation)?.try_into()?,
                schema_epoch: d.schema_epoch,
                schema_digest: digest(d.schema_digest)?,
                plan_digest: digest(d.plan_digest)?,
                phase: DecisionPhase::try_from(d.phase)
                    .map_err(|_| invalid("unknown CDC decision phase"))?,
                participants,
            },
        };
        if result.recovery_operation == result.decision.operation
            || !result
                .decision
                .participants
                .iter()
                .any(|p| p.lineage.group_id == result.participant_group)
        {
            return Err(invalid(
                "invalid finalization participant/recovery identity",
            ));
        }
        Ok(result)
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Envelope {
    pub position: Position,
    pub schema_digest: [u8; 32],
    pub operation: OperationId,
    pub transaction: Option<Finalization>,
    pub events: Vec<Event>,
}
impl Envelope {
    fn budget_bytes(&self) -> usize {
        88 + self
            .transaction
            .as_ref()
            .map_or(0, Finalization::budget_bytes)
            + self
                .events
                .iter()
                .map(|e| 4 + e.mutation.budget_bytes())
                .sum::<usize>()
    }
}
impl TryFrom<wire::CdcEnvelope> for Envelope {
    type Error = RpcStatus;
    fn try_from(v: wire::CdcEnvelope) -> Result<Self> {
        if v.events.is_empty() || v.events.len() > MAX_EVENTS_PER_ENVELOPE as usize {
            return Err(invalid("invalid CDC envelope event count"));
        }
        let position: Position = required(v.position)?.try_into()?;
        position.validate(true)?;
        let mut previous = -1;
        let mut bytes = 0;
        let events = v
            .events
            .into_iter()
            .map(|event| {
                if event.mutation_ordinal <= previous
                    || event.mutation_ordinal >= MAX_EVENTS_PER_ENVELOPE
                {
                    return Err(invalid("invalid CDC event ordinal"));
                }
                previous = event.mutation_ordinal;
                let mutation: Mutation = required(event.mutation)?.try_into()?;
                bytes += mutation.budget_bytes();
                if bytes > MAX_COMMAND_BYTES - 109 {
                    return Err(invalid("CDC envelope mutation budget exceeded"));
                }
                Ok(Event {
                    cursor: Cursor {
                        position,
                        mutation_ordinal: event.mutation_ordinal,
                    },
                    mutation,
                })
            })
            .collect::<Result<_>>()?;
        let result = Self {
            position,
            schema_digest: digest(v.schema_digest)?,
            operation: required(v.operation)?.try_into()?,
            transaction: v.transaction.map(Finalization::try_from).transpose()?,
            events,
        };
        if let Some(t) = &result.transaction {
            let p = t
                .decision
                .participants
                .iter()
                .find(|p| p.lineage.group_id == t.participant_group)
                .unwrap();
            let prepared = p
                .prepared
                .ok_or_else(|| invalid("missing CDC prepared position"))?;
            if t.decision.phase != DecisionPhase::DecisionCommit
                || t.decision.operation != result.operation
                || p.schema_digest != result.schema_digest
                || prepared.index >= position.index
                || prepared.term > position.term
            {
                return Err(invalid("CDC transaction differs from committed envelope"));
            }
        }
        Ok(result)
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Batch {
    pub id: Id,
    pub definition: Definition,
    pub lineage: Lineage,
    pub from: Cursor,
    pub next: Cursor,
    pub cut: Position,
    pub envelopes: Vec<Envelope>,
}
impl Batch {
    pub fn budget_bytes(&self) -> usize {
        116 + self.definition.budget_bytes()
            + self
                .envelopes
                .iter()
                .map(Envelope::budget_bytes)
                .sum::<usize>()
    }
}
impl TryFrom<wire::CdcBatch> for Batch {
    type Error = RpcStatus;
    fn try_from(v: wire::CdcBatch) -> Result<Self> {
        if v.envelopes.len() > MAX_RECORDS as usize {
            return Err(invalid("too many CDC envelopes"));
        }
        let result = Self {
            id: id(v.id)?,
            definition: required(v.definition)?.try_into()?,
            lineage: required(v.lineage)?.try_into()?,
            from: required(v.from)?.try_into()?,
            next: required(v.next)?.try_into()?,
            cut: required(v.cut)?.try_into()?,
            envelopes: v
                .envelopes
                .into_iter()
                .map(Envelope::try_from)
                .collect::<Result<_>>()?,
        };
        let end = Cursor::after(result.cut)?;
        let start = result
            .definition
            .start
            .0
            .get(&result.lineage)
            .ok_or_else(|| invalid("foreign CDC batch lineage"))?;
        if !result.from.covers(start)?
            || !result.next.covers(&result.from)?
            || !end.covers(&result.next)?
        {
            return Err(invalid("invalid CDC batch cursor bounds"));
        }
        let mut previous = -1;
        let mut events = 0;
        for e in &result.envelopes {
            if e.position.index <= previous
                || !end.covers(&Cursor::after(e.position)?)?
                || !result.next.covers(&Cursor::after(e.position)?)?
            {
                return Err(invalid("CDC envelopes changed order/cut"));
            }
            previous = e.position.index;
            for event in &e.events {
                if !result
                    .definition
                    .columns
                    .contains(&event.mutation.column_id)
                    || result.from.covers(&event.cursor)?
                    || !result.next.covers(&event.cursor)?
                {
                    return Err(invalid("CDC event unselected or outside cursor bounds"));
                }
            }
            if let Some(t) = &e.transaction {
                let d = &t.decision;
                let p = d
                    .participants
                    .iter()
                    .find(|p| p.lineage.group_id == t.participant_group)
                    .unwrap();
                if t.participant_group != result.lineage.group_id
                    || p.lineage != result.lineage
                    || d.cluster_id != result.definition.cluster_id
                    || d.database_id != result.definition.database_id
                    || d.domain_id != result.definition.domain_id
                {
                    return Err(invalid("CDC transaction changed lineage/namespace"));
                }
            }
            events += e.events.len();
        }
        if events > MAX_EVENTS || result.budget_bytes() > MAX_BATCH_BYTES {
            return Err(invalid("CDC projected batch exceeds bound"));
        }
        Ok(result)
    }
}
/// GAP and TOO_LARGE retain the exact lineage and completed cut; no implicit ACK occurs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Outcome {
    Data(Batch),
    Gap,
    TooLarge { required_bytes: i32 },
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Poll {
    pub lineage: Lineage,
    pub outcome: Outcome,
    pub floor: Cursor,
    pub cut: Position,
}
impl Poll {
    pub fn decode(v: wire::CdcPoll, subscription: Id, maximum_records: i32) -> Result<Self> {
        if !(1..=MAX_RECORDS).contains(&maximum_records) || v.encoded_len() > MAX_FRAME_BYTES {
            return Err(invalid("oversized CDC poll"));
        }
        let lineage: Lineage = required(v.lineage)?.try_into()?;
        let floor: Cursor = required(v.floor)?.try_into()?;
        let cut: Position = required(v.cut)?.try_into()?;
        if !Cursor::after(cut)?.covers(&floor)? {
            return Err(invalid("CDC floor exceeds cut"));
        }
        let outcome = match wire::CdcOutcome::try_from(v.outcome)
            .map_err(|_| invalid("unknown CDC outcome"))?
        {
            wire::CdcOutcome::Data => {
                let b: Batch = required(v.batch)?.try_into()?;
                if v.required_bytes != 0
                    || b.lineage != lineage
                    || b.cut != cut
                    || !b.from.covers(&floor)?
                    || b.definition.subscription_id != subscription
                {
                    return Err(invalid("invalid CDC DATA outcome"));
                }
                Outcome::Data(b)
            }
            wire::CdcOutcome::Gap => {
                if v.batch.is_some() || v.required_bytes != 0 {
                    return Err(invalid("invalid CDC GAP"));
                }
                Outcome::Gap
            }
            wire::CdcOutcome::TooLarge => {
                if v.batch.is_some() || !(1..=MAX_POLL_BYTES).contains(&v.required_bytes) {
                    return Err(invalid("invalid CDC TOO_LARGE"));
                }
                Outcome::TooLarge {
                    required_bytes: v.required_bytes,
                }
            }
        };
        Ok(Self {
            lineage,
            outcome,
            floor,
            cut,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Range {
    pub lineage: Lineage,
    pub configuration_version: i64,
    pub topology: Topology,
    pub lower_inclusive: Option<Vec<u8>>,
    pub upper_exclusive: Option<Vec<u8>>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Directory {
    pub canonical: Vec<u8>,
    pub cluster_id: Id,
    pub database_id: Id,
    pub domain_id: Id,
    pub schema_epoch: i64,
    pub schema_digest: [u8; 32],
    pub generation: i64,
    pub columns: Vec<Id>,
    pub routing_prefix_bytes: i32,
    pub ranges: Vec<Range>,
}
// Parse only the current directory carrier to bind its typed namespace/columns/lineages.
struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.0.len() < n {
            return Err(invalid("truncated canonical CDC directory"));
        }
        let (head, tail) = self.0.split_at(n);
        self.0 = tail;
        Ok(head)
    }
    fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn id(&mut self) -> Result<Id> {
        Ok(self.take(16)?.try_into().unwrap())
    }
    fn count(&mut self, minimum: i32, maximum: usize) -> Result<usize> {
        let n = self.i32()?;
        if n < minimum || n as usize > maximum {
            return Err(invalid("invalid canonical CDC count"));
        }
        Ok(n as usize)
    }
    fn bound(&mut self) -> Result<Option<Vec<u8>>> {
        let n = self.i32()?;
        if n == -1 {
            return Ok(None);
        }
        if n < 0 {
            return Err(invalid("invalid canonical CDC bound"));
        }
        Ok(Some(self.take(n as usize)?.to_vec()))
    }
    fn ascii(&mut self, maximum: usize) -> Result<Vec<u8>> {
        let n = self.count(1, maximum)?;
        let bytes = self.take(n)?;
        if !bytes.is_ascii() {
            return Err(invalid("non ASCII CDC placement"));
        }
        Ok(bytes.to_vec())
    }
}
fn validate_endpoint(address: &[u8]) -> Result<()> {
    if !address
        .iter()
        .all(|b| b.is_ascii_alphanumeric() || b"._-[]:".contains(b))
    {
        return Err(invalid("invalid CDC endpoint characters"));
    }
    let colon = address
        .iter()
        .rposition(|b| *b == b':')
        .ok_or_else(|| invalid("CDC endpoint requires host:port"))?;
    let (host, port) = (&address[..colon], &address[colon + 1..]);
    if host.is_empty()
        || port.is_empty()
        || port.len() > 5
        || !port.iter().all(u8::is_ascii_digit)
        || host.contains(&b':') && !(host.starts_with(b"[") && host.ends_with(b"]"))
    {
        return Err(invalid("invalid CDC endpoint host/port"));
    }
    let number = port.iter().fold(0u32, |n, b| n * 10 + (b - b'0') as u32);
    if !(1..=65535).contains(&number) {
        return Err(invalid("CDC endpoint port outside valid range"));
    }
    Ok(())
}
impl Directory {
    pub fn decode(v: wire::CdcDirectory, expected_domain: Id) -> Result<Self> {
        if v.canonical.len() < 112
            || v.canonical.len() > 1024 * 1024
            || v.encoded_len() > MAX_FRAME_BYTES
            || v.columns.is_empty()
            || v.columns.len() > MAX_COLUMNS
            || v.ranges.is_empty()
            || v.ranges.len() > MAX_LINEAGES
        {
            return Err(invalid("invalid CDC directory bounds"));
        }
        let result = Self {
            canonical: v.canonical,
            cluster_id: id(v.cluster_id)?,
            database_id: id(v.database_id)?,
            domain_id: id(v.domain_id)?,
            schema_epoch: v.schema_epoch,
            schema_digest: digest(v.schema_digest)?,
            generation: v.generation,
            columns: v.columns.into_iter().map(id).collect::<Result<_>>()?,
            routing_prefix_bytes: v.routing_prefix_bytes,
            ranges: v
                .ranges
                .into_iter()
                .map(|r| {
                    Ok(Range {
                        lineage: required(r.lineage)?.try_into()?,
                        configuration_version: r.configuration_version,
                        topology: Topology::try_from(r.topology)
                            .map_err(|_| invalid("unknown CDC topology"))?,
                        lower_inclusive: r.lower_inclusive,
                        upper_exclusive: r.upper_exclusive,
                    })
                })
                .collect::<Result<_>>()?,
        };
        if result.domain_id != expected_domain
            || result.schema_epoch < 0
            || result.generation < 0
            || !(0..=1024 * 1024).contains(&result.routing_prefix_bytes)
            || result.columns.iter().collect::<HashSet<_>>().len() != result.columns.len()
        {
            return Err(invalid("invalid CDC directory identity"));
        }
        let mut r = Reader(&result.canonical);
        if r.i32()? != 1
            || r.id()? != result.cluster_id
            || r.id()? != result.database_id
            || r.id()? != result.domain_id
            || r.i64()? != result.schema_epoch
            || r.take(32)? != result.schema_digest
            || r.i64()? != result.generation
            || r.i32()? != result.routing_prefix_bytes
        {
            return Err(invalid("CDC directory carrier changed namespace/schema"));
        }
        let count = r.count(0, MAX_COLUMNS)?;
        let mut canonical_columns = Vec::with_capacity(count);
        for _ in 0..count {
            canonical_columns.push(r.id()?);
        }
        if canonical_columns.windows(2).any(|p| p[0] >= p[1]) {
            return Err(invalid("noncanonical CDC directory columns"));
        }
        let mut selected = result.columns.clone();
        selected.sort_unstable();
        if canonical_columns != selected || r.count(1, MAX_LINEAGES)? != result.ranges.len() {
            return Err(invalid("CDC directory carrier changed columns/ranges"));
        }
        let mut range_ids = HashSet::new();
        let mut groups = HashSet::new();
        for (i, range) in result.ranges.iter().enumerate() {
            if !range_ids.insert(range.lineage.range_id)
                || !groups.insert(range.lineage.group_id)
                || range.configuration_version < 0
                || r.id()? != range.lineage.range_id
                || r.id()? != range.lineage.group_id
                || r.i64()? != range.lineage.range_epoch
                || r.i64()? != range.configuration_version
                || r.bound()? != range.lower_inclusive
                || r.bound()? != range.upper_exclusive
                || r.take(1)?[0] != range.topology as u8 + 1
            {
                return Err(invalid("CDC directory carrier changed lineage/bounds"));
            }
            if (i == 0 && range.lower_inclusive.is_some())
                || (i > 0
                    && (range.lower_inclusive.is_none()
                        || range.lower_inclusive != result.ranges[i - 1].upper_exclusive))
                || (i == result.ranges.len() - 1 && range.upper_exclusive.is_some())
                || (i < result.ranges.len() - 1 && range.upper_exclusive.is_none())
                || matches!((&range.lower_inclusive,&range.upper_exclusive),(Some(a),Some(b)) if a >= b)
                || result.routing_prefix_bytes > 0
                    && [&range.lower_inclusive, &range.upper_exclusive]
                        .iter()
                        .any(|b| {
                            b.as_ref()
                                .is_some_and(|b| b.len() != result.routing_prefix_bytes as usize)
                        })
            {
                return Err(invalid("CDC directory coverage changed"));
            }
            let replicas = r.count(1, 16)?;
            let mut previous_replica = None;
            let mut voters = 0;
            let mut nodes = HashSet::new();
            let mut incarnations = HashSet::new();
            let mut peers = HashSet::new();
            let mut addresses = HashSet::new();
            for _ in 0..replicas {
                if !nodes.insert(r.id()?) || !incarnations.insert(r.id()?) {
                    return Err(invalid("duplicate CDC replica node/incarnation"));
                }
                let replica = r.id()?;
                if previous_replica.is_some_and(|p| p >= replica) {
                    return Err(invalid("noncanonical CDC replicas"));
                }
                previous_replica = Some(replica);
                match r.take(1)?[0] {
                    1 => voters += 1,
                    2 => (),
                    _ => return Err(invalid("invalid replica role")),
                }
                let peer = r.ascii(128)?;
                let address = r.ascii(256)?;
                if !peer
                    .iter()
                    .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(b))
                {
                    return Err(invalid("invalid CDC peer ID"));
                }
                validate_endpoint(&address)?;
                if !peers.insert(peer) || !addresses.insert(address) {
                    return Err(invalid("duplicate CDC replica peer/address"));
                }
            }
            if !(1..=5).contains(&voters) {
                return Err(invalid("invalid CDC voter count"));
            }
        }
        if !r.0.is_empty() {
            return Err(invalid("trailing canonical CDC directory"));
        }
        Ok(result)
    }
    /// Admit all current active lineages explicitly; topology changes require a new barrier.
    pub fn definition(
        &self,
        subscription_id: Id,
        incarnation_id: Id,
        columns: Vec<Id>,
        start: Checkpoint,
    ) -> Result<Definition> {
        // Public fields can be changed by a caller, so validate the carrier again before hashing it.
        let wire = wire::CdcDirectory {
            canonical: self.canonical.clone(),
            cluster_id: self.cluster_id.to_vec(),
            database_id: self.database_id.to_vec(),
            domain_id: self.domain_id.to_vec(),
            schema_epoch: self.schema_epoch,
            schema_digest: self.schema_digest.to_vec(),
            generation: self.generation,
            columns: self.columns.iter().map(|id| id.to_vec()).collect(),
            routing_prefix_bytes: self.routing_prefix_bytes,
            ranges: self
                .ranges
                .iter()
                .map(|r| {
                    Ok(wire::CdcRange {
                        lineage: Some(r.lineage.encode()?),
                        configuration_version: r.configuration_version,
                        topology: r.topology as i32,
                        lower_inclusive: r.lower_inclusive.clone(),
                        upper_exclusive: r.upper_exclusive.clone(),
                    })
                })
                .collect::<Result<_>>()?,
        };
        Self::decode(wire, self.domain_id)?;
        let lineages = self
            .ranges
            .iter()
            .map(|r| r.lineage)
            .collect::<HashSet<_>>();
        if self
            .ranges
            .iter()
            .any(|r| r.topology != Topology::RangeActive)
            || columns.iter().any(|c| !self.columns.contains(c))
            || lineages != start.0.keys().copied().collect::<HashSet<_>>()
        {
            return Err(invalid("CDC definition does not cover active directory"));
        }
        let mut definition = Definition {
            cluster_id: self.cluster_id,
            database_id: self.database_id,
            domain_id: self.domain_id,
            subscription_id,
            incarnation_id,
            directory_digest: Sha256::digest(&self.canonical).into(),
            columns,
            start,
        };
        definition.columns.sort_by(uuid_order);
        definition.validate()?;
        Ok(definition)
    }
}
#[derive(Clone, Copy, Debug)]
struct Deadline(tokio::time::Instant);
#[derive(Clone, Copy, Debug)]
struct DeadlineInterceptor;
impl Interceptor for DeadlineInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>> {
        if let Some(deadline) = request.extensions().get::<Deadline>() {
            let remaining = deadline
                .0
                .saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(RpcStatus::deadline_exceeded("CDC deadline expired"));
            }
            request.set_timeout(remaining);
        }
        Ok(request)
    }
}
type Client = wire::distributed_cdc_service_client::DistributedCdcServiceClient<
    InterceptedService<Channel, DeadlineInterceptor>,
>;
#[derive(Clone, Debug)]
pub struct CdcClient {
    client: Client,
    context: RequestContext,
}
impl CdcClient {
    /// Uses capabilities already negotiated on the caller's channel. No implicit consumption or ACK.
    pub fn new(
        channel: Channel,
        context: RequestContext,
        capabilities: &crate::Capabilities,
    ) -> Result<Self> {
        if capabilities.cdc_mode != crate::CdcMode::VectorSubscriptions
            || capabilities.backend_mode != crate::BackendMode::Distributed
        {
            return Err(RpcStatus::failed_precondition(
                "backend does not support vector CDC",
            ));
        }
        Ok(Self {
            client:
                wire::distributed_cdc_service_client::DistributedCdcServiceClient::with_interceptor(
                    channel,
                    DeadlineInterceptor,
                )
                .max_decoding_message_size(MAX_FRAME_BYTES)
                .max_encoding_message_size(MAX_REQUEST_BYTES),
            context,
        })
    }
    pub fn with_context(&self, context: RequestContext) -> Self {
        Self {
            client: self.client.clone(),
            context,
        }
    }
    async fn call<T: Message, R, F, Fut>(&self, message: T, call: F) -> Result<R>
    where
        F: FnOnce(Client, Request<T>) -> Fut,
        Fut: Future<Output = Result<tonic::Response<R>>>,
    {
        if message.encoded_len() > MAX_REQUEST_BYTES {
            return Err(invalid("oversized CDC request"));
        }
        if !matches!(self.context.profile, 2 | 3 | 4 | 6)
            || self.context.timeout_nanos <= 0
            || self.context.profile == 2 && self.context.timeout_nanos == i64::MAX
        {
            return Err(invalid("invalid CDC request context"));
        }
        let mut request = Request::new(message);
        let deadline = if self.context.timeout_nanos == i64::MAX {
            None
        } else {
            let timeout = Duration::from_nanos(self.context.timeout_nanos as u64);
            let deadline = tokio::time::Instant::now()
                .checked_add(timeout)
                .ok_or_else(|| invalid("CDC timeout exceeds clock range"))?;
            request.set_timeout(timeout);
            request.extensions_mut().insert(Deadline(deadline));
            Some(deadline)
        };
        let future = call(self.client.clone(), request);
        let response = if let Some(deadline) = deadline {
            tokio::time::timeout_at(deadline, future)
                .await
                .map_err(|_| RpcStatus::deadline_exceeded("CDC deadline expired"))??
        } else {
            future.await?
        };
        Ok(response.into_inner())
    }
    fn subscription(&self, subscription_id: Id) -> wire::CdcSubscriptionRequest {
        wire::CdcSubscriptionRequest {
            subscription_id: subscription_id.to_vec(),
            context: Some(self.context.clone()),
        }
    }
    pub async fn context(&self, domain_id: Id) -> Result<Directory> {
        Directory::decode(
            self.call(
                wire::CdcContextRequest {
                    domain_id: domain_id.to_vec(),
                    context: Some(self.context.clone()),
                },
                |mut c, r| async move { c.context(r).await },
            )
            .await?,
            domain_id,
        )
    }
    pub async fn create(&self, definition: Definition) -> Result<Status> {
        let result = Status::decode(
            self.call(
                wire::CdcCreateRequest {
                    definition: Some(definition.encode()?),
                    context: Some(self.context.clone()),
                },
                |mut c, r| async move { c.create(r).await },
            )
            .await?,
            definition.subscription_id,
        )?;
        let mut expected = definition;
        expected.columns.sort_by(uuid_order);
        if result.state.definition != expected {
            return Err(invalid("CDC create response changed definition"));
        }
        Ok(result)
    }
    pub async fn recover(&self, subscription_id: Id) -> Result<Status> {
        Status::decode(
            self.call(self.subscription(subscription_id), |mut c, r| async move {
                c.recover(r).await
            })
            .await?,
            subscription_id,
        )
    }
    pub async fn status(&self, subscription_id: Id) -> Result<Status> {
        Status::decode(
            self.call(self.subscription(subscription_id), |mut c, r| async move {
                c.status(r).await
            })
            .await?,
            subscription_id,
        )
    }
    pub async fn poll(
        &self,
        subscription_id: Id,
        max_records: i32,
        max_bytes: i32,
    ) -> Result<Poll> {
        if !(1..=MAX_RECORDS).contains(&max_records)
            || !(MIN_POLL_BYTES..=MAX_POLL_BYTES).contains(&max_bytes)
        {
            return Err(invalid("invalid CDC native poll budget"));
        }
        Poll::decode(
            self.call(
                wire::CdcPollRequest {
                    subscription_id: subscription_id.to_vec(),
                    max_records,
                    max_bytes,
                    context: Some(self.context.clone()),
                },
                |mut c, r| async move { c.poll(r).await },
            )
            .await?,
            subscription_id,
            max_records,
        )
    }
    pub async fn acknowledge(&self, subscription_id: Id, batch_id: Id) -> Result<Status> {
        Status::decode(
            self.call(
                wire::CdcAcknowledgeRequest {
                    subscription_id: subscription_id.to_vec(),
                    batch_id: batch_id.to_vec(),
                    context: Some(self.context.clone()),
                },
                |mut c, r| async move { c.acknowledge(r).await },
            )
            .await?,
            subscription_id,
        )
    }
    pub async fn close_subscription(&self, subscription_id: Id) -> Result<Status> {
        Status::decode(
            self.call(self.subscription(subscription_id), |mut c, r| async move {
                c.close_subscription(r).await
            })
            .await?,
            subscription_id,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn lineage() -> Lineage {
        Lineage {
            range_id: [1; 16],
            group_id: [2; 16],
            range_epoch: 3,
        }
    }
    fn position() -> Position {
        Position { term: 2, index: 4 }
    }
    fn checkpoint() -> Checkpoint {
        Checkpoint(BTreeMap::from([(lineage(), Cursor::beginning())]))
    }
    fn definition() -> Definition {
        Definition {
            cluster_id: [3; 16],
            database_id: [4; 16],
            domain_id: [5; 16],
            subscription_id: [6; 16],
            incarnation_id: [7; 16],
            directory_digest: [8; 32],
            columns: vec![[9; 16]],
            start: checkpoint(),
        }
    }
    fn operation() -> wire::CdcOperationId {
        wire::CdcOperationId {
            session_id: [10; 16].to_vec(),
            epoch: 1,
            sequence: 2,
        }
    }
    fn envelope() -> wire::CdcEnvelope {
        wire::CdcEnvelope {
            position: Some(position().encode()),
            schema_digest: [11; 32].to_vec(),
            operation: Some(operation()),
            transaction: None,
            events: vec![wire::CdcEvent {
                mutation_ordinal: 3,
                mutation: Some(wire::CdcMutation {
                    kind: MutationKind::MutationMerge as i32,
                    column_id: [9; 16].to_vec(),
                    key: vec![1],
                    value: Some(vec![2]),
                    return_mode: ReturnMode::ReturnNone as i32,
                    member_keys: vec![],
                }),
            }],
        }
    }
    fn batch() -> wire::CdcBatch {
        wire::CdcBatch {
            id: [12; 16].to_vec(),
            definition: Some(definition().encode().unwrap()),
            lineage: Some(lineage().encode().unwrap()),
            from: Some(Cursor::beginning().encode().unwrap()),
            next: Some(Cursor::after(position()).unwrap().encode().unwrap()),
            cut: Some(position().encode()),
            envelopes: vec![envelope()],
        }
    }
    fn poll() -> wire::CdcPoll {
        wire::CdcPoll {
            lineage: Some(lineage().encode().unwrap()),
            outcome: wire::CdcOutcome::Data as i32,
            batch: Some(batch()),
            required_bytes: 0,
            floor: Some(Cursor::beginning().encode().unwrap()),
            cut: Some(position().encode()),
        }
    }
    #[test]
    fn definition_and_checkpoint_roundtrip_qualified_lineages() {
        let d = definition();
        let wire = d.encode().unwrap();
        let bytes = wire.encode_to_vec();
        assert_eq!(
            Definition::try_from(wire::CdcDefinition::decode(bytes.as_slice()).unwrap()).unwrap(),
            d
        );
        let mut c = checkpoint().encode().unwrap();
        c.entries.push(c.entries[0].clone());
        assert!(Checkpoint::try_from(c).is_err());
        let mut changed = lineage();
        changed.range_epoch += 1;
        assert!(checkpoint()
            .advance(changed, Cursor::after(position()).unwrap())
            .is_err());
        let mut bad = d.encode().unwrap();
        bad.subscription_id.pop();
        assert!(Definition::try_from(bad).is_err());
        let mut bad = d.encode().unwrap();
        bad.columns.push(bad.columns[0].clone());
        assert!(Definition::try_from(bad).is_err());
    }
    #[test]
    fn cursor_rejects_term_substitution_and_regression() {
        let after = Cursor::after(position()).unwrap();
        assert!(after.covers(&Cursor::beginning()).unwrap());
        let mut changed = after;
        changed.position.term += 1;
        assert!(changed.covers(&after).is_err());
        changed = after;
        changed.position.index += 1;
        changed.position.term -= 1;
        assert!(changed.covers(&after).is_err());
        changed = after;
        changed.mutation_ordinal = MAX_EVENTS_PER_ENVELOPE + 1;
        assert!(changed.encode().is_err());
    }
    #[test]
    fn projected_data_preserves_selected_merge_operand_and_original_ordinal() {
        let bytes = poll().encode_to_vec();
        let result = Poll::decode(
            wire::CdcPoll::decode(bytes.as_slice()).unwrap(),
            definition().subscription_id,
            1,
        )
        .unwrap();
        let Outcome::Data(batch) = result.outcome else {
            panic!("expected data")
        };
        assert_eq!(batch.envelopes[0].events[0].cursor.mutation_ordinal, 3);
        assert_eq!(
            batch.envelopes[0].events[0].mutation.kind,
            MutationKind::MutationMerge
        );
        assert_eq!(batch.envelopes[0].events[0].mutation.value, Some(vec![2]));
        assert_eq!(batch.next, Cursor::after(position()).unwrap());
        assert_eq!(
            batch.budget_bytes(),
            116 + definition().budget_bytes() + 88 + 4 + 28
        );
    }
    #[test]
    fn empty_advancing_batch_is_data_requiring_explicit_ack() {
        let mut v = poll();
        v.batch.as_mut().unwrap().envelopes.clear();
        let result = Poll::decode(v, definition().subscription_id, 1).unwrap();
        let Outcome::Data(batch) = result.outcome else {
            panic!("expected advancing data")
        };
        assert!(batch.envelopes.is_empty());
        assert_ne!(batch.from, batch.next);
        assert_eq!(batch.id, [12; 16]); // Consumer still receives a batch identity to ACK explicitly.
    }
    #[test]
    fn gaps_and_budget_outcomes_retain_lineage_and_reject_data_payloads() {
        for outcome in [wire::CdcOutcome::Gap, wire::CdcOutcome::TooLarge] {
            let mut v = poll();
            v.outcome = outcome as i32;
            v.batch = None;
            v.required_bytes = if outcome == wire::CdcOutcome::TooLarge {
                MAX_POLL_BYTES
            } else {
                0
            };
            let decoded = Poll::decode(v.clone(), definition().subscription_id, 1).unwrap();
            assert_eq!(decoded.lineage, lineage());
            assert_eq!(decoded.cut, position());
            v.batch = Some(batch());
            assert!(Poll::decode(v, definition().subscription_id, 1).is_err());
        }
        let mut v = poll();
        v.outcome = wire::CdcOutcome::TooLarge as i32;
        v.batch = None;
        assert!(Poll::decode(v, definition().subscription_id, 1).is_err());
    }
    #[test]
    fn batches_reject_unselected_columns_wrong_identity_order_and_cut() {
        let mut v = batch();
        v.envelopes[0].events[0]
            .mutation
            .as_mut()
            .unwrap()
            .column_id = [99; 16].to_vec();
        assert!(Batch::try_from(v).is_err());
        let mut v = poll();
        v.batch
            .as_mut()
            .unwrap()
            .definition
            .as_mut()
            .unwrap()
            .subscription_id = [99; 16].to_vec();
        assert!(Poll::decode(v, definition().subscription_id, 1).is_err());
        let mut v = batch();
        v.envelopes.push(envelope());
        assert!(Batch::try_from(v).is_err());
        let mut v = batch();
        v.next = Some(Cursor::beginning().encode().unwrap());
        assert!(Batch::try_from(v).is_err());
        let mut v = envelope();
        v.events.push(v.events[0].clone());
        assert!(Envelope::try_from(v).is_err());
        let mut v = envelope();
        v.events[0].mutation_ordinal = MAX_EVENTS_PER_ENVELOPE;
        assert!(Envelope::try_from(v).is_err());
    }
    #[test]
    fn retained_batch_is_not_rejected_by_smaller_retry_record_budget() {
        let mut v = poll();
        let mut next = envelope();
        next.position.as_mut().unwrap().index += 1;
        let b = v.batch.as_mut().unwrap();
        b.envelopes.push(next);
        b.cut.as_mut().unwrap().index += 1;
        b.next.as_mut().unwrap().position.as_mut().unwrap().index += 1;
        v.cut.as_mut().unwrap().index += 1;
        assert!(Poll::decode(v, definition().subscription_id, 1).is_ok());
    }

    #[test]
    fn mutation_presence_bucket_members_and_unsigned_range_bounds_are_strict() {
        let mutation = || envelope().events[0].mutation.clone().unwrap();
        let mut v = mutation();
        v.kind = MutationKind::MutationDelete as i32;
        assert!(Mutation::try_from(v).is_err());
        let mut v = mutation();
        v.kind = MutationKind::MutationBucketMerge as i32;
        assert!(Mutation::try_from(v).is_err());
        let mut v = mutation();
        v.kind = MutationKind::MutationBucketMerge as i32;
        v.member_keys = vec![vec![]];
        assert!(Mutation::try_from(v).is_ok());
        let mut v = mutation();
        v.kind = MutationKind::MutationDeleteRange as i32;
        v.key = vec![128];
        v.value = Some(vec![127]);
        assert!(Mutation::try_from(v).is_err());
        let mut v = mutation();
        v.value = Some(vec![0; MAX_COMMAND_BYTES]);
        assert!(Mutation::try_from(v).is_err());
        let mut v = envelope();
        v.events[0].mutation.as_mut().unwrap().value = Some(vec![0; MAX_COMMAND_BYTES - 26]);
        assert!(Envelope::try_from(v).is_err());
    }
    fn finalization() -> wire::CdcFinalization {
        wire::CdcFinalization {
            home_group: [13; 16].to_vec(),
            home_epoch: 1,
            participant_group: lineage().group_id.to_vec(),
            recovery_operation: Some(wire::CdcOperationId {
                sequence: 99,
                ..operation()
            }),
            decision: Some(wire::CdcDecision {
                cluster_id: definition().cluster_id.to_vec(),
                database_id: definition().database_id.to_vec(),
                domain_id: definition().domain_id.to_vec(),
                transaction_id: [14; 16].to_vec(),
                operation: Some(operation()),
                schema_epoch: 1,
                schema_digest: [11; 32].to_vec(),
                plan_digest: [15; 32].to_vec(),
                phase: DecisionPhase::DecisionCommit as i32,
                participants: vec![wire::CdcParticipant {
                    lineage: Some(lineage().encode().unwrap()),
                    schema_digest: [11; 32].to_vec(),
                    plan_digest: [15; 32].to_vec(),
                    prepared: Some(Position { term: 1, index: 2 }.encode()),
                }],
            }),
        }
    }
    #[test]
    fn transaction_metadata_binds_commit_operation_preparation_and_namespace() {
        let mut v = batch();
        v.envelopes[0].transaction = Some(finalization());
        let b = Batch::try_from(v.clone()).unwrap();
        assert_eq!(
            b.envelopes[0]
                .transaction
                .as_ref()
                .unwrap()
                .decision
                .transaction_id,
            [14; 16]
        );
        v.envelopes[0]
            .transaction
            .as_mut()
            .unwrap()
            .decision
            .as_mut()
            .unwrap()
            .domain_id = [99; 16].to_vec();
        assert!(Batch::try_from(v).is_err());
        let mut v = envelope();
        let mut t = finalization();
        t.decision.as_mut().unwrap().participants[0].prepared = Some(position().encode());
        v.transaction = Some(t);
        assert!(Envelope::try_from(v).is_err());
        let mut v = envelope();
        let mut t = finalization();
        t.decision.as_mut().unwrap().phase = DecisionPhase::DecisionAbort as i32;
        v.transaction = Some(t);
        assert!(Envelope::try_from(v).is_err());
    }
    #[test]
    fn status_generation_is_bounded_by_delivery_ack_and_closure() {
        let state = || wire::CdcState {
            definition: Some(definition().encode().unwrap()),
            phase: StatePhase::SubscriptionActive as i32,
            generation: 0,
            checkpoint: Some(checkpoint().encode().unwrap()),
            delivered: Some(checkpoint().encode().unwrap()),
        };
        let status = wire::CdcStatus {
            state: Some(state()),
            phase: LocalPhase::Create as i32,
        };
        assert!(Status::decode(status.clone(), definition().subscription_id).is_ok());
        assert!(Status::decode(status, [99; 16]).is_err());
        let mut v = state();
        v.generation = 1;
        assert!(State::try_from(v).is_err());
        let mut v = state();
        v.delivered = Some(
            checkpoint()
                .advance(lineage(), Cursor::after(position()).unwrap())
                .unwrap()
                .encode()
                .unwrap(),
        );
        v.generation = 1;
        assert!(State::try_from(v.clone()).is_ok());
        v.checkpoint = v.delivered.clone();
        assert!(State::try_from(v).is_err());
    }
    type ReplicaFixture<'a> = (Id, Id, Id, &'a [u8], &'a [u8]);
    fn directory() -> wire::CdcDirectory {
        directory_with_replicas(&[([16; 16], [17; 16], [18; 16], b"peer", b"localhost:9000")])
    }
    fn directory_with_replicas(replicas: &[ReplicaFixture<'_>]) -> wire::CdcDirectory {
        let mut canonical = Vec::new();
        canonical.extend_from_slice(&1i32.to_be_bytes());
        for uuid in [
            definition().cluster_id,
            definition().database_id,
            definition().domain_id,
        ] {
            canonical.extend_from_slice(&uuid);
        }
        canonical.extend_from_slice(&1i64.to_be_bytes());
        canonical.extend_from_slice(&[11; 32]);
        canonical.extend_from_slice(&0i64.to_be_bytes());
        canonical.extend_from_slice(&0i32.to_be_bytes());
        canonical.extend_from_slice(&1i32.to_be_bytes());
        canonical.extend_from_slice(&[9; 16]);
        canonical.extend_from_slice(&1i32.to_be_bytes());
        canonical.extend_from_slice(&lineage().range_id);
        canonical.extend_from_slice(&lineage().group_id);
        canonical.extend_from_slice(&3i64.to_be_bytes());
        canonical.extend_from_slice(&0i64.to_be_bytes());
        canonical.extend_from_slice(&(-1i32).to_be_bytes());
        canonical.extend_from_slice(&(-1i32).to_be_bytes());
        canonical.push(1);
        canonical.extend_from_slice(&(replicas.len() as i32).to_be_bytes());
        for (node, incarnation, replica, peer, address) in replicas {
            for uuid in [node, incarnation, replica] {
                canonical.extend_from_slice(uuid);
            }
            canonical.push(1);
            for s in [peer, address] {
                canonical.extend_from_slice(&(s.len() as i32).to_be_bytes());
                canonical.extend_from_slice(s);
            }
        }
        wire::CdcDirectory {
            canonical,
            cluster_id: definition().cluster_id.to_vec(),
            database_id: definition().database_id.to_vec(),
            domain_id: definition().domain_id.to_vec(),
            schema_epoch: 1,
            schema_digest: [11; 32].to_vec(),
            generation: 0,
            columns: vec![[9; 16].to_vec()],
            routing_prefix_bytes: 0,
            ranges: vec![wire::CdcRange {
                lineage: Some(lineage().encode().unwrap()),
                configuration_version: 0,
                topology: Topology::RangeActive as i32,
                lower_inclusive: None,
                upper_exclusive: None,
            }],
        }
    }
    #[test]
    fn canonical_placement_rejects_duplicate_nodes_incarnations_peers_and_endpoints() {
        let first = (
            [16; 16],
            [17; 16],
            [18; 16],
            b"peer".as_slice(),
            b"localhost:9000".as_slice(),
        );
        let second = (
            [19; 16],
            [20; 16],
            [21; 16],
            b"other".as_slice(),
            b"localhost:9001".as_slice(),
        );
        assert!(Directory::decode(
            directory_with_replicas(&[first, second]),
            definition().domain_id
        )
        .is_ok());
        for field in 0..5 {
            let mut duplicate = second;
            match field {
                0 => duplicate.0 = first.0,
                1 => duplicate.1 = first.1,
                2 => duplicate.2 = first.2,
                3 => duplicate.3 = first.3,
                _ => duplicate.4 = first.4,
            }
            assert!(Directory::decode(
                directory_with_replicas(&[first, duplicate]),
                definition().domain_id
            )
            .is_err());
        }
    }
    #[test]
    fn canonical_placement_requires_exact_peer_and_host_port_syntax() {
        let replica = (
            [16; 16],
            [17; 16],
            [18; 16],
            b"peer".as_slice(),
            b"localhost:9000".as_slice(),
        );
        for peer in [b"bad peer".as_slice(), b"a/b", b"a:b", b"", b"x@y"] {
            let mut invalid = replica;
            invalid.3 = peer;
            assert!(
                Directory::decode(directory_with_replicas(&[invalid]), definition().domain_id)
                    .is_err()
            );
        }
        for endpoint in [
            b"localhost".as_slice(),
            b":123",
            b"localhost:",
            b"localhost:0",
            b"localhost:65536",
            b"localhost:000001",
            b"localhost:abc",
            b"::1:9000",
            b"user@host:9000",
            b"host/name:9000",
        ] {
            let mut invalid = replica;
            invalid.4 = endpoint;
            assert!(
                Directory::decode(directory_with_replicas(&[invalid]), definition().domain_id)
                    .is_err()
            );
        }
        for endpoint in [
            b"[::1]:9000".as_slice(),
            b"host-name.example:65535",
            b"127.0.0.1:00001",
        ] {
            let mut valid = replica;
            valid.4 = endpoint;
            assert!(
                Directory::decode(directory_with_replicas(&[valid]), definition().domain_id)
                    .is_ok()
            );
        }
    }
    #[test]
    fn transaction_prepared_vote_requires_positive_index() {
        let mut finalization = finalization();
        finalization.decision.as_mut().unwrap().participants[0].prepared =
            Some(Position { term: 1, index: 0 }.encode());
        assert!(Finalization::try_from(finalization).is_err());
    }
    #[test]
    fn directory_uses_unsigned_uuid_order_while_definition_and_checkpoint_use_signed_order() {
        let positive = [0u8; 16];
        let mut negative = positive;
        negative[0] = 0x80;
        let mut v = directory();
        v.columns = vec![negative.to_vec(), positive.to_vec()];
        v.canonical[104..108].copy_from_slice(&2i32.to_be_bytes());
        v.canonical[108..124].copy_from_slice(&positive);
        v.canonical.splice(124..124, negative);
        let d = Directory::decode(v.clone(), definition().domain_id).unwrap();
        let definition = d
            .definition([6; 16], [7; 16], vec![positive, negative], checkpoint())
            .unwrap();
        assert_eq!(definition.columns, vec![negative, positive]);
        assert_eq!(
            definition.encode().unwrap().columns,
            vec![negative.to_vec(), positive.to_vec()]
        );
        v.canonical[108..124].copy_from_slice(&negative);
        v.canonical[124..140].copy_from_slice(&positive);
        assert!(Directory::decode(v, definition.domain_id).is_err());
        let checkpoint = Checkpoint(BTreeMap::from([
            (
                Lineage {
                    range_id: positive,
                    ..lineage()
                },
                Cursor::beginning(),
            ),
            (
                Lineage {
                    range_id: negative,
                    ..lineage()
                },
                Cursor::beginning(),
            ),
        ]));
        let wire = checkpoint.encode().unwrap();
        assert_eq!(wire.entries[0].lineage.as_ref().unwrap().range_id, negative);
        assert_eq!(wire.entries[1].lineage.as_ref().unwrap().range_id, positive);
        assert_eq!(Checkpoint::try_from(wire).unwrap(), checkpoint);
    }
    #[test]
    fn context_crosschecks_canonical_namespace_and_lineages_before_definition_hash() {
        let wire = directory();
        let d = Directory::decode(wire.clone(), definition().domain_id).unwrap();
        let expected: [u8; 32] = Sha256::digest(&wire.canonical).into();
        assert_eq!(
            d.definition([6; 16], [7; 16], vec![[9; 16]], checkpoint())
                .unwrap()
                .directory_digest,
            expected
        );
        let mut v = wire.clone();
        v.domain_id = [99; 16].to_vec();
        assert!(Directory::decode(v, [99; 16]).is_err());
        let mut v = wire.clone();
        v.ranges[0].lineage.as_mut().unwrap().range_epoch += 1;
        assert!(Directory::decode(v, definition().domain_id).is_err());
        let mut v = wire;
        v.canonical.pop();
        assert!(Directory::decode(v, definition().domain_id).is_err());
        assert!(d
            .definition([6; 16], [7; 16], vec![[99; 16]], checkpoint())
            .is_err());
    }
}
