//! Inspect or resume the original retained UUID. Ordinary create/delete/upload calls issue
//! fresh identities. Dropping a lifecycle future cancels observation, not server-owned work.
use crate::{
    client::{await_bound_call, DeadlineInterceptor},
    schema_proto as wire, RequestContext, RockserverClient,
};
use prost::Message;
use tonic::{service::interceptor::InterceptedService, transport::Channel, Status};
type Result<T> = std::result::Result<T, Status>;
const MAX_FRAME: usize = 65536;
const MAX_OPERATIONS: usize = 128;
fn bad() -> Status {
    Status::data_loss("Malformed retained schema operation")
}
macro_rules! enumeration {
    ($name:ident {$($variant:ident),+}) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq)] pub enum $name {$($variant),+}
        impl $name { fn decode(tag: i32) -> Result<Self> { const VALUES: &[$name] = &[$($name::$variant),+];
            usize::try_from(tag).ok().and_then(|n| VALUES.get(n)).copied().ok_or_else(bad) } }
    }
}
enumeration!(SchemaOperationKind {
    Create,
    Delete,
    DeleteIfExists,
    Upload
});
enumeration!(SchemaOperationState {
    Queued,
    NotStarted,
    Running,
    Paused,
    Done,
    Aborted,
    Conflict
});
enumeration!(SchemaOperationPhase {
    Allocate,
    Schema,
    Definition,
    Begin,
    Artifacts,
    Capacity,
    Admissions,
    Fencing,
    Fence,
    Install,
    Publish,
    Activate,
    Retire,
    Cleanup,
    Done,
    Aborted,
    Conflict,
    OperatorBegin,
    OperatorChunks,
    OperatorPublish
});
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaOperationResult {
    pub request_id: [u8; 16],
    pub kind: SchemaOperationKind,
    pub value: i64,
    pub changed: bool,
    pub aborted: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaOperationStatus {
    pub request_id: [u8; 16],
    pub kind: SchemaOperationKind,
    pub state: SchemaOperationState,
    pub phase: Option<SchemaOperationPhase>,
    pub result: Option<SchemaOperationResult>,
}
impl SchemaOperationStatus {
    fn decode(w: wire::SchemaOperationStatus, expected: Option<[u8; 16]>) -> Result<Self> {
        if w.encoded_len() > MAX_FRAME {
            return Err(bad());
        }
        let request_id: [u8; 16] = w.request_id.as_slice().try_into().map_err(|_| bad())?;
        if expected.is_some_and(|id| id != request_id) {
            return Err(bad());
        }
        let kind = SchemaOperationKind::decode(w.kind)?;
        let state = SchemaOperationState::decode(w.state)?;
        let phase = w.phase.map(SchemaOperationPhase::decode).transpose()?;
        let result = w
            .result
            .map(|r| -> Result<SchemaOperationResult> {
                let id: [u8; 16] = r.request_id.as_slice().try_into().map_err(|_| bad())?;
                let result_kind = SchemaOperationKind::decode(r.kind)?;
                if id != request_id || result_kind != kind || r.aborted && r.changed {
                    return Err(bad());
                }
                Ok(SchemaOperationResult {
                    request_id: id,
                    kind: result_kind,
                    value: r.value,
                    changed: r.changed,
                    aborted: r.aborted,
                })
            })
            .transpose()?;
        use SchemaOperationState::*;
        if matches!(state, Done | Aborted) != result.is_some()
            || result
                .as_ref()
                .is_some_and(|r| r.aborted != (state == Aborted))
            || state == Done && phase.is_some_and(|p| p != SchemaOperationPhase::Done)
            || state == Aborted && phase.is_some_and(|p| p != SchemaOperationPhase::Aborted)
            || state == Conflict && phase.is_some_and(|p| p != SchemaOperationPhase::Conflict)
        {
            return Err(bad());
        }
        Ok(Self {
            request_id,
            kind,
            state,
            phase,
            result,
        })
    }
}
fn inventory(w: wire::SchemaOperationInventory) -> Result<Vec<SchemaOperationStatus>> {
    if w.encoded_len() > MAX_FRAME || w.operations.len() > MAX_OPERATIONS {
        return Err(bad());
    }
    let values: Vec<_> = w
        .operations
        .into_iter()
        .map(|s| SchemaOperationStatus::decode(s, None))
        .collect::<Result<_>>()?;
    if values
        .windows(2)
        .any(|p| p[0].request_id >= p[1].request_id)
    {
        return Err(bad());
    }
    Ok(values)
}
#[derive(Clone, Debug)]
pub struct SchemaOperationsClient {
    base: RockserverClient,
    client: wire::schema_operations_service_client::SchemaOperationsServiceClient<
        InterceptedService<Channel, DeadlineInterceptor>,
    >,
}
macro_rules! operation {
    ($name:ident) => {
        pub async fn $name(&self, id: [u8; 16]) -> Result<SchemaOperationStatus> {
            let prepared = self.base.contextual_request(wire::SchemaOperationRequest {
                request_id: id.to_vec(),
                context: Some(self.base.context.clone()),
            })?;
            let mut client = self.client.clone();
            SchemaOperationStatus::decode(
                await_bound_call(prepared.deadline, client.$name(prepared.request))
                    .await?
                    .into_inner(),
                Some(id),
            )
        }
    };
}
impl SchemaOperationsClient {
    pub(crate) fn from_client(base: RockserverClient, channel: Channel) -> Self {
        Self {base,client:wire::schema_operations_service_client::SchemaOperationsServiceClient::with_interceptor(channel,DeadlineInterceptor).max_decoding_message_size(MAX_FRAME)}
    }
    pub fn with_context(&self, context: RequestContext) -> Self {
        Self {
            base: self.base.with_context(context),
            client: self.client.clone(),
        }
    }
    pub async fn inventory(&self) -> Result<Vec<SchemaOperationStatus>> {
        let prepared = self
            .base
            .contextual_request(wire::SchemaOperationInventoryRequest {
                context: Some(self.base.context.clone()),
            })?;
        let mut client = self.client.clone();
        inventory(
            await_bound_call(prepared.deadline, client.inventory(prepared.request))
                .await?
                .into_inner(),
        )
    }
    operation!(status);
    operation!(resume);
    operation!(abort);
}
#[cfg(test)]
mod tests {
    use super::*;
    fn paused() -> wire::SchemaOperationStatus {
        wire::SchemaOperationStatus {
            request_id: vec![1; 16],
            kind: 0,
            state: 3,
            phase: Some(8),
            result: None,
        }
    }
    #[test]
    fn strict_identity_enums_result_and_inventory() {
        assert!(SchemaOperationStatus::decode(paused(), Some([1; 16])).is_ok());
        assert!(SchemaOperationStatus::decode(paused(), Some([2; 16])).is_err());
        let mut w = paused();
        w.kind = 99;
        assert!(SchemaOperationStatus::decode(w, None).is_err());
        let mut w = paused();
        w.phase = Some(20);
        assert!(SchemaOperationStatus::decode(w, None).is_err());
        let mut w = paused();
        w.state = 4;
        assert!(SchemaOperationStatus::decode(w, None).is_err());
        let mut w = paused();
        w.request_id.pop();
        assert!(SchemaOperationStatus::decode(w, None).is_err());
        assert!(inventory(wire::SchemaOperationInventory {
            operations: vec![paused(); 129]
        })
        .is_err());
        assert!(inventory(wire::SchemaOperationInventory {
            operations: vec![paused(); 2]
        })
        .is_err());
        let mut w = paused();
        w.state = 5;
        w.phase = Some(15);
        w.result = Some(wire::SchemaOperationResult {
            request_id: vec![1; 16],
            kind: 0,
            value: 0,
            changed: false,
            aborted: true,
        });
        assert!(SchemaOperationStatus::decode(w.clone(), None).is_ok());
        w.result.as_mut().unwrap().kind = 3;
        assert!(SchemaOperationStatus::decode(w, None).is_err());
    }
}
