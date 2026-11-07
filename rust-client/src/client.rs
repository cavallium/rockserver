use crate::proto::rocks_db_service_client::RocksDbServiceClient;
use crate::proto::*; // For request/response types
use crate::types::*;
use crate::types::{Column, ColumnSchema};
use futures::{Stream, StreamExt};
use std::fmt::{Display, Formatter};
use std::future::Future;
use crate::sessions::{
    deadline_before_sent, multi_keys_bytes, multi_kv_bytes, partly_applied, read_multi_items, split_batch, MultiItems,
    SessionWrites,
    DEFAULT_RETRY_WINDOW, NUMBERED_MULTI_MAX_BYTES,
};
use futures::stream::BoxStream;
use futures::TryStreamExt;
use std::sync::Arc;
use std::time::Duration;
use tonic::service::interceptor::InterceptedService;
use tonic::service::Interceptor;
use tonic::transport::Channel;
use tonic::{Request, Status}; // Disambiguate from proto

pub type Result<T> = std::result::Result<T, Status>;

pub const REQUIRED_WORKLOAD_CONTRACT_VERSION: i32 = 3;

/// The content hash `check_merge_operator` takes: SHA-256 of the jar, as the server computes it.
pub fn merge_operator_hash(jar_payload: &[u8]) -> Vec<u8> {
    use sha2::Digest;
    sha2::Sha256::digest(jar_payload).to_vec()
}

fn duration_nanos(value: Duration) -> Result<i64> {
    if value.is_zero() {
        return Err(Status::invalid_argument("lease TTL must be positive"));
    }
    Ok(value.as_nanos().min(i64::MAX as u128) as i64)
}

#[derive(Debug)]
pub enum RockserverConnectError {
    Transport(tonic::transport::Error),
    Capabilities(Status),
}

impl Display for RockserverConnectError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(error) => write!(formatter, "failed to connect to Rockserver: {error}"),
            Self::Capabilities(error) => {
                write!(formatter, "Rockserver capability handshake failed: {error}")
            }
        }
    }
}

impl std::error::Error for RockserverConnectError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transport(error) => Some(error),
            Self::Capabilities(error) => Some(error),
        }
    }
}

/// Client for interacting with the RockServer via gRPC.
///
/// This client provides a safe and idiomatic Rust API over the raw gRPC service.
/// It handles type conversions and provides higher-level abstractions where appropriate.
///
/// # Cloning
/// The client is cheap to clone. Cloning creates a new handle to the same underlying connection.
#[derive(Clone, Debug)]
pub struct RockserverClient {
    client: DeadlineClient,
    context: RequestContext,
    /// Numbered writes (`crate::sessions`), on a replicated server that advertises client sessions; shared by
    /// every view of this client.
    sessions: Option<Arc<SessionWrites>>,
    session_retry_window: Duration,
}

type DeadlineClient = RocksDbServiceClient<InterceptedService<Channel, DeadlineInterceptor>>;

#[derive(Clone, Copy, Debug)]
struct BoundDeadline(tokio::time::Instant);

#[derive(Clone, Copy, Debug, Default)]
struct DeadlineInterceptor;

impl Interceptor for DeadlineInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>> {
        if let Some(bound) = request.extensions().get::<BoundDeadline>().copied() {
            let remaining = bound
                .0
                .saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(Status::deadline_exceeded(
                    "request deadline expired in client readiness queue",
                ));
            }
            request.set_timeout(remaining);
        }
        Ok(request)
    }
}

struct PreparedRequest<T> {
    request: Request<T>,
    deadline: Option<tokio::time::Instant>,
}

async fn await_bound_call<F, R>(deadline: Option<tokio::time::Instant>, future: F) -> Result<R>
where
    F: Future<Output = Result<R>>,
{
    if let Some(deadline) = deadline {
        tokio::time::timeout_at(deadline, future)
            .await
            .map_err(|_| {
                Status::deadline_exceeded("request deadline expired in client readiness queue")
            })?
    } else {
        future.await
    }
}

impl RockserverClient {
    /// Connect to the RockServer at the specified destination.
    ///
    /// The channel has one address and tonic's defaults: no HTTP/2 keepalive and no connect timeout. Calls are
    /// resent to the same node, and a node that stops answering without closing the connection is noticed only by
    /// a call's deadline (or, for a numbered write without one, its retry window). For failover or dead-peer
    /// detection, build a `Channel` yourself (`Endpoint::http2_keep_alive_interval`, `keep_alive_timeout`,
    /// `connect_timeout`, `Channel::balance_list` over several nodes) and pass it to [`RockserverClient::new`].
    ///
    /// # Arguments
    /// * `dst` - The destination to connect to. Can be a string like `http://[::1]:50051`.
    ///
    /// # Returns
    /// A `Result` containing the connected client or a transport/capability error.
    pub async fn connect<D>(
        dst: D,
        context: RequestContext,
    ) -> std::result::Result<Self, RockserverConnectError>
    where
        D: std::convert::TryInto<tonic::transport::Endpoint>,
        D::Error: Into<tonic::codegen::StdError>,
    {
        let endpoint =
            tonic::transport::Endpoint::new(dst).map_err(RockserverConnectError::Transport)?;
        let channel = endpoint
            .connect()
            .await
            .map_err(RockserverConnectError::Transport)?;
        let mut handshake_client = RocksDbServiceClient::new(channel.clone());
        let capabilities = Self::require_capabilities(&mut handshake_client)
            .await
            .map_err(RockserverConnectError::Capabilities)?;
        Ok(Self::with_capabilities(channel, context, &capabilities))
    }

    /// Creates a new client from an existing Tonic `Channel` after the mandatory
    /// workload-contract capability handshake.
    pub async fn new(channel: Channel, context: RequestContext) -> Result<Self> {
        let mut handshake_client = RocksDbServiceClient::new(channel.clone());
        let capabilities = Self::require_capabilities(&mut handshake_client).await?;
        Ok(Self::with_capabilities(channel, context, &capabilities))
    }

    fn with_capabilities(channel: Channel, context: RequestContext, capabilities: &CapabilitiesResponse) -> Self {
        Self {
            client: RocksDbServiceClient::with_interceptor(channel, DeadlineInterceptor),
            context,
            sessions: capabilities
                .client_sessions
                .then(|| Arc::new(SessionWrites::new(capabilities.numbered_batches))),
            session_retry_window: DEFAULT_RETRY_WINDOW,
        }
    }

    #[cfg(test)]
    fn new_unchecked(channel: Channel, context: RequestContext) -> Self {
        Self {
            client: RocksDbServiceClient::with_interceptor(channel, DeadlineInterceptor),
            context,
            sessions: None,
            session_retry_window: DEFAULT_RETRY_WINDOW,
        }
    }

    async fn require_capabilities(client: &mut RocksDbServiceClient<Channel>) -> Result<CapabilitiesResponse> {
        let mut request = Request::new(CapabilitiesRequest {});
        request.set_timeout(Duration::from_secs(10));
        let capabilities = client.get_capabilities(request).await?.into_inner();
        if capabilities.workload_contract_version != REQUIRED_WORKLOAD_CONTRACT_VERSION {
            return Err(Status::failed_precondition(format!(
				"incompatible Rockserver workload contract: required version {} with bounded ranges and resumable raw scans, got version {}",
				REQUIRED_WORKLOAD_CONTRACT_VERSION,
				capabilities.workload_contract_version,
			)));
        }
        Ok(capabilities)
    }

    /// Returns a client view that applies one mandatory workload context to every generic request.
    /// The underlying channel is shared and cloning this view is cheap.
    pub fn with_context(&self, context: RequestContext) -> Self {
        Self {
            client: self.client.clone(),
            context,
            sessions: self.sessions.clone(),
            session_retry_window: self.session_retry_window,
        }
    }

    fn validated_timeout(&self) -> Result<Option<Duration>> {
        match self.context.profile {
            2 | 3 | 4 | 6 => {}
            0 => {
                return Err(Status::invalid_argument(
                    "request workload profile is required",
                ))
            }
            1 | 5 | 7 => {
                return Err(Status::invalid_argument(
                    "workload profile is owned by Rockserver",
                ));
            }
            value => {
                return Err(Status::invalid_argument(format!(
                    "unknown workload profile: {value}"
                )));
            }
        }
        if self.context.workload_contract_version != REQUIRED_WORKLOAD_CONTRACT_VERSION {
            return Err(Status::invalid_argument(
                "request context must use workload contract version 3",
            ));
        }
        if self.context.timeout_nanos <= 0 {
            return Err(Status::invalid_argument("timeout_nanos must be positive"));
        }
        if self.context.profile == 2 && self.context.timeout_nanos == i64::MAX {
            return Err(Status::invalid_argument(
                "LATENCY requires a finite timeout",
            ));
        }

        Ok(if self.context.timeout_nanos == i64::MAX {
            None
        } else {
            Some(Duration::from_nanos(self.context.timeout_nanos as u64))
        })
    }

    fn contextual_request<T>(&self, message: T) -> Result<PreparedRequest<T>> {
        let timeout = self.validated_timeout()?;
        let deadline =
            timeout.and_then(|duration| tokio::time::Instant::now().checked_add(duration));
        let mut request = Request::new(message);
        if let Some(timeout) = timeout {
            request.set_timeout(timeout);
            request.extensions_mut().insert(BoundDeadline(
                deadline.expect("finite timeout has a deadline"),
            ));
        }
        Ok(PreparedRequest { request, deadline })
    }

    async fn call_contextual<T, R, F, Fut>(&self, message: T, call: F) -> Result<R>
    where
        F: FnOnce(DeadlineClient, Request<T>) -> Fut,
        Fut: Future<Output = Result<R>>,
    {
        let prepared = self.contextual_request(message)?;
        let future = call(self.client.clone(), prepared.request);
        await_bound_call(prepared.deadline, future).await
    }

    fn protected_request<T>(&self, message: T) -> Result<Request<T>> {
        if self.context.workload_contract_version != REQUIRED_WORKLOAD_CONTRACT_VERSION {
            return Err(Status::invalid_argument(
                "request context must use workload contract version 3",
            ));
        }
        Ok(Request::new(message))
    }

    // ============================================================================================
    // Transaction Management
    // ============================================================================================

    /// Opens a new transaction with the specified timeout.
    ///
    /// # Arguments
    /// * `timeout_ms` - The transaction timeout in milliseconds.
    ///
    /// # Returns
    /// The transaction ID.
    /// Opens a client session on a replicated server whose capabilities advertise `client_sessions`: the
    /// session id to number writes with (see `RequestContext::with_request_id`). Opening twice only leaves an
    /// unused session, which expires, so this call can be retried after any failure.
    pub async fn open_client_session(&self) -> Result<i64> {
        let mut context = self.context.clone();
        context.request_id = None;
        let req = OpenClientSessionRequest {
            context: Some(context),
        };
        let resp = self
            .call_contextual(req, |mut client, request| async move {
                client.open_client_session(request).await
            })
            .await?;
        Ok(resp.into_inner().session_id)
    }

    pub async fn open_transaction(&self, transaction_lease_ttl: Duration) -> Result<i64> {
        let req = OpenTransactionRequest {
            transaction_lease_ttl_nanos: duration_nanos(transaction_lease_ttl)?,
            context: Some(self.context.clone()),
        };
        let resp = self
            .call_contextual(req, |mut client, request| async move {
                client.open_transaction(request).await
            })
            .await?;
        Ok(resp.into_inner().transaction_id)
    }

    /// Closes an existing transaction.
    ///
    /// # Arguments
    /// * `transaction_id` - The ID of the transaction to close.
    /// * `commit` - If true, commits the transaction. If false, aborts it.
    /// # Returns
    /// `true` if the operation was successful.
    async fn close_transaction_once(&self, transaction_id: i64, commit: bool) -> Result<bool> {
        let req = CloseTransactionRequest {
            transaction_id,
            commit,
            context: Some(self.context.clone()),
        };
        let resp = if commit {
            self.call_contextual(req, |mut client, request| async move {
                client.close_transaction(request).await
            })
            .await?
        } else {
            self.client
                .clone()
                .close_transaction(self.protected_request(req)?)
                .await?
        };
        Ok(resp.into_inner().successful)
    }

    /// Closes a failed update explicitly.
    async fn close_failed_update_once(&self, update_id: i64) -> Result<()> {
        let req = CloseFailedUpdateRequest {
            update_id,
            workload_contract_version: REQUIRED_WORKLOAD_CONTRACT_VERSION,
        };
        self.client.clone().close_failed_update(req).await?;
        Ok(())
    }

    // ============================================================================================
    // Column Management
    // ============================================================================================

    /// Creates a new column with the given name and schema.
    async fn create_column_once(&self, name: String, schema: ColumnSchema) -> Result<i64> {
        let req = CreateColumnRequest {
            name,
            schema: Some(schema.into()),
            context: Some(self.context.clone()),
        };
        let resp = self
            .call_contextual(req, |mut client, request| async move {
                client.create_column(request).await
            })
            .await?;
        Ok(resp.into_inner().column_id)
    }

    /// Deletes a column by its ID.
    async fn delete_column_once(&self, column_id: i64) -> Result<()> {
        let req = DeleteColumnRequest {
            column_id,
            context: Some(self.context.clone()),
        };
        self.call_contextual(req, |mut client, request| async move {
            client.delete_column(request).await
        })
        .await?;
        Ok(())
    }

    /// Deletes a column by name if it exists.
    ///
    /// Returns `true` when a physical column was deleted and `false` when it was already absent.
    async fn delete_column_if_exists_once(&self, name: String) -> Result<bool> {
        let req = DeleteColumnIfExistsRequest {
            name,
            context: Some(self.context.clone()),
        };
        let resp = self
            .call_contextual(req, |mut client, request| async move {
                client.delete_column_if_exists(request).await
            })
            .await?;
        Ok(resp.into_inner().deleted)
    }

    /// Uploads a merge operator: a jar with the class `class_name`, referenced by columns as `operator_name`.
    ///
    /// Returns the operator's version. The server deduplicates an upload whose content equals that of an existing
    /// version of the same name, and answers with that version.
    async fn upload_merge_operator_once(
        &self,
        operator_name: String,
        class_name: String,
        jar_payload: Vec<u8>,
    ) -> Result<i64> {
        let req = UploadMergeOperatorRequest {
            operator_name,
            class_name,
            jar_payload,
            context: Some(self.context.clone()),
        };
        let resp = self
            .call_contextual(req, |mut client, request| async move {
                client.upload_merge_operator(request).await
            })
            .await?;
        Ok(resp.into_inner().version)
    }

    /// Returns the version of merge operator `operator_name` whose jar has the SHA-256 digest `hash`, or `None` if
    /// no version has that content.
    pub async fn check_merge_operator(&self, operator_name: String, hash: Vec<u8>) -> Result<Option<i64>> {
        let req = CheckMergeOperatorRequest {
            operator_name,
            hash,
            context: Some(self.context.clone()),
        };
        let resp = self
            .call_contextual(req, |mut client, request| async move {
                client.check_merge_operator(request).await
            })
            .await?;
        Ok(resp.into_inner().version)
    }

    /// Returns the version of merge operator `operator_name` with this jar, uploading it first if no version has
    /// the same content (compared by SHA-256, as the Java client's `ensureMergeOperator` does).
    pub async fn ensure_merge_operator(
        &self,
        operator_name: String,
        class_name: String,
        jar_payload: Vec<u8>,
    ) -> Result<i64> {
        let hash = merge_operator_hash(&jar_payload);
        if let Some(version) = self
            .check_merge_operator(operator_name.clone(), hash)
            .await?
        {
            return Ok(version);
        }
        self.upload_merge_operator(operator_name, class_name, jar_payload)
            .await
    }

    /// Retrieves the ID of a column by its name.
    pub async fn get_column_id(&self, name: String) -> Result<i64> {
        let req = GetColumnIdRequest {
            name,
            context: Some(self.context.clone()),
        };
        let resp = self
            .call_contextual(req, |mut client, request| async move {
                client.get_column_id(request).await
            })
            .await?;
        Ok(resp.into_inner().column_id)
    }

    /// Aggregates current SST properties without flushing. Excludes memtables and blob files.
    /// Requires an ANALYTICAL or BATCH context. Counts physical entries, not live logical rows.
    pub async fn get_table_properties(&self, column_id: i64) -> Result<ColumnTablePropertiesResponse> {
        let req = GetTablePropertiesRequest {
            column_id,
            context: Some(self.context.clone()),
        };
        let resp = self.call_contextual(req, |mut client, request| async move {
            client.get_table_properties(request).await
        }).await?;
        Ok(resp.into_inner())
    }

    /// Returns RocksDB's unbounded estimate of physical keys in a column.
    pub async fn estimate_num_keys(&self, column_id: i64) -> Result<i64> {
        let req = EstimateNumKeysRequest {
            column_id,
            context: Some(self.context.clone()),
        };
        let resp = self
            .call_contextual(req, |mut client, request| async move {
                client.estimate_num_keys(request).await
            })
            .await?;
        Ok(resp.into_inner().count)
    }

    /// Retrieves definitions for all existing columns.
    pub async fn get_all_column_definitions(&self) -> Result<Vec<Column>> {
        let req = GetAllColumnDefinitionsRequest {
            context: Some(self.context.clone()),
        };
        let resp = self
            .call_contextual(req, |mut client, request| async move {
                client.get_all_column_definitions(request).await
            })
            .await?;
        Ok(resp
            .into_inner()
            .columns
            .into_iter()
            .map(|c| c.into())
            .collect())
    }

    // ============================================================================================
    // Data Operations - Put
    // ============================================================================================

    /// Puts a value for a specific key.
    async fn put_once(
        &self,
        transaction_or_update_id: i64,
        column_id: i64,
        keys: Vec<Vec<u8>>,
        value: Vec<u8>,
    ) -> Result<()> {
        let req = PutRequest {
            transaction_or_update_id,
            column_id,
            data: Some(Kv { keys, value }),
            context: Some(self.context.clone()),
        };
        self.call_contextual(req, |mut client, request| async move {
            client.put(request).await
        })
        .await?;
        Ok(())
    }

    /// Ensures a value exists, eliding the write only when RockServer proves equality from memory.
    async fn put_ensure_once(
        &self,
        transaction_or_update_id: i64,
        column_id: i64,
        keys: Vec<Vec<u8>>,
        value: Vec<u8>,
    ) -> Result<()> {
        let req = PutRequest {
            transaction_or_update_id,
            column_id,
            data: Some(Kv { keys, value }),
            context: Some(self.context.clone()),
        };
        self.call_contextual(req, |mut client, request| async move {
            client.put_ensure(request).await
        })
        .await?;
        Ok(())
    }

    /// Puts a value and returns the previous value if it existed.
    async fn put_get_previous_once(
        &self,
        transaction_or_update_id: i64,
        column_id: i64,
        keys: Vec<Vec<u8>>,
        value: Vec<u8>,
    ) -> Result<Option<Vec<u8>>> {
        let req = PutRequest {
            transaction_or_update_id,
            column_id,
            data: Some(Kv { keys, value }),
            context: Some(self.context.clone()),
        };
        let resp = self
            .call_contextual(req, |mut client, request| async move {
                client.put_get_previous(request).await
            })
            .await?;
        Ok(resp.into_inner().previous)
    }

    /// Puts a value and returns the delta between previous and new value.
    async fn put_get_delta_once(
        &self,
        transaction_or_update_id: i64,
        column_id: i64,
        keys: Vec<Vec<u8>>,
        value: Vec<u8>,
    ) -> Result<Delta> {
        let req = PutRequest {
            transaction_or_update_id,
            column_id,
            data: Some(Kv { keys, value }),
            context: Some(self.context.clone()),
        };
        let resp = self
            .call_contextual(req, |mut client, request| async move {
                client.put_get_delta(request).await
            })
            .await?;
        Ok(resp.into_inner())
    }

    /// Puts a value and returns whether the value actually changed.
    async fn put_get_changed_once(
        &self,
        transaction_or_update_id: i64,
        column_id: i64,
        keys: Vec<Vec<u8>>,
        value: Vec<u8>,
    ) -> Result<bool> {
        let req = PutRequest {
            transaction_or_update_id,
            column_id,
            data: Some(Kv { keys, value }),
            context: Some(self.context.clone()),
        };
        let resp = self
            .call_contextual(req, |mut client, request| async move {
                client.put_get_changed(request).await
            })
            .await?;
        Ok(resp.into_inner().changed)
    }

    /// Puts a value and returns whether a previous value was present.
    async fn put_get_previous_presence_once(
        &self,
        transaction_or_update_id: i64,
        column_id: i64,
        keys: Vec<Vec<u8>>,
        value: Vec<u8>,
    ) -> Result<bool> {
        let req = PutRequest {
            transaction_or_update_id,
            column_id,
            data: Some(Kv { keys, value }),
            context: Some(self.context.clone()),
        };
        let resp = self
            .call_contextual(req, |mut client, request| async move {
                client.put_get_previous_presence(request).await
            })
            .await?;
        Ok(resp.into_inner().present)
    }

    /// Streams a batch of KV pairs to be put into the database.
    async fn put_batch_once(
        &self,
        column_id: i64,
        mode: PutBatchMode,
        batches: impl Stream<Item = KvBatch> + Send + 'static,
    ) -> Result<()> {
        let initial = PutBatchRequest {
            put_batch_request_type: Some(put_batch_request::PutBatchRequestType::InitialRequest(
                PutBatchInitialRequest {
                    column_id,
                    mode: mode.into(),
                    context: Some(self.context.clone()),
                },
            )),
        };

        let request_stream = async_stream::stream! {
            yield initial;
            for await batch in batches {
                yield PutBatchRequest {
                    put_batch_request_type: Some(put_batch_request::PutBatchRequestType::Data(batch)),
                };
            }
        };

        self.call_contextual(request_stream, |mut client, request| async move {
            client.put_batch(request).await
        })
        .await?;
        Ok(())
    }

    /// Streams a batch of KV pairs to be merged into the database.
    async fn merge_batch_once(
        &self,
        column_id: i64,
        mode: MergeBatchMode,
        batches: impl Stream<Item = KvBatch> + Send + 'static,
    ) -> Result<()> {
        let initial = MergeBatchRequest {
            merge_batch_request_type: Some(
                merge_batch_request::MergeBatchRequestType::InitialRequest(
                    MergeBatchInitialRequest {
                        column_id,
                        mode: mode.into(),
                        context: Some(self.context.clone()),
                    },
                ),
            ),
        };

        let request_stream = async_stream::stream! {
            yield initial;
            for await batch in batches {
                yield MergeBatchRequest {
                    merge_batch_request_type: Some(merge_batch_request::MergeBatchRequestType::Data(batch)),
                };
            }
        };

        self.call_contextual(request_stream, |mut client, request| async move {
            client.merge_batch(request).await
        })
        .await?;
        Ok(())
    }

    /// Puts multiple KV pairs at once (list based).
    async fn put_multi_list_once(
        &self,
        transaction_or_update_id: i64,
        column_id: i64,
        data: Vec<Kv>,
    ) -> Result<()> {
        let req = PutMultiListRequest {
            initial_request: Some(PutMultiInitialRequest {
                transaction_or_update_id,
                column_id,
                context: Some(self.context.clone()),
            }),
            data,
        };
        self.call_contextual(req, |mut client, request| async move {
            client.put_multi_list(request).await
        })
        .await?;
        Ok(())
    }

    /// Streams multiple KV pairs to put.
    async fn put_multi_once(
        &self,
        transaction_or_update_id: i64,
        column_id: i64,
        items: impl Stream<Item = Kv> + Send + 'static,
    ) -> Result<()> {
        let initial = PutMultiRequest {
            put_multi_request_type: Some(put_multi_request::PutMultiRequestType::InitialRequest(
                PutMultiInitialRequest {
                    transaction_or_update_id,
                    column_id,
                    context: Some(self.context.clone()),
                },
            )),
        };

        let request_stream = async_stream::stream! {
            yield initial;
            for await item in items {
                yield PutMultiRequest {
                    put_multi_request_type: Some(put_multi_request::PutMultiRequestType::Data(item)),
                };
            }
        };

        self.call_contextual(request_stream, |mut client, request| async move {
            client.put_multi(request).await
        })
        .await?;
        Ok(())
    }

    /// Streams values to ensure, using RockServer's memory-tier write-elision path when eligible.
    async fn put_multi_ensure_once(
        &self,
        transaction_or_update_id: i64,
        column_id: i64,
        items: impl Stream<Item = Kv> + Send + 'static,
    ) -> Result<()> {
        let initial = PutMultiRequest {
            put_multi_request_type: Some(put_multi_request::PutMultiRequestType::InitialRequest(
                PutMultiInitialRequest {
                    transaction_or_update_id,
                    column_id,
                    context: Some(self.context.clone()),
                },
            )),
        };

        let request_stream = async_stream::stream! {
            yield initial;
            for await item in items {
                yield PutMultiRequest {
                    put_multi_request_type: Some(put_multi_request::PutMultiRequestType::Data(item)),
                };
            }
        };

        self.call_contextual(request_stream, |mut client, request| async move {
            client.put_multi_ensure(request).await
        })
        .await?;
        Ok(())
    }

    /// Streams multiple KV pairs and retrieves previous values.
    async fn put_multi_get_previous_once(
        &self,
        transaction_or_update_id: i64,
        column_id: i64,
        items: impl Stream<Item = Kv> + Send + 'static,
    ) -> Result<impl Stream<Item = Result<Previous>>> {
        let initial = PutMultiRequest {
            put_multi_request_type: Some(put_multi_request::PutMultiRequestType::InitialRequest(
                PutMultiInitialRequest {
                    transaction_or_update_id,
                    column_id,
                    context: Some(self.context.clone()),
                },
            )),
        };

        let request_stream = async_stream::stream! {
            yield initial;
            for await item in items {
                yield PutMultiRequest {
                    put_multi_request_type: Some(put_multi_request::PutMultiRequestType::Data(item)),
                };
            }
        };

        let resp = self
            .call_contextual(request_stream, |mut client, request| async move {
                client.put_multi_get_previous(request).await
            })
            .await?;
        Ok(resp.into_inner())
    }

    /// Streams multiple KV pairs and retrieves deltas.
    async fn put_multi_get_delta_once(
        &self,
        transaction_or_update_id: i64,
        column_id: i64,
        items: impl Stream<Item = Kv> + Send + 'static,
    ) -> Result<impl Stream<Item = Result<Delta>>> {
        let initial = PutMultiRequest {
            put_multi_request_type: Some(put_multi_request::PutMultiRequestType::InitialRequest(
                PutMultiInitialRequest {
                    transaction_or_update_id,
                    column_id,
                    context: Some(self.context.clone()),
                },
            )),
        };

        let request_stream = async_stream::stream! {
            yield initial;
            for await item in items {
                yield PutMultiRequest {
                    put_multi_request_type: Some(put_multi_request::PutMultiRequestType::Data(item)),
                };
            }
        };

        let resp = self
            .call_contextual(request_stream, |mut client, request| async move {
                client.put_multi_get_delta(request).await
            })
            .await?;
        Ok(resp.into_inner())
    }

    /// Streams multiple KV pairs and retrieves change status.
    async fn put_multi_get_changed_once(
        &self,
        transaction_or_update_id: i64,
        column_id: i64,
        items: impl Stream<Item = Kv> + Send + 'static,
    ) -> Result<impl Stream<Item = Result<Changed>>> {
        let initial = PutMultiRequest {
            put_multi_request_type: Some(put_multi_request::PutMultiRequestType::InitialRequest(
                PutMultiInitialRequest {
                    transaction_or_update_id,
                    column_id,
                    context: Some(self.context.clone()),
                },
            )),
        };

        let request_stream = async_stream::stream! {
            yield initial;
            for await item in items {
                yield PutMultiRequest {
                    put_multi_request_type: Some(put_multi_request::PutMultiRequestType::Data(item)),
                };
            }
        };

        let resp = self
            .call_contextual(request_stream, |mut client, request| async move {
                client.put_multi_get_changed(request).await
            })
            .await?;
        Ok(resp.into_inner())
    }

    /// Streams multiple KV pairs and retrieves previous presence status.
    async fn put_multi_get_previous_presence_once(
        &self,
        transaction_or_update_id: i64,
        column_id: i64,
        items: impl Stream<Item = Kv> + Send + 'static,
    ) -> Result<impl Stream<Item = Result<PreviousPresence>>> {
        let initial = PutMultiRequest {
            put_multi_request_type: Some(put_multi_request::PutMultiRequestType::InitialRequest(
                PutMultiInitialRequest {
                    transaction_or_update_id,
                    column_id,
                    context: Some(self.context.clone()),
                },
            )),
        };

        let request_stream = async_stream::stream! {
            yield initial;
            for await item in items {
                yield PutMultiRequest {
                    put_multi_request_type: Some(put_multi_request::PutMultiRequestType::Data(item)),
                };
            }
        };

        let resp = self
            .call_contextual(request_stream, |mut client, request| async move {
                client.put_multi_get_previous_presence(request).await
            })
            .await?;
        Ok(resp.into_inner())
    }

    // ============================================================================================
    // Data Operations - Delete
    // ============================================================================================

    /// Deletes a value for a specific key.
    async fn delete_once(
        &self,
        transaction_or_update_id: i64,
        column_id: i64,
        keys: Vec<Vec<u8>>,
    ) -> Result<()> {
        let req = DeleteRequest {
            transaction_or_update_id,
            column_id,
            keys,
            context: Some(self.context.clone()),
        };
        self.call_contextual(req, |mut client, request| async move {
            client.delete(request).await
        })
        .await?;
        Ok(())
    }

    /// Deletes a value and returns the previous value when present.
    async fn delete_get_previous_once(
        &self,
        transaction_or_update_id: i64,
        column_id: i64,
        keys: Vec<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>> {
        let req = DeleteRequest {
            transaction_or_update_id,
            column_id,
            keys,
            context: Some(self.context.clone()),
        };
        let response = self
            .call_contextual(req, |mut client, request| async move {
                client.delete_get_previous(request).await
            })
            .await?;
        Ok(response.into_inner().previous)
    }

    /// Deletes a value and reports whether it existed.
    async fn delete_get_previous_presence_once(
        &self,
        transaction_or_update_id: i64,
        column_id: i64,
        keys: Vec<Vec<u8>>,
    ) -> Result<bool> {
        let req = DeleteRequest {
            transaction_or_update_id,
            column_id,
            keys,
            context: Some(self.context.clone()),
        };
        let response = self
            .call_contextual(req, |mut client, request| async move {
                client.delete_get_previous_presence(request).await
            })
            .await?;
        Ok(response.into_inner().present)
    }

    /// Deletes the encoded half-open range `[start_keys_inclusive, end_keys_exclusive)`.
    async fn delete_range_once(
        &self,
        column_id: i64,
        start_keys_inclusive: Vec<Vec<u8>>,
        end_keys_exclusive: Vec<Vec<u8>>,
    ) -> Result<()> {
        let req = DeleteRangeRequest {
            column_id,
            start_keys_inclusive,
            end_keys_exclusive,
            context: Some(self.context.clone()),
        };
        self.call_contextual(req, |mut client, request| async move {
            client.delete_range(request).await
        })
        .await?;
        Ok(())
    }

    /// Streams keys to delete.
    async fn delete_multi_once(
        &self,
        transaction_or_update_id: i64,
        column_id: i64,
        keys: impl Stream<Item = Vec<Vec<u8>>> + Send + 'static,
    ) -> Result<()> {
        let requests = self.delete_multi_requests(transaction_or_update_id, column_id, keys);
        self.call_contextual(requests, |mut client, request| async move {
            client.delete_multi(request).await
        })
        .await?;
        Ok(())
    }

    /// Streams keys to delete and returns their previous values.
    async fn delete_multi_get_previous_once(
        &self,
        transaction_or_update_id: i64,
        column_id: i64,
        keys: impl Stream<Item = Vec<Vec<u8>>> + Send + 'static,
    ) -> Result<impl Stream<Item = Result<Previous>>> {
        let requests = self.delete_multi_requests(transaction_or_update_id, column_id, keys);
        let response = self
            .call_contextual(requests, |mut client, request| async move {
                client.delete_multi_get_previous(request).await
            })
            .await?;
        Ok(response.into_inner())
    }

    /// Streams keys to delete and reports whether each value existed.
    async fn delete_multi_get_previous_presence_once(
        &self,
        transaction_or_update_id: i64,
        column_id: i64,
        keys: impl Stream<Item = Vec<Vec<u8>>> + Send + 'static,
    ) -> Result<impl Stream<Item = Result<PreviousPresence>>> {
        let requests = self.delete_multi_requests(transaction_or_update_id, column_id, keys);
        let response = self
            .call_contextual(requests, |mut client, request| async move {
                client.delete_multi_get_previous_presence(request).await
            })
            .await?;
        Ok(response.into_inner())
    }

    fn delete_multi_requests(
        &self,
        transaction_or_update_id: i64,
        column_id: i64,
        keys: impl Stream<Item = Vec<Vec<u8>>> + Send + 'static,
    ) -> impl Stream<Item = DeleteMultiRequest> + Send + 'static {
        let context = Some(self.context.clone());
        async_stream::stream! {
            yield DeleteMultiRequest {
                delete_multi_request_type: Some(delete_multi_request::DeleteMultiRequestType::InitialRequest(
                    DeleteMultiInitialRequest { transaction_or_update_id, column_id, context },
                )),
            };
            for await item_keys in keys {
                yield DeleteMultiRequest {
                    delete_multi_request_type: Some(delete_multi_request::DeleteMultiRequestType::Data(
                        DeleteRequest {
                            transaction_or_update_id: 0,
                            column_id: 0,
                            keys: item_keys,
                            context: None,
                        },
                    )),
                };
            }
        }
    }

    // ============================================================================================
    // Data Operations - Merge
    // ============================================================================================

    /// Merges multiple KV pairs.
    async fn merge_multi_once(
        &self,
        transaction_or_update_id: i64,
        column_id: i64,
        items: impl Stream<Item = Kv> + Send + 'static,
    ) -> Result<()> {
        let initial = MergeMultiRequest {
            merge_multi_request_type: Some(
                merge_multi_request::MergeMultiRequestType::InitialRequest(
                    MergeMultiInitialRequest {
                        transaction_or_update_id,
                        column_id,
                        context: Some(self.context.clone()),
                    },
                ),
            ),
        };

        let request_stream = async_stream::stream! {
            yield initial;
            for await item in items {
                yield MergeMultiRequest {
                    merge_multi_request_type: Some(merge_multi_request::MergeMultiRequestType::Data(item)),
                };
            }
        };

        self.call_contextual(request_stream, |mut client, request| async move {
            client.merge_multi(request).await
        })
        .await?;
        Ok(())
    }

    /// Merges multiple KV pairs and returns the merged result.
    async fn merge_multi_get_merged_once(
        &self,
        transaction_or_update_id: i64,
        column_id: i64,
        items: impl Stream<Item = Kv> + Send + 'static,
    ) -> Result<impl Stream<Item = Result<Merged>>> {
        let initial = MergeMultiRequest {
            merge_multi_request_type: Some(
                merge_multi_request::MergeMultiRequestType::InitialRequest(
                    MergeMultiInitialRequest {
                        transaction_or_update_id,
                        column_id,
                        context: Some(self.context.clone()),
                    },
                ),
            ),
        };

        let request_stream = async_stream::stream! {
            yield initial;
            for await item in items {
                yield MergeMultiRequest {
                    merge_multi_request_type: Some(merge_multi_request::MergeMultiRequestType::Data(item)),
                };
            }
        };

        let resp = self
            .call_contextual(request_stream, |mut client, request| async move {
                client.merge_multi_get_merged(request).await
            })
            .await?;
        Ok(resp.into_inner())
    }

    /// Merges a value for a specific key.
    async fn merge_once(
        &self,
        transaction_or_update_id: i64,
        column_id: i64,
        keys: Vec<Vec<u8>>,
        value: Vec<u8>,
    ) -> Result<()> {
        let req = MergeRequest {
            transaction_or_update_id,
            column_id,
            data: Some(Kv { keys, value }),
            context: Some(self.context.clone()),
        };
        self.call_contextual(req, |mut client, request| async move {
            client.merge(request).await
        })
        .await?;
        Ok(())
    }

    /// Merges a value and returns the result.
    async fn merge_get_merged_once(
        &self,
        transaction_or_update_id: i64,
        column_id: i64,
        keys: Vec<Vec<u8>>,
        value: Vec<u8>,
    ) -> Result<Option<Vec<u8>>> {
        let req = MergeRequest {
            transaction_or_update_id,
            column_id,
            data: Some(Kv { keys, value }),
            context: Some(self.context.clone()),
        };
        let resp = self
            .call_contextual(req, |mut client, request| async move {
                client.merge_get_merged(request).await
            })
            .await?;
        Ok(resp.into_inner().merged)
    }

    // ============================================================================================
    // Data Operations - Get
    // ============================================================================================

    /// Gets a value by key.
    pub async fn get(
        &self,
        transaction_or_update_id: i64,
        column_id: i64,
        keys: Vec<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>> {
        let req = GetRequest {
            transaction_or_update_id,
            column_id,
            keys,
            context: Some(self.context.clone()),
        };
        let resp = self
            .call_contextual(req, |mut client, request| async move {
                client.get(request).await
            })
            .await?;
        Ok(resp.into_inner().value)
    }

    /// Gets a value for update (locking).
    async fn get_for_update_once(
        &self,
        transaction_or_update_id: i64,
        column_id: i64,
        keys: Vec<Vec<u8>>,
    ) -> Result<UpdateBegin> {
        let req = GetRequest {
            transaction_or_update_id,
            column_id,
            keys,
            context: Some(self.context.clone()),
        };
        let resp = self
            .call_contextual(req, |mut client, request| async move {
                client.get_for_update(request).await
            })
            .await?;
        Ok(resp.into_inner())
    }

    /// Checks if a key exists.
    pub async fn exists(
        &self,
        transaction_or_update_id: i64,
        column_id: i64,
        keys: Vec<Vec<u8>>,
    ) -> Result<bool> {
        let req = GetRequest {
            transaction_or_update_id,
            column_id,
            keys,
            context: Some(self.context.clone()),
        };
        let resp = self
            .call_contextual(req, |mut client, request| async move {
                client.exists(request).await
            })
            .await?;
        Ok(resp.into_inner().present)
    }

    /// Checks several logical keys for presence in one bounded request.
    pub async fn exists_multi(
        &self,
        transaction_id: i64,
        column_id: i64,
        keys_multi: Vec<Vec<Vec<u8>>>,
    ) -> Result<Vec<bool>> {
        let req = ExistsMultiRequest {
            transaction_id,
            column_id,
            keys_multi: keys_multi
                .into_iter()
                .map(|keys| KeyTuple { keys })
                .collect(),
            context: Some(self.context.clone()),
        };
        let resp = self
            .call_contextual(req, |mut client, request| async move {
                client.exists_multi(request).await
            })
            .await?;
        Ok(resp.into_inner().present)
    }

    // ============================================================================================
    // Iterators
    // ============================================================================================

    /// Opens an iterator for scanning keys.
    pub async fn open_iterator(
        &self,
        transaction_id: i64,
        column_id: i64,
        start_keys_inclusive: Vec<Vec<u8>>,
        end_keys_exclusive: Vec<Vec<u8>>,
        reverse: bool,
        iterator_lease_ttl: Duration,
    ) -> Result<i64> {
        let req = OpenIteratorRequest {
            transaction_id,
            column_id,
            start_keys_inclusive,
            end_keys_exclusive,
            reverse,
            iterator_lease_ttl_nanos: duration_nanos(iterator_lease_ttl)?,
            context: Some(self.context.clone()),
        };
        let resp = self
            .call_contextual(req, |mut client, request| async move {
                client.open_iterator(request).await
            })
            .await?;
        Ok(resp.into_inner().iterator_id)
    }

    /// Closes an active iterator.
    pub async fn close_iterator(&self, iterator_id: i64) -> Result<()> {
        let req = CloseIteratorRequest {
            iterator_id,
            workload_contract_version: REQUIRED_WORKLOAD_CONTRACT_VERSION,
        };
        self.client.clone().close_iterator(req).await?;
        Ok(())
    }

    /// Seeks the iterator to a specific key.
    pub async fn seek_to(&self, iteration_id: i64, keys: Vec<Vec<u8>>) -> Result<()> {
        let req = SeekToRequest {
            iteration_id,
            keys,
            context: Some(self.context.clone()),
        };
        self.call_contextual(req, |mut client, request| async move {
            client.seek_to(request).await
        })
        .await?;
        Ok(())
    }

    /// Advances the iterator.
    pub async fn subsequent(
        &self,
        iteration_id: i64,
        skip_count: i64,
        take_count: i64,
    ) -> Result<()> {
        let req = SubsequentRequest {
            iteration_id,
            skip_count,
            take_count,
            context: Some(self.context.clone()),
        };
        self.call_contextual(req, |mut client, request| async move {
            client.subsequent(request).await
        })
        .await?;
        Ok(())
    }

    /// Advances the iterator and checks existence.
    pub async fn subsequent_exists(
        &self,
        iteration_id: i64,
        skip_count: i64,
        take_count: i64,
    ) -> Result<bool> {
        let req = SubsequentRequest {
            iteration_id,
            skip_count,
            take_count,
            context: Some(self.context.clone()),
        };
        let resp = self
            .call_contextual(req, |mut client, request| async move {
                client.subsequent_exists(request).await
            })
            .await?;
        Ok(resp.into_inner().present)
    }

    /// Advances the iterator and retrieves values.
    pub async fn subsequent_multi_get(
        &self,
        iteration_id: i64,
        skip_count: i64,
        take_count: i64,
    ) -> Result<impl Stream<Item = Result<Kv>>> {
        let req = SubsequentRequest {
            iteration_id,
            skip_count,
            take_count,
            context: Some(self.context.clone()),
        };
        let resp = self
            .call_contextual(req, |mut client, request| async move {
                client.subsequent_multi_get(request).await
            })
            .await?;
        Ok(resp.into_inner())
    }

    // ============================================================================================
    // Range Operations
    // ============================================================================================

    /// Reduces a range to get the first and last keys.
    pub async fn reduce_range_first_and_last(
        &self,
        transaction_id: i64,
        column_id: i64,
        start_keys_inclusive: Vec<Vec<u8>>,
        end_keys_exclusive: Vec<Vec<u8>>,
        reverse: bool,
    ) -> Result<FirstAndLast> {
        let req = GetRangeRequest {
            transaction_id,
            column_id,
            start_keys_inclusive,
            end_keys_exclusive,
            reverse,
            context: Some(self.context.clone()),
        };
        let resp = self
            .call_contextual(req, |mut client, request| async move {
                client.reduce_range_first_and_last(request).await
            })
            .await?;
        Ok(resp.into_inner())
    }

    /// Counts entries in a range.
    pub async fn reduce_range_entries_count(
        &self,
        transaction_id: i64,
        column_id: i64,
        start_keys_inclusive: Vec<Vec<u8>>,
        end_keys_exclusive: Vec<Vec<u8>>,
        reverse: bool,
    ) -> Result<i64> {
        let req = GetRangeRequest {
            transaction_id,
            column_id,
            start_keys_inclusive,
            end_keys_exclusive,
            reverse,
            context: Some(self.context.clone()),
        };
        let resp = self
            .call_contextual(req, |mut client, request| async move {
                client.reduce_range_entries_count(request).await
            })
            .await?;
        Ok(resp.into_inner().count)
    }

    /// Retrieves all KV pairs in a range.
    pub async fn get_all_in_range(
        &self,
        transaction_id: i64,
        column_id: i64,
        start_keys_inclusive: Vec<Vec<u8>>,
        end_keys_exclusive: Vec<Vec<u8>>,
        reverse: bool,
    ) -> Result<impl Stream<Item = Result<Kv>>> {
        let req = GetRangeRequest {
            transaction_id,
            column_id,
            start_keys_inclusive,
            end_keys_exclusive,
            reverse,
            context: Some(self.context.clone()),
        };
        let resp = self
            .call_contextual(req, |mut client, request| async move {
                client.get_all_in_range(request).await
            })
            .await?;
        Ok(resp.into_inner())
    }

    /// Retrieves one bounded page. `resume_after` is exclusive in both directions;
    /// callers repeat the original bounds on every continuation.
    pub async fn get_range_page(
        &self,
        transaction_id: i64,
        column_id: i64,
        start_keys_inclusive: Vec<Vec<u8>>,
        end_keys_exclusive: Vec<Vec<u8>>,
        reverse: bool,
        resume_after: Option<Vec<Vec<u8>>>,
        request_type: RangeRequestType,
        budget: RangeBudget,
    ) -> Result<RangePage> {
        match request_type as i32 {
            1 | 2 => {}
            _ => return Err(Status::invalid_argument("range request type is required")),
        }
        if budget.max_items <= 0 {
            return Err(Status::invalid_argument("range max_items must be positive"));
        }
        if budget.max_bytes <= 0 {
            return Err(Status::invalid_argument("range max_bytes must be positive"));
        }
        let req = GetRangePageRequest {
            transaction_id,
            column_id,
            start_keys_inclusive,
            end_keys_exclusive,
            reverse,
            resume_after: resume_after.map(|keys| RangeKey { keys }),
            request_type: request_type as i32,
            budget: Some(budget),
            context: Some(self.context.clone()),
        };
        let response = self
            .call_contextual(req, |mut client, request| async move {
                client.get_range_page(request).await
            })
            .await?;
        Ok(response.into_inner())
    }

    /// Scans the column raw (ignoring transactions, usually).
    pub async fn scan_raw(
        &self,
        column_id: i64,
        shard_index: i32,
        shard_count: i32,
    ) -> Result<impl Stream<Item = Result<KvBatch>>> {
        let req = ScanRawRequest {
            column_id,
            shard_index,
            shard_count,
            context: Some(self.context.clone()),
            completed_sst_tokens: Vec::new(),
            resumable: false,
            accepts_tagged_sst_tokens: false,
        };
        let resp = self
            .call_contextual(req, |mut client, request| async move {
                client.scan_raw(request).await
            })
            .await?;
        Ok(resp.into_inner().map(|res| match res {
            Ok(response) => match response.event {
                Some(scan_raw_response::Event::Serialized(serialized))
                    if response.completed_sst_token_after_batch.is_none() =>
                {
                    decode_kv_batch(&serialized)
                }
                _ => Err(Status::internal(
                    "Rockserver returned a resumable event to a legacy raw scan",
                )),
            },
            Err(e) => Err(e),
        }))
    }

    /// Scans immutable SSTs with durable completion tokens. A caller may pass
    /// tokens checkpointed by an earlier attempt; live matching SSTs are skipped.
    pub async fn scan_raw_resumable(
        &self,
        column_id: i64,
        shard_index: i32,
        shard_count: i32,
        completed_ssts: impl IntoIterator<Item = RawSstToken>,
    ) -> Result<impl Stream<Item = Result<RawScanEvent>>> {
        let req = ScanRawRequest {
            column_id,
            shard_index,
            shard_count,
            context: Some(self.context.clone()),
            completed_sst_tokens: completed_ssts
                .into_iter()
                .map(RawSstToken::into_string)
                .collect(),
            resumable: true,
            accepts_tagged_sst_tokens: true,
        };
        let resp = self
            .call_contextual(req, |mut client, request| async move {
                client.scan_raw(request).await
            })
            .await?;
        Ok(resp
            .into_inner()
            .map(|result| result.and_then(decode_raw_scan_event)))
    }

    // ============================================================================================
    // Maintenance
    // ============================================================================================

    /// Flushes the database.
    pub async fn flush(&self) -> Result<()> {
        let req = FlushRequest {
            workload_contract_version: REQUIRED_WORKLOAD_CONTRACT_VERSION,
            wal_only: false,
        };
        self.client.clone().flush(req).await?;
        Ok(())
    }

    /// Syncs the WAL to stable storage without forcing memtables to SST files.
    /// Older servers safely fall back to a full database flush.
    pub async fn flush_wal(&self) -> Result<()> {
        let req = FlushRequest {
            workload_contract_version: REQUIRED_WORKLOAD_CONTRACT_VERSION,
            wal_only: true,
        };
        self.client.clone().flush(req).await?;
        Ok(())
    }

    /// Compacts the database.
    pub async fn compact(&self) -> Result<()> {
        let req = CompactRequest {
            workload_contract_version: REQUIRED_WORKLOAD_CONTRACT_VERSION,
        };
        self.client.clone().compact(req).await?;
        Ok(())
    }
}

/// A closure sending one copy of a numbered write through the view it is given: clones `$name`s for each copy.
macro_rules! copy {
    ($view:ident => $call:expr $(; $($name:ident),*)?) => {
        move |$view: RockserverClient| {
            $($(let $name = $name.clone();)*)?
            async move { $call.await }
        }
    };
}

async fn collected<T, S>(stream: impl Future<Output = Result<S>>) -> Result<Vec<T>>
where
    S: Stream<Item = Result<T>>,
{
    stream.await?.try_collect().await
}

/// The deadline of one chunk of a numbered batch sent now: the call's, for a caller with a deadline; otherwise the
/// retry window from now (the window of a call without a deadline is per chunk, not per call).
fn chunk_deadline(
    call_deadline: tokio::time::Instant,
    finite: bool,
    window: Duration,
) -> tokio::time::Instant {
    if finite {
        return call_deadline;
    }
    let now = tokio::time::Instant::now();
    now.checked_add(window)
        .unwrap_or_else(|| now + Duration::from_secs(10 * 365 * 86_400))
}

fn replayed<T: Send + 'static>(values: Vec<T>) -> BoxStream<'static, Result<T>> {
    futures::stream::iter(values.into_iter().map(Ok)).boxed()
}

// ================================================================================================
// Client sessions: numbered writes (see `crate::sessions`)
// ================================================================================================

impl RockserverClient {
    /// True if this view numbers its writes through a client session: the server advertised `client_sessions`
    /// at connection and the view was not made with `without_client_sessions`.
    ///
    /// Numbered: writes outside transactions (put, delete, merge and their multi and result variants), the write
    /// that ends an update opened through this client (`get_for_update` outside a transaction), transaction
    /// commits, schema changes (`create_column`, `delete_column`, `delete_column_if_exists`,
    /// `upload_merge_operator`), `delete_range`, and `put_batch`/`merge_batch` chunk by chunk when the server also
    /// advertises `numbered_batches`. A numbered write whose outcome is unknown (the call was lost, or the server
    /// answered `OUTCOME_UNKNOWN`) is sent again with the same number until the context's deadline, or the session
    /// retry window for a context without one; the server applies it at most once. A write staged in a
    /// transaction is not numbered. A view whose context already carries a request id
    /// (`RequestContext::with_request_id`) is numbered by its caller and sends its writes once.
    ///
    /// A numbered multi write reads its whole input stream before sending it, and a numbered multi write with
    /// results reads every result before returning them, since a copy may have to be sent again.
    pub fn client_sessions(&self) -> bool {
        self.sessions.is_some()
    }

    /// How many copies of numbered writes this client (all its views) sent again after an unknown outcome: a lost
    /// call, `OUTCOME_UNKNOWN`, or a deadline error not flagged "not applied". 0 without client sessions.
    pub fn numbered_resends(&self) -> u64 {
        self.sessions.as_ref().map_or(0, |writes| writes.resends())
    }

    /// A view of this client that sends every write once, unnumbered, as to a server without client sessions.
    pub fn without_client_sessions(&self) -> Self {
        Self {
            sessions: None,
            ..self.clone()
        }
    }

    /// A view of this client that resends a numbered write of a context without a deadline for at most `window`
    /// (30 seconds by default).
    pub fn with_session_retry_window(&self, window: Duration) -> Self {
        Self {
            session_retry_window: window,
            ..self.clone()
        }
    }

    /// The session writes that number a write with this transaction or update id, if this view numbers it.
    fn numbering(&self, transaction_or_update_id: i64) -> Option<Arc<SessionWrites>> {
        if self.context.request_id.is_some() {
            return None;
        }
        self.sessions
            .as_ref()
            .filter(|writes| writes.numbers(transaction_or_update_id))
            .cloned()
    }

    /// The deadline of a numbered write sent now, and whether the caller has one.
    fn numbered_deadline(&self) -> Result<(tokio::time::Instant, bool)> {
        let timeout = self.validated_timeout()?;
        let now = tokio::time::Instant::now();
        let window = timeout.unwrap_or(self.session_retry_window);
        let deadline = now
            .checked_add(window)
            .unwrap_or_else(|| now + Duration::from_secs(10 * 365 * 86_400));
        Ok((deadline, timeout.is_some()))
    }

    /// The view one copy is sent through: this context with the request id and what is left of the deadline.
    fn copy_view(&self, id: Option<ClientRequestId>, timeout: Option<Duration>) -> Self {
        let mut context = self.context.clone();
        context.request_id = id;
        if let Some(timeout) = timeout {
            context.timeout_nanos = timeout.as_nanos().clamp(1, (i64::MAX - 1) as u128) as i64;
        }
        Self {
            client: self.client.clone(),
            context,
            sessions: None,
            session_retry_window: self.session_retry_window,
        }
    }

    async fn numbered_until<R, S, SF>(
        &self,
        writes: &SessionWrites,
        deadline: tokio::time::Instant,
        finite: bool,
        mut send: S,
    ) -> Result<R>
    where
        S: FnMut(RockserverClient) -> SF,
        SF: Future<Output = Result<R>>,
    {
        writes
            .run(
                deadline,
                finite,
                |timeout| {
                    let view = self.copy_view(None, timeout);
                    async move { view.open_client_session().await }
                },
                |id, timeout| send(self.copy_view(Some(id), timeout)),
            )
            .await
    }

    /// Sends a numbered write; an update it ends is forgotten once it has its answer.
    async fn numbered<R, S, SF>(&self, writes: &SessionWrites, update_id: i64, send: S) -> Result<R>
    where
        S: FnMut(RockserverClient) -> SF,
        SF: Future<Output = Result<R>>,
    {
        let started = self.numbered_deadline()?;
        self.numbered_from(writes, update_id, started, send).await
    }

    /// [`Self::numbered`] with the deadline `started` taken earlier, by [`Self::read_numbered_multi`].
    async fn numbered_from<R, S, SF>(
        &self,
        writes: &SessionWrites,
        update_id: i64,
        (deadline, finite): (tokio::time::Instant, bool),
        send: S,
    ) -> Result<R>
    where
        S: FnMut(RockserverClient) -> SF,
        SF: Future<Output = Result<R>>,
    {
        let result = self.numbered_until(writes, deadline, finite, send).await;
        writes.update_ended(update_id);
        result
    }

    /// Reads the input of a numbered multi write ([`read_multi_items`]), with its deadline taken first: the
    /// caller's deadline counts from the call, as when the stream is sent as it is read, not from the end of its
    /// input. A caller with a deadline that its input outlasts gets `READ_DEADLINE_EXCEEDED` flagged not applied
    /// (nothing was sent), and the rest of the input is not read. Returns the deadline with the items.
    async fn read_numbered_multi<T: Send + 'static>(
        &self,
        items: impl Stream<Item = T> + Send + 'static,
        size: fn(&T) -> u64,
    ) -> Result<(MultiItems<T>, (tokio::time::Instant, bool))> {
        let (deadline, finite) = self.numbered_deadline()?;
        let read = read_multi_items(items, size, NUMBERED_MULTI_MAX_BYTES);
        let items = if finite {
            tokio::time::timeout_at(deadline, read)
                .await
                .map_err(|_| deadline_before_sent())?
        } else {
            read.await
        };
        // A caller without a deadline reads its input without a bound; its retry window starts when the write is
        // sent, as for a batch chunk and in the Java client, not when the call began.
        let deadline = chunk_deadline(deadline, finite, self.session_retry_window);
        Ok((items, (deadline, finite)))
    }

    /// The view an unnumbered multi write over the numbering limit is sent through, after its first items were
    /// read: this one with what is left of the deadline `started`.
    fn streamed_view(&self, (deadline, finite): (tokio::time::Instant, bool)) -> Self {
        let left = finite.then(|| deadline.saturating_duration_since(tokio::time::Instant::now()));
        self.copy_view(None, left)
    }

    /// A batch write as numbered chunks, one at a time, in order. A caller with a deadline sends every chunk within
    /// it; a caller without one gives each chunk its own retry window from the moment it is sent, as the Java client
    /// does, so that a long batch can still resend a late chunk. Once a chunk was applied, a failure leaves the call
    /// partly applied: `INVALID_ARGUMENT` (`INVALID_REQUEST`), not to be resent as a whole.
    async fn numbered_batch<S, SF>(
        &self,
        writes: &SessionWrites,
        merge: bool,
        column_id: i64,
        batches: impl Stream<Item = KvBatch>,
        mut send: S,
    ) -> Result<()>
    where
        S: FnMut(RockserverClient, KvBatch) -> SF,
        SF: Future<Output = Result<()>>,
    {
        let (call_deadline, finite) = self.numbered_deadline()?;
        let mut applied_chunks = 0u64;
        let mut applied_keys = 0u64;
        futures::pin_mut!(batches);
        while let Some(batch) = batches.next().await {
            for chunk in split_batch(batch) {
                let keys = chunk.entries.len() as u64;
                let deadline = chunk_deadline(call_deadline, finite, self.session_retry_window);
                let result = self
                    .numbered_until(writes, deadline, finite, |view| send(view, chunk.clone()))
                    .await;
                if let Err(error) = result {
                    return Err(if applied_chunks == 0 {
                        error
                    } else {
                        partly_applied(merge, column_id, applied_chunks, applied_keys, &error)
                    });
                }
                applied_chunks += 1;
                applied_keys += keys;
            }
        }
        Ok(())
    }

    // ---- Transactions and updates ----

    /// Closes an existing transaction: commits it if `commit`, else rolls it back. Returns `true` if successful. A
    /// commit is numbered (see `client_sessions`).
    pub async fn close_transaction(&self, transaction_id: i64, commit: bool) -> Result<bool> {
        match self.numbering(0).filter(|_| commit) {
            Some(writes) => {
                self.numbered(&writes, 0, copy!(view => view.close_transaction_once(transaction_id, true)))
                    .await
            }
            None => self.close_transaction_once(transaction_id, commit).await,
        }
    }

    /// Closes a failed update explicitly.
    pub async fn close_failed_update(&self, update_id: i64) -> Result<()> {
        if let Some(writes) = &self.sessions {
            writes.update_ended(update_id);
        }
        self.close_failed_update_once(update_id).await
    }

    /// Gets a value for update (locking). Outside a transaction, the write that ends the update is numbered.
    pub async fn get_for_update(
        &self,
        transaction_or_update_id: i64,
        column_id: i64,
        keys: Vec<Vec<u8>>,
    ) -> Result<UpdateBegin> {
        let begin = self
            .get_for_update_once(transaction_or_update_id, column_id, keys)
            .await?;
        if let (Some(writes), 0, Some(update_id)) =
            (&self.sessions, transaction_or_update_id, begin.update_id)
        {
            writes.update_opened(update_id);
        }
        Ok(begin)
    }

    // ---- Schema ----

    /// Creates a new column with the given name and schema. Numbered.
    pub async fn create_column(&self, name: String, schema: ColumnSchema) -> Result<i64> {
        match self.numbering(0) {
            Some(writes) => {
                self.numbered(&writes, 0, copy!(view => view.create_column_once(name, schema); name, schema))
                    .await
            }
            None => self.create_column_once(name, schema).await,
        }
    }

    /// Deletes a column by its ID. Numbered.
    pub async fn delete_column(&self, column_id: i64) -> Result<()> {
        match self.numbering(0) {
            Some(writes) => {
                self.numbered(&writes, 0, copy!(view => view.delete_column_once(column_id)))
                    .await
            }
            None => self.delete_column_once(column_id).await,
        }
    }

    /// Deletes a column by name if it exists: `true` when a column was deleted, `false` when it was already absent.
    /// Numbered: a copy answers as the first did.
    pub async fn delete_column_if_exists(&self, name: String) -> Result<bool> {
        match self.numbering(0) {
            Some(writes) => {
                self.numbered(&writes, 0, copy!(view => view.delete_column_if_exists_once(name); name))
                    .await
            }
            None => self.delete_column_if_exists_once(name).await,
        }
    }

    /// Uploads a merge operator: a jar with the class `class_name`, referenced by columns as `operator_name`.
    /// Returns the operator's version; an upload whose content equals an existing version of the same name answers
    /// with that version. Numbered.
    pub async fn upload_merge_operator(
        &self,
        operator_name: String,
        class_name: String,
        jar_payload: Vec<u8>,
    ) -> Result<i64> {
        match self.numbering(0) {
            Some(writes) => {
                self.numbered(
                    &writes,
                    0,
                    copy!(view => view.upload_merge_operator_once(operator_name, class_name, jar_payload);
                        operator_name, class_name, jar_payload),
                )
                .await
            }
            None => {
                self.upload_merge_operator_once(operator_name, class_name, jar_payload)
                    .await
            }
        }
    }

    // ---- Single writes ----

    /// Puts a value for a specific key.
    pub async fn put(&self, transaction_or_update_id: i64, column_id: i64, keys: Vec<Vec<u8>>, value: Vec<u8>) -> Result<()> {
        let tx = transaction_or_update_id;
        match self.numbering(tx) {
            Some(writes) => self.numbered(&writes, tx, copy!(view => view.put_once(tx, column_id, keys, value); keys, value)).await,
            None => self.put_once(tx, column_id, keys, value).await,
        }
    }

    /// Ensures a value exists, eliding the write only when RockServer proves equality from memory.
    pub async fn put_ensure(&self, transaction_or_update_id: i64, column_id: i64, keys: Vec<Vec<u8>>, value: Vec<u8>) -> Result<()> {
        let tx = transaction_or_update_id;
        match self.numbering(tx) {
            Some(writes) => self.numbered(&writes, tx, copy!(view => view.put_ensure_once(tx, column_id, keys, value); keys, value)).await,
            None => self.put_ensure_once(tx, column_id, keys, value).await,
        }
    }

    /// Puts a value and returns the previous value if it existed.
    pub async fn put_get_previous(&self, transaction_or_update_id: i64, column_id: i64, keys: Vec<Vec<u8>>, value: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let tx = transaction_or_update_id;
        match self.numbering(tx) {
            Some(writes) => self.numbered(&writes, tx, copy!(view => view.put_get_previous_once(tx, column_id, keys, value); keys, value)).await,
            None => self.put_get_previous_once(tx, column_id, keys, value).await,
        }
    }

    /// Puts a value and returns the delta between previous and new value.
    pub async fn put_get_delta(&self, transaction_or_update_id: i64, column_id: i64, keys: Vec<Vec<u8>>, value: Vec<u8>) -> Result<Delta> {
        let tx = transaction_or_update_id;
        match self.numbering(tx) {
            Some(writes) => self.numbered(&writes, tx, copy!(view => view.put_get_delta_once(tx, column_id, keys, value); keys, value)).await,
            None => self.put_get_delta_once(tx, column_id, keys, value).await,
        }
    }

    /// Puts a value and returns whether the value actually changed.
    pub async fn put_get_changed(&self, transaction_or_update_id: i64, column_id: i64, keys: Vec<Vec<u8>>, value: Vec<u8>) -> Result<bool> {
        let tx = transaction_or_update_id;
        match self.numbering(tx) {
            Some(writes) => self.numbered(&writes, tx, copy!(view => view.put_get_changed_once(tx, column_id, keys, value); keys, value)).await,
            None => self.put_get_changed_once(tx, column_id, keys, value).await,
        }
    }

    /// Puts a value and returns whether a previous value was present.
    pub async fn put_get_previous_presence(&self, transaction_or_update_id: i64, column_id: i64, keys: Vec<Vec<u8>>, value: Vec<u8>) -> Result<bool> {
        let tx = transaction_or_update_id;
        match self.numbering(tx) {
            Some(writes) => self.numbered(&writes, tx, copy!(view => view.put_get_previous_presence_once(tx, column_id, keys, value); keys, value)).await,
            None => self.put_get_previous_presence_once(tx, column_id, keys, value).await,
        }
    }

    /// Deletes a value for a specific key.
    pub async fn delete(&self, transaction_or_update_id: i64, column_id: i64, keys: Vec<Vec<u8>>) -> Result<()> {
        let tx = transaction_or_update_id;
        match self.numbering(tx) {
            Some(writes) => self.numbered(&writes, tx, copy!(view => view.delete_once(tx, column_id, keys); keys)).await,
            None => self.delete_once(tx, column_id, keys).await,
        }
    }

    /// Deletes a value and returns the previous value when present.
    pub async fn delete_get_previous(&self, transaction_or_update_id: i64, column_id: i64, keys: Vec<Vec<u8>>) -> Result<Option<Vec<u8>>> {
        let tx = transaction_or_update_id;
        match self.numbering(tx) {
            Some(writes) => self.numbered(&writes, tx, copy!(view => view.delete_get_previous_once(tx, column_id, keys); keys)).await,
            None => self.delete_get_previous_once(tx, column_id, keys).await,
        }
    }

    /// Deletes a value and reports whether it existed.
    pub async fn delete_get_previous_presence(&self, transaction_or_update_id: i64, column_id: i64, keys: Vec<Vec<u8>>) -> Result<bool> {
        let tx = transaction_or_update_id;
        match self.numbering(tx) {
            Some(writes) => self.numbered(&writes, tx, copy!(view => view.delete_get_previous_presence_once(tx, column_id, keys); keys)).await,
            None => self.delete_get_previous_presence_once(tx, column_id, keys).await,
        }
    }

    /// Deletes the encoded half-open range `[start_keys_inclusive, end_keys_exclusive)`. Numbered: a copy sent after
    /// later writes into the range does not delete them.
    pub async fn delete_range(&self, column_id: i64, start_keys_inclusive: Vec<Vec<u8>>, end_keys_exclusive: Vec<Vec<u8>>) -> Result<()> {
        match self.numbering(0) {
            Some(writes) => self.numbered(&writes, 0, copy!(view => view.delete_range_once(column_id, start_keys_inclusive, end_keys_exclusive); start_keys_inclusive, end_keys_exclusive)).await,
            None => self.delete_range_once(column_id, start_keys_inclusive, end_keys_exclusive).await,
        }
    }

    /// Merges a value for a specific key.
    pub async fn merge(&self, transaction_or_update_id: i64, column_id: i64, keys: Vec<Vec<u8>>, value: Vec<u8>) -> Result<()> {
        let tx = transaction_or_update_id;
        match self.numbering(tx) {
            Some(writes) => self.numbered(&writes, tx, copy!(view => view.merge_once(tx, column_id, keys, value); keys, value)).await,
            None => self.merge_once(tx, column_id, keys, value).await,
        }
    }

    /// Merges a value and returns the result.
    pub async fn merge_get_merged(&self, transaction_or_update_id: i64, column_id: i64, keys: Vec<Vec<u8>>, value: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let tx = transaction_or_update_id;
        match self.numbering(tx) {
            Some(writes) => self.numbered(&writes, tx, copy!(view => view.merge_get_merged_once(tx, column_id, keys, value); keys, value)).await,
            None => self.merge_get_merged_once(tx, column_id, keys, value).await,
        }
    }

    // ---- Multi writes ----

    /// Puts multiple KV pairs at once (list based).
    pub async fn put_multi_list(&self, transaction_or_update_id: i64, column_id: i64, data: Vec<Kv>) -> Result<()> {
        let tx = transaction_or_update_id;
        match self.numbering(tx) {
            Some(writes) => self.numbered(&writes, tx, copy!(view => view.put_multi_list_once(tx, column_id, data); data)).await,
            None => self.put_multi_list_once(tx, column_id, data).await,
        }
    }

    /// Streams multiple KV pairs to put. Numbered when the stream counts at most `sessions::NUMBERED_MULTI_MAX_BYTES`, read whole first; a larger
    /// stream is sent unnumbered, streamed.
    pub async fn put_multi(&self, transaction_or_update_id: i64, column_id: i64, items: impl Stream<Item = Kv> + Send + 'static) -> Result<()> {
        let tx = transaction_or_update_id;
        match self.numbering(tx) {
            Some(writes) => match self.read_numbered_multi(items, multi_kv_bytes).await? {
                (MultiItems::Whole(items), started) => self.numbered_from(&writes, tx, started, copy!(view => view.put_multi_once(tx, column_id, futures::stream::iter(items)); items)).await,
                (MultiItems::Streamed(items), started) => {
                    let result = self.streamed_view(started).put_multi_once(tx, column_id, items).await;
                    writes.update_ended(tx);
                    result
                }
            },
            None => self.put_multi_once(tx, column_id, items).await,
        }
    }

    /// Streams values to ensure, using RockServer's memory-tier write-elision path when eligible. Numbered, the
    /// stream is read whole first.
    pub async fn put_multi_ensure(&self, transaction_or_update_id: i64, column_id: i64, items: impl Stream<Item = Kv> + Send + 'static) -> Result<()> {
        let tx = transaction_or_update_id;
        match self.numbering(tx) {
            Some(writes) => match self.read_numbered_multi(items, multi_kv_bytes).await? {
                (MultiItems::Whole(items), started) => self.numbered_from(&writes, tx, started, copy!(view => view.put_multi_ensure_once(tx, column_id, futures::stream::iter(items)); items)).await,
                (MultiItems::Streamed(items), started) => {
                    let result = self.streamed_view(started).put_multi_ensure_once(tx, column_id, items).await;
                    writes.update_ended(tx);
                    result
                }
            },
            None => self.put_multi_ensure_once(tx, column_id, items).await,
        }
    }

    /// Streams multiple KV pairs and retrieves previous values. Numbered when the stream counts at most `sessions::NUMBERED_MULTI_MAX_BYTES` (both streams are then read
    /// whole); a larger stream is sent unnumbered, streamed.
    pub async fn put_multi_get_previous(&self, transaction_or_update_id: i64, column_id: i64, items: impl Stream<Item = Kv> + Send + 'static) -> Result<impl Stream<Item = Result<Previous>>> {
        let tx = transaction_or_update_id;
        Ok(match self.numbering(tx) {
            Some(writes) => match self.read_numbered_multi(items, multi_kv_bytes).await? {
                (MultiItems::Whole(items), started) => replayed(self.numbered_from(&writes, tx, started, copy!(view => collected(view.put_multi_get_previous_once(tx, column_id, futures::stream::iter(items))); items)).await?),
                (MultiItems::Streamed(items), started) => {
                    let results = self.streamed_view(started).put_multi_get_previous_once(tx, column_id, items).await;
                    writes.update_ended(tx);
                    results?.boxed()
                }
            },
            None => self.put_multi_get_previous_once(tx, column_id, items).await?.boxed(),
        })
    }

    /// Streams multiple KV pairs and retrieves deltas. Numbered when the stream counts at most `sessions::NUMBERED_MULTI_MAX_BYTES` (both streams are then read
    /// whole); a larger stream is sent unnumbered, streamed.
    pub async fn put_multi_get_delta(&self, transaction_or_update_id: i64, column_id: i64, items: impl Stream<Item = Kv> + Send + 'static) -> Result<impl Stream<Item = Result<Delta>>> {
        let tx = transaction_or_update_id;
        Ok(match self.numbering(tx) {
            Some(writes) => match self.read_numbered_multi(items, multi_kv_bytes).await? {
                (MultiItems::Whole(items), started) => replayed(self.numbered_from(&writes, tx, started, copy!(view => collected(view.put_multi_get_delta_once(tx, column_id, futures::stream::iter(items))); items)).await?),
                (MultiItems::Streamed(items), started) => {
                    let results = self.streamed_view(started).put_multi_get_delta_once(tx, column_id, items).await;
                    writes.update_ended(tx);
                    results?.boxed()
                }
            },
            None => self.put_multi_get_delta_once(tx, column_id, items).await?.boxed(),
        })
    }

    /// Streams multiple KV pairs and retrieves change status. Numbered when the stream counts at most `sessions::NUMBERED_MULTI_MAX_BYTES` (both streams are then read
    /// whole); a larger stream is sent unnumbered, streamed.
    pub async fn put_multi_get_changed(&self, transaction_or_update_id: i64, column_id: i64, items: impl Stream<Item = Kv> + Send + 'static) -> Result<impl Stream<Item = Result<Changed>>> {
        let tx = transaction_or_update_id;
        Ok(match self.numbering(tx) {
            Some(writes) => match self.read_numbered_multi(items, multi_kv_bytes).await? {
                (MultiItems::Whole(items), started) => replayed(self.numbered_from(&writes, tx, started, copy!(view => collected(view.put_multi_get_changed_once(tx, column_id, futures::stream::iter(items))); items)).await?),
                (MultiItems::Streamed(items), started) => {
                    let results = self.streamed_view(started).put_multi_get_changed_once(tx, column_id, items).await;
                    writes.update_ended(tx);
                    results?.boxed()
                }
            },
            None => self.put_multi_get_changed_once(tx, column_id, items).await?.boxed(),
        })
    }

    /// Streams multiple KV pairs and retrieves previous presence status. Numbered when the stream counts at most `sessions::NUMBERED_MULTI_MAX_BYTES` (both streams are then read
    /// whole); a larger stream is sent unnumbered, streamed.
    pub async fn put_multi_get_previous_presence(&self, transaction_or_update_id: i64, column_id: i64, items: impl Stream<Item = Kv> + Send + 'static) -> Result<impl Stream<Item = Result<PreviousPresence>>> {
        let tx = transaction_or_update_id;
        Ok(match self.numbering(tx) {
            Some(writes) => match self.read_numbered_multi(items, multi_kv_bytes).await? {
                (MultiItems::Whole(items), started) => replayed(self.numbered_from(&writes, tx, started, copy!(view => collected(view.put_multi_get_previous_presence_once(tx, column_id, futures::stream::iter(items))); items)).await?),
                (MultiItems::Streamed(items), started) => {
                    let results = self.streamed_view(started).put_multi_get_previous_presence_once(tx, column_id, items).await;
                    writes.update_ended(tx);
                    results?.boxed()
                }
            },
            None => self.put_multi_get_previous_presence_once(tx, column_id, items).await?.boxed(),
        })
    }

    /// Streams keys to delete. Numbered when the stream counts at most `sessions::NUMBERED_MULTI_MAX_BYTES`, read whole first; a larger
    /// stream is sent unnumbered, streamed.
    pub async fn delete_multi(&self, transaction_or_update_id: i64, column_id: i64, keys: impl Stream<Item = Vec<Vec<u8>>> + Send + 'static) -> Result<()> {
        let tx = transaction_or_update_id;
        match self.numbering(tx) {
            Some(writes) => match self.read_numbered_multi(keys, multi_keys_bytes).await? {
                (MultiItems::Whole(keys), started) => self.numbered_from(&writes, tx, started, copy!(view => view.delete_multi_once(tx, column_id, futures::stream::iter(keys)); keys)).await,
                (MultiItems::Streamed(keys), started) => {
                    let result = self.streamed_view(started).delete_multi_once(tx, column_id, keys).await;
                    writes.update_ended(tx);
                    result
                }
            },
            None => self.delete_multi_once(tx, column_id, keys).await,
        }
    }

    /// Streams keys to delete and returns their previous values. Numbered when the stream counts at most `sessions::NUMBERED_MULTI_MAX_BYTES` (both streams are then read
    /// whole); a larger stream is sent unnumbered, streamed.
    pub async fn delete_multi_get_previous(&self, transaction_or_update_id: i64, column_id: i64, keys: impl Stream<Item = Vec<Vec<u8>>> + Send + 'static) -> Result<impl Stream<Item = Result<Previous>>> {
        let tx = transaction_or_update_id;
        Ok(match self.numbering(tx) {
            Some(writes) => match self.read_numbered_multi(keys, multi_keys_bytes).await? {
                (MultiItems::Whole(keys), started) => replayed(self.numbered_from(&writes, tx, started, copy!(view => collected(view.delete_multi_get_previous_once(tx, column_id, futures::stream::iter(keys))); keys)).await?),
                (MultiItems::Streamed(keys), started) => {
                    let results = self.streamed_view(started).delete_multi_get_previous_once(tx, column_id, keys).await;
                    writes.update_ended(tx);
                    results?.boxed()
                }
            },
            None => self.delete_multi_get_previous_once(tx, column_id, keys).await?.boxed(),
        })
    }

    /// Streams keys to delete and reports whether each value existed. Numbered when the stream counts at most `sessions::NUMBERED_MULTI_MAX_BYTES` (both streams are then read
    /// whole); a larger stream is sent unnumbered, streamed.
    pub async fn delete_multi_get_previous_presence(&self, transaction_or_update_id: i64, column_id: i64, keys: impl Stream<Item = Vec<Vec<u8>>> + Send + 'static) -> Result<impl Stream<Item = Result<PreviousPresence>>> {
        let tx = transaction_or_update_id;
        Ok(match self.numbering(tx) {
            Some(writes) => match self.read_numbered_multi(keys, multi_keys_bytes).await? {
                (MultiItems::Whole(keys), started) => replayed(self.numbered_from(&writes, tx, started, copy!(view => collected(view.delete_multi_get_previous_presence_once(tx, column_id, futures::stream::iter(keys))); keys)).await?),
                (MultiItems::Streamed(keys), started) => {
                    let results = self.streamed_view(started).delete_multi_get_previous_presence_once(tx, column_id, keys).await;
                    writes.update_ended(tx);
                    results?.boxed()
                }
            },
            None => self.delete_multi_get_previous_presence_once(tx, column_id, keys).await?.boxed(),
        })
    }

    /// Merges multiple KV pairs. Numbered when the stream counts at most `sessions::NUMBERED_MULTI_MAX_BYTES`, read whole first; a larger
    /// stream is sent unnumbered, streamed.
    pub async fn merge_multi(&self, transaction_or_update_id: i64, column_id: i64, items: impl Stream<Item = Kv> + Send + 'static) -> Result<()> {
        let tx = transaction_or_update_id;
        match self.numbering(tx) {
            Some(writes) => match self.read_numbered_multi(items, multi_kv_bytes).await? {
                (MultiItems::Whole(items), started) => self.numbered_from(&writes, tx, started, copy!(view => view.merge_multi_once(tx, column_id, futures::stream::iter(items)); items)).await,
                (MultiItems::Streamed(items), started) => {
                    let result = self.streamed_view(started).merge_multi_once(tx, column_id, items).await;
                    writes.update_ended(tx);
                    result
                }
            },
            None => self.merge_multi_once(tx, column_id, items).await,
        }
    }

    /// Merges multiple KV pairs and returns the merged results. Numbered when the stream counts at most `sessions::NUMBERED_MULTI_MAX_BYTES` (both streams are then read
    /// whole); a larger stream is sent unnumbered, streamed.
    pub async fn merge_multi_get_merged(&self, transaction_or_update_id: i64, column_id: i64, items: impl Stream<Item = Kv> + Send + 'static) -> Result<impl Stream<Item = Result<Merged>>> {
        let tx = transaction_or_update_id;
        Ok(match self.numbering(tx) {
            Some(writes) => match self.read_numbered_multi(items, multi_kv_bytes).await? {
                (MultiItems::Whole(items), started) => replayed(self.numbered_from(&writes, tx, started, copy!(view => collected(view.merge_multi_get_merged_once(tx, column_id, futures::stream::iter(items))); items)).await?),
                (MultiItems::Streamed(items), started) => {
                    let results = self.streamed_view(started).merge_multi_get_merged_once(tx, column_id, items).await;
                    writes.update_ended(tx);
                    results?.boxed()
                }
            },
            None => self.merge_multi_get_merged_once(tx, column_id, items).await?.boxed(),
        })
    }

    // ---- Batches ----

    /// Streams batches of KV pairs to put. On a server that numbers batches, each batch is split into the chunks
    /// the server writes as one entry each (`sessions::split_batch`), and each chunk is a numbered write.
    pub async fn put_batch(&self, column_id: i64, mode: PutBatchMode, batches: impl Stream<Item = KvBatch> + Send + 'static) -> Result<()> {
        match self.numbering(0).filter(|writes| writes.numbered_batches()) {
            Some(writes) => {
                self.numbered_batch(&writes, false, column_id, batches, move |view, chunk| async move {
                    view.put_batch_once(column_id, mode, futures::stream::iter([chunk])).await
                })
                .await
            }
            None => self.put_batch_once(column_id, mode, batches).await,
        }
    }

    /// Streams batches of KV pairs to merge, numbered chunk by chunk as `put_batch`.
    pub async fn merge_batch(&self, column_id: i64, mode: MergeBatchMode, batches: impl Stream<Item = KvBatch> + Send + 'static) -> Result<()> {
        match self.numbering(0).filter(|writes| writes.numbered_batches()) {
            Some(writes) => {
                self.numbered_batch(&writes, true, column_id, batches, move |view, chunk| async move {
                    view.merge_batch_once(column_id, mode, futures::stream::iter([chunk])).await
                })
                .await
            }
            None => self.merge_batch_once(column_id, mode, batches).await,
        }
    }
}

fn decode_raw_scan_event(response: ScanRawResponse) -> Result<RawScanEvent> {
    let ScanRawResponse {
        event,
        completed_sst_token_after_batch,
    } = response;
    match event {
        Some(scan_raw_response::Event::Serialized(serialized)) => {
            let completed_sst_token = completed_sst_token_after_batch
                .map(RawSstToken::new)
                .transpose()
                .map_err(Status::internal)?;
            Ok(RawScanEvent::Batch {
                batch: decode_kv_batch(&serialized)?,
                completed_sst_token,
            })
        }
        Some(scan_raw_response::Event::CompletedSstToken(token)) => {
            if completed_sst_token_after_batch.is_some() {
                return Err(Status::internal(
                    "Rockserver returned two completion tokens in one raw-scan event",
                ));
            }
            Ok(RawScanEvent::SstCompleted(
                RawSstToken::new(token).map_err(Status::internal)?,
            ))
        }
        None => Err(Status::internal(
            "Rockserver returned an empty resumable raw-scan event",
        )),
    }
}

fn decode_kv_batch(mut buf: &[u8]) -> Result<KvBatch> {
    use std::convert::TryInto;

    let get_u32 = |b: &mut &[u8]| -> Result<u32> {
        if b.len() < 4 {
            return Err(Status::internal("Buffer too short for u32"));
        }
        let (int_bytes, rest) = b.split_at(4);
        *b = rest;
        Ok(u32::from_le_bytes(int_bytes.try_into().unwrap()))
    };

    let get_u8 = |b: &mut &[u8]| -> Result<u8> {
        if b.len() < 1 {
            return Err(Status::internal("Buffer too short for u8"));
        }
        let (byte, rest) = b.split_at(1);
        *b = rest;
        Ok(byte[0])
    };

    let kv_count = get_u32(&mut buf)?;
    let mut entries = Vec::with_capacity(kv_count as usize);

    for _ in 0..kv_count {
        let keys_count = get_u8(&mut buf)?;
        let mut keys = Vec::with_capacity(keys_count as usize);

        for _ in 0..keys_count {
            let key_len = get_u32(&mut buf)?;
            if buf.len() < key_len as usize {
                return Err(Status::internal("Buffer too short for key"));
            }
            let (key_bytes, rest) = buf.split_at(key_len as usize);
            buf = rest;
            keys.push(key_bytes.to_vec());
        }

        let val_len = get_u32(&mut buf)?;
        if buf.len() < val_len as usize {
            return Err(Status::internal("Buffer too short for value"));
        }
        let (val_bytes, rest) = buf.split_at(val_len as usize);
        buf = rest;

        entries.push(Kv {
            keys,
            value: val_bytes.to_vec(),
        });
    }

    Ok(KvBatch { entries })
}

#[cfg(test)]
mod workload_context_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::task::{Context, Poll};
    use tonic::codegen::{http, Service};
    use tonic::transport::Endpoint;

    #[derive(Clone)]
    struct NeverReadyService {
        calls: Arc<AtomicUsize>,
    }

    impl Service<http::Request<tonic::body::BoxBody>> for NeverReadyService {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::io::Error;
        type Future = std::future::Ready<std::result::Result<Self::Response, Self::Error>>;

        fn poll_ready(
            &mut self,
            _context: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Pending
        }

        fn call(&mut self, _request: http::Request<tonic::body::BoxBody>) -> Self::Future {
            self.calls.fetch_add(1, Ordering::SeqCst);
            std::future::ready(Ok(http::Response::new(tonic::body::empty_body())))
        }
    }

    #[tokio::test]
    async fn client_views_retain_independent_workload_contexts() {
        let channel = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let ingest = RockserverClient::new_unchecked(channel, RequestContext::ingest());
        assert_eq!(ingest.context.profile, 4);

        let analytical = ingest.with_context(RequestContext::analytical());
        assert_eq!(analytical.context.profile, 3);
        assert_eq!(ingest.context.profile, 4);
    }

    #[tokio::test]
    async fn wrong_contract_context_fails_before_transport() {
        let channel = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let context = RequestContext {
            profile: 2,
            workload_contract_version: 2,
            timeout_nanos: 1,
            request_id: None,
        };
        let error = RockserverClient::new_unchecked(channel, context)
            .get_column_id("never-sent".to_owned())
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn wrong_contract_rollback_fails_before_transport() {
        let channel = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let context = RequestContext {
            profile: 2,
            workload_contract_version: 2,
            timeout_nanos: 1,
            request_id: None,
        };
        let error = RockserverClient::new_unchecked(channel, context)
            .close_transaction(1, false)
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn generated_client_readiness_wait_expires_before_service_call() {
        let calls = Arc::new(AtomicUsize::new(0));
        let service = NeverReadyService {
            calls: calls.clone(),
        };
        let mut generated = RocksDbServiceClient::with_interceptor(service, DeadlineInterceptor);
        let deadline = tokio::time::Instant::now() + Duration::from_millis(5);
        let mut request = Request::new(GetColumnIdRequest {
            name: "never-dispatched".to_owned(),
            context: Some(RequestContext::latency(Duration::from_millis(5))),
        });
        request.set_timeout(Duration::from_millis(5));
        request.extensions_mut().insert(BoundDeadline(deadline));

        let error = await_bound_call(Some(deadline), generated.get_column_id(request))
            .await
            .unwrap_err();

        assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn interceptor_rewrites_transport_timeout_at_the_actual_call_boundary() {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(100);
        let mut request = Request::new(());
        request.set_timeout(Duration::from_millis(100));
        request.extensions_mut().insert(BoundDeadline(deadline));
        let original = request
            .metadata()
            .get("grpc-timeout")
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        tokio::time::sleep(Duration::from_millis(10)).await;

        let rewritten = DeadlineInterceptor.call(request).unwrap();
        let remaining = rewritten
            .metadata()
            .get("grpc-timeout")
            .unwrap()
            .to_str()
            .unwrap();
        assert_ne!(remaining, original);
    }

    /// A numbered batch of a caller without a deadline gives each chunk its own retry window: a chunk sent after the
    /// window of the call as a whole would have run out is still resent after an unknown outcome.
    #[tokio::test]
    async fn each_chunk_of_a_batch_without_a_deadline_gets_its_own_retry_window() {
        let channel = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let mut client = RockserverClient::new_unchecked(channel, RequestContext::batch())
            .with_session_retry_window(Duration::from_millis(400));
        client.sessions = Some(Arc::new(SessionWrites::with_open_session(true, 9)));
        let writes = client.sessions.clone().unwrap();
        let batch = |k: u8| KvBatch {
            entries: vec![Kv { keys: vec![vec![k]], value: vec![k] }],
        };
        let copies = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sent = copies.clone();
        let result = client
            .numbered_batch(
                &writes,
                false,
                1,
                futures::stream::iter([batch(1), batch(2), batch(3)]),
                move |view, chunk| {
                    let sent = sent.clone();
                    async move {
                        let id = view.context.request_id.clone().expect("numbered");
                        let key = chunk.entries[0].keys[0][0];
                        let first_copy = {
                            let mut sent = sent.lock().unwrap();
                            let first = !sent.iter().any(|(k, _)| *k == key);
                            sent.push((key, id.sequence));
                            first
                        };
                        if key == 3 && first_copy {
                            return Err(Status::unavailable("leader lost"));
                        }
                        if key < 3 {
                            // Two chunks take 500 ms: past the 400 ms window of the call as a whole.
                            tokio::time::sleep(Duration::from_millis(250)).await;
                        }
                        Ok(())
                    }
                },
            )
            .await;
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(*copies.lock().unwrap(), vec![(1, 0), (2, 1), (3, 2), (3, 2)]);
    }

    #[test]
    fn resumable_raw_scan_decodes_coalesced_completion_without_extra_event() {
        let event = decode_raw_scan_event(ScanRawResponse {
            event: Some(scan_raw_response::Event::Serialized(vec![0, 0, 0, 0])),
            completed_sst_token_after_batch: Some("/000123.sst".to_owned()),
        })
        .unwrap();

        assert_eq!(
            event,
            RawScanEvent::Batch {
                batch: KvBatch {
                    entries: Vec::new()
                },
                completed_sst_token: Some(RawSstToken::new("/000123.sst".to_owned()).unwrap()),
            }
        );
    }

    #[test]
    fn resumable_raw_scan_decodes_empty_sst_completion() {
        let event = decode_raw_scan_event(ScanRawResponse {
            event: Some(scan_raw_response::Event::CompletedSstToken(
                "000456.sst".to_owned(),
            )),
            completed_sst_token_after_batch: None,
        })
        .unwrap();

        assert_eq!(
            event,
            RawScanEvent::SstCompleted(RawSstToken::new("000456.sst".to_owned()).unwrap())
        );
    }

    #[test]
    fn resumable_raw_scan_rejects_ambiguous_or_malformed_completion() {
        for response in [
            ScanRawResponse {
                event: Some(scan_raw_response::Event::CompletedSstToken(
                    "000123.sst".to_owned(),
                )),
                completed_sst_token_after_batch: Some("000124.sst".to_owned()),
            },
            ScanRawResponse {
                event: Some(scan_raw_response::Event::CompletedSstToken(
                    "../000123.sst".to_owned(),
                )),
                completed_sst_token_after_batch: None,
            },
            ScanRawResponse {
                event: None,
                completed_sst_token_after_batch: None,
            },
        ] {
            assert_eq!(
                decode_raw_scan_event(response).unwrap_err().code(),
                tonic::Code::Internal
            );
        }
    }
}

#[cfg(test)]
mod numbered_multi_tests {
    use super::*;
    use crate::sessions::{is_not_applied, is_outcome_unknown, rocksdb_error_type};
    use tonic::transport::Endpoint;

    /// A client numbering its writes through an already open session, over a channel to a closed port: every copy
    /// it sends fails as unavailable, an unknown outcome.
    fn numbering_client(timeout: Duration) -> RockserverClient {
        let channel = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let mut client =
            RockserverClient::new_unchecked(channel, RequestContext::batch_with_timeout(timeout));
        client.sessions = Some(Arc::new(SessionWrites::with_open_session(true, 7)));
        client
    }

    /// An input stream of `count` small items, one every `every`.
    fn slow_items(count: u64, every: Duration) -> impl Stream<Item = Kv> + Send + 'static {
        futures::stream::iter(0..count).then(move |k| async move {
            tokio::time::sleep(every).await;
            Kv {
                keys: vec![k.to_be_bytes().to_vec()],
                value: vec![1],
            }
        })
    }

    /// R6-4: the deadline of a numbered multi write counts from the call. An input stream that outlasts it ends the
    /// write at the deadline, flagged not applied (nothing was sent), instead of starting the deadline once the
    /// whole input was read and then sending copies for a full timeout.
    #[tokio::test]
    async fn a_numbered_multi_write_whose_input_outlasts_its_deadline_is_not_sent() {
        for variant in 0..3 {
            let client = numbering_client(Duration::from_millis(200));
            let start = std::time::Instant::now();
            let error = match variant {
                0 => client
                    .put_multi(0, 1, slow_items(10, Duration::from_millis(100)))
                    .await
                    .unwrap_err(),
                1 => client
                    .merge_multi_get_merged(0, 1, slow_items(10, Duration::from_millis(100)))
                    .await
                    .err()
                    .expect("refused"),
                _ => client
                    .delete_multi(
                        0,
                        1,
                        slow_items(10, Duration::from_millis(100)).map(|kv| kv.keys),
                    )
                    .await
                    .unwrap_err(),
            };
            let elapsed = start.elapsed();
            assert!(
                elapsed < Duration::from_millis(700),
                "variant {variant}: ended after {elapsed:?}, not at the deadline"
            );
            assert_eq!(rocksdb_error_type(&error), Some("READ_DEADLINE_EXCEEDED"), "variant {variant}: {error:?}");
            assert!(is_not_applied(&error), "variant {variant}: {error:?}");
            assert!(!is_outcome_unknown(&error), "variant {variant}: {error:?}");
        }
    }

    /// An input that ends within the deadline is sent with what is left of it: copies to an unreachable server end
    /// as an unknown outcome by the call's deadline, not by one counted from the end of the input.
    #[tokio::test]
    async fn a_numbered_multi_write_keeps_the_deadline_of_its_call() {
        let client = numbering_client(Duration::from_millis(2000));
        let start = std::time::Instant::now();
        let error = client
            .put_multi(0, 1, slow_items(10, Duration::from_millis(150)))
            .await
            .unwrap_err();
        let elapsed = start.elapsed();
        // The input takes 1.5 s: the call's deadline is at 2 s, one counted from the input's end at 3.5 s. The bound
        // leaves 1 s for sleep overshoot and connect attempts on a loaded machine.
        assert!(
            elapsed < Duration::from_millis(3000),
            "ended after {elapsed:?}: the deadline started after the input"
        );
        assert!(is_outcome_unknown(&error) || is_not_applied(&error), "{error:?}");
    }

    /// R6-4 review: a caller without a deadline gets its retry window from the moment the write is sent, not from
    /// the call. An input that takes longer than the window to read still leaves the write a full window of resends.
    #[tokio::test]
    async fn a_numbered_multi_write_without_a_deadline_starts_its_retry_window_when_sent() {
        let channel = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let mut client = RockserverClient::new_unchecked(channel, RequestContext::batch());
        client.sessions = Some(Arc::new(SessionWrites::with_open_session(true, 7)));
        let client = client.with_session_retry_window(Duration::from_millis(1000));
        // The input takes 1.5 s, longer than the 1 s window.
        let error = client
            .put_multi(0, 1, slow_items(10, Duration::from_millis(150)))
            .await
            .unwrap_err();
        assert!(is_outcome_unknown(&error), "{error:?}");
        assert!(
            client.numbered_resends() > 0,
            "the write was sent once and never resent: its window ran out while the input was read"
        );
    }
}
