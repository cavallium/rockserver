use crate::proto::rocks_db_service_client::RocksDbServiceClient;
use crate::proto::*; // For request/response types
use crate::types::*;
use crate::types::{Column, ColumnSchema};
use futures::{Stream, StreamExt};
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::time::Duration;
use tonic::service::interceptor::InterceptedService;
use tonic::service::Interceptor;
use tonic::transport::Channel;
use tonic::{Request, Status}; // Disambiguate from proto

pub type Result<T> = std::result::Result<T, Status>;

pub const REQUIRED_WORKLOAD_CONTRACT_VERSION: i32 = 3;
const DEFAULT_CDC_MAX_RESPONSE_BYTES: i32 = 4 * 1024 * 1024;

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
        Self::require_capabilities(&mut handshake_client)
            .await
            .map_err(RockserverConnectError::Capabilities)?;
        Ok(Self {
            client: RocksDbServiceClient::with_interceptor(channel, DeadlineInterceptor),
            context,
        })
    }

    /// Creates a new client from an existing Tonic `Channel` after the mandatory
    /// workload-contract capability handshake.
    pub async fn new(channel: Channel, context: RequestContext) -> Result<Self> {
        let mut handshake_client = RocksDbServiceClient::new(channel.clone());
        Self::require_capabilities(&mut handshake_client).await?;
        Ok(Self {
            client: RocksDbServiceClient::with_interceptor(channel, DeadlineInterceptor),
            context,
        })
    }

    #[cfg(test)]
    fn new_unchecked(channel: Channel, context: RequestContext) -> Self {
        Self {
            client: RocksDbServiceClient::with_interceptor(channel, DeadlineInterceptor),
            context,
        }
    }

    async fn require_capabilities(client: &mut RocksDbServiceClient<Channel>) -> Result<()> {
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
        Ok(())
    }

    /// Returns a client view that applies one mandatory workload context to every generic request.
    /// The underlying channel is shared and cloning this view is cheap.
    pub fn with_context(&self, context: RequestContext) -> Self {
        Self {
            client: self.client.clone(),
            context,
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
    pub async fn close_transaction(&self, transaction_id: i64, commit: bool) -> Result<bool> {
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
    pub async fn close_failed_update(&self, update_id: i64) -> Result<()> {
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
    pub async fn create_column(&self, name: String, schema: ColumnSchema) -> Result<i64> {
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
    pub async fn delete_column(&self, column_id: i64) -> Result<()> {
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
    pub async fn delete_column_if_exists(&self, name: String) -> Result<bool> {
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
    pub async fn put(
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
    pub async fn put_ensure(
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
    pub async fn put_get_previous(
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
    pub async fn put_get_delta(
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
    pub async fn put_get_changed(
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
    pub async fn put_get_previous_presence(
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
    pub async fn put_batch(
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
    pub async fn merge_batch(
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
    pub async fn put_multi_list(
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
    pub async fn put_multi(
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
    pub async fn put_multi_ensure(
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
    pub async fn put_multi_get_previous(
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
    pub async fn put_multi_get_delta(
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
    pub async fn put_multi_get_changed(
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
    pub async fn put_multi_get_previous_presence(
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
    pub async fn delete(
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
    pub async fn delete_get_previous(
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
    pub async fn delete_get_previous_presence(
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
    pub async fn delete_range(
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
    pub async fn delete_multi(
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
    pub async fn delete_multi_get_previous(
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
    pub async fn delete_multi_get_previous_presence(
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
    pub async fn merge_multi(
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
    pub async fn merge_multi_get_merged(
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
    pub async fn merge(
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
    pub async fn merge_get_merged(
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
    pub async fn get_for_update(
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

    // ============================================================================================
    // CDC
    // ============================================================================================

    /// Creates a new CDC stream.
    pub async fn cdc_create(
        &self,
        id: String,
        from_seq: Option<i64>,
        column_ids: Vec<i64>,
        resolved_values: Option<bool>,
    ) -> Result<i64> {
        let req = CdcCreateRequest {
            id,
            from_seq,
            column_ids,
            resolved_values,
            expected_last_committed: Some(cdc_create_request::ExpectedLastCommitted::ExpectAbsent(
                (),
            )),
            workload_contract_version: REQUIRED_WORKLOAD_CONTRACT_VERSION,
        };
        let resp = self.client.clone().cdc_create(req).await?;
        Ok(resp.into_inner().start_seq)
    }

    /// Creates or updates a CDC stream with an atomic durable-checkpoint precondition.
    pub async fn cdc_create_checked(
        &self,
        id: String,
        from_seq: Option<i64>,
        column_ids: Vec<i64>,
        resolved_values: Option<bool>,
        precondition: CdcCreatePrecondition,
    ) -> std::result::Result<i64, CdcError> {
        let expected_last_committed = match precondition {
            CdcCreatePrecondition::Absent => {
                Some(cdc_create_request::ExpectedLastCommitted::ExpectAbsent(()))
            }
            CdcCreatePrecondition::LastCommitted(sequence) => {
                Some(cdc_create_request::ExpectedLastCommitted::ExpectedLastCommittedSeq(sequence))
            }
        };
        let req = CdcCreateRequest {
            id,
            from_seq,
            column_ids,
            resolved_values,
            expected_last_committed,
            workload_contract_version: REQUIRED_WORKLOAD_CONTRACT_VERSION,
        };
        let resp = self
            .client
            .clone()
            .cdc_create(req)
            .await
            .map_err(CdcError::from)?;
        Ok(resp.into_inner().start_seq)
    }

    /// Deletes a CDC stream.
    pub async fn cdc_delete(&self, id: String) -> Result<()> {
        let req = CdcDeleteRequest {
            id,
            workload_contract_version: REQUIRED_WORKLOAD_CONTRACT_VERSION,
        };
        self.client.clone().cdc_delete(req).await?;
        Ok(())
    }

    /// Returns the earliest CDC cursor still available in the database WAL.
    pub async fn cdc_get_earliest_available_sequence(&self) -> Result<i64> {
        let resp = self
            .client
            .clone()
            .cdc_get_earliest_available_sequence(CdcGetEarliestAvailableSequenceRequest {
                workload_contract_version: REQUIRED_WORKLOAD_CONTRACT_VERSION,
            })
            .await?;
        Ok(resp.into_inner().sequence)
    }

    /// Returns the durable last committed sequence for a CDC subscription.
    ///
    /// `None` means that the subscription metadata does not exist.
    pub async fn cdc_get_last_committed_sequence(&self, id: String) -> Result<Option<i64>> {
        let req = CdcGetLastCommittedSequenceRequest {
            id,
            workload_contract_version: REQUIRED_WORKLOAD_CONTRACT_VERSION,
        };
        let resp = self
            .client
            .clone()
            .cdc_get_last_committed_sequence(req)
            .await?;
        Ok(resp.into_inner().last_committed_seq)
    }

    /// Commits a sequence number for a CDC stream.
    pub async fn cdc_commit(&self, id: String, seq: i64) -> Result<()> {
        let req = CdcCommitRequest {
            id,
            seq,
            workload_contract_version: REQUIRED_WORKLOAD_CONTRACT_VERSION,
        };
        self.client.clone().cdc_commit(req).await?;
        Ok(())
    }

    /// Commits a CDC sequence while preserving a typed missing-subscription failure.
    pub async fn cdc_commit_checked(
        &self,
        id: String,
        seq: i64,
    ) -> std::result::Result<(), CdcError> {
        let req = CdcCommitRequest {
            id,
            seq,
            workload_contract_version: REQUIRED_WORKLOAD_CONTRACT_VERSION,
        };
        self.client
            .clone()
            .cdc_commit(req)
            .await
            .map_err(CdcError::from)?;
        Ok(())
    }

    /// Polls for new CDC events.
    pub async fn cdc_poll(
        &self,
        id: String,
        from_seq: Option<i64>,
        max_events: i64,
    ) -> Result<impl Stream<Item = Result<CdcEvent>>> {
        let req = CdcPollRequest {
            id,
            from_seq,
            max_events,
            max_response_bytes: DEFAULT_CDC_MAX_RESPONSE_BYTES,
            workload_contract_version: REQUIRED_WORKLOAD_CONTRACT_VERSION,
        };
        let resp = self.client.clone().cdc_poll(req).await?;
        Ok(resp.into_inner())
    }

    /// Polls CDC events and classifies missing subscription metadata on both stream setup and items.
    pub async fn cdc_poll_checked(
        &self,
        id: String,
        from_seq: Option<i64>,
        max_events: i64,
    ) -> std::result::Result<impl Stream<Item = std::result::Result<CdcEvent, CdcError>>, CdcError>
    {
        let req = CdcPollRequest {
            id,
            from_seq,
            max_events,
            max_response_bytes: DEFAULT_CDC_MAX_RESPONSE_BYTES,
            workload_contract_version: REQUIRED_WORKLOAD_CONTRACT_VERSION,
        };
        let resp = self
            .client
            .clone()
            .cdc_poll(req)
            .await
            .map_err(CdcError::from)?;
        Ok(resp.into_inner().map(|item| item.map_err(CdcError::from)))
    }

    /// Polls a CDC batch and returns the server's exact next cursor. The cursor can advance
    /// even when column filtering leaves the returned event list empty.
    pub async fn cdc_poll_batch(
        &self,
        id: String,
        from_seq: Option<i64>,
        max_events: i64,
    ) -> Result<CdcPollResponse> {
        let req = CdcPollRequest {
            id,
            from_seq,
            max_events,
            max_response_bytes: DEFAULT_CDC_MAX_RESPONSE_BYTES,
            workload_contract_version: REQUIRED_WORKLOAD_CONTRACT_VERSION,
        };
        let resp = self.client.clone().cdc_poll_batch(req).await?;
        Ok(resp.into_inner())
    }

    /// Polls a CDC batch while preserving a typed missing-subscription failure.
    pub async fn cdc_poll_batch_checked(
        &self,
        id: String,
        from_seq: Option<i64>,
        max_events: i64,
    ) -> std::result::Result<CdcPollResponse, CdcError> {
        let req = CdcPollRequest {
            id,
            from_seq,
            max_events,
            max_response_bytes: DEFAULT_CDC_MAX_RESPONSE_BYTES,
            workload_contract_version: REQUIRED_WORKLOAD_CONTRACT_VERSION,
        };
        let resp = self
            .client
            .clone()
            .cdc_poll_batch(req)
            .await
            .map_err(CdcError::from)?;
        Ok(resp.into_inner())
    }

    /// High-level method to stream CDC events continuously.
    ///
    /// This method manages polling, retries (TODO), and offset commits based on the provided options.
    ///
    /// # Arguments
    /// * `id` - The CDC stream ID.
    /// * `options` - Configuration options for streaming.
    /// * `processor` - A closure that processes each event.
    pub async fn cdc_stream<F, Fut>(
        &self,
        id: String,
        options: CdcStreamOptions,
        mut processor: F,
    ) -> Result<()>
    where
        F: FnMut(CdcEvent) -> Fut,
        Fut: std::future::Future<
            Output = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>,
        >,
    {
        let mut seq = options.from_seq;
        let batch_size = if options.batch_size > 0 {
            options.batch_size
        } else {
            1000
        };
        let idle_delay = options.idle_delay;

        loop {
            // TODO: Handle transport errors with retry backoff?
            let stream_result = self.cdc_poll(id.clone(), seq, batch_size).await;

            let stream = match stream_result {
                Ok(s) => s,
                Err(e) => return Err(e),
            };

            let mut events = Vec::new();
            let mut pinned_stream = Box::pin(stream);

            while let Some(event_res) = pinned_stream.next().await {
                match event_res {
                    Ok(event) => events.push(event),
                    Err(e) => return Err(e),
                }
            }

            if events.is_empty() {
                tokio::time::sleep(idle_delay).await;
                continue;
            }

            let last_seq = events.last().unwrap().seq;

            for event in events {
                if let Err(e) = processor(event.clone()).await {
                    return Err(Status::internal(format!("Processor error: {}", e)));
                }

                if options.commit_mode == CdcCommitMode::PerEvent {
                    self.cdc_commit(id.clone(), event.seq).await?;
                }
            }

            if options.commit_mode == CdcCommitMode::Batch {
                self.cdc_commit(id.clone(), last_seq).await?;
            }

            seq = Some(last_seq + 1);
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
