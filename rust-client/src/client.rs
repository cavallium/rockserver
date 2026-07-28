use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tonic::transport::Channel;
use tonic::{Request, Status};
use futures::{Stream, StreamExt};
use crate::proto::rocks_db_service_client::RocksDbServiceClient;
use crate::proto::*; // For request/response types
use crate::types::*;
use crate::types::{Column, ColumnSchema}; // Disambiguate from proto

pub type Result<T> = std::result::Result<T, Status>;

/// Client for interacting with the RockServer via gRPC.
///
/// This client provides a safe and idiomatic Rust API over the raw gRPC service.
/// It handles type conversions and provides higher-level abstractions where appropriate.
///
/// # Cloning
/// The client is cheap to clone. Cloning creates a new handle to the same underlying connection.
#[derive(Clone, Debug)]
pub struct RockserverClient {
    client: RocksDbServiceClient<Channel>,
	context: RequestContext,
}

impl RockserverClient {
    /// Connect to the RockServer at the specified destination.
    ///
    /// # Arguments
    /// * `dst` - The destination to connect to. Can be a string like `http://[::1]:50051`.
    ///
    /// # Returns
    /// A `Result` containing the connected client or a transport error.
	pub async fn connect<D>(dst: D, context: RequestContext) -> std::result::Result<Self, tonic::transport::Error>
    where
        D: std::convert::TryInto<tonic::transport::Endpoint>,
        D::Error: Into<tonic::codegen::StdError>,
    {
        let client = RocksDbServiceClient::connect(dst).await?;
		Ok(Self { client, context })
    }

    /// Create a new client from an existing Tonic `Channel`.
	pub fn new(channel: Channel, context: RequestContext) -> Self {
		Self {
			client: RocksDbServiceClient::new(channel),
			context,
		}
	}

	/// Returns a client view that applies one mandatory workload context to every generic request.
	/// The underlying channel is shared and cloning this view is cheap.
	pub fn with_context(&self, context: RequestContext) -> Self {
		Self {
			client: self.client.clone(),
			context,
		}
	}

	fn contextual_request<T>(&self, message: T) -> Result<Request<T>> {
		self.contextual_request_with_timeout(message, None)
	}

	fn contextual_request_with_timeout<T>(
		&self,
		message: T,
		operation_timeout_ms: Option<i64>,
	) -> Result<Request<T>> {
		match self.context.profile {
			2 | 3 | 4 | 6 => {}
			0 => return Err(Status::invalid_argument("request workload profile is required")),
			1 | 5 | 7 => {
				return Err(Status::invalid_argument("workload profile is owned by Rockserver"));
			}
			value => {
				return Err(Status::invalid_argument(format!(
					"unknown workload profile: {value}"
				)));
			}
		}
		if self.context.deadline_epoch_millis <= 0 {
			return Err(Status::invalid_argument("deadline_epoch_millis must be positive"));
		}
		if self.context.profile == 2 && self.context.deadline_epoch_millis == i64::MAX {
			return Err(Status::invalid_argument("LATENCY requires a finite deadline"));
		}

		let mut timeout = if self.context.deadline_epoch_millis == i64::MAX {
			None
		} else {
			let now_millis = SystemTime::now()
				.duration_since(UNIX_EPOCH)
				.map_err(|_| Status::internal("system clock is before the Unix epoch"))?
				.as_millis()
				.min(i64::MAX as u128) as i64;
			let remaining_millis = self.context.deadline_epoch_millis - now_millis;
			if remaining_millis <= 0 {
				return Err(Status::deadline_exceeded("request deadline already expired"));
			}
			Some(Duration::from_millis(remaining_millis as u64))
		};

		if let Some(operation_timeout_ms) = operation_timeout_ms {
			if operation_timeout_ms < 0 {
				return Err(Status::invalid_argument("operation timeout must be non-negative"));
			}
			if operation_timeout_ms != i64::MAX {
				let operation_timeout = Duration::from_millis(operation_timeout_ms as u64);
				timeout = Some(timeout.map_or(operation_timeout, |value| value.min(operation_timeout)));
			}
		}

		let mut request = Request::new(message);
		if let Some(timeout) = timeout {
			request.set_timeout(timeout);
		}
		Ok(request)
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
    pub async fn open_transaction(&self, timeout_ms: i64) -> Result<i64> {
		let req = OpenTransactionRequest { timeout_ms, context: Some(self.context.clone()) };
		let resp = self.client.clone().open_transaction(self.contextual_request(req)?).await?;
        Ok(resp.into_inner().transaction_id)
    }

    /// Closes an existing transaction.
    ///
    /// # Arguments
    /// * `transaction_id` - The ID of the transaction to close.
    /// * `commit` - If true, commits the transaction. If false, aborts it.
    /// * `timeout_ms` - Timeout for the commit operation.
    ///
    /// # Returns
    /// `true` if the operation was successful.
    pub async fn close_transaction(&self, transaction_id: i64, commit: bool, timeout_ms: i64) -> Result<bool> {
        let req = CloseTransactionRequest {
            transaction_id,
            timeout_ms,
            commit,
            context: Some(self.context.clone()),
        };
		let request = if commit { self.contextual_request(req)? } else { Request::new(req) };
		let resp = self.client.clone().close_transaction(request).await?;
        Ok(resp.into_inner().successful)
    }

    /// Closes a failed update explicitly.
    pub async fn close_failed_update(&self, update_id: i64) -> Result<()> {
        let req = CloseFailedUpdateRequest { update_id };
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
		let resp = self.client.clone().create_column(self.contextual_request(req)?).await?;
        Ok(resp.into_inner().column_id)
    }

    /// Deletes a column by its ID.
    pub async fn delete_column(&self, column_id: i64) -> Result<()> {
        let req = DeleteColumnRequest { column_id, context: Some(self.context.clone()) };
		self.client.clone().delete_column(self.contextual_request(req)?).await?;
        Ok(())
    }

    /// Deletes a column by name if it exists.
    ///
    /// Returns `true` when a physical column was deleted and `false` when it was already absent.
    pub async fn delete_column_if_exists(&self, name: String) -> Result<bool> {
        let req = DeleteColumnIfExistsRequest { name, context: Some(self.context.clone()) };
		let resp = self.client.clone().delete_column_if_exists(self.contextual_request(req)?).await?;
        Ok(resp.into_inner().deleted)
    }

    /// Retrieves the ID of a column by its name.
    pub async fn get_column_id(&self, name: String) -> Result<i64> {
		let req = GetColumnIdRequest { name, context: Some(self.context.clone()) };
		let resp = self.client.clone().get_column_id(self.contextual_request(req)?).await?;
        Ok(resp.into_inner().column_id)
    }

    /// Returns RocksDB's unbounded estimate of physical keys in a column.
    pub async fn estimate_num_keys(&self, column_id: i64) -> Result<i64> {
		let req = EstimateNumKeysRequest { column_id, context: Some(self.context.clone()) };
		let resp = self.client.clone().estimate_num_keys(self.contextual_request(req)?).await?;
        Ok(resp.into_inner().count)
    }

    /// Retrieves definitions for all existing columns.
    pub async fn get_all_column_definitions(&self) -> Result<Vec<Column>> {
		let req = GetAllColumnDefinitionsRequest { context: Some(self.context.clone()) };
		let resp = self.client.clone().get_all_column_definitions(self.contextual_request(req)?).await?;
        Ok(resp.into_inner().columns.into_iter().map(|c| c.into()).collect())
    }

    // ============================================================================================
    // Data Operations - Put
    // ============================================================================================

    /// Puts a value for a specific key.
    pub async fn put(&self, transaction_or_update_id: i64, column_id: i64, keys: Vec<Vec<u8>>, value: Vec<u8>) -> Result<()> {
        let req = PutRequest {
            transaction_or_update_id,
            column_id,
            data: Some(Kv { keys, value }),
            context: Some(self.context.clone()),
        };
		self.client.clone().put(self.contextual_request(req)?).await?;
        Ok(())
    }
    
    /// Puts a value and returns the previous value if it existed.
    pub async fn put_get_previous(&self, transaction_or_update_id: i64, column_id: i64, keys: Vec<Vec<u8>>, value: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let req = PutRequest {
            transaction_or_update_id,
            column_id,
            data: Some(Kv { keys, value }),
            context: Some(self.context.clone()),
        };
		let resp = self.client.clone().put_get_previous(self.contextual_request(req)?).await?;
        Ok(resp.into_inner().previous)
    }

    /// Puts a value and returns the delta between previous and new value.
    pub async fn put_get_delta(&self, transaction_or_update_id: i64, column_id: i64, keys: Vec<Vec<u8>>, value: Vec<u8>) -> Result<Delta> {
        let req = PutRequest {
            transaction_or_update_id,
            column_id,
            data: Some(Kv { keys, value }),
            context: Some(self.context.clone()),
        };
		let resp = self.client.clone().put_get_delta(self.contextual_request(req)?).await?;
        Ok(resp.into_inner())
    }

    /// Puts a value and returns whether the value actually changed.
    pub async fn put_get_changed(&self, transaction_or_update_id: i64, column_id: i64, keys: Vec<Vec<u8>>, value: Vec<u8>) -> Result<bool> {
        let req = PutRequest {
            transaction_or_update_id,
            column_id,
            data: Some(Kv { keys, value }),
            context: Some(self.context.clone()),
        };
		let resp = self.client.clone().put_get_changed(self.contextual_request(req)?).await?;
        Ok(resp.into_inner().changed)
    }

    /// Puts a value and returns whether a previous value was present.
    pub async fn put_get_previous_presence(&self, transaction_or_update_id: i64, column_id: i64, keys: Vec<Vec<u8>>, value: Vec<u8>) -> Result<bool> {
        let req = PutRequest {
            transaction_or_update_id,
            column_id,
            data: Some(Kv { keys, value }),
            context: Some(self.context.clone()),
        };
		let resp = self.client.clone().put_get_previous_presence(self.contextual_request(req)?).await?;
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

		self.client.clone().put_batch(self.contextual_request(request_stream)?).await?;
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
            merge_batch_request_type: Some(merge_batch_request::MergeBatchRequestType::InitialRequest(
                MergeBatchInitialRequest {
                    column_id,
                    mode: mode.into(),
                    context: Some(self.context.clone()),
                },
            )),
        };

        let request_stream = async_stream::stream! {
            yield initial;
            for await batch in batches {
                yield MergeBatchRequest {
                    merge_batch_request_type: Some(merge_batch_request::MergeBatchRequestType::Data(batch)),
                };
            }
        };

		self.client.clone().merge_batch(self.contextual_request(request_stream)?).await?;
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
		self.client.clone().put_multi_list(self.contextual_request(req)?).await?;
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

		self.client.clone().put_multi(self.contextual_request(request_stream)?).await?;
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

		let resp = self.client.clone().put_multi_get_previous(self.contextual_request(request_stream)?).await?;
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

		let resp = self.client.clone().put_multi_get_delta(self.contextual_request(request_stream)?).await?;
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

		let resp = self.client.clone().put_multi_get_changed(self.contextual_request(request_stream)?).await?;
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

		let resp = self.client.clone().put_multi_get_previous_presence(self.contextual_request(request_stream)?).await?;
        Ok(resp.into_inner())
    }

    // ============================================================================================
    // Data Operations - Delete
    // ============================================================================================

    /// Deletes a value for a specific key.
    pub async fn delete(&self, transaction_or_update_id: i64, column_id: i64, keys: Vec<Vec<u8>>) -> Result<()> {
        let req = DeleteRequest {
            transaction_or_update_id,
            column_id,
            keys,
            context: Some(self.context.clone()),
        };
		self.client.clone().delete(self.contextual_request(req)?).await?;
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
		let response = self.client.clone().delete_get_previous(self.contextual_request(req)?).await?;
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
		let response = self.client.clone().delete_get_previous_presence(self.contextual_request(req)?).await?;
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
		self.client.clone().delete_range(self.contextual_request(req)?).await?;
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
		self.client.clone().delete_multi(self.contextual_request(requests)?).await?;
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
		let response = self.client.clone().delete_multi_get_previous(self.contextual_request(requests)?).await?;
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
		let response = self.client.clone().delete_multi_get_previous_presence(self.contextual_request(requests)?).await?;
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
            merge_multi_request_type: Some(merge_multi_request::MergeMultiRequestType::InitialRequest(
                MergeMultiInitialRequest {
                    transaction_or_update_id,
                    column_id,
                    context: Some(self.context.clone()),
                },
            )),
        };

        let request_stream = async_stream::stream! {
            yield initial;
            for await item in items {
                yield MergeMultiRequest {
                    merge_multi_request_type: Some(merge_multi_request::MergeMultiRequestType::Data(item)),
                };
            }
        };

		self.client.clone().merge_multi(self.contextual_request(request_stream)?).await?;
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
            merge_multi_request_type: Some(merge_multi_request::MergeMultiRequestType::InitialRequest(
                MergeMultiInitialRequest {
                    transaction_or_update_id,
                    column_id,
                    context: Some(self.context.clone()),
                },
            )),
        };

        let request_stream = async_stream::stream! {
            yield initial;
            for await item in items {
                yield MergeMultiRequest {
                    merge_multi_request_type: Some(merge_multi_request::MergeMultiRequestType::Data(item)),
                };
            }
        };

		let resp = self.client.clone().merge_multi_get_merged(self.contextual_request(request_stream)?).await?;
        Ok(resp.into_inner())
    }

    /// Merges a value for a specific key.
    pub async fn merge(&self, transaction_or_update_id: i64, column_id: i64, keys: Vec<Vec<u8>>, value: Vec<u8>) -> Result<()> {
        let req = MergeRequest {
            transaction_or_update_id,
            column_id,
            data: Some(Kv { keys, value }),
            context: Some(self.context.clone()),
        };
		self.client.clone().merge(self.contextual_request(req)?).await?;
        Ok(())
    }

    /// Merges a value and returns the result.
    pub async fn merge_get_merged(&self, transaction_or_update_id: i64, column_id: i64, keys: Vec<Vec<u8>>, value: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let req = MergeRequest {
            transaction_or_update_id,
            column_id,
            data: Some(Kv { keys, value }),
            context: Some(self.context.clone()),
        };
		let resp = self.client.clone().merge_get_merged(self.contextual_request(req)?).await?;
        Ok(resp.into_inner().merged)
    }

    // ============================================================================================
    // Data Operations - Get
    // ============================================================================================

    /// Gets a value by key.
    pub async fn get(&self, transaction_or_update_id: i64, column_id: i64, keys: Vec<Vec<u8>>) -> Result<Option<Vec<u8>>> {
		let req = GetRequest {
			transaction_or_update_id,
			column_id,
			keys,
			context: Some(self.context.clone()),
        };
		let resp = self.client.clone().get(self.contextual_request(req)?).await?;
        Ok(resp.into_inner().value)
    }

    /// Gets a value for update (locking).
    pub async fn get_for_update(&self, transaction_or_update_id: i64, column_id: i64, keys: Vec<Vec<u8>>) -> Result<UpdateBegin> {
		let req = GetRequest {
			transaction_or_update_id,
			column_id,
			keys,
			context: Some(self.context.clone()),
        };
		let resp = self.client.clone().get_for_update(self.contextual_request(req)?).await?;
        Ok(resp.into_inner())
    }

    /// Checks if a key exists.
    pub async fn exists(&self, transaction_or_update_id: i64, column_id: i64, keys: Vec<Vec<u8>>) -> Result<bool> {
		let req = GetRequest {
			transaction_or_update_id,
			column_id,
			keys,
			context: Some(self.context.clone()),
        };
		let resp = self.client.clone().exists(self.contextual_request(req)?).await?;
        Ok(resp.into_inner().present)
    }

    /// Checks several logical keys for presence in one bounded request.
    pub async fn exists_multi(
        &self,
        transaction_id: i64,
        column_id: i64,
        keys_multi: Vec<Vec<Vec<u8>>>,
        timeout_ms: i64,
    ) -> Result<Vec<bool>> {
        let req = ExistsMultiRequest {
            transaction_id,
            column_id,
            keys_multi: keys_multi
                .into_iter()
				.map(|keys| KeyTuple { keys })
				.collect(),
			timeout_ms,
			context: Some(self.context.clone()),
        };
		let resp = self.client.clone().exists_multi(
			self.contextual_request_with_timeout(req, Some(timeout_ms))?,
		).await?;
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
        timeout_ms: i64,
    ) -> Result<i64> {
        let req = OpenIteratorRequest {
            transaction_id,
            column_id,
            start_keys_inclusive,
            end_keys_exclusive,
			reverse,
			timeout_ms,
			context: Some(self.context.clone()),
        };
		let resp = self.client.clone().open_iterator(self.contextual_request(req)?).await?;
        Ok(resp.into_inner().iterator_id)
    }

    /// Closes an active iterator.
    pub async fn close_iterator(&self, iterator_id: i64) -> Result<()> {
        let req = CloseIteratorRequest { iterator_id };
        self.client.clone().close_iterator(req).await?;
        Ok(())
    }

    /// Seeks the iterator to a specific key.
    pub async fn seek_to(&self, iteration_id: i64, keys: Vec<Vec<u8>>) -> Result<()> {
		let req = SeekToRequest { iteration_id, keys, context: Some(self.context.clone()) };
		self.client.clone().seek_to(self.contextual_request(req)?).await?;
        Ok(())
    }

    /// Advances the iterator.
    pub async fn subsequent(&self, iteration_id: i64, skip_count: i64, take_count: i64) -> Result<()> {
        let req = SubsequentRequest {
			iteration_id,
			skip_count,
			take_count,
			context: Some(self.context.clone()),
        };
		self.client.clone().subsequent(self.contextual_request(req)?).await?;
        Ok(())
    }
    
    /// Advances the iterator and checks existence.
    pub async fn subsequent_exists(&self, iteration_id: i64, skip_count: i64, take_count: i64) -> Result<bool> {
        let req = SubsequentRequest {
			iteration_id,
			skip_count,
			take_count,
			context: Some(self.context.clone()),
        };
		let resp = self.client.clone().subsequent_exists(self.contextual_request(req)?).await?;
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
		let resp = self.client.clone().subsequent_multi_get(self.contextual_request(req)?).await?;
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
        timeout_ms: i64,
    ) -> Result<FirstAndLast> {
        let req = GetRangeRequest {
            transaction_id,
            column_id,
            start_keys_inclusive,
            end_keys_exclusive,
			reverse,
			timeout_ms,
			context: Some(self.context.clone()),
        };
		let resp = self.client.clone().reduce_range_first_and_last(
			self.contextual_request_with_timeout(req, Some(timeout_ms))?,
		).await?;
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
        timeout_ms: i64,
    ) -> Result<i64> {
        let req = GetRangeRequest {
            transaction_id,
            column_id,
            start_keys_inclusive,
            end_keys_exclusive,
			reverse,
			timeout_ms,
			context: Some(self.context.clone()),
        };
		let resp = self.client.clone().reduce_range_entries_count(
			self.contextual_request_with_timeout(req, Some(timeout_ms))?,
		).await?;
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
        timeout_ms: i64,
    ) -> Result<impl Stream<Item = Result<Kv>>> {
        let req = GetRangeRequest {
            transaction_id,
            column_id,
            start_keys_inclusive,
            end_keys_exclusive,
			reverse,
			timeout_ms,
			context: Some(self.context.clone()),
        };
		let resp = self.client.clone().get_all_in_range(
			self.contextual_request_with_timeout(req, Some(timeout_ms))?,
		).await?;
        Ok(resp.into_inner())
    }

    /// Scans the column raw (ignoring transactions, usually).
    pub async fn scan_raw(&self, column_id: i64, shard_index: i32, shard_count: i32) -> Result<impl Stream<Item = Result<KvBatch>>> {
		let req = ScanRawRequest {
			column_id,
			shard_index,
			shard_count,
			context: Some(self.context.clone()),
		};
		let resp = self.client.clone().scan_raw(self.contextual_request(req)?).await?;
        Ok(resp.into_inner().map(|res| {
            match res {
                Ok(batch) => decode_kv_batch(&batch.serialized),
                Err(e) => Err(e),
            }
        }))
    }

    // ============================================================================================
    // Maintenance
    // ============================================================================================

    /// Flushes the database.
    pub async fn flush(&self) -> Result<()> {
        let req = FlushRequest {};
        self.client.clone().flush(req).await?;
        Ok(())
    }

    /// Compacts the database.
    pub async fn compact(&self) -> Result<()> {
        let req = CompactRequest {};
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
            expected_last_committed: None,
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
    ) -> std::result::Result<i64, CdcCreateError> {
        let expected_last_committed = match precondition {
            CdcCreatePrecondition::Unchecked => None,
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
        };
        let resp = self
            .client
            .clone()
            .cdc_create(req)
            .await
            .map_err(CdcCreateError::from)?;
        Ok(resp.into_inner().start_seq)
    }

    /// Deletes a CDC stream.
    pub async fn cdc_delete(&self, id: String) -> Result<()> {
        let req = CdcDeleteRequest { id };
        self.client.clone().cdc_delete(req).await?;
        Ok(())
    }

    /// Returns the earliest CDC cursor still available in the database WAL.
    pub async fn cdc_get_earliest_available_sequence(&self) -> Result<i64> {
        let resp = self
            .client
            .clone()
            .cdc_get_earliest_available_sequence(())
            .await?;
        Ok(resp.into_inner().sequence)
    }

    /// Returns the durable last committed sequence for a CDC subscription.
    ///
    /// `None` means that the subscription metadata does not exist.
    pub async fn cdc_get_last_committed_sequence(&self, id: String) -> Result<Option<i64>> {
        let req = CdcGetLastCommittedSequenceRequest { id };
        let resp = self
            .client
            .clone()
            .cdc_get_last_committed_sequence(req)
            .await?;
        Ok(resp.into_inner().last_committed_seq)
    }

    /// Commits a sequence number for a CDC stream.
    pub async fn cdc_commit(&self, id: String, seq: i64) -> Result<()> {
        let req = CdcCommitRequest { id, seq };
        self.client.clone().cdc_commit(req).await?;
        Ok(())
    }

    /// Commits a CDC sequence while preserving a typed missing-subscription failure.
    pub async fn cdc_commit_checked(
        &self,
        id: String,
        seq: i64,
    ) -> std::result::Result<(), CdcError> {
        let req = CdcCommitRequest { id, seq };
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
            max_response_bytes: 0,
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
            max_response_bytes: 0,
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
            max_response_bytes: 0,
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
            max_response_bytes: 0,
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
        Fut: std::future::Future<Output = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>>,
    {
        let mut seq = options.from_seq;
        let batch_size = if options.batch_size > 0 { options.batch_size } else { 1000 };
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

fn decode_kv_batch(mut buf: &[u8]) -> Result<KvBatch> {
    use std::convert::TryInto;

    let get_u32 = |b: &mut &[u8]| -> Result<u32> {
        if b.len() < 4 { return Err(Status::internal("Buffer too short for u32")); }
        let (int_bytes, rest) = b.split_at(4);
        *b = rest;
        Ok(u32::from_le_bytes(int_bytes.try_into().unwrap()))
    };

    let get_u8 = |b: &mut &[u8]| -> Result<u8> {
        if b.len() < 1 { return Err(Status::internal("Buffer too short for u8")); }
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
            if buf.len() < key_len as usize { return Err(Status::internal("Buffer too short for key")); }
            let (key_bytes, rest) = buf.split_at(key_len as usize);
            buf = rest;
            keys.push(key_bytes.to_vec());
        }

        let val_len = get_u32(&mut buf)?;
        if buf.len() < val_len as usize { return Err(Status::internal("Buffer too short for value")); }
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
    use tonic::transport::Endpoint;

    #[tokio::test]
	async fn client_views_retain_independent_workload_contexts() {
		let channel = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
		let ingest = RockserverClient::new(channel, RequestContext::ingest());
		assert_eq!(ingest.context.profile, 4);

		let analytical = ingest.with_context(RequestContext::analytical());
		assert_eq!(analytical.context.profile, 3);
		assert_eq!(ingest.context.profile, 4);
	}

	#[tokio::test]
	async fn operation_timeout_is_applied_as_the_smaller_tonic_deadline() {
		let channel = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
		let context = RequestContext::for_profile(
			WorkloadProfile::Batch,
			SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64 + 10_000,
		);
		let client = RockserverClient::new(channel, context);
		let request = client.contextual_request_with_timeout((), Some(5)).unwrap();
		assert_eq!(request.metadata().get("grpc-timeout").unwrap(), "5000000n");
	}

	#[tokio::test]
	async fn expired_context_fails_before_transport() {
		let channel = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
		let context = RequestContext {
			profile: 2,
			deadline_epoch_millis: 1,
		};
		let error = RockserverClient::new(channel, context)
			.get_column_id("never-sent".to_owned())
			.await
			.unwrap_err();
		assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
	}
}
