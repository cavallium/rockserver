use crate::proto::rocks_db_service_client::RocksDbServiceClient;
use crate::proto::*; // For request/response types
use crate::types::*;
use crate::types::{Column, ColumnSchema};
use futures::{Stream, StreamExt};
use tonic::transport::Channel;
use tonic::{Code, Status}; // Disambiguate from proto

pub type Result<T> = std::result::Result<T, Status>;

const ROCKSERVER_ERROR_PREFIX: &str = "RocksDBError: [uid:";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RockserverStatusError {
    pub error_type: String,
    pub message: String,
}

pub trait RockserverStatusExt {
    fn rockserver_error(&self) -> Option<RockserverStatusError>;

    fn is_rockserver_error(&self, error_type: &str) -> bool {
        self.rockserver_error()
            .is_some_and(|error| error.error_type == error_type)
    }
}

impl RockserverStatusExt for Status {
    fn rockserver_error(&self) -> Option<RockserverStatusError> {
        if self.code() != Code::Internal {
            return None;
        }
        parse_rockserver_status_message(self.message())
    }
}

fn parse_rockserver_status_message(message: &str) -> Option<RockserverStatusError> {
    let rest = message.strip_prefix(ROCKSERVER_ERROR_PREFIX)?;
    let close_index = rest.find(']')?;
    let error_type = rest[..close_index].to_string();
    let message = rest[(close_index + 1)..].trim_start().to_string();
    Some(RockserverStatusError {
        error_type,
        message,
    })
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
    client: RocksDbServiceClient<Channel>,
}

impl RockserverClient {
    /// Connect to the RockServer at the specified destination.
    ///
    /// # Arguments
    /// * `dst` - The destination to connect to. Can be a string like `http://[::1]:50051`.
    ///
    /// # Returns
    /// A `Result` containing the connected client or a transport error.
    pub async fn connect<D>(dst: D) -> std::result::Result<Self, tonic::transport::Error>
    where
        D: std::convert::TryInto<tonic::transport::Endpoint>,
        D::Error: Into<tonic::codegen::StdError>,
    {
        let client = RocksDbServiceClient::connect(dst).await?;
        Ok(Self { client })
    }

    /// Create a new client from an existing Tonic `Channel`.
    pub fn new(channel: Channel) -> Self {
        Self {
            client: RocksDbServiceClient::new(channel),
        }
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
        let req = OpenTransactionRequest { timeout_ms };
        let resp = self.client.clone().open_transaction(req).await?;
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
    pub async fn close_transaction(
        &self,
        transaction_id: i64,
        commit: bool,
        timeout_ms: i64,
    ) -> Result<bool> {
        let req = CloseTransactionRequest {
            transaction_id,
            timeout_ms,
            commit,
        };
        let resp = self.client.clone().close_transaction(req).await?;
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
        };
        let resp = self.client.clone().create_column(req).await?;
        Ok(resp.into_inner().column_id)
    }

    /// Deletes a column by its ID.
    pub async fn delete_column(&self, column_id: i64) -> Result<()> {
        let req = DeleteColumnRequest { column_id };
        self.client.clone().delete_column(req).await?;
        Ok(())
    }

    /// Retrieves the ID of a column by its name.
    pub async fn get_column_id(&self, name: String) -> Result<i64> {
        let req = GetColumnIdRequest { name };
        let resp = self.client.clone().get_column_id(req).await?;
        Ok(resp.into_inner().column_id)
    }

    /// Retrieves definitions for all existing columns.
    pub async fn get_all_column_definitions(&self) -> Result<Vec<Column>> {
        let req = GetAllColumnDefinitionsRequest {};
        let resp = self.client.clone().get_all_column_definitions(req).await?;
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
        };
        self.client.clone().put(req).await?;
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
        };
        let resp = self.client.clone().put_get_previous(req).await?;
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
        };
        let resp = self.client.clone().put_get_delta(req).await?;
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
        };
        let resp = self.client.clone().put_get_changed(req).await?;
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
        };
        let resp = self.client.clone().put_get_previous_presence(req).await?;
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

        self.client.clone().put_batch(request_stream).await?;
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

        self.client.clone().merge_batch(request_stream).await?;
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
            }),
            data,
        };
        self.client.clone().put_multi_list(req).await?;
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

        self.client.clone().put_multi(request_stream).await?;
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
            .client
            .clone()
            .put_multi_get_previous(request_stream)
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
            .client
            .clone()
            .put_multi_get_delta(request_stream)
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
            .client
            .clone()
            .put_multi_get_changed(request_stream)
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
            .client
            .clone()
            .put_multi_get_previous_presence(request_stream)
            .await?;
        Ok(resp.into_inner())
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

        self.client.clone().merge_multi(request_stream).await?;
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
            .client
            .clone()
            .merge_multi_get_merged(request_stream)
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
        };
        self.client.clone().merge(req).await?;
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
        };
        let resp = self.client.clone().merge_get_merged(req).await?;
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
        };
        let resp = self.client.clone().get(req).await?;
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
        };
        let resp = self.client.clone().get_for_update(req).await?;
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
        };
        let resp = self.client.clone().exists(req).await?;
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
        };
        let resp = self.client.clone().open_iterator(req).await?;
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
        let req = SeekToRequest { iteration_id, keys };
        self.client.clone().seek_to(req).await?;
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
        };
        self.client.clone().subsequent(req).await?;
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
        };
        let resp = self.client.clone().subsequent_exists(req).await?;
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
        };
        let resp = self.client.clone().subsequent_multi_get(req).await?;
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
        };
        let resp = self.client.clone().reduce_range_first_and_last(req).await?;
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
        };
        let resp = self.client.clone().reduce_range_entries_count(req).await?;
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
        };
        let resp = self.client.clone().get_all_in_range(req).await?;
        Ok(resp.into_inner())
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
        };
        let resp = self.client.clone().scan_raw(req).await?;
        Ok(resp.into_inner().map(|res| match res {
            Ok(batch) => decode_kv_batch(&batch.serialized).map_err(|status| *status),
            Err(e) => Err(e),
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
        };
        let resp = self.client.clone().cdc_create(req).await?;
        Ok(resp.into_inner().start_seq)
    }

    /// Deletes a CDC stream.
    pub async fn cdc_delete(&self, id: String) -> Result<()> {
        let req = CdcDeleteRequest { id };
        self.client.clone().cdc_delete(req).await?;
        Ok(())
    }

    /// Commits a sequence number for a CDC stream.
    pub async fn cdc_commit(&self, id: String, seq: i64) -> Result<()> {
        let req = CdcCommitRequest { id, seq };
        self.client.clone().cdc_commit(req).await?;
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
        };
        let resp = self.client.clone().cdc_poll(req).await?;
        Ok(resp.into_inner())
    }

    /// Polls for a CDC batch, including the next cursor even when filters emit no events.
    pub async fn cdc_poll_batch(
        &self,
        id: String,
        from_seq: Option<i64>,
        max_events: i64,
    ) -> Result<CdcPollBatchResponse> {
        let req = CdcPollRequest {
            id,
            from_seq,
            max_events,
        };
        let resp = self.client.clone().cdc_poll_batch(req).await?;
        Ok(resp.into_inner())
    }

    async fn cdc_commit_with_retry(
        &self,
        id: String,
        seq: i64,
        retry_base_delay: std::time::Duration,
    ) -> Result<()> {
        let mut retry_delay = retry_base_delay;
        loop {
            match self.cdc_commit(id.clone(), seq).await {
                Ok(()) => return Ok(()),
                Err(e) if is_retryable_cdc_error(&e) => {
                    tokio::time::sleep(retry_delay).await;
                    retry_delay = next_cdc_retry_delay(retry_delay);
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// High-level method to stream CDC events continuously.
    ///
    /// This method manages polling, transient transport retries, and offset commits based on the provided options.
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
        let retry_base_delay = normalize_cdc_retry_delay(idle_delay);
        let mut retry_delay = retry_base_delay;

        loop {
            let batch_result = self.cdc_poll_batch(id.clone(), seq, batch_size).await;

            let batch = match batch_result {
                Ok(batch) => batch,
                Err(e) if is_retryable_cdc_error(&e) => {
                    tokio::time::sleep(retry_delay).await;
                    retry_delay = next_cdc_retry_delay(retry_delay);
                    continue;
                }
                Err(e) => return Err(e),
            };
            retry_delay = retry_base_delay;

            let events = batch.events;
            seq = Some(batch.next_seq);

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
                    self.cdc_commit_with_retry(id.clone(), event.seq, retry_base_delay)
                        .await?;
                }
            }

            if options.commit_mode == CdcCommitMode::Batch {
                self.cdc_commit_with_retry(id.clone(), last_seq, retry_base_delay)
                    .await?;
            }
        }
    }
}

fn normalize_cdc_retry_delay(delay: std::time::Duration) -> std::time::Duration {
    if delay.is_zero() {
        std::time::Duration::from_millis(100)
    } else {
        delay
    }
}

fn next_cdc_retry_delay(delay: std::time::Duration) -> std::time::Duration {
    delay
        .saturating_mul(2)
        .min(std::time::Duration::from_secs(30))
}

fn is_retryable_cdc_error(status: &Status) -> bool {
    matches!(
        status.code(),
        Code::Unavailable
            | Code::DeadlineExceeded
            | Code::ResourceExhausted
            | Code::Aborted
            | Code::Unknown
    )
}

fn decode_kv_batch(mut buf: &[u8]) -> std::result::Result<KvBatch, Box<Status>> {
    use std::convert::TryInto;

    let get_u32 = |b: &mut &[u8]| -> std::result::Result<u32, Box<Status>> {
        if b.len() < 4 {
            return Err(Box::new(Status::internal("Buffer too short for u32")));
        }
        let (int_bytes, rest) = b.split_at(4);
        *b = rest;
        Ok(u32::from_le_bytes(int_bytes.try_into().unwrap()))
    };

    let get_u8 = |b: &mut &[u8]| -> std::result::Result<u8, Box<Status>> {
        if b.is_empty() {
            return Err(Box::new(Status::internal("Buffer too short for u8")));
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
                return Err(Box::new(Status::internal("Buffer too short for key")));
            }
            let (key_bytes, rest) = buf.split_at(key_len as usize);
            buf = rest;
            keys.push(key_bytes.to_vec());
        }

        let val_len = get_u32(&mut buf)?;
        if buf.len() < val_len as usize {
            return Err(Box::new(Status::internal("Buffer too short for value")));
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
