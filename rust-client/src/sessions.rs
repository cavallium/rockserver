//! Writes that take effect at most once on a replicated server, across lost connections and leader changes,
//! through a client session (Raft dissertation, section 6.3). The Rust counterpart of the Java client's
//! `SessionWrites`: same numbering, same resend rule, same answers.
//!
//! The session is opened on the first numbered write. Each write gets the next number of the session and keeps it:
//! when its outcome is unknown (the call was lost, or the server answered `OUTCOME_UNKNOWN`), the same numbered
//! write is sent again, after a short pause, until the caller's deadline (the retry window for a caller without
//! one). The server applies a numbered write at most once and answers every copy with the first result. The caller
//! gets `OUTCOME_UNKNOWN` only when the deadline came before an answer settled it.
//!
//! A caller that drops the future of a write stops it: no copy is sent any more, and its number is final. If a
//! copy was sent, the write may have been applied; dropping cannot tell which.

use crate::proto::{ClientRequestId, KvBatch};
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::fmt::Display;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;
use tonic::metadata::{MetadataMap, MetadataValue};
use tonic::{Code, Status};

/// Trailer a replicated server sets on a failed call: `no` when the failure says that this copy was not applied.
pub const APPLIED_METADATA_KEY: &str = "rockserver-applied";
/// Value of [`APPLIED_METADATA_KEY`] for a copy that was not applied.
pub const NOT_APPLIED: &str = "no";
/// How long a write of a caller without a deadline is sent again after an unknown outcome, by default.
pub const DEFAULT_RETRY_WINDOW: Duration = Duration::from_secs(30);
/// Bytes of keys and values per batch chunk, as the server's `KVBatchChunks`.
pub const CHUNK_BYTES: u64 = 4 * 1024 * 1024;
/// Overhead counted per key segment and per value in a chunk, as the server's `KVBatchChunks`.
pub const ITEM_OVERHEAD_BYTES: u64 = 16;

const FIRST_PAUSE: Duration = Duration::from_millis(50);
const MAX_PAUSE: Duration = Duration::from_secs(1);
/// How long an update id is remembered: longer than any update lease.
const UPDATE_MEMORY: Duration = Duration::from_secs(600);
const MAX_REMEMBERED_UPDATES: usize = 1 << 16;
const ERROR_PREFIX: &str = "RocksDBError: [uid:";

/// The Rockserver error type a failed call carries in its message (`RocksDBError: [uid:TYPE] ...`), if any.
pub fn rocksdb_error_type(status: &Status) -> Option<&str> {
    let rest = status.message().strip_prefix(ERROR_PREFIX)?;
    let end = rest.find(']')?;
    Some(&rest[..end])
}

/// True if the server says that this copy of the write was not applied (the `rockserver-applied: no` trailer).
pub fn is_not_applied(status: &Status) -> bool {
    status
        .metadata()
        .get(APPLIED_METADATA_KEY)
        .and_then(|value| value.to_str().ok())
        == Some(NOT_APPLIED)
}

/// True if the write may have been applied and the answer does not say: a lost call, or `OUTCOME_UNKNOWN`. A
/// numbered write that failed so can be sent again with the same number to learn its outcome.
pub fn is_outcome_unknown(status: &Status) -> bool {
    match rocksdb_error_type(status) {
        Some("OUTCOME_UNKNOWN") => true,
        // A deadline is unknown too, unless the server says the write was not applied.
        Some("READ_DEADLINE_EXCEEDED") => !is_not_applied(status),
        Some(_) => false,
        // Not an answer of Rockserver: the transport lost the call (UNAVAILABLE, CANCELLED when a stopping server
        // resets it, DEADLINE_EXCEEDED of the transport, UNKNOWN for a broken stream).
        None => matches!(
            status.code(),
            Code::Unavailable | Code::Cancelled | Code::DeadlineExceeded | Code::Unknown
        ),
    }
}

/// The server says this copy was not applied but might be if sent again: overload, or a failure flagged not applied.
fn is_retryable_not_applied(status: &Status) -> bool {
    match rocksdb_error_type(status) {
        Some("UPDATE_RETRY") => false,
        Some("SERVER_OVERLOADED") => true,
        _ => is_not_applied(status),
    }
}

fn rocks_status(code: Code, error_type: &str, message: impl Display, not_applied: bool) -> Status {
    let message = format!("{ERROR_PREFIX}{error_type}] {message}");
    if not_applied {
        let mut metadata = MetadataMap::new();
        metadata.insert(APPLIED_METADATA_KEY, MetadataValue::from_static(NOT_APPLIED));
        Status::with_metadata(code, message, metadata)
    } else {
        Status::new(code, message)
    }
}

fn unknown_outcome(cause: &Status) -> Status {
    rocks_status(
        Code::Unknown,
        "OUTCOME_UNKNOWN",
        format_args!(
            "The write was sent, and no answer settled its outcome before its deadline: {}",
            cause.message()
        ),
        false,
    )
}

fn not_sent(cause: impl Display) -> Status {
    rocks_status(
        Code::ResourceExhausted,
        "SERVER_OVERLOADED",
        format_args!("Could not open a client session; the write was not sent: {cause}"),
        true,
    )
}

pub(crate) fn deadline_before_sent() -> Status {
    rocks_status(
        Code::DeadlineExceeded,
        "READ_DEADLINE_EXCEEDED",
        "The request deadline expired before the write was sent",
        true,
    )
}

/// The same failure, flagged not applied.
fn flagged_not_applied(status: &Status) -> Status {
    if is_not_applied(status) {
        return status.clone();
    }
    let mut metadata = status.metadata().clone();
    metadata.insert(APPLIED_METADATA_KEY, MetadataValue::from_static(NOT_APPLIED));
    Status::with_metadata(status.code(), status.message().to_owned(), metadata)
}

/// The numbering of one session.
struct Session {
    id: i64,
    numbers: Mutex<Numbers>,
}

#[derive(Default)]
struct Numbers {
    next: i64,
    in_flight: BTreeSet<i64>,
}

impl Session {
    fn allocate(self: &Arc<Self>) -> Number {
        let mut numbers = self.numbers.lock().unwrap();
        let sequence = numbers.next;
        numbers.next += 1;
        numbers.in_flight.insert(sequence);
        Number {
            session: self.clone(),
            sequence,
        }
    }

    fn completed_below(&self) -> i64 {
        let numbers = self.numbers.lock().unwrap();
        numbers
            .in_flight
            .first()
            .copied()
            .unwrap_or(numbers.next)
    }
}

/// The number of a write, final once dropped: the write has its answer, or the caller dropped it.
struct Number {
    session: Arc<Session>,
    sequence: i64,
}

impl Drop for Number {
    fn drop(&mut self) {
        self.session
            .numbers
            .lock()
            .unwrap()
            .in_flight
            .remove(&self.sequence);
    }
}

/// Update ids returned by `get_for_update`, remembered so that the write ending the update is numbered.
#[derive(Default)]
struct Updates {
    /// Id and the time it is forgotten, oldest first; an entry whose time differs from `ids` is stale.
    order: VecDeque<(i64, Instant)>,
    ids: HashMap<i64, Instant>,
    forgotten: u64,
}

/// The session state shared by every view of one client.
#[derive(Default)]
pub(crate) struct SessionWrites {
    /// The current session; `None` before the first write and after it expired. Held while one is opened.
    session: tokio::sync::Mutex<Option<Arc<Session>>>,
    updates: Mutex<Updates>,
    numbered_batches: bool,
    /// Copies sent again after an unknown outcome, over the life of this client.
    resends: std::sync::atomic::AtomicU64,
}

impl std::fmt::Debug for SessionWrites {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SessionWrites")
            .field("numbered_batches", &self.numbered_batches)
            .finish_non_exhaustive()
    }
}

fn can_pause(deadline: Instant, pause: Duration) -> bool {
    // Room for the pause and a copy that can still reach the server.
    deadline.saturating_duration_since(Instant::now()) > pause + FIRST_PAUSE
}

/// The timeout of a copy sent now: what is left of a finite deadline (`None` once it passed), or no timeout.
fn remaining(deadline: Instant, finite: bool) -> Option<Option<Duration>> {
    if !finite {
        return Some(None);
    }
    let left = deadline.saturating_duration_since(Instant::now());
    if left.is_zero() {
        None
    } else {
        Some(Some(left))
    }
}

/// Awaits a copy or an opening. A caller with a deadline sends it with what is left as its gRPC timeout, so the
/// transport ends it. A caller without one sends it with no timeout, so that the server still sees a caller without
/// a deadline. Nothing else ends it then: `connect` sets no keepalive, so a silent node (power loss, SIGSTOP, a
/// partition with the host up) would hold it forever. The window ends it here instead, with `None`.
async fn within_window<F: Future>(deadline: Instant, finite: bool, call: F) -> Option<F::Output> {
    if finite {
        Some(call.await)
    } else {
        tokio::time::timeout_at(deadline, call).await.ok()
    }
}

impl SessionWrites {
    pub(crate) fn new(numbered_batches: bool) -> Self {
        Self {
            numbered_batches,
            ..Self::default()
        }
    }

    pub(crate) fn numbered_batches(&self) -> bool {
        self.numbered_batches
    }

    /// Copies sent again after an unknown outcome, over the life of this client.
    pub(crate) fn resends(&self) -> u64 {
        self.resends.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// True if a write with this transaction or update id is numbered: outside transactions, or the write that ends
    /// an update this client opened. A write staged in a transaction is not: sending it again could stage it twice.
    pub(crate) fn numbers(&self, transaction_or_update_id: i64) -> bool {
        transaction_or_update_id == 0
            || self
                .updates
                .lock()
                .unwrap()
                .ids
                .contains_key(&transaction_or_update_id)
    }

    /// Remembers an update a `get_for_update` outside a transaction opened.
    pub(crate) fn update_opened(&self, update_id: i64) {
        if update_id == 0 {
            return;
        }
        let now = Instant::now();
        let mut updates = self.updates.lock().unwrap();
        while let Some(&(id, forget_at)) = updates.order.front() {
            if updates.ids.get(&id) != Some(&forget_at) {
                updates.order.pop_front();
            } else if forget_at <= now {
                updates.order.pop_front();
                updates.ids.remove(&id);
            } else {
                break;
            }
        }
        if updates.ids.len() >= MAX_REMEMBERED_UPDATES && !updates.ids.contains_key(&update_id) {
            // The oldest is forgotten: the write that ends it is sent unnumbered, once.
            while let Some((id, forget_at)) = updates.order.pop_front() {
                if updates.ids.get(&id) == Some(&forget_at) {
                    updates.ids.remove(&id);
                    updates.forgotten += 1;
                    break;
                }
            }
        }
        let forget_at = now + UPDATE_MEMORY;
        updates.ids.insert(update_id, forget_at);
        updates.order.push_back((update_id, forget_at));
    }

    /// Forgets an update: it ended (its final write has an answer, or it was closed as failed).
    pub(crate) fn update_ended(&self, update_id: i64) {
        if update_id != 0 {
            self.updates.lock().unwrap().ids.remove(&update_id);
        }
    }

    /// Writes sharing session `id`, already open (tests that send without a server).
    #[cfg(test)]
    pub(crate) fn with_open_session(numbered_batches: bool, id: i64) -> Self {
        let writes = Self::new(numbered_batches);
        *writes.session.try_lock().unwrap() = Some(Arc::new(Session {
            id,
            numbers: Mutex::new(Numbers::default()),
        }));
        writes
    }

    #[cfg(test)]
    fn forgotten_updates(&self) -> u64 {
        self.updates.lock().unwrap().forgotten
    }

    /// The current session, opened if there is none. Opening is retried, within this write's deadline, after an
    /// unknown outcome or a "not applied" answer: opening twice only leaves an unused session, which expires.
    async fn session<O, OF>(&self, deadline: Instant, finite: bool, open: &mut O) -> Result<Arc<Session>, Status>
    where
        O: FnMut(Option<Duration>) -> OF,
        OF: Future<Output = Result<i64, Status>>,
    {
        // Writes wait for the one opening a session; if it fails, the next one opens again, in its own time.
        let mut slot = tokio::time::timeout_at(deadline, self.session.lock())
            .await
            .map_err(|_| not_sent("the deadline came while another write was opening the session"))?;
        if let Some(session) = slot.as_ref() {
            return Ok(session.clone());
        }
        let mut pause = FIRST_PAUSE;
        loop {
            let Some(timeout) = remaining(deadline, finite) else {
                return Err(not_sent("the deadline came before a session was opened"));
            };
            let Some(opened) = within_window(deadline, finite, open(timeout)).await else {
                return Err(not_sent("no answer to the opening of a session before the retry window ended"));
            };
            match opened {
                Ok(id) => {
                    let session = Arc::new(Session {
                        id,
                        numbers: Mutex::new(Numbers::default()),
                    });
                    *slot = Some(session.clone());
                    return Ok(session);
                }
                Err(status) => {
                    if (is_outcome_unknown(&status) || is_retryable_not_applied(&status))
                        && can_pause(deadline, pause)
                    {
                        tokio::time::sleep(pause).await;
                        pause = (pause * 2).min(MAX_PAUSE);
                        continue;
                    }
                    // Not sent: whatever the opening's own outcome, the write was not applied.
                    return Err(match rocksdb_error_type(&status) {
                        Some(kind) if kind != "OUTCOME_UNKNOWN" => flagged_not_applied(&status),
                        _ => not_sent(status.message()),
                    });
                }
            }
        }
    }

    async fn session_expired(&self, expired: &Arc<Session>) {
        let mut slot = self.session.lock().await;
        if slot
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, expired))
        {
            *slot = None;
        }
    }

    /// Sends one numbered write, again after unknown outcomes, until `deadline`. `finite` tells whether the caller
    /// has a deadline (copies then carry what is left of it) or only the retry window (copies carry none). `open`
    /// opens a session with a timeout; `send` sends one copy with its request id and timeout.
    pub(crate) async fn run<R, O, OF, S, SF>(
        &self,
        deadline: Instant,
        finite: bool,
        mut open: O,
        mut send: S,
    ) -> Result<R, Status>
    where
        O: FnMut(Option<Duration>) -> OF,
        OF: Future<Output = Result<i64, Status>>,
        S: FnMut(ClientRequestId, Option<Duration>) -> SF,
        SF: Future<Output = Result<R, Status>>,
    {
        let mut pause = FIRST_PAUSE;
        let mut reissues = 0u32;
        // A copy was sent and its outcome is unknown: no "not applied" answer can end the write any more.
        let mut maybe_applied = false;
        'numbering: loop {
            let session = self.session(deadline, finite, &mut open).await?;
            let number = session.allocate();
            loop {
                let Some(timeout) = remaining(deadline, finite) else {
                    return Err(if maybe_applied {
                        unknown_outcome(&Status::deadline_exceeded("the deadline came"))
                    } else {
                        deadline_before_sent()
                    });
                };
                let id = ClientRequestId {
                    session_id: session.id,
                    sequence: number.sequence,
                    completed_below: session.completed_below(),
                };
                if maybe_applied {
                    self.resends.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                let status = match within_window(deadline, finite, send(id, timeout)).await {
                    Some(Ok(value)) => return Ok(value),
                    Some(Err(status)) => status,
                    // The copy may have been applied; the window has no room for another.
                    None => {
                        return Err(unknown_outcome(&Status::deadline_exceeded(
                            "no answer to the copy before the retry window ended",
                        )))
                    }
                };
                if rocksdb_error_type(&status) == Some("SESSION_EXPIRED") {
                    // This copy was refused before it ran. Copies sent earlier are this client's to judge.
                    drop(number);
                    self.session_expired(&session).await;
                    if maybe_applied {
                        return Err(unknown_outcome(&status));
                    }
                    reissues += 1;
                    if reissues == 1 {
                        // The first copy, not applied: sent as a new write.
                        continue 'numbering;
                    }
                    if can_pause(deadline, pause) {
                        // The new session expired too: not in a tight loop.
                        tokio::time::sleep(pause).await;
                        pause = (pause * 2).min(MAX_PAUSE);
                        continue 'numbering;
                    }
                    // Not applied, and no time left to send it again.
                    return Err(flagged_not_applied(&status));
                }
                if !is_outcome_unknown(&status)
                    && !(maybe_applied && is_retryable_not_applied(&status))
                {
                    // The answer: a result recorded by the server, or a failure that says this write was not applied.
                    return Err(status);
                }
                maybe_applied = true;
                if !can_pause(deadline, pause) {
                    return Err(unknown_outcome(&status));
                }
                tokio::time::sleep(pause).await;
                pause = (pause * 2).min(MAX_PAUSE);
            }
        }
    }
}

/// Bytes a numbered streamed multi write is read whole up to (`put_multi`, `delete_multi`, `merge_multi` and their
/// variants with results), counted as [`multi_kv_bytes`] / [`multi_keys_bytes`] do. A larger stream is sent
/// unnumbered, streamed as before numbering, so that its size has no limit and the client does not hold it: such a
/// write is sent once, and a lost call leaves its outcome unknown. The server's own limit for a numbered multi write
/// (16 MiB, `GrpcServerImpl.NUMBERED_MULTI_MAX_BYTES`, counted the same way) is four times this.
pub const NUMBERED_MULTI_MAX_BYTES: u64 = CHUNK_BYTES;
/// Heap cost the server counts per item of a numbered multi write besides its bytes
/// (`GrpcServerImpl.NUMBERED_MULTI_ITEM_HEAP_BYTES`).
pub const MULTI_ITEM_HEAP_BYTES: u64 = 96;

/// Counted bytes of one item of a numbered `put_multi` / `merge_multi`, as the server counts them.
pub fn multi_kv_bytes(item: &crate::proto::Kv) -> u64 {
    item_size(item) + MULTI_ITEM_HEAP_BYTES
}

/// Counted bytes of one key of a numbered `delete_multi`, as the server counts them.
pub fn multi_keys_bytes(keys: &Vec<Vec<u8>>) -> u64 {
    keys.iter()
        .map(|key| key.len() as u64 + ITEM_OVERHEAD_BYTES)
        .sum::<u64>()
        .max(ITEM_OVERHEAD_BYTES)
        + MULTI_ITEM_HEAP_BYTES
}

/// A streamed multi write's items, read for numbering.
pub(crate) enum MultiItems<T> {
    /// The whole stream, within [`NUMBERED_MULTI_MAX_BYTES`]: sent as one numbered write.
    Whole(Vec<T>),
    /// Larger: the items read so far followed by the rest of the stream, to send unnumbered.
    Streamed(futures::stream::BoxStream<'static, T>),
}

/// Reads `items` up to `limit` counted bytes: all of them, or the stream again once it is larger.
pub(crate) async fn read_multi_items<T: Send + 'static>(
    items: impl futures::Stream<Item = T> + Send + 'static,
    size: fn(&T) -> u64,
    limit: u64,
) -> MultiItems<T> {
    use futures::StreamExt;
    let mut items = items.boxed();
    let mut read = Vec::new();
    let mut bytes = 0u64;
    while let Some(item) = items.next().await {
        bytes = bytes.saturating_add(size(&item));
        read.push(item);
        if bytes > limit {
            return MultiItems::Streamed(futures::stream::iter(read).chain(items).boxed());
        }
    }
    MultiItems::Whole(read)
}

fn item_size(item: &crate::proto::Kv) -> u64 {
    item.keys
        .iter()
        .map(|key| key.len() as u64 + ITEM_OVERHEAD_BYTES)
        .sum::<u64>()
        + item.value.len() as u64
        + ITEM_OVERHEAD_BYTES
}

/// Splits a batch into the chunks a replicated server writes as one log entry each, in order, as the server's
/// `KVBatchChunks` does: greedy, a chunk ends before the item that would take it over [`CHUNK_BYTES`], counting
/// [`ITEM_OVERHEAD_BYTES`] per key segment and per value; an item larger than that is a chunk of its own. An empty
/// batch has no chunks.
pub fn split_batch(batch: KvBatch) -> Vec<KvBatch> {
    let mut chunks = Vec::new();
    let mut current = Vec::new();
    let mut bytes = 0u64;
    for item in batch.entries {
        let size = item_size(&item);
        if !current.is_empty() && bytes + size > CHUNK_BYTES {
            chunks.push(KvBatch {
                entries: std::mem::take(&mut current),
            });
            bytes = 0;
        }
        bytes += size;
        current.push(item);
    }
    if !current.is_empty() {
        chunks.push(KvBatch { entries: current });
    }
    chunks
}

/// The failure of a batch call some of whose chunks were applied: not safe to resend as a whole.
pub(crate) fn partly_applied(merge: bool, column_id: i64, chunks: u64, keys: u64, cause: &Status) -> Status {
    rocks_status(
        Code::InvalidArgument,
        "INVALID_REQUEST",
        format_args!(
            "{} on column {column_id} was partly applied: {chunks} chunks ({keys} keys) were applied before it \
             failed, so it is not safe to resend as a whole: {}",
            if merge { "mergeBatch" } else { "putBatch" },
            cause.message()
        ),
        false,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::Kv;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn rocks(code: Code, kind: &str, not_applied: bool) -> Status {
        rocks_status(code, kind, "test", not_applied)
    }

    fn opener(
        ids: Arc<Mutex<Vec<Result<i64, Status>>>>,
    ) -> impl FnMut(Option<Duration>) -> std::future::Ready<Result<i64, Status>> {
        move |_| {
            let mut ids = ids.lock().unwrap();
            std::future::ready(if ids.len() > 1 { ids.remove(0) } else { ids[0].clone() })
        }
    }

    fn fixed_opener(id: i64) -> impl FnMut(Option<Duration>) -> std::future::Ready<Result<i64, Status>> {
        move |_| std::future::ready(Ok(id))
    }

    /// A sender answering from a script, recording every id it was sent.
    fn script(
        answers: Vec<Result<&'static str, Status>>,
        sent: Arc<Mutex<Vec<ClientRequestId>>>,
    ) -> impl FnMut(ClientRequestId, Option<Duration>) -> std::future::Ready<Result<&'static str, Status>> {
        let answers = Arc::new(Mutex::new(answers));
        move |id, _| {
            sent.lock().unwrap().push(id);
            let mut answers = answers.lock().unwrap();
            std::future::ready(answers.remove(0))
        }
    }

    fn soon(millis: u64) -> Instant {
        Instant::now() + Duration::from_millis(millis)
    }

    #[test]
    fn classifies_answers_like_the_java_client() {
        assert!(is_outcome_unknown(&rocks(Code::Unknown, "OUTCOME_UNKNOWN", false)));
        assert!(is_outcome_unknown(&Status::unavailable("gone")));
        assert!(is_outcome_unknown(&Status::cancelled("reset")));
        assert!(is_outcome_unknown(&Status::deadline_exceeded("transport")));
        assert!(is_outcome_unknown(&rocks(Code::DeadlineExceeded, "READ_DEADLINE_EXCEEDED", false)));
        assert!(!is_outcome_unknown(&rocks(Code::DeadlineExceeded, "READ_DEADLINE_EXCEEDED", true)));
        assert!(!is_outcome_unknown(&rocks(Code::InvalidArgument, "INVALID_REQUEST", false)));
        assert!(!is_outcome_unknown(&rocks(Code::ResourceExhausted, "SERVER_OVERLOADED", false)));
        assert!(is_retryable_not_applied(&rocks(Code::ResourceExhausted, "SERVER_OVERLOADED", false)));
        assert!(is_retryable_not_applied(&rocks(Code::DeadlineExceeded, "READ_DEADLINE_EXCEEDED", true)));
        assert!(!is_retryable_not_applied(&rocks(Code::Internal, "UPDATE_RETRY", true)));
        assert_eq!(
            rocksdb_error_type(&rocks(Code::FailedPrecondition, "SESSION_EXPIRED", false)),
            Some("SESSION_EXPIRED")
        );
        assert_eq!(rocksdb_error_type(&Status::internal("RocksDBError: [uid:")), None);
    }

    #[tokio::test]
    async fn a_write_keeps_its_number_across_unknown_outcomes() {
        let writes = SessionWrites::new(true);
        let sent = Arc::new(Mutex::new(Vec::new()));
        let result = writes
            .run(
                soon(5_000),
                true,
                fixed_opener(7),
                script(
                    vec![
                        Err(Status::unavailable("lost")),
                        Err(rocks(Code::Unknown, "OUTCOME_UNKNOWN", false)),
                        // After an unknown outcome, "not applied" says nothing about the earlier copies.
                        Err(rocks(Code::ResourceExhausted, "SERVER_OVERLOADED", false)),
                        Ok("done"),
                    ],
                    sent.clone(),
                ),
            )
            .await;
        assert_eq!(result.unwrap(), "done");
        let sent = sent.lock().unwrap();
        assert_eq!(sent.len(), 4);
        assert!(sent.iter().all(|id| id.session_id == 7 && id.sequence == 0));
        assert_eq!(writes.resends(), 3, "every copy after the first is a resend");
    }

    #[tokio::test]
    async fn numbers_follow_each_other_and_completed_below_tracks_the_oldest_unfinished() {
        let writes = Arc::new(SessionWrites::new(true));
        let (release, wait) = tokio::sync::oneshot::channel::<()>();
        let first = {
            let writes = writes.clone();
            tokio::spawn(async move {
                let mut wait = Some(wait);
                writes
                    .run(soon(5_000), true, fixed_opener(1), move |id, _| {
                        let wait = wait.take();
                        async move {
                            if let Some(wait) = wait {
                                let _ = wait.await;
                            }
                            Ok::<_, Status>(id)
                        }
                    })
                    .await
            })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        let second = writes
            .run(soon(5_000), true, fixed_opener(1), |id, _| async move { Ok::<_, Status>(id) })
            .await
            .unwrap();
        assert_eq!(second.sequence, 1);
        assert_eq!(second.completed_below, 0, "number 0 is still in flight");
        release.send(()).unwrap();
        assert_eq!(first.await.unwrap().unwrap().sequence, 0);
        let third = writes
            .run(soon(5_000), true, fixed_opener(1), |id, _| async move { Ok::<_, Status>(id) })
            .await
            .unwrap();
        assert_eq!((third.sequence, third.completed_below), (2, 2));
    }

    #[tokio::test]
    async fn a_failure_that_answers_ends_the_write_without_a_copy() {
        let writes = SessionWrites::new(true);
        let sent = Arc::new(Mutex::new(Vec::new()));
        let error = writes
            .run(
                soon(5_000),
                true,
                fixed_opener(1),
                script(vec![Err(rocks(Code::InvalidArgument, "INVALID_REQUEST", false))], sent.clone()),
            )
            .await
            .unwrap_err();
        assert_eq!(rocksdb_error_type(&error), Some("INVALID_REQUEST"));
        assert_eq!(sent.lock().unwrap().len(), 1);
        // Overload on a first copy: not applied, the caller's to resend.
        let error = writes
            .run(
                soon(5_000),
                true,
                fixed_opener(1),
                script(vec![Err(rocks(Code::ResourceExhausted, "SERVER_OVERLOADED", false))], sent.clone()),
            )
            .await
            .unwrap_err();
        assert_eq!(rocksdb_error_type(&error), Some("SERVER_OVERLOADED"));
        assert_eq!(sent.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn the_deadline_ends_an_unknown_write_as_outcome_unknown() {
        let writes = SessionWrites::new(true);
        let copies = Arc::new(AtomicUsize::new(0));
        let counted = copies.clone();
        let error = writes
            .run(soon(300), true, fixed_opener(1), move |_, timeout| {
                counted.fetch_add(1, Ordering::SeqCst);
                assert!(timeout.unwrap() <= Duration::from_millis(300));
                std::future::ready(Err::<(), _>(Status::unavailable("lost")))
            })
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::Unknown);
        assert_eq!(rocksdb_error_type(&error), Some("OUTCOME_UNKNOWN"));
        assert!(!is_not_applied(&error));
        let copies = copies.load(Ordering::SeqCst);
        assert!((2..=6).contains(&copies), "paused between copies: {copies}");
    }

    #[tokio::test]
    async fn a_first_copy_refused_for_an_expired_session_goes_again_in_a_new_session() {
        let writes = SessionWrites::new(true);
        let sent = Arc::new(Mutex::new(Vec::new()));
        let sessions = Arc::new(Mutex::new(vec![Ok(1), Ok(2)]));
        let result = writes
            .run(
                soon(5_000),
                true,
                opener(sessions),
                script(
                    vec![Err(rocks(Code::FailedPrecondition, "SESSION_EXPIRED", true)), Ok("done")],
                    sent.clone(),
                ),
            )
            .await;
        assert_eq!(result.unwrap(), "done");
        let sent = sent.lock().unwrap();
        assert_eq!((sent[0].session_id, sent[1].session_id), (1, 2));
        assert_eq!(sent[1].sequence, 0);
    }

    #[tokio::test]
    async fn an_expired_session_after_an_unknown_outcome_is_outcome_unknown() {
        let writes = SessionWrites::new(true);
        let sent = Arc::new(Mutex::new(Vec::new()));
        let error = writes
            .run(
                soon(5_000),
                true,
                fixed_opener(1),
                script(
                    vec![
                        Err(Status::unavailable("lost")),
                        Err(rocks(Code::FailedPrecondition, "SESSION_EXPIRED", false)),
                    ],
                    sent.clone(),
                ),
            )
            .await
            .unwrap_err();
        assert_eq!(rocksdb_error_type(&error), Some("OUTCOME_UNKNOWN"));
        assert_eq!(sent.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn sessions_that_keep_expiring_end_flagged_not_applied_at_the_deadline() {
        let writes = SessionWrites::new(true);
        let copies = Arc::new(AtomicUsize::new(0));
        let counted = copies.clone();
        let error = writes
            .run(soon(400), true, fixed_opener(1), move |_, _| {
                counted.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Err::<(), _>(rocks(Code::FailedPrecondition, "SESSION_EXPIRED", false)))
            })
            .await
            .unwrap_err();
        assert_eq!(rocksdb_error_type(&error), Some("SESSION_EXPIRED"));
        assert!(is_not_applied(&error));
        let copies = copies.load(Ordering::SeqCst);
        assert!((2..=8).contains(&copies), "not a tight loop: {copies}");
    }

    #[tokio::test]
    async fn opening_is_retried_and_a_refusal_ends_the_write_unsent() {
        let writes = SessionWrites::new(true);
        let sessions = Arc::new(Mutex::new(vec![Err(Status::unavailable("lost")), Ok(9)]));
        let sent = Arc::new(Mutex::new(Vec::new()));
        let result = writes
            .run(soon(5_000), true, opener(sessions), script(vec![Ok("done")], sent.clone()))
            .await;
        assert_eq!(result.unwrap(), "done");
        assert_eq!(sent.lock().unwrap()[0].session_id, 9);

        let refused = SessionWrites::new(true);
        let error = refused
            .run(
                soon(5_000),
                true,
                |_| std::future::ready(Err::<i64, _>(Status::unimplemented("no sessions"))),
                script(vec![Ok("never")], sent.clone()),
            )
            .await
            .unwrap_err();
        assert_eq!(rocksdb_error_type(&error), Some("SERVER_OVERLOADED"));
        assert!(is_not_applied(&error));
        assert_eq!(sent.lock().unwrap().len(), 1, "nothing sent");
    }

    #[tokio::test]
    async fn a_dropped_write_finishes_its_number() {
        let writes = Arc::new(SessionWrites::new(true));
        let pending = {
            let writes = writes.clone();
            tokio::spawn(async move {
                writes
                    .run(soon(60_000), true, fixed_opener(3), |_, _| {
                        std::future::pending::<Result<(), Status>>()
                    })
                    .await
            })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        pending.abort();
        let _ = pending.await;
        let next = writes
            .run(soon(5_000), true, fixed_opener(3), |id, _| async move { Ok::<_, Status>(id) })
            .await
            .unwrap();
        assert_eq!((next.sequence, next.completed_below), (1, 1));
    }

    #[tokio::test]
    async fn a_caller_without_deadline_sends_copies_without_timeout_within_the_window() {
        let writes = SessionWrites::new(true);
        let error = writes
            .run(soon(200), false, fixed_opener(1), |_, timeout| {
                assert_eq!(timeout, None);
                std::future::ready(Err::<(), _>(Status::unavailable("lost")))
            })
            .await
            .unwrap_err();
        assert_eq!(rocksdb_error_type(&error), Some("OUTCOME_UNKNOWN"));
    }

    #[tokio::test]
    async fn a_copy_that_never_answers_ends_a_caller_without_deadline_at_the_window() {
        // A silent node (power loss, SIGSTOP, a partition with the host up): the copy has no gRPC timeout, and
        // nothing on the channel ends it, so only the window can.
        let writes = SessionWrites::new(true);
        let started = Instant::now();
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            writes.run(soon(1_000), false, fixed_opener(1), |_, timeout| {
                assert_eq!(timeout, None, "the server still sees a caller without a deadline");
                std::future::pending::<Result<(), Status>>()
            }),
        )
        .await
        .expect("the write ended with the window, not hung");
        let error = result.unwrap_err();
        assert_eq!(rocksdb_error_type(&error), Some("OUTCOME_UNKNOWN"));
        assert!(!is_not_applied(&error));
        let took = started.elapsed();
        assert!(took >= Duration::from_millis(900) && took < Duration::from_millis(2_000), "took {took:?}");
    }

    #[tokio::test]
    async fn an_opening_that_never_answers_ends_a_caller_without_deadline_unsent_at_the_window() {
        let writes = SessionWrites::new(true);
        let sent = Arc::new(Mutex::new(Vec::new()));
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            writes.run(
                soon(1_000),
                false,
                |timeout| {
                    assert_eq!(timeout, None);
                    std::future::pending::<Result<i64, Status>>()
                },
                script(vec![Ok("never")], sent.clone()),
            ),
        )
        .await
        .expect("the opening ended with the window, not hung");
        let error = result.unwrap_err();
        assert_eq!(rocksdb_error_type(&error), Some("SERVER_OVERLOADED"));
        assert!(is_not_applied(&error));
        assert!(sent.lock().unwrap().is_empty(), "nothing sent");
    }

    #[tokio::test]
    async fn a_session_opened_after_the_window_ends_a_caller_without_deadline_unsent() {
        // The opening answers, but only once the window has ended (a late lock or a late answer): the copy must
        // not go out, so the write is known not applied, as for a caller with a deadline.
        let writes = SessionWrites::new(true);
        let sent = Arc::new(Mutex::new(Vec::new()));
        let error = writes
            .run(
                soon(50),
                false,
                |_| {
                    std::thread::sleep(Duration::from_millis(150));
                    std::future::ready(Ok(1))
                },
                script(vec![Ok("sent after the window")], sent.clone()),
            )
            .await
            .unwrap_err();
        assert!(sent.lock().unwrap().is_empty(), "no copy sent after the window");
        assert_eq!(rocksdb_error_type(&error), Some("READ_DEADLINE_EXCEEDED"));
        assert!(is_not_applied(&error));
    }

    #[test]
    fn updates_are_remembered_until_they_end() {
        let writes = SessionWrites::new(true);
        assert!(writes.numbers(0));
        assert!(!writes.numbers(42), "a transaction's staged write");
        writes.update_opened(42);
        assert!(writes.numbers(42));
        writes.update_ended(42);
        assert!(!writes.numbers(42));
        writes.update_opened(43);
        writes.update_opened(43);
        writes.update_ended(43);
        assert!(!writes.numbers(43), "re-observing an id does not leave a stale copy behind");
        assert_eq!(writes.forgotten_updates(), 0);
    }

    fn kv(key_len: usize, value_len: usize) -> Kv {
        Kv {
            keys: vec![vec![0; key_len]],
            value: vec![0; value_len],
        }
    }

    #[test]
    fn batches_split_as_the_server_splits_them() {
        assert!(split_batch(KvBatch { entries: vec![] }).is_empty());
        // Two items with an 8-byte key fill one chunk to the byte; the third starts a new one.
        let boundary = (CHUNK_BYTES / 2 - 8 - 2 * ITEM_OVERHEAD_BYTES) as usize;
        let chunks = split_batch(KvBatch {
            entries: vec![kv(8, boundary), kv(8, boundary), kv(8, boundary), kv(8, boundary), kv(8, boundary)],
        });
        assert_eq!(chunks.iter().map(|c| c.entries.len()).collect::<Vec<_>>(), vec![2, 2, 1]);
        let over = split_batch(KvBatch {
            entries: vec![kv(8, boundary + 1), kv(8, boundary)],
        });
        assert_eq!(over.len(), 2);
        // An item larger than a chunk is a chunk of its own.
        let huge = split_batch(KvBatch {
            entries: vec![kv(1, 10), kv(1, (CHUNK_BYTES * 2) as usize), kv(1, 10)],
        });
        assert_eq!(huge.iter().map(|c| c.entries.len()).collect::<Vec<_>>(), vec![1, 1, 1]);
        // Splitting a chunk again gives that chunk.
        for chunk in chunks {
            assert_eq!(split_batch(chunk.clone()), vec![chunk]);
        }
    }

    #[tokio::test]
    async fn a_multi_write_over_the_limit_is_streamed_again_whole_and_in_order() {
        use futures::StreamExt;
        let kv = |k: u8| Kv { keys: vec![vec![k; 8]], value: vec![k] };
        // 8-byte key + 1-byte value + 2 x 16 + 96 = 137 counted bytes per item.
        assert_eq!(multi_kv_bytes(&kv(0)), 137);
        assert_eq!(multi_keys_bytes(&vec![vec![0u8; 8]]), 8 + 16 + 96);
        assert_eq!(multi_keys_bytes(&vec![]), 16 + 96);
        match read_multi_items(futures::stream::iter((0..3).map(kv)), multi_kv_bytes, 3 * 137).await {
            MultiItems::Whole(items) => assert_eq!(items.len(), 3),
            MultiItems::Streamed(_) => panic!("within the limit"),
        }
        let read = Arc::new(AtomicUsize::new(0));
        let counter = read.clone();
        let items = futures::stream::iter(0..10u8).map(move |k| {
            counter.fetch_add(1, Ordering::SeqCst);
            kv(k)
        });
        match read_multi_items(items, multi_kv_bytes, 3 * 137).await {
            MultiItems::Whole(_) => panic!("over the limit"),
            MultiItems::Streamed(rest) => {
                assert_eq!(read.load(Ordering::SeqCst), 4, "stops reading past the limit");
                let keys: Vec<u8> = rest.map(|item| item.keys[0][0]).collect().await;
                assert_eq!(keys, (0..10).collect::<Vec<u8>>());
            }
        }
    }

}
