package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.ReplicationFailureInjector;
import it.cavallium.rockserver.core.cluster.ReplicationFailurePhase;
import it.cavallium.rockserver.core.cluster.command.FailureInjectingReplicatedLogClient;
import it.cavallium.rockserver.core.cluster.command.ReplicatedLogClient;
import it.cavallium.rockserver.core.cluster.command.ReplicatedLogCommit;
import it.cavallium.rockserver.core.cluster.command.ReplicatedMutationApplier;
import it.cavallium.rockserver.core.cluster.command.ReplicatedMutationCodec;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.KVBatch;
import it.cavallium.rockserver.core.common.MergeBatchMode;
import it.cavallium.rockserver.core.common.PutBatchMode;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import reactor.core.publisher.Flux;

class ReplicatedWriteSyncAPITest {

    @Test
    void replicatesPutDeleteAndMergeBeforeAcknowledging() {
        var harness = singleNodeHarness();
        var api = writeApi(harness);
        var keys = new Keys(buf("key"));

        api.put(0, 4, keys, buf("value"), RequestType.none());
        api.delete(0, 4, keys, RequestType.none());
        api.merge(0, 4, keys, buf("delta"), RequestType.none());

        assertEquals(3, harness.recordingApi.commands.size());
        var put = assertInstanceOf(RocksDBAPICommand.RocksDBAPICommandSingle.Put.class,
                harness.recordingApi.commands.getFirst());
        assertEquals(4, put.columnId());
        assertEquals(keys, put.keys());
        assertArrayEquals(bytes("value"), put.value().toByteArray());
        assertEquals(RequestType.RequestTypeId.NOTHING, put.requestType().getRequestTypeId());

        var delete = assertInstanceOf(RocksDBAPICommand.RocksDBAPICommandSingle.Delete.class,
                harness.recordingApi.commands.get(1));
        assertEquals(4, delete.columnId());
        assertEquals(keys, delete.keys());
        assertEquals(RequestType.RequestTypeId.NOTHING, delete.requestType().getRequestTypeId());

        var merge = assertInstanceOf(RocksDBAPICommand.RocksDBAPICommandSingle.Merge.class,
                harness.recordingApi.commands.get(2));
        assertEquals(4, merge.columnId());
        assertEquals(keys, merge.keys());
        assertArrayEquals(bytes("delta"), merge.value().toByteArray());
        assertEquals(RequestType.RequestTypeId.NOTHING, merge.requestType().getRequestTypeId());
    }

    @Test
    void replicatesMultiWritesInOrderBeforeAcknowledging() {
        var harness = singleNodeHarness();
        var api = writeApi(harness);
        var firstKey = new Keys(buf("key-1"));
        var secondKey = new Keys(buf("key-2"));
        var keys = List.of(firstKey, secondKey);

        var putResult = api.putMulti(0, 4, keys, List.of(buf("value-1"), buf("value-2")), RequestType.none());
        var deleteResult = api.deleteMulti(0, 4, keys, RequestType.none());
        var mergeResult = api.mergeMulti(0, 4, keys, List.of(buf("delta-1"), buf("delta-2")), RequestType.none());

        assertEquals(List.of(), putResult);
        assertEquals(List.of(), deleteResult);
        assertEquals(List.of(), mergeResult);
        assertEquals(3, harness.logClient.payloads.size());
        assertEquals(3, harness.recordingApi.commands.size());

        var putMulti = assertInstanceOf(RocksDBAPICommand.RocksDBAPICommandSingle.PutMulti.class,
                harness.recordingApi.commands.getFirst());
        assertEquals(4, putMulti.columnId());
        assertEquals(keys, putMulti.keys());
        assertArrayEquals(bytes("value-1"), assertInstanceOf(Buf.class, putMulti.values().getFirst()).toByteArray());
        assertArrayEquals(bytes("value-2"), assertInstanceOf(Buf.class, putMulti.values().get(1)).toByteArray());
        assertEquals(RequestType.RequestTypeId.NOTHING, putMulti.requestType().getRequestTypeId());

        var deleteMulti = assertInstanceOf(RocksDBAPICommand.RocksDBAPICommandSingle.DeleteMulti.class,
                harness.recordingApi.commands.get(1));
        assertEquals(keys, deleteMulti.keys());
        assertEquals(RequestType.RequestTypeId.NOTHING, deleteMulti.requestType().getRequestTypeId());

        var mergeMulti = assertInstanceOf(RocksDBAPICommand.RocksDBAPICommandSingle.MergeMulti.class,
                harness.recordingApi.commands.get(2));
        assertEquals(keys, mergeMulti.keys());
        assertArrayEquals(bytes("delta-1"), assertInstanceOf(Buf.class, mergeMulti.values().getFirst()).toByteArray());
        assertArrayEquals(bytes("delta-2"), assertInstanceOf(Buf.class, mergeMulti.values().get(1)).toByteArray());
        assertEquals(RequestType.RequestTypeId.NOTHING, mergeMulti.requestType().getRequestTypeId());
    }

    @Test
    void emptyMultiWriteReturnsEmptyResultWithoutProposing() {
        var harness = singleNodeHarness();
        var api = writeApi(harness);

        var result = api.putMulti(0, 4, List.of(), List.of(), RequestType.none());

        assertEquals(List.of(), result);
        assertEquals(0, harness.logClient.payloads.size());
        assertEquals(0, harness.recordingApi.commands.size());
    }

    @Test
    void replicatedWriteApiUsesLogClientAbstraction() {
        var logClient = new RecordingLogClient();
        var api = new LegacyReplicatedWriteSyncFixture(logClient);
        var firstKey = new Keys(buf("key-1"));
        var secondKey = new Keys(buf("key-2"));

        var result = api.putMulti(0, 4, List.of(firstKey, secondKey),
                List.of(buf("value-1"), buf("value-2")),
                RequestType.none());

        assertEquals(List.of(), result);
        assertEquals(1, logClient.payloads.size());
        var mutations = ReplicatedMutationCodec.decodeBatch(logClient.payloads.getFirst());
        assertEquals(2, mutations.size());
        assertEquals(firstKey, mutations.getFirst().keys());
        assertEquals(secondKey, mutations.get(1).keys());
    }

    @Test
    void legacyFixtureRejectsNullLogClientWithExplicitDiagnostics() {
        var failure = assertThrows(IllegalArgumentException.class, () -> new LegacyReplicatedWriteSyncFixture(null));

        assertEquals("logClient must not be null", failure.getMessage());
    }

    @Test
    void failureInjectingLogClientCoversAppendCommitAndResponseSendPhases() {
        var payload = bytes("payload");
        var appendClient = new FailureInjectingReplicatedLogClient(new RecordingLogClient(),
                ReplicationFailureInjector.failOn(ReplicationFailurePhase.APPEND,
                        new IllegalStateException("append failed")));
        var commitClient = new FailureInjectingReplicatedLogClient(new RecordingLogClient(),
                ReplicationFailureInjector.failOn(ReplicationFailurePhase.COMMIT,
                        new IllegalStateException("commit failed")));
        var responseSendClient = new FailureInjectingReplicatedLogClient(new RecordingLogClient(),
                ReplicationFailureInjector.failOn(ReplicationFailurePhase.RESPONSE_SEND,
                        new IllegalStateException("response send failed")));

        var appendError = assertThrows(IllegalStateException.class, () -> appendClient.propose(payload));
        var commitError = assertThrows(CompletionException.class, () -> commitClient.propose(payload).join());
        var responseSendError = assertThrows(CompletionException.class,
                () -> responseSendClient.propose(payload).join());

        assertEquals("append failed", appendError.getMessage());
        assertEquals("commit failed", commitError.getCause().getMessage());
        assertEquals("response send failed", responseSendError.getCause().getMessage());
    }

    @Test
    void failureInjectingLogClientPropagatesCancellationToDelegate() {
        var phases = new ArrayList<ReplicationFailurePhase>();
        var delegateResponse = new CompletableFuture<ReplicatedLogCommit>();
        var client = new FailureInjectingReplicatedLogClient(
                _ -> delegateResponse,
                phases::add
        );

        var response = client.propose(bytes("payload"));

        assertTrue(response.cancel(true));
        assertTrue(delegateResponse.isCancelled());
        assertEquals(List.of(ReplicationFailurePhase.APPEND), phases);
    }

    @Test
    void rejectsWriteResultRequestsUntilResponseCacheExists() {
        var harness = singleNodeHarness();
        var api = writeApi(harness);

        var ex = assertThrows(RocksDBException.class,
                () -> api.put(0, 4, new Keys(buf("key")), buf("value"), RequestType.previous()));

        assertEquals(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED, ex.getErrorUniqueId());
        assertEquals(0, harness.recordingApi.commands.size());
    }

    @Test
    void rejectsMultiWriteResultRequestsUntilResponseCacheExists() {
        var harness = singleNodeHarness();
        var api = writeApi(harness);

        var putEx = assertThrows(RocksDBException.class,
                () -> api.putMulti(0, 4, List.of(new Keys(buf("key"))), List.of(buf("value")), RequestType.previous()));
        var deleteEx = assertThrows(RocksDBException.class,
                () -> api.deleteMulti(0, 4, List.of(new Keys(buf("key"))), RequestType.previous()));
        var mergeEx = assertThrows(RocksDBException.class,
                () -> api.mergeMulti(0, 4, List.of(new Keys(buf("key"))), List.of(buf("delta")), RequestType.merged()));

        assertEquals(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED, putEx.getErrorUniqueId());
        assertEquals(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED, deleteEx.getErrorUniqueId());
        assertEquals(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED, mergeEx.getErrorUniqueId());
        assertEquals(0, harness.recordingApi.commands.size());
    }

    @Test
    void rejectsMultiWriteMismatchedKeysAndValues() {
        var harness = singleNodeHarness();
        var api = writeApi(harness);

        var putError = assertThrows(RocksDBException.class,
                () -> api.putMulti(0, 4, List.of(new Keys(buf("key-1")), new Keys(buf("key-2"))),
                        List.of(buf("value-1")), RequestType.none()));
        var mergeError = assertThrows(RocksDBException.class,
                () -> api.mergeMulti(0, 4, List.of(new Keys(buf("key-1")), new Keys(buf("key-2"))),
                        List.of(buf("delta-1")), RequestType.none()));

        assertEquals(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, putError.getErrorUniqueId());
        assertEquals(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, mergeError.getErrorUniqueId());
        assertEquals(0, harness.recordingApi.commands.size());
    }

    @Test
    void rejectsTransactionalWriteContext() {
        var harness = singleNodeHarness();
        var api = writeApi(harness);

        var ex = assertThrows(RocksDBException.class,
                () -> api.put(99, 4, new Keys(buf("key")), buf("value"), RequestType.none()));

        assertEquals(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED, ex.getErrorUniqueId());
        assertEquals(0, harness.recordingApi.commands.size());
    }

    @Test
    void rejectsLocalHandlesAtReplicatedApiBoundary() {
        var harness = singleNodeHarness();
        var api = writeApi(harness);

        var openTransaction = assertThrows(RocksDBException.class, () -> api.openTransaction(1000));
        var forUpdate = assertThrows(RocksDBException.class,
                () -> api.get(0, 4, new Keys(buf("key")), RequestType.forUpdate()));
        var openIterator = assertThrows(RocksDBException.class,
                () -> api.openIterator(0, 4, null, null, false, 1000));

        assertEquals(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED, openTransaction.getErrorUniqueId());
        assertEquals(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED, forUpdate.getErrorUniqueId());
        assertEquals(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED, openIterator.getErrorUniqueId());
        assertEquals(0, harness.recordingApi.commands.size());
    }

    @Test
    void rejectsRangeTransactionHandlesBeforeProposal() {
        var harness = singleNodeHarness();
        var api = writeApi(harness);

        assertNotImplemented(() -> api.reduceRange(99, 4, null, null, false, RequestType.entriesCount(), 1000));
        assertNotImplemented(() -> api.getRange(99, 4, null, null, false, RequestType.allInRange(), 1000));

        assertEquals(0, harness.logClient.payloads.size());
        assertEquals(0, harness.recordingApi.commands.size());
    }

    @Test
    void rejectsUnsafeBatchAndSstModesBeforeProposal() {
        var harness = singleNodeHarness();
        var api = writeApi(harness);

        assertNotImplemented(() -> api.putBatch(4, Flux.<KVBatch>empty(), PutBatchMode.WRITE_BATCH_NO_WAL));
        assertNotImplemented(() -> api.putBatch(4, Flux.<KVBatch>empty(), PutBatchMode.SST_INGESTION));
        assertNotImplemented(() -> api.putBatch(4, Flux.<KVBatch>empty(), PutBatchMode.SST_INGEST_BEHIND));
        assertNotImplemented(() -> api.mergeBatch(4, Flux.<KVBatch>empty(), MergeBatchMode.MERGE_WRITE_BATCH_NO_WAL));
        assertNotImplemented(() -> api.mergeBatch(4, Flux.<KVBatch>empty(), MergeBatchMode.MERGE_SST_INGESTION));
        assertNotImplemented(() -> api.mergeBatch(4, Flux.<KVBatch>empty(), MergeBatchMode.MERGE_SST_INGEST_BEHIND));

        assertEquals(0, harness.logClient.payloads.size());
        assertEquals(0, harness.recordingApi.commands.size());
    }

    @Test
    void rejectsEmbeddedCdcHandlesBeforeProposal() {
        var harness = singleNodeHarness();
        var api = writeApi(harness);

        assertNotImplemented(() -> api.cdcCreate("sub", null, null));
        assertNotImplemented(() -> api.cdcDelete("sub"));
        assertNotImplemented(() -> api.cdcCommit("sub", 10));
        assertNotImplemented(() -> api.cdcPoll("sub", null, 10));

        assertEquals(0, harness.logClient.payloads.size());
        assertEquals(0, harness.recordingApi.commands.size());
    }

    @Test
    void rejectsLocalAdminCommandsBeforeProposal() {
        var harness = singleNodeHarness();
        var api = writeApi(harness);

        assertNotImplemented(api::flush);
        assertNotImplemented(api::compact);
        assertNotImplemented(() -> api.scanRaw(4, 0, 1));

        assertEquals(0, harness.logClient.payloads.size());
        assertEquals(0, harness.recordingApi.commands.size());
    }

    @Test
    void rejectsReadsOnWriteReplicationPath() {
        var harness = singleNodeHarness();
        var api = writeApi(harness);

        var ex = assertThrows(RocksDBException.class,
                () -> api.get(0, 4, new Keys(buf("key")), RequestType.current()));

        assertEquals(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED, ex.getErrorUniqueId());
    }

    @Test
    void doesNotAcknowledgeWhenProposalDoesNotCommit() {
        var harness = threeNodeLeaderWithoutFollowers();
        var api = writeApi(harness);

        var ex = assertThrows(RocksDBException.class,
                () -> api.put(0, 4, new Keys(buf("key")), buf("value"), RequestType.none()));

        assertEquals(RocksDBException.RocksDBErrorType.COMMIT_FAILED_TRY_AGAIN, ex.getErrorUniqueId());
        assertEquals(0, harness.recordingApi.commands.size());
    }

    private static Harness singleNodeHarness() {
        return harness(true);
    }

    private static Harness threeNodeLeaderWithoutFollowers() {
        return harness(false);
    }

    private static Harness harness(boolean committed) {
        var recordingApi = new RecordingApi();
        return new Harness(recordingApi, new ApplyingLogClient(recordingApi, committed));
    }

    private static LegacyReplicatedWriteSyncFixture writeApi(Harness harness) {
        return new LegacyReplicatedWriteSyncFixture(harness.logClient);
    }

    private static Buf buf(String value) {
        return Buf.wrap(bytes(value));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static void assertNotImplemented(Executable executable) {
        var ex = assertThrows(RocksDBException.class, executable);
        assertEquals(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED, ex.getErrorUniqueId());
    }

    private record Harness(RecordingApi recordingApi, ApplyingLogClient logClient) {
    }

    private static final class RecordingApi implements RocksDBSyncAPI {

        private final List<RocksDBAPICommand<?, ?, ?>> commands = new ArrayList<>();

        @Override
        public <R, RS, RA> RS requestSync(RocksDBAPICommand<R, RS, RA> req) {
            commands.add(req);
            return null;
        }
    }

    private static final class RecordingLogClient implements ReplicatedLogClient {

        private final List<byte[]> payloads = new ArrayList<>();

        @Override
        public CompletableFuture<ReplicatedLogCommit> propose(byte[] payload) {
            payloads.add(payload.clone());
            return CompletableFuture.completedFuture(new ReplicatedLogCommit(payloads.size(), true));
        }
    }

    private static final class ApplyingLogClient implements ReplicatedLogClient {

        private final RecordingApi api;
        private final boolean committed;
        private final List<byte[]> payloads = new ArrayList<>();

        private ApplyingLogClient(RecordingApi api, boolean committed) {
            this.api = api;
            this.committed = committed;
        }

        @Override
        public CompletableFuture<ReplicatedLogCommit> propose(byte[] payload) {
            var copied = payload.clone();
            payloads.add(copied);
            if (committed) {
                new ReplicatedMutationApplier(api).apply(ReplicatedMutationCodec.decodeBatch(copied));
            }
            return CompletableFuture.completedFuture(new ReplicatedLogCommit(payloads.size(), committed));
        }
    }
}
