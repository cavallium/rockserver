package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.cluster.ReplicatedCommandClassifier;
import it.cavallium.rockserver.core.cluster.ReplicatedCommandKind;
import it.cavallium.rockserver.core.cluster.ClientId;
import it.cavallium.rockserver.core.cluster.ClientRequestId;
import it.cavallium.rockserver.core.cluster.ClusterId;
import it.cavallium.rockserver.core.cluster.RatisQuorumCondition;
import it.cavallium.rockserver.core.cluster.ReplicatedCommand;
import it.cavallium.rockserver.core.cluster.ReplicationFailureInjector;
import it.cavallium.rockserver.core.cluster.ReplicationFailurePhase;
import it.cavallium.rockserver.core.cluster.ReplicatedWriteRetryPolicy;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.WriteDurability;
import it.cavallium.rockserver.core.cluster.WriteDurabilityMatrixEntry;
import it.cavallium.rockserver.core.common.MergeBatchMode;
import it.cavallium.rockserver.core.common.PutBatchMode;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBException;

import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

@SuppressWarnings("DataFlowIssue")
class ReplicatedCommandClassifierTest {

    @Test
    void rejectsNullInputsWithExplicitDiagnostics() {
        var command = new RocksDBAPICommand.RocksDBAPICommandSingle.Put<>(0, 1, null, null, RequestType.none());
        var clusterId = new ClusterId(new UUID(0, 1));
        var shardId = new ShardId(1);
        var requestId = new ClientRequestId(new ClientId(new UUID(0, 2)), 3);

        assertIllegalArgument("command must not be null", () -> ReplicatedCommandClassifier.classify(null));
        assertIllegalArgument("command must not be null", () -> ReplicatedCommandClassifier.usesLocalHandle(null));
        assertIllegalArgument("command must not be null",
                () -> ReplicatedCommandClassifier.usesUnsafeLocalWritePath(null));
        assertIllegalArgument("command must not be null",
                () -> ReplicatedCommandClassifier.requiresResponseDedupe(null));
        assertIllegalArgument("command must not be null", () -> ReplicatedCommandClassifier.retryPolicy(null));
        assertIllegalArgument("command must not be null",
                () -> ReplicatedCommandClassifier.requiresStateMachineRead(null));
        assertIllegalArgument("command must not be null",
                () -> ReplicatedCommandClassifier.requireSafeForReplicatedLog(null));

        assertIllegalArgument("clusterId must not be null", () -> new ReplicatedCommand(null, shardId,
                1, 1, requestId, ReplicatedCommandKind.REPLICATED_WRITE, command, 0, WriteDurability.QUORUM_COMMIT));
        assertIllegalArgument("shardId must not be null", () -> new ReplicatedCommand(clusterId, null,
                1, 1, requestId, ReplicatedCommandKind.REPLICATED_WRITE, command, 0, WriteDurability.QUORUM_COMMIT));
        assertIllegalArgument("requestId must not be null", () -> new ReplicatedCommand(clusterId, shardId,
                1, 1, null, ReplicatedCommandKind.REPLICATED_WRITE, command, 0, WriteDurability.QUORUM_COMMIT));
        assertIllegalArgument("kind must not be null", () -> new ReplicatedCommand(clusterId, shardId,
                1, 1, requestId, null, command, 0, WriteDurability.QUORUM_COMMIT));
        assertIllegalArgument("command must not be null", () -> new ReplicatedCommand(clusterId, shardId,
                1, 1, requestId, ReplicatedCommandKind.REPLICATED_WRITE, null, 0, WriteDurability.QUORUM_COMMIT));
        assertIllegalArgument("durability must not be null", () -> new ReplicatedCommand(clusterId, shardId,
                1, 1, requestId, ReplicatedCommandKind.REPLICATED_WRITE, command, 0, null));

        assertIllegalArgument("requestedDurability must not be null", () -> new WriteDurabilityMatrixEntry(null,
                WriteDurability.QUORUM_COMMIT_APPLY, RatisQuorumCondition.MAJORITY_COMMITTED,
                true, true, false, false, false, false, true));
        assertIllegalArgument("effectiveAdapterDurability must not be null", () -> new WriteDurabilityMatrixEntry(
                WriteDurability.QUORUM_COMMIT, null, RatisQuorumCondition.MAJORITY_COMMITTED,
                true, true, false, false, false, false, true));
        assertIllegalArgument("ratisQuorumCondition must not be null", () -> new WriteDurabilityMatrixEntry(
                WriteDurability.QUORUM_COMMIT, WriteDurability.QUORUM_COMMIT_APPLY, null,
                true, true, false, false, false, false, true));

        assertIllegalArgument("failedPhase must not be null",
                () -> ReplicationFailureInjector.failOn(null, new IllegalStateException("boom")));
        assertIllegalArgument("failure must not be null",
                () -> ReplicationFailureInjector.failOn(ReplicationFailurePhase.APPEND, null));
    }

    @Test
    void classifiesMutationAsReplicatedWrite() {
        var command = new RocksDBAPICommand.RocksDBAPICommandSingle.Put<>(0, 1, null, null, RequestType.none());

        assertEquals(ReplicatedCommandKind.REPLICATED_WRITE, ReplicatedCommandClassifier.classify(command));
        assertTrue(ReplicatedCommandClassifier.classify(command).entersReplicatedLog());
    }

    @Test
    void classifiesReadAsRoutedRead() {
        var command = new RocksDBAPICommand.RocksDBAPICommandSingle.Get<>(0, 1, null, RequestType.current());

        assertEquals(ReplicatedCommandKind.ROUTED_READ, ReplicatedCommandClassifier.classify(command));
        assertFalse(ReplicatedCommandClassifier.classify(command).entersReplicatedLog());
    }

    @Test
    void classifiesLocalSessionCommand() {
        var command = new RocksDBAPICommand.RocksDBAPICommandSingle.OpenTransaction(1000);

        assertEquals(ReplicatedCommandKind.LOCAL_SESSION, ReplicatedCommandClassifier.classify(command));
    }

    @Test
    void classifiesNonZeroTransactionOrUpdateIdAsLocalSession() {
        var command = new RocksDBAPICommand.RocksDBAPICommandSingle.Put<>(42, 1, null, null, RequestType.none());

        assertTrue(ReplicatedCommandClassifier.usesLocalHandle(command));
        assertEquals(ReplicatedCommandKind.LOCAL_SESSION, ReplicatedCommandClassifier.classify(command));
        assertThrows(RocksDBException.class, () -> ReplicatedCommandClassifier.requireSafeForReplicatedLog(command));
    }

    @Test
    void classifiesForUpdateGetAsLocalSession() {
        var command = new RocksDBAPICommand.RocksDBAPICommandSingle.Get<>(0, 1, null, RequestType.forUpdate());

        assertTrue(ReplicatedCommandClassifier.usesLocalHandle(command));
        assertEquals(ReplicatedCommandKind.LOCAL_SESSION, ReplicatedCommandClassifier.classify(command));
    }

    @Test
    void classifiesCdcCommandSeparately() {
        var command = new RocksDBAPICommand.CdcCreate("sub", null, null, null);

        assertEquals(ReplicatedCommandKind.CDC, ReplicatedCommandClassifier.classify(command));
    }

    @Test
    void rejectsReadCommandAsReplicatedLogEntry() {
        var command = new RocksDBAPICommand.RocksDBAPICommandSingle.Get<>(0, 1, null, RequestType.current());

        assertThrows(RocksDBException.class, () -> ReplicatedCommandClassifier.requireSafeForReplicatedLog(command));
    }

    @Test
    void replicatedCommandValidatesItsLogEntryContract() {
        var clusterId = new ClusterId(new UUID(0, 1));
        var shardId = new ShardId(1);
        var requestId = new ClientRequestId(new ClientId(new UUID(0, 2)), 3);
        var write = new RocksDBAPICommand.RocksDBAPICommandSingle.Put<>(0, 1, null, null, RequestType.none());
        var read = new RocksDBAPICommand.RocksDBAPICommandSingle.Get<>(0, 1, null, RequestType.current());

        ReplicatedCommand.from(clusterId, shardId, 1, 1, requestId, write, 0, WriteDurability.QUORUM_COMMIT)
                .requireReplicatedLogEntry();
        assertThrows(RocksDBException.class, () -> ReplicatedCommand.from(clusterId,
                shardId,
                1,
                1,
                requestId,
                read,
                0,
                WriteDurability.QUORUM_COMMIT
        ).requireReplicatedLogEntry());
    }

    @Test
    void rejectsUnsafeBatchModeInReplicatedLog() {
        var command = new RocksDBAPICommand.RocksDBAPICommandSingle.PutBatch(1,
                Flux.empty(),
                PutBatchMode.WRITE_BATCH_NO_WAL
        );

        assertTrue(ReplicatedCommandClassifier.usesUnsafeLocalWritePath(command));
        assertThrows(RocksDBException.class, () -> ReplicatedCommandClassifier.requireSafeForReplicatedLog(command));
    }

    @Test
    void rejectsEveryEmbeddedOnlyLocalDurabilityShortcutInReplicatedMode() {
        for (var mode : PutBatchMode.values()) {
            var command = new RocksDBAPICommand.RocksDBAPICommandSingle.PutBatch(1, Flux.empty(), mode);
            if (mode == PutBatchMode.WRITE_BATCH) {
                assertFalse(ReplicatedCommandClassifier.usesUnsafeLocalWritePath(command));
                continue;
            }

            var error = assertThrows(RocksDBException.class,
                    () -> ReplicatedCommandClassifier.requireSafeForReplicatedLog(command));

            assertTrue(ReplicatedCommandClassifier.usesUnsafeLocalWritePath(command));
            assertEquals(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED, error.getErrorUniqueId());
            assertEquals(ReplicatedWriteRetryPolicy.UNSUPPORTED, ReplicatedCommandClassifier.retryPolicy(command));
        }

        for (var mode : MergeBatchMode.values()) {
            var command = new RocksDBAPICommand.RocksDBAPICommandSingle.MergeBatch(1, Flux.empty(), mode);
            if (mode == MergeBatchMode.MERGE_WRITE_BATCH) {
                assertFalse(ReplicatedCommandClassifier.usesUnsafeLocalWritePath(command));
                continue;
            }

            var error = assertThrows(RocksDBException.class,
                    () -> ReplicatedCommandClassifier.requireSafeForReplicatedLog(command));

            assertTrue(ReplicatedCommandClassifier.usesUnsafeLocalWritePath(command));
            assertEquals(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED, error.getErrorUniqueId());
            assertEquals(ReplicatedWriteRetryPolicy.UNSUPPORTED, ReplicatedCommandClassifier.retryPolicy(command));
        }
    }

    @Test
    void identifiesResponseProducingWritesThatNeedDedupe() {
        var blindPut = new RocksDBAPICommand.RocksDBAPICommandSingle.Put<>(0, 1, null, null, RequestType.none());
        var previousPut = new RocksDBAPICommand.RocksDBAPICommandSingle.Put<>(0, 1, null, null, RequestType.previous());
        var createColumn = new RocksDBAPICommand.RocksDBAPICommandSingle.CreateColumn("column", null);
        var uploadMergeOperator =
                new RocksDBAPICommand.RocksDBAPICommandSingle.UploadMergeOperator("op", "ClassName", new byte[0]);

        assertFalse(ReplicatedCommandClassifier.requiresResponseDedupe(blindPut));
        assertTrue(ReplicatedCommandClassifier.requiresResponseDedupe(previousPut));
        assertTrue(ReplicatedCommandClassifier.requiresResponseDedupe(createColumn));
        assertFalse(ReplicatedCommandClassifier.requiresResponseDedupe(uploadMergeOperator));
    }

    @Test
    void identifiesReadModifyWriteCommands() {
        var blindPut = new RocksDBAPICommand.RocksDBAPICommandSingle.Put<>(0, 1, null, null, RequestType.none());
        var deltaPut = new RocksDBAPICommand.RocksDBAPICommandSingle.Put<>(0, 1, null, null, RequestType.delta());
        var mergeBatch = new RocksDBAPICommand.RocksDBAPICommandSingle.MergeBatch(1,
                Flux.empty(),
                MergeBatchMode.MERGE_WRITE_BATCH
        );

        assertFalse(ReplicatedCommandClassifier.requiresStateMachineRead(blindPut));
        assertTrue(ReplicatedCommandClassifier.requiresStateMachineRead(deltaPut));
        assertTrue(ReplicatedCommandClassifier.requiresStateMachineRead(mergeBatch));
    }

    @Test
    void nullWriteRequestTypesAreExplicitInvalidRequests() {
        assertNullRequestTypeInvalid(
                new RocksDBAPICommand.RocksDBAPICommandSingle.Put<>(0, 1, null, null, null),
                "put requestType must not be null"
        );
        assertNullRequestTypeInvalid(
                new RocksDBAPICommand.RocksDBAPICommandSingle.PutMulti<>(0, 1, List.of(), List.of(), null),
                "putMulti requestType must not be null"
        );
        assertNullRequestTypeInvalid(
                new RocksDBAPICommand.RocksDBAPICommandSingle.Delete<>(0, 1, null, null),
                "delete requestType must not be null"
        );
        assertNullRequestTypeInvalid(
                new RocksDBAPICommand.RocksDBAPICommandSingle.DeleteMulti<>(0, 1, List.of(), null),
                "deleteMulti requestType must not be null"
        );
        assertNullRequestTypeInvalid(
                new RocksDBAPICommand.RocksDBAPICommandSingle.Merge<>(0, 1, null, null, null),
                "merge requestType must not be null"
        );
        assertNullRequestTypeInvalid(
                new RocksDBAPICommand.RocksDBAPICommandSingle.MergeMulti<>(0, 1, List.of(), List.of(), null),
                "mergeMulti requestType must not be null"
        );
    }

    @Test
    void assignsRetryPolicyToEveryReplicatedWriteShape() {
        assertPolicy(ReplicatedWriteRetryPolicy.RESPONSE_DEDUPE,
                new RocksDBAPICommand.RocksDBAPICommandSingle.CreateColumn("column", null));
        assertPolicy(ReplicatedWriteRetryPolicy.ACK_ONLY_DEDUPE,
                new RocksDBAPICommand.RocksDBAPICommandSingle.DeleteColumn(1));

        assertPolicy(ReplicatedWriteRetryPolicy.ACK_ONLY_DEDUPE,
                new RocksDBAPICommand.RocksDBAPICommandSingle.Put<>(0, 1, null, null, RequestType.none()));
        assertPolicy(ReplicatedWriteRetryPolicy.RESPONSE_DEDUPE,
                new RocksDBAPICommand.RocksDBAPICommandSingle.Put<>(0, 1, null, null, RequestType.previous()));
        assertPolicy(ReplicatedWriteRetryPolicy.ACK_ONLY_DEDUPE,
                new RocksDBAPICommand.RocksDBAPICommandSingle.Delete<>(0, 1, null, RequestType.none()));
        assertPolicy(ReplicatedWriteRetryPolicy.RESPONSE_DEDUPE,
                new RocksDBAPICommand.RocksDBAPICommandSingle.Delete<>(0, 1, null, RequestType.previousPresence()));
        assertPolicy(ReplicatedWriteRetryPolicy.ACK_ONLY_DEDUPE,
                new RocksDBAPICommand.RocksDBAPICommandSingle.Merge<>(0, 1, null, null, RequestType.none()));
        assertPolicy(ReplicatedWriteRetryPolicy.RESPONSE_DEDUPE,
                new RocksDBAPICommand.RocksDBAPICommandSingle.Merge<>(0, 1, null, null, RequestType.merged()));

        assertPolicy(ReplicatedWriteRetryPolicy.ACK_ONLY_DEDUPE,
                new RocksDBAPICommand.RocksDBAPICommandSingle.PutMulti<>(0, 1, List.of(), List.of(), RequestType.none()));
        assertPolicy(ReplicatedWriteRetryPolicy.RESPONSE_DEDUPE,
                new RocksDBAPICommand.RocksDBAPICommandSingle.PutMulti<>(0, 1, List.of(), List.of(), RequestType.delta()));
        assertPolicy(ReplicatedWriteRetryPolicy.ACK_ONLY_DEDUPE,
                new RocksDBAPICommand.RocksDBAPICommandSingle.DeleteMulti<>(0, 1, List.of(), RequestType.none()));
        assertPolicy(ReplicatedWriteRetryPolicy.RESPONSE_DEDUPE,
                new RocksDBAPICommand.RocksDBAPICommandSingle.DeleteMulti<>(0, 1, List.of(), RequestType.previous()));
        assertPolicy(ReplicatedWriteRetryPolicy.ACK_ONLY_DEDUPE,
                new RocksDBAPICommand.RocksDBAPICommandSingle.MergeMulti<>(0, 1, List.of(), List.of(), RequestType.none()));
        assertPolicy(ReplicatedWriteRetryPolicy.RESPONSE_DEDUPE,
                new RocksDBAPICommand.RocksDBAPICommandSingle.MergeMulti<>(0, 1, List.of(), List.of(), RequestType.merged()));

        assertPolicy(ReplicatedWriteRetryPolicy.ACK_ONLY_DEDUPE,
                new RocksDBAPICommand.RocksDBAPICommandSingle.PutBatch(1, Flux.empty(), PutBatchMode.WRITE_BATCH));
        assertPolicy(ReplicatedWriteRetryPolicy.ACK_ONLY_DEDUPE,
                new RocksDBAPICommand.RocksDBAPICommandSingle.MergeBatch(1, Flux.empty(), MergeBatchMode.MERGE_WRITE_BATCH));
    }

    @Test
    void assignsUnsupportedRetryPolicyToNonReplicatedOrUnsafeCommands() {
        assertPolicy(ReplicatedWriteRetryPolicy.UNSUPPORTED,
                new RocksDBAPICommand.RocksDBAPICommandSingle.Get<>(0, 1, null, RequestType.current()));
        assertPolicy(ReplicatedWriteRetryPolicy.UNSUPPORTED,
                new RocksDBAPICommand.RocksDBAPICommandSingle.UploadMergeOperator("op", "ClassName", new byte[0]));
        assertPolicy(ReplicatedWriteRetryPolicy.UNSUPPORTED,
                new RocksDBAPICommand.RocksDBAPICommandSingle.Put<>(99, 1, null, null, RequestType.none()));
        assertPolicy(ReplicatedWriteRetryPolicy.UNSUPPORTED,
                new RocksDBAPICommand.RocksDBAPICommandSingle.PutBatch(1, Flux.empty(), PutBatchMode.WRITE_BATCH_NO_WAL));
    }

    private static void assertPolicy(ReplicatedWriteRetryPolicy expected, RocksDBAPICommand<?, ?, ?> command) {
        assertEquals(expected, ReplicatedCommandClassifier.retryPolicy(command));
        assertEquals(expected != ReplicatedWriteRetryPolicy.UNSUPPORTED, expected.requiresDedupeRecord());
        assertEquals(expected == ReplicatedWriteRetryPolicy.RESPONSE_DEDUPE, expected.requiresResponseRecord());
    }

    private static void assertNullRequestTypeInvalid(RocksDBAPICommand<?, ?, ?> command, String expectedMessage) {
        assertEquals(ReplicatedCommandKind.REPLICATED_WRITE, ReplicatedCommandClassifier.classify(command));
        assertFalse(ReplicatedCommandClassifier.requiresResponseDedupe(command));
        ReplicatedCommandClassifier.requiresStateMachineRead(command);
        assertEquals(ReplicatedWriteRetryPolicy.UNSUPPORTED, ReplicatedCommandClassifier.retryPolicy(command));

        var error = assertThrows(RocksDBException.class,
                () -> ReplicatedCommandClassifier.requireSafeForReplicatedLog(command));
        assertEquals(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, error.getErrorUniqueId());
        assertEquals(expectedMessage, error.getMessage());
    }

    private static void assertIllegalArgument(String expectedMessage, org.junit.jupiter.api.function.Executable action) {
        var failure = assertThrows(IllegalArgumentException.class, action);

        assertEquals(expectedMessage, failure.getMessage());
    }
}
