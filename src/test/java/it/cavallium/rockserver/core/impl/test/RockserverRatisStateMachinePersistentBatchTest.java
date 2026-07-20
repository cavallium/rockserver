package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.ClientId;
import it.cavallium.rockserver.core.cluster.ClientRequestId;
import it.cavallium.rockserver.core.cluster.ClusterColumnMetadata;
import it.cavallium.rockserver.core.cluster.ClusterId;
import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.PartitionDescriptor;
import it.cavallium.rockserver.core.cluster.PartitionSplitState;
import it.cavallium.rockserver.core.cluster.ReplicaMember;
import it.cavallium.rockserver.core.cluster.ReplicaRole;
import it.cavallium.rockserver.core.cluster.ReplicationFailureInjector;
import it.cavallium.rockserver.core.cluster.ReplicationFailurePhase;
import it.cavallium.rockserver.core.cluster.PartitionBoundaryRules;
import it.cavallium.rockserver.core.cluster.RockserverStorageKeyspace;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.command.ClientDedupeState;
import it.cavallium.rockserver.core.cluster.command.ClientDedupeStateCodec;
import it.cavallium.rockserver.core.cluster.command.FailureInjectingReplicatedLogClient;
import it.cavallium.rockserver.core.cluster.command.PartitionStateMetadataCodec;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteCommand;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteCommandCodec;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteCommandSealer;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteCommandValidator;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteResponseCodec;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteResponseMode;
import it.cavallium.rockserver.core.cluster.command.ReplicatedCdcEventCodec;
import it.cavallium.rockserver.core.cluster.command.ReplicatedLogClient;
import it.cavallium.rockserver.core.cluster.command.ReplicatedLogCommit;
import it.cavallium.rockserver.core.cluster.command.ReplicatedMutation;
import it.cavallium.rockserver.core.cluster.command.ReplicatedMutationOp;
import it.cavallium.rockserver.core.cluster.command.RocksDBPartitionStateMachineApplier;
import it.cavallium.rockserver.core.cluster.command.RocksDBPartitionStateWriteBatchWriter;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.impl.rocksdb.RocksDBLoader;
import it.cavallium.rockserver.core.replication.ratis.RockserverRatisStateMachine;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.Checkpoint;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

class RockserverRatisStateMachinePersistentBatchTest {

    @BeforeAll
    static void loadRocksDb() {
        RocksDBLoader.loadLibrary();
    }

    @Test
    void appliesNoResponseCommandThroughAtomicPartitionWriteBatch(@TempDir Path directory) throws Exception {
        var metadata = metadata();
        var deleteKey = RockserverStorageKeyspace.userData(4, buf(0x19));
        var command = new PartitionWriteCommand(requestId(), 3, List.of(
                ReplicatedMutation.put(4, new Keys(buf(0x18)), text("value")),
                ReplicatedMutation.delete(4, new Keys(buf(0x19)))
        ));

        try (var options = new Options().setCreateIfMissing(true);
             var db = RocksDB.open(options, directory.resolve("partition-db").toString())) {
            db.put(deleteKey.toByteArray(), text("stale").toByteArray());
            var stateMachine = persistentStateMachine(db, metadata);

            var result = stateMachine.applyCommitted(5, PartitionWriteCommandCodec.encode(command));

            assertTrue(result.applied());
            assertEquals(2, result.mutationsApplied());
            assertEquals(List.of(), result.responses());
            assertEquals(5, stateMachine.lastAppliedIndex());
            assertArrayEquals(text("value").toByteArray(),
                    db.get(RockserverStorageKeyspace.userData(4, buf(0x18)).toByteArray()));
            assertNull(db.get(deleteKey.toByteArray()));
            assertEquals(List.of(), PartitionWriteResponseCodec.decodeResponses(
                    db.get(RockserverStorageKeyspace.dedupeResponse(requestId()).toByteArray())));
            assertEquals(ClientDedupeState.acknowledged(requestId()), ClientDedupeStateCodec.decode(
                    db.get(RockserverStorageKeyspace.dedupeClientState(requestId().clientId()).toByteArray())));
            assertEquals(5, PartitionStateMetadataCodec.decodeLastAppliedIndex(
                    db.get(RockserverStorageKeyspace.lastAppliedIndex().toByteArray())));
        }
    }

    @Test
    @SuppressWarnings("DataFlowIssue")
    void persistentBatchApplierRejectsNullInputsWithExplicitDiagnostics(@TempDir Path directory) throws Exception {
        var metadata = metadata();

        try (var options = new Options().setCreateIfMissing(true);
             var db = RocksDB.open(options, directory.resolve("partition-db").toString())) {
            var writer = new RocksDBPartitionStateWriteBatchWriter(db);
            var applier = new RocksDBPartitionStateMachineApplier(metadata, writer);

            assertIllegalArgument("metadata must not be null",
                    () -> new RocksDBPartitionStateMachineApplier((ClusterMetadata) null, writer));
            assertIllegalArgument("writer must not be null",
                    () -> new RocksDBPartitionStateMachineApplier(metadata,
                            (RocksDBPartitionStateWriteBatchWriter) null));
            assertIllegalArgument("failureInjector must not be null",
                    () -> new RocksDBPartitionStateMachineApplier(metadata, writer, null));
            assertIllegalArgument("cdcPartition must not be null",
                    () -> new RocksDBPartitionStateMachineApplier(metadata, (PartitionDescriptor) null, db));
            assertIllegalArgument("command must not be null", () -> applier.apply(5, null));
        }
    }

    @Test
    void appliesResponseProducingPutThroughAtomicPartitionWriteBatch(@TempDir Path directory)
            throws Exception {
        var metadata = metadata();
        var userKey = RockserverStorageKeyspace.userData(4, buf(0x18));
        var command = PartitionWriteCommand.single(requestId(), 3,
                ReplicatedMutation.put(4, new Keys(buf(0x18)), text("value")),
                PartitionWriteResponseMode.PREVIOUS_VALUE);

        try (var options = new Options().setCreateIfMissing(true);
             var db = RocksDB.open(options, directory.resolve("partition-db").toString())) {
            db.put(userKey.toByteArray(), text("previous").toByteArray());
            var stateMachine = persistentStateMachine(db, metadata);

            var result = stateMachine.applyCommitted(5, PartitionWriteCommandCodec.encode(command));

            assertTrue(result.applied());
            assertEquals(1, result.responses().size());
            assertArrayEquals(text("previous").toByteArray(), ((Buf) result.responses().getFirst()).toByteArray());
            assertArrayEquals(text("value").toByteArray(), db.get(userKey.toByteArray()));
            var dedupeResponses = PartitionWriteResponseCodec.decodeResponses(
                    db.get(RockserverStorageKeyspace.dedupeResponse(requestId()).toByteArray()));
            assertEquals(1, dedupeResponses.size());
            assertArrayEquals(text("previous").toByteArray(), ((Buf) dedupeResponses.getFirst()).toByteArray());
            assertEquals(5, PartitionStateMetadataCodec.decodeLastAppliedIndex(
                    db.get(RockserverStorageKeyspace.lastAppliedIndex().toByteArray())));
        }
    }

    @Test
    void appliesResponseProducingDeleteThroughAtomicPartitionWriteBatch(@TempDir Path directory)
            throws Exception {
        var metadata = metadata();
        var userKey = RockserverStorageKeyspace.userData(4, buf(0x18));
        var command = PartitionWriteCommand.single(requestId(), 3,
                ReplicatedMutation.delete(4, new Keys(buf(0x18))),
                PartitionWriteResponseMode.PREVIOUS_PRESENCE);

        try (var options = new Options().setCreateIfMissing(true);
             var db = RocksDB.open(options, directory.resolve("partition-db").toString())) {
            db.put(userKey.toByteArray(), text("previous").toByteArray());
            var stateMachine = persistentStateMachine(db, metadata);

            var result = stateMachine.applyCommitted(5, PartitionWriteCommandCodec.encode(command));

            assertTrue(result.applied());
            assertEquals(List.of(Boolean.TRUE), result.responses());
            assertNull(db.get(userKey.toByteArray()));
            var dedupeResponses = PartitionWriteResponseCodec.decodeResponses(
                    db.get(RockserverStorageKeyspace.dedupeResponse(requestId()).toByteArray()));
            assertEquals(List.of(Boolean.TRUE), dedupeResponses);
            assertEquals(5, PartitionStateMetadataCodec.decodeLastAppliedIndex(
                    db.get(RockserverStorageKeyspace.lastAppliedIndex().toByteArray())));
        }
    }

    @Test
    void persistentBatchApplierRejectsMergeResponseProductionWithoutAdvancing(@TempDir Path directory)
            throws Exception {
        var metadata = metadata();
        var userKey = RockserverStorageKeyspace.userData(4, buf(0x18));
        var command = PartitionWriteCommand.single(requestId(), 3,
                ReplicatedMutation.merge(4, new Keys(buf(0x18)), text("delta")),
                PartitionWriteResponseMode.MERGED_VALUE);

        try (var options = new Options().setCreateIfMissing(true);
             var db = RocksDB.open(options, directory.resolve("partition-db").toString())) {
            db.put(userKey.toByteArray(), text("previous").toByteArray());
            var stateMachine = persistentStateMachine(db, metadata);

            assertThrows(IllegalArgumentException.class,
                    () -> stateMachine.applyCommitted(5, PartitionWriteCommandCodec.encode(command)));

            assertEquals(0, stateMachine.lastAppliedIndex());
            assertNull(db.get(RockserverStorageKeyspace.lastAppliedIndex().toByteArray()));
        }
    }

    @Test
    void appliesResponseProducingMergeThroughDeterministicResolverAndAtomicBatch(@TempDir Path directory)
            throws Exception {
        var metadata = metadata();
        var userKey = RockserverStorageKeyspace.userData(4, buf(0x18));
        var command = PartitionWriteCommand.single(requestId(), 3,
                ReplicatedMutation.merge(4, new Keys(buf(0x18)), text("-delta")),
                PartitionWriteResponseMode.MERGED_VALUE);

        try (var options = new Options().setCreateIfMissing(true);
             var db = RocksDB.open(options, directory.resolve("partition-db").toString())) {
            db.put(userKey.toByteArray(), text("previous").toByteArray());
            var stateMachine = persistentStateMachine(db, metadata,
                    (_, previousValue, operand) -> {
                        assertNotNull(previousValue);
                        return text(textValue(previousValue) + textValue(operand));
                    });

            var result = stateMachine.applyCommitted(5, PartitionWriteCommandCodec.encode(command));

            assertTrue(result.applied());
            assertEquals(1, result.responses().size());
            assertArrayEquals(text("previous-delta").toByteArray(),
                    ((Buf) result.responses().getFirst()).toByteArray());
            assertArrayEquals(text("previous-delta").toByteArray(), db.get(userKey.toByteArray()));
            var dedupeResponses = PartitionWriteResponseCodec.decodeResponses(
                    db.get(RockserverStorageKeyspace.dedupeResponse(requestId()).toByteArray()));
            assertEquals(1, dedupeResponses.size());
            assertArrayEquals(text("previous-delta").toByteArray(),
                    ((Buf) dedupeResponses.getFirst()).toByteArray());
            assertEquals(5, PartitionStateMetadataCodec.decodeLastAppliedIndex(
                    db.get(RockserverStorageKeyspace.lastAppliedIndex().toByteArray())));
        }
    }

    @Test
    void mergeResolverMustReturnMergedValueWithoutAdvancing(@TempDir Path directory)
            throws Exception {
        var metadata = metadata();
        var userKey = RockserverStorageKeyspace.userData(4, buf(0x18));
        var command = PartitionWriteCommand.single(requestId(), 3,
                ReplicatedMutation.merge(4, new Keys(buf(0x18)), text("-delta")),
                PartitionWriteResponseMode.MERGED_VALUE);

        try (var options = new Options().setCreateIfMissing(true);
             var db = RocksDB.open(options, directory.resolve("partition-db").toString())) {
            db.put(userKey.toByteArray(), text("previous").toByteArray());
            var stateMachine = persistentStateMachine(db, metadata, (_, _, _) -> null);

            var failure = assertThrows(IllegalStateException.class,
                    () -> stateMachine.applyCommitted(5, PartitionWriteCommandCodec.encode(command)));

            assertEquals("mergedValue must not be null", failure.getMessage());
            assertEquals(0, stateMachine.lastAppliedIndex());
            assertArrayEquals(text("previous").toByteArray(), db.get(userKey.toByteArray()));
            assertNull(db.get(RockserverStorageKeyspace.dedupeResponse(requestId()).toByteArray()));
            assertNull(db.get(RockserverStorageKeyspace.lastAppliedIndex().toByteArray()));
        }
    }

    @Test
    void duplicateRequestReplaysStoredResponseWithoutApplyingMutationAgain(@TempDir Path directory)
            throws Exception {
        var metadata = metadata();
        var userKey = RockserverStorageKeyspace.userData(4, buf(0x18));
        var firstCommand = PartitionWriteCommand.single(requestId(), 3,
                ReplicatedMutation.put(4, new Keys(buf(0x18)), text("first")),
                PartitionWriteResponseMode.PREVIOUS_VALUE);
        var duplicatePayload = PartitionWriteCommandCodec.encode(PartitionWriteCommand.single(requestId(), 3,
                ReplicatedMutation.put(4, new Keys(buf(0x18)), text("second")),
                PartitionWriteResponseMode.PREVIOUS_VALUE));

        try (var options = new Options().setCreateIfMissing(true);
             var db = RocksDB.open(options, directory.resolve("partition-db").toString())) {
            db.put(userKey.toByteArray(), text("previous").toByteArray());
            var stateMachine = persistentStateMachine(db, metadata);

            stateMachine.applyCommitted(5, PartitionWriteCommandCodec.encode(firstCommand));
            var duplicate = stateMachine.applyCommitted(6, duplicatePayload);

            assertEquals(1, duplicate.responses().size());
            assertArrayEquals(text("previous").toByteArray(), ((Buf) duplicate.responses().getFirst()).toByteArray());
            assertArrayEquals(text("first").toByteArray(), db.get(userKey.toByteArray()));
            assertEquals(6, PartitionStateMetadataCodec.decodeLastAppliedIndex(
                    db.get(RockserverStorageKeyspace.lastAppliedIndex().toByteArray())));
        }
    }

    @Test
    void duplicateRequestRejectsStoredResponseCountAboveCommandShape(@TempDir Path directory)
            throws Exception {
        var metadata = metadata();
        var userKey = RockserverStorageKeyspace.userData(4, buf(0x18));
        var command = PartitionWriteCommand.single(requestId(), 3,
                ReplicatedMutation.put(4, new Keys(buf(0x18)), text("value")),
                PartitionWriteResponseMode.PREVIOUS_VALUE);

        try (var options = new Options().setCreateIfMissing(true);
             var db = RocksDB.open(options, directory.resolve("partition-db").toString())) {
            db.put(RockserverStorageKeyspace.dedupeResponse(requestId()).toByteArray(),
                    PartitionWriteResponseCodec.encodeResponses(List.of(text("previous"), text("extra"))));
            var stateMachine = persistentStateMachine(db, metadata);

            var error = assertThrows(IllegalArgumentException.class,
                    () -> stateMachine.applyCommitted(5, PartitionWriteCommandCodec.encode(command)));

            assertEquals("Partition write response count 2 exceeds limit 1", error.getMessage());
            assertEquals(0, stateMachine.lastAppliedIndex());
            assertNull(db.get(userKey.toByteArray()));
            assertNull(db.get(RockserverStorageKeyspace.lastAppliedIndex().toByteArray()));
        }
    }

    @Test
    void retryAfterExpiredDedupeResponseReturnsUnknownOutcomeError(@TempDir Path directory)
            throws Exception {
        var metadata = metadata();
        var userKey = RockserverStorageKeyspace.userData(4, buf(0x18));
        var command = PartitionWriteCommand.single(requestId(), 3,
                ReplicatedMutation.put(4, new Keys(buf(0x18)), text("value")),
                PartitionWriteResponseMode.PREVIOUS_VALUE);

        try (var options = new Options().setCreateIfMissing(true);
             var db = RocksDB.open(options, directory.resolve("partition-db").toString())) {
            db.put(RockserverStorageKeyspace.dedupeClientState(requestId().clientId()).toByteArray(),
                    ClientDedupeStateCodec.encode(ClientDedupeState.acknowledged(requestId(), 2, 99)));
            var stateMachine = persistentStateMachine(db, metadata);

            var error = assertThrows(RocksDBException.class,
                    () -> stateMachine.applyCommitted(5, PartitionWriteCommandCodec.encode(command)));

            assertEquals(RocksDBException.RocksDBErrorType.DEDUPE_EXPIRED_UNKNOWN_OUTCOME, error.getErrorUniqueId());
            assertNull(db.get(userKey.toByteArray()));
        }
    }

    @Test
    void applyFailureInjectionDoesNotAdvanceOrPersist(@TempDir Path directory) throws Exception {
        var metadata = metadata();
        var userKey = RockserverStorageKeyspace.userData(4, buf(0x18));
        var command = PartitionWriteCommand.single(requestId(), 3,
                ReplicatedMutation.put(4, new Keys(buf(0x18)), text("value")),
                PartitionWriteResponseMode.NONE);

        try (var options = new Options().setCreateIfMissing(true);
             var db = RocksDB.open(options, directory.resolve("partition-db").toString())) {
            var applier = new RocksDBPartitionStateMachineApplier(metadata, db);
            var stateMachine = new RockserverRatisStateMachine(applier,
                    commandToValidate -> PartitionWriteCommandValidator.validate(metadata, partition(),
                            commandToValidate),
                    ReplicationFailureInjector.failOn(ReplicationFailurePhase.APPLY,
                            new IllegalStateException("apply failed")));

            var error = assertThrows(IllegalStateException.class,
                    () -> stateMachine.applyCommitted(5, PartitionWriteCommandCodec.encode(command)));

            assertEquals("apply failed", error.getMessage());
            assertEquals(0, stateMachine.lastAppliedIndex());
            assertNull(db.get(userKey.toByteArray()));
            assertNull(db.get(RockserverStorageKeyspace.lastAppliedIndex().toByteArray()));
        }
    }

    @Test
    void responsePersistenceFailureDoesNotAdvanceOrPersist(@TempDir Path directory) throws Exception {
        var metadata = metadata();
        var userKey = RockserverStorageKeyspace.userData(4, buf(0x18));
        var command = PartitionWriteCommand.single(requestId(), 3,
                ReplicatedMutation.put(4, new Keys(buf(0x18)), text("value")),
                PartitionWriteResponseMode.PREVIOUS_VALUE);

        try (var options = new Options().setCreateIfMissing(true);
             var db = RocksDB.open(options, directory.resolve("partition-db").toString())) {
            db.put(userKey.toByteArray(), text("previous").toByteArray());
            var failureInjector = ReplicationFailureInjector.failOn(ReplicationFailurePhase.RESPONSE_PERSISTENCE,
                    new IllegalStateException("response persistence failed"));
            var stateMachine = persistentStateMachine(db, metadata, null, failureInjector);

            var error = assertThrows(IllegalStateException.class,
                    () -> stateMachine.applyCommitted(5, PartitionWriteCommandCodec.encode(command)));

            assertEquals("response persistence failed", error.getMessage());
            assertEquals(0, stateMachine.lastAppliedIndex());
            assertArrayEquals(text("previous").toByteArray(), db.get(userKey.toByteArray()));
            assertNull(db.get(RockserverStorageKeyspace.dedupeResponse(requestId()).toByteArray()));
            assertNull(db.get(RockserverStorageKeyspace.lastAppliedIndex().toByteArray()));
        }
    }

    @Test
    void rocksDbWriteFailureDoesNotAdvanceOrPersistPartialBatch(@TempDir Path directory) throws Exception {
        var metadata = metadata();
        var userKey = RockserverStorageKeyspace.userData(4, buf(0x18));
        var command = PartitionWriteCommand.single(requestId(), 3,
                ReplicatedMutation.put(4, new Keys(buf(0x18)), text("value")),
                PartitionWriteResponseMode.NONE);

        try (var options = new Options().setCreateIfMissing(true);
             var db = RocksDB.open(options, directory.resolve("partition-db").toString())) {
            var failureInjector = ReplicationFailureInjector.failOn(ReplicationFailurePhase.ROCKSDB_WRITE,
                    new IllegalStateException("rocksdb write failed"));
            var stateMachine = persistentStateMachine(db, metadata, null, failureInjector);

            var error = assertThrows(IllegalStateException.class,
                    () -> stateMachine.applyCommitted(5, PartitionWriteCommandCodec.encode(command)));

            assertEquals("rocksdb write failed", error.getMessage());
            assertEquals(0, stateMachine.lastAppliedIndex());
            assertNull(db.get(userKey.toByteArray()));
            assertNull(db.get(RockserverStorageKeyspace.dedupeResponse(requestId()).toByteArray()));
            assertNull(db.get(RockserverStorageKeyspace.lastAppliedIndex().toByteArray()));
        }
    }

    @Test
    void replayedLogsProduceIdenticalPartitionState(@TempDir Path directory) throws Exception {
        var metadata = metadata();
        var commands = List.of(
                PartitionWriteCommand.single(requestId(7), 3,
                        ReplicatedMutation.put(4, new Keys(buf(0x18)), text("first")),
                        PartitionWriteResponseMode.NONE),
                PartitionWriteCommand.single(requestId(8), 3,
                        ReplicatedMutation.put(4, new Keys(buf(0x19)), text("second")),
                        PartitionWriteResponseMode.NONE),
                PartitionWriteCommand.single(requestId(9), 3,
                        ReplicatedMutation.delete(4, new Keys(buf(0x18))),
                        PartitionWriteResponseMode.NONE)
        );

        try (var options = new Options().setCreateIfMissing(true);
             var first = RocksDB.open(options, directory.resolve("first-db").toString());
             var second = RocksDB.open(options, directory.resolve("second-db").toString())) {
            applyCommands(first, metadata, commands);
            applyCommands(second, metadata, commands);

            assertNull(first.get(RockserverStorageKeyspace.userData(4, buf(0x18)).toByteArray()));
            assertNull(second.get(RockserverStorageKeyspace.userData(4, buf(0x18)).toByteArray()));
            assertArrayEquals(first.get(RockserverStorageKeyspace.userData(4, buf(0x19)).toByteArray()),
                    second.get(RockserverStorageKeyspace.userData(4, buf(0x19)).toByteArray()));
            assertArrayEquals(first.get(RockserverStorageKeyspace.dedupeResponse(requestId(9)).toByteArray()),
                    second.get(RockserverStorageKeyspace.dedupeResponse(requestId(9)).toByteArray()));
            assertArrayEquals(first.get(RockserverStorageKeyspace.dedupeClientState(requestId().clientId())
                            .toByteArray()),
                    second.get(RockserverStorageKeyspace.dedupeClientState(requestId().clientId()).toByteArray()));
            assertArrayEquals(first.get(RockserverStorageKeyspace.lastAppliedIndex().toByteArray()),
                    second.get(RockserverStorageKeyspace.lastAppliedIndex().toByteArray()));
        }
    }

    @Test
    void retryAfterResponseSendFailureReturnsOriginalStoredResponse(@TempDir Path directory) throws Exception {
        var metadata = metadata();
        var userKey = RockserverStorageKeyspace.userData(4, buf(0x18));
        var command = PartitionWriteCommand.single(requestId(), 3,
                ReplicatedMutation.put(4, new Keys(buf(0x18)), text("first")),
                PartitionWriteResponseMode.PREVIOUS_VALUE);
        var retryCommand = PartitionWriteCommand.single(requestId(), 3,
                ReplicatedMutation.put(4, new Keys(buf(0x18)), text("second")),
                PartitionWriteResponseMode.PREVIOUS_VALUE);

        try (var options = new Options().setCreateIfMissing(true);
             var db = RocksDB.open(options, directory.resolve("partition-db").toString())) {
            db.put(userKey.toByteArray(), text("previous").toByteArray());
            var stateMachine = persistentStateMachine(db, metadata);
            var failingLogClient = new FailureInjectingReplicatedLogClient(
                    new ApplyingLogClient(stateMachine, 5),
                    ReplicationFailureInjector.failOn(ReplicationFailurePhase.RESPONSE_SEND,
                            new IllegalStateException("response send failed"))
            );

            var responseSendError = assertThrows(CompletionException.class,
                    () -> failingLogClient.propose(PartitionWriteCommandCodec.encode(command)).join());
            var retry = stateMachine.applyCommitted(6, PartitionWriteCommandCodec.encode(retryCommand));

            assertEquals("response send failed", responseSendError.getCause().getMessage());
            assertEquals(1, retry.responses().size());
            assertArrayEquals(text("previous").toByteArray(), ((Buf) retry.responses().getFirst()).toByteArray());
            assertArrayEquals(text("first").toByteArray(), db.get(userKey.toByteArray()));
            assertEquals(6, PartitionStateMetadataCodec.decodeLastAppliedIndex(
                    db.get(RockserverStorageKeyspace.lastAppliedIndex().toByteArray())));
        }
    }

    @Test
    void checkpointAndReopenPreservePartitionState(@TempDir Path directory) throws Exception {
        var metadata = metadata();
        var dbDirectory = directory.resolve("partition-db");
        var checkpointDirectory = directory.resolve("partition-checkpoint");
        var userKey = RockserverStorageKeyspace.userData(4, buf(0x18));
        var command = PartitionWriteCommand.single(requestId(), 3,
                ReplicatedMutation.put(4, new Keys(buf(0x18)), text("value")),
                PartitionWriteResponseMode.PREVIOUS_VALUE);

        try (var options = new Options().setCreateIfMissing(true);
             var db = RocksDB.open(options, dbDirectory.toString())) {
            db.put(userKey.toByteArray(), text("previous").toByteArray());
            persistentStateMachine(db, metadata).applyCommitted(5, PartitionWriteCommandCodec.encode(command));
            try (var checkpoint = Checkpoint.create(db)) {
                checkpoint.createCheckpoint(checkpointDirectory.toString());
            }
        }

        try (var options = new Options().setCreateIfMissing(false);
             var reopened = RocksDB.open(options, dbDirectory.toString());
             var checkpoint = RocksDB.open(options, checkpointDirectory.toString())) {
            assertPartitionStatePreserved(reopened, userKey);
            assertPartitionStatePreserved(checkpoint, userKey);
        }
    }

    @Test
    void replicatedCdcEventsPersistFromCommittedApplyPath(@TempDir Path directory) throws Exception {
        var metadata = metadata();
        var command = new PartitionWriteCommand(requestId(), 3, List.of(
                ReplicatedMutation.put(4, new Keys(buf(0x18)), text("value")),
                ReplicatedMutation.delete(4, new Keys(buf(0x19)))
        ));
        var duplicateCommand = PartitionWriteCommand.single(requestId(), 3,
                ReplicatedMutation.put(4, new Keys(buf(0x1a)), text("duplicate")),
                PartitionWriteResponseMode.NONE);

        try (var options = new Options().setCreateIfMissing(true);
             var db = RocksDB.open(options, directory.resolve("partition-db").toString())) {
            var stateMachine = persistentStateMachineWithCdc(db, metadata);

            var result = stateMachine.applyCommitted(5, PartitionWriteCommandCodec.encode(command));
            var duplicate = stateMachine.applyCommitted(6, PartitionWriteCommandCodec.encode(duplicateCommand));

            assertTrue(result.applied());
            assertTrue(duplicate.applied());
            var event0 = ReplicatedCdcEventCodec.decode(
                    db.get(RockserverStorageKeyspace.cdcEvent(5, 0).toByteArray()));
            var event1 = ReplicatedCdcEventCodec.decode(
                    db.get(RockserverStorageKeyspace.cdcEvent(5, 1).toByteArray()));

            assertEquals(metadata.epoch(), event0.token().metadataEpoch());
            assertEquals(partition().partitionId(), event0.token().partitionId());
            assertEquals(partition().epoch(), event0.token().partitionEpoch());
            assertEquals(5, event0.token().appliedIndex());
            assertEquals(0, event0.token().eventOrdinal());
            assertEquals(3, event0.schemaEpoch());
            assertEquals(4, event0.columnId());
            assertEquals(ReplicatedMutationOp.PUT, event0.operation());
            assertArrayEquals(PartitionBoundaryRules.routingKey(schema(), new Keys(buf(0x18))).toByteArray(),
                    event0.key().toByteArray());
            var event0Value = event0.value();
            assertNotNull(event0Value);
            assertArrayEquals(text("value").toByteArray(), event0Value.toByteArray());

            assertEquals(1, event1.token().eventOrdinal());
            assertEquals(ReplicatedMutationOp.DELETE, event1.operation());
            assertArrayEquals(PartitionBoundaryRules.routingKey(schema(), new Keys(buf(0x19))).toByteArray(),
                    event1.key().toByteArray());
            assertNull(event1.value());
            assertNull(db.get(RockserverStorageKeyspace.cdcEvent(6, 0).toByteArray()));
        }
    }

    @Test
    void sealedReplayUsesAdmissionKeysAndEpochsAfterSchemaEvolution(@TempDir Path directory) throws Exception {
        var admittedPartition = partition(3);
        var admittedMetadata = metadata(schema(), 3, 7, admittedPartition);
        var command = PartitionWriteCommandSealer.seal(admittedMetadata, PartitionWriteCommand.single(
                requestId(),
                3,
                ReplicatedMutation.put(4, new Keys(buf(0x18)), text("value"))
        ));
        var reloadedPartition = partition(4);
        var reloadedMetadata = metadata(schemaWithLongerKey(), 4, 8, reloadedPartition);
        var userKey = RockserverStorageKeyspace.userData(4, buf(0x18));

        try (var options = new Options().setCreateIfMissing(true);
             var db = RocksDB.open(options, directory.resolve("partition-db").toString())) {
            var applier = new RocksDBPartitionStateMachineApplier(reloadedMetadata, reloadedPartition, db);
            var stateMachine = new RockserverRatisStateMachine(applier,
                    replay -> PartitionWriteCommandValidator.validateForReplay(reloadedMetadata, reloadedPartition,
                            replay));

            stateMachine.applyCommitted(5, PartitionWriteCommandCodec.encode(command));

            assertArrayEquals(text("value").toByteArray(), db.get(userKey.toByteArray()));
            var event = ReplicatedCdcEventCodec.decode(
                    db.get(RockserverStorageKeyspace.cdcEvent(5, 0).toByteArray()));
            assertEquals(7, event.token().metadataEpoch());
            assertEquals(reloadedPartition.partitionId(), event.token().partitionId());
            assertEquals(3, event.token().partitionEpoch());
            assertEquals(3, event.schemaEpoch());
            assertArrayEquals(buf(0x18).toByteArray(), event.key().toByteArray());
        }
    }

    @Test
    void legacyUnsealedReplayFailsClosedAfterSchemaEvolution(@TempDir Path directory) throws Exception {
        var reloadedPartition = partition(4);
        var reloadedMetadata = metadata(schemaWithLongerKey(), 4, 8, reloadedPartition);
        var command = PartitionWriteCommand.single(
                requestId(),
                3,
                ReplicatedMutation.put(4, new Keys(buf(0x18)), text("value"))
        );

        try (var options = new Options().setCreateIfMissing(true);
             var db = RocksDB.open(options, directory.resolve("partition-db").toString())) {
            var applier = new RocksDBPartitionStateMachineApplier(reloadedMetadata, db);
            var stateMachine = new RockserverRatisStateMachine(applier,
                    replay -> PartitionWriteCommandValidator.validateForReplay(reloadedMetadata, reloadedPartition,
                            replay));

            var error = assertThrows(IllegalArgumentException.class,
                    () -> stateMachine.applyCommitted(5, PartitionWriteCommandCodec.encode(command)));

            assertEquals("partition write schema epoch does not match column schema epoch", error.getMessage());
            assertEquals(0, stateMachine.lastAppliedIndex());
            assertNull(db.get(RockserverStorageKeyspace.userData(4, buf(0x18)).toByteArray()));
        }
    }

    private static RockserverRatisStateMachine persistentStateMachine(RocksDB db, ClusterMetadata metadata) {
        return persistentStateMachine(db, metadata, null);
    }

    private static RockserverRatisStateMachine persistentStateMachineWithCdc(RocksDB db, ClusterMetadata metadata) {
        var partition = partition();
        var applier = new RocksDBPartitionStateMachineApplier(metadata, partition, db);
        return new RockserverRatisStateMachine(applier,
                command -> PartitionWriteCommandValidator.validate(metadata, partition, command));
    }

    private static RockserverRatisStateMachine persistentStateMachine(RocksDB db,
                                                                      ClusterMetadata metadata,
                                                                      it.cavallium.rockserver.core.cluster.command.PartitionMergeResolver mergeResolver) {
        return persistentStateMachine(db, metadata, mergeResolver, ReplicationFailureInjector.none());
    }

    private static RockserverRatisStateMachine persistentStateMachine(RocksDB db,
                                                                      ClusterMetadata metadata,
                                                                      it.cavallium.rockserver.core.cluster.command.PartitionMergeResolver mergeResolver,
                                                                      ReplicationFailureInjector failureInjector) {
        var applier = mergeResolver == null
                ? new RocksDBPartitionStateMachineApplier(metadata, db, failureInjector)
                : new RocksDBPartitionStateMachineApplier(metadata, db, mergeResolver, failureInjector);
        var partition = partition();
        return new RockserverRatisStateMachine(applier,
                command -> PartitionWriteCommandValidator.validate(metadata, partition, command));
    }

    private static void applyCommands(RocksDB db,
                                      ClusterMetadata metadata,
                                      List<PartitionWriteCommand> commands) {
        var stateMachine = persistentStateMachine(db, metadata);
        for (int i = 0; i < commands.size(); i++) {
            stateMachine.applyCommitted(5L + i, PartitionWriteCommandCodec.encode(commands.get(i)));
        }
    }

    private static void assertPartitionStatePreserved(RocksDB db, Buf userKey) throws Exception {
        assertArrayEquals(text("value").toByteArray(), db.get(userKey.toByteArray()));
        var dedupeResponses = PartitionWriteResponseCodec.decodeResponses(
                db.get(RockserverStorageKeyspace.dedupeResponse(requestId()).toByteArray()));
        assertEquals(1, dedupeResponses.size());
        assertArrayEquals(text("previous").toByteArray(), ((Buf) dedupeResponses.getFirst()).toByteArray());
        assertEquals(ClientDedupeState.acknowledged(requestId()), ClientDedupeStateCodec.decode(
                db.get(RockserverStorageKeyspace.dedupeClientState(requestId().clientId()).toByteArray())));
        assertEquals(5, PartitionStateMetadataCodec.decodeLastAppliedIndex(
                db.get(RockserverStorageKeyspace.lastAppliedIndex().toByteArray())));
    }

    private static void assertIllegalArgument(String expectedMessage, org.junit.jupiter.api.function.Executable action) {
        var failure = assertThrows(IllegalArgumentException.class, action);

        assertEquals(expectedMessage, failure.getMessage());
    }

    private static ClusterMetadata metadata() {
        var column = new ClusterColumnMetadata(4, "accounts", schema(), 3);
        return new ClusterMetadata(new ClusterId(new UUID(0, 1)), 7, Map.of(), Map.of(4L, column));
    }

    private static ClusterMetadata metadata(ColumnSchema schema,
                                            long schemaEpoch,
                                            long metadataEpoch,
                                            PartitionDescriptor partition) {
        var column = new ClusterColumnMetadata(4, "accounts", schema, schemaEpoch);
        return new ClusterMetadata(new ClusterId(new UUID(0, 1)), metadataEpoch, Map.of(), Map.of(4L, column),
                Map.of(partition.partitionId(), partition));
    }

    private static ColumnSchema schema() {
        return ColumnSchema.of(IntArrayList.of(1), ObjectArrayList.of(), true);
    }

    private static ColumnSchema schemaWithLongerKey() {
        return ColumnSchema.of(IntArrayList.of(2), ObjectArrayList.of(), true);
    }

    private static PartitionDescriptor partition() {
        return partition(3);
    }

    private static PartitionDescriptor partition(long epoch) {
        var leader = NodeId.of("n1");
        return new PartitionDescriptor(
                new ShardId(7),
                buf(0x10),
                buf(0x20),
                epoch,
                "group-7",
                List.of(new ReplicaMember(leader, URI.create("http://127.0.0.1:10001"), ReplicaRole.VOTER, 0, 0,
                        true)),
                leader,
                Map.of("rack", "rack-a"),
                PartitionSplitState.ACTIVE
        );
    }

    private static ClientRequestId requestId() {
        return requestId(7);
    }

    private static ClientRequestId requestId(long sequence) {
        return new ClientRequestId(new ClientId(new UUID(0, 1)), sequence);
    }

    private static final class ApplyingLogClient implements ReplicatedLogClient {

        private final RockserverRatisStateMachine stateMachine;
        private long nextIndex;

        private ApplyingLogClient(RockserverRatisStateMachine stateMachine, long nextIndex) {
            this.stateMachine = stateMachine;
            this.nextIndex = nextIndex;
        }

        @Override
        public CompletableFuture<ReplicatedLogCommit> propose(byte[] payload) {
            var logIndex = nextIndex++;
            stateMachine.applyCommitted(logIndex, payload);
            return CompletableFuture.completedFuture(new ReplicatedLogCommit(logIndex, true));
        }
    }

    private static Buf buf(int... values) {
        var bytes = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            bytes[i] = (byte) values[i];
        }
        return Buf.wrap(bytes);
    }

    private static Buf text(String value) {
        return Buf.wrap(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String textValue(Buf value) {
        return new String(value.toByteArray(), StandardCharsets.UTF_8);
    }
}
