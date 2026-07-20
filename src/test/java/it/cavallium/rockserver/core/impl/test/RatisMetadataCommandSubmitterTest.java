package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.cluster.ClusterColumnMetadata;
import it.cavallium.rockserver.core.cluster.ClusterId;
import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.command.ReplicatedLogCommit;
import it.cavallium.rockserver.core.cluster.metadata.MetadataCommand;
import it.cavallium.rockserver.core.cluster.metadata.MetadataCommandCodec;
import it.cavallium.rockserver.core.cluster.metadata.MetadataStateMachine;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.replication.ratis.RatisMetadataCommandSubmitter;
import it.cavallium.rockserver.core.replication.ratis.RatisPartitionClient;
import it.cavallium.rockserver.core.replication.ratis.RatisPartitionDescriptor;
import it.cavallium.rockserver.core.replication.ratis.RatisPeerDescriptor;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterMetadataConfig;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RatisMetadataCommandSubmitterTest {

    @Test
    void proposesMetadataCommandsAndReadsCommittedSnapshotThroughRatisClient() {
        var metadata = new ClusterMetadata(new ClusterId(new UUID(0, 1)), 7, Map.of());
        var client = new ApplyingMetadataClient(metadata);
        var submitter = new RatisMetadataCommandSubmitter(client);
        var command = new MetadataCommand.CreateColumn(
                7,
                new ClusterColumnMetadata(42, "accounts", schema(), 0)
        );

        var committed = submitter.submit(command);

        assertEquals(command, MetadataCommandCodec.decode(client.proposedPayload));
        assertArrayEquals(RatisMetadataCommandSubmitter.snapshotQueryPayload(), client.queryPayload);
        assertEquals(8, committed.epoch());
        assertEquals("accounts", committed.requireColumn(42).name());
    }

    @Test
    void writesAndReadsSnapshotFile(@TempDir Path tempDir) throws Exception {
        var metadata = new ClusterMetadata(new ClusterId(new UUID(0, 1)), 7, Map.of());
        var path = tempDir.resolve("metadata.snapshot");

        RatisMetadataCommandSubmitter.writeSnapshot(path, metadata);

        assertEquals(metadata, RatisMetadataCommandSubmitter.readSnapshot(path));
    }

    @Test
    void rejectsNullInputsWithExplicitDiagnostics(@TempDir Path tempDir) {
        var metadata = new ClusterMetadata(new ClusterId(new UUID(0, 1)), 7, Map.of());
        var snapshotPath = tempDir.resolve("metadata.snapshot");
        var nullConstructorClient = assertThrows(IllegalArgumentException.class,
                () -> new RatisMetadataCommandSubmitter(null));
        var nullReadClient = assertThrows(IllegalArgumentException.class,
                () -> RatisMetadataCommandSubmitter.readCommittedMetadata(null));
        var nullWritePath = assertThrows(IllegalArgumentException.class,
                () -> RatisMetadataCommandSubmitter.writeSnapshot(null, metadata));
        var nullReadPath = assertThrows(IllegalArgumentException.class,
                () -> RatisMetadataCommandSubmitter.readSnapshot(null));
        var nullEncodedMetadata = assertThrows(IllegalArgumentException.class,
                () -> RatisMetadataCommandSubmitter.encodeSnapshot(null));
        var nullWrittenMetadata = assertThrows(IllegalArgumentException.class,
                () -> RatisMetadataCommandSubmitter.writeSnapshot(snapshotPath, null));

        assertEquals("metadataClient must not be null", nullConstructorClient.getMessage());
        assertEquals("metadataClient must not be null", nullReadClient.getMessage());
        assertEquals("path must not be null", nullWritePath.getMessage());
        assertEquals("path must not be null", nullReadPath.getMessage());
        assertEquals("metadata must not be null", nullEncodedMetadata.getMessage());
        assertEquals("metadata must not be null", nullWrittenMetadata.getMessage());
    }

    @Test
    void readSnapshotRejectsOversizedFileBeforeLoading(@TempDir Path tempDir) throws Exception {
        var path = tempDir.resolve("metadata.snapshot");
        try (var file = new RandomAccessFile(path.toFile(), "rw")) {
            file.setLength(RatisProcessAdapterMetadataConfig.MAX_METADATA_CONFIG_BYTES + 1L);
        }

        assertEquals("Invalid Ratis process adapter metadata payload length: "
                        + (RatisProcessAdapterMetadataConfig.MAX_METADATA_CONFIG_BYTES + 1L),
                assertThrows(IllegalArgumentException.class,
                        () -> RatisMetadataCommandSubmitter.readSnapshot(path))
                        .getMessage());
    }

    @Test
    void readCommittedMetadataPropagatesNestedFatalQueryFailureCause() {
        try (var client = new ApplyingMetadataClient(new ClusterMetadata(new ClusterId(new UUID(0, 1)), 7, Map.of()))) {
            var failure = new AssertionError("query failed");
            client.queryResponse = CompletableFuture.failedFuture(new CompletionException(new CompletionException(failure)));

            assertSame(failure, assertThrows(AssertionError.class,
                    () -> RatisMetadataCommandSubmitter.readCommittedMetadata(client)));
            assertArrayEquals(RatisMetadataCommandSubmitter.snapshotQueryPayload(), client.queryPayload);
        }
    }

    @Test
    void readCommittedMetadataFlattensNestedCheckedQueryFailureCause() {
        try (var client = new ApplyingMetadataClient(new ClusterMetadata(new ClusterId(new UUID(0, 1)), 7, Map.of()))) {
            var failure = new IOException("query failed");
            client.queryResponse = CompletableFuture.failedFuture(new CompletionException(new CompletionException(failure)));

            var thrown = assertThrows(CompletionException.class,
                    () -> RatisMetadataCommandSubmitter.readCommittedMetadata(client));

            assertEquals("metadata snapshot query failed", thrown.getMessage());
            assertSame(failure, thrown.getCause());
            assertArrayEquals(RatisMetadataCommandSubmitter.snapshotQueryPayload(), client.queryPayload);
        }
    }

    @Test
    void interruptionCancelsCommittedSnapshotQueryAndRestoresInterruptFlag() {
        try (var client = new ApplyingMetadataClient(new ClusterMetadata(new ClusterId(new UUID(0, 1)), 7, Map.of()))) {
            var response = new CompletableFuture<byte[]>();
            client.queryResponse = response;

            Thread.currentThread().interrupt();
            try {
                var failure = assertThrows(CompletionException.class,
                        () -> RatisMetadataCommandSubmitter.readCommittedMetadata(client));

                assertInstanceOf(InterruptedException.class, failure.getCause());
                assertEquals("metadata snapshot query interrupted", failure.getMessage());
                assertTrue(response.isCancelled());
                assertTrue(Thread.interrupted());
            } finally {
                clearInterruptFlag();
            }
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static void clearInterruptFlag() {
        Thread.interrupted();
    }

    private static ColumnSchema schema() {
        return ColumnSchema.of(IntArrayList.of(1), ObjectArrayList.of(), true);
    }

    private static final class ApplyingMetadataClient implements RatisPartitionClient {

        private final MetadataStateMachine stateMachine;
        private long nextIndex = 1;
        private byte[] proposedPayload;
        private byte[] queryPayload;
        private CompletableFuture<byte[]> queryResponse;

        private ApplyingMetadataClient(ClusterMetadata metadata) {
            this.stateMachine = new MetadataStateMachine(metadata);
        }

        @Override
        public RatisPartitionDescriptor descriptor() {
            return new RatisPartitionDescriptor(
                    "metadata-group",
                    "metadata-raft-group",
                    "n1",
                    List.of(new RatisPeerDescriptor("n1", "grpc://127.0.0.1:19001", "rack-a")),
                    Path.of("target", "metadata-group")
            );
        }

        @Override
        public CompletableFuture<ReplicatedLogCommit> propose(byte[] payload) {
            proposedPayload = payload;
            var index = nextIndex++;
            stateMachine.applyCommitted(index, payload);
            return CompletableFuture.completedFuture(new ReplicatedLogCommit(index, true));
        }

        @Override
        public CompletableFuture<ReplicatedLogCommit> waitForCommit(long index) {
            return CompletableFuture.completedFuture(new ReplicatedLogCommit(index, true));
        }

        @Override
        public CompletableFuture<byte[]> queryLinearizable(byte[] request) {
            queryPayload = request;
            if (queryResponse == null) {
                return CompletableFuture.completedFuture(
                        RatisMetadataCommandSubmitter.encodeSnapshot(stateMachine.metadata())
                );
            }
            return queryResponse;
        }

        @Override
        public void close() {
        }
    }
}
