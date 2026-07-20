package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.cluster.RatisQuorumCondition;
import it.cavallium.rockserver.core.cluster.WriteDurability;
import it.cavallium.rockserver.core.replication.ratis.RatisAdapterBoundary;
import it.cavallium.rockserver.core.replication.ratis.RatisAdapterBoundaryMode;
import it.cavallium.rockserver.core.replication.ratis.RatisPartitionDescriptor;
import it.cavallium.rockserver.core.replication.ratis.RatisPeerDescriptor;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterCommand;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class RatisAdapterBoundaryTest {

    @Test
    void supportedBoundaryModesKeepRatisOffProductionModulePath() {
        for (var mode : RatisAdapterBoundaryMode.values()) {
            assertFalse(mode.usesProductionModulePath());
        }

        var boundary = RatisAdapterBoundary.classpathIsolated("ratis-3.2.2-classpath-adapter");

        assertTrue(boundary.keepsRatisOffProductionModulePath());
        assertEquals(RatisAdapterBoundaryMode.CLASSPATH_ISOLATED_ADAPTER, boundary.mode());
    }

    @Test
    void boundaryRejectsLineBrokenImplementationNames() {
        assertThrows(IllegalArgumentException.class,
                () -> RatisAdapterBoundary.classpathIsolated("ratis\nclasspath"));
        assertThrows(IllegalArgumentException.class,
                () -> RatisAdapterBoundary.externalProcess("ratis\rprocess"));
    }

    @Test
    void boundaryRejectsNullInputsWithExplicitDiagnostics() {
        var nullMode = assertThrows(IllegalArgumentException.class,
                () -> new RatisAdapterBoundary(null, "test-ratis-adapter"));
        var nullImplementationName = assertThrows(IllegalArgumentException.class,
                () -> RatisAdapterBoundary.classpathIsolated(null));
        var nullPeerId = assertThrows(IllegalArgumentException.class,
                () -> new RatisPeerDescriptor(null, "grpc://127.0.0.1:19000", "rack-a"));
        var nullEndpoint = assertThrows(IllegalArgumentException.class,
                () -> new RatisPeerDescriptor("n1", null, "rack-a"));
        var nullFailureDomain = assertThrows(IllegalArgumentException.class,
                () -> new RatisPeerDescriptor("n1", "grpc://127.0.0.1:19000", null));
        var nullJavaExecutable = assertThrows(IllegalArgumentException.class, () -> adapterCommand(
                null,
                Path.of("/var/lib/rockserver/adapter-work"),
                "it.cavallium.rockserver.ratis.AdapterMain",
                Map.of()
        ));
        var nullWorkingDirectory = assertThrows(IllegalArgumentException.class, () -> adapterCommand(
                Path.of("/usr/lib/jvm/default/bin/java"),
                null,
                "it.cavallium.rockserver.ratis.AdapterMain",
                Map.of()
        ));
        var nullMainClass = assertThrows(IllegalArgumentException.class, () -> adapterCommand(
                Path.of("/usr/lib/jvm/default/bin/java"),
                Path.of("/var/lib/rockserver/adapter-work"),
                null,
                Map.of()
        ));
        var nullEnvironment = assertThrows(IllegalArgumentException.class, () -> adapterCommand(
                Path.of("/usr/lib/jvm/default/bin/java"),
                Path.of("/var/lib/rockserver/adapter-work"),
                "it.cavallium.rockserver.ratis.AdapterMain",
                null
        ));
        var environmentWithNullKey = new HashMap<String, String>();
        environmentWithNullKey.put(null, "value");
        var nullEnvironmentKey = assertThrows(IllegalArgumentException.class, () -> adapterCommand(
                Path.of("/usr/lib/jvm/default/bin/java"),
                Path.of("/var/lib/rockserver/adapter-work"),
                "it.cavallium.rockserver.ratis.AdapterMain",
                environmentWithNullKey
        ));
        var environmentWithNullValue = new HashMap<String, String>();
        environmentWithNullValue.put("SAFE_ENV", null);
        var nullEnvironmentValue = assertThrows(IllegalArgumentException.class, () -> adapterCommand(
                Path.of("/usr/lib/jvm/default/bin/java"),
                Path.of("/var/lib/rockserver/adapter-work"),
                "it.cavallium.rockserver.ratis.AdapterMain",
                environmentWithNullValue
        ));

        assertEquals("mode must not be null", nullMode.getMessage());
        assertEquals("implementationName must not be null", nullImplementationName.getMessage());
        assertEquals("peerId must not be null", nullPeerId.getMessage());
        assertEquals("endpoint must not be null", nullEndpoint.getMessage());
        assertEquals("failureDomain must not be null", nullFailureDomain.getMessage());
        assertEquals("javaExecutable must not be null", nullJavaExecutable.getMessage());
        assertEquals("workingDirectory must not be null", nullWorkingDirectory.getMessage());
        assertEquals("mainClass must not be null", nullMainClass.getMessage());
        assertEquals("environment must not be null", nullEnvironment.getMessage());
        assertEquals("environment key must not be null", nullEnvironmentKey.getMessage());
        assertEquals("environment value must not be null", nullEnvironmentValue.getMessage());
    }

    @Test
    void partitionDescriptorRequiresLocalPeerInVoters() {
        var voters = List.of(new RatisPeerDescriptor("n1", "grpc://127.0.0.1:19000", "rack-a"));

        var ex = assertThrows(IllegalArgumentException.class, () -> new RatisPartitionDescriptor(
                "p1",
                "g1",
                "n2",
                voters,
                Path.of("data/p1")
        ));

        assertEquals("localPeerId must be present in voters", ex.getMessage());
    }

    @Test
    void partitionDescriptorRejectsNullVotersAndDataPathsWithExplicitDiagnostics() {
        var voter = new RatisPeerDescriptor("n1", "grpc://127.0.0.1:19000", "rack-a");
        var votersWithNullEntry = new ArrayList<RatisPeerDescriptor>();
        votersWithNullEntry.add(voter);
        votersWithNullEntry.add(null);

        var nullVoters = assertThrows(IllegalArgumentException.class, () -> new RatisPartitionDescriptor(
                "p1",
                "g1",
                "n1",
                null,
                Path.of("data/p1")
        ));
        var nullVoterEntry = assertThrows(IllegalArgumentException.class, () -> new RatisPartitionDescriptor(
                "p1",
                "g1",
                "n1",
                votersWithNullEntry,
                Path.of("data/p1")
        ));
        var nullDataPath = assertThrows(IllegalArgumentException.class, () -> new RatisPartitionDescriptor(
                "p1",
                "g1",
                "n1",
                List.of(voter),
                null
        ));
        var blankDataPath = assertThrows(IllegalArgumentException.class, () -> new RatisPartitionDescriptor(
                "p1",
                "g1",
                "n1",
                List.of(voter),
                Path.of("")
        ));
        var lineUnsafeDataPath = assertThrows(IllegalArgumentException.class, () -> new RatisPartitionDescriptor(
                "p1",
                "g1",
                "n1",
                List.of(voter),
                Path.of("data/p1\ninjected=true")
        ));

        assertEquals("voters must not be null", nullVoters.getMessage());
        assertEquals("voters entry must not be null", nullVoterEntry.getMessage());
        assertEquals("dataPath must not be null", nullDataPath.getMessage());
        assertEquals("dataPath must not be blank", blankDataPath.getMessage());
        assertEquals("dataPath must not contain NUL, CR, or LF", lineUnsafeDataPath.getMessage());
    }

    @Test
    void partitionDescriptorRejectsDuplicateVoters() {
        assertThrows(IllegalArgumentException.class, () -> new RatisPartitionDescriptor(
                "p1",
                "g1",
                "n1",
                List.of(
                        new RatisPeerDescriptor("n1", "grpc://127.0.0.1:19000", "rack-a"),
                        new RatisPeerDescriptor("n1", "grpc://127.0.0.1:19001", "rack-b")
                ),
                Path.of("data/p1")
        ));

        assertThrows(IllegalArgumentException.class, () -> new RatisPartitionDescriptor(
                "p1",
                "g1",
                "n1",
                List.of(
                        new RatisPeerDescriptor("n1", "grpc://127.0.0.1:19000", "rack-a"),
                        new RatisPeerDescriptor("n2", "grpc://127.0.0.1:19000", "rack-b")
                ),
                Path.of("data/p1")
        ));
    }

    @Test
    void partitionAndPeerDescriptorsRejectLineBrokenIdentifiers() {
        var voters = List.of(new RatisPeerDescriptor("n1", "grpc://127.0.0.1:19000", "rack-a"));

        assertThrows(IllegalArgumentException.class, () -> new RatisPartitionDescriptor(
                "p1\ninjected=true",
                "g1",
                "n1",
                voters,
                Path.of("data/p1")
        ));
        assertThrows(IllegalArgumentException.class, () -> new RatisPartitionDescriptor(
                "p1",
                "g1\rbroken",
                "n1",
                voters,
                Path.of("data/p1")
        ));
        assertThrows(IllegalArgumentException.class,
                () -> new RatisPeerDescriptor("n1\ninjected=true", "grpc://127.0.0.1:19000", "rack-a"));
        assertThrows(IllegalArgumentException.class,
                () -> new RatisPeerDescriptor("n1", "grpc://127.0.0.1:19000", "rack-a\rbroken"));
    }

    @Test
    void partitionDescriptorCopiesVoters() {
        var voters = new ArrayList<RatisPeerDescriptor>();
        voters.add(new RatisPeerDescriptor("n1", "grpc://127.0.0.1:19000", "rack-a"));

        var descriptor = new RatisPartitionDescriptor("p1", "g1", "n1", voters, Path.of("data/p1"));
        voters.add(new RatisPeerDescriptor("n2", "grpc://127.0.0.1:19001", "rack-b"));

        assertEquals(1, descriptor.voters().size());
        assertThrows(UnsupportedOperationException.class,
                () -> descriptor.voters().add(new RatisPeerDescriptor("n3", "grpc://127.0.0.1:19002", "rack-c")));
    }

    @Test
    void commitOnlyDurabilityFallsBackToApplyWhenPreApplyAckIsUnavailable() {
        assertTrue(WriteDurability.QUORUM_COMMIT.requiresPreApplyCommitAckPath());
        assertFalse(WriteDurability.QUORUM_COMMIT_APPLY.requiresPreApplyCommitAckPath());
        assertFalse(WriteDurability.QUORUM_COMMIT_APPLY_FLUSH.requiresPreApplyCommitAckPath());

        assertEquals(WriteDurability.QUORUM_COMMIT_APPLY, WriteDurability.QUORUM_COMMIT.applyBasedFallback());
        assertEquals(WriteDurability.QUORUM_COMMIT_APPLY, WriteDurability.QUORUM_COMMIT_APPLY.applyBasedFallback());
        assertEquals(WriteDurability.QUORUM_COMMIT_APPLY_FLUSH,
                WriteDurability.QUORUM_COMMIT_APPLY_FLUSH.applyBasedFallback());
    }

    @Test
    void mapsWriteDurabilityToRatisRocksDbMatrix() {
        var commitOnly = WriteDurability.QUORUM_COMMIT.matrixEntry(false);
        assertEquals(WriteDurability.QUORUM_COMMIT, commitOnly.requestedDurability());
        assertEquals(WriteDurability.QUORUM_COMMIT_APPLY, commitOnly.effectiveAdapterDurability());
        assertEquals(RatisQuorumCondition.MAJORITY_COMMITTED, commitOnly.ratisQuorumCondition());
        assertFalse(commitOnly.modeRequiresLeaderApplyBeforeAck());
        assertTrue(commitOnly.adapterWaitsForLeaderApplyBeforeAck());
        assertFalse(commitOnly.modeRequiresRocksDbPersistenceBeforeAck());
        assertFalse(commitOnly.requiresResponsePersistenceBeforeAck());
        assertFalse(commitOnly.requiresExplicitFlushBeforeAck());
        assertTrue(commitOnly.responseProducingWritesAllowed());
        assertTrue(commitOnly.usesApplyBasedFallback());

        var commitOnlyResponse = WriteDurability.QUORUM_COMMIT.matrixEntry(true);
        assertFalse(commitOnlyResponse.responseProducingWritesAllowed());

        var commitApply = WriteDurability.QUORUM_COMMIT_APPLY.matrixEntry(true);
        assertEquals(WriteDurability.QUORUM_COMMIT_APPLY, commitApply.effectiveAdapterDurability());
        assertTrue(commitApply.modeRequiresLeaderApplyBeforeAck());
        assertTrue(commitApply.adapterWaitsForLeaderApplyBeforeAck());
        assertTrue(commitApply.modeRequiresRocksDbPersistenceBeforeAck());
        assertTrue(commitApply.requiresResponsePersistenceBeforeAck());
        assertFalse(commitApply.requiresExplicitFlushBeforeAck());
        assertTrue(commitApply.responseProducingWritesAllowed());
        assertFalse(commitApply.usesApplyBasedFallback());

        var commitApplyFlush = WriteDurability.QUORUM_COMMIT_APPLY_FLUSH.matrixEntry(true);
        assertEquals(WriteDurability.QUORUM_COMMIT_APPLY_FLUSH, commitApplyFlush.effectiveAdapterDurability());
        assertTrue(commitApplyFlush.modeRequiresRocksDbPersistenceBeforeAck());
        assertTrue(commitApplyFlush.requiresResponsePersistenceBeforeAck());
        assertTrue(commitApplyFlush.requiresExplicitFlushBeforeAck());
        assertTrue(commitApplyFlush.responseProducingWritesAllowed());
        assertFalse(commitApplyFlush.usesApplyBasedFallback());
    }

    private static RatisProcessAdapterCommand adapterCommand(Path javaExecutable,
                                                             Path workingDirectory,
                                                             String mainClass,
                                                             Map<String, String> environment) {
        return new RatisProcessAdapterCommand(
                javaExecutable,
                workingDirectory,
                List.of(Path.of("/opt/rockserver-ratis-adapter/adapter/rockserver-ratis-adapter.jar")),
                mainClass,
                List.of("-Xmx512m"),
                List.of("--print-health"),
                environment
        );
    }
}
