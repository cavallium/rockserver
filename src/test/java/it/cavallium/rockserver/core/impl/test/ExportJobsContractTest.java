package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.WorkloadCost;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.common.export.ExportAsyncAPI;
import it.cavallium.rockserver.core.common.export.ExportCommand;
import it.cavallium.rockserver.core.common.export.ExportJobs;
import java.io.File;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ExportJobsContractTest {
    private final UUID job = new UUID(0, 1), group = new UUID(0, 2), lineage = new UUID(0, 3);
    private ExportJobs.ChunkId id(byte[] hash) { return new ExportJobs.ChunkId(job, group, lineage, 1, 1, 2, 0, hash); }
    private ExportJobs.Chunk chunk() { return new ExportJobs.Chunk(id(new byte[32]), -1, new byte[]{1}, new byte[]{2}, 1, 2, 20, true); }

    @Test void signedColumnsAreCanonicalUniqueAndImmutable() {
        var columns = new ArrayList<>(List.of(Long.MAX_VALUE, 0L, Long.MIN_VALUE));
        var create = new ExportJobs.Create(job, columns, ExportJobs.Limits.defaults());
        columns.clear();
        assertEquals(List.of(Long.MIN_VALUE, 0L, Long.MAX_VALUE), create.columns());
        assertThrows(UnsupportedOperationException.class, () -> create.columns().add(1L));
        assertThrows(IllegalArgumentException.class, () -> new ExportJobs.Create(job, List.of(1L, 1L), create.limits()));
        assertThrows(IllegalArgumentException.class, () -> new ExportJobs.Create(job, List.of(), create.limits()));
        assertThrows(NullPointerException.class, () -> new ExportJobs.Create(null, List.of(1L), create.limits()));
        assertThrows(NullPointerException.class, () -> new ExportJobs.Create(job, Arrays.asList((Long)null), create.limits()));
        assertThrows(IllegalArgumentException.class, () -> new ExportJobs.Create(job,
                java.util.stream.LongStream.range(0, 257).boxed().toList(), create.limits()));
    }

    @Test void identitiesHaveDeepEqualityAndAllBuffersAreCopied() {
        byte[] hash = new byte[32];
        var first = id(hash); var equal = id(hash.clone());
        hash[0] = 1; first.contentHash()[0] = 2;
        assertEquals(equal, first); assertEquals(equal.hashCode(), first.hashCode());
        assertNotEquals(first, id(hash));
        byte[] lower = {1}, upper = {2};
        var chunk = new ExportJobs.Chunk(first, -1, lower, upper, 1, 2, 20, true);
        lower[0] = 8; upper[0] = 9; chunk.lowerInclusive()[0] = 5; chunk.upperExclusive()[0] = 6;
        assertArrayEquals(new byte[]{1}, chunk.lowerInclusive()); assertArrayEquals(new byte[]{2}, chunk.upperExclusive());
        byte[] bytes = {7}; var page = new ExportJobs.FilePage(first, 0, bytes, false);
        bytes[0] = 8; page.bytes()[0] = 9; assertArrayEquals(new byte[]{7}, page.bytes());
        var chunks = new ArrayList<>(List.of(chunk)); var manifest = new ExportJobs.ManifestPage(chunks, 1, false);
        chunks.clear(); assertEquals(List.of(chunk), manifest.chunks());
        assertThrows(UnsupportedOperationException.class, () -> manifest.chunks().clear());
    }

    @Test void limitAndPageDimensionsMatchBackendBounds() {
        var defaults = ExportJobs.Limits.defaults();
        assertEquals(4096, defaults.maximumChunks()); assertEquals(Duration.ofSeconds(60), defaults.snapshotTtl());
        assertThrows(IllegalArgumentException.class, () -> limits(65537, defaults.maximumJournalBytes(), defaults.maximumDiskBytes(), Duration.ofSeconds(60)));
        assertThrows(IllegalArgumentException.class, () -> limits(1, 8L * 1024 * 1024, defaults.maximumDiskBytes(), Duration.ofSeconds(60)));
        assertThrows(IllegalArgumentException.class, () -> limits(1, 65L * 1024 * 1024, defaults.maximumDiskBytes(), Duration.ofSeconds(60)));
        assertThrows(IllegalArgumentException.class, () -> limits(1, defaults.maximumJournalBytes(), 1, Duration.ofSeconds(60)));
        assertThrows(IllegalArgumentException.class, () -> limits(1, defaults.maximumJournalBytes(), defaults.maximumDiskBytes(), Duration.ofSeconds(61)));
        assertThrows(IllegalArgumentException.class, () -> limits(1, defaults.maximumJournalBytes(), defaults.maximumDiskBytes(), Duration.ofNanos(500)));
        assertThrows(IllegalArgumentException.class, () -> limits(1, defaults.maximumJournalBytes(), defaults.maximumDiskBytes(), Duration.ofNanos(1_000_001)));
        assertDoesNotThrow(() -> limits(1, defaults.maximumJournalBytes(), defaults.maximumDiskBytes(), Duration.ofMillis(1)));
        assertDoesNotThrow(() -> limits(1, defaults.maximumJournalBytes(), defaults.maximumDiskBytes(), Duration.ofMillis(60_000)));
        assertThrows(IllegalArgumentException.class, () -> new ExportJobs.ManifestRequest(job, -1, 1, 65536));
        assertThrows(IllegalArgumentException.class, () -> new ExportJobs.ManifestRequest(job, 0, 129, 65536));
        assertThrows(IllegalArgumentException.class, () -> new ExportJobs.ManifestRequest(job, 0, 1, 65535));
        assertThrows(IllegalArgumentException.class, () -> new ExportJobs.ManifestPage(List.of(), 0, true));
        assertThrows(IllegalArgumentException.class, () -> new ExportJobs.ManifestPage(List.of(chunk()), 0, false));
        assertThrows(IllegalArgumentException.class, () -> new ExportJobs.ReadRequest(id(new byte[32]), 0, 1024 * 1024 + 1));
        assertThrows(IllegalArgumentException.class, () -> new ExportJobs.FilePage(id(new byte[32]), Long.MAX_VALUE, new byte[]{1}, false));
        assertThrows(IllegalArgumentException.class, () -> new ExportJobs.FilePage(id(new byte[32]), 0, new byte[0], true));
        assertThrows(IllegalArgumentException.class, () -> new ExportJobs.Chunk(id(new byte[32]), 1, new byte[ExportJobs.MAX_BOUND_BYTES], new byte[]{1}, 1, 1, 1, false));
        assertThrows(IllegalArgumentException.class, () -> new ExportJobs.Chunk(id(new byte[32]), 1, null, null, 0, 0, 1, false));
        assertDoesNotThrow(() -> new ExportJobs.Chunk(id(new byte[32]), 1, null, null, 0, 0, 0, false));
    }

    private ExportJobs.Limits limits(int chunks, long journal, long disk, Duration ttl) {
        return new ExportJobs.Limits(chunks, journal, disk, 65536, 16L * 1024 * 1024,
                32L * 1024 * 1024, Duration.ofSeconds(30), ttl);
    }

    @Test void commandDispatchPreservesRequestContextAndFuture() {
        var context = it.cavallium.rockserver.core.common.RequestContext.batch();
        var future = new java.util.concurrent.CompletableFuture<>();
        var calls = new ArrayList<String>();
        var arguments = new ArrayList<Object>();
        var api = (ExportAsyncAPI) java.lang.reflect.Proxy.newProxyInstance(
                ExportAsyncAPI.class.getClassLoader(), new Class<?>[]{ExportAsyncAPI.class}, (proxy, method, args) -> {
                    calls.add(method.getName()); arguments.add(args[0]); assertSame(context, args[1]); return future;
                });
        var create = new ExportJobs.Create(job, List.of(1L), ExportJobs.Limits.defaults());
        var manifest = new ExportJobs.ManifestRequest(job, 0, 1, 65536);
        var read = new ExportJobs.ReadRequest(id(new byte[32]), 0, 65536);
        var chunkId = id(new byte[32]);
        var commands = List.<ExportCommand<?>>of(new ExportCommand.Create(create), new ExportCommand.Recover(job),
                new ExportCommand.Step(job), new ExportCommand.Status(job), new ExportCommand.Manifest(manifest),
                new ExportCommand.Read(read), new ExportCommand.Acknowledge(chunkId),
                new ExportCommand.Release(job), new ExportCommand.Cancel(job));
        for (var command : commands) assertSame(future, command.handleAsync(api, context));
        assertEquals(List.of("createExport", "recoverExport", "stepExport", "exportStatus", "exportManifest",
                "readExport", "acknowledgeExport", "releaseExport", "cancelExport"), calls);
        var expected = List.of(create, job, job, job, manifest, read, chunkId, job, job);
        for (int i = 0; i < expected.size(); i++) assertSame(expected.get(i), arguments.get(i));
        assertFalse(future.isDone());
    }

    @Test void publicContractHasNoHostPathsAndCommandsOwnAdmissionMetadata() {
        for (var type : ExportJobs.class.getDeclaredClasses()) {
            if (!type.isRecord()) continue;
            for (var field : type.getRecordComponents()) {
                assertNotEquals(Path.class, field.getType()); assertNotEquals(File.class, field.getType());
                assertFalse(field.getName().toLowerCase(java.util.Locale.ROOT).contains("path"));
            }
        }
        for (var method : ExportAsyncAPI.class.getDeclaredMethods()) {
            assertEquals(java.util.concurrent.CompletableFuture.class, method.getReturnType());
        }
        var create = new ExportCommand.Create(new ExportJobs.Create(job, List.of(1L), ExportJobs.Limits.defaults()));
        assertEquals(OperationFamily.MUTATION, create.operationFamily()); assertEquals(WorkloadProfile.BATCH, create.defaultProfile());
        assertEquals(OperationFamily.FULL_SCAN_AGGREGATE, new ExportCommand.Step(job).operationFamily());
        assertEquals(OperationFamily.METADATA, new ExportCommand.Status(job).operationFamily());
        var manifest = new ExportCommand.Manifest(new ExportJobs.ManifestRequest(job, 0, 128, ExportJobs.MAX_MANIFEST_BYTES));
        assertEquals(OperationFamily.RANGE_PAGE, manifest.operationFamily());
        assertTrue(manifest.estimatedBytes() >= ExportJobs.MAX_MANIFEST_BYTES);
        assertTrue(manifest.estimatedBytes() <= WorkloadCost.MAX_ESTIMATED_BYTES);
        for (ExportCommand<?> command : List.of(new ExportCommand.Acknowledge(id(new byte[32])), new ExportCommand.Release(job), new ExportCommand.Cancel(job))) {
            assertEquals(OperationFamily.CONTROL, command.operationFamily());
            assertEquals(WorkloadProfile.CONTROL, command.protectedProfile());
        }
    }
}
