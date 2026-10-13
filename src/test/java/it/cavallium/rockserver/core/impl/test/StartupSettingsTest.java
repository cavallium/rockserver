package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.config.StartupSettings;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class StartupSettingsTest {
    @TempDir Path directory;
    private int serial;

    private StartupSettings resolve(String text) throws Exception {
        Path file = directory.resolve("config-" + serial++ + ".conf");
        Files.writeString(file, text);
        return StartupSettings.resolve(ConfigParser.parse(file), directory.resolve("db"), 5);
    }
    private static final String ENABLED_SNAPSHOT = """
            database.global.hot-cache-snapshot { enabled: true, directory: "../ssd" }
            database.global.block-cache-metadata-size: 128MiB
            """;
    private static final String ENABLED_ADMISSION = """
            database.global.open-files-async: true
            database.global.startup-io {
              enabled: true
              default-storage-id: hdd
              storage-groups: [ { id: hdd, paths: [data], workload-groups: [default] } ]
            }
            """;

    @Test void actualDecoderKeepsBothFeaturesOffWithoutChangingCacheEngine() throws Exception {
        var config = ConfigParser.parseDefault();
        assertFalse(config.global().startupIo().enabled());
        assertFalse(config.global().hotCacheSnapshot().enabled());
        var resolved = StartupSettings.resolve(config, directory, 5);
        assertFalse(resolved.admission().enabled()); assertTrue(resolved.admission().storageGroups().isEmpty());
        assertNull(resolved.admission().storageFor(directory));
        assertFalse(resolved.caches().get("default").enabled());
        assertEquals(0, resolved.snapshotAuxiliaryBytes());
        assertNull(resolved.caches().get("default").directory());
        assertFalse(config.global().useClockCache());
        assertEquals(65L << 20, resolved.caches().get("default").data().lookupBufferBytes());
        // Disabled admission does not require an unfinished storage mapping or async opening.
        assertFalse(resolve("database.global.startup-io { enabled: false, default-storage-id: missing }")
                .admission().enabled());
    }

    @Test void namedPoliciesIndependentlyInheritDataMetadataDirectoryAndEnabledState() throws Exception {
        var resolved = resolve(ENABLED_SNAPSHOT + """
                database.global.hot-cache-snapshot.data { sample-every: 32, period: PT0S }
                database.global.hot-cache-snapshot.metadata { sample-every: 8 }
                database.global.block-caches: [
                  { name: sender, size: 64MiB, metadata-size: 16MiB,
                    snapshot: { data: { sample-every: 4 }, metadata: { read-ceiling: 2MiB } } },
                  { name: cold, size: 32MiB, snapshot: { enabled: false } }
                ]
                """);
        var defaults = resolved.caches().get("default"); var sender = resolved.caches().get("sender");
        assertTrue(sender.enabled()); assertEquals(directory.resolve("ssd"), sender.directory());
        assertEquals(4, sender.data().sampleEvery()); assertEquals(8, sender.metadata().sampleEvery());
        assertEquals(defaults.data().snapshotBudgetBytes(), sender.data().snapshotBudgetBytes());
        assertEquals(defaults.metadata().diskBudgetBytes(), sender.metadata().diskBudgetBytes());
        assertEquals(2L << 20, sender.metadata().readCeilingBytesPerSecond());
        assertEquals(0, sender.data().periodMillis()); assertEquals(300000, sender.metadata().periodMillis());
        assertFalse(resolved.caches().get("cold").enabled());
        assertThrows(UnsupportedOperationException.class, () -> resolved.caches().clear());
    }

    @Test void namedCacheCanEnableSnapshotsWhileGlobalCacheRemainsOff() throws Exception {
        var settings = resolve("""
                database.global.hot-cache-snapshot { enabled: false, directory: "../ssd" }
                database.global.block-caches: [
                  { name: sender, size: 64MiB, metadata-size: 16MiB, snapshot: { enabled: true } }
                ]
                """);
        assertFalse(settings.caches().get("default").enabled());
        assertTrue(settings.caches().get("sender").enabled());
        assertEquals(directory.resolve("ssd"), settings.caches().get("sender").directory());
        assertEquals(65536, settings.caches().get("sender").data().collectorEntries());
        assertEquals(16384, settings.caches().get("sender").metadata().collectorEntries());
    }

    @Test void admissionRoutesPhysicalPrefixesAndWorkloadCohortsWithImmutableDefaults() throws Exception {
        var resolved = resolve(ENABLED_ADMISSION + """
                database.parallelism.workload-groups: [{name: sender}]
                database.global.startup-io.storage-groups: [
                  { id: hdd, paths: [data], workload-groups: [default] },
                  { id: ssd, paths: ["data/ssd"], workload-groups: [sender],
                    maximum-opens: 2, progress-interval: PT0.5S, stale-after: PT3S }
                ]
                """);
        var io = resolved.admission();
        assertEquals("ssd", io.storageFor(directory.resolve("db/data/ssd/table.sst")).id());
        assertEquals("hdd", io.storageFor(directory.resolve("db/data/other.sst")).id());
        assertEquals("hdd", io.storageFor(directory.resolve("elsewhere/table.sst")).id());
        assertEquals("ssd", io.storageForWorkload("sender").id());
        assertEquals("hdd", io.storageForWorkload(null).id());
        assertEquals(5, io.storageGroups().get("hdd").maximumOpens());
        assertEquals(500, io.storageGroups().get("ssd").progressIntervalMillis());
        assertThrows(UnsupportedOperationException.class, () -> io.storageGroups().get("hdd").paths().clear());
    }

    @Test void admissionRejectsUnqualifiedOpeningAndAmbiguousOrInvalidMappings() {
        for (String invalid : new String[] {
                "database.global.open-files-async: false",
                "database.global.maximum-open-files: 100",
                "database.global.startup-io.default-storage-id: missing",
                "database.global.startup-io.storage-groups: []",
                "database.global.startup-io.storage-groups: [{id: hdd}, {id: hdd}]",
                "database.global.startup-io.storage-groups: [{id: hdd, maximum-opens: 0}]",
                "database.global.startup-io.storage-groups: [{id: hdd, maximum-opens: 1025}]",
                "database.global.startup-io.storage-groups: [{id: hdd, progress-interval: PT0S}]",
                "database.global.startup-io.storage-groups: [{id: hdd, progress-interval: PT0.000001S}]",
                "database.global.startup-io.storage-groups: [{id: hdd, progress-interval: PT61S, stale-after: PT61S}]",
                "database.global.startup-io.storage-groups: [{id: hdd, progress-interval: PT0.001S, stale-after: PT0.049S}]",
                "database.global.startup-io.storage-groups: [{id: hdd, stale-after: PT61S}]",
                "database.global.startup-io.storage-groups: [{id: hdd, progress-interval: PT3S, stale-after: PT2S}]",
                "database.global.startup-io.storage-groups: [{id: hdd, paths: [data]}, {id: ssd, paths: [\"data/../data\"]}]",
                "database.global.startup-io.storage-groups: [{id: hdd, workload-groups: [default, default]}]",
                "database.global.startup-io.storage-groups: [{id: hdd, workload-groups: [missing]}]"
        }) assertThrows(IllegalArgumentException.class, () -> resolve(ENABLED_ADMISSION + invalid), invalid);
    }

    @Test void snapshotRejectsMissingDirectoryAndInvalidNativeQuotasBeforeAnyNativeAllocation() {
        assertThrows(IllegalArgumentException.class, () -> resolve("database.global.hot-cache-snapshot.enabled: true"));
        assertThrows(IllegalArgumentException.class, () -> resolve(ENABLED_SNAPSHOT
                + "database.global.block-cache-metadata-size: 0B"));
        for (String invalid : new String[] {
                "disk-budget: 0B", "snapshot-budget: 2GiB", "snapshot-budget: 127B", "snapshot-budget: 1KiB",
                "collector-entries: 65", "collector-entries: 2097152", "index-ram: 1B",
                "maximum-block: 0B", "maximum-block: 17MiB", "lookup-buffer: 64MiB",
                "minimum-free-disk: -1B", "snapshot-deadline: PT0S", "snapshot-deadline: PT2H",
                "snapshot-deadline: PT0.000001S", "period: PT25H", "sample-every: 3",
                "lookup-concurrency: 0", "lookup-concurrency: 65", "read-ceiling: -1B", "write-ceiling: 1B"
        }) assertThrows(IllegalArgumentException.class, () -> resolve(ENABLED_SNAPSHOT
                + "database.global.hot-cache-snapshot.data { " + invalid + " }"), invalid);
        assertDoesNotThrow(() -> resolve(ENABLED_SNAPSHOT + """
                database.parallelism.workload-groups: [{name: __shared}]
                """));
    }

    @Test void sourceCacheNamesArePreservedForLoaderGeneratedNamespaces() throws Exception {
        var settings = resolve(ENABLED_SNAPSHOT + """
                database.global.block-caches: [{name: "source/name", size: 64MiB, metadata-size: 16MiB}]
                """);
        assertTrue(settings.caches().get("source/name").enabled());
    }

    @Test void knownSnapshotMemoryQuotasAreAccountedAndCombinedOverflowIsRejected() throws Exception {
        var settings = resolve(ENABLED_SNAPSHOT);
        var cache = settings.caches().get("default");
        long expected = 0;
        for (var policy : java.util.List.of(cache.data(), cache.metadata())) {
            expected += policy.collectorEntries() * 32 + policy.indexRamBytes()
                    + policy.maximumBlockBytes() + policy.lookupBufferBytes();
        }
        assertEquals(expected, settings.snapshotAuxiliaryBytes());
        assertThrows(IllegalArgumentException.class, () -> resolve(ENABLED_SNAPSHOT
                + "database.global.hot-cache-snapshot.data.index-ram: 9223372036854775807B"));
    }

    @Test void zeroCeilingsAndExplicitCapturePeriodAreValidAndCapacityOverflowIsRejected() throws Exception {
        var settings = resolve(ENABLED_SNAPSHOT + """
                database.global.hot-cache-snapshot.data {
                  minimum-free-disk: 0B, read-ceiling: 0B, write-ceiling: 0B, period: PT0S
                }
                """);
        var data = settings.caches().get("default").data();
        assertEquals(0, data.readCeilingBytesPerSecond()); assertEquals(0, data.writeCeilingBytesPerSecond());
        assertEquals(0, data.periodMillis()); assertEquals(0, data.minimumFreeDiskBytes());
        assertThrows(IllegalArgumentException.class, () -> resolve(ENABLED_SNAPSHOT
                + "database.global.block-cache: 9223372036854775807B"));
        assertThrows(IllegalArgumentException.class, () -> resolve(ENABLED_SNAPSHOT
                + "database.global.hot-cache-snapshot.data.snapshot-deadline: PT100000000000000S"));
    }
}
