package it.cavallium.rockserver.core.impl.test;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.impl.FFMAbstractMergeOperator;
import it.cavallium.rockserver.core.impl.MergeOperatorRegistry;
import it.cavallium.rockserver.core.impl.MergeOperatorRegistry.DeterministicContract;
import it.cavallium.rockserver.core.impl.MergeOperatorRegistry.DistributedOperatorDescriptor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.junit.jupiter.api.Assertions.*;

class DistributedMergeOperatorRegistryTest {
    @TempDir Path directory;
    private static final DeterministicContract QUALIFIED = new DeterministicContract("append-contract", 1, true);

    /** Observes constructor input without relying on the RocksDB native name callback. */
    public static class RecordingOperator extends FFMAbstractMergeOperator {
        private final String identity;
        public RecordingOperator(String identity) {
            super(identity);
            this.identity = identity;
        }
        public String constructorIdentity() { return identity; }
        @Override public Buf merge(Buf key, Buf existingValue, List<Buf> operands) {
            return existingValue;
        }
    }

    private byte[] artifact(boolean includeClass) throws Exception {
        var bytes = new ByteArrayOutputStream();
        try (var jar = new JarOutputStream(bytes)) {
            if (includeClass) {
                String entry = RecordingOperator.class.getName().replace('.', '/') + ".class";
                jar.putNextEntry(new JarEntry(entry));
                try (var source = RecordingOperator.class.getClassLoader().getResourceAsStream(entry)) {
                    assertNotNull(source);
                    source.transferTo(jar);
                }
                jar.closeEntry();
            }
        }
        return bytes.toByteArray();
    }

    private DistributedOperatorDescriptor descriptor(long version, byte[] jar, DeterministicContract contract) throws Exception {
        return new DistributedOperatorDescriptor("recording", version,
                HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(jar)),
                RecordingOperator.class.getName(), 1, contract);
    }

    private String identity(FFMAbstractMergeOperator operator) throws Exception {
        return (String) operator.getClass().getMethod("constructorIdentity").invoke(operator);
    }

    @Test
    void stableAcrossRegistriesSeparateFromStandaloneAndClosedOnce() throws Exception {
        RocksDB.loadLibrary();
        try (var options = new Options().setCreateIfMissing(true);
             var db = RocksDB.open(options, directory.toString());
             var family = db.getDefaultColumnFamily();
             var first = new MergeOperatorRegistry(db, family);
             var second = new MergeOperatorRegistry(db, family)) {
            byte[] jar = artifact(true);
            long version = first.upload("recording", RecordingOperator.class.getName(), jar);
            assertEquals(version, first.upload("recording", RecordingOperator.class.getName(), jar));
            var descriptor = descriptor(version, jar, QUALIFIED);
            var standalone = first.get("recording", version);
            var distributed = first.getDistributed(descriptor);
            var independent = second.getDistributed(descriptor);
            assertNotSame(standalone, distributed);
            assertNotSame(distributed, independent);
            assertSame(distributed, first.getDistributed(descriptor));
            assertEquals(identity(distributed), identity(independent));
            assertTrue(identity(distributed).matches("DistributedOp-v1-[0-9a-f]{64}"));
            assertTrue(identity(standalone).startsWith("LoadedOp-"));
            var nextCodec = new DistributedOperatorDescriptor(descriptor.name(), descriptor.version(),
                    descriptor.artifactSha256(), descriptor.className(), 2, QUALIFIED);
            assertNotEquals(identity(distributed), identity(first.getDistributed(nextCodec)));
            first.close();
            assertFalse(standalone.isOwningHandle());
            assertFalse(distributed.isOwningHandle());
            assertTrue(independent.isOwningHandle());
        }
    }

    @Test
    void rejectsUnqualifiedContractEvenWhenArtifactExists() throws Exception {
        RocksDB.loadLibrary();
        try (var options = new Options().setCreateIfMissing(true);
             var db = RocksDB.open(options, directory.toString());
             var family = db.getDefaultColumnFamily();
             var registry = new MergeOperatorRegistry(db, family)) {
            byte[] jar = artifact(true);
            long version = registry.upload("recording", RecordingOperator.class.getName(), jar);
            var absent = descriptor(version, jar, null);
            var unadmitted = descriptor(version, jar, new DeterministicContract("append-contract", 1, false));
            assertThrows(IllegalArgumentException.class, () -> registry.getDistributed(absent));
            assertThrows(IllegalArgumentException.class, () -> registry.getDistributed(unadmitted));
        }
    }

    @Test
    void rejectsDigestClassAndExactVersionMismatch() throws Exception {
        RocksDB.loadLibrary();
        try (var options = new Options().setCreateIfMissing(true);
             var db = RocksDB.open(options, directory.toString());
             var family = db.getDefaultColumnFamily();
             var registry = new MergeOperatorRegistry(db, family)) {
            byte[] jar = artifact(true);
            long version = registry.upload("recording", RecordingOperator.class.getName(), jar);
            var correct = descriptor(version, jar, QUALIFIED);
            var wrongHash = new DistributedOperatorDescriptor("recording", version, "0".repeat(64),
                    correct.className(), 1, QUALIFIED);
            var wrongClass = new DistributedOperatorDescriptor("recording", version, correct.artifactSha256(),
                    "unrelated.Operator", 1, QUALIFIED);
            assertThrows(IllegalArgumentException.class, () -> registry.getDistributed(wrongHash));
            assertThrows(IllegalArgumentException.class, () -> registry.getDistributed(wrongClass));
            var missingVersion = descriptor(version + 1, jar, QUALIFIED);
            assertThrows(it.cavallium.rockserver.core.common.RocksDBException.class,
                    () -> registry.getDistributed(missingVersion));
            assertNotNull(registry.getDistributed(correct));
        }
    }

    @Test
    void cannotSubstituteParentClasspathClassForMissingJarClass() throws Exception {
        RocksDB.loadLibrary();
        try (var options = new Options().setCreateIfMissing(true);
             var db = RocksDB.open(options, directory.toString());
             var family = db.getDefaultColumnFamily();
             var registry = new MergeOperatorRegistry(db, family)) {
            byte[] emptyJar = artifact(false);
            // Existing standalone behavior permits parent loading; distributed artifact identity does not.
            long version = registry.upload("recording", RecordingOperator.class.getName(), emptyJar);
            assertNotNull(registry.get("recording", version));
            var descriptor = descriptor(version, emptyJar, QUALIFIED);
            assertThrows(it.cavallium.rockserver.core.common.RocksDBException.class,
                    () -> registry.getDistributed(descriptor));
        }
    }
}
