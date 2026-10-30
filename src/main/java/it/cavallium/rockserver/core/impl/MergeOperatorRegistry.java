package it.cavallium.rockserver.core.impl;

import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.impl.rocksdb.LeakSafeWriteBatch;
import it.cavallium.rockserver.core.impl.rocksdb.LeakSafeWriteOptions;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.SecureClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

/**
 * Registry for managing remote merge operators.
 * <p>
 * Handles uploading, versioning, storage (in RocksDB), and class loading of merge operators.
 * Ensures isolation by using separate ClassLoaders for each operator version.
 */
public class MergeOperatorRegistry implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(MergeOperatorRegistry.class);

    private final RocksDB db;
    private final ColumnFamilyHandle cfh;
    // Cache: OperatorName -> Version -> OperatorInstance
    private final Map<String, Map<Long, FFMAbstractMergeOperator>> cache = new ConcurrentHashMap<>();

    public MergeOperatorRegistry(RocksDB db, ColumnFamilyHandle cfh) {
        this.db = db;
        this.cfh = cfh;
    }

    /**
     * Uploads a new merge operator.
     * Persists the JAR and metadata to RocksDB.
     *
     * @param name      Operator name
     * @param className Fully qualified class name of the operator
     * @param jarData   JAR file content
     * @return Assigned version number
     */
    public long upload(String name, String className, byte[] jarData) {
        if (name == null || name.isBlank()) throw new IllegalArgumentException("Operator name cannot be empty");
        if (className == null || className.isBlank()) throw new IllegalArgumentException("Class name cannot be empty");
        if (jarData == null || jarData.length == 0) throw new IllegalArgumentException("JAR data cannot be empty");

        // Validate JAR content before storage
        validateJar(jarData);
        
        byte[] hash;
        try {
            java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
            hash = digest.digest(jarData);
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        synchronized (this) {
            try {
                // Check deduplication
                Long existing = findIndexedVersion(name, hash);
                if (existing != null) {
                    // double check if the data exists
                    byte[] key = encodeKey(name, existing);
                    if (db.get(cfh, key) != null) {
                        LOG.info("Merge operator '{}' with same content already exists as version {}. Reusing.", name, existing);
                        return existing;
                    }
                }

                // A data record without its hash index can only come from an interrupted write performed
                // before the records were written atomically. Reuse its version (and repair the index)
                // so that replaying the same upload yields the same version as on a clean replica.
                long currentVersion = readCurrentVersion(name);
                Long orphan = findVersionWithJar(name, currentVersion, jarData);
                if (orphan != null) {
                    try (var batch = new LeakSafeWriteBatch()) {
                        batch.put(cfh, encodeHashKey(name, hash), encodeLong(orphan));
                        write(batch);
                    }
                    LOG.info("Merge operator '{}' with same content already exists as version {} without hash index."
                            + " Repaired the index and reusing.", name, orphan);
                    return orphan;
                }

                long version = nextVersion(name, currentVersion);
                byte[] key = encodeKey(name, version);
                byte[] value = encodeValue(className, jarData);

                // Instantiate before writing anything: an operator that cannot be loaded leaves no rows behind,
                // so uploading it again fails the same way instead of finding them and reusing the version.
                FFMAbstractMergeOperator op = loadAndInstantiate(className, jarData);

                // Write the version counter, the data and the hash index atomically
                try (var batch = new LeakSafeWriteBatch()) {
                    batch.put(cfh, encodeMetaKey(name), encodeLong(version));
                    batch.put(cfh, key, value);
                    batch.put(cfh, encodeHashKey(name, hash), encodeLong(version));
                    write(batch);
                }
                cache.computeIfAbsent(name, k -> new ConcurrentHashMap<>()).put(version, op);

                LOG.info("Uploaded merge operator '{}' version {} (class: {})", name, version, className);
                return version;
            } catch (RocksDBException e) {
                throw e; // Propagate existing RocksDBException
            } catch (Exception e) {
                throw RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, "Failed to upload merge operator: " + name, e);
            }
        }
    }

    /**
     * Checks if a merge operator with the given hash already exists.
     * @param name Operator name
     * @param hash SHA-256 hash of the JAR content
     * @return The version number if found, or null
     */
    public Long check(String name, byte[] hash) {
        if (name == null || hash == null) return null;
        try {
            return findIndexedVersion(name, hash);
        } catch (Exception e) {
            LOG.warn("Failed to check merge operator existence by hash", e);
        }
        return null;
    }

    /**
     * Looks up the hash index, propagating read failures.
     */
    private Long findIndexedVersion(String name, byte[] hash) throws org.rocksdb.RocksDBException, IOException {
        byte[] val = db.get(cfh, encodeHashKey(name, hash));
        if (val == null) {
            return null;
        }
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(val))) {
            return dis.readLong();
        }
    }

    private byte[] encodeHashKey(String name, byte[] hash) {
        String hashHex = java.util.HexFormat.of().formatHex(hash);
        return ("hash:" + name + ":" + hashHex).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Retrieves a merge operator instance.
     *
     * @param name    Operator name
     * @param version Operator version
     * @return The operator instance
     */
    public FFMAbstractMergeOperator get(String name, long version) {
        return cache.computeIfAbsent(name, k -> new ConcurrentHashMap<>())
                    .computeIfAbsent(version, v -> loadFromDb(name, v));
    }

    /**
     * Returns the number of distinct operator names currently cached.
     */
    public int getOperatorsCount() {
        return cache.size();
    }

    /**
     * Returns the total number of cached operator instances across all versions.
     */
    public int getTotalVersionsCount() {
        int total = 0;
        for (Map<Long, FFMAbstractMergeOperator> versions : cache.values()) {
            total += versions.size();
        }
        return total;
    }

    private FFMAbstractMergeOperator loadFromDb(String name, long version) {
        try {
            byte[] key = encodeKey(name, version);
            byte[] value = db.get(cfh, key);
            if (value == null) {
                throw RocksDBException.of(RocksDBErrorType.COLUMN_NOT_FOUND, "Merge operator not found: " + name + " v" + version);
            }
            // Decode value
            try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(value))) {
                String className = dis.readUTF();
                int jarLen = dis.readInt();
                byte[] jarData = new byte[jarLen];
                dis.readFully(jarData);
                return loadAndInstantiate(className, jarData);
            }
        } catch (RocksDBException e) {
            throw e;
        } catch (Exception e) {
            throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, "Failed to load merge operator " + name + " v" + version, e);
        }
    }

    private long readCurrentVersion(String name) throws org.rocksdb.RocksDBException, IOException {
        byte[] val = db.get(cfh, encodeMetaKey(name));
        if (val == null) {
            return 0;
        }
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(val))) {
            return dis.readLong();
        }
    }

    /**
     * Returns the version to assign to a new upload.
     * <p>
     * Normally this is the current version plus one. If the current version has no data record, the counter
     * was advanced by an interrupted write performed before the records were written atomically, so the
     * version was never assigned and is reused.
     */
    private long nextVersion(String name, long currentVersion) throws org.rocksdb.RocksDBException {
        if (currentVersion > 0 && db.get(cfh, encodeKey(name, currentVersion)) == null) {
            return currentVersion;
        }
        return currentVersion + 1;
    }

    /**
     * Finds a stored version of the operator whose JAR content equals {@code jarData}.
     */
    private Long findVersionWithJar(String name, long currentVersion, byte[] jarData)
            throws org.rocksdb.RocksDBException, IOException {
        for (long version = 1; version <= currentVersion; version++) {
            byte[] value = db.get(cfh, encodeKey(name, version));
            if (value == null) {
                continue;
            }
            try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(value))) {
                dis.readUTF();
                int jarLen = dis.readInt();
                if (jarLen != jarData.length) {
                    continue;
                }
                byte[] storedJar = new byte[jarLen];
                dis.readFully(storedJar);
                if (Arrays.equals(storedJar, jarData)) {
                    return version;
                }
            }
        }
        return null;
    }

    /**
     * Writes a batch with the default write options, which are the ones {@link RocksDB#put} uses.
     */
    private void write(LeakSafeWriteBatch batch) throws org.rocksdb.RocksDBException {
        try (var writeOptions = new LeakSafeWriteOptions("merge-operator-registry-write-options")) {
            db.write(writeOptions, batch);
        }
    }

    private static byte[] encodeLong(long value) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeLong(value);
        }
        return baos.toByteArray();
    }

    private byte[] encodeMetaKey(String name) {
        // Key: "meta:<name>", holds the last assigned version
        return ("meta:" + name).getBytes(StandardCharsets.UTF_8);
    }

    private byte[] encodeKey(String name, long version) {
        // Key: "data:<name>:<version>"
        return ("data:" + name + ":" + version).getBytes(StandardCharsets.UTF_8);
    }

    private byte[] encodeValue(String className, byte[] jarData) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeUTF(className);
            dos.writeInt(jarData.length);
            dos.write(jarData);
        }
        return baos.toByteArray();
    }

    private FFMAbstractMergeOperator loadAndInstantiate(String className, byte[] jarData) {
        try {
            InMemoryClassLoader cl;
            try {
                cl = new InMemoryClassLoader(jarData, this.getClass().getClassLoader());
            } catch (IOException e) {
                throw invalidJar(e);
            }
            Class<?> clazz = cl.loadClass(className);
            if (!FFMAbstractMergeOperator.class.isAssignableFrom(clazz)) {
                throw new IllegalArgumentException("Class " + className + " does not extend FFMAbstractMergeOperator");
            }
            // Try constructor with name first, then no-arg
            try {
                return (FFMAbstractMergeOperator) clazz.getConstructor(String.class).newInstance("LoadedOp-" + System.nanoTime());
            } catch (NoSuchMethodException e) {
                 return (FFMAbstractMergeOperator) clazz.getConstructor().newInstance();
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to instantiate " + className, e);
        }
    }
    
    private void validateJar(byte[] jarData) {
        try (JarInputStream jis = new JarInputStream(new ByteArrayInputStream(jarData))) {
            jis.getNextJarEntry();
        } catch (IOException e) {
            throw invalidJar(e);
        }
    }

    /**
     * Rejects JAR content that cannot be parsed. The parse error is kept as suppressed rather than as the
     * cause: invalid content is a deterministic rejection, not an I/O failure of this machine's storage.
     */
    private static IllegalArgumentException invalidJar(IOException parseError) {
        var invalid = new IllegalArgumentException("Invalid JAR data");
        invalid.addSuppressed(parseError);
        return invalid;
    }

    @Override
    public void close() {
        for (Map<Long, FFMAbstractMergeOperator> versions : cache.values()) {
            for (FFMAbstractMergeOperator op : versions.values()) {
                try {
                    if (op != null) {
                        // Close only if this Java reference still owns the native handle.
                        // ColumnFamilyOptions takes ownership when setMergeOperator is called and
                        // will close the operator when options are closed. In that case, calling
                        // close() here could double-free. Guard with isOwningHandle().
                        if (op.isOwningHandle()) {
                            op.close();
                        }
                    }
                } catch (Throwable e) {
                    LOG.debug("Ignoring error while closing merge operator", e);
                }
            }
        }
        cache.clear();
    }

    public record MergeOperatorInfo(String name, long version, String className) {}

    public List<MergeOperatorInfo> listAll() {
        List<MergeOperatorInfo> result = new ArrayList<>();
        try (RocksIterator it = db.newIterator(cfh)) {
            it.seekToFirst();
            while (it.isValid()) {
                String key = new String(it.key(), StandardCharsets.UTF_8);
                if (key.startsWith("data:")) {
                    try {
                        int lastColon = key.lastIndexOf(':');
                        if (lastColon > 4) {
                            String name = key.substring(5, lastColon);
                            long version = Long.parseLong(key.substring(lastColon + 1));

                            String className;
                            try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(it.value()))) {
                                className = dis.readUTF();
                            }
                            result.add(new MergeOperatorInfo(name, version, className));
                        }
                    } catch (Exception e) {
                        // Ignore malformed keys
                    }
                }
                it.next();
            }
        }
        return result;
    }

    private static class InMemoryClassLoader extends SecureClassLoader {
        private final Map<String, byte[]> classes = new ConcurrentHashMap<>();

        public InMemoryClassLoader(byte[] jarData, ClassLoader parent) throws IOException {
            super(parent);
            try (JarInputStream jis = new JarInputStream(new ByteArrayInputStream(jarData))) {
                JarEntry entry;
                while ((entry = jis.getNextJarEntry()) != null) {
                    if (entry.getName().endsWith(".class")) {
                        String className = entry.getName().replace('/', '.').replace(".class", "");
                        ByteArrayOutputStream baos = new ByteArrayOutputStream();
                        byte[] buffer = new byte[4096];
                        int bytesRead;
                        while ((bytesRead = jis.read(buffer)) != -1) {
                            baos.write(buffer, 0, bytesRead);
                        }
                        classes.put(className, baos.toByteArray());
                    }
                }
            }
        }

        @Override
        protected Class<?> findClass(String name) throws ClassNotFoundException {
            byte[] bytes = classes.get(name);
            if (bytes == null) {
                return super.findClass(name);
            }
            return defineClass(name, bytes, 0, bytes.length);
        }
    }
}
