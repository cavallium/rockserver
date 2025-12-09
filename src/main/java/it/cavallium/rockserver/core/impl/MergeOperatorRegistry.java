package it.cavallium.rockserver.core.impl;

import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.SecureClassLoader;
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

        synchronized (this) {
            try {
                long version = getNextVersion(name);
                byte[] key = encodeKey(name, version);
                byte[] value = encodeValue(className, jarData);
                db.put(cfh, key, value);

                // Pre-load to verify instantiation and cache it
                loadAndCache(name, version, className, jarData);

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

    private long getNextVersion(String name) throws org.rocksdb.RocksDBException, IOException {
        byte[] metaKey = ("meta:" + name).getBytes(StandardCharsets.UTF_8);
        byte[] val = db.get(cfh, metaKey);
        long ver = 0;
        if (val != null) {
            try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(val))) {
                ver = dis.readLong();
            }
        }
        long newVer = ver + 1;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeLong(newVer);
        }
        db.put(cfh, metaKey, baos.toByteArray());
        return newVer;
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

    private FFMAbstractMergeOperator loadAndCache(String name, long version, String className, byte[] jarData) {
        FFMAbstractMergeOperator op = loadAndInstantiate(className, jarData);
        cache.computeIfAbsent(name, k -> new ConcurrentHashMap<>()).put(version, op);
        return op;
    }

    private FFMAbstractMergeOperator loadAndInstantiate(String className, byte[] jarData) {
        try {
            InMemoryClassLoader cl = new InMemoryClassLoader(jarData, this.getClass().getClassLoader());
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
            if (jis.getNextJarEntry() == null) {
                // Warning: JAR might be empty of entries but have a manifest.
                // We could inspect Manifest but for now simple check is enough.
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid JAR data", e);
        }
    }

    @Override
    public void close() {
        for (Map<Long, FFMAbstractMergeOperator> versions : cache.values()) {
            for (FFMAbstractMergeOperator op : versions.values()) {
                try {
                    op.close();
                } catch (Exception e) {
                    LOG.error("Failed to close merge operator", e);
                }
            }
        }
        cache.clear();
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
