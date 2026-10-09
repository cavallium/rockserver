package it.cavallium.rockserver.core.impl;

import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.SecureClassLoader;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Objects;
import java.util.ArrayList;
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
    private final Map<DistributedOperatorDescriptor, FFMAbstractMergeOperator> distributedCache = new ConcurrentHashMap<>();

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
                Long existing = check(name, hash);
                if (existing != null) {
                    // double check if the data exists
                    byte[] key = encodeKey(name, existing);
                    if (db.get(cfh, key) != null) {
                        LOG.info("Merge operator '{}' with same content already exists as version {}. Reusing.", name, existing);
                        return existing;
                    }
                }
                
                long version = getNextVersion(name);
                byte[] key = encodeKey(name, version);
                byte[] value = encodeValue(className, jarData);
                
                // Write data
                db.put(cfh, key, value);

                // Write hash index
                byte[] hashKey = encodeHashKey(name, hash);
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try (DataOutputStream dos = new DataOutputStream(baos)) {
                    dos.writeLong(version);
                }
                db.put(cfh, hashKey, baos.toByteArray());

                // Pre-load to verify instantiation and cache it
                FFMAbstractMergeOperator op = loadAndInstantiate(className, jarData);
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
            byte[] key = encodeHashKey(name, hash);
            byte[] val = db.get(cfh, key);
            if (val != null) {
                try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(val))) {
                    return dis.readLong();
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to check merge operator existence by hash", e);
        }
        return null;
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

    /** Explicit admission metadata, not bytecode verification or a sandbox guarantee. */
    public record DeterministicContract(String identifier, int version, boolean admitted) {
        public DeterministicContract {
            if (identifier == null || identifier.isBlank() || version <= 0) {
                throw new IllegalArgumentException("Deterministic contract requires an identifier and positive version");
            }
        }
    }

    /** Exact immutable artifact/schema identity; SHA-256 is canonical lowercase hexadecimal. */
    public record DistributedOperatorDescriptor(String name, long version, String artifactSha256,
                                               String className, int valueCodecVersion, int operandCodecVersion,
                                               DeterministicContract contract) {
        public DistributedOperatorDescriptor {
            if (name == null || name.isBlank() || className == null || className.isBlank()
                    || version < 0 || valueCodecVersion <= 0 || operandCodecVersion <= 0) {
                throw new IllegalArgumentException("Distributed operator requires exact name, version, class and codec version");
            }
            if (artifactSha256 == null || !artifactSha256.matches("[0-9a-f]{64}")) {
                throw new IllegalArgumentException("Artifact digest must be canonical SHA-256 hexadecimal");
            }
        }
    }

    /**
     * Loads only an explicitly admitted deterministic operator, with a replica-stable constructor identity.
     * Admission must establish purity externally; this method cannot prove arbitrary Java code deterministic.
     * The distributed instance never shares native ownership with the standalone cache.
     */
    public synchronized FFMAbstractMergeOperator getDistributed(DistributedOperatorDescriptor descriptor) {
        Objects.requireNonNull(descriptor, "descriptor");
        if (descriptor.contract() == null || !descriptor.contract().admitted()) {
            throw new IllegalArgumentException("Merge operator has no admitted deterministic contract");
        }
        return distributedCache.computeIfAbsent(descriptor, this::loadDistributedFromDb);
    }

    private FFMAbstractMergeOperator loadDistributedFromDb(DistributedOperatorDescriptor descriptor) {
        try {
            byte[] stored = db.get(cfh, encodeKey(descriptor.name(), descriptor.version()));
            if (stored == null) {
                throw RocksDBException.of(RocksDBErrorType.COLUMN_NOT_FOUND,
                        "Merge operator not found: " + descriptor.name() + " v" + descriptor.version());
            }
            try (var input = new DataInputStream(new ByteArrayInputStream(stored))) {
                String className = input.readUTF();
                int jarLength = input.readInt();
                if (jarLength <= 0 || jarLength != input.available()) {
                    throw new IllegalArgumentException("Malformed stored operator artifact");
                }
                byte[] jar = input.readNBytes(jarLength);
                if (!descriptor.className().equals(className)
                        || !descriptor.artifactSha256().equals(HexFormat.of().formatHex(sha256(jar)))) {
                    throw new IllegalArgumentException("Distributed operator artifact/class mismatch");
                }
                return instantiateDistributed(descriptor,jar);
            }
        } catch (RocksDBException | IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR,
                    "Failed to load distributed merge operator " + descriptor.name(), e);
        }
    }

    public static final int MAX_DISTRIBUTED_JAR_BYTES=8*1024*1024;
    /** Exact admitted immutable artifact factory, usable before a database is opened. */
    public static FFMAbstractMergeOperator instantiateDistributed(DistributedOperatorDescriptor descriptor,byte[] jar) {
        Objects.requireNonNull(descriptor); Objects.requireNonNull(jar);
        if(descriptor.contract()==null || !descriptor.contract().admitted()) throw new IllegalArgumentException("Distributed operator lacks trusted admission");
        if(jar.length==0 || jar.length>MAX_DISTRIBUTED_JAR_BYTES) throw new IllegalArgumentException("Distributed operator artifact size mismatch");
        jar=jar.clone();
        if(!descriptor.artifactSha256().equals(HexFormat.of().formatHex(sha256(jar)))) throw new IllegalArgumentException("Distributed operator artifact digest/size mismatch");
        try {
            var loader=new InMemoryClassLoader(jar,MergeOperatorRegistry.class.getClassLoader(),true);
            Class<?> type=loader.findClass(descriptor.className());
            if(!FFMAbstractMergeOperator.class.isAssignableFrom(type)) throw new IllegalArgumentException("Operator must extend shared FFMAbstractMergeOperator");
            try(var construction=FFMAbstractMergeOperator.trackConstruction()) {
                var operator=(FFMAbstractMergeOperator)type.getConstructor(String.class).newInstance(distributedConstructorIdentity(descriptor));
                construction.adopt(operator); return operator;
            }
        } catch(IllegalArgumentException rejected) { throw rejected; }
        catch(Exception failure) { throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR,"Failed to instantiate exact distributed operator",failure); }
    }
    private static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new AssertionError(e);
        }
    }

    private static String distributedConstructorIdentity(DistributedOperatorDescriptor descriptor) throws IOException {
        var bytes = new ByteArrayOutputStream();
        try (var output = new DataOutputStream(bytes)) {
            output.writeUTF(descriptor.name());
            output.writeLong(descriptor.version());
            output.writeUTF(descriptor.artifactSha256());
            output.writeUTF(descriptor.className());
            output.writeInt(descriptor.valueCodecVersion()); output.writeInt(descriptor.operandCodecVersion());
            output.writeUTF(descriptor.contract().identifier());
            output.writeInt(descriptor.contract().version());
        }
        return "DistributedOp-" + HexFormat.of().formatHex(sha256(bytes.toByteArray()));
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
            jis.getNextJarEntry();
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid JAR data", e);
        }
    }

    @Override
    public synchronized void close() {
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
        for (FFMAbstractMergeOperator op : distributedCache.values()) {
            try {
                if (op.isOwningHandle()) op.close();
            } catch (Throwable e) {
                LOG.debug("Ignoring error while closing distributed merge operator", e);
            }
        }
        distributedCache.clear();
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
        private final Map<String,byte[]> classes=new ConcurrentHashMap<>();
        private final boolean distributed;
        public InMemoryClassLoader(byte[] jarData,ClassLoader parent) throws IOException { this(jarData,parent,false); }
        public InMemoryClassLoader(byte[] jarData,ClassLoader parent,boolean distributed) throws IOException {
            super(parent); this.distributed=distributed;
            if(distributed) {
                try(var input=new java.util.zip.ZipInputStream(new ByteArrayInputStream(jarData))) {
                    java.util.zip.ZipEntry entry; long expanded=0; int entries=0;
                    while((entry=input.getNextEntry())!=null) {
                        if(++entries>4096 || entry.getName().length()>4096) throw new IOException("Distributed artifact entries exceed budget");
                        boolean isClass=entry.getName().endsWith(".class"); String className=isClass ? entry.getName().substring(0,entry.getName().length()-6).replace('/','.') : null;
                        if(isClass && (sharedApi(className) || classes.containsKey(className))) throw new IOException("Invalid distributed artifact class entry");
                        var bytes=isClass ? new ByteArrayOutputStream() : null; byte[] buffer=new byte[4096]; int count; long entryBytes=0;
                        while((count=input.read(buffer))!=-1) {
                            expanded+=count; entryBytes+=count;
                            if(expanded>64L*1024*1024 || entryBytes>8L*1024*1024) throw new IOException("Distributed artifact expanded bytes exceed budget");
                            if(bytes!=null) bytes.write(buffer,0,count);
                        }
                        if(bytes!=null) classes.put(className,bytes.toByteArray());
                    }
                }
            } else try(var input=new JarInputStream(new ByteArrayInputStream(jarData))) {
                JarEntry entry;
                while((entry=input.getNextJarEntry())!=null) if(entry.getName().endsWith(".class")) {
                    String name=entry.getName().replace('/','.').replace(".class",""); var bytes=new ByteArrayOutputStream(); input.transferTo(bytes); classes.put(name,bytes.toByteArray());
                }
            }
        }

        private static boolean sharedApi(String name) {
            return name.startsWith("java.") || name.startsWith("javax.") || name.startsWith("jdk.") || name.startsWith("sun.") || name.startsWith("com.sun.")
                    || name.startsWith("org.rocksdb.") || name.startsWith("org.slf4j.") || name.startsWith("org.jetbrains.annotations.")
                    || name.startsWith("it.cavallium.buffer.") || name.startsWith("it.cavallium.rockserver.core.common.") || name.startsWith("it.unimi.dsi.fastutil.")
                    || name.equals(FFMAbstractMergeOperator.class.getName()) || name.startsWith(FFMAbstractMergeOperator.class.getName()+"$")
                    || name.equals(FFMByteArrayMergeOperator.class.getName()) || name.startsWith(FFMByteArrayMergeOperator.class.getName()+"$");
        }
        @Override protected synchronized Class<?> loadClass(String name,boolean resolve) throws ClassNotFoundException {
            if(!distributed) return super.loadClass(name,resolve);
            Class<?> type=findLoadedClass(name);
            if(type==null) {
                if(sharedApi(name)) type=getParent().loadClass(name);
                else if(classes.containsKey(name)) type=findClass(name);
                else throw new ClassNotFoundException("Dependency absent from exact operator artifact: "+name);
            }
            if(resolve) resolveClass(type); return type;
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
