package it.cavallium.rockserver.core.impl;

import it.cavallium.buffer.Buf;
import it.cavallium.buffer.MemorySegmentBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jetbrains.annotations.VisibleForTesting;
import org.rocksdb.MergeOperator;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksObject;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A bridge class that allows implementing RocksDB MergeOperators in pure Java
 * using the Java FFM API and the {@link Buf} interface for Zero-Copy performance.
 * <p>
 * This implementation avoids copying native memory to Java byte arrays for inputs.
 * It wraps native pointers in {@link MemorySegmentBuf}, allowing direct off-heap access.
 * <p>
 * Performance Features:
 * 1. <b>Zero-Copy Inputs:</b> Key, Existing Value, and Operands are wrappers around native pointers.
 * 2. <b>Critical Downcalls:</b> Malloc/Free use critical linker options to skip thread state transitions.
 * 3. <b>Native-to-Native Output:</b> If the result Buf is off-heap, it is copied directly to the result buffer using memcpy.
 */
public abstract class FFMAbstractMergeOperator extends MergeOperator {

    // --- Native Linker Setup ---
    private static final Linker LINKER = Linker.nativeLinker();
    // Upcall stubs are allocated on a per-instance Arena so they are freed when the operator
    // is disposed, preventing native stub memory from accumulating across operator rotations.
    private static final SymbolLookup LOOKUP;
    private static final Logger LOG = LoggerFactory.getLogger(FFMAbstractMergeOperator.class);
    
    // Critical Downcalls for speed
    private static MethodHandle MALLOC;
    private static MethodHandle FREE;
    private static final String ALLOCATOR_TYPE;
    
    private static MethodHandle CREATE_OP;
    private static MethodHandle DESTROY_OP;

    // Cache layout sizes for hot-path arithmetic
    private static final long ADDRESS_SIZE = ValueLayout.ADDRESS.byteSize();
    private static final long LONG_SIZE = ValueLayout.JAVA_LONG.byteSize();

    static {
        RocksDB.loadLibrary();

        LOOKUP = name -> SymbolLookup.loaderLookup().find(name)
                .or(() -> LINKER.defaultLookup().find(name));

        // Try to find jemalloc first to match RocksDB's internal allocator (if linked with jemalloc).
        // Ensure both je_malloc and je_free are present; otherwise fall back to standard malloc/free
        // to avoid allocator mismatch.
        MemorySegment mallocAddr;
        MemorySegment freeAddr;
        var jeMallocOpt = LOOKUP.find("je_malloc");
        var jeFreeOpt = LOOKUP.find("je_free");
        if (jeMallocOpt.isPresent() && jeFreeOpt.isPresent()) {
            mallocAddr = jeMallocOpt.get();
            freeAddr = jeFreeOpt.get();
            ALLOCATOR_TYPE = "jemalloc";
        } else {
            mallocAddr = LOOKUP.find("malloc")
                    .orElseThrow(() -> new UnsatisfiedLinkError("Native symbol not found: malloc"));
            freeAddr = LOOKUP.find("free")
                    .orElseThrow(() -> new UnsatisfiedLinkError("Native symbol not found: free"));
            ALLOCATOR_TYPE = "libc";
        }

        MALLOC = LINKER.downcallHandle(
                mallocAddr,
                FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG),
                Linker.Option.critical(false)
        );

        FREE = LINKER.downcallHandle(
                freeAddr,
                FunctionDescriptor.ofVoid(ValueLayout.ADDRESS),
                Linker.Option.critical(false)
        );

        try {
            LOG.info("FFM merge-operator allocator: {}", ALLOCATOR_TYPE);
        } catch (Throwable ignored) {
            // Logging must not fail class initialization
        }

        CREATE_OP = LINKER.downcallHandle(
                findOrThrow("rocksdb_mergeoperator_create"),
                FunctionDescriptor.of(ValueLayout.ADDRESS,
                        ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS,
                        ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS)
        );

        DESTROY_OP = LINKER.downcallHandle(
                findOrThrow("rocksdb_mergeoperator_destroy"),
                FunctionDescriptor.ofVoid(ValueLayout.ADDRESS)
        );
    }
    private static MemorySegment findOrThrow(String name) {
        return LOOKUP.find(name).orElseThrow(() ->
                new UnsatisfiedLinkError("Native symbol not found: " + name));
    }

    private static final ThreadLocal<ConstructionScope> CONSTRUCTION=new ThreadLocal<>();
    /** Owns native bridge allocations during one admitted reflective construction. */
    public static final class ConstructionScope implements AutoCloseable {
        private final ConstructionScope previous;
        private final Thread thread=Thread.currentThread();
        private final List<FFMAbstractMergeOperator> created=new ArrayList<>();
        private final java.util.Set<FFMAbstractMergeOperator> adopted=java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        private boolean closed;
        private ConstructionScope() {
            previous=CONSTRUCTION.get(); int depth=0;
            for(var scope=previous;scope!=null;scope=scope.previous) if(++depth>=8) throw new IllegalStateException("Operator construction nesting exceeds budget");
            CONSTRUCTION.set(this);
        }
        public List<FFMAbstractMergeOperator> created() { return List.copyOf(created); }
        public void adopt(FFMAbstractMergeOperator operator) {
            if(closed || thread!=Thread.currentThread()) throw new IllegalArgumentException("Operator construction ownership changed");
            var owners=java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<FFMAbstractMergeOperator,Boolean>());
            operator.collectOwnedDependencies(owners);
            if(owners.size()!=created.size() || !owners.containsAll(created)) throw new IllegalArgumentException("Operator constructor created unexpected native owners");
            for(var owner:owners) if(!owner.isOwningHandle() || !owner.instanceArena.scope().isAlive() || owner.sharedPtrContainer.address()!=owner.nativeHandle_) throw new IllegalArgumentException("Operator constructor disposed its native bridge");
            for(var owner:owners) owner.dependenciesSealed=true;
            adopted.addAll(owners);
        }
        @Override public void close() {
            if(closed) return;
            if(thread!=Thread.currentThread() || CONSTRUCTION.get()!=this) throw new IllegalStateException("Operator construction scope ownership changed");
            closed=true; CONSTRUCTION.set(previous);
            for(var operator:created) if(!adopted.contains(operator)) operator.closeConstructionOwner();
        }
    }
    public static ConstructionScope trackConstruction() { return new ConstructionScope(); }
    private static void recordConstruction(FFMAbstractMergeOperator operator) {
        for(var scope=CONSTRUCTION.get();scope!=null;scope=scope.previous) {
            if(scope.created.size()>=16) throw new IllegalStateException("Operator construction allocations exceed budget"); scope.created.add(operator);
        }
    }
    private final List<FFMAbstractMergeOperator> ownedDependencies=new ArrayList<>();
    private FFMAbstractMergeOperator dependencyOwner;
    private boolean dependenciesSealed;
    /** Transfer a private owning callback dependency to this native bridge's lifetime. */
    protected final void retainOwnedDependency(FFMAbstractMergeOperator dependency) {
        Objects.requireNonNull(dependency);
        synchronized(FFMAbstractMergeOperator.class) {
            if(dependenciesSealed || !isOwningHandle() || !instanceArena.scope().isAlive() || !dependency.isOwningHandle() || !dependency.instanceArena.scope().isAlive() || dependency.dependencyOwner!=null || dependency==this) throw new IllegalArgumentException("Callback dependency lacks private native ownership");
            var owners=java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<FFMAbstractMergeOperator,Boolean>());
            collectOwnedDependencies(owners); dependency.collectOwnedDependencies(owners);
            if(owners.size()>16) throw new IllegalArgumentException("Callback dependency ownership exceeds budget");
            dependency.dependencyOwner=this; ownedDependencies.add(dependency);
        }
    }
    private void collectOwnedDependencies(java.util.Set<FFMAbstractMergeOperator> owners) {
        if(!owners.add(this)) throw new IllegalArgumentException("Cyclic/duplicate native callback dependency");
        for(var dependency:ownedDependencies) dependency.collectOwnedDependencies(owners);
    }
    private void closeConstructionOwner() { owningHandle_.set(false); if(instanceArena.scope().isAlive()) disposeBridge(sharedPtrContainer.address()); }

    // --- Instance Members ---
    private final Arena instanceArena = Arena.ofShared();
    private final MemorySegment nameSegment;
    private final MemorySegment sharedPtrContainer;
    
    // Keep strong references to prevent GC collection of stubs
    @SuppressWarnings("FieldCanBeLocal") private final MemorySegment nameStub;
    @SuppressWarnings("FieldCanBeLocal") private final MemorySegment destructorStub;
    @SuppressWarnings("FieldCanBeLocal") private final MemorySegment fullMergeStub;
    @SuppressWarnings("FieldCanBeLocal") private final MemorySegment partialMergeStub;
    @SuppressWarnings("FieldCanBeLocal") private final MemorySegment deleteValueStub;

    public FFMAbstractMergeOperator(String name) {
        super(0);
        MemorySegment allocatedRaw=MemorySegment.NULL;
        try {
            this.nameSegment = instanceArena.allocateFrom(name);

            this.nameStub = createNameCallback();
            this.destructorStub = createNoOpCallback();
            this.fullMergeStub = createFullMergeCallback();
            this.partialMergeStub = createPartialMergeCallback();
            this.deleteValueStub = createDeleteValueCallback();

            MemorySegment rawCpObject = (MemorySegment) CREATE_OP.invokeExact(
                    MemorySegment.NULL,
                    destructorStub,
                    fullMergeStub,
                    partialMergeStub,
                    deleteValueStub,
                    nameStub
            );

            allocatedRaw=rawCpObject;
            if (rawCpObject.equals(MemorySegment.NULL)) {
                throw new RuntimeException("rocksdb_mergeoperator_create returned NULL");
            }

            this.sharedPtrContainer = instanceArena.allocate(16, 8);
            sharedPtrContainer.set(ValueLayout.ADDRESS, 0, rawCpObject); 
            sharedPtrContainer.set(ValueLayout.ADDRESS, 8, MemorySegment.NULL);

            setNativeHandle(sharedPtrContainer.address()); recordConstruction(this);
        } catch(Throwable t) {
            disOwnNativeHandle();
            if(!allocatedRaw.equals(MemorySegment.NULL)) try { DESTROY_OP.invokeExact(allocatedRaw); } catch(Throwable cleanup) { t.addSuppressed(cleanup); }
            instanceArena.close();
            throw new RuntimeException("Failed to construct FFM MergeOperator", t);
        }
    }

    /**
     * Merge the key, existing value, and operands.
     * <p>
     * Inputs are provided as {@link Buf}. To maximize performance, use the Buf API directly
     * to read primitives (e.g., {@code operands.get(0).getLong(0)}) instead of converting to arrays.
     */
    public abstract Buf merge(Buf key, Buf existingValue, List<Buf> operands);

    /**
     * Optimizes merging of multiple operands during flush/compaction.
     * <p>
     * Implementations should return a merged value or {@code null} to signal that the
     * result could not be produced (RocksDB will then fallback to full merge later).
     */
    public Buf partialMergeMulti(Buf key, List<Buf> operands) {
        return null;
    }

    /**
     * Legacy pair-wise partial merge hook. Deprecated in RocksDB in favor of {@link #partialMergeMulti(Buf, List)}.
     * Default returns {@code null} to request a full merge.
     */
    @Deprecated
    public Buf partialMerge(Buf key, Buf leftOperand, Buf rightOperand) {
        return null;
    }

    /**
     * Hook invoked when user merge logic throws. Override to monitor or log errors.
     * This is called for both full and partial merges. Default implementation prints the stack trace.
     */
    protected void onMergeError(Throwable t, boolean partial, Buf key) {
        LOG.error("Merge error (partial={})", partial, t);
    }

    private void handleMergeError(Throwable t, boolean partial, Buf key) {
        try {
            onMergeError(t, partial, key);
        } catch (Throwable hookEx) {
            // Swallow hook errors to keep RocksDB stable
            LOG.error("Error in onMergeError hook", hookEx);
        }
    }

    private void setNativeHandle(long handle) {
        try {
            injectNativeHandle(handle);
        } catch (Exception e) {
            throw new RuntimeException("Could not inject native handle", e);
        }
    }

    /**
     * Visible for testing; separated to allow injection failure simulation.
     */
		@VisibleForTesting
    protected void injectNativeHandle(long handle) throws Exception {
        Field field = RocksObject.class.getDeclaredField("nativeHandle_");
        field.setAccessible(true);
        field.setLong(this, handle);
    }

    @Override
		public void disposeInternal(long handle) { disposeBridge(handle); }
    private void disposeBridge(long handle) {
        try {
            if (sharedPtrContainer != null && instanceArena.scope().isAlive()) {
                MemorySegment rawObj = sharedPtrContainer.get(ValueLayout.ADDRESS, 0);
                if (!rawObj.equals(MemorySegment.NULL)) {
                    DESTROY_OP.invokeExact(rawObj);
                }
            }
        } catch (Throwable e) {
            LOG.warn("Error disposing native handle", e);
        } finally {
            instanceArena.close();
            for(var dependency:ownedDependencies) if(dependency.isOwningHandle()) {
                try { dependency.close(); }
                catch(Throwable failure) { LOG.warn("Error disposing callback dependency",failure); }
                finally { dependency.closeConstructionOwner(); }
            }
        }
    }

    // --- Callbacks ---

    private MemorySegment createNameCallback() {
        FunctionDescriptor desc = FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS);
        try {
            MethodHandle mh = MethodHandles.lookup().findVirtual(
                FFMAbstractMergeOperator.class, 
                "nameCallback", 
                MethodType.methodType(MemorySegment.class, MemorySegment.class)
            ).bindTo(this);
            return LINKER.upcallStub(mh, desc, instanceArena);
        } catch (Exception e) { throw new RuntimeException(e); }
    }

    private MemorySegment nameCallback(MemorySegment state) {
        return nameSegment;
    }

    private MemorySegment createNoOpCallback() {
        FunctionDescriptor desc = FunctionDescriptor.ofVoid(ValueLayout.ADDRESS);
        try {
            MethodHandle mh = MethodHandles.lookup().findStatic(
                FFMAbstractMergeOperator.class, 
                "noOpCallback", 
                MethodType.methodType(void.class, MemorySegment.class)
            );
            return LINKER.upcallStub(mh, desc, instanceArena);
        } catch (Exception e) { throw new RuntimeException(e); }
    }

    private static void noOpCallback(MemorySegment state) {
        // No-op to prevent SIGSEGV
    }

    private MemorySegment createFullMergeCallback() {
        FunctionDescriptor desc = FunctionDescriptor.of(ValueLayout.ADDRESS,
                ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_INT,
                ValueLayout.ADDRESS, ValueLayout.ADDRESS
        );
        try {
            MethodHandle mh = MethodHandles.lookup().findVirtual(
                FFMAbstractMergeOperator.class,
                "fullMerge",
                MethodType.methodType(MemorySegment.class, MemorySegment.class, MemorySegment.class, long.class,
                            MemorySegment.class, long.class, MemorySegment.class, MemorySegment.class, int.class,
                            MemorySegment.class, MemorySegment.class)
            ).bindTo(this);
            return LINKER.upcallStub(mh, desc, instanceArena);
        } catch (Exception e) { throw new RuntimeException(e); }
    }

    private MemorySegment fullMerge(MemorySegment state, MemorySegment keyPtr, long keyLen,
                                    MemorySegment existPtr, long existLen,
                                    MemorySegment opListPtr, MemorySegment opLenPtr, int numOps,
                                    MemorySegment successPtr, MemorySegment newValLenPtr) {
        // Initialize pessimistically
        successPtr.reinterpret(1).set(ValueLayout.JAVA_BYTE, 0, (byte) 0);
        newValLenPtr.reinterpret(8).set(ValueLayout.JAVA_LONG, 0, 0L);

        Buf key = null;
        try {
            // Zero-Copy: Wrap native pointers in MemorySegmentBuf
            key = new MemorySegmentBuf(keyPtr.reinterpret(keyLen));
            Buf existing = null;
            if (existPtr.address() != 0) {
                existing = new MemorySegmentBuf(existPtr.reinterpret(existLen));
            }

            // Access native array of pointers
            MemorySegment opsPtrs = opListPtr.reinterpret(numOps * ADDRESS_SIZE);
            MemorySegment opsLens = opLenPtr.reinterpret(numOps * LONG_SIZE);

            List<Buf> operands = new ArrayList<>(numOps);
            for (int i = 0; i < numOps; i++) {
                MemorySegment opPtr = opsPtrs.getAtIndex(ValueLayout.ADDRESS, i);
                long len = opsLens.getAtIndex(ValueLayout.JAVA_LONG, i);
                // Zero-Copy Wrapper
                operands.add(new MemorySegmentBuf(opPtr.reinterpret(len)));
            }

            try {
                Buf result = merge(key, existing, operands);

                if (result != null) {
                    return allocateResult(result, successPtr, newValLenPtr);
                }
            } catch (Throwable t) {
                // System.err.println("DEBUG: Caught exception in fullMerge: " + t);
                try {
                    handleMergeError(t, false, key);
                } catch (Throwable ignored) {
                    LOG.error("Swallowed exception in fullMerge handleMergeError", ignored);
                }
            }

            // Fallback: preserve existing value (or first operand) to avoid corruption
            // However, this fallback is dangerous because it hides failures and discards operands.
            // Returning NULL allows RocksDB to handle the failure (e.g. aborting compaction or preserving operands).
            return MemorySegment.NULL;
        } catch (Throwable t) {
            try {
                handleMergeError(t, false, key);
            } catch (Throwable ignored) {
                LOG.error("Swallowed exception in fullMerge outer catch", ignored);
            }
            return MemorySegment.NULL;
        }
    }

    private MemorySegment createPartialMergeCallback() {
        FunctionDescriptor desc = FunctionDescriptor.of(ValueLayout.ADDRESS,
                ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_INT,
                ValueLayout.ADDRESS, ValueLayout.ADDRESS
        );
        try {
            MethodHandle mh = MethodHandles.lookup().findVirtual(
                FFMAbstractMergeOperator.class,
                "partialMergeCb",
                MethodType.methodType(MemorySegment.class, MemorySegment.class, MemorySegment.class, long.class,
                            MemorySegment.class, MemorySegment.class, int.class,
                            MemorySegment.class, MemorySegment.class)
            ).bindTo(this);
            return LINKER.upcallStub(mh, desc, instanceArena);
        } catch (Exception e) { throw new RuntimeException(e); }
    }

    private MemorySegment partialMergeCb(MemorySegment state, MemorySegment keyPtr, long keyLen,
                                         MemorySegment opListPtr, MemorySegment opLenListPtr, int numOps,
                                         MemorySegment successPtr, MemorySegment newValLenPtr) {
        // Initialize pessimistically
        successPtr.reinterpret(1).set(ValueLayout.JAVA_BYTE, 0, (byte) 0);
        newValLenPtr.reinterpret(8).set(ValueLayout.JAVA_LONG, 0, 0L);

        if (numOps == 0) return MemorySegment.NULL;

        Buf key = null;
        try {
            key = new MemorySegmentBuf(keyPtr.reinterpret(keyLen));

            MemorySegment opsPtrs = opListPtr.reinterpret(numOps * ADDRESS_SIZE);
            MemorySegment opsLens = opLenListPtr.reinterpret(numOps * LONG_SIZE);

            try {
                // Build operands list (Zero-Copy)
                List<Buf> operands = new ArrayList<>(numOps);
                for (int i = 0; i < numOps; i++) {
                    MemorySegment ptr = opsPtrs.getAtIndex(ValueLayout.ADDRESS, i);
                    long len = opsLens.getAtIndex(ValueLayout.JAVA_LONG, i);
                    operands.add(new MemorySegmentBuf(ptr.reinterpret(len)));
                }

                Buf merged = partialMergeMulti(key, operands);
                if (merged == null && operands.size() == 2) {
                    merged = partialMerge(key, operands.getFirst(), operands.getLast());
                }

                if (merged != null) {
                    return allocateResult(merged, successPtr, newValLenPtr);
                }
            } catch (Throwable t) { 
                try {
                    handleMergeError(t, true, key); 
                } catch (Throwable ignored) {
                    LOG.error("Swallowed exception in partialMergeCb handleMergeError", ignored);
                }
            }

            return MemorySegment.NULL;
        } catch (Throwable t) {
            try {
                handleMergeError(t, true, key);
            } catch (Throwable ignored) {
                LOG.error("Swallowed exception in partialMergeCb outer catch", ignored);
            }
            return MemorySegment.NULL;
        }
    }

    private MemorySegment createDeleteValueCallback() {
        FunctionDescriptor desc = FunctionDescriptor.ofVoid(ValueLayout.ADDRESS,
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG);
        try {
            MethodHandle mh = MethodHandles.lookup().findVirtual(
                FFMAbstractMergeOperator.class,
                "deleteValueCb",
                MethodType.methodType(void.class, MemorySegment.class, MemorySegment.class, long.class)
            ).bindTo(this);
            return LINKER.upcallStub(mh, desc, instanceArena);
        } catch (Exception e) { throw new RuntimeException(e); }
    }

    private void deleteValueCb(MemorySegment state, MemorySegment valuePtr, long len) {
        try {
            if (!valuePtr.equals(MemorySegment.NULL)) {
                FREE.invokeExact(valuePtr);
            }
        } catch (Throwable t) {
            LOG.warn("Error in deleteValueCb", t);
        }
    }

    private MemorySegment allocateResult(Buf data, MemorySegment successPtr, MemorySegment lenPtr) throws Throwable {
        long size = data.size();
        
        // Malloc(0) safety: allocate 1 byte
        long allocSize = (size == 0) ? 1 : size;
        
        // Critical downcall for speed
        MemorySegment resMem = (MemorySegment) MALLOC.invokeExact(allocSize);
        if (resMem.equals(MemorySegment.NULL)) throw new OutOfMemoryError("Native malloc failed");
        
        if (size > 0) {
            resMem = resMem.reinterpret(size);
            
            // OPTIMIZATION: Native-to-Native Copy
            // We wrap the newly allocated native memory in a mutable MemorySegmentBuf wrapper
            // and tell it to pull bytes from the source 'data' Buf.
            // If 'data' is also a MemorySegmentBuf (e.g. from partial merge), this triggers 
            // a fast MemorySegment.copy() (memcpy), bypassing the Java Heap completely.
            Buf resultWrapper = new MemorySegmentBuf(resMem);
            resultWrapper.setBytesFromBuf(0, data, 0, (int) size);
        }
        
        successPtr.reinterpret(1).set(ValueLayout.JAVA_BYTE, 0, (byte) 1);
        lenPtr.reinterpret(8).set(ValueLayout.JAVA_LONG, 0, size);
        
        return resMem;
    }
}