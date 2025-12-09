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
    private static final MethodHandle MALLOC;
    private static final MethodHandle FREE;
    private static final String ALLOCATOR_TYPE;
    
    private static final MethodHandle CREATE_OP;
    private static final MethodHandle DESTROY_OP;

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

            if (rawCpObject.equals(MemorySegment.NULL)) {
                throw new RuntimeException("rocksdb_mergeoperator_create returned NULL");
            }

            this.sharedPtrContainer = instanceArena.allocate(16, 8);
            sharedPtrContainer.set(ValueLayout.ADDRESS, 0, rawCpObject); 
            sharedPtrContainer.set(ValueLayout.ADDRESS, 8, MemorySegment.NULL);

            setNativeHandle(sharedPtrContainer.address());

        } catch (Throwable t) {
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
        t.printStackTrace();
    }

    private void handleMergeError(Throwable t, boolean partial, Buf key) {
        try {
            onMergeError(t, partial, key);
        } catch (Throwable ignored) {
            // Swallow hook errors to keep RocksDB stable
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
		public void disposeInternal(long handle) {
        try {
            if (sharedPtrContainer != null && sharedPtrContainer.address() == handle) {
                MemorySegment rawObj = sharedPtrContainer.get(ValueLayout.ADDRESS, 0);
                if (!rawObj.equals(MemorySegment.NULL)) {
                    DESTROY_OP.invokeExact(rawObj);
                }
            }
        } catch (Throwable e) {
            // suppress
        } finally {
            instanceArena.close();
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
                handleMergeError(t, false, key);
            }

            // Fallback: preserve existing value (or first operand) to avoid corruption
            Buf fallback = existing;
            if (fallback == null && !operands.isEmpty()) {
                fallback = operands.getFirst();
            }
            if (fallback == null) {
                fallback = Buf.wrap(new byte[0]);
            }

            try {
                return allocateResult(fallback, successPtr, newValLenPtr);
            } catch (Throwable t) {
                handleMergeError(t, false, key);
                // last resort: signal failure
                return MemorySegment.NULL;
            }
        } catch (Throwable t) {
            handleMergeError(t, false, key);
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
                handleMergeError(t, true, key); 
            }

            // Fallback: return first operand to keep data intact
            try {
                MemorySegment ptr0 = opsPtrs.getAtIndex(ValueLayout.ADDRESS, 0);
                long len0 = opsLens.getAtIndex(ValueLayout.JAVA_LONG, 0);
                Buf first = new MemorySegmentBuf(ptr0.reinterpret(len0));
                return allocateResult(first, successPtr, newValLenPtr);
            } catch (Throwable t) {
                handleMergeError(t, true, key);
                return MemorySegment.NULL;
            }
        } catch (Throwable t) {
            handleMergeError(t, true, key);
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
            // suppress
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