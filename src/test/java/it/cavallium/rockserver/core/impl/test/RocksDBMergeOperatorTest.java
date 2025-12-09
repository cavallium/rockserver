package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.impl.FFMAbstractMergeOperator;
import it.cavallium.rockserver.core.impl.FFMByteArrayMergeOperator;
import it.cavallium.rockserver.core.impl.MyStringAppendOperator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import sun.misc.Unsafe;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RocksDBMergeOperatorTest {

    @Test
    void testFullMergeAppend(@TempDir Path dbPath) throws Exception {
        RocksDB.loadLibrary();

        try (Options options = new Options()) {
            options.setCreateIfMissing(true);
            options.setMergeOperator(new MyStringAppendOperator());

            try (RocksDB db = RocksDB.open(options, dbPath.toString())) {
                byte[] key = "my_list".getBytes(StandardCharsets.UTF_8);

                db.put(key, "Apple".getBytes(StandardCharsets.UTF_8));
                db.merge(key, "Banana".getBytes(StandardCharsets.UTF_8));
                db.merge(key, "Cherry".getBytes(StandardCharsets.UTF_8));

                byte[] result = db.get(key);
                String resultStr = new String(result, StandardCharsets.UTF_8);
                assertEquals("Apple,Banana,Cherry", resultStr);
            }
        }
    }

    @Test
    void testRocksDBAppFlowMirrorsDemo(@TempDir Path dbPath) throws Exception {
        RocksDB.loadLibrary();

        try (Options options = new Options()) {
            options.setCreateIfMissing(true);
            options.setMergeOperator(new MyStringAppendOperator());

            try (RocksDB db = RocksDB.open(options, dbPath.toString())) {
                byte[] key = "my_list".getBytes(StandardCharsets.UTF_8);

                db.put(key, "Apple".getBytes(StandardCharsets.UTF_8));
                db.merge(key, "Banana".getBytes(StandardCharsets.UTF_8));
                db.merge(key, "Cherry".getBytes(StandardCharsets.UTF_8));

                byte[] result = db.get(key);
                String resultStr = new String(result, StandardCharsets.UTF_8);
                assertEquals("Apple,Banana,Cherry", resultStr);
            }
        }
    }

    @Test
    void testMergeExceptionDoesNotCrash(@TempDir Path dbPath) throws Exception {
        RocksDB.loadLibrary();

        try (Options options = new Options()) {
            options.setCreateIfMissing(true);
            options.setMergeOperator(new ThrowingMergeOperator());

            try (RocksDB db = RocksDB.open(options, dbPath.toString())) {
                byte[] key = "safe_key".getBytes(UTF_8);

                db.put(key, "base".getBytes(UTF_8));

                // Merge throws from user code; bridge should swallow and fail the op without crashing or propagating
                assertDoesNotThrow(() -> db.merge(key, "boom".getBytes(UTF_8)));

                // DB remains usable after merge failure
                assertEquals("base", new String(db.get(key), UTF_8));

                db.put(key, "after".getBytes(UTF_8));
                assertEquals("after", new String(db.get(key), UTF_8));
            }
        }
    }

    @Test
    void testMergeErrorHookExceptionIsSwallowed(@TempDir Path dbPath) throws Exception {
        RocksDB.loadLibrary();

        try (Options options = new Options()) {
            options.setCreateIfMissing(true);
            options.setMergeOperator(new HookThrowingHookOperator());

            try (RocksDB db = RocksDB.open(options, dbPath.toString())) {
                byte[] key = "safe_key".getBytes(UTF_8);

                db.put(key, "base".getBytes(UTF_8));

                assertDoesNotThrow(() -> db.merge(key, "boom".getBytes(UTF_8)));

                assertEquals("base", new String(db.get(key), UTF_8));

                db.put(key, "after".getBytes(UTF_8));
                assertEquals("after", new String(db.get(key), UTF_8));
            }
        }
    }

    @Test
    void testMergeErrorHookIsInvoked(@TempDir Path dbPath) throws Exception {
        RocksDB.loadLibrary();

        HookedThrowingOperator op = new HookedThrowingOperator();

        try (Options options = new Options()) {
            options.setCreateIfMissing(true);
            options.setMergeOperator(op);

            try (RocksDB db = RocksDB.open(options, dbPath.toString())) {
                byte[] key = "hook_key".getBytes(UTF_8);

                db.put(key, "v".getBytes(UTF_8));

                assertDoesNotThrow(() -> db.merge(key, "boom".getBytes(UTF_8)));

                assertEquals("v", new String(db.get(key), UTF_8));
            }
        }

        assertTrue(op.errorSeen, "onMergeError hook should be called when user merge throws");
    }

    @Test
    void testFullMergeErrorDoesNotAbortOnError(@TempDir Path dbPath) throws Exception {
        RocksDB.loadLibrary();

        try (Options options = new Options()) {
            options.setCreateIfMissing(true);
            options.setMergeOperator(new ErroringMergeOperator());

            try (RocksDB db = RocksDB.open(options, dbPath.toString())) {
                byte[] key = "err_key".getBytes(UTF_8);

                db.put(key, "base".getBytes(UTF_8));

                // Merge path throws an Error; bridge must swallow and not abort the process
                assertDoesNotThrow(() -> db.merge(key, "boom".getBytes(UTF_8)));

                // DB remains usable
                assertEquals("base", new String(db.get(key), UTF_8));

                db.put(key, "after".getBytes(UTF_8));
                assertEquals("after", new String(db.get(key), UTF_8));
            }
        }
    }

    @Test
    void testPartialMergeErrorDoesNotAbortOnError() throws Exception {
        TestableErroringPartialMergeOp op = new TestableErroringPartialMergeOp();

        byte[] key = "k".getBytes(UTF_8);
        byte[][] operands = new byte[][]{
                "a".getBytes(UTF_8),
                "b".getBytes(UTF_8)
        };

        Result res = invokePartialMergeCb(op, key, operands);

        // Should fall back to first operand without crashing
        assertEquals((byte) 1, res.success);
        assertEquals("a", res.asString());
        assertTrue(op.errorSeen, "Error thrown in partial merge should be reported but swallowed");
    }

    @Test
    void testPartialMergeMultiPreferred() throws Exception {
        TestableMergeOp op = new TestableMergeOp(TestableMergeOp.Mode.MULTI_OK);

        byte[] key = "k".getBytes(UTF_8);
        byte[][] operands = new byte[][]{
                "a".getBytes(UTF_8),
                "b".getBytes(UTF_8),
                "c".getBytes(UTF_8)
        };

        Result res = invokePartialMergeCb(op, key, operands);

        assertEquals((byte) 1, res.success);
        assertEquals("a+b+c", res.asString());
        assertEquals("MULTI", op.invocationOrder.get());
    }

    @Test
    void testPartialMergeMultiFallsBackToPairwise() throws Exception {
        TestableMergeOp op = new TestableMergeOp(TestableMergeOp.Mode.MULTI_NULL_PAIRWISE);

        byte[] key = "k".getBytes(UTF_8);
        byte[][] operands = new byte[][]{
                "l".getBytes(UTF_8),
                "r".getBytes(UTF_8)
        };

        Result res = invokePartialMergeCb(op, key, operands);

        assertEquals((byte) 1, res.success);
        assertEquals("l|r", res.asString());
        assertEquals("PAIRWISE", op.invocationOrder.get());
    }

    @Test
    void testPartialMergeErrorFallsBackAndReports() throws Exception {
        TestableMergeOp op = new TestableMergeOp(TestableMergeOp.Mode.MULTI_THROW);

        byte[] key = "k".getBytes(UTF_8);
        byte[][] operands = new byte[][]{
                "x".getBytes(UTF_8),
                "y".getBytes(UTF_8)
        };

        Result res = invokePartialMergeCb(op, key, operands);

        assertEquals((byte) 1, res.success, "fallback should still allocate result");
        assertEquals("x", res.asString(), "fallback must return first operand");
        assertTrue(op.errorSeen, "onMergeError should be invoked on exception");
    }

    @Test
    void testPartialMergeZeroOperandsReturnsNull() throws Exception {
        TestableMergeOp op = new TestableMergeOp(TestableMergeOp.Mode.MULTI_OK);

        byte[] key = "k".getBytes(UTF_8);
        byte[][] operands = new byte[][]{};

        Result res = invokePartialMergeCb(op, key, operands);

        assertEquals((byte) 0, res.success, "success flag must stay 0 when no operands");
        assertEquals(0L, res.len, "length must be zero when no result allocated");
    }

    @Test
    void testPartialMergeEmptyResultSetsZeroLength() throws Exception {
        FFMAbstractMergeOperator op = new FFMAbstractMergeOperator("EmptyPartialResultOp") {
            @Override
            public it.cavallium.buffer.Buf merge(it.cavallium.buffer.Buf key, it.cavallium.buffer.Buf existingValue, List<it.cavallium.buffer.Buf> operands) {
                throw new UnsupportedOperationException();
            }

            @Override
            public it.cavallium.buffer.Buf partialMergeMulti(it.cavallium.buffer.Buf key, List<it.cavallium.buffer.Buf> operands) {
                return it.cavallium.buffer.Buf.wrap(new byte[0]);
            }
        };

        byte[] key = "k".getBytes(UTF_8);
        byte[][] operands = new byte[][]{
                "op".getBytes(UTF_8)
        };

        Result res = invokePartialMergeCb(op, key, operands);

        assertEquals((byte) 1, res.success, "empty result should still mark success");
        assertEquals(0L, res.len, "length should be zero for empty result");
        assertEquals(0, res.data.length, "returned buffer should be zero-length");
    }

    @Test
    void testPartialMergeMultiNullWithManyOperandsFallsBackFirst() throws Exception {
        FFMAbstractMergeOperator op = new FFMAbstractMergeOperator("NullPartialMergeOp") {
            @Override
            public it.cavallium.buffer.Buf merge(it.cavallium.buffer.Buf key, it.cavallium.buffer.Buf existingValue, List<it.cavallium.buffer.Buf> operands) {
                throw new UnsupportedOperationException();
            }

            @Override
            public it.cavallium.buffer.Buf partialMergeMulti(it.cavallium.buffer.Buf key, List<it.cavallium.buffer.Buf> operands) {
                return null;
            }

            @Override
            public it.cavallium.buffer.Buf partialMerge(it.cavallium.buffer.Buf key, it.cavallium.buffer.Buf leftOperand, it.cavallium.buffer.Buf rightOperand) {
                return null;
            }
        };

        byte[] key = "k".getBytes(UTF_8);
        byte[][] operands = new byte[][]{
                "one".getBytes(UTF_8),
                "two".getBytes(UTF_8),
                "three".getBytes(UTF_8)
        };

        Result res = invokePartialMergeCb(op, key, operands);

        assertEquals((byte) 1, res.success);
        assertEquals("one", res.asString(), "fallback must return first operand when all partial merges return null");
    }

    @Test
    void testFullMergeFallbackUsesExistingWhenMergeReturnsNull() throws Exception {
        FFMAbstractMergeOperator op = new FFMAbstractMergeOperator("NullReturningMerge") {
            @Override
            public it.cavallium.buffer.Buf merge(it.cavallium.buffer.Buf key, it.cavallium.buffer.Buf existingValue, List<it.cavallium.buffer.Buf> operands) {
                return null; // force fallback
            }
        };

        byte[] key = "k".getBytes(UTF_8);
        byte[] existing = "base".getBytes(UTF_8);
        byte[][] operands = new byte[][]{
                "op1".getBytes(UTF_8),
                "op2".getBytes(UTF_8)
        };

        Result res = invokeFullMergeCb(op, key, existing, operands);

        assertEquals((byte) 1, res.success);
        assertEquals("base", res.asString(), "fallback must preserve existing value when merge returns null");
    }

    @Test
    void testFullMergeEmptyResultSetsZeroLength() throws Exception {
        FFMAbstractMergeOperator op = new FFMAbstractMergeOperator("EmptyFullResultOp") {
            @Override
            public it.cavallium.buffer.Buf merge(it.cavallium.buffer.Buf key, it.cavallium.buffer.Buf existingValue, List<it.cavallium.buffer.Buf> operands) {
                return it.cavallium.buffer.Buf.wrap(new byte[0]);
            }
        };

        byte[] key = "k".getBytes(UTF_8);
        byte[] existing = "base".getBytes(UTF_8);
        byte[][] operands = new byte[][]{
                "op".getBytes(UTF_8)
        };

        Result res = invokeFullMergeCb(op, key, existing, operands);

        assertEquals((byte) 1, res.success, "empty result should still mark success");
        assertEquals(0L, res.len, "length should be zero for empty result");
        assertEquals(0, res.data.length, "returned buffer should be zero-length");
    }

    @Test
    void testFullMergeFallbackUsesFirstOperandWhenNoExisting() throws Exception {
        FFMAbstractMergeOperator op = new FFMAbstractMergeOperator("NullReturningMergeNoExisting") {
            @Override
            public it.cavallium.buffer.Buf merge(it.cavallium.buffer.Buf key, it.cavallium.buffer.Buf existingValue, List<it.cavallium.buffer.Buf> operands) {
                return null; // force fallback
            }
        };

        byte[] key = "k".getBytes(UTF_8);
        byte[][] operands = new byte[][]{
                "first".getBytes(UTF_8),
                "second".getBytes(UTF_8)
        };

        Result res = invokeFullMergeCb(op, key, null, operands);

        assertEquals((byte) 1, res.success);
        assertEquals("first", res.asString(), "fallback must use first operand when no existing value");
    }

    @Test
    void testFullMergeFallbackToEmptyWhenNoExistingAndNoOperands() throws Exception {
        FFMAbstractMergeOperator op = new FFMAbstractMergeOperator("NullReturningMergeEmpty") {
            @Override
            public it.cavallium.buffer.Buf merge(it.cavallium.buffer.Buf key, it.cavallium.buffer.Buf existingValue, List<it.cavallium.buffer.Buf> operands) {
                return null; // force fallback
            }
        };

        byte[] key = "k".getBytes(UTF_8);
        byte[][] operands = new byte[][]{};

        Result res = invokeFullMergeCb(op, key, null, operands);

        assertEquals((byte) 1, res.success);
        assertEquals(0L, res.len);
        assertEquals("", res.asString(), "fallback should allocate empty value when nothing to preserve");
    }

    @Test
    void testPartialMergeBothNullFallbacksToFirstOperandWithTwoOperands() throws Exception {
        FFMAbstractMergeOperator op = new FFMAbstractMergeOperator("NullPartialMergeTwoOps") {
            @Override
            public it.cavallium.buffer.Buf merge(it.cavallium.buffer.Buf key, it.cavallium.buffer.Buf existingValue, List<it.cavallium.buffer.Buf> operands) {
                throw new UnsupportedOperationException();
            }

            @Override
            public it.cavallium.buffer.Buf partialMergeMulti(it.cavallium.buffer.Buf key, List<it.cavallium.buffer.Buf> operands) {
                return null;
            }

            @Override
            public it.cavallium.buffer.Buf partialMerge(it.cavallium.buffer.Buf key, it.cavallium.buffer.Buf leftOperand, it.cavallium.buffer.Buf rightOperand) {
                return null;
            }
        };

        byte[] key = "k".getBytes(UTF_8);
        byte[][] operands = new byte[][]{
                "left".getBytes(UTF_8),
                "right".getBytes(UTF_8)
        };

        Result res = invokePartialMergeCb(op, key, operands);

        assertEquals((byte) 1, res.success, "fallback should mark success when returning first operand");
        assertEquals("left", res.asString(), "fallback must return first operand when both partial merges return null");
    }

    @Test
    void testPartialMergeDefaultPairwiseFallbackWhenUnimplemented() throws Exception {
        FFMAbstractMergeOperator op = new DefaultPartialMergeOp();

        byte[] key = "k".getBytes(UTF_8);
        byte[][] operands = new byte[][]{
                "l".getBytes(UTF_8),
                "r".getBytes(UTF_8)
        };

        Result res = invokePartialMergeCb(op, key, operands);

        assertEquals((byte) 1, res.success, "fallback should succeed when partial merge hooks are unimplemented");
        assertEquals("l", res.asString(), "fallback must return first operand when default partialMerge returns null");
    }

    @Test
    void testOnMergeErrorReceivesKeyForFullMerge() throws Exception {
        KeyCapturingFullMergeOp op = new KeyCapturingFullMergeOp();

        byte[] key = "full-key".getBytes(UTF_8);
        byte[] existing = "base".getBytes(UTF_8);
        byte[][] operands = new byte[][]{
                "op1".getBytes(UTF_8)
        };

        Result res = invokeFullMergeCb(op, key, existing, operands);

        assertEquals("full-key", op.keySeen, "onMergeError must receive the merge key");
        assertEquals((byte) 1, res.success, "fallback should still succeed");
    }

    @Test
    void testOnMergeErrorReceivesKeyForPartialMerge() throws Exception {
        KeyCapturingPartialMergeOp op = new KeyCapturingPartialMergeOp();

        byte[] key = "partial-key".getBytes(UTF_8);
        byte[][] operands = new byte[][]{
                "a".getBytes(UTF_8),
                "b".getBytes(UTF_8)
        };

        Result res = invokePartialMergeCb(op, key, operands);

        assertEquals("partial-key", op.keySeen, "onMergeError must receive the partial merge key");
        assertEquals((byte) 1, res.success, "fallback should still succeed");
        assertEquals("a", res.asString(), "fallback should return first operand");
    }

    @Test
    void testPartialMergeOuterCatchInvokesHandleMergeError() throws Exception {
        OuterCatchOp op = new OuterCatchOp();

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment dummyKey = arena.allocate(1);
            MemorySegment opPtrs = arena.allocate(ValueLayout.ADDRESS);
            MemorySegment opLens = arena.allocate(ValueLayout.JAVA_LONG);
            opPtrs.setAtIndex(ValueLayout.ADDRESS, 0, arena.allocate(1));
            opLens.setAtIndex(ValueLayout.JAVA_LONG, 0, 1L);

            Result res = invokePartialMergeCbRaw(op, dummyKey, -1L, opPtrs, opLens, 1);

            assertEquals((byte) 0, res.success, "outer catch should signal failure");
            assertTrue(op.errorSeen, "handleMergeError must be invoked on outer failure");
            assertTrue(op.partialSeen, "partial flag should be true for partial merge outer catch");
            assertNull(op.keyString, "key should be null when failing before wrap");
        }
    }

    @Test
    void testFullMergeOuterCatchInvokesHandleMergeError() throws Exception {
        OuterCatchOp op = new OuterCatchOp();

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment dummyKey = arena.allocate(1);
            MemorySegment existing = arena.allocate(1);
            MemorySegment opPtrs = arena.allocate(ValueLayout.ADDRESS);
            MemorySegment opLens = arena.allocate(ValueLayout.JAVA_LONG);
            opPtrs.setAtIndex(ValueLayout.ADDRESS, 0, arena.allocate(1));
            opLens.setAtIndex(ValueLayout.JAVA_LONG, 0, 1L);

            Result res = invokeFullMergeCbRaw(op, dummyKey, -1L, existing, 1L, opPtrs, opLens, 1);

            assertEquals((byte) 0, res.success, "outer catch should signal failure");
            assertTrue(op.errorSeen, "handleMergeError must be invoked on outer failure");
            assertFalse(op.partialSeen, "partial flag should be false for full merge outer catch");
            assertNull(op.keyString, "key should be null when failing before wrap");
        }
    }

    @Test
    void testByteArrayPartialMergeFallsBackToPairwise() throws Exception {
        Result res = invokePartialMergeCb(new PairwiseFallbackByteArrayOp(), "k", "L", "R");

        assertEquals((byte) 1, res.success);
        assertEquals("L+R", res.asString());
    }

    @Test
    void testByteArrayDeprecatedOnMergeErrorIsInvoked() throws Exception {
        DeprecatedHookByteArrayOp op = new DeprecatedHookByteArrayOp();

        byte[] key = "k".getBytes(UTF_8);
        byte[] existing = "base".getBytes(UTF_8);
        byte[][] operands = new byte[][]{
                "op".getBytes(UTF_8)
        };

        Result res = invokeFullMergeCb(op, key, existing, operands);

        assertEquals((byte) 1, res.success, "fallback should still succeed");
        assertTrue(op.errorSeen, "deprecated onMergeError must be invoked");
        assertFalse(op.partialSeen, "full merge should report partial=false");
    }

    @Test
    void testByteArrayDeprecatedOnMergeErrorInvokedForPartial() throws Exception {
        DeprecatedPartialHook op = new DeprecatedPartialHook();

        byte[] key = "k".getBytes(UTF_8);
        byte[][] operands = new byte[][]{ "l".getBytes(UTF_8), "r".getBytes(UTF_8) };

        Result res = invokePartialMergeCb(op, key, operands);

        assertEquals((byte) 1, res.success, "fallback should return first operand");
        assertTrue(op.isErrorSeen(), "deprecated onMergeError must be invoked on partial errors");
        assertTrue(op.isPartialSeen(), "partial flag should be true for partial merge errors");
        assertEquals("l", res.asString());
    }

    @Test
    void testByteArrayPartialMergeMultiPreferred() throws Exception {
        Result res = invokePartialMergeCb(new MultiPreferredByteArrayOp(), "k", "a", "b", "c");

        assertEquals((byte) 1, res.success);
        assertEquals("a+b+c", res.asString());
    }

    @Test
    void testByteArrayPartialMergeMultiNullManyOperandsFallbacksFirst() throws Exception {
        Result res = invokePartialMergeCb(new MultiNullFallbackByteArrayOp(), "k", "one", "two", "three");

        assertEquals((byte) 1, res.success);
        assertEquals("one", res.asString(), "fallback must return first operand when partial merges return null");
    }

    @Test
    void testByteArrayPartialMergeBothNullFallbacksFirstOperand() throws Exception {
        Result res = invokePartialMergeCb(new BothNullByteArrayOp(), "k", "left", "right");

        assertEquals((byte) 1, res.success, "fallback should mark success when returning first operand");
        assertEquals("left", res.asString(), "fallback must return first operand when both partial merges return null");
    }

    @Test
    void testByteArrayPartialMergeEmptyResultSetsZeroLength() throws Exception {
        Result res = invokePartialMergeCb(new EmptyPartialByteArrayOp(), "k", "op");

        assertEquals((byte) 1, res.success);
        assertEquals(0L, res.len);
        assertEquals(0, res.data.length);
    }

    @Test
    void testByteArrayFullMergeFallbackUsesExistingWhenMergeReturnsNull() throws Exception {
        Result res = invokeFullMergeCb(new NullFullMergeByteArrayOp(), "k", "base", "op1", "op2");

        assertEquals((byte) 1, res.success);
        assertEquals("base", res.asString());
    }

    @Test
    void testByteArrayFullMergeEmptyResultSetsZeroLength() throws Exception {
        Result res = invokeFullMergeCb(new EmptyFullMergeByteArrayOp(), "k", "base", "op");

        assertEquals((byte) 1, res.success);
        assertEquals(0L, res.len);
        assertEquals(0, res.data.length);
    }

    @Test
    void testConstructorThrowsWhenCreateReturnsNull() throws Exception {
        MethodHandle replacement = MethodHandles.dropArguments(
                MethodHandles.constant(MemorySegment.class, MemorySegment.NULL),
                0,
                MemorySegment.class, MemorySegment.class, MemorySegment.class,
                MemorySegment.class, MemorySegment.class, MemorySegment.class
        );

        MethodHandle original = swapHandle("CREATE_OP", replacement);
        try {
            RuntimeException ex = org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class, () -> new NoopOp("NullCreateOp"));
            assertTrue(ex.getMessage().contains("Failed to construct FFM MergeOperator"));
            assertNotNull(ex.getCause());
            assertTrue(ex.getCause().getMessage().contains("rocksdb_mergeoperator_create returned NULL"));
        } finally {
            swapHandle("CREATE_OP", original);
        }
    }

    @Test
    void testConstructorWrapsCreateException() throws Exception {
        MethodHandle replacement = MethodHandles.dropArguments(
                MethodHandles.throwException(MemorySegment.class, IllegalStateException.class)
                        .bindTo(new IllegalStateException("create boom")),
                0,
                MemorySegment.class, MemorySegment.class, MemorySegment.class,
                MemorySegment.class, MemorySegment.class, MemorySegment.class
        );

        MethodHandle original = swapHandle("CREATE_OP", replacement);
        try {
            RuntimeException ex = org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class, () -> new NoopOp("ThrowingCreateOp"));
            assertTrue(ex.getMessage().contains("Failed to construct FFM MergeOperator"));
            assertTrue(ex.getCause() instanceof IllegalStateException);
        } finally {
            swapHandle("CREATE_OP", original);
        }
    }

    @Test
    void testConstructorFailsWhenNativeHandleInjectionBlocked() throws Exception {
        RuntimeException ex = org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class, HandleInjectionFailOp::new);
        assertTrue(ex.getMessage().contains("Failed to construct FFM MergeOperator"));
        assertNotNull(ex.getCause());
        assertTrue(ex.getCause().getMessage().contains("Could not inject native handle"));
    }

    @Test
    void testDisposeInternalSuppressesDestroyErrors() throws Exception {
        NoopOp op = new NoopOp("DestroyErrorOp");
        MethodHandle throwingDestroy = MethodHandles.dropArguments(
                MethodHandles.throwException(void.class, IllegalStateException.class)
                        .bindTo(new IllegalStateException("destroy boom")),
                0,
                MemorySegment.class
        );

        MethodHandle original = swapHandle("DESTROY_OP", throwingDestroy);
        try {
            MemorySegment container = getSharedPtrContainer(op);
            long handle = container.address();
            assertDoesNotThrow(() -> op.disposeInternal(handle));
        } finally {
            swapHandle("DESTROY_OP", original);
        }
    }

    @Test
    void testPartialMergeFallbackAllocationFailureReturnsNull() throws Exception {
        MethodHandle failingMalloc = MethodHandles.dropArguments(
                MethodHandles.constant(MemorySegment.class, MemorySegment.NULL),
                0,
                long.class
        );

        MethodHandle original = swapHandle("MALLOC", failingMalloc);
        try {
            FallbackAllocFailOp op = new FallbackAllocFailOp();
            byte[] key = "k".getBytes(UTF_8);
            byte[][] operands = new byte[][]{ "x".getBytes(UTF_8) };

            Result res = invokePartialMergeCb(op, key, operands);

            assertEquals((byte) 0, res.success, "allocation failure should signal failure");
            assertEquals(0L, res.len);
            assertEquals("k", op.keySeen);
            assertTrue(op.errorSeen, "onMergeError must be invoked on allocation failure");
            assertTrue(op.partialSeen, "partial flag should be true");
        } finally {
            swapHandle("MALLOC", original);
        }
    }

    @SuppressWarnings({"deprecation", "removal"})
    private static MethodHandle swapHandle(String fieldName, MethodHandle replacement) throws Exception {
        Field f = FFMAbstractMergeOperator.class.getDeclaredField(fieldName);
        f.setAccessible(true);
        Object base = getUnsafe().staticFieldBase(f);
        long offset = getUnsafe().staticFieldOffset(f);
        MethodHandle original = (MethodHandle) getUnsafe().getObject(base, offset);
        getUnsafe().putObject(base, offset, replacement);
        return original;
    }

    private static Unsafe getUnsafe() throws Exception {
        Field f = Unsafe.class.getDeclaredField("theUnsafe");
        f.setAccessible(true);
        return (Unsafe) f.get(null);
    }

    private static MemorySegment getSharedPtrContainer(FFMAbstractMergeOperator op) throws Exception {
        Field f = FFMAbstractMergeOperator.class.getDeclaredField("sharedPtrContainer");
        f.setAccessible(true);
        return (MemorySegment) f.get(op);
    }

    private static final class NoopOp extends FFMAbstractMergeOperator {
        private NoopOp(String name) {
            super(name);
        }

        @Override
        public it.cavallium.buffer.Buf merge(it.cavallium.buffer.Buf key, it.cavallium.buffer.Buf existingValue, List<it.cavallium.buffer.Buf> operands) {
            return null;
        }

        @Override
        public it.cavallium.buffer.Buf partialMergeMulti(it.cavallium.buffer.Buf key, List<it.cavallium.buffer.Buf> operands) {
            return null;
        }

        @Override
        public it.cavallium.buffer.Buf partialMerge(it.cavallium.buffer.Buf key, it.cavallium.buffer.Buf leftOperand, it.cavallium.buffer.Buf rightOperand) {
            return null;
        }
    }

    private static final class HandleInjectionFailOp extends FFMAbstractMergeOperator {
        private HandleInjectionFailOp() {
            super("HandleInjectionFailOp");
        }

        @Override
        protected void injectNativeHandle(long handle) throws Exception {
            throw new IllegalAccessException("blocked");
        }

        @Override
        public it.cavallium.buffer.Buf merge(it.cavallium.buffer.Buf key, it.cavallium.buffer.Buf existingValue, List<it.cavallium.buffer.Buf> operands) {
            return null;
        }
    }

    private static final class FallbackAllocFailOp extends FFMAbstractMergeOperator {
        private volatile boolean errorSeen;
        private volatile boolean partialSeen;
        private volatile String keySeen;

        private FallbackAllocFailOp() {
            super("FallbackAllocFailOp");
        }

        @Override
        public it.cavallium.buffer.Buf merge(it.cavallium.buffer.Buf key, it.cavallium.buffer.Buf existingValue, List<it.cavallium.buffer.Buf> operands) {
            return null;
        }

        @Override
        public it.cavallium.buffer.Buf partialMergeMulti(it.cavallium.buffer.Buf key, List<it.cavallium.buffer.Buf> operands) {
            return null;
        }

        @Override
        public it.cavallium.buffer.Buf partialMerge(it.cavallium.buffer.Buf key, it.cavallium.buffer.Buf leftOperand, it.cavallium.buffer.Buf rightOperand) {
            return null;
        }

        @Override
        protected void onMergeError(Throwable t, boolean partial, it.cavallium.buffer.Buf key) {
            this.errorSeen = true;
            this.partialSeen = partial;
            this.keySeen = key != null ? new String(key.asArray(), UTF_8) : null;
        }
    }

    private static final class ThrowingMergeOperator extends FFMByteArrayMergeOperator {

        private ThrowingMergeOperator() {
            super("ThrowingMergeOperator");
        }

        @Override
        public byte[] merge(byte[] key, byte[] existingValue, java.util.List<byte[]> operands) {
            throw new RuntimeException("merge boom");
        }
    }

    private static final class HookedThrowingOperator extends FFMByteArrayMergeOperator {

        private volatile boolean errorSeen;

        private HookedThrowingOperator() {
            super("HookedThrowingOperator");
        }

        @Override
        public byte[] merge(byte[] key, byte[] existingValue, java.util.List<byte[]> operands) {
            throw new IllegalStateException("merge kaboom");
        }

        @Override
        protected void onMergeError(Throwable t, boolean partial, it.cavallium.buffer.Buf key) {
            this.errorSeen = true;
        }
    }

    private static final class HookThrowingHookOperator extends FFMByteArrayMergeOperator {

        private HookThrowingHookOperator() {
            super("HookThrowingHookOperator");
        }

        @Override
        public byte[] merge(byte[] key, byte[] existingValue, java.util.List<byte[]> operands) {
            throw new IllegalStateException("merge explode");
        }

        @Override
        protected void onMergeError(Throwable t, boolean partial, it.cavallium.buffer.Buf key) {
            throw new RuntimeException("hook explode");
        }
    }

    private static final class ErroringMergeOperator extends FFMByteArrayMergeOperator {

        private ErroringMergeOperator() {
            super("ErroringMergeOperator");
        }

        @Override
        public byte[] merge(byte[] key, byte[] existingValue, java.util.List<byte[]> operands) {
            throw new AssertionError("boom");
        }
    }

    private static final class KeyCapturingFullMergeOp extends FFMAbstractMergeOperator {

        private volatile String keySeen;

        private KeyCapturingFullMergeOp() {
            super("KeyCapturingFullMergeOp");
        }

        @Override
        public it.cavallium.buffer.Buf merge(it.cavallium.buffer.Buf key, it.cavallium.buffer.Buf existingValue, List<it.cavallium.buffer.Buf> operands) {
            throw new IllegalStateException("boom");
        }

        @Override
        protected void onMergeError(Throwable t, boolean partial, it.cavallium.buffer.Buf key) {
            this.keySeen = new String(key.asArray(), UTF_8);
        }
    }

    private static final class KeyCapturingPartialMergeOp extends FFMAbstractMergeOperator {

        private volatile String keySeen;

        private KeyCapturingPartialMergeOp() {
            super("KeyCapturingPartialMergeOp");
        }

        @Override
        public it.cavallium.buffer.Buf merge(it.cavallium.buffer.Buf key, it.cavallium.buffer.Buf existingValue, List<it.cavallium.buffer.Buf> operands) {
            throw new UnsupportedOperationException();
        }

        @Override
        public it.cavallium.buffer.Buf partialMergeMulti(it.cavallium.buffer.Buf key, List<it.cavallium.buffer.Buf> operands) {
            throw new IllegalStateException("partial boom");
        }

        @Override
        public it.cavallium.buffer.Buf partialMerge(it.cavallium.buffer.Buf key, it.cavallium.buffer.Buf leftOperand, it.cavallium.buffer.Buf rightOperand) {
            throw new IllegalStateException("pair boom");
        }

        @Override
        protected void onMergeError(Throwable t, boolean partial, it.cavallium.buffer.Buf key) {
            this.keySeen = new String(key.asArray(), UTF_8);
        }
    }

    private static final class TestableErroringPartialMergeOp extends FFMAbstractMergeOperator {

        private volatile boolean errorSeen;

        private TestableErroringPartialMergeOp() {
            super("TestableErroringPartialMergeOp");
        }

        @Override
        public it.cavallium.buffer.Buf merge(it.cavallium.buffer.Buf key, it.cavallium.buffer.Buf existingValue, List<it.cavallium.buffer.Buf> operands) {
            throw new UnsupportedOperationException();
        }

        @Override
        public it.cavallium.buffer.Buf partialMergeMulti(it.cavallium.buffer.Buf key, List<it.cavallium.buffer.Buf> operands) {
            throw new AssertionError("partial multi boom");
        }

        @Override
        public it.cavallium.buffer.Buf partialMerge(it.cavallium.buffer.Buf key, it.cavallium.buffer.Buf leftOperand, it.cavallium.buffer.Buf rightOperand) {
            throw new AssertionError("partial pair boom");
        }

        @Override
        protected void onMergeError(Throwable t, boolean partial, it.cavallium.buffer.Buf key) {
            errorSeen = true;
        }
    }

    private static final class TestableMergeOp extends FFMAbstractMergeOperator {

        enum Mode { MULTI_OK, MULTI_NULL_PAIRWISE, MULTI_THROW }

        private final Mode mode;
        private volatile boolean errorSeen;
        private final java.util.concurrent.atomic.AtomicReference<String> invocationOrder = new java.util.concurrent.atomic.AtomicReference<>("");

        private TestableMergeOp(Mode mode) {
            super("TestableMergeOp" + mode);
            this.mode = mode;
        }

        @Override
        public it.cavallium.buffer.Buf merge(it.cavallium.buffer.Buf key, it.cavallium.buffer.Buf existingValue, List<it.cavallium.buffer.Buf> operands) {
            throw new UnsupportedOperationException();
        }

        @Override
        public it.cavallium.buffer.Buf partialMergeMulti(it.cavallium.buffer.Buf key, List<it.cavallium.buffer.Buf> operands) {
            return switch (mode) {
                case MULTI_OK -> {
                    invocationOrder.compareAndSet("", "MULTI");
                    yield it.cavallium.buffer.Buf.wrap(String.join("+", operands.stream().map(o -> new String(o.asArray(), UTF_8)).toList()).getBytes(UTF_8));
                }
                case MULTI_NULL_PAIRWISE -> null;
                case MULTI_THROW -> throw new IllegalStateException("boom");
            };
        }

        @Override
        public it.cavallium.buffer.Buf partialMerge(it.cavallium.buffer.Buf key, it.cavallium.buffer.Buf leftOperand, it.cavallium.buffer.Buf rightOperand) {
            invocationOrder.compareAndSet("", "PAIRWISE");
            return it.cavallium.buffer.Buf.wrap((new String(leftOperand.asArray(), UTF_8) + "|" + new String(rightOperand.asArray(), UTF_8)).getBytes(UTF_8));
        }

        @Override
        protected void onMergeError(Throwable t, boolean partial, it.cavallium.buffer.Buf key) {
            errorSeen = true;
        }
    }

    private static final class OuterCatchOp extends FFMAbstractMergeOperator {

        private volatile boolean errorSeen;
        private volatile boolean partialSeen;
        private volatile String keyString;

        private OuterCatchOp() {
            super("OuterCatchOp");
        }

        @Override
        public it.cavallium.buffer.Buf merge(it.cavallium.buffer.Buf key, it.cavallium.buffer.Buf existingValue, List<it.cavallium.buffer.Buf> operands) {
            return null;
        }

        @Override
        public it.cavallium.buffer.Buf partialMergeMulti(it.cavallium.buffer.Buf key, List<it.cavallium.buffer.Buf> operands) {
            return null;
        }

        @Override
        public it.cavallium.buffer.Buf partialMerge(it.cavallium.buffer.Buf key, it.cavallium.buffer.Buf leftOperand, it.cavallium.buffer.Buf rightOperand) {
            return null;
        }

        @Override
        protected void onMergeError(Throwable t, boolean partial, it.cavallium.buffer.Buf key) {
            this.errorSeen = true;
            this.partialSeen = partial;
            this.keyString = key != null ? new String(key.asArray(), UTF_8) : null;
        }
    }

    private static final class DefaultPartialMergeOp extends FFMAbstractMergeOperator {

        private DefaultPartialMergeOp() {
            super("DefaultPartialMergeOp");
        }

        @Override
        public it.cavallium.buffer.Buf merge(it.cavallium.buffer.Buf key, it.cavallium.buffer.Buf existingValue, List<it.cavallium.buffer.Buf> operands) {
            return null;
        }

        @Override
        public it.cavallium.buffer.Buf partialMergeMulti(it.cavallium.buffer.Buf key, List<it.cavallium.buffer.Buf> operands) {
            return null;
        }

        @Override
        public it.cavallium.buffer.Buf partialMerge(it.cavallium.buffer.Buf key, it.cavallium.buffer.Buf leftOperand, it.cavallium.buffer.Buf rightOperand) {
            return null;
        }
    }

    private static final class DeprecatedHookByteArrayOp extends FFMByteArrayMergeOperator {

        private volatile boolean errorSeen;
        private volatile boolean partialSeen;

        private DeprecatedHookByteArrayOp() {
            super("DeprecatedHookByteArrayOp");
        }

        @Override
        public byte[] merge(byte[] key, byte[] existingValue, List<byte[]> operands) {
            throw new IllegalStateException("boom");
        }

        @Override
        public byte[] partialMergeMulti(byte[] key, List<byte[]> operands) {
            return null;
        }

        @Override
        public byte[] partialMerge(byte[] key, byte[] leftOperand, byte[] rightOperand) {
            return null;
        }

        @Override
        protected void onMergeError(Throwable t, boolean partial) {
            this.errorSeen = true;
            this.partialSeen = partial;
        }
    }

    private static final class DeprecatedPartialHook extends FFMByteArrayMergeOperator {

        private volatile boolean errorSeen;
        private volatile boolean partialSeen;

        private DeprecatedPartialHook() {
            super("DeprecatedPartialHook");
        }

        @Override
        public byte[] merge(byte[] key, byte[] existingValue, List<byte[]> operands) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] partialMergeMulti(byte[] key, List<byte[]> operands) {
            throw new IllegalStateException("partial boom");
        }

        @Override
        protected void onMergeError(Throwable t, boolean partial) {
            this.errorSeen = true;
            this.partialSeen = partial;
        }

        boolean isErrorSeen() {
            return errorSeen;
        }

        boolean isPartialSeen() {
            return partialSeen;
        }
    }

    private static final class PairwiseFallbackByteArrayOp extends FFMByteArrayMergeOperator {

        private PairwiseFallbackByteArrayOp() {
            super("PairwiseFallbackByteArrayOp");
        }

        @Override
        public byte[] merge(byte[] key, byte[] existingValue, List<byte[]> operands) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] partialMergeMulti(byte[] key, List<byte[]> operands) {
            return null;
        }

        @Override
        public byte[] partialMerge(byte[] key, byte[] leftOperand, byte[] rightOperand) {
            return (new String(leftOperand, UTF_8) + "+" + new String(rightOperand, UTF_8)).getBytes(UTF_8);
        }
    }

    private static final class MultiPreferredByteArrayOp extends FFMByteArrayMergeOperator {

        private MultiPreferredByteArrayOp() {
            super("MultiPreferredByteArrayOp");
        }

        @Override
        public byte[] merge(byte[] key, byte[] existingValue, List<byte[]> operands) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] partialMergeMulti(byte[] key, List<byte[]> operands) {
            return String.join("+", operands.stream().map(o -> new String(o, UTF_8)).toList()).getBytes(UTF_8);
        }
    }

    private static final class MultiNullFallbackByteArrayOp extends FFMByteArrayMergeOperator {

        private MultiNullFallbackByteArrayOp() {
            super("MultiNullFallbackByteArrayOp");
        }

        @Override
        public byte[] merge(byte[] key, byte[] existingValue, List<byte[]> operands) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] partialMergeMulti(byte[] key, List<byte[]> operands) {
            return null;
        }
    }

    private static final class BothNullByteArrayOp extends FFMByteArrayMergeOperator {

        private BothNullByteArrayOp() {
            super("BothNullByteArrayOp");
        }

        @Override
        public byte[] merge(byte[] key, byte[] existingValue, List<byte[]> operands) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] partialMergeMulti(byte[] key, List<byte[]> operands) {
            return null;
        }

        @Override
        public byte[] partialMerge(byte[] key, byte[] leftOperand, byte[] rightOperand) {
            return null;
        }
    }

    private static final class EmptyPartialByteArrayOp extends FFMByteArrayMergeOperator {

        private EmptyPartialByteArrayOp() {
            super("EmptyPartialByteArrayOp");
        }

        @Override
        public byte[] merge(byte[] key, byte[] existingValue, List<byte[]> operands) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] partialMergeMulti(byte[] key, List<byte[]> operands) {
            return new byte[0];
        }
    }

    private static final class NullFullMergeByteArrayOp extends FFMByteArrayMergeOperator {

        private NullFullMergeByteArrayOp() {
            super("NullFullMergeByteArrayOp");
        }

        @Override
        public byte[] merge(byte[] key, byte[] existingValue, List<byte[]> operands) {
            return null;
        }
    }

    private static final class EmptyFullMergeByteArrayOp extends FFMByteArrayMergeOperator {

        private EmptyFullMergeByteArrayOp() {
            super("EmptyFullMergeByteArrayOp");
        }

        @Override
        public byte[] merge(byte[] key, byte[] existingValue, List<byte[]> operands) {
            return new byte[0];
        }
    }

    private record Result(byte success, long len, byte[] data) {
        String asString() {
            return new String(data, UTF_8);
        }
    }

    private static Result invokePartialMergeCb(FFMAbstractMergeOperator op, byte[] key, byte[][] operands) throws Exception {
        Method m = FFMAbstractMergeOperator.class.getDeclaredMethod(
                "partialMergeCb",
                java.lang.foreign.MemorySegment.class,
                java.lang.foreign.MemorySegment.class,
                long.class,
                java.lang.foreign.MemorySegment.class,
                java.lang.foreign.MemorySegment.class,
                int.class,
                java.lang.foreign.MemorySegment.class,
                java.lang.foreign.MemorySegment.class
        );
        m.setAccessible(true);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment keySeg = arena.allocate(key.length);
            MemorySegment.copy(MemorySegment.ofArray(key), 0, keySeg, 0, key.length);

            MemorySegment opPtrs = arena.allocate(operands.length * ValueLayout.ADDRESS.byteSize(), ValueLayout.ADDRESS.byteAlignment());
            MemorySegment opLens = arena.allocate(operands.length * ValueLayout.JAVA_LONG.byteSize(), ValueLayout.JAVA_LONG.byteAlignment());
            for (int i = 0; i < operands.length; i++) {
                MemorySegment opSeg = arena.allocate(operands[i].length);
                MemorySegment.copy(MemorySegment.ofArray(operands[i]), 0, opSeg, 0, operands[i].length);
                opPtrs.setAtIndex(ValueLayout.ADDRESS, i, opSeg);
                opLens.setAtIndex(ValueLayout.JAVA_LONG, i, (long) operands[i].length);
            }

            MemorySegment successPtr = arena.allocate(ValueLayout.JAVA_BYTE);
            MemorySegment lenPtr = arena.allocate(ValueLayout.JAVA_LONG);

            MemorySegment resSeg = (MemorySegment) m.invoke(op,
                    MemorySegment.NULL,
                    keySeg,
                    (long) key.length,
                    opPtrs,
                    opLens,
                    operands.length,
                    successPtr,
                    lenPtr);

            byte success = successPtr.get(ValueLayout.JAVA_BYTE, 0);
            long len = lenPtr.get(ValueLayout.JAVA_LONG, 0);
            byte[] data = new byte[(int) len];
            MemorySegment.copy(resSeg.reinterpret(len), 0, MemorySegment.ofArray(data), 0, len);
            return new Result(success, len, data);
        }
    }

    private static Result invokePartialMergeCb(FFMByteArrayMergeOperator op, String key, String... operands) throws Exception {
        byte[] keyBytes = key.getBytes(UTF_8);
        byte[][] operandBytes = new byte[operands.length][];
        for (int i = 0; i < operands.length; i++) {
            operandBytes[i] = operands[i].getBytes(UTF_8);
        }
        return invokePartialMergeCb(op, keyBytes, operandBytes);
    }

    private static Result invokeFullMergeCb(FFMAbstractMergeOperator op, byte[] key, byte[] existing, byte[][] operands) throws Exception {
        Method m = FFMAbstractMergeOperator.class.getDeclaredMethod(
                "fullMerge",
                java.lang.foreign.MemorySegment.class,
                java.lang.foreign.MemorySegment.class,
                long.class,
                java.lang.foreign.MemorySegment.class,
                long.class,
                java.lang.foreign.MemorySegment.class,
                java.lang.foreign.MemorySegment.class,
                int.class,
                java.lang.foreign.MemorySegment.class,
                java.lang.foreign.MemorySegment.class
        );
        m.setAccessible(true);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment keySeg = arena.allocate(key.length);
            MemorySegment.copy(MemorySegment.ofArray(key), 0, keySeg, 0, key.length);

            MemorySegment existingSeg = MemorySegment.NULL;
            long existingLen = 0;
            if (existing != null) {
                existingSeg = arena.allocate(existing.length);
                MemorySegment.copy(MemorySegment.ofArray(existing), 0, existingSeg, 0, existing.length);
                existingLen = existing.length;
            }

            MemorySegment opPtrs = arena.allocate(operands.length * ValueLayout.ADDRESS.byteSize(), ValueLayout.ADDRESS.byteAlignment());
            MemorySegment opLens = arena.allocate(operands.length * ValueLayout.JAVA_LONG.byteSize(), ValueLayout.JAVA_LONG.byteAlignment());
            for (int i = 0; i < operands.length; i++) {
                MemorySegment opSeg = arena.allocate(operands[i].length);
                MemorySegment.copy(MemorySegment.ofArray(operands[i]), 0, opSeg, 0, operands[i].length);
                opPtrs.setAtIndex(ValueLayout.ADDRESS, i, opSeg);
                opLens.setAtIndex(ValueLayout.JAVA_LONG, i, (long) operands[i].length);
            }

            MemorySegment successPtr = arena.allocate(ValueLayout.JAVA_BYTE);
            MemorySegment lenPtr = arena.allocate(ValueLayout.JAVA_LONG);

            MemorySegment resSeg = (MemorySegment) m.invoke(op,
                    MemorySegment.NULL,
                    keySeg,
                    (long) key.length,
                    existingSeg,
                    existingLen,
                    opPtrs,
                    opLens,
                    operands.length,
                    successPtr,
                    lenPtr);

            byte success = successPtr.get(ValueLayout.JAVA_BYTE, 0);
            long len = lenPtr.get(ValueLayout.JAVA_LONG, 0);
            byte[] data = new byte[(int) len];
            if (len > 0) {
                MemorySegment.copy(resSeg.reinterpret(len), 0, MemorySegment.ofArray(data), 0, len);
            }
            return new Result(success, len, data);
        }
    }

    private static Result invokeFullMergeCb(FFMByteArrayMergeOperator op, String key, String existing, String... operands) throws Exception {
        byte[] keyBytes = key.getBytes(UTF_8);
        byte[] existingBytes = existing != null ? existing.getBytes(UTF_8) : null;
        byte[][] operandBytes = new byte[operands.length][];
        for (int i = 0; i < operands.length; i++) {
            operandBytes[i] = operands[i].getBytes(UTF_8);
        }
        return invokeFullMergeCb(op, keyBytes, existingBytes, operandBytes);
    }

    private static Result invokePartialMergeCbRaw(FFMAbstractMergeOperator op,
                                                  MemorySegment keySeg,
                                                  long keyLen,
                                                  MemorySegment opPtrs,
                                                  MemorySegment opLens,
                                                  int numOps) throws Exception {
        Method m = FFMAbstractMergeOperator.class.getDeclaredMethod(
                "partialMergeCb",
                java.lang.foreign.MemorySegment.class,
                java.lang.foreign.MemorySegment.class,
                long.class,
                java.lang.foreign.MemorySegment.class,
                java.lang.foreign.MemorySegment.class,
                int.class,
                java.lang.foreign.MemorySegment.class,
                java.lang.foreign.MemorySegment.class
        );
        m.setAccessible(true);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment successPtr = arena.allocate(ValueLayout.JAVA_BYTE);
            MemorySegment lenPtr = arena.allocate(ValueLayout.JAVA_LONG);

            MemorySegment resSeg = (MemorySegment) m.invoke(op,
                    MemorySegment.NULL,
                    keySeg,
                    keyLen,
                    opPtrs,
                    opLens,
                    numOps,
                    successPtr,
                    lenPtr);

            byte success = successPtr.get(ValueLayout.JAVA_BYTE, 0);
            long len = lenPtr.get(ValueLayout.JAVA_LONG, 0);
            byte[] data = len > 0 ? new byte[(int) len] : new byte[0];
            if (len > 0) {
                MemorySegment.copy(resSeg.reinterpret(len), 0, MemorySegment.ofArray(data), 0, len);
            }
            return new Result(success, len, data);
        }
    }

    private static Result invokeFullMergeCbRaw(FFMAbstractMergeOperator op,
                                               MemorySegment keySeg,
                                               long keyLen,
                                               MemorySegment existingSeg,
                                               long existingLen,
                                               MemorySegment opPtrs,
                                               MemorySegment opLens,
                                               int numOps) throws Exception {
        Method m = FFMAbstractMergeOperator.class.getDeclaredMethod(
                "fullMerge",
                java.lang.foreign.MemorySegment.class,
                java.lang.foreign.MemorySegment.class,
                long.class,
                java.lang.foreign.MemorySegment.class,
                long.class,
                java.lang.foreign.MemorySegment.class,
                java.lang.foreign.MemorySegment.class,
                int.class,
                java.lang.foreign.MemorySegment.class,
                java.lang.foreign.MemorySegment.class
        );
        m.setAccessible(true);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment successPtr = arena.allocate(ValueLayout.JAVA_BYTE);
            MemorySegment lenPtr = arena.allocate(ValueLayout.JAVA_LONG);

            MemorySegment resSeg = (MemorySegment) m.invoke(op,
                    MemorySegment.NULL,
                    keySeg,
                    keyLen,
                    existingSeg,
                    existingLen,
                    opPtrs,
                    opLens,
                    numOps,
                    successPtr,
                    lenPtr);

            byte success = successPtr.get(ValueLayout.JAVA_BYTE, 0);
            long len = lenPtr.get(ValueLayout.JAVA_LONG, 0);
            byte[] data = len > 0 ? new byte[(int) len] : new byte[0];
            if (len > 0) {
                MemorySegment.copy(resSeg.reinterpret(len), 0, MemorySegment.ofArray(data), 0, len);
            }
            return new Result(success, len, data);
        }
    }
}
