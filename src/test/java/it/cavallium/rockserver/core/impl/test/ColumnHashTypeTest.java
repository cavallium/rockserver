package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.impl.XXHash32;

import org.junit.jupiter.api.Test;

class ColumnHashTypeTest {

    @Test
    void fixedInteger32CopiesValidFourByteInput() {
        var input = Buf.wrap(new byte[]{0, 0, 0, 7});
        var output = Buf.createZeroes(Integer.BYTES);

        ColumnHashType.FIXEDINTEGER32.hash(input, output);

        assertEquals(7, output.getInt(0));
    }

    @Test
    void fixedInteger32RejectsNonFourByteInputWithPublicError() {
        var input = Buf.wrap(new byte[]{1, 2});
        var output = Buf.createZeroes(Integer.BYTES);

        var failure = assertThrows(RocksDBException.class,
                () -> ColumnHashType.FIXEDINTEGER32.hash(input, output));

        assertEquals(RocksDBException.RocksDBErrorType.UNSUPPORTED_HASH_SIZE, failure.getErrorUniqueId());
        assertEquals("FIXEDINTEGER32 input must be 4 bytes, got 2", failure.getMessage());
    }

    @Test
    void hashRejectsNullInputsWithPublicError() {
        var input = Buf.wrap(new byte[]{1});
        var output = Buf.createZeroes(Byte.BYTES);

        var nullInput = assertThrows(RocksDBException.class,
                () -> ColumnHashType.XXHASH8.hash(null, output));
        var nullOutput = assertThrows(RocksDBException.class,
                () -> ColumnHashType.ALLSAME8.hash(input, null));

        assertEquals(RocksDBException.RocksDBErrorType.NULL_ARGUMENT, nullInput.getErrorUniqueId());
        assertEquals("XXHASH8 hash inputData must not be null", nullInput.getMessage());
        assertEquals(RocksDBException.RocksDBErrorType.NULL_ARGUMENT, nullOutput.getErrorUniqueId());
        assertEquals("ALLSAME8 hashResult must not be null", nullOutput.getMessage());
    }

    @Test
    void hashRejectsTooSmallOutputWithPublicError() {
        var input = Buf.wrap(new byte[]{1});

        var shortXxhash32Output = assertThrows(RocksDBException.class,
                () -> ColumnHashType.XXHASH32.hash(input, Buf.createZeroes(Integer.BYTES - 1)));
        var emptyXxhash8Output = assertThrows(RocksDBException.class,
                () -> ColumnHashType.XXHASH8.hash(input, Buf.createZeroes(0)));

        assertEquals(RocksDBException.RocksDBErrorType.UNSUPPORTED_HASH_SIZE,
                shortXxhash32Output.getErrorUniqueId());
        assertEquals("XXHASH32 hash output must be at least 4 bytes, got 3",
                shortXxhash32Output.getMessage());
        assertEquals(RocksDBException.RocksDBErrorType.UNSUPPORTED_HASH_SIZE,
                emptyXxhash8Output.getErrorUniqueId());
        assertEquals("XXHASH8 hash output must be at least 1 bytes, got 0",
                emptyXxhash8Output.getMessage());
    }

    @Test
    void xxhash32ImplementationRejectsTooSmallOutputWithArgumentError() {
        var input = Buf.wrap(new byte[]{1});
        var output = Buf.createZeroes(Integer.BYTES - 1);

        var failure = assertThrows(IllegalArgumentException.class,
                () -> XXHash32.getInstance().hash(input, 0, input.size(), 0, output));

        assertEquals("result must be at least 4 bytes, got 3", failure.getMessage());
    }
}
