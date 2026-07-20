package it.cavallium.rockserver.core.common;

import it.cavallium.rockserver.core.impl.XXHash32;
import it.cavallium.buffer.Buf;

public enum ColumnHashType implements HashFunction {
    XXHASH32(Integer.BYTES, (inputData, hashResult) -> XXHash32.getInstance()
            .hash(inputData, 0, inputData.size(), 0, hashResult)),
    XXHASH8(Byte.BYTES, (inputData, hashResult) -> {
        var xxHash = XXHash32.getInstance().hash(Utils.toByteArray(inputData), 0, inputData.size(), 0);
        hashResult.setByte(0, (byte) xxHash);
    }),
    ALLSAME8(Byte.BYTES, (inputData, hashResult) -> hashResult.setByte(0, (byte) 0)),
    FIXEDINTEGER32(Integer.BYTES, (inputData, hashResult) -> {
        if (inputData.size() != Integer.BYTES) {
            throw RocksDBException.of(RocksDBException.RocksDBErrorType.UNSUPPORTED_HASH_SIZE,
                    "FIXEDINTEGER32 input must be 4 bytes, got " + inputData.size());
        }
        var i = inputData.getInt(0);
        hashResult.setInt(0, i);
    });

    private final int bytes;
    private final HashFunction hashFunction;

    ColumnHashType(int bytes, HashFunction hashFunction) {
        this.bytes = bytes;
        this.hashFunction = hashFunction;
    }

    public int bytesSize() {
        return bytes;
    }

    public void hash(Buf inputData, Buf hashResult) {
        if (inputData == null) {
            throw RocksDBException.of(RocksDBException.RocksDBErrorType.NULL_ARGUMENT,
                    name() + " hash inputData must not be null");
        }
        if (hashResult == null) {
            throw RocksDBException.of(RocksDBException.RocksDBErrorType.NULL_ARGUMENT,
                    name() + " hashResult must not be null");
        }
        if (hashResult.size() < bytes) {
            throw RocksDBException.of(RocksDBException.RocksDBErrorType.UNSUPPORTED_HASH_SIZE,
                    name() + " hash output must be at least " + bytes + " bytes, got " + hashResult.size());
        }
        hashFunction.hash(inputData, hashResult);
    }

}
