package it.cavallium.rockserver.core.common;

import it.cavallium.rockserver.core.impl.XXHash32;
import it.cavallium.buffer.Buf;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

public enum ColumnHashType implements HashFunction {
	XXHASH32(Integer.BYTES, (inputData, hashResult) -> hashResult.setInt(0, XXHash32.getInstance()
			.hash(inputData, 0, inputData.size(), 0))),
	// The low byte of the xxHash32
	XXHASH8(Byte.BYTES, (inputData, hashResult) -> hashResult.setByte(0, (byte) XXHash32.getInstance()
			.hash(inputData, 0, inputData.size(), 0))),
	ALLSAME8(Byte.BYTES, (inputData, hashResult) -> hashResult.setByte(0, (byte) 0)),
	FIXEDINTEGER32(Integer.BYTES, (inputData, hashResult) -> {
		if (inputData.size() != Integer.BYTES) {
			throw new UnsupportedOperationException("Input type is not 32 bits");
		}
		var i = inputData.getInt(0);
		hashResult.setInt(0, i);
	});

	// Composite keys store 32-bit values big-endian, like Buf.setInt
	private static final VarHandle INT_BE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);

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
		hashFunction.hash(inputData, hashResult);
	}

	/**
	 * Writes the same {@link #bytesSize()} bytes as {@link #hash(Buf, Buf)} straight into {@code out} at
	 * {@code offset}, without an intermediate result buffer.
	 */
	public void hashInto(Buf inputData, byte[] out, int offset) {
		switch (this) {
			case XXHASH32 -> INT_BE.set(out, offset, xxHash32(inputData));
			case XXHASH8 -> out[offset] = (byte) xxHash32(inputData);
			case ALLSAME8 -> out[offset] = 0;
			case FIXEDINTEGER32 -> {
				if (inputData.size() != Integer.BYTES) {
					throw new UnsupportedOperationException("Input type is not 32 bits");
				}
				INT_BE.set(out, offset, inputData.getInt(0));
			}
		}
	}

	private static int xxHash32(Buf inputData) {
		var xxHash = XXHash32.getInstance();
		byte[] array = inputData.getBackingByteArrayStrict();
		if (array != null) {
			return xxHash.hash(array, inputData.getBackingByteArrayOffset(), inputData.size(), 0);
		}
		return xxHash.hash(inputData, 0, inputData.size(), 0);
	}

}
