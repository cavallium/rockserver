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
	ALLSAME8(Byte.BYTES, (inputData, hashResult) -> hashResult.setByte(0, (byte) 0));

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

}
