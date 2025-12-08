package it.cavallium.rockserver.core.impl;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.rocksdb.*;

public class MyStringAppendOperator extends FFMByteArrayMergeOperator {

	public MyStringAppendOperator() {
		super("MyStringAppendOperator");
	}

	@Override
	public byte[] merge(byte[] key, byte[] existingValue, List<byte[]> operands) {
		StringBuilder sb = new StringBuilder();

		// Handle existing value
		if (existingValue != null) {
			sb.append(new String(existingValue, StandardCharsets.UTF_8));
		}

		// Append all operands
		for (byte[] op : operands) {
			sb.append(",").append(new String(op, StandardCharsets.UTF_8));
		}

		return sb.toString().getBytes(StandardCharsets.UTF_8);
	}
}