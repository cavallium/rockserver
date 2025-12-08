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
		int elementCount = operands.size() + (existingValue != null ? 1 : 0);
		int estimatedLength = Math.max(elementCount - 1, 0);
		if (existingValue != null) {
			estimatedLength += existingValue.length;
		}
		for (byte[] op : operands) {
			estimatedLength += op.length;
		}

		StringBuilder sb = new StringBuilder(estimatedLength);
		boolean first = true;

		if (existingValue != null) {
			sb.append(new String(existingValue, StandardCharsets.UTF_8));
			first = false;
		}

		for (byte[] op : operands) {
			if (!first) {
				sb.append(',');
			}
			sb.append(new String(op, StandardCharsets.UTF_8));
			first = false;
		}

		return sb.toString().getBytes(StandardCharsets.UTF_8);
	}
}