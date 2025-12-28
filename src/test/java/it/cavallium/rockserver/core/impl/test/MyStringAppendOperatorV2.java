package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.impl.FFMByteArrayMergeOperator;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class MyStringAppendOperatorV2 extends FFMByteArrayMergeOperator {

	public MyStringAppendOperatorV2() {
		super("MyStringAppendOperator"); // Keep same name to simulate update of same logical operator, or different?
        // Usually name should match if we want RocksDB to accept it without complain?
        // RocksDB checks name. If name differs, it might complain "Merge operator changed".
        // But here we are changing the implementation (Class).
        // Let's keep the name "MyStringAppendOperator" but change the separator.
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
				sb.append(';'); // V2 uses semicolon
			}
			sb.append(new String(op, StandardCharsets.UTF_8));
			first = false;
		}

		return sb.toString().getBytes(StandardCharsets.UTF_8);
	}
}
