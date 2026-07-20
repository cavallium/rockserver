package it.cavallium.rockserver.core.cluster.command;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.Keys;

public record PartitionPointReadCommand(long schemaEpoch,
										long columnId,
										Keys keys) {

	public PartitionPointReadCommand {
		if (schemaEpoch < 0) {
			throw new IllegalArgumentException("schemaEpoch must be >= 0");
		}
		if (columnId < 0) {
			throw new IllegalArgumentException("columnId must be >= 0");
		}
		keys = copyKeys(requireArgument(keys, "keys"));
	}

	@Override
	public Keys keys() {
		return copyKeys(keys);
	}

	private static Keys copyKeys(Keys source) {
		var sourceKeys = requireArgument(source.keys(), "keys array");
		var copiedKeys = new Buf[sourceKeys.length];
		for (int i = 0; i < sourceKeys.length; i++) {
			copiedKeys[i] = Buf.wrap(requireArgument(sourceKeys[i], "keys[" + i + "]")
					.toByteArray()).freeze();
		}
		return new Keys(copiedKeys);
	}

	private static <T> T requireArgument(T value, String name) {
		if (value == null) {
			throw new IllegalArgumentException(name + " must not be null");
		}
		return value;
	}
}
