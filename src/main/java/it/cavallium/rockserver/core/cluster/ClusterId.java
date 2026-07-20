package it.cavallium.rockserver.core.cluster;

import java.util.UUID;

public record ClusterId(UUID value) implements Comparable<ClusterId> {

	public ClusterId {
		requireArgument(value, "value");
	}

	public static ClusterId random() {
		return new ClusterId(UUID.randomUUID());
	}

	public static ClusterId parse(String value) {
		return new ClusterId(UUID.fromString(value));
	}

	@Override
	public int compareTo(ClusterId other) {
		return value.compareTo(other.value);
	}

	@Override
	public String toString() {
		return value.toString();
	}

	private static <T> T requireArgument(T value, String name) {
		if (value == null) {
			throw new IllegalArgumentException(name + " must not be null");
		}
		return value;
	}
}
