package it.cavallium.rockserver.core.cluster;

import java.util.UUID;

public record ClientId(UUID value) implements Comparable<ClientId> {

	public ClientId {
		requireArgument(value, "value");
	}

	public static ClientId random() {
		return new ClientId(UUID.randomUUID());
	}

	public static ClientId parse(String value) {
		return new ClientId(UUID.fromString(value));
	}

	@Override
	public int compareTo(ClientId other) {
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
