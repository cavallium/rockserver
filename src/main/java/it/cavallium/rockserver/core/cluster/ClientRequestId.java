package it.cavallium.rockserver.core.cluster;

public record ClientRequestId(ClientId clientId, long sequence) implements Comparable<ClientRequestId> {

	public ClientRequestId {
		requireArgument(clientId, "clientId");
		if (sequence < 0) {
			throw new IllegalArgumentException("sequence must be >= 0");
		}
	}

	public static ClientRequestId first(ClientId clientId) {
		return new ClientRequestId(clientId, 0);
	}

	public ClientRequestId next() {
		if (sequence == Long.MAX_VALUE) {
			throw new IllegalStateException("client request sequence exhausted");
		}
		return new ClientRequestId(clientId, sequence + 1);
	}

	@Override
	public int compareTo(ClientRequestId other) {
		int clientCmp = clientId.compareTo(other.clientId);
		if (clientCmp != 0) {
			return clientCmp;
		}
		return Long.compare(sequence, other.sequence);
	}

	private static <T> T requireArgument(T value, String name) {
		if (value == null) {
			throw new IllegalArgumentException(name + " must not be null");
		}
		return value;
	}
}
