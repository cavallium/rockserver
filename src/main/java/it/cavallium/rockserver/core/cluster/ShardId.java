package it.cavallium.rockserver.core.cluster;

public record ShardId(long value) implements Comparable<ShardId> {

	public static final ShardId DEFAULT = new ShardId(0);

	public ShardId {
		if (value < 0) {
			throw new IllegalArgumentException("shard id must be >= 0");
		}
	}

	@Override
	public int compareTo(ShardId other) {
		return Long.compare(value, other.value);
	}

	@Override
	public String toString() {
		return Long.toUnsignedString(value);
	}
}
