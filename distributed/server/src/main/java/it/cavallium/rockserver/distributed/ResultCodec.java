package it.cavallium.rockserver.distributed;

/** Binary encoding of {@link ApplyResult}, carried in Raft replies from the leader to the proposing node. */
final class ResultCodec {

	private ResultCodec() {}

	static byte[] encode(ApplyResult result) {
		throw new UnsupportedOperationException("TODO");
	}

	/** @throws IllegalArgumentException on malformed input */
	static ApplyResult decode(byte[] data) {
		throw new UnsupportedOperationException("TODO");
	}
}
