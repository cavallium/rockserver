package it.cavallium.rockserver.distributed;

/**
 * Binary encoding of {@link LogEntry}. The format is versioned by a leading byte; there is one current version.
 * Must be deterministic: equal entries encode to equal bytes.
 */
final class LogEntryCodec {

	static final byte VERSION = 1;

	private LogEntryCodec() {}

	static byte[] encode(LogEntry entry) {
		throw new UnsupportedOperationException("TODO");
	}

	/** @throws IllegalArgumentException on malformed input or unknown version */
	static LogEntry decode(byte[] data) {
		throw new UnsupportedOperationException("TODO");
	}
}
