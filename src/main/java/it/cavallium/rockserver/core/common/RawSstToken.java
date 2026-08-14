package it.cavallium.rockserver.core.common;

import java.util.Objects;
import org.jetbrains.annotations.NotNull;

/**
 * Database-scoped identity of one immutable SST file captured by a raw scan.
 *
 * <p>The value is RocksDB's immutable table filename. Clients should still treat
 * it as opaque and return it unchanged: tokens are meaningful only to the
 * Rockserver database that issued them. A later resumable scan skips the exact
 * SST only while that filename is still live; unknown or stale tokens are
 * ignored.</p>
 */
public record RawSstToken(String value) {

	private static final String SUFFIX = ".sst";
	private static final int MIN_FILE_NUMBER_DIGITS = 6;
	private static final int MAX_FILE_NUMBER_DIGITS = 20;
	public static final int MIN_CHARACTERS = MIN_FILE_NUMBER_DIGITS + SUFFIX.length();
	public static final int MAX_CHARACTERS = 1 + MAX_FILE_NUMBER_DIGITS + SUFFIX.length();

	public RawSstToken {
		Objects.requireNonNull(value, "value");
		int digitsStart = value.startsWith("/") ? 1 : 0;
		int digitsEnd = value.length() - SUFFIX.length();
		int digitCount = digitsEnd - digitsStart;
		if (!value.endsWith(SUFFIX)
				|| digitCount < MIN_FILE_NUMBER_DIGITS
				|| digitCount > MAX_FILE_NUMBER_DIGITS
				|| (digitCount > MIN_FILE_NUMBER_DIGITS && value.charAt(digitsStart) == '0')) {
			throw invalidToken();
		}
		for (int i = digitsStart; i < digitsEnd; i++) {
			char character = value.charAt(i);
			if (character < '0' || character > '9') {
				throw invalidToken();
			}
		}
		try {
			if (Long.parseUnsignedLong(value, digitsStart, digitsEnd, 10) == 0L) {
				throw invalidToken();
			}
		} catch (NumberFormatException invalidNumber) {
			throw invalidToken();
		}
	}

	private static IllegalArgumentException invalidToken() {
		return new IllegalArgumentException("Raw SST token must be a canonical RocksDB table filename, "
				+ "for example /000123.sst");
	}

	@Override
	public @NotNull String toString() {
		return value;
	}
}
