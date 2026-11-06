package it.cavallium.rockserver.core.common;

import java.util.Objects;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Database-scoped identity of one immutable SST file captured by a raw scan.
 *
 * <p>The value is RocksDB's immutable table filename. Clients should still treat
 * it as opaque and return it unchanged: tokens are meaningful only to the
 * Rockserver database that issued them. A later resumable scan skips the exact
 * SST only while that filename is still live; unknown or stale tokens are
 * ignored.</p>
 *
 * <p>A server may tag the filename with its origin, as {@code <origin>@<filename>}
 * (the replicated server names the node and the Raft term that issued it, see
 * {@link #tagged}): file numbers are unique only within one database, so a
 * replicated server ignores a token issued by another node, or by itself under
 * another term, instead of skipping a different file of the same name.</p>
 */
public record RawSstToken(String value) {

	private static final String SUFFIX = ".sst";
	private static final int MIN_FILE_NUMBER_DIGITS = 6;
	private static final int MAX_FILE_NUMBER_DIGITS = 20;
	private static final char ORIGIN_SEPARATOR = '@';
	/** The longest origin tag, see {@link #tagged}. */
	public static final int MAX_ORIGIN_CHARACTERS = 128;
	public static final int MIN_CHARACTERS = MIN_FILE_NUMBER_DIGITS + SUFFIX.length();
	public static final int MAX_CHARACTERS = MAX_ORIGIN_CHARACTERS + 1 + 1 + MAX_FILE_NUMBER_DIGITS + SUFFIX.length();

	public RawSstToken {
		Objects.requireNonNull(value, "value");
		int separator = value.indexOf(ORIGIN_SEPARATOR);
		if (separator >= 0) {
			validateOrigin(value, separator);
		}
		int digitsStart = separator + 1 + (value.startsWith("/", separator + 1) ? 1 : 0);
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

	/** {@code token}'s filename tagged with {@code origin}, which may hold ASCII letters, digits and {@code ._~-}. */
	public static RawSstToken tagged(String origin, RawSstToken token) {
		return new RawSstToken(origin + ORIGIN_SEPARATOR + token.fileName());
	}

	/** The origin tag, or {@code null} for a plain filename. */
	public @Nullable String origin() {
		int separator = value.indexOf(ORIGIN_SEPARATOR);
		return separator < 0 ? null : value.substring(0, separator);
	}

	/** The RocksDB table filename, without the origin tag. */
	public RawSstToken untagged() {
		int separator = value.indexOf(ORIGIN_SEPARATOR);
		return separator < 0 ? this : new RawSstToken(value.substring(separator + 1));
	}

	private String fileName() {
		return value.substring(value.indexOf(ORIGIN_SEPARATOR) + 1);
	}

	private static void validateOrigin(String value, int separator) {
		if (separator == 0 || separator > MAX_ORIGIN_CHARACTERS) {
			throw invalidToken();
		}
		for (int i = 0; i < separator; i++) {
			char c = value.charAt(i);
			if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
					|| c == '.' || c == '_' || c == '~' || c == '-')) {
				throw invalidToken();
			}
		}
	}

	private static IllegalArgumentException invalidToken() {
		return new IllegalArgumentException("Raw SST token must be a canonical RocksDB table filename, "
				+ "for example /000123.sst, optionally tagged as <origin>@/000123.sst");
	}

	@Override
	public @NotNull String toString() {
		return value;
	}
}
