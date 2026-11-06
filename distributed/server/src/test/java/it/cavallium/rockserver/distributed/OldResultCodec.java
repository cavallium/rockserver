package it.cavallium.rockserver.distributed;

import static it.cavallium.rockserver.distributed.OldLogEntryCodec.writeBuf;
import static it.cavallium.rockserver.distributed.OldLogEntryCodec.writeBytes;
import static it.cavallium.rockserver.distributed.OldLogEntryCodec.writeNullableBuf;
import static it.cavallium.rockserver.distributed.OldLogEntryCodec.writeString;
import static java.nio.charset.StandardCharsets.UTF_8;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.Delta;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.jetbrains.annotations.Nullable;

/**
 * Binary encoding of {@link ApplyResult}, carried in Raft replies from the leader to the proposing node.
 *
 * <p>Layout: a tag byte for {@code Ok}, {@code Failed} or {@code ResendAlone}. {@code Ok} is followed by its value,
 * written as a type byte and then the value's fields; {@code Failed} by the error type name and the message;
 * {@code ResendAlone} by nothing. Primitive encodings are those of {@link OldLogEntryCodec}.</p>
 *
 * <p>A failure message is written leniently: it comes from an arbitrary exception during apply, so characters
 * that are not valid Unicode, such as unpaired surrogates, become {@code '?'} instead of failing the reply.</p>
 *
 * <p>Frozen copy of the stream-based codec that the exact-size one replaced, kept as the reference of the
 * wire format for {@link CodecEquivalenceTest} and {@link CodecBench}. Never change it.</p>
 */
final class OldResultCodec {

	private static final byte OK = 1;
	private static final byte FAILED = 2;
	private static final byte RESEND_ALONE = 3;

	private static final byte NULL = 0;
	private static final byte BUF = 1;
	private static final byte FALSE = 2;
	private static final byte TRUE = 3;
	private static final byte LONG = 4;
	private static final byte DELTA = 5;
	private static final byte LIST = 6;

	/** Deepest list nesting accepted, so that a malformed reply cannot overflow the decoder's stack. */
	static final int MAX_LIST_DEPTH = 16;

	private OldResultCodec() {}

	/** @throws IllegalArgumentException if the value of an {@code Ok} result has an unsupported type */
	static byte[] encode(ApplyResult result) {
		Objects.requireNonNull(result, "result");
		var bytes = new ByteArrayOutputStream();
		try (var out = new DataOutputStream(bytes)) {
			switch (result) {
				case ApplyResult.Ok(Object value) -> {
					out.writeByte(OK);
					writeValue(out, value, 0);
				}
				case ApplyResult.Failed(RocksDBErrorType type, String message) -> {
					out.writeByte(FAILED);
					writeString(out, type.name());
					writeBytes(out, message.getBytes(UTF_8));
				}
				case ApplyResult.ResendAlone _ -> out.writeByte(RESEND_ALONE);
			}
		} catch (IOException e) {
			// ByteArrayOutputStream never throws
			throw new UncheckedIOException(e);
		}
		return bytes.toByteArray();
	}

	/**
	 * Largest reply {@link #encodeAll} builds. A node that is not the leader receives it through gRPC, whose message
	 * limit is {@code MAX_ENTRY_BYTES} + 2 MiB (see {@link RaftSettings}): the last MiB is room for the Raft reply
	 * around it. A single result as large as the largest value a write can store still fits.
	 */
	static final int MAX_REPLY_BYTES = RaftSettings.MAX_ENTRY_BYTES + (1 << 20);

	/** Largest result that fits alone in a reply, which adds a count and the result's length. */
	static final int MAX_RESULT_BYTES = MAX_REPLY_BYTES - 2 * Integer.BYTES;

	/** {@link #encodeAll(List, int)} with {@link #MAX_REPLY_BYTES}. */
	static byte[] encodeAll(List<ApplyResult> results) {
		return encodeAll(results, MAX_REPLY_BYTES);
	}

	/**
	 * Encodes the results of a batch, in order: their count, then each one's length and {@link #encode} bytes.
	 *
	 * <p>It never fails: every replica runs it after the entry was applied, and an exception would stop the
	 * replica. A result that cannot be encoded, or that does not fit in {@code maxBytes} even alone, is replaced as
	 * {@link #bounded} does; apply already recorded it so. When the results together do not fit, the largest ones
	 * (the first of equal ones) are replaced by {@link ApplyResult#RESEND_ALONE} until they fit. Each of those fits
	 * alone, so its proposer sends it again, alone if need be, and gets its recorded result. The choice depends
	 * only on the results, so every replica builds the same reply: Ratis can answer a client's retry with the reply
	 * a replica computed before it became the leader.</p>
	 */
	static byte[] encodeAll(List<ApplyResult> results, int maxBytes) {
		int count = results.size();
		var chosen = new ApplyResult[count];
		var sizes = new long[count];
		long total = Integer.BYTES;
		for (int i = 0; i < count; i++) {
			chosen[i] = bounded(results.get(i), maxBytes - 2L * Integer.BYTES);
			sizes[i] = encodedSize(chosen[i]);
			total += Integer.BYTES + sizes[i];
		}
		// Only with more than one result: a single bounded one fits.
		if (total > maxBytes) {
			var largestFirst = new Integer[count];
			for (int i = 0; i < count; i++) {
				largestFirst[i] = i;
			}
			Arrays.sort(largestFirst, (a, b) -> sizes[a] != sizes[b] ? Long.compare(sizes[b], sizes[a])
					: Integer.compare(a, b));
			long resendSize = encodedSize(ApplyResult.RESEND_ALONE);
			for (int i : largestFirst) {
				if (total <= maxBytes || sizes[i] <= resendSize) {
					break;
				}
				chosen[i] = ApplyResult.RESEND_ALONE;
				total -= sizes[i] - resendSize;
				sizes[i] = resendSize;
			}
		}
		// Fits in an int: it is at most maxBytes, or every result is now as small as RESEND_ALONE.
		var buffer = ByteBuffer.allocate((int) total).putInt(count);
		for (var result : chosen) {
			byte[] bytes = encode(result);
			buffer.putInt(bytes.length).put(bytes);
		}
		return buffer.array();
	}

	/** {@link #bounded(ApplyResult, long)} with {@link #MAX_RESULT_BYTES}. */
	static ApplyResult bounded(ApplyResult result) {
		return bounded(result, MAX_RESULT_BYTES);
	}

	/**
	 * {@code result}, or the failure that replaces it if it cannot be encoded or its encoding is larger than
	 * {@code maxBytes}. The result is measured without being encoded, so a huge one costs no memory. A successful
	 * result that is replaced was applied, and its failure says so; a rejection keeps its error type with a short
	 * message. The replacement depends only on the result, so every replica records and returns the same one.
	 */
	static ApplyResult bounded(ApplyResult result, long maxBytes) {
		long size;
		try {
			size = encodedSize(result);
		} catch (IllegalArgumentException ex) {
			return new ApplyResult.Failed(RocksDBErrorType.INTERNAL_ERROR,
					"The result cannot be encoded; the write was applied");
		}
		if (size <= maxBytes) {
			return result;
		}
		return switch (result) {
			case ApplyResult.Failed(RocksDBErrorType type, String _) -> new ApplyResult.Failed(type,
					"The failure message (" + size + " bytes) is too large for a reply");
			default -> tooLarge(size);
		};
	}

	/** The result that replaces one of {@code size} bytes that does not fit in a reply. */
	static ApplyResult.Failed tooLarge(long size) {
		return new ApplyResult.Failed(RocksDBErrorType.INVALID_REQUEST,
				"The result (" + size + " bytes) is too large for a reply; the write was applied");
	}

	/**
	 * The length of {@link #encode}'s bytes for {@code result}, computed without encoding it.
	 *
	 * @throws IllegalArgumentException if {@link #encode} would reject it
	 */
	static long encodedSize(ApplyResult result) {
		return switch (result) {
			case ApplyResult.Ok(Object value) -> 1 + valueSize(value, 0);
			// The error type name is ASCII.
			case ApplyResult.Failed(RocksDBErrorType type, String message) ->
					1 + Integer.BYTES + type.name().length() + Integer.BYTES + message.getBytes(UTF_8).length;
			case ApplyResult.ResendAlone _ -> 1;
		};
	}

	private static long valueSize(@Nullable Object value, int depth) {
		return switch (value) {
			case null -> 1;
			case Buf buf -> 1 + Integer.BYTES + buf.size();
			case Boolean _ -> 1;
			case Long _ -> 1 + Long.BYTES;
			case Delta<?>(Object previous, Object current) ->
					1 + nullableBufSize(deltaSide(previous)) + nullableBufSize(deltaSide(current));
			case List<?> list -> {
				if (depth >= MAX_LIST_DEPTH) {
					throw new IllegalArgumentException("Result lists are nested deeper than " + MAX_LIST_DEPTH);
				}
				long size = 1 + Integer.BYTES;
				for (Object element : list) {
					size += valueSize(element, depth + 1);
				}
				yield size;
			}
			default -> throw new IllegalArgumentException("Unsupported result type: " + value.getClass().getName());
		};
	}

	private static long nullableBufSize(@Nullable Buf buf) {
		return Integer.BYTES + (buf == null ? 0 : buf.size());
	}

	/** @throws IllegalArgumentException on malformed input */
	static List<ApplyResult> decodeAll(byte[] data) {
		var in = new OldLogEntryCodec.Input(data);
		int count = in.readCount(Integer.BYTES + 1);
		var results = new ArrayList<ApplyResult>(count);
		for (int i = 0; i < count; i++) {
			results.add(decode(in.readBytes()));
		}
		in.finish();
		return results;
	}

	/** @throws IllegalArgumentException on malformed input */
	static ApplyResult decode(byte[] data) {
		var in = new OldLogEntryCodec.Input(data);
		byte tag = in.readByte();
		ApplyResult result = switch (tag) {
			case OK -> new ApplyResult.Ok(readValue(in, 0));
			case FAILED -> new ApplyResult.Failed(readErrorType(in), in.readString());
			case RESEND_ALONE -> ApplyResult.RESEND_ALONE;
			default -> throw new IllegalArgumentException("Unknown result tag: " + tag);
		};
		in.finish();
		return result;
	}

	private static void writeValue(DataOutputStream out, @Nullable Object value, int depth) throws IOException {
		switch (value) {
			case null -> out.writeByte(NULL);
			case Buf buf -> {
				out.writeByte(BUF);
				writeBuf(out, buf);
			}
			case Boolean bool -> out.writeByte(bool ? TRUE : FALSE);
			case Long number -> {
				out.writeByte(LONG);
				out.writeLong(number);
			}
			case Delta<?>(Object previous, Object current) -> {
				out.writeByte(DELTA);
				writeNullableBuf(out, deltaSide(previous));
				writeNullableBuf(out, deltaSide(current));
			}
			case List<?> list -> {
				if (depth >= MAX_LIST_DEPTH) {
					throw new IllegalArgumentException("Result lists are nested deeper than " + MAX_LIST_DEPTH);
				}
				out.writeByte(LIST);
				out.writeInt(list.size());
				for (Object element : list) {
					writeValue(out, element, depth + 1);
				}
			}
			default -> throw new IllegalArgumentException("Unsupported result type: " + value.getClass().getName());
		}
	}

	private static @Nullable Buf deltaSide(@Nullable Object value) {
		if (value == null || value instanceof Buf) {
			return (Buf) value;
		}
		throw new IllegalArgumentException("Unsupported delta value type: " + value.getClass().getName());
	}

	private static @Nullable Object readValue(OldLogEntryCodec.Input in, int depth) {
		byte type = in.readByte();
		return switch (type) {
			case NULL -> null;
			case BUF -> in.readBuf();
			case FALSE -> Boolean.FALSE;
			case TRUE -> Boolean.TRUE;
			case LONG -> in.readLong();
			case DELTA -> new Delta<>(in.readNullableBuf(), in.readNullableBuf());
			case LIST -> {
				if (depth >= MAX_LIST_DEPTH) {
					throw new IllegalArgumentException("Result lists are nested deeper than " + MAX_LIST_DEPTH);
				}
				int size = in.readCount(1);
				// Not presized: nested lists could each claim the whole input before any element is read
				var list = new ArrayList<>();
				for (int i = 0; i < size; i++) {
					list.add(readValue(in, depth + 1));
				}
				yield list;
			}
			default -> throw new IllegalArgumentException("Unknown result value type: " + type);
		};
	}

	private static RocksDBErrorType readErrorType(OldLogEntryCodec.Input in) {
		String name = in.readString();
		try {
			return RocksDBErrorType.valueOf(name);
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException("Unknown error type: " + name, e);
		}
	}
}
