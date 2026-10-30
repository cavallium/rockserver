package it.cavallium.rockserver.distributed;

import static it.cavallium.rockserver.distributed.LogEntryCodec.writeBuf;
import static it.cavallium.rockserver.distributed.LogEntryCodec.writeBytes;
import static it.cavallium.rockserver.distributed.LogEntryCodec.writeNullableBuf;
import static it.cavallium.rockserver.distributed.LogEntryCodec.writeString;
import static java.nio.charset.StandardCharsets.UTF_8;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.Delta;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.jetbrains.annotations.Nullable;

/**
 * Binary encoding of {@link ApplyResult}, carried in Raft replies from the leader to the proposing node.
 *
 * <p>Layout: a tag byte for {@code Ok} or {@code Failed}. {@code Ok} is followed by its value, written as a type
 * byte and then the value's fields; {@code Failed} by the error type name and the message. Primitive encodings
 * are those of {@link LogEntryCodec}.</p>
 *
 * <p>A failure message is written leniently: it comes from an arbitrary exception during apply, so characters
 * that are not valid Unicode, such as unpaired surrogates, become {@code '?'} instead of failing the reply.</p>
 */
final class ResultCodec {

	private static final byte OK = 1;
	private static final byte FAILED = 2;

	private static final byte NULL = 0;
	private static final byte BUF = 1;
	private static final byte FALSE = 2;
	private static final byte TRUE = 3;
	private static final byte LONG = 4;
	private static final byte DELTA = 5;
	private static final byte LIST = 6;

	/** Deepest list nesting accepted, so that a malformed reply cannot overflow the decoder's stack. */
	static final int MAX_LIST_DEPTH = 16;

	private ResultCodec() {}

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
			}
		} catch (IOException e) {
			// ByteArrayOutputStream never throws
			throw new UncheckedIOException(e);
		}
		return bytes.toByteArray();
	}

	/** Encodes the results of a batch, in order: their count, then each one's length and {@link #encode} bytes. */
	static byte[] encodeAll(List<ApplyResult> results) {
		var encoded = new ArrayList<byte[]>(results.size());
		int size = Integer.BYTES;
		for (var result : results) {
			byte[] bytes = encode(result);
			encoded.add(bytes);
			size = Math.addExact(size, Integer.BYTES + bytes.length);
		}
		var buffer = java.nio.ByteBuffer.allocate(size).putInt(encoded.size());
		for (byte[] bytes : encoded) {
			buffer.putInt(bytes.length).put(bytes);
		}
		return buffer.array();
	}

	/** @throws IllegalArgumentException on malformed input */
	static List<ApplyResult> decodeAll(byte[] data) {
		var in = new LogEntryCodec.Input(data);
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
		var in = new LogEntryCodec.Input(data);
		byte tag = in.readByte();
		ApplyResult result = switch (tag) {
			case OK -> new ApplyResult.Ok(readValue(in, 0));
			case FAILED -> new ApplyResult.Failed(readErrorType(in), in.readString());
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

	private static @Nullable Object readValue(LogEntryCodec.Input in, int depth) {
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

	private static RocksDBErrorType readErrorType(LogEntryCodec.Input in) {
		String name = in.readString();
		try {
			return RocksDBErrorType.valueOf(name);
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException("Unknown error type: " + name, e);
		}
	}
}
