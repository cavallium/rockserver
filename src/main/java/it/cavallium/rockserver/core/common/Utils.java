package it.cavallium.rockserver.core.common;

import static java.util.Objects.requireNonNullElse;

import com.google.protobuf.ByteString;
import java.io.IOException;
import it.cavallium.buffer.Buf;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.VisibleForTesting;

public class Utils {

	private static final Buf DUMMY_EMPTY_VALUE = Buf.wrap(new byte[] {-1}).freeze();
	public static final int DEFAULT_PORT = 5333;
	private static final Buf EMPTY_BUF = Buf.create(0).freeze();

	public static Buf dummyRocksDBEmptyValue() {
		return DUMMY_EMPTY_VALUE;
	}

	/**
	 * Returns the value of the {@code int} argument, throwing an exception if the value overflows an {@code char}.
	 *
	 * @param value the int value
	 * @return the argument as a char
	 * @throws ArithmeticException if the {@code argument} overflows a char
	 * @since 1.8
	 */
	public static char toCharExact(int value) {
		if (value < Character.MIN_VALUE || value > Character.MAX_VALUE) {
			throw new ArithmeticException("char overflow");
		} else {
			return (char) value;
		}
	}

	/**
	 * Returns the value of the {@code long} argument, throwing an exception if the value overflows an {@code char}.
	 *
	 * @param value the long value
	 * @return the argument as a char
	 * @throws ArithmeticException if the {@code argument} overflows a char
	 * @since 1.8
	 */
	public static char toCharExact(long value) {
		if (value < Character.MIN_VALUE || value > Character.MAX_VALUE) {
			throw new ArithmeticException("char overflow");
		} else {
			return (char) value;
		}
	}

	@Contract(value = "!null -> !null; null -> null", pure = true)
	public static Buf toBuf(byte... array) {
		if (array != null) {
			return Buf.wrap(array);
		} else {
			return null;
		}
	}

	@VisibleForTesting
	@Contract(value = "null -> null", pure = true)
	public static Buf toBufSimple(int... array) {
		if (array != null) {
			var newArray = new byte[array.length];
			for (int i = 0; i < array.length; i++) {
				newArray[i] = (byte) array[i];
			}
			return Buf.wrap(newArray);
		} else {
			return null;
		}
	}

	public static byte[] toByteArray(Buf buf) {
		return buf != null ? buf.toByteArray() : null;
	}

	public static <T, U> List<U> mapList(Collection<T> input, Function<T, U> mapper) {
		var result = new ArrayList<U>(input.size());
		input.forEach(t -> result.add(mapper.apply(t)));
		return result;
	}

	public static void deleteDirectory(String path) throws RocksDBException {
		try (Stream<Path> pathStream = Files.walk(Path.of(path))) {
			pathStream.sorted(Comparator.reverseOrder()).forEach(f -> {
				try {
					Files.deleteIfExists(f);
				} catch (IOException e) {
					throw RocksDBException.of(RocksDBException.RocksDBErrorType.DIRECTORY_DELETE, e);
				}
			});
		} catch (IOException e) {
			throw RocksDBException.of(RocksDBException.RocksDBErrorType.DIRECTORY_DELETE, e);
		}
	}

	public static boolean valueEquals(Buf previousValue, Buf currentValue) {
		previousValue = requireNonNullElse(previousValue, emptyBuf());
		currentValue = requireNonNullElse(currentValue, emptyBuf());
		return previousValue.equals(currentValue);
	}

	public static int valueHash(Buf value) {
		value = requireNonNullElse(value, emptyBuf());
		return value.hashCode();
	}

	public static HostAndPort parseHostAndPort(URI uri) {
		return new HostAndPort(uri.getHost(), parsePort(uri));
	}

	public static int parsePort(URI uri) {
		var port = uri.getPort();
		if (port == -1) {
			return DEFAULT_PORT;
		} else {
			return port;
		}
	}

	public static String toPrettyString(Buf s) {
		return HexFormat.of().formatHex(s.getBackingByteArray(), s.getBackingByteArrayFrom(), s.getBackingByteArrayTo());
	}

	public static Buf emptyBuf() {
		return EMPTY_BUF;
	}

	public static Buf fromHeapByteBuffer(ByteBuffer resultBuffer) {
		if (resultBuffer == null) return null;
		var arrayOffset = resultBuffer.arrayOffset();
		return Buf.wrap(resultBuffer.array(), arrayOffset, arrayOffset + resultBuffer.limit());
	}

	public static Buf fromByteBuffer(ByteBuffer resultBuffer) {
		if (resultBuffer == null) return null;
		if (resultBuffer.hasArray()) {
			return fromHeapByteBuffer(resultBuffer);
		} else {
			var out = new byte[resultBuffer.remaining()];
			resultBuffer.get(out);
			return Buf.wrap(out);
		}
	}

	public static Buf toBuf(ByteString data) {
		return data != null ? Buf.wrap(data.toByteArray()) : null;
	}

	public static ByteString toByteString(Buf buf) {
		return ByteString.copyFrom(buf.getBackingByteArray(),
				buf.getBackingByteArrayOffset(),
				buf.getBackingByteArrayLength()
		);
	}

	public static ByteBuffer asByteBuffer(Buf buf) {
		return ByteBuffer.wrap(buf.getBackingByteArray(),
				buf.getBackingByteArrayOffset(),
				buf.getBackingByteArrayLength());
	}

	public record HostAndPort(String host, int port) {}
}
