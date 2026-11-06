package it.cavallium.rockserver.core.common;

import static java.util.Objects.requireNonNullElse;

import com.google.protobuf.ByteString;
import java.io.IOException;
import it.cavallium.buffer.Buf;
import java.net.URI;
import java.net.URISyntaxException;
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

	/**
	 * The hosts of a connection URL such as {@code http://host1:5333,host2:5333}: one or more, in order. A URL
	 * with a single host gives the same address as {@link #parseHostAndPort(URI)}. Parse the URL with
	 * {@link #parseConnectionUrl(String)}, which also accepts IPv6 literals in a list.
	 */
	public static List<HostAndPort> parseHostAndPorts(URI uri) {
		if (uri.getHost() != null) {
			return List.of(parseHostAndPort(uri));
		}
		var authority = uri.getAuthority();
		if (authority == null || authority.isBlank()) {
			throw new IllegalArgumentException("The URL has no host: " + uri);
		}
		return parseHostAndPorts(authority);
	}

	/**
	 * Parses a comma-separated list of {@code host[:port]}, where an IPv6 literal is written in brackets
	 * ({@code [::1]:5333}). A missing port is {@link #DEFAULT_PORT}. The order is kept: it is the order a client
	 * tries the hosts in.
	 */
	public static List<HostAndPort> parseHostAndPorts(String hosts) {
		var result = new ArrayList<HostAndPort>();
		for (var rawEntry : hosts.split(",", -1)) {
			var entry = rawEntry.strip();
			if (entry.isEmpty()) {
				throw new IllegalArgumentException("Empty host in the host list \"" + hosts + "\"");
			}
			String host;
			String port;
			if (entry.startsWith("[")) {
				int close = entry.indexOf(']');
				if (close < 0) {
					throw new IllegalArgumentException("Unclosed IPv6 bracket in \"" + entry + "\"");
				}
				host = entry.substring(1, close);
				var rest = entry.substring(close + 1);
				if (rest.isEmpty()) {
					port = null;
				} else if (rest.startsWith(":")) {
					port = rest.substring(1);
				} else {
					throw new IllegalArgumentException("Invalid host \"" + entry + "\"");
				}
			} else {
				int colon = entry.indexOf(':');
				if (colon >= 0 && entry.indexOf(':', colon + 1) >= 0) {
					throw new IllegalArgumentException("Write the IPv6 address in brackets: \"" + entry + "\"");
				}
				host = colon >= 0 ? entry.substring(0, colon) : entry;
				port = colon >= 0 ? entry.substring(colon + 1) : null;
			}
			if (host.isEmpty()) {
				throw new IllegalArgumentException("Empty host name in \"" + entry + "\"");
			}
			result.add(new HostAndPort(host, port == null ? DEFAULT_PORT : parsePort(port, entry)));
		}
		return List.copyOf(result);
	}

	private static int parsePort(String port, String entry) {
		final int parsed;
		try {
			parsed = Integer.parseInt(port);
		} catch (NumberFormatException ex) {
			throw new IllegalArgumentException("Invalid port in \"" + entry + "\"", ex);
		}
		if (parsed < 1 || parsed > 65535) {
			throw new IllegalArgumentException("Port out of range in \"" + entry + "\"");
		}
		return parsed;
	}

	/**
	 * Parses a connection URL like {@link URI#URI(String)}, and also accepts a list of hosts whose IPv6 literals
	 * are in brackets ({@code http://[::1]:5333,[::2]:5333}), which {@code URI} refuses: their brackets are
	 * percent-encoded, and {@link #parseHostAndPorts(URI)} reads them back.
	 */
	public static URI parseConnectionUrl(String url) throws URISyntaxException {
		int authorityStart = url.indexOf("://");
		if (authorityStart > 0) {
			authorityStart += 3;
			int authorityEnd = authorityStart;
			while (authorityEnd < url.length() && "/?#".indexOf(url.charAt(authorityEnd)) < 0) {
				authorityEnd++;
			}
			var authority = url.substring(authorityStart, authorityEnd);
			if (authority.indexOf(',') >= 0) {
				url = url.substring(0, authorityStart)
						+ authority.replace("[", "%5B").replace("]", "%5D")
						+ url.substring(authorityEnd);
			}
		}
		return new URI(url);
	}

	/** The inverse of {@link #parseHostAndPorts(String)}. */
	public static String formatHostAndPorts(List<HostAndPort> addresses) {
		var result = new StringBuilder();
		for (var address : addresses) {
			if (!result.isEmpty()) {
				result.append(',');
			}
			var host = address.host();
			if (host.indexOf(':') >= 0 && !host.startsWith("[")) {
				result.append('[').append(host).append(']');
			} else {
				result.append(host);
			}
			result.append(':').append(address.port());
		}
		return result.toString();
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
		return Buf.wrap(resultBuffer.array(), arrayOffset + resultBuffer.position(), arrayOffset + resultBuffer.limit());
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
