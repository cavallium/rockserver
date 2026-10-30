package it.cavallium.rockserver.core.impl.test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.spi.InetAddressResolver;
import java.net.spi.InetAddressResolverProvider;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

/**
 * A name service for tests: the names registered here resolve as the test says, all others as usual. The JVM's cache
 * of successful lookups is off for tests ({@code sun.net.inetaddr.ttl} in the surefire configuration). Failed lookups
 * stay cached for 10 s, so that a name that failed cannot resolve again within a test.
 */
public final class FakeHosts extends InetAddressResolverProvider {

	private static final Map<String, Answer> NAMES = new ConcurrentHashMap<>();

	private interface Answer {

		InetAddress[] lookup() throws IOException, InterruptedException;
	}

	/** {@code name} resolves to the loopback address. */
	static void serve(String name) {
		NAMES.put(name, FakeHosts::loopback);
	}

	/** Looking up {@code name} fails, after {@code delay}. */
	static void fail(String name, Duration delay) {
		NAMES.put(name, () -> {
			Thread.sleep(delay);
			throw new UnknownHostException(name);
		});
	}

	/** Looking up {@code name} waits for the latch returned, then resolves to the loopback address. */
	static CountDownLatch hang(String name) {
		var release = new CountDownLatch(1);
		NAMES.put(name, () -> {
			release.await();
			return loopback();
		});
		return release;
	}

	private static InetAddress[] loopback() throws UnknownHostException {
		return new InetAddress[] {InetAddress.getByAddress(new byte[] {127, 0, 0, 1})};
	}

	@Override
	public InetAddressResolver get(Configuration configuration) {
		var builtin = configuration.builtinResolver();
		return new InetAddressResolver() {
			@Override
			public Stream<InetAddress> lookupByName(String host, LookupPolicy lookupPolicy) throws UnknownHostException {
				var answer = NAMES.get(host);
				if (answer == null) {
					return builtin.lookupByName(host, lookupPolicy);
				}
				try {
					return Stream.of(answer.lookup());
				} catch (UnknownHostException e) {
					throw e;
				} catch (IOException | InterruptedException e) {
					throw new UnknownHostException(host + ": " + e);
				}
			}

			@Override
			public String lookupByAddress(byte[] address) throws UnknownHostException {
				return builtin.lookupByAddress(address);
			}
		};
	}

	@Override
	public String name() {
		return "fake-hosts";
	}
}
