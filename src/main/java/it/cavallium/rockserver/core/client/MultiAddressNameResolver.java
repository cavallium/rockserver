package it.cavallium.rockserver.core.client;

import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;
import io.grpc.NameResolverRegistry;
import io.grpc.Status;
import io.grpc.StatusOr;
import io.grpc.SynchronizationContext;
import io.grpc.SynchronizationContext.ScheduledHandle;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves a fixed list of Rockserver nodes, in the order given, for a channel with several addresses.
 *
 * <p>The target is {@code rockserver-hosts:///host1:port1,host2:port2}. Each host gives one address group, with
 * every address its name resolves to; with the {@code pick_first} policy the channel connects to the first address
 * that accepts a connection and keeps it until that connection is lost, then starts again from the first. Names are
 * resolved again when the load balancer asks for it (after the connection is lost or every address failed).</p>
 *
 * <p>A failed lookup must not take down what works, and the channel asks for a resolution exactly when it is moving
 * to another node, so: a host whose lookup fails keeps its last good addresses, one that never resolved is left out,
 * and the channel sees an error only while no host ever resolved. Hosts are looked up concurrently, and a lookup that
 * takes longer than {@link #LOOKUP_WAIT_MILLIS} does not hold back the others: what resolved so far is published, and
 * the slow host follows with an update if it ever answers. A resolver runs one lookup per host at a time, so a hung
 * name holds one thread, not one for each resolution.</p>
 */
final class MultiAddressNameResolver extends NameResolver {

	static final String SCHEME = "rockserver-hosts";
	/**
	 * The last good addresses of each host, set by the channel's builder: a channel creates a new resolver whenever it
	 * leaves idle mode, and the addresses must outlive it. Only used in the channel's synchronization context.
	 */
	static final Args.Key<Map<HostAndPort, List<SocketAddress>>> LAST_GOOD_ADDRESSES
			= Args.Key.create("rockserver-last-good-addresses");
	/** Short enough for the capability handshake to still reach nodes after a resolution that waited this long. */
	private static final long LOOKUP_WAIT_MILLIS = 1_000;
	private static final Logger LOG = LoggerFactory.getLogger(MultiAddressNameResolver.class);

	static {
		// The default registry is the one channels built for a target consult.
		NameResolverRegistry.getDefaultRegistry().register(new Provider());
	}

	private final List<HostAndPort> addresses;
	private final String authority;
	private final SynchronizationContext syncContext;
	private final ScheduledExecutorService scheduledExecutor;
	private final @Nullable Executor offloadExecutor;
	/** Guarded by syncContext. */
	private final Map<HostAndPort, List<SocketAddress>> lastGood;
	/** The hosts with a lookup running. Guarded by syncContext. */
	private final Set<HostAndPort> lookups = new HashSet<>();
	/** The hosts whose latest lookup failed, with why. Guarded by syncContext. */
	private final Map<HostAndPort, String> failures = new HashMap<>();
	/** Guarded by syncContext. */
	private Listener2 listener;
	/** The wait for the lookups of the current resolution, null once it ended. Guarded by syncContext. */
	private @Nullable ScheduledHandle lookupWait;
	/** The last result given to the listener. Guarded by syncContext. */
	private @Nullable List<EquivalentAddressGroup> published;
	/** Guarded by syncContext. */
	private boolean shutdown;

	private MultiAddressNameResolver(List<HostAndPort> addresses, Args args) {
		this.addresses = addresses;
		this.authority = Utils.formatHostAndPorts(List.of(addresses.getFirst()));
		this.syncContext = args.getSynchronizationContext();
		this.scheduledExecutor = args.getScheduledExecutorService();
		this.offloadExecutor = args.getOffloadExecutor();
		this.lastGood = Objects.requireNonNullElseGet(args.getArg(LAST_GOOD_ADDRESSES), HashMap::new);
	}

	/** The channel target naming these addresses, in order. */
	static String target(List<HostAndPort> addresses) {
		if (addresses.isEmpty()) {
			throw new IllegalArgumentException("No address");
		}
		try {
			// The multi-argument constructor quotes what a path cannot hold, such as IPv6 brackets.
			return new URI(SCHEME, "", "/" + Utils.formatHostAndPorts(addresses), null).toString();
		} catch (URISyntaxException e) {
			throw new IllegalArgumentException("Invalid addresses: " + addresses, e);
		}
	}

	@Override
	public String getServiceAuthority() {
		return authority;
	}

	@Override
	public void start(Listener2 listener) {
		this.listener = listener;
		resolve();
	}

	@Override
	public void refresh() {
		resolve();
	}

	@Override
	public void shutdown() {
		shutdown = true;
		if (lookupWait != null) {
			lookupWait.cancel();
			lookupWait = null;
		}
	}

	private void resolve() {
		if (lookupWait != null || shutdown) {
			return;
		}
		for (var address : addresses) {
			if (lookups.add(address)) {
				startLookup(address);
			}
		}
		lookupWait = syncContext.schedule(this::endLookupWait, LOOKUP_WAIT_MILLIS, TimeUnit.MILLISECONDS,
				scheduledExecutor);
	}

	private void startLookup(HostAndPort address) {
		Runnable lookup = () -> {
			List<SocketAddress> resolved = null;
			String failure = null;
			try {
				resolved = Arrays.stream(InetAddress.getAllByName(address.host()))
						.<SocketAddress>map(inet -> new InetSocketAddress(inet, address.port()))
						.toList();
			} catch (UnknownHostException | RuntimeException e) {
				failure = e.toString();
			}
			var result = resolved;
			var error = failure;
			syncContext.execute(() -> lookupEnded(address, result, error));
		};
		if (offloadExecutor != null) {
			offloadExecutor.execute(lookup);
		} else {
			Thread.ofPlatform().daemon().name("rockserver-hosts-lookup").start(lookup);
		}
	}

	private void lookupEnded(HostAndPort address, @Nullable List<SocketAddress> resolved, @Nullable String failure) {
		lookups.remove(address);
		if (shutdown) {
			return;
		}
		if (resolved != null) {
			lastGood.put(address, resolved);
			failures.remove(address);
		} else {
			failures.put(address, failure);
			LOG.warn("Rockserver host {} not resolved, {}: {}", address.host(),
					lastGood.containsKey(address) ? "using its last addresses" : "left out", failure);
		}
		if (lookupWait != null && lookups.isEmpty()) {
			lookupWait.cancel();
			lookupWait = null;
		}
		if (lookupWait == null) {
			publish();
		}
	}

	private void endLookupWait() {
		lookupWait = null;
		publish();
	}

	private void publish() {
		var groups = addresses.stream()
				.map(lastGood::get)
				.filter(Objects::nonNull)
				.map(EquivalentAddressGroup::new)
				.toList();
		if (groups.isEmpty()) {
			listener.onError(Status.UNAVAILABLE.withDescription("No Rockserver host resolved: " + failures));
		} else if (!groups.equals(published)) {
			published = groups;
			listener.onResult2(ResolutionResult.newBuilder()
					.setAddressesOrError(StatusOr.fromValue(groups))
					.build());
		}
	}

	private static final class Provider extends NameResolverProvider {

		@Override
		public @Nullable NameResolver newNameResolver(URI targetUri, Args args) {
			if (!SCHEME.equals(targetUri.getScheme())) {
				return null;
			}
			var path = targetUri.getPath();
			if (path == null || path.length() <= 1) {
				throw new IllegalArgumentException("No address in " + targetUri);
			}
			return new MultiAddressNameResolver(Utils.parseHostAndPorts(path.substring(1)), args);
		}

		@Override
		public String getDefaultScheme() {
			return SCHEME;
		}

		@Override
		protected boolean isAvailable() {
			return true;
		}

		@Override
		protected int priority() {
			// Only ever chosen by its scheme: never the default for targets without one.
			return 0;
		}
	}
}
