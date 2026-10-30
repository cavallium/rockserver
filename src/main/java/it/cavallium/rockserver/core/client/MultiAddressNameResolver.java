package it.cavallium.rockserver.core.client;

import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;
import io.grpc.NameResolverRegistry;
import io.grpc.Status;
import io.grpc.StatusOr;
import io.grpc.SynchronizationContext;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves a fixed list of Rockserver nodes, in the order given, for a channel with several addresses.
 *
 * <p>The target is {@code rockserver-hosts:///host1:port1,host2:port2}. Each host gives one address group, with
 * every address its name resolves to; with the {@code pick_first} policy the channel connects to the first address
 * that accepts a connection and keeps it until that connection is lost, then starts again from the first. A host
 * that does not resolve is left out of that resolution; when no host resolves, the channel sees UNAVAILABLE. Names
 * are resolved again when the load balancer asks for it (after the connection is lost or every address failed).</p>
 */
final class MultiAddressNameResolver extends NameResolver {

	static final String SCHEME = "rockserver-hosts";
	private static final Logger LOG = LoggerFactory.getLogger(MultiAddressNameResolver.class);

	static {
		// The default registry is the one channels built for a target consult.
		NameResolverRegistry.getDefaultRegistry().register(new Provider());
	}

	private final List<HostAndPort> addresses;
	private final String authority;
	private final SynchronizationContext syncContext;
	private final @Nullable Executor offloadExecutor;
	/** Guarded by syncContext. */
	private Listener2 listener;
	/** Guarded by syncContext. */
	private boolean resolving;
	/** Guarded by syncContext. */
	private boolean shutdown;

	private MultiAddressNameResolver(List<HostAndPort> addresses, Args args) {
		this.addresses = addresses;
		this.authority = Utils.formatHostAndPorts(List.of(addresses.getFirst()));
		this.syncContext = args.getSynchronizationContext();
		this.offloadExecutor = args.getOffloadExecutor();
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
	}

	private void resolve() {
		if (resolving || shutdown) {
			return;
		}
		resolving = true;
		Runnable lookup = () -> {
			var groups = new ArrayList<EquivalentAddressGroup>(addresses.size());
			var failures = new ArrayList<String>();
			for (var address : addresses) {
				try {
					var resolved = Arrays.stream(InetAddress.getAllByName(address.host()))
							.<SocketAddress>map(inet -> new InetSocketAddress(inet, address.port()))
							.toList();
					groups.add(new EquivalentAddressGroup(resolved));
				} catch (UnknownHostException | RuntimeException e) {
					failures.add(address.host() + ": " + e);
				}
			}
			syncContext.execute(() -> {
				resolving = false;
				if (shutdown) {
					return;
				}
				if (!failures.isEmpty()) {
					LOG.warn("Rockserver hosts not resolved, left out until the next resolution: {}", failures);
				}
				if (groups.isEmpty()) {
					listener.onError(Status.UNAVAILABLE.withDescription("No Rockserver host resolved: " + failures));
				} else {
					listener.onResult2(ResolutionResult.newBuilder()
							.setAddressesOrError(StatusOr.fromValue(List.copyOf(groups)))
							.build());
				}
			});
		};
		if (offloadExecutor != null) {
			offloadExecutor.execute(lookup);
		} else {
			lookup.run();
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
