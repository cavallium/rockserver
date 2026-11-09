package it.cavallium.rockserver.core.client;

import io.grpc.Attributes;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import it.cavallium.rockserver.core.common.LeaderHint;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.LongSupplier;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For a connection to several nodes of a replicated cluster: sends the calls that the leader serves straight to it,
 * on a second channel, instead of through the node the ordinary channel uses, which would forward them (etcd clientv3
 * and the TiKV client's region cache do the same). Nodes name their leader in every response ({@link LeaderHint});
 * the first response that names one opens a channel to it. Point reads ({@code get}, {@code exists},
 * {@code existsMulti}, {@code getMulti}), maintenance and the handshake stay on the ordinary channel: followers serve
 * point reads themselves, and maintenance concerns the node it reaches.
 *
 * <p>Calls go to the leader's channel only while it is connected; until then, and once it fails, they take the
 * ordinary channel, which every node serves. A call that ends with {@code UNAVAILABLE} on the leader's channel drops
 * it, and the same address is not taken again for {@link #FAILED_HINT_BACKOFF_NANOS}. Each failed address is
 * remembered on its own, so two addresses that keep failing (two nodes that each name the other) do not alternate
 * without a pause. A call in flight when its
 * channel fails fails as on any lost connection: whether a call may run again is the caller's decision.</p>
 *
 * <p>By default ({@link Mode#LISTED}) a hinted address is used only if it is one of the addresses the client was given
 * (same host name and port): a server cannot send the client to an address its user did not list, and an address the
 * members use among themselves (an internal name, a NAT address) may not be reachable from the client. The system
 * property {@value #MODE_PROPERTY} set to {@code any} accepts any address, and {@code off} turns hints off.</p>
 */
final class LeaderHintRouter implements AutoCloseable {

	private static final Logger LOG = LoggerFactory.getLogger(LeaderHintRouter.class);
	static final String MODE_PROPERTY = "it.cavallium.rockserver.grpc.client.leader-hint";
	/** How long an address whose channel failed is not taken again. */
	static final long FAILED_HINT_BACKOFF_NANOS = TimeUnit.SECONDS.toNanos(2);
	static final Metadata.Key<String> KEY = Metadata.Key.of(LeaderHint.METADATA_KEY, Metadata.ASCII_STRING_MARSHALLER);

	/** Which hinted addresses are used. */
	enum Mode {
		/** None. */
		OFF,
		/** Only addresses in the client's list. */
		LISTED,
		/** Any. */
		ANY
	}

	/**
	 * gRPC methods, by bare name, that the leader serves: they go to the leader's channel when there is one. With
	 * {@link #ORDINARY_METHODS}, every method of the service, which a test checks.
	 */
	static final Set<String> LEADER_METHODS = Set.of(
			// The session is registered through the log: a follower would forward it.
			"openClientSession",
			"openTransaction", "closeTransaction", "closeFailedUpdate", "getForUpdate",
			"put", "putEnsure", "putBatch", "putMulti", "putMultiEnsure", "putMultiList", "putMultiListEnsure",
			"putGetPrevious", "putMultiGetPrevious", "putGetDelta", "putMultiGetDelta", "putGetChanged",
			"putMultiGetChanged", "putGetPreviousPresence", "putMultiGetPreviousPresence",
			"delete", "deleteMulti", "deleteRange", "deleteGetPrevious", "deleteMultiGetPrevious",
			"deleteGetPreviousPresence", "deleteMultiGetPreviousPresence",
			"merge", "mergeBatch", "mergeMulti", "mergeGetMerged", "mergeMultiGetMerged",
			"createColumn", "deleteColumn", "deleteColumnIfExists", "uploadMergeOperator",
			"openIterator", "closeIterator", "seekTo", "subsequent", "subsequentExists", "subsequentMultiGet",
			"reduceRangeFirstAndLast", "reduceRangeEntriesCount", "getAllInRange", "getAllInRangeNoCache", "getRangePage",
			"scanRaw", "getColumnId", "getAllColumnDefinitions", "checkMergeOperator", "estimateNumKeys");

	/**
	 * gRPC methods, by bare name, that stay on the ordinary channel: the handshake (the capabilities of the node the
	 * connection reaches), point reads (followers serve them), and maintenance and node statistics, which concern the
	 * node they reach.
	 */
	static final Set<String> ORDINARY_METHODS = Set.of(
			"getCapabilities", "get", "exists", "existsMulti", "getMulti",
			"flush", "compact", "getSstMetadata", "compactFiles", "getTableProperties");

	/**
	 * The leader's channel. Once it is no longer the current one ({@link #retire}), it is shut down when no call that
	 * {@link #route} handed out on it waits for its start (R46-1): a call started on a channel already shut down fails
	 * {@code UNAVAILABLE} without being sent, and a write that fails so ends with an unknown outcome. The count is
	 * atomic, without a lock: it is on the path of every leader-bound call.
	 */
	static final class Hint {
		private final String address;
		private final ManagedChannel channel;
		/**
		 * Calls handed out on {@link #channel} and not started (nor cancelled) yet. A call that is never started nor
		 * cancelled, which no gRPC stub does, would keep a retired channel open (idle) until the process ends.
		 */
		private final AtomicInteger unstarted = new AtomicInteger();
		/** No longer the current hint: shut down once {@link #unstarted} is 0. */
		private volatile boolean retired;

		Hint(String address, ManagedChannel channel) {
			this.address = address;
			this.channel = channel;
		}

		String address() {
			return address;
		}

		ManagedChannel channel() {
			return channel;
		}

		/** Counts a call handed out on the channel; false, counting nothing, if the hint was retired. */
		boolean acquire() {
			unstarted.incrementAndGet();
			if (retired) {
				release();
				return false;
			}
			return true;
		}

		/** A call counted by {@link #acquire} started, or was cancelled before its start. */
		void release() {
			// Either this sees the retirement, or retire() sees the count at 0: the channel is shut down at least once.
			if (unstarted.decrementAndGet() == 0 && retired) {
				channel.shutdown();
			}
		}

		boolean retired() {
			return retired;
		}

		/** No longer the leader's channel: shuts it down now, or once the calls handed out on it started. */
		void retire() {
			retired = true;
			if (unstarted.get() == 0) {
				channel.shutdown();
			}
		}
	}

	/**
	 * Tests only: run by a replay once the replayed call is the current one, before it is started (R43-3).
	 */
	static volatile @Nullable Runnable replaySwappedForTesting;

	private final Mode mode;
	/** Methods, by bare name, that run again on the ordinary channel when the leader's channel fails before answering. */
	private final Set<String> replayable;
	private final Set<String> listed = new HashSet<>();
	/** Opens a channel to the leader; null until {@link #start}, and hints are ignored until then. */
	private volatile @Nullable Function<HostAndPort, ManagedChannel> channels;
	private final LongSupplier nanoTime;
	private final Object lock = new Object();
	private volatile @Nullable Hint current;
	/** Addresses whose channel failed, with when: not taken again for {@link #FAILED_HINT_BACKOFF_NANOS}. Guarded by {@link #lock}. */
	private final Map<String, Long> failedAt = new HashMap<>();
	/** Guarded by {@link #lock}. */
	private boolean closed;

	/**
	 * @param listed the addresses the client was given
	 */
	LeaderHintRouter(Mode mode, List<HostAndPort> listed) {
		this(mode, listed, Set.of(), System::nanoTime);
	}

	/**
	 * @param replayable methods, by bare name, that the ordinary channel's retry policy would send again: they run
	 *                   again on the ordinary channel when the leader's channel fails before answering
	 */
	LeaderHintRouter(Mode mode, List<HostAndPort> listed, Set<String> replayable) {
		this(mode, listed, replayable, System::nanoTime);
	}

	LeaderHintRouter(Mode mode, List<HostAndPort> listed, Set<String> replayable, LongSupplier nanoTime) {
		this.mode = mode;
		this.replayable = Set.copyOf(replayable);
		listed.forEach(address -> this.listed.add(normalize(address.host(), address.port())));
		this.nanoTime = nanoTime;
	}

	/**
	 * Takes hints from now on.
	 *
	 * @param channels opens a channel to the leader, configured as the ordinary one
	 */
	void start(Function<HostAndPort, ManagedChannel> channels) {
		this.channels = channels;
	}

	/** The mode set by {@value #MODE_PROPERTY}: {@code listed} (the default), {@code any} or {@code off}. */
	static Mode configuredMode() {
		var value = System.getProperty(MODE_PROPERTY, "listed").trim().toUpperCase(Locale.ROOT);
		try {
			return Mode.valueOf(value);
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException("Invalid " + MODE_PROPERTY + ": " + value + ", expected listed, any or off");
		}
	}

	/** Reads the leader's address from the responses of the channels it is added to. */
	ClientInterceptor hintReader() {
		return new ClientInterceptor() {
			@Override
			public <Q, R> ClientCall<Q, R> interceptCall(MethodDescriptor<Q, R> method, CallOptions options, Channel next) {
				return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, options)) {
					@Override
					public void start(Listener<R> listener, Metadata headers) {
						super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<>(listener) {
							@Override
							public void onHeaders(Metadata responseHeaders) {
								offer(responseHeaders.get(KEY));
								super.onHeaders(responseHeaders);
							}

							@Override
							public void onClose(Status status, Metadata trailers) {
								offer(trailers.get(KEY));
								super.onClose(status, trailers);
							}
						}, headers);
					}
				};
			}
		};
	}

	/** A channel that sends the calls the leader serves to the leader's channel, while it is connected, else to {@code main}. */
	Channel route(Channel main) {
		return new Channel() {
			@Override
			public <Q, R> ClientCall<Q, R> newCall(MethodDescriptor<Q, R> method, CallOptions options) {
				var hint = leaderChannelFor(method);
				// Counted until the caller starts the call, so that the channel is not shut down under it (R46-1).
				if (hint == null || !hint.acquire()) {
					return main.newCall(method, options);
				}
				try {
					return new LeaderCall<>(hint, main, method, options, replayable.contains(method.getBareMethodName()));
				} catch (RuntimeException | Error e) {
					hint.release();
					throw e;
				}
			}

			@Override
			public String authority() {
				return main.authority();
			}
		};
	}

	/**
	 * A call on the leader's channel. It drops the hint when the call ends with {@code UNAVAILABLE}, or when the node
	 * answers without naming itself as the leader (it was deposed, or is cut off and knows no leader). A
	 * {@code replayable} call (a read that the ordinary channel's retry policy would send again) that fails with
	 * {@code UNAVAILABLE} before any answer runs again on the ordinary channel, which fails over to a live node; the
	 * leader's channel itself does not retry, so this happens at once. Other calls fail as on any lost connection.
	 *
	 * <p>Methods of a {@link ClientCall} come from one caller at a time, listener calls from one thread at a time; the
	 * two may overlap (the listener runs on the channel's executor), hence the lock. Each caller method forwards to the
	 * current call while it holds the lock, and a replay starts the new call and forwards what the caller did so far
	 * while it holds it too, so that the caller's later operations reach the new call after those, never before its
	 * start (R43-3). The lock is reentrant: a listener called back on the caller's own thread inside a forwarded method
	 * (a channel that fails the call at once) replays there. Nothing that holds the lock waits for another thread: the
	 * methods of a {@link ClientCall} do not block, and {@link #drop} runs outside it.</p>
	 *
	 * <p>The call holds its hint ({@link Hint#acquire}) until it is started or cancelled. If the hint was retired in
	 * between (a response named a new leader, or a call on the channel failed), it starts on the ordinary channel
	 * instead: nothing was sent yet, and the retired channel may be a dead node's (R46-1).</p>
	 */
	private final class LeaderCall<Q, R> extends ClientCall<Q, R> {

		private final Hint hint;
		private final Channel main;
		private final MethodDescriptor<Q, R> method;
		private final CallOptions options;
		private final boolean replayable;
		private final Object callLock = new Object();
		/** Guarded by callLock. */
		private ClientCall<Q, R> current;
		/** What the caller did so far, to replay it; guarded by callLock. Only kept for a replayable call. */
		private final List<Q> sent = new ArrayList<>();
		private int requested;
		private boolean halfClosed;
		private boolean cancelled;
		private boolean replayed;
		/** An answer came: the call is no longer replayable. Guarded by callLock. */
		private boolean answered;
		private @Nullable Listener<R> listener;
		private @Nullable Metadata headers;
		/** Counted in {@link Hint#unstarted} until started or cancelled; guarded by callLock. */
		private boolean holding = true;

		LeaderCall(Hint hint, Channel main, MethodDescriptor<Q, R> method, CallOptions options, boolean replayable) {
			this.hint = hint;
			this.main = main;
			this.method = method;
			this.options = options;
			this.replayable = replayable;
			this.current = hint.channel().newCall(method, options);
		}

		@Override
		public void start(Listener<R> listener, Metadata headers) {
			boolean release = false;
			try {
				synchronized (callLock) {
					release = holding;
					holding = false;
					this.listener = listener;
					if (replayable) {
						var copy = new Metadata();
						copy.merge(headers);
						this.headers = copy;
					}
					if (release && hint.retired()) {
						// The hint was retired after this call was handed out (R46-1): the ordinary channel serves it, as any
						// call made after the retirement. The call made on the leader's channel was never started: nothing
						// was sent there.
						current = main.newCall(method, options);
						current.start(listener, headers);
					} else {
						current.start(new LeaderListener(listener), headers);
					}
				}
			} finally {
				if (release) {
					// Outside the lock: it may shut the retired channel down.
					hint.release();
				}
			}
		}

		@Override
		public void request(int numMessages) {
			synchronized (callLock) {
				requested += numMessages;
				current.request(numMessages);
			}
		}

		@Override
		public void cancel(@Nullable String message, @Nullable Throwable cause) {
			boolean release;
			synchronized (callLock) {
				cancelled = true;
				release = holding;
				holding = false;
				current.cancel(message, cause);
			}
			if (release) {
				// Cancelled before its start: it no longer keeps a retired channel open.
				hint.release();
			}
		}

		@Override
		public void halfClose() {
			synchronized (callLock) {
				halfClosed = true;
				current.halfClose();
			}
		}

		@Override
		public void sendMessage(Q message) {
			synchronized (callLock) {
				if (replayable) {
					sent.add(message);
				}
				current.sendMessage(message);
			}
		}

		@Override
		public boolean isReady() {
			synchronized (callLock) {
				return current.isReady();
			}
		}

		@Override
		public void setMessageCompression(boolean enabled) {
			synchronized (callLock) {
				current.setMessageCompression(enabled);
			}
		}

		@Override
		public Attributes getAttributes() {
			synchronized (callLock) {
				return current.getAttributes();
			}
		}

		/**
		 * Runs the call again on the ordinary channel, if it is replayable and nothing came back; returns false if it
		 * cannot.
		 */
		private boolean replay(Listener<R> listener) {
			// Under the lock to the end (R43-3): the caller's next request, message or half-close reaches the new call only
			// once it is started and has what the caller did so far, in order. Outside it, a request could reach the call
			// before its start (grpc: "Not started") or a half-close come before the replayed message.
			synchronized (callLock) {
				if (!replayable || answered || replayed || cancelled || headers == null) {
					return false;
				}
				replayed = true;
				var call = main.newCall(method, options);
				current = call;
				var swapped = replaySwappedForTesting;
				if (swapped != null) {
					swapped.run();
				}
				call.start(listener, headers);
				if (requested > 0) {
					call.request(requested);
				}
				sent.forEach(call::sendMessage);
				if (halfClosed) {
					call.halfClose();
				}
				return true;
			}
		}

		private final class LeaderListener extends ForwardingClientCallListener.SimpleForwardingClientCallListener<R> {

			private final Listener<R> caller;
			private boolean headersSeen;

			LeaderListener(Listener<R> caller) {
				super(caller);
				this.caller = caller;
			}

			@Override
			public void onHeaders(Metadata responseHeaders) {
				headersSeen = true;
				answered();
				checkNamesItself(responseHeaders);
				super.onHeaders(responseHeaders);
			}

			@Override
			public void onMessage(R message) {
				answered();
				super.onMessage(message);
			}

			@Override
			public void onClose(Status status, Metadata trailers) {
				if (status.getCode() == Status.Code.UNAVAILABLE) {
					drop(hint);
					if (replay(caller)) {
						return;
					}
				} else if (!headersSeen && status.getCode() != Status.Code.CANCELLED
						&& status.getCode() != Status.Code.DEADLINE_EXCEEDED) {
					// An answer without headers: the node's trailers name its leader instead.
					checkNamesItself(trailers);
				}
				super.onClose(status, trailers);
			}

			private void answered() {
				synchronized (callLock) {
					answered = true;
				}
			}

			/** A node that no longer names itself as the leader is not one: back to the ordinary channel. */
			private void checkNamesItself(Metadata metadata) {
				var named = metadata.get(KEY);
				if (named == null || !named.equals(hint.address())) {
					// A new name, if any, was taken by the leader channel's own hint reader already.
					drop(hint);
				}
			}
		}
	}

	/** The leader's channel for {@code method}, if it goes there and the channel is connected. */
	@Nullable Hint leaderChannelFor(MethodDescriptor<?, ?> method) {
		var hint = current;
		if (hint == null || !LEADER_METHODS.contains(method.getBareMethodName())) {
			return null;
		}
		var state = hint.channel().getState(true);
		return switch (state) {
			case READY -> hint;
			case TRANSIENT_FAILURE, SHUTDOWN -> {
				drop(hint);
				yield null;
			}
			// Connecting: the ordinary channel serves meanwhile.
			case IDLE, CONNECTING -> null;
		};
	}

	/** The address of the leader's channel, if there is one. */
	@Nullable String currentAddress() {
		var hint = current;
		return hint == null ? null : hint.address();
	}

	/** Takes {@code value}, a leader's address from a response, if it is new and allowed. */
	void offer(@Nullable String value) {
		var channels = this.channels;
		if (value == null || mode == Mode.OFF || channels == null) {
			return;
		}
		var hint = current;
		if (hint != null && hint.address().equals(value)) {
			return;
		}
		int colon = value.lastIndexOf(':');
		HostAndPort address;
		try {
			address = new HostAndPort(value.substring(0, colon), Integer.parseInt(value.substring(colon + 1)));
		} catch (RuntimeException e) {
			LOG.debug("Ignoring a malformed leader address: {}", value);
			return;
		}
		if (mode == Mode.LISTED && !listed.contains(normalize(address.host(), address.port()))) {
			LOG.debug("Ignoring the leader address {}: not one of the client's addresses", value);
			return;
		}
		Hint previous;
		ManagedChannel created;
		synchronized (lock) {
			hint = current;
			if (closed || (hint != null && hint.address().equals(value)) || backingOff(value)) {
				return;
			}
			created = channels.apply(address);
			previous = hint;
			current = new Hint(value, created);
		}
		// Connects now, so that the next call to the leader finds it ready.
		created.getState(true);
		if (previous != null) {
			// Calls in flight on it end as they do; calls handed out on it and not started yet are sent elsewhere first.
			previous.retire();
		}
		LOG.debug("Leader-bound calls go to {}", value);
	}

	/** True if {@code address}'s channel failed less than {@link #FAILED_HINT_BACKOFF_NANOS} ago. Holds {@link #lock}. */
	private boolean backingOff(String address) {
		var at = failedAt.get(address);
		return at != null && nanoTime.getAsLong() - at < FAILED_HINT_BACKOFF_NANOS;
	}

	/** Stops using {@code hint}'s channel, if it is still the current one. */
	private void drop(Hint hint) {
		synchronized (lock) {
			if (current != hint) {
				return;
			}
			current = null;
			long now = nanoTime.getAsLong();
			// Small: only addresses that failed within the backoff stay.
			failedAt.values().removeIf(at -> now - at >= FAILED_HINT_BACKOFF_NANOS);
			failedAt.put(hint.address(), now);
		}
		hint.retire();
		LOG.debug("Leader-bound calls go to the ordinary channel: {} failed", hint.address());
	}

	@Override
	public void close() {
		Hint hint;
		synchronized (lock) {
			closed = true;
			hint = current;
			current = null;
		}
		if (hint != null) {
			hint.channel().shutdownNow();
			try {
				hint.channel().awaitTermination(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
	}

	private static String normalize(String host, int port) {
		return host.toLowerCase(Locale.ROOT) + ":" + port;
	}
}
