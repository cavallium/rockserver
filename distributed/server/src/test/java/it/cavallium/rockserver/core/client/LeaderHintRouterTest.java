package it.cavallium.rockserver.core.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * Channel selection of {@link LeaderHintRouter}, with fake channels. It lives here, in the core client's package on the
 * class path, because the core module's own tests run as a separate module that sees only exported classes.
 */
class LeaderHintRouterTest {

	private static final List<HostAndPort> LISTED = List.of(new HostAndPort("node-a", 5333),
			new HostAndPort("node-b", 5333), new HostAndPort("node-c", 5333));

	private final AtomicLong now = new AtomicLong();
	private final List<FakeChannel> opened = new ArrayList<>();
	private final FakeChannel main = new FakeChannel("main");

	private LeaderHintRouter router(LeaderHintRouter.Mode mode) {
		var router = new LeaderHintRouter(mode, LISTED, Set.of("getRangePage"), now::get);
		router.start(address -> {
			var channel = new FakeChannel(address.host() + ":" + address.port());
			opened.add(channel);
			return channel;
		});
		return router;
	}

	private static MethodDescriptor<byte[], byte[]> method(String name) {
		return MethodDescriptor.<byte[], byte[]>newBuilder()
				.setType(MethodDescriptor.MethodType.UNARY)
				.setFullMethodName(MethodDescriptor.generateFullMethodName("RocksDBService", name))
				.setRequestMarshaller(BytesMarshaller.INSTANCE)
				.setResponseMarshaller(BytesMarshaller.INSTANCE)
				.build();
	}

	/** Starts a call of {@code name} on the routed channel; returns the fake channel that got it. */
	private FakeChannel call(Channel routed, String name) {
		var channels = new ArrayList<>(opened);
		channels.add(main);
		var before = channels.stream().map(channel -> channel.calls).toList();
		var call = routed.newCall(method(name), CallOptions.DEFAULT);
		call.start(new ClientCall.Listener<>() {}, new Metadata());
		FakeChannel got = null;
		for (int i = 0; i < channels.size(); i++) {
			if (channels.get(i).calls != before.get(i)) {
				assertNull(got, "the call went to two channels");
				got = channels.get(i);
			}
		}
		assertTrue(got != null, "the call went nowhere");
		return got;
	}

	@Test
	void withoutHintEverythingTakesTheOrdinaryChannel() {
		var router = router(LeaderHintRouter.Mode.LISTED);
		var routed = router.route(main);
		assertSame(main, call(routed, "put"));
		assertSame(main, call(routed, "get"));
		assertTrue(opened.isEmpty());
	}

	@Test
	void leaderBoundCallsGoToTheLeaderOnceItIsConnectedAndPointReadsStay() {
		var router = router(LeaderHintRouter.Mode.LISTED);
		var routed = router.route(main);
		router.offer("node-b:5333");
		assertEquals(1, opened.size());
		var leader = opened.getFirst();
		assertTrue(leader.connectRequested, "the leader's channel connects at once");

		// Still connecting: the ordinary channel serves meanwhile.
		leader.state = ConnectivityState.CONNECTING;
		assertSame(main, call(routed, "put"));

		leader.state = ConnectivityState.READY;
		for (var name : List.of("put", "getForUpdate", "openTransaction", "closeTransaction", "getRangePage",
				"deleteRange")) {
			assertSame(leader, call(routed, name), name);
		}
		for (var name : List.of("get", "exists", "existsMulti", "getMulti", "getCapabilities", "flush", "compact")) {
			assertSame(main, call(routed, name), name);
		}

		// The same hint again changes nothing.
		router.offer("node-b:5333");
		assertEquals(1, opened.size());
	}

	@Test
	void newLeaderReplacesTheChannel() {
		var router = router(LeaderHintRouter.Mode.LISTED);
		var routed = router.route(main);
		router.offer("node-b:5333");
		router.offer("node-c:5333");
		assertEquals(2, opened.size());
		assertTrue(opened.get(0).shutdown, "the old leader's channel is shut down");
		opened.get(1).state = ConnectivityState.READY;
		assertSame(opened.get(1), call(routed, "put"));
	}

	@Test
	void unlistedAddressIsIgnoredUnlessAnyIsAllowed() {
		var listed = router(LeaderHintRouter.Mode.LISTED);
		listed.offer("10.0.0.7:5333");
		listed.offer("node-b:6000");
		listed.offer("garbage");
		assertTrue(opened.isEmpty());
		// Host names compare without case.
		listed.offer("NODE-B:5333");
		assertEquals(1, opened.size());

		opened.clear();
		var any = router(LeaderHintRouter.Mode.ANY);
		any.offer("10.0.0.7:5333");
		assertEquals(1, opened.size());

		opened.clear();
		var off = router(LeaderHintRouter.Mode.OFF);
		off.offer("node-b:5333");
		assertTrue(opened.isEmpty());
	}

	@Test
	void hintsBeforeStartAreIgnored() {
		var router = new LeaderHintRouter(LeaderHintRouter.Mode.LISTED, LISTED);
		router.offer("node-b:5333");
		assertNull(router.currentAddress());
	}

	@Test
	void unavailableOnTheLeaderChannelDropsTheHintForAWhile() {
		var router = router(LeaderHintRouter.Mode.LISTED);
		var routed = router.route(main);
		router.offer("node-b:5333");
		var leader = opened.getFirst();
		leader.state = ConnectivityState.READY;
		assertSame(leader, call(routed, "put"));

		leader.lastListener.onClose(Status.UNAVAILABLE, new Metadata());
		assertNull(router.currentAddress());
		assertTrue(leader.shutdown);
		assertSame(main, call(routed, "put"));

		// A follower still names the dead leader: not taken again at once.
		router.offer("node-b:5333");
		assertEquals(1, opened.size());
		now.addAndGet(LeaderHintRouter.FAILED_HINT_BACKOFF_NANOS + 1);
		router.offer("node-b:5333");
		assertEquals(2, opened.size());
	}

	/**
	 * Two nodes that each name the other while their channels keep failing: each address waits its own backoff, so the
	 * client does not open a channel at every hint, alternating between them.
	 */
	@Test
	void twoFailingHintsDoNotAlternateWithoutBackoff() {
		var router = router(LeaderHintRouter.Mode.LISTED);
		var routed = router.route(main);
		router.offer("node-b:5333");
		var b = opened.getLast();
		b.state = ConnectivityState.READY;
		call(routed, "put");
		b.lastListener.onClose(Status.UNAVAILABLE, new Metadata());

		router.offer("node-c:5333");
		var c = opened.getLast();
		assertEquals(2, opened.size());
		c.state = ConnectivityState.READY;
		call(routed, "put");
		c.lastListener.onClose(Status.UNAVAILABLE, new Metadata());

		// Both failed within the backoff: neither is taken again, in any order.
		router.offer("node-b:5333");
		router.offer("node-c:5333");
		router.offer("node-b:5333");
		assertEquals(2, opened.size(), "no channel opened while both addresses back off");
		assertNull(router.currentAddress());

		now.addAndGet(LeaderHintRouter.FAILED_HINT_BACKOFF_NANOS + 1);
		router.offer("node-b:5333");
		assertEquals(3, opened.size());
		assertEquals("node-b:5333", router.currentAddress());
	}

	/**
	 * Every method of the service is classified: sent to the leader, or explicitly kept on the ordinary channel. A new
	 * RPC must be added to one of the two sets.
	 */
	@Test
	void everyServiceMethodIsClassified() {
		var methods = new java.util.TreeSet<String>();
		for (var method : it.cavallium.rockserver.core.common.api.proto.RocksDBServiceGrpc.getServiceDescriptor()
				.getMethods()) {
			methods.add(method.getBareMethodName());
		}
		var both = new java.util.TreeSet<>(LeaderHintRouter.LEADER_METHODS);
		both.retainAll(LeaderHintRouter.ORDINARY_METHODS);
		assertEquals(Set.of(), both, "in both sets");
		var unclassified = new java.util.TreeSet<>(methods);
		unclassified.removeAll(LeaderHintRouter.LEADER_METHODS);
		unclassified.removeAll(LeaderHintRouter.ORDINARY_METHODS);
		assertEquals(Set.of(), unclassified, "methods in neither set");
		var unknown = new java.util.TreeSet<>(LeaderHintRouter.LEADER_METHODS);
		unknown.addAll(LeaderHintRouter.ORDINARY_METHODS);
		unknown.removeAll(methods);
		assertEquals(Set.of(), unknown, "names that are not methods of the service");
		assertTrue(LeaderHintRouter.LEADER_METHODS.contains("openClientSession"), "registered through the log");
	}

	private static Metadata naming(@Nullable String leader) {
		var metadata = new Metadata();
		if (leader != null) {
			metadata.put(LeaderHintRouter.KEY, leader);
		}
		return metadata;
	}

	@Test
	void otherErrorsOfTheLeaderKeepTheHint() {
		var router = router(LeaderHintRouter.Mode.LISTED);
		var routed = router.route(main);
		router.offer("node-b:5333");
		var leader = opened.getFirst();
		leader.state = ConnectivityState.READY;
		call(routed, "put");
		leader.lastListener.onClose(Status.ABORTED, naming("node-b:5333"));
		assertEquals("node-b:5333", router.currentAddress());
		// A deadline or a cancellation is the client's own: nothing to learn about the node.
		call(routed, "put");
		leader.lastListener.onClose(Status.DEADLINE_EXCEEDED, new Metadata());
		assertEquals("node-b:5333", router.currentAddress());
	}

	@Test
	void hintedNodeThatNoLongerNamesItselfIsDropped() {
		var router = router(LeaderHintRouter.Mode.LISTED);
		var routed = router.route(main);
		router.offer("node-b:5333");
		var leader = opened.getFirst();
		leader.state = ConnectivityState.READY;

		// A deposed or isolated node: it fails calls without naming any leader.
		call(routed, "put");
		leader.lastListener.onClose(Status.RESOURCE_EXHAUSTED, naming(null));
		assertNull(router.currentAddress());
		assertSame(main, call(routed, "put"));

		// Answering with headers that name another node.
		now.addAndGet(LeaderHintRouter.FAILED_HINT_BACKOFF_NANOS + 1);
		router.offer("node-b:5333");
		var again = opened.getLast();
		again.state = ConnectivityState.READY;
		assertSame(again, call(routed, "put"));
		again.lastListener.onHeaders(naming("node-c:5333"));
		assertTrue(again.shutdown);
		assertSame(main, call(routed, "put"));
	}

	@Test
	void replayableReadFailingOnTheLeaderRunsAgainOnTheOrdinaryChannel() {
		var router = router(LeaderHintRouter.Mode.LISTED);
		var routed = router.route(main);
		router.offer("node-b:5333");
		var leader = opened.getFirst();
		leader.state = ConnectivityState.READY;

		var closes = new ArrayList<Status>();
		var call = routed.newCall(method("getRangePage"), CallOptions.DEFAULT);
		call.start(new ClientCall.Listener<>() {
			@Override
			public void onClose(Status status, Metadata trailers) {
				closes.add(status);
			}
		}, new Metadata());
		call.request(1);
		call.sendMessage(new byte[] {1});
		call.halfClose();
		assertEquals(1, leader.calls);
		int mainCalls = main.calls;

		// The leader dies before answering: the read runs again through the ordinary channel, as the caller sent it.
		leader.lastListener.onClose(Status.UNAVAILABLE, new Metadata());
		assertTrue(closes.isEmpty(), "the caller saw the failure: " + closes);
		assertEquals(mainCalls + 1, main.calls);
		assertEquals(List.of(1), main.lastCall.requested);
		assertEquals(1, main.lastCall.messages);
		assertTrue(main.lastCall.halfClosed);
		assertNull(router.currentAddress());
		main.lastListener.onClose(Status.OK, new Metadata());
		assertEquals(List.of(Status.OK), closes);

		// A write is not sent again: its outcome is the caller's to judge.
		now.addAndGet(LeaderHintRouter.FAILED_HINT_BACKOFF_NANOS + 1);
		router.offer("node-b:5333");
		var again = opened.getLast();
		again.state = ConnectivityState.READY;
		var writeCloses = new ArrayList<Status>();
		var write = routed.newCall(method("put"), CallOptions.DEFAULT);
		write.start(new ClientCall.Listener<>() {
			@Override
			public void onClose(Status status, Metadata trailers) {
				writeCloses.add(status);
			}
		}, new Metadata());
		mainCalls = main.calls;
		again.lastListener.onClose(Status.UNAVAILABLE, new Metadata());
		assertEquals(List.of(Status.Code.UNAVAILABLE), writeCloses.stream().map(Status::getCode).toList());
		assertEquals(mainCalls, main.calls);

		// Nor is a read that already got an answer.
		now.addAndGet(LeaderHintRouter.FAILED_HINT_BACKOFF_NANOS + 1);
		router.offer("node-b:5333");
		var third = opened.getLast();
		third.state = ConnectivityState.READY;
		var readCloses = new ArrayList<Status>();
		var read = routed.newCall(method("getRangePage"), CallOptions.DEFAULT);
		read.start(new ClientCall.Listener<>() {
			@Override
			public void onClose(Status status, Metadata trailers) {
				readCloses.add(status);
			}
		}, new Metadata());
		third.lastListener.onHeaders(naming("node-b:5333"));
		mainCalls = main.calls;
		third.lastListener.onClose(Status.UNAVAILABLE, new Metadata());
		assertEquals(1, readCloses.size());
		assertEquals(mainCalls, main.calls);
	}

	@Test
	void failedChannelIsDroppedWhenACallLooksForIt() {
		var router = router(LeaderHintRouter.Mode.LISTED);
		var routed = router.route(main);
		router.offer("node-b:5333");
		opened.getFirst().state = ConnectivityState.TRANSIENT_FAILURE;
		assertSame(main, call(routed, "put"));
		assertNull(router.currentAddress());
	}

	@Test
	void hintReaderTakesTheAddressFromHeadersAndTrailers() {
		var router = router(LeaderHintRouter.Mode.LISTED);
		var reader = router.hintReader();
		var call = reader.interceptCall(method("get"), CallOptions.DEFAULT, main);
		call.start(new ClientCall.Listener<>() {}, new Metadata());
		var headers = new Metadata();
		headers.put(LeaderHintRouter.KEY, "node-a:5333");
		main.lastListener.onHeaders(headers);
		assertEquals("node-a:5333", router.currentAddress());

		var failed = reader.interceptCall(method("get"), CallOptions.DEFAULT, main);
		failed.start(new ClientCall.Listener<>() {}, new Metadata());
		var trailers = new Metadata();
		trailers.put(LeaderHintRouter.KEY, "node-c:5333");
		main.lastListener.onClose(Status.UNAVAILABLE, trailers);
		assertEquals("node-c:5333", router.currentAddress());
	}

	/** Records its calls; its state is set by the test. */
	private static final class FakeChannel extends ManagedChannel {

		final String name;
		volatile ConnectivityState state = ConnectivityState.IDLE;
		boolean connectRequested;
		boolean shutdown;
		/** The listener of the last call started on this channel. */
		ClientCall.@Nullable Listener<?> lastListener;
		@Nullable FakeCall<?, ?> lastCall;
		int calls;

		FakeChannel(String name) {
			this.name = name;
		}

		@Override
		public <Q, R> ClientCall<Q, R> newCall(MethodDescriptor<Q, R> method, CallOptions options) {
			calls++;
			var call = new FakeCall<Q, R>();
			lastCall = call;
			return call;
		}

		@Override
		public String authority() {
			return name;
		}

		@Override
		public ConnectivityState getState(boolean requestConnection) {
			if (requestConnection) {
				connectRequested = true;
			}
			return shutdown ? ConnectivityState.SHUTDOWN : state;
		}

		@Override
		public ManagedChannel shutdown() {
			shutdown = true;
			return this;
		}

		@Override
		public boolean isShutdown() {
			return shutdown;
		}

		@Override
		public boolean isTerminated() {
			return shutdown;
		}

		@Override
		public ManagedChannel shutdownNow() {
			return shutdown();
		}

		@Override
		public boolean awaitTermination(long timeout, TimeUnit unit) {
			return true;
		}

		@Override
		public String toString() {
			return name;
		}

		private final class FakeCall<Q, R> extends ClientCall<Q, R> {

			@Override
			public void start(Listener<R> listener, Metadata headers) {
				lastListener = listener;
			}

			final List<Integer> requested = new ArrayList<>();
			int messages;
			boolean halfClosed;

			@Override
			public void request(int numMessages) {
				requested.add(numMessages);
			}

			@Override
			public void cancel(@Nullable String message, @Nullable Throwable cause) {}

			@Override
			public void halfClose() {
				halfClosed = true;
			}

			@Override
			public void sendMessage(Q message) {
				messages++;
			}
		}
	}

	private enum BytesMarshaller implements MethodDescriptor.Marshaller<byte[]> {
		INSTANCE;

		@Override
		public InputStream stream(byte[] value) {
			return new ByteArrayInputStream(value);
		}

		@Override
		public byte[] parse(InputStream stream) {
			return new byte[0];
		}
	}
}
