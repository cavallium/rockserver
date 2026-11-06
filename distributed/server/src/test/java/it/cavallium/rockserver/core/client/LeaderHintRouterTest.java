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
		var router = new LeaderHintRouter(mode, LISTED, now::get);
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

	@Test
	void otherErrorsKeepTheHint() {
		var router = router(LeaderHintRouter.Mode.LISTED);
		var routed = router.route(main);
		router.offer("node-b:5333");
		var leader = opened.getFirst();
		leader.state = ConnectivityState.READY;
		call(routed, "put");
		leader.lastListener.onClose(Status.ABORTED, new Metadata());
		assertEquals("node-b:5333", router.currentAddress());
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
		int calls;

		FakeChannel(String name) {
			this.name = name;
		}

		@Override
		public <Q, R> ClientCall<Q, R> newCall(MethodDescriptor<Q, R> method, CallOptions options) {
			calls++;
			return new FakeCall<>();
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

			@Override
			public void request(int numMessages) {}

			@Override
			public void cancel(@Nullable String message, @Nullable Throwable cause) {}

			@Override
			public void halfClose() {}

			@Override
			public void sendMessage(Q message) {}
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
