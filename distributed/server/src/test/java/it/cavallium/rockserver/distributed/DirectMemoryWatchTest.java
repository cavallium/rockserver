package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * CR2-1: a direct memory error stops the node only on a thread of replication. On a thread that served one client
 * request (the API's transport, a forward) the request fails and the node goes on: halting would carry the same request
 * to the next node, and halt it too.
 */
class DirectMemoryWatchTest {

	private static final String APPENDER = "n0@group-0A1B2C3D4E5F->n1-GrpcLogAppender-LogAppenderDaemon";
	private static final String API_LOOP = "rockserver-grpc-server-loop-3-1";

	@TempDir
	Path temp;

	@Test
	void onlyThreadsOfReplicationAreFatal() {
		for (var name : List.of(APPENDER, "n0@group-0A1B2C3D4E5F-SegmentedRaftLogWorker",
				"n0@group-0A1B2C3D4E5F-StateMachineUpdater", "n0@group-0A1B2C3D4E5F-LeaderStateImpl",
				"n0-server-thread3", "n0-client-thread1", "n0-impl-thread12", "n0-groupManagement",
				"grpc-default-worker-ELG-3-2", "grpc-default-boss-ELG-1-1", "grpc-default-executor-4",
				// Ratis' shaded gRPC where epoll does not load (musl, noexec tmpdir, noNative, macOS)
				"grpc-nio-worker-ELG-3-1", "grpc-nio-boss-ELG-1-1", "")) {
			assertTrue(DirectMemoryWatch.fatalOn(name), name);
		}
		assertTrue(DirectMemoryWatch.fatalOn(null));
		for (var name : List.of(API_LOOP, "rockserver-grpc-n0-forward-loop-2", "rockserver-grpc-n0-forward-callback-7",
				"grpc-call-12", "grpc-proxy-3", "rockserver-n0-local-5", "rockserver-propose-9", "parallel-2", "main")) {
			assertFalse(DirectMemoryWatch.fatalOn(name), name);
		}
	}

	/** The names above, pinned on a running cluster: a Ratis or gRPC upgrade that renames its threads fails here. */
	@Test
	void theThreadsOfARunningClusterAreClassified() throws Exception {
		try (var cluster = new InProcessCluster(temp, 3, config -> config).startAll()) {
			var leader = cluster.awaitLeader(Duration.ofSeconds(30));
			var db = leader.connection().getSyncApi(RequestContext.batch(Duration.ofSeconds(20)));
			long column = db.createColumn("c", ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
			db.put(0, column, new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(1).array())),
					Buf.wrap(new byte[] {1}), RequestType.none());
			cluster.awaitConverged(Duration.ofSeconds(30));
			var names = Thread.getAllStackTraces().keySet().stream().map(Thread::getName).toList();
			for (var kind : List.of("GrpcLogAppender", "SegmentedRaftLogWorker", "StateMachineUpdater",
					"grpc-default-worker-ELG", "-server-thread")) {
				var matching = names.stream().filter(name -> name.contains(kind)).toList();
				assertFalse(matching.isEmpty(), () -> "no thread named " + kind + " in " + names);
				matching.forEach(name -> assertTrue(DirectMemoryWatch.fatalOn(name), name));
			}
			var api = names.stream().filter(name -> name.startsWith("rockserver-grpc-server-loop")).toList();
			assertFalse(api.isEmpty(), () -> "the API's event loop threads are named: " + names);
			api.forEach(name -> assertFalse(DirectMemoryWatch.fatalOn(name), name));
		}
	}

	/** Through JFR: an error on an API thread is reported as survived, one on an appender as fatal. */
	@Test
	void theWatchHaltsOnlyForAThreadOfReplication() throws Exception {
		var fatal = new CompletableFuture<String>();
		var survived = new CopyOnWriteArrayList<String>();
		try (var watch = DirectMemoryWatch.start(fatal::complete, survived::add)) {
			if (watch == null) {
				return;
			}
			long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
			// JFR records once the stream runs: make errors until one arrives.
			while (survived.isEmpty() && System.nanoTime() - deadline < 0) {
				makeError(API_LOOP);
				Thread.sleep(200);
			}
			assertFalse(survived.isEmpty(), "the error on the API thread was seen");
			assertTrue(survived.getFirst().contains(API_LOOP), survived.getFirst());
			// Recorded and dispatched in order: had it been fatal, it would be there by now.
			makeError(APPENDER);
			var description = fatal.get(30, TimeUnit.SECONDS);
			assertNotNull(description);
			assertTrue(description.contains(APPENDER) && description.contains("direct buffer memory"), description);
			assertTrue(survived.stream().noneMatch(text -> text.contains(APPENDER)), survived::toString);
		}
	}

	private static void makeError(String threadName) throws InterruptedException {
		Thread.ofPlatform().name(threadName).start(() -> {
			var error = new OutOfMemoryError("Cannot reserve 8 bytes of direct buffer memory (allocated: 1, limit: 2)");
			assertNotNull(error.getMessage());
		}).join();
	}
}
