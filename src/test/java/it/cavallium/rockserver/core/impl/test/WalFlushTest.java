package it.cavallium.rockserver.core.impl.test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.*;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.api.proto.FlushRequest;
import it.cavallium.rockserver.core.server.GrpcServer;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
class WalFlushTest {
	@TempDir Path tempDir;

	@Test
	void defaultCommandAndOldWireRetainFullFlushAndOldImplementationsFallBack() throws Exception {
		assertFalse(new RocksDBAPICommand.Flush().walOnly());
		assertFalse(FlushRequest.parseFrom(new byte[] {8, 3}).getWalOnly());
		assertEquals(3, FlushRequest.parseFrom(new byte[] {8, 3}).getWorkloadContractVersion());
		assertEquals(2, FlushRequest.getDescriptor().findFieldByName("wal_only").getNumber());
		var syncCalls = new AtomicInteger();
		new RocksDBAPICommand.Flush(true).handleSync(new RocksDBSyncAPI() {
			@Override public void flush() { syncCalls.incrementAndGet(); }
		});
		assertEquals(1, syncCalls.get());
		var asyncCalls = new AtomicInteger();
		new RocksDBAPICommand.Flush(true).handleAsync(new RocksDBAsyncAPI() {
			@Override public reactor.core.publisher.Mono<it.cavallium.rockserver.core.common.cdc.CdcBatch>
					cdcPollBatchAsync(String id, Long fromSeq, long maxEvents) {
				return reactor.core.publisher.Mono.error(new UnsupportedOperationException());
			}
			@Override public CompletableFuture<Void> flushAsync() {
				asyncCalls.incrementAndGet();
				return CompletableFuture.completedFuture(null);
			}
		}).get(5, SECONDS);
		assertEquals(1, asyncCalls.get());
	}

	@Test
	void embeddedWalBarrierDoesNotFlushMemtablesAndFullFlushStillDoes() throws Exception {
		try (var connection = new EmbeddedConnection(tempDir.resolve("embedded"), "wal-barrier", null)) {
			var logging = new it.cavallium.rockserver.core.client.LoggingClient(connection);
			assertWalOnly(connection, logging.getSyncApi(RequestContext.batch()),
					logging.getAsyncApi(RequestContext.batch()));
		}
	}

	@Test
	void grpcClientWalBarrierKeepsMemtablesAndFullFlushBehavior() throws Exception {
		try (var connection = new EmbeddedConnection(tempDir.resolve("grpc"), "grpc-wal-barrier", null);
				var server = new GrpcServer(connection, new InetSocketAddress("127.0.0.1", 0))) {
			server.start();
			try (var client = GrpcConnection.forHostAndPort("wal-client",
					new Utils.HostAndPort("127.0.0.1", server.getPort()))) {
				assertWalOnly(connection, client.getSyncApi(RequestContext.batch()),
						client.getAsyncApi(RequestContext.batch()));
			}
		}
	}

	private static void assertWalOnly(EmbeddedConnection connection, RocksDBSyncAPI sync, RocksDBAsyncAPI async)
			throws Exception {
		var local = connection.getSyncApi(RequestContext.batch());
		long column = local.createColumn("wal-column", ColumnSchema.of(IntList.of(1), ObjectList.of(), true));
		var key = new Keys(Buf.wrap(new byte[] {1}));
		var value = Buf.wrap(new byte[] {42});
		local.put(0, column, key, value, RequestType.none());
		var fullFlushes = new AtomicInteger();
		connection.getInternalDB().setColumnMaintenanceObserverForTesting(fullFlushes::incrementAndGet);
		try {
			assertTrue(local.getSstMetadata(column, -1).files().isEmpty());
			sync.flushWal();
			async.flushWalAsync().get(5, SECONDS);
			assertEquals(0, fullFlushes.get());
			assertTrue(local.getSstMetadata(column, -1).files().isEmpty(), "WAL sync must leave the memtable in memory");
			assertEquals(value, local.get(0, column, key, RequestType.current()));
			sync.flush();
			assertEquals(1, fullFlushes.get());
			assertFalse(local.getSstMetadata(column, -1).files().isEmpty(), "full flush must still create an SST");
		} finally {
			connection.getInternalDB().setColumnMaintenanceObserverForTesting(null);
		}
	}
}
