package it.cavallium.rockserver.core.impl.test;

import static it.cavallium.rockserver.core.common.Utils.toBufSimple;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.server.GrpcServer;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import reactor.core.publisher.Flux;

@Timeout(60)
class GrpcExactRangeCountTest {

	private static final int VALUE_ENTRY_COUNT = 4_200;
	private static final int BUCKETED_ENTRY_COUNT = 257;

	@TempDir
	Path tempDir;

	@Test
	void remoteRangesReuseOneNativeIteratorWithoutOpeningProtocolIterators() throws Exception {
		try (var embedded = new EmbeddedConnection(tempDir.resolve("db"), "grpc-exact-range-count", null)) {
			var backend = embedded.getSyncApi();
			long valueColumn = backend.createColumn("values",
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			long bucketedColumn = backend.createColumn("bucketed-values",
					ColumnSchema.of(IntList.of(1), ObjectList.of(ColumnHashType.ALLSAME8), true));

			for (int i = 0; i < VALUE_ENTRY_COUNT; i++) {
				backend.put(0, valueColumn, intKey(i), intValue(i), RequestType.none());
			}
			for (int i = 0; i < BUCKETED_ENTRY_COUNT; i++) {
				backend.put(0, bucketedColumn, bucketedKey(i), intValue(i), RequestType.none());
			}
			backend.flush();
			var iteratorOpens = new AtomicInteger();
			embedded.getInternalDB().setRangeIteratorOpenObserverForTesting(iteratorOpens::incrementAndGet);

			assertEquals(0, embedded.getInternalDB().getOpenIteratorsCount());
			try (var server = new GrpcServer(embedded, new InetSocketAddress("127.0.0.1", 0))) {
				server.start();
				try (var client = GrpcConnection.forHostAndPort("grpc-exact-range-count",
						new Utils.HostAndPort("127.0.0.1", server.getPort()))) {
					iteratorOpens.set(0);
					assertEquals(VALUE_ENTRY_COUNT, exactCount(client, valueColumn, false));
					assertEquals(1, iteratorOpens.get(),
							"remote exact count must reuse one native iterator across physical slices");
					assertEquals(0, embedded.getInternalDB().getOpenIteratorsCount(),
							"exact value count must not create a protocol iterator registration");

					iteratorOpens.set(0);
					long bucketedCount = client.getAsyncApi().reduceRangeAsync(0,
							bucketedColumn,
							null,
							null,
							true,
							RequestType.entriesCount(),
							10_000).get(10, TimeUnit.SECONDS);
					assertEquals(BUCKETED_ENTRY_COUNT, bucketedCount);
					assertEquals(1, iteratorOpens.get(),
							"remote bucket count must use one native iterator");
					assertEquals(0, embedded.getInternalDB().getOpenIteratorsCount(),
							"exact bucketed count must not create a protocol iterator registration");

					iteratorOpens.set(0);
					var endpoints = client.getAsyncApi().reduceRangeAsync(0,
							valueColumn,
							null,
							null,
							false,
							RequestType.firstAndLast(),
							10_000).get(10, TimeUnit.SECONDS);
					assertEquals(intKey(0), endpoints.first().keys());
					assertEquals(intKey(VALUE_ENTRY_COUNT - 1), endpoints.last().keys());
					assertEquals(1, iteratorOpens.get(),
							"remote endpoints must be two seeks on one native iterator");

					var expectedValueColumnValues = java.util.stream.IntStream.range(0, VALUE_ENTRY_COUNT)
							.mapToObj(GrpcExactRangeCountTest::intValue)
							.toList();
					iteratorOpens.set(0);
					var valueColumnValues = Flux.from(client.getAsyncApi().getRangeAsync(0,
							valueColumn,
							null,
							null,
							false,
							RequestType.allInRange(),
							10_000))
							.map(kv -> kv.value())
							.collectList()
							.block(Duration.ofSeconds(20));
					assertIterableEquals(expectedValueColumnValues, valueColumnValues);
					assertEquals(1, iteratorOpens.get(),
							"remote full range must reuse one native iterator across scheduler slices");

					var expectedValues = java.util.stream.IntStream.range(0, BUCKETED_ENTRY_COUNT)
							.mapToObj(GrpcExactRangeCountTest::intValue)
							.toList();
					iteratorOpens.set(0);
					var forwardValues = Flux.from(client.getAsyncApi().getRangeAsync(0,
							bucketedColumn,
							null,
							null,
							false,
							RequestType.allInRange(),
							10_000))
							.map(kv -> kv.value())
							.collectList()
							.block();
					assertIterableEquals(expectedValues, forwardValues);
					assertEquals(1, iteratorOpens.get());

					var expectedReverse = new ArrayList<>(expectedValues);
					java.util.Collections.reverse(expectedReverse);
					iteratorOpens.set(0);
					var reverseValues = Flux.from(client.getAsyncApi().getRangeAsync(0,
							bucketedColumn,
							null,
							null,
							true,
							RequestType.allInRange(),
							10_000))
							.map(kv -> kv.value())
							.collectList()
							.block();
					assertIterableEquals(expectedReverse, reverseValues);
					assertEquals(1, iteratorOpens.get());
				}
			}
			embedded.getInternalDB().setRangeIteratorOpenObserverForTesting(null);
		}
	}

	private static long exactCount(GrpcConnection connection, long columnId, boolean reverse) {
		return connection.getSyncApi().reduceRange(0,
				columnId,
				null,
				null,
				reverse,
				RequestType.entriesCount(),
				10_000);
	}

	private static Keys intKey(int value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(value).array()));
	}

	private static Keys bucketedKey(int value) {
		return new Keys(toBufSimple(7), toBufSimple(value >>> Byte.SIZE, value & 0xFF));
	}

	private static Buf intValue(int value) {
		return Buf.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
	}
}
