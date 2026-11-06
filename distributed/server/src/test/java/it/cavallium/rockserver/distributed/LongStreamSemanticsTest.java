package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import reactor.core.publisher.Flux;

/**
 * The snapshot semantics of a long range stream outside a transaction ({@code getRangeAsync} with transaction id 0),
 * pinned against concurrent writes, on the leader and through a follower that forwards the stream to it.
 *
 * <p>What the test pins: the stream reads one point-in-time view of the leader's database, taken when its cursor opens
 * there (after the read barrier). Every write acknowledged before the stream started is in it; writes made while it
 * runs are not, even for keys the stream reaches only later: updated keys keep their old values, deleted keys are still
 * there, and keys inserted ahead of the cursor do not appear. It is one RocksDB iterator, opened once per stream, whose
 * implicit snapshot is its sequence number at creation. The stream holds that view (and the files it pins) until it
 * ends, as on a single node. A forwarded stream is the leader's stream relayed: the same view. A leader change ends
 * the stream with a retryable error instead of resuming it elsewhere (see {@code ForwardingFailoverTest}).</p>
 */
class LongStreamSemanticsTest {

	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	private static final RequestContext CONTEXT = RequestContext.batch(Duration.ofSeconds(60));
	/** Enough data for several range chunks (8 MiB each at most), so most of the stream is read after the writes. */
	private static final int KEYS = 3000;
	private static final int VALUE_BYTES = 8 * 1024;
	private static final int READ_BEFORE_WRITES = 10;

	@TempDir
	Path temp;
	private InProcessCluster cluster;

	@AfterEach
	void tearDown() throws Exception {
		if (cluster != null) {
			cluster.close();
		}
	}

	@Test
	void aLongStreamSeesOnePointInTimeViewOnTheLeaderAndThroughAFollower() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var leader = cluster.awaitLeader(Duration.ofSeconds(30));
		var follower = cluster.anyFollower();
		var writer = leader.connection().getSyncApi(CONTEXT);
		check(writer, writer.createColumn("onLeader", SCHEMA), leader);
		check(writer, writer.createColumn("forwarded", SCHEMA), follower);
	}

	/** Fills {@code column}, starts a stream on {@code reader}, writes concurrently, and checks what it returned. */
	private void check(RocksDBSyncAPI writer, long column, ReplicatedNode reader) throws Exception {
		for (int first = 0; first < KEYS; first += 100) {
			var keys = new ArrayList<Keys>();
			var values = new ArrayList<Buf>();
			for (int i = first; i < first + 100; i++) {
				// Even keys only: the odd ones are free for inserts ahead of the cursor.
				keys.add(key(2L * i));
				values.add(value("v0", i));
			}
			writer.putMulti(0, column, keys, values, RequestType.none());
		}
		var stream = Flux.from(reader.connection().getAsyncApi(CONTEXT)
				.getRangeAsync(0, column, null, null, false, RequestType.<KV>allInRange()))
				.toIterable(1)
				.iterator();
		var seen = new ArrayList<KV>();
		for (int i = 0; i < READ_BEFORE_WRITES; i++) {
			seen.add(stream.next());
		}
		// While the stream is open: every key updated, the last hundred deleted, and new keys inserted everywhere,
		// all acknowledged before the stream reads further.
		for (int first = 0; first < KEYS; first += 100) {
			var keys = new ArrayList<Keys>();
			var values = new ArrayList<Buf>();
			var inserted = new ArrayList<Keys>();
			var insertedValues = new ArrayList<Buf>();
			for (int i = first; i < first + 100; i++) {
				keys.add(key(2L * i));
				values.add(value("v1", i));
				inserted.add(key(2L * i + 1));
				insertedValues.add(value("new", i));
			}
			writer.putMulti(0, column, keys, values, RequestType.none());
			writer.putMulti(0, column, inserted, insertedValues, RequestType.none());
		}
		var deleted = new ArrayList<Keys>();
		for (int i = KEYS - 100; i < KEYS; i++) {
			deleted.add(key(2L * i));
		}
		writer.deleteMulti(0, column, deleted, RequestType.none());
		while (stream.hasNext()) {
			seen.add(stream.next());
		}

		assertEquals(KEYS, seen.size(), "the stream saw inserted or lost deleted keys");
		for (int i = 0; i < KEYS; i++) {
			var kv = seen.get(i);
			assertEquals(2L * i, ByteBuffer.wrap(kv.keys().keys()[0].toByteArray()).getLong(), "key at " + i);
			assertTrue(Arrays.equals(value("v0", i).toByteArray(), kv.value().toByteArray()),
					() -> "key " + kv + " holds a value written after the stream started");
		}
		// A stream started now sees the writes.
		var after = Flux.from(reader.connection().getAsyncApi(CONTEXT)
				.getRangeAsync(0, column, null, null, false, RequestType.<KV>allInRange())).collectList().block();
		assertEquals(2 * KEYS - 100, after.size());
	}

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}

	private static Buf value(String version, int index) {
		var bytes = new byte[VALUE_BYTES];
		var prefix = (version + "-" + index + "-").getBytes(StandardCharsets.UTF_8);
		System.arraycopy(prefix, 0, bytes, 0, prefix.length);
		return Buf.wrap(bytes);
	}
}
