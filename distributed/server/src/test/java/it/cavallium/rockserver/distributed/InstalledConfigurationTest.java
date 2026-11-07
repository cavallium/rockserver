package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.distributed.RemovalRule.Conf;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Round-13 review, finding 1, and round 15 (R14-1): the term bounds of the configurations snapshot chunks gave Ratis,
 * one per configuration, never lowered by a chunk of another configuration, nor by concurrent chunks.
 */
class InstalledConfigurationTest {

	private static final List<String> ABX = List.of("a", "b", "x");
	private static final List<String> AB = List.of("a", "b");

	private static long bound(Path raftDir, long index, List<String> newHalf, List<String> oldHalf) {
		return InstalledConfiguration.boundOf(InstalledConfiguration.read(raftDir),
				new Conf(RemovalRule.UNKNOWN_TERM, index, newHalf, oldHalf));
	}

	@Test
	void theBoundIsTheHighestLeaderTermOfEachConfiguration(@TempDir Path raftDir) throws Exception {
		assertEquals(RemovalRule.UNKNOWN_TERM, bound(raftDir, 40, ABX, AB));
		InstalledConfiguration.update(raftDir, 0, 3, 40, ABX, AB);
		assertEquals(3, bound(raftDir, 40, List.of("x", "b", "a"), List.of("b", "a")));
		// A chunk of a later leader with the same configuration raises it; one of an earlier term does not lower it.
		InstalledConfiguration.update(raftDir, 0, 5, 40, ABX, AB);
		InstalledConfiguration.update(raftDir, 0, 4, 40, ABX, AB);
		assertEquals(5, bound(raftDir, 40, ABX, AB));
		// Another configuration has a bound of its own, and the first one keeps its bound.
		InstalledConfiguration.update(raftDir, 0, 6, 41, AB, List.of());
		assertEquals(6, bound(raftDir, 41, AB, List.of()));
		assertEquals(5, bound(raftDir, 40, ABX, AB));
		// Other halves at the same index, or an unknown old half: no bound.
		assertEquals(RemovalRule.UNKNOWN_TERM, bound(raftDir, 41, ABX, List.of()));
		assertEquals(RemovalRule.UNKNOWN_TERM, InstalledConfiguration.boundOf(InstalledConfiguration.read(raftDir),
				new Conf(RemovalRule.UNKNOWN_TERM, 41, AB, null)));
		// A damaged file bounds nothing.
		Files.writeString(raftDir.resolve(InstalledConfiguration.FILE), "41\n");
		assertNull(InstalledConfiguration.read(raftDir));
		assertEquals(RemovalRule.UNKNOWN_TERM, bound(raftDir, 41, AB, List.of()));
	}

	/** What RaftTraffic records from a chunk: the configuration's index and halves, with the leader's term. */
	@Test
	void aChunkIsRecordedWithItsLeadersTerm(@TempDir Path raftDir) throws Exception {
		var conf = RaftProtos.RaftConfigurationProto.newBuilder();
		for (var id : ABX) {
			conf.addPeers(RaftProtos.RaftPeerProto.newBuilder().setId(ByteString.copyFromUtf8(id)));
		}
		for (var id : AB) {
			conf.addOldPeers(RaftProtos.RaftPeerProto.newBuilder().setId(ByteString.copyFromUtf8(id)));
		}
		var request = RaftProtos.InstallSnapshotRequestProto.newBuilder().setLeaderTerm(7)
				.setLastRaftConfigurationLogEntryProto(RaftProtos.LogEntryProto.newBuilder().setIndex(40)
						.setConfigurationEntry(conf))
				.build();
		InstalledConfiguration.record(raftDir, 0, request);
		assertEquals(7, bound(raftDir, 40, ABX, AB));
		// A chunk without a configuration changes nothing.
		InstalledConfiguration.record(raftDir, 0, RaftProtos.InstallSnapshotRequestProto.newBuilder().setLeaderTerm(9)
				.build());
		assertEquals(7, bound(raftDir, 40, ABX, AB));
	}

	/**
	 * R14-1: a chunk of a deposed leader with another configuration D', then one of an older deposed leader with the
	 * configuration D that Ratis held before: Ratis takes both (it applies the configuration of a chunk it rejects for
	 * its term), and D's bound must stay the highest term of a chunk with D. With one record, replaced by each chunk of
	 * another configuration, it became 2.
	 */
	@Test
	void aChunkOfAnotherConfigurationDoesNotLowerTheBound(@TempDir Path raftDir) throws Exception {
		InstalledConfiguration.update(raftDir, 0, 6, 40, ABX, AB);
		InstalledConfiguration.update(raftDir, 0, 4, 39, AB, List.of());
		InstalledConfiguration.update(raftDir, 0, 2, 40, ABX, AB);
		assertEquals(6, bound(raftDir, 40, ABX, AB));
		assertEquals(4, bound(raftDir, 39, AB, List.of()));
	}

	/**
	 * R14-1: install streams of several leaders run at once; every configuration's bound must be the highest term sent
	 * with it, with no update lost and no failed write (the temporary file is shared).
	 */
	@Test
	void concurrentChunksKeepTheHighestTermOfEveryConfiguration(@TempDir Path raftDir) throws Exception {
		int threads = 8;
		int rounds = 100;
		long[] indexes = {40, 41, 42, 43};
		var highest = new ConcurrentHashMap<Long, Long>();
		var failures = Collections.synchronizedList(new ArrayList<Throwable>());
		var barrier = new CyclicBarrier(threads);
		var workers = new ArrayList<Thread>();
		for (int t = 0; t < threads; t++) {
			long seed = t;
			workers.add(Thread.ofPlatform().start(() -> {
				var random = new Random(seed);
				try {
					for (int round = 0; round < rounds; round++) {
						barrier.await(30, TimeUnit.SECONDS);
						for (int k = 0; k < 2; k++) {
							long index = indexes[random.nextInt(indexes.length)];
							long term = 1 + random.nextInt(1000);
							InstalledConfiguration.update(raftDir, 0, term, index, ABX, AB);
							highest.merge(index, term, Math::max);
						}
					}
				} catch (Throwable e) {
					failures.add(e);
					barrier.reset();
				}
			}));
		}
		for (var worker : workers) {
			worker.join(TimeUnit.MINUTES.toMillis(2));
		}
		assertTrue(failures.isEmpty(), () -> "failed updates: " + failures);
		for (Map.Entry<Long, Long> entry : highest.entrySet()) {
			assertEquals(entry.getValue(), bound(raftDir, entry.getKey(), ABX, AB), "bound of index " + entry.getKey());
		}
	}

	/** A configuration evicted by newer ones comes back with a bound not below its old one (the floor). */
	@Test
	void anEvictedConfigurationComesBackWithABoundNotBelowItsOldOne(@TempDir Path raftDir) throws Exception {
		InstalledConfiguration.update(raftDir, 0, 100, 1, ABX, AB);
		for (int i = 2; i <= InstalledConfiguration.CAP + 1; i++) {
			InstalledConfiguration.update(raftDir, 0, 1, i, ABX, AB);
		}
		InstalledConfiguration.update(raftDir, 0, 2, 1, ABX, AB);
		long bound = bound(raftDir, 1, ABX, AB);
		assertTrue(bound >= 100, "bound " + bound);
		assertEquals(1, bound(raftDir, InstalledConfiguration.CAP + 1, ABX, AB));
	}

	/**
	 * A file of the single record of round 13 may hold a bound that R14-1 left too low: a start discards it, and a
	 * configuration recorded after that is bounded by no less than the term Ratis had then (the floor).
	 */
	@Test
	void aStartDiscardsARecordOfTheOldFormat(@TempDir Path raftDir) throws Exception {
		Files.writeString(raftDir.resolve(InstalledConfiguration.FILE), "40\n2\na,b,x\na,b\n");
		InstalledConfiguration.initialize(raftDir, 5);
		assertEquals(RemovalRule.UNKNOWN_TERM, bound(raftDir, 40, ABX, AB));
		InstalledConfiguration.update(raftDir, 0, 2, 40, ABX, AB);
		assertEquals(5, bound(raftDir, 40, ABX, AB));
		// A start keeps a file of this format (Ratis' term is then at least every recorded one).
		InstalledConfiguration.update(raftDir, 0, 7, 41, ABX, AB);
		InstalledConfiguration.initialize(raftDir, 9);
		assertEquals(7, bound(raftDir, 41, ABX, AB));
		assertEquals(5, bound(raftDir, 40, ABX, AB));
	}

	/**
	 * R15 review, findings 2 and 4: a record file deleted or made unreadable while the node runs is rewritten with the
	 * floor given (Ratis' current term, which no chunk it handled exceeds), instead of being read as an empty record
	 * (floor 0: a later stale chunk of a configuration Ratis took before would get its own, lower term) or failing every
	 * install stream until the next start.
	 */
	@Test
	void aLostOrUnreadableFileIsRewrittenWithTheFloorGiven(@TempDir Path raftDir) throws Exception {
		InstalledConfiguration.initialize(raftDir, 0);
		InstalledConfiguration.update(raftDir, 0, 6, 40, ABX, AB);
		Files.delete(raftDir.resolve(InstalledConfiguration.FILE));
		InstalledConfiguration.update(raftDir, 9, 2, 40, ABX, AB);
		assertEquals(9, bound(raftDir, 40, ABX, AB));
		Files.writeString(raftDir.resolve(InstalledConfiguration.FILE), "not a record\n");
		InstalledConfiguration.update(raftDir, 11, 3, 41, AB, List.of());
		assertEquals(11, bound(raftDir, 41, AB, List.of()));
		assertEquals(RemovalRule.UNKNOWN_TERM, bound(raftDir, 40, ABX, AB));
	}

	/**
	 * R15 review, finding 3: a start that could not read Ratis' term writes the floor {@link Long#MAX_VALUE}; the next
	 * start that reads it lowers the floor, and every bound above it, to that term (no chunk Ratis handled exceeds it).
	 */
	@Test
	void aStartLowersAFloorAboveTheTermRatisPersisted(@TempDir Path raftDir) throws Exception {
		InstalledConfiguration.initialize(raftDir, Long.MAX_VALUE);
		InstalledConfiguration.update(raftDir, 0, 3, 40, ABX, AB);
		InstalledConfiguration.update(raftDir, 0, 4, 42, ABX, AB);
		assertEquals(Long.MAX_VALUE, bound(raftDir, 40, ABX, AB));
		// Another start that cannot read the term changes nothing.
		InstalledConfiguration.initialize(raftDir, Long.MAX_VALUE);
		assertEquals(Long.MAX_VALUE, bound(raftDir, 40, ABX, AB));
		InstalledConfiguration.initialize(raftDir, 5);
		assertEquals(5, bound(raftDir, 40, ABX, AB));
		assertEquals(5, bound(raftDir, 42, ABX, AB));
		InstalledConfiguration.update(raftDir, 0, 2, 41, ABX, AB);
		assertEquals(5, bound(raftDir, 41, ABX, AB));
		// A bound below the term stays.
		InstalledConfiguration.update(raftDir, 0, 7, 43, ABX, AB);
		InstalledConfiguration.initialize(raftDir, 8);
		assertEquals(7, bound(raftDir, 43, ABX, AB));
		assertEquals(5, bound(raftDir, 40, ABX, AB));
	}

	/** R15 review, finding 5: a file replaced behind the cached record (another writer) is read again. */
	@Test
	void aFileReplacedBehindTheCacheIsReadAgain(@TempDir Path raftDir) throws Exception {
		InstalledConfiguration.initialize(raftDir, 0);
		InstalledConfiguration.update(raftDir, 0, 3, 40, ABX, AB);
		var other = raftDir.resolve("other");
		Files.writeString(other, "records-v2 1\n41\n8\na,b\n\n", StandardCharsets.UTF_8);
		Files.move(other, raftDir.resolve(InstalledConfiguration.FILE), StandardCopyOption.REPLACE_EXISTING);
		InstalledConfiguration.update(raftDir, 0, 4, 42, ABX, AB);
		assertEquals(8, bound(raftDir, 41, AB, List.of()));
		assertEquals(4, bound(raftDir, 42, ABX, AB));
		assertEquals(RemovalRule.UNKNOWN_TERM, bound(raftDir, 40, ABX, AB));
	}

	/** An older version reads the first line as an index: it must fail there, and so bound nothing. */
	@Test
	void theFileStartsWithATagThatAnOlderVersionCannotRead(@TempDir Path raftDir) throws Exception {
		InstalledConfiguration.initialize(raftDir, 0);
		InstalledConfiguration.update(raftDir, 0, 3, 40, ABX, AB);
		var lines = Files.readAllLines(raftDir.resolve(InstalledConfiguration.FILE), StandardCharsets.UTF_8);
		assertThrows(NumberFormatException.class, () -> Long.parseLong(lines.getFirst().trim()), lines.toString());
	}
}
