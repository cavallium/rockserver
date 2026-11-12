package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.unimi.dsi.fastutil.ints.IntList;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.proto.RaftProtos.RaftPeerProto;
import org.apache.ratis.server.raftlog.LogProtoUtils;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

/**
 * What a node does at its start when {@code raft-meta.conf} holds the bytes of two configuration entries (R67, row
 * R66-3).
 *
 * <p>Ratis 3.3.1 writes that file as one bare {@code LogEntryProto} (no length prefix) through a temporary file with a
 * fixed name, from two threads that share no lock (design: "Upstream", R66). If both open the temporary file before
 * either writes, each writes from offset 0 of the same inode, so the file ends as the later writer's bytes followed by
 * the rest of the earlier writer's, when that one is longer: B followed by A[|B|..]. It is read with
 * {@code LogEntryProto.Builder.mergeFrom(InputStream)}, which merges instead of failing on what it can skip: a
 * field of an unexpected wire type is skipped as an unknown field, and a repeated field of two parts concatenates. This
 * test writes such files into the stopped node's {@code raft-meta.conf} and pins what a start does with each:</p>
 * <ul>
 * <li>a file that does not parse, or that parses to a configuration Ratis rejects (a peer id twice): exit 1,
 * {@code RaftServerProxy has failed};</li>
 * <li>a file that parses (B followed by a tail of A that the parser skips) is read as B exactly, a complete
 * configuration that one of the two writers wrote. If the log of the node still holds a configuration entry with an
 * index at or above B's, that entry wins at the start (the log is replayed after the file, and the highest index is
 * the current configuration), and the node serves with the right one.</li>
 * </ul>
 *
 * <p>Multi-process: part of the heavy suites.</p>
 */
@Timeout(600)
class MixedConfigurationFileTest {

	private static final List<String> SERVER_ARGS = List.of(
			"--election-timeout-ms", "400:800",
			"--request-timeout-ms", "3000");
	private static final Duration READY_TIMEOUT = Duration.ofSeconds(60);
	private static final RequestContext REQUEST = RequestContext.batch(Duration.ofSeconds(20));
	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	private static final int SERVING = -1;

	// Kept on failure: the node logs and data directories are the evidence.
	@TempDir(cleanup = CleanupMode.ON_SUCCESS)
	Path root;
	private ProcessCluster cluster;

	@AfterEach
	void tearDown() {
		if (cluster != null) {
			cluster.close();
		}
	}

	@Test
	void aMixOfTwoConfigurationEntriesEitherStopsTheNodeAtItsStartOrIsOneOfThem() throws Exception {
		cluster = ProcessCluster.start(root.resolve("cluster"), 3, ProcessCluster.pipelined(SERVER_ARGS));
		cluster.waitReady(READY_TIMEOUT);
		var leader = ClientFailoverTest.awaitLeader(cluster);
		var victim = cluster.nodeIds().stream().filter(id -> !id.equals(leader)).findFirst().orElseThrow();
		long column;
		try (var connection = GrpcConnection.forHostAndPort("before", cluster.apiAddress(leader))) {
			var api = connection.getSyncApi(REQUEST);
			column = api.createColumn("data", SCHEMA);
			api.put(0, column, key(1), Buf.wrap(new byte[] {1}), RequestType.none());
		}
		Path conf;
		try (var files = Files.walk(cluster.dataDir(victim))) {
			conf = files.filter(file -> file.getFileName().toString().equals("raft-meta.conf")).findFirst()
					.orElseThrow(() -> new AssertionError("no raft-meta.conf under " + cluster.dataDir(victim)));
		}
		// A leader change makes every node apply a configuration entry of the new term, so that the file holds an entry
		// with a term and an index, as in a running cluster (the file of a new node has index 0).
		var next = cluster.nodeIds().stream().filter(id -> !id.equals(leader) && !id.equals(victim)).findFirst().orElseThrow();
		cluster.admin(cluster.nodeIds(), "transfer-leader", next);
		long deadline = System.nanoTime() + READY_TIMEOUT.toNanos();
		while (LogEntryProto.parseFrom(Files.readAllBytes(conf)).getIndex() == 0) {
			assertTrue(System.nanoTime() - deadline < 0, () -> "the victim never wrote a configuration entry\n" + cluster.logTail(victim));
			Thread.sleep(50);
		}
		try (var connection = GrpcConnection.forHostAndPort("down", cluster.apiAddress(next))) {
			connection.getSyncApi(REQUEST).put(0, column, key(2), Buf.wrap(new byte[] {2}), RequestType.none());
		}
		cluster.stop(victim);
		byte[] original = Files.readAllBytes(conf);
		var real = LogEntryProto.parseFrom(original);
		assertTrue(real.hasConfigurationEntry(), "the node's own file is a configuration entry");
		int peers = real.getConfigurationEntry().getPeersCount();
		assertEquals(3, peers);

		// Configurations of this cluster as Ratis would have written them: the node's own, longer ones (a membership
		// change adds a peer, a joint one holds both halves), and two voters without the victim.
		var peerList = real.getConfigurationEntry().getPeersList();
		var extras = new ArrayList<RaftPeerProto>();
		var longers = new ArrayList<byte[]>();
		for (int i = 0; i < 3; i++) {
			extras.add(peerList.get(0).toBuilder().setId(ByteString.copyFromUtf8("x" + i)).setAddress("127.0.0.1:" + (i + 1)).build());
			var cur = real.getConfigurationEntry().toBuilder().addAllPeers(extras);
			longers.add(with(real, real.getIndex(), cur).toByteArray());
			longers.add(with(real, real.getIndex(), cur.clone().addAllOldPeers(peerList)).toByteArray());
		}
		var others = new ArrayList<RaftPeerProto>(peerList);
		others.removeIf(peer -> peer.getId().toStringUtf8().equals(victim));
		byte[] stable = real.toByteArray();
		// Entries of two voters without the victim, at an index below and one far above the end of its log.
		var wrongLow = new ArrayList<byte[]>();
		var wrongHigh = new ArrayList<byte[]>();
		for (int i = 0; i < 40; i++) {
			wrongLow.add(with(real, 1 + i, real.getConfigurationEntry().toBuilder().clearPeers().addAllPeers(others)).toByteArray());
			wrongHigh.add(with(real, real.getIndex() + 100_000 + i, real.getConfigurationEntry().toBuilder().clearPeers()
					.addAllPeers(others)).toByteArray());
		}

		var mixes = new LinkedHashMap<String, byte[]>();
		mixes.put("empty", new byte[0]);
		mixes.put("cut in the middle", Arrays.copyOf(stable, stable.length / 2));
		mixes.put("cut by 9 bytes", Arrays.copyOf(stable, stable.length - 9));
		mixes.put("two entries one after the other", cat(stable, stable));
		mixes.put("one entry, then a longer one", cat(stable, longers.get(0)));
		mixes.put("the node's own over a longer one that does not parse", overThatDoesNot(List.of(stable), longers, true));
		mixes.put("the node's own over a longer one", overThatDoesNot(List.of(stable), longers, false));
		mixes.put("two voters without the victim, index 1 to 40, over a longer one", overThatDoesNot(wrongLow, longers, false));
		mixes.put("two voters without the victim, index above the log, over a longer one", overThatDoesNot(wrongHigh, longers, false));
		var results = new LinkedHashMap<String, String>();
		for (var mix : mixes.entrySet()) {
			Files.write(conf, mix.getValue());
			String parsed = parse(mix.getValue());
			boolean parses = !parsed.startsWith("unparsable");
			cluster.start(victim);
			int outcome = outcome(victim);
			results.put(mix.getKey(), (outcome == SERVING ? "serves" : "exit " + outcome) + "; parsed: " + parsed);
			System.out.println("MixedConfigurationFileTest: " + mix.getKey() + ": " + results.get(mix.getKey()));
			assertEquals(parses ? SERVING : 1, outcome, () -> mix.getKey() + ": " + parsed + "\n" + cluster.logTail(victim));
			if (parses) {
				assertServes(victim, column);
			} else {
				assertTrue(Files.readString(cluster.stderrLog(victim)).contains("RaftServerProxy has failed"),
						() -> mix.getKey() + "\n" + cluster.logTail(victim));
			}
			cluster.stop(victim);
			Files.write(conf, original);
		}
		// The file put back: the node starts and serves.
		cluster.start(victim);
		cluster.waitReady(victim, READY_TIMEOUT);
		assertServes(victim, column);
		results.forEach((name, result) -> System.out.println("RESULT " + name + " => " + result));
	}

	/**
	 * Over every overlay of an entry of {@code written} on one of {@code earlier} (the earlier writer's, longer
	 * entry), the first that parses ({@code !cutsTail}) or the first that does not.
	 */
	private static byte[] overThatDoesNot(List<byte[]> written, List<byte[]> earlier, boolean cutsTail) {
		for (var w : written) {
			for (var e : earlier) {
				if (e.length > w.length && parse(over(w, e)).startsWith("unparsable") == cutsTail) {
					return over(w, e);
				}
			}
		}
		throw new AssertionError("no overlay " + (cutsTail ? "fails" : "parses"));
	}

	/** The node answers reads and writes, has applied what the others committed, and the three agree. */
	private void assertServes(String victim, long column) throws Exception {
		cluster.waitReady(victim, READY_TIMEOUT);
		try (var connection = GrpcConnection.forHostAndPort("after", cluster.apiAddress(victim))) {
			var api = connection.getSyncApi(REQUEST);
			for (int i = 1; i <= 2; i++) {
				Buf value = api.get(0, column, key(i), RequestType.current());
				assertEquals(i, value == null ? -1 : value.toByteArray()[0], "key " + i + " read through node " + victim);
			}
		}
		long deadline = System.nanoTime() + READY_TIMEOUT.toNanos();
		while (true) {
			var status = cluster.adminResult(cluster.nodeIds(), "status");
			if (status.exitCode() == 0 && ColdRestartTest.convergedIndex(cluster.nodeIds(), status.output()) != null) {
				return;
			}
			assertTrue(System.nanoTime() - deadline < 0, () -> "The nodes did not reach the same applied index:\n"
					+ status.output() + cluster.logTail(victim));
			Thread.sleep(100);
		}
	}

	/**
	 * Every overlay of a shorter entry on a longer one, over entries of several terms, indexes (varints of one to four
	 * bytes), voter sets, joint halves and listeners, is unparsable or the shorter entry exactly: the part of the longer
	 * one that is left is skipped by the parser as unknown fields or makes it fail. Never a third configuration.
	 */
	@Test
	void anOverlayOfTwoEntriesIsNeverAThirdConfiguration() {
		var peers = new ArrayList<RaftPeerProto>();
		for (int i = 1; i <= 5; i++) {
			peers.add(RaftPeerProto.newBuilder().setId(ByteString.copyFromUtf8("" + i)).setAddress("node-" + i + ":" + (28000 + 2 * i))
					.build());
		}
		var listener = RaftPeerProto.newBuilder().setId(ByteString.copyFromUtf8("7")).setAddress("node-7:28014")
				.setStartupRole(org.apache.ratis.proto.RaftProtos.RaftPeerRole.LISTENER).build();
		var sets = List.of(peers.subList(0, 3), peers.subList(1, 3), peers.subList(0, 2), peers.subList(0, 4), peers, peers.subList(2, 3));
		long[] terms = {1, 2, 3, 200};
		long[] indexes = {0, 1, 5, 127, 128, 300, 16383, 16384, 100000, 2097152};
		int unparsable = 0;
		int shorter = 0;
		for (long ta : terms) {
			for (long tb : terms) {
				for (long ia : indexes) {
					for (long ib : indexes) {
						for (var sa : sets) {
							for (var sb : sets) {
								for (int variant = 0; variant < 4; variant++) {
									boolean jointA = (variant & 1) != 0;
									boolean listenersA = (variant & 2) != 0;
									byte[] earlier = entry(ta, ia, sa, jointA ? peers.subList(0, 3) : List.of(), listenersA ? listener : null);
									byte[] written = entry(tb, ib, sb, variant == 3 ? peers.subList(0, 3) : List.of(), null);
									if (earlier.length <= written.length) {
										continue;
									}
									String result = parse(over(written, earlier));
									if (result.startsWith("unparsable")) {
										unparsable++;
									} else {
										assertEquals(parse(written), result, "an overlay parses as a third configuration");
										shorter++;
									}
								}
							}
						}
					}
				}
			}
		}
		System.out.println("MixedConfigurationFileTest overlays: " + unparsable + " unparsable, " + shorter
				+ " exactly the entry that was written over");
		assertTrue(unparsable > 1000 && shorter > 1000, "the sweep covers both outcomes: " + unparsable + ", " + shorter);
	}

	private static byte[] entry(long term, long index, List<RaftPeerProto> voters, List<RaftPeerProto> oldVoters,
			RaftPeerProto listener) {
		var configuration = org.apache.ratis.proto.RaftProtos.RaftConfigurationProto.newBuilder().addAllPeers(voters)
				.addAllOldPeers(oldVoters);
		if (listener != null) {
			configuration.addListeners(listener).addOldListeners(listener);
		}
		return LogEntryProto.newBuilder().setTerm(term).setIndex(index).setConfigurationEntry(configuration).build().toByteArray();
	}

	/** SERVING once the node printed READY, else its exit code. */
	private int outcome(String id) {
		long deadline = System.nanoTime() + READY_TIMEOUT.toNanos();
		while (System.nanoTime() - deadline < 0) {
			try {
				cluster.waitReady(id, Duration.ofMillis(200));
				return SERVING;
			} catch (IllegalStateException e) {
				if (!cluster.isRunning(id)) {
					return cluster.exitCode(id);
				}
			}
		}
		throw new AssertionError("node " + id + " neither exited nor printed READY\n" + cluster.logTail(id));
	}

	private static LogEntryProto with(LogEntryProto base, long index,
			org.apache.ratis.proto.RaftProtos.RaftConfigurationProto.Builder configuration) {
		return base.toBuilder().setIndex(index).setConfigurationEntry(configuration).build();
	}

	/** What two overlapping writes from offset 0 leave: {@code written} (the later one), then the rest of {@code earlier}. */
	private static byte[] over(byte[] written, byte[] earlier) {
		assertTrue(earlier.length > written.length, "the earlier writer's entry is the longer one");
		return cat(written, Arrays.copyOfRange(earlier, written.length, earlier.length));
	}

	private static byte[] cat(byte[]... parts) {
		var out = new ByteArrayOutputStream();
		for (var part : parts) {
			out.writeBytes(part);
		}
		return out.toByteArray();
	}

	/** What Ratis' {@code readRaftConfiguration} makes of the bytes. */
	static String parse(byte[] bytes) {
		try {
			var entry = LogEntryProto.newBuilder().mergeFrom(new ByteArrayInputStream(bytes)).build();
			return "term " + entry.getTerm() + " index " + entry.getIndex() + " " + LogProtoUtils.toRaftConfiguration(entry);
		} catch (Throwable e) {
			return "unparsable (" + e.getClass().getSimpleName() + ")";
		}
	}

	private static Keys key(long value) {
		return new Keys(Buf.wrap(java.nio.ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}
}
