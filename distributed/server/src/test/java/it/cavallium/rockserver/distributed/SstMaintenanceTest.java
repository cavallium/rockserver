package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RawScanEvent;
import it.cavallium.rockserver.core.common.RawSstToken;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.SstMaintenance;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * CR-8, CR-9, DD-39: the SST maintenance calls and {@code scanRaw} describe one database, the leader's, whichever node
 * receives them; raw-scan tokens carry the node and term that issued them, and other ones are ignored.
 */
class SstMaintenanceTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(30);
	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);

	@TempDir
	Path temp;
	private InProcessCluster cluster;

	@AfterEach
	void tearDown() throws Exception {
		if (cluster != null) {
			cluster.close();
		}
	}

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}

	@Test
	void rawScanTokensAndMaintenanceCallsReferToTheLeadersDatabase() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var follower = cluster.anyFollower();
		var context = RequestContext.batch(Duration.ofSeconds(20));
		RocksDBSyncAPI viaLeader = leader.connection().getSyncApi(context);
		RocksDBSyncAPI viaFollower = follower.connection().getSyncApi(context);
		long column = viaLeader.createColumn("c", SCHEMA);
		for (long i = 0; i < 100; i++) {
			viaLeader.put(0, column, key(i), Buf.wrap(new byte[100]), RequestType.none());
		}
		// Flush acts on the node that receives it: the leader's files.
		viaLeader.flush();

		long term = leader.stateMachine().division().getInfo().getCurrentTerm();
		String origin = Router.sstTokenOrigin(leader.config().nodeId(), term);
		var first = viaFollower.scanRawResumable(column, 0, 1, Set.of()).toList();
		var tokens = tokens(first);
		assertFalse(tokens.isEmpty(), "the flushed table was scanned");
		for (var token : tokens) {
			assertEquals(origin, token.origin(), token::toString);
		}
		assertTrue(batches(first) > 0);

		// Its own tokens skip the tables they name, through any node.
		assertEquals(0, batches(viaFollower.scanRawResumable(column, 0, 1, tokens).toList()));
		assertEquals(0, batches(viaLeader.scanRawResumable(column, 0, 1, tokens).toList()));
		// Tokens of another node or term, or untagged ones, name files of another database: the tables are scanned again.
		var otherTerm = retag(tokens, Router.sstTokenOrigin(leader.config().nodeId(), term + 1));
		assertEquals(batches(first), batches(viaLeader.scanRawResumable(column, 0, 1, otherTerm).toList()));
		var otherNode = retag(tokens, Router.sstTokenOrigin(follower.config().nodeId(), term));
		assertEquals(batches(first), batches(viaLeader.scanRawResumable(column, 0, 1, otherNode).toList()));
		var untagged = tokens.stream().map(RawSstToken::untagged).collect(Collectors.toSet());
		assertEquals(batches(first), batches(viaLeader.scanRawResumable(column, 0, 1, untagged).toList()));

		// Over gRPC, through the follower: a client that does not accept tagged tokens (built before them) gets plain
		// filenames, which it can parse; one that does gets the leader's tags.
		assertEquals(untagged, grpcTokens(follower, column, false));
		assertEquals(tokens, grpcTokens(follower, column, true));

		// The metadata comes from the leader, whichever node is asked: the same session and files.
		var fromLeader = viaLeader.getSstMetadata(column, -1);
		var fromFollower = viaFollower.getSstMetadata(column, -1);
		assertEquals(fromLeader.session(), fromFollower.session());
		assertEquals(fromLeader.files().stream().map(SstMaintenance.File::name).collect(Collectors.toSet()),
				fromFollower.files().stream().map(SstMaintenance.File::name).collect(Collectors.toSet()));
		assertNotNull(viaFollower.getTableProperties(column));

		// The internal column is refused to compactFiles too.
		long internal = leader.stateMachine().store().metaColumnId();
		var refused = assertThrows(RocksDBException.class, () -> viaFollower.compactFiles(new SstMaintenance.Request(
				internal, fromLeader.session(), List.of("000001.sst"), 1, 0, 1 << 20, 1 << 20, 1, false)));
		assertEquals(RocksDBException.RocksDBErrorType.INVALID_REQUEST, refused.getErrorUniqueId(), refused::toString);
	}

	@Test
	void tokensKeepTheirFileNameUnderAnOriginTag() {
		var file = new RawSstToken("/000123.sst");
		assertNull(file.origin());
		var tagged = RawSstToken.tagged("n1~t7", file);
		assertEquals("n1~t7@/000123.sst", tagged.value());
		assertEquals("n1~t7", tagged.origin());
		assertEquals(file, tagged.untagged());
		assertEquals(tagged, RawSstToken.tagged("n1~t7", RawSstToken.tagged("other", file)));
		for (var invalid : List.of("@/000123.sst", "n 1@/000123.sst", "n1@n2@/000123.sst", "n1@", "n1@/000000.sst",
				"x".repeat(RawSstToken.MAX_ORIGIN_CHARACTERS + 1) + "@/000123.sst")) {
			assertThrows(IllegalArgumentException.class, () -> new RawSstToken(invalid), invalid);
		}
		assertEquals("n1~t7", Router.sstTokenOrigin("n1", 7));
		assertEquals("node~2fa~t3", Router.sstTokenOrigin("node/a", 3));
		var longId = Router.sstTokenOrigin("n".repeat(500), 3);
		assertTrue(longId.length() <= RawSstToken.MAX_ORIGIN_CHARACTERS, longId);
		RawSstToken.tagged(longId, file);
	}

	private static Set<RawSstToken> tokens(List<RawScanEvent> events) {
		var tokens = new HashSet<RawSstToken>();
		for (var event : events) {
			switch (event) {
				case RawScanEvent.Batch batch when batch.completedSstToken() != null -> tokens.add(batch.completedSstToken());
				case RawScanEvent.SstCompleted completed -> tokens.add(completed.token());
				default -> {
				}
			}
		}
		return tokens;
	}

	private static long batches(List<RawScanEvent> events) {
		return events.stream().filter(RawScanEvent.Batch.class::isInstance).count();
	}

	private static Set<RawSstToken> grpcTokens(ReplicatedNode node, long column, boolean acceptsTagged) {
		var channel = io.grpc.netty.NettyChannelBuilder.forAddress(node.config().self().apiHost(),
				node.config().self().apiPort()).usePlaintext().build();
		try {
			var context = it.cavallium.rockserver.core.common.api.proto.RequestContext.newBuilder()
					.setProfile(it.cavallium.rockserver.core.common.api.proto.WorkloadProfile.BATCH)
					.setWorkloadContractVersion(3)
					.setTimeoutNanos(Duration.ofSeconds(20).toNanos())
					.build();
			var request = it.cavallium.rockserver.core.common.api.proto.ScanRawRequest.newBuilder()
					.setColumnId(column)
					.setShardCount(1)
					.setContext(context)
					.setResumable(true)
					.setAcceptsTaggedSstTokens(acceptsTagged)
					.build();
			var responses = it.cavallium.rockserver.core.common.api.proto.RocksDBServiceGrpc.newBlockingStub(channel)
					.scanRaw(request);
			var result = new HashSet<RawSstToken>();
			while (responses.hasNext()) {
				var response = responses.next();
				if (response.hasCompletedSstTokenAfterBatch()) {
					// The constructor of the clients built before tagged tokens accepts only plain filenames.
					result.add(new RawSstToken(response.getCompletedSstTokenAfterBatch()));
				}
				if (response.getEventCase() == it.cavallium.rockserver.core.common.api.proto.ScanRawResponse.EventCase.COMPLETEDSSTTOKEN) {
					result.add(new RawSstToken(response.getCompletedSstToken()));
				}
			}
			return result;
		} finally {
			channel.shutdownNow();
		}
	}

	private static Set<RawSstToken> retag(Set<RawSstToken> tokens, String origin) {
		return tokens.stream().map(token -> RawSstToken.tagged(origin, token)).collect(Collectors.toSet());
	}
}
