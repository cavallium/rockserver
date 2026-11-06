package it.cavallium.rockserver.distributed;

import static it.cavallium.rockserver.distributed.ScenarioSupport.BATCH_CONTEXT;
import static it.cavallium.rockserver.distributed.ScenarioSupport.CLIENT_RETRY_TIMEOUT;
import static it.cavallium.rockserver.distributed.ScenarioSupport.PHASE_TIMEOUT;
import static it.cavallium.rockserver.distributed.ScenarioSupport.READY_TIMEOUT;
import static it.cavallium.rockserver.distributed.ScenarioSupport.SCHEMA;
import static it.cavallium.rockserver.distributed.ScenarioSupport.key;
import static it.cavallium.rockserver.distributed.ScenarioSupport.text;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Salvaged from the old PublicQuorum suite, on a 3-node process cluster and the public gRPC client
 * ({@link GrpcConnection} with every node's address, client sessions on):
 *
 * <ul>
 *   <li>RT-70: a schema change whose reply is lost is recovered through its request id. The client's link to the
 *   leader drops the reply of {@code deleteColumnIfExists} (the leader commits and applies it, nothing fails); once it
 *   applied, a column of the same name is created again by another client, then the link is reset: the client library
 *   sends the request again with the same id, and the answer comes from the record of the first one ({@code true}),
 *   while the new column stays. In the {@code kill} variant the leader is SIGKILLed instead of the link being reset,
 *   so the copy reaches a new leader, which answers from the replicated record. The same for {@code
 *   uploadMergeOperator}: the copy gets the version the first one created.</li>
 *   <li>RT-72: a transaction opened through a follower is bound to the leader of that moment. The leader is
 *   SIGKILLed; the commit through the follower is refused as retryable (never rebased onto the new leader), and its
 *   writes are absent, also a while later.</li>
 * </ul>
 *
 * <p>Multi-process: part of the heavy suites.</p>
 */
@Timeout(300)
class PublicApiRecoveryTest {

	private static final Duration CALL_TIMEOUT = Duration.ofSeconds(40);
	private static final String OPERATOR_CLASS = "it.cavallium.rockserver.core.impl.MyStringAppendOperator";

	@TempDir
	Path root;
	private ProcessCluster cluster;
	private ClusterClient client;
	private GrpcConnection connection;
	private ScenarioSupport support;

	@AfterEach
	void tearDown() {
		if (connection != null) {
			connection.close();
		}
		if (client != null) {
			client.close();
		}
		if (cluster != null) {
			cluster.close();
		}
	}

	private void start() throws Exception {
		var rockserverConfig = root.resolve("rockserver.conf");
		Files.writeString(rockserverConfig, InProcessCluster.MERGE_CONFIG);
		cluster = ProcessCluster.start(root.resolve("cluster"), 3, List.of(
				"--rockserver-config", rockserverConfig.toString(),
				"--election-timeout-ms", "400:800",
				"--request-timeout-ms", "5000"));
		cluster.waitReady(READY_TIMEOUT);
		client = new ClusterClient(cluster.apiAddresses(), CLIENT_RETRY_TIMEOUT, false);
		support = new ScenarioSupport(cluster);
	}

	/** The public client, through the client links, the leader's first. */
	private GrpcConnection publicClient(String leader) {
		var addresses = new ArrayList<HostAndPort>();
		addresses.add(cluster.proxiedApiAddress(leader));
		for (var id : cluster.members()) {
			if (!id.equals(leader)) {
				addresses.add(cluster.proxiedApiAddress(id));
			}
		}
		return GrpcConnection.forHostsAndPorts("public-recovery", addresses);
	}

	@ParameterizedTest(name = "{0}")
	@ValueSource(strings = {"reset", "kill"})
	void aDeleteColumnWhoseReplyIsLostAnswersFromItsRecord(String recovery) throws Exception {
		start();
		long doomed = client.createColumn("doomed", SCHEMA, BATCH_CONTEXT).valueOrThrow();
		var leader = support.awaitLeader(PHASE_TIMEOUT);
		connection = publicClient(leader);
		var api = connection.getAsyncApi(RequestContext.batch(CALL_TIMEOUT));
		// The connection to the leader is open before its link starts losing replies.
		assertEquals(doomed, api.getColumnIdAsync("doomed").get(10, TimeUnit.SECONDS));

		cluster.clientLink(leader).dropReplies();
		CompletableFuture<Boolean> deletion = api.deleteColumnIfExistsAsync("doomed");
		ScenarioSupport.await(PHASE_TIMEOUT, () -> !client.getColumnId("doomed", BATCH_CONTEXT).acknowledged(),
				"the deletion did not apply");
		assertTrue(cluster.clientLink(leader).hasLostReplies(), "the reply was not lost");
		// Another client creates the name again: a copy that ran again would delete it.
		long recreated = client.createColumn("doomed", SCHEMA, BATCH_CONTEXT).valueOrThrow();
		assertTrue(recreated != doomed);
		support.awaitQuiescent();
		recover(recovery, leader);

		assertEquals(Boolean.TRUE, deletion.get(CALL_TIMEOUT.toSeconds(), TimeUnit.SECONDS),
				"the copy answers as the first deletion did");
		assertEquals(recreated, client.getColumnId("doomed", BATCH_CONTEXT).valueOrThrow(),
				"the column created after the deletion is still there: the deletion applied once");
	}

	@ParameterizedTest(name = "{0}")
	@ValueSource(strings = {"reset", "kill"})
	void aMergeOperatorUploadWhoseReplyIsLostAnswersFromItsRecord(String recovery) throws Exception {
		start();
		var leader = support.awaitLeader(PHASE_TIMEOUT);
		connection = publicClient(leader);
		var api = connection.getAsyncApi(RequestContext.batch(CALL_TIMEOUT));
		var jar = jar("first");
		var hash = MessageDigest.getInstance("SHA-256").digest(jar);
		assertNull(api.checkMergeOperatorAsync("op", hash).get(10, TimeUnit.SECONDS));

		cluster.clientLink(leader).dropReplies();
		CompletableFuture<Long> upload = api.uploadMergeOperatorAsync("op", OPERATOR_CLASS, jar);
		Long[] stored = {null};
		ScenarioSupport.await(PHASE_TIMEOUT, () -> {
			var check = client.read(BATCH_CONTEXT, other -> other.checkMergeOperator("op", hash));
			return check.acknowledged() && (stored[0] = check.value()) != null;
		}, "the upload did not apply");
		// A later version under the same name: the copy must still answer with the first one's.
		var second = jar("second");
		long later = client.write(BATCH_CONTEXT, other -> other.uploadMergeOperator("op", OPERATOR_CLASS, second))
				.valueOrThrow();
		assertEquals(stored[0] + 1, later);
		support.awaitQuiescent();
		recover(recovery, leader);

		assertEquals(stored[0], upload.get(CALL_TIMEOUT.toSeconds(), TimeUnit.SECONDS));
		assertEquals(later, client.write(BATCH_CONTEXT, other -> other.uploadMergeOperator("op", OPERATOR_CLASS,
				second)).valueOrThrow(), "the same content keeps its version");
	}

	/** Ends the lost-reply state: the link is reset (the leader stays), or the leader is killed. */
	private void recover(String recovery, String leader) throws Exception {
		if (recovery.equals("kill")) {
			cluster.kill(leader);
			cluster.clientLink(leader).unblock();
			var newLeader = support.awaitLeader(PHASE_TIMEOUT);
			assertTrue(!newLeader.equals(leader));
		} else {
			// Resets the connection that lost bytes: the client sees it fail and sends the request again.
			cluster.clientLink(leader).unblock();
		}
	}

	@Test
	void aTransactionOpenedThroughAFollowerIsRefusedAfterItsLeaderIsKilled() throws Exception {
		start();
		long column = client.createColumn("c", SCHEMA, BATCH_CONTEXT).valueOrThrow();
		var leader = support.awaitLeader(PHASE_TIMEOUT);
		long term = support.lastEntryTerm(leader);
		var follower = cluster.members().stream().filter(id -> !id.equals(leader)).findFirst().orElseThrow();
		connection = GrpcConnection.forHostAndPort("follower-transaction", cluster.apiAddress(follower));
		var api = connection.getSyncApi(RequestContext.batch(Duration.ofSeconds(20)));
		long transaction = api.openTransaction(Duration.ofSeconds(60));
		api.put(transaction, column, key(1), text("in the transaction"), RequestType.none());
		api.put(transaction, column, key(2), text("in the transaction"), RequestType.none());

		cluster.kill(leader);
		var newLeader = support.awaitLeader(PHASE_TIMEOUT);
		assertTrue(support.lastEntryTerm(newLeader) > term);
		var refused = assertThrows(RocksDBException.class, () -> api.closeTransaction(transaction, true),
				"a transaction of the killed leader was committed by the new one");
		System.out.println("Commit after the leader's kill: " + refused);
		assertInstanceOf(RocksDBException.class, refused);
		assertTrue(ClusterClient.isRetryable(refused) || refused.isNotApplied(),
				() -> "the refusal must say that running the transaction again is safe: " + refused);

		// Absent now, and later: nothing rebased the transaction or applied it late.
		for (int check = 0; check < 2; check++) {
			for (long k = 1; k <= 2; k++) {
				var read = client.get(column, key(k), ScenarioSupport.READ_CONTEXT);
				assertEquals(OpStatus.OK, read.status(), String.valueOf(read.error()));
				assertNull(read.value(), "key " + k + " of the refused transaction");
			}
			Thread.sleep(2_000);
		}
		cluster.start(leader);
		cluster.waitReady(leader, READY_TIMEOUT);
		support.awaitCaughtUp(leader);
		// The same transaction run again, through the same follower, commits.
		long again = api.openTransaction(Duration.ofSeconds(60));
		api.put(again, column, key(1), text("again"), RequestType.none());
		assertTrue(api.closeTransaction(again, true));
		var read = client.get(column, key(1), ScenarioSupport.READ_CONTEXT);
		assertNotNull(read.value());
		assertEquals("again", ScenarioSupport.text(read.value()));
	}

	/** A jar with one entry naming {@code tag}: jars of different tags differ, so the registry stores both. */
	private static byte[] jar(String tag) throws IOException {
		var out = new ByteArrayOutputStream();
		try (var jar = new JarOutputStream(out, new Manifest())) {
			jar.putNextEntry(new ZipEntry("tag.txt"));
			jar.write(tag.getBytes(StandardCharsets.UTF_8));
			jar.closeEntry();
		}
		return out.toByteArray();
	}
}
