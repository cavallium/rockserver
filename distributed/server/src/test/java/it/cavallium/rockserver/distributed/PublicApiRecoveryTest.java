package it.cavallium.rockserver.distributed;

import static it.cavallium.rockserver.distributed.ScenarioSupport.BATCH_CONTEXT;
import static it.cavallium.rockserver.distributed.ScenarioSupport.CLIENT_RETRY_TIMEOUT;
import static it.cavallium.rockserver.distributed.ScenarioSupport.PHASE_TIMEOUT;
import static it.cavallium.rockserver.distributed.ScenarioSupport.READY_TIMEOUT;
import static it.cavallium.rockserver.distributed.ScenarioSupport.SCHEMA;
import static it.cavallium.rockserver.distributed.ScenarioSupport.key;
import static it.cavallium.rockserver.distributed.ScenarioSupport.text;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
 *   uploadMergeOperator}: the copy gets the version the first one created. The registry deduplicates by content, so
 *   that answer would be the same after a second apply: the test also checks that no replica logged the upload as
 *   applied again ("already exists"), with the registry's log at info on every node.</li>
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

	private static final String REGISTRY_LOG_LEVEL =
			"org.slf4j.simpleLogger.log.it.cavallium.rockserver.core.impl.MergeOperatorRegistry";

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
		cluster = ProcessCluster.create(root.resolve("cluster"), 3, 0, List.of(
				"--rockserver-config", rockserverConfig.toString(),
				"--election-timeout-ms", "400:800",
				"--request-timeout-ms", "5000"));
		for (var id : cluster.members()) {
			// The registry logs every upload it applies, also one it answers with an existing version: that line is
			// how the upload test sees a second apply (see aMergeOperatorUploadWhoseReplyIsLostAnswersFromItsRecord).
			cluster.setJvmOptions(id, List.of("-D" + REGISTRY_LOG_LEVEL + "=info"));
			cluster.start(id);
		}
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
		openSession(api);

		cluster.clientLink(leader).dropReplies();
		CompletableFuture<Boolean> deletion = api.deleteColumnIfExistsAsync("doomed");
		ScenarioSupport.await(PHASE_TIMEOUT, () -> !client.getColumnId("doomed", BATCH_CONTEXT).acknowledged(),
				"the deletion did not apply");
		awaitLostReply(leader);
		assertFalse(deletion.isDone(), "the deletion was answered although its reply was lost");
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
		openSession(api);

		cluster.clientLink(leader).dropReplies();
		CompletableFuture<Long> upload = api.uploadMergeOperatorAsync("op", OPERATOR_CLASS, jar);
		Long[] stored = {null};
		ScenarioSupport.await(PHASE_TIMEOUT, () -> {
			var check = client.read(BATCH_CONTEXT, other -> other.checkMergeOperator("op", hash));
			return check.acknowledged() && (stored[0] = check.value()) != null;
		}, "the upload did not apply");
		awaitLostReply(leader);
		assertFalse(upload.isDone(), "the upload was answered although its reply was lost");
		// A later version under the same name: the copy must still answer with the first one's.
		var second = jar("second");
		long later = client.write(BATCH_CONTEXT, other -> other.uploadMergeOperator("op", OPERATOR_CLASS, second))
				.valueOrThrow();
		assertEquals(stored[0] + 1, later);
		support.awaitQuiescent();
		recover(recovery, leader);

		assertEquals(stored[0], upload.get(CALL_TIMEOUT.toSeconds(), TimeUnit.SECONDS));
		// The registry deduplicates by content, so a copy applied again would also answer stored[0]: the answer alone
		// does not tell. Every replica that applies an upload logs it, a repeated content as "already exists".
		support.awaitQuiescent();
		var applied = "Uploaded merge operator 'op' version " + stored[0] + " ";
		var reapplied = "Merge operator 'op' with same content already exists as version " + stored[0] + ".";
		assertTrue(cluster.members().stream().anyMatch(id -> cluster.output(id).contains(applied)),
				"no node logged the first apply: the check below would see nothing");
		for (var id : cluster.members()) {
			assertFalse(cluster.output(id).contains(reapplied), () -> "node " + id + " applied the upload again");
		}
		// Through the test's own client, whatever node it used last (review of R62, 5): when that is the killed leader,
		// the client finds its port closed and takes the upload, a numbered write, to the next node with nothing sent.
		// Before, the core client sent it again to that one refused address until the call's deadline and ended it as
		// of unknown outcome, one run in three (ClusterClientNumberedFailoverTest).
		assertEquals(later, client.write(BATCH_CONTEXT, other -> other.uploadMergeOperator("op", OPERATOR_CLASS, second))
				.valueOrThrow(), "the same content keeps its version");
	}

	/**
	 * Opens the client session of {@code api}'s connection, with a numbered write that changes nothing (R62-5). The
	 * client opens its session with its first numbered write (a schema change or an upload is one, a column lookup is
	 * not). Without this the first message through the lossy link was {@code openClientSession}: its reply was the one
	 * lost, the call ran to its deadline, the channel moved to a follower, and the write under test went there and was
	 * answered, with no reply of its own lost.
	 */
	private static void openSession(it.cavallium.rockserver.core.common.RocksDBAsyncAPI api) throws Exception {
		assertEquals(Boolean.FALSE, api.deleteColumnIfExistsAsync("never-created").get(20, TimeUnit.SECONDS));
	}

	/**
	 * Waits until the leader's client link dropped bytes of a reply. The caller saw the write applied through another
	 * client, which can be a moment before the leader's own reply reaches the link.
	 */
	private void awaitLostReply(String leader) {
		ScenarioSupport.await(Duration.ofSeconds(10), () -> cluster.clientLink(leader).hasLostReplies(),
				"the reply was not lost");
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
