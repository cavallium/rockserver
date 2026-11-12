package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.server.GrpcServer;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.IntConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@code copy-columns} from a real single-node Rockserver (gRPC) to a real 3-node in-process replicated cluster: the
 * command line is run through {@link ReplicatedServerMain#main}, with the exit replaced as in {@link CommandLineTest}.
 */
@Timeout(180)
class CopyColumnsTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(30);
	private static final ColumnSchema FIXED = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	/** 256 buckets for thousands of keys: every bucket holds several entries. */
	private static final ColumnSchema BUCKETED = ColumnSchema.of(IntList.of(Integer.BYTES),
			ObjectList.of(ColumnHashType.XXHASH8), true);
	private static final ColumnSchema NO_VALUE = ColumnSchema.of(IntList.of(Integer.BYTES, Long.BYTES), ObjectList.of(), false);
	private static final int BATCH_BYTES = 64 * 1024;

	private static final class Exited extends Error {
		final int code;

		Exited(int code) {
			super("exit " + code, null, false, false);
			this.code = code;
		}
	}

	private record Run(int code, String out, String err) {}

	@TempDir
	Path temp;
	private EmbeddedConnection sourceDb;
	private GrpcServer sourceServer;
	private InProcessCluster cluster;
	private IntConsumer previousExiter;

	@BeforeEach
	void start() throws Exception {
		previousExiter = ReplicatedServerMain.exiter;
		ReplicatedServerMain.exiter = code -> {
			throw new Exited(code);
		};
		sourceDb = new EmbeddedConnection(temp.resolve("source"), "copy-source", null);
		sourceServer = new GrpcServer(sourceDb, new InetSocketAddress("127.0.0.1", 0));
		sourceServer.start();
		cluster = new InProcessCluster(Files.createDirectories(temp.resolve("cluster")), 3, c -> c).startAll();
		cluster.awaitLeader(TIMEOUT);
	}

	@AfterEach
	void stop() throws Exception {
		ReplicatedServerMain.exiter = previousExiter;
		cluster.close();
		sourceServer.close();
		sourceDb.close();
	}

	private String sourceAddress() {
		return "127.0.0.1:" + sourceServer.getPort();
	}

	private String targetAddresses() {
		var addresses = new ArrayList<String>();
		for (var id : cluster.ids()) {
			addresses.add(cluster.config(id).self().apiHost() + ":" + cluster.config(id).self().apiPort());
		}
		return String.join(",", addresses);
	}

	private static synchronized Run run(String... args) {
		var out = new ByteArrayOutputStream();
		var err = new ByteArrayOutputStream();
		var oldOut = System.out;
		var oldErr = System.err;
		System.setOut(new PrintStream(out, true, StandardCharsets.UTF_8));
		System.setErr(new PrintStream(err, true, StandardCharsets.UTF_8));
		int code = 0;
		try {
			ReplicatedServerMain.main(args);
		} catch (Exited exited) {
			code = exited.code;
		} catch (Throwable e) {
			code = 1;
			err.writeBytes(("uncaught " + e).getBytes(StandardCharsets.UTF_8));
		} finally {
			System.setOut(oldOut);
			System.setErr(oldErr);
		}
		var run = new Run(code, out.toString(StandardCharsets.UTF_8), err.toString(StandardCharsets.UTF_8));
		System.out.println("$ " + String.join(" ", args) + "\n-> " + code + "\n" + run.out() + run.err());
		return run;
	}

	private Run copy(String... extra) {
		var args = new ArrayList<>(List.of("copy-columns", "--from", sourceAddress(), "--to", targetAddresses(),
				"--batch-bytes", String.valueOf(BATCH_BYTES)));
		args.addAll(List.of(extra));
		return run(args.toArray(String[]::new));
	}

	private static Buf buf(byte[] bytes) {
		return Buf.wrap(bytes);
	}

	private static Buf longBuf(long value) {
		return buf(java.nio.ByteBuffer.allocate(Long.BYTES).putLong(value).array());
	}

	private static Buf intBuf(int value) {
		return buf(java.nio.ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
	}

	private static Buf value(int index, int size) {
		var bytes = new byte[size];
		for (int i = 0; i < size; i++) {
			bytes[i] = (byte) (index * 31 + i);
		}
		return buf(bytes);
	}

	/** Values of several sizes: empty, tiny, medium, and larger than the batch of {@link #BATCH_BYTES}. */
	private static int valueSize(int index) {
		return switch (index % 500) {
			case 0 -> 0;
			case 1 -> 300 * 1024;
			case 2 -> 5000;
			default -> 8 + index % 40;
		};
	}

	private void populate(RocksDBSyncAPI api) {
		long fixed = api.createColumn("fixed", FIXED);
		long bucketed = api.createColumn("bucketed", BUCKETED);
		long noValue = api.createColumn("skipped", NO_VALUE);
		for (int i = 0; i < 3000; i++) {
			api.put(0, fixed, new Keys(longBuf(i * 7L)), value(i, valueSize(i)), RequestType.none());
		}
		for (int i = 0; i < 2500; i++) {
			var variable = buf(("key-" + i + "-" + "x".repeat(i % 50)).getBytes(StandardCharsets.UTF_8));
			api.put(0, bucketed, new Keys(intBuf(i % 10), variable), value(i, i % 100), RequestType.none());
		}
		for (int i = 0; i < 300; i++) {
			api.put(0, noValue, new Keys(intBuf(i % 5), longBuf(i)), buf(new byte[0]), RequestType.none());
		}
	}

	private static Set<KV> contents(RocksDBSyncAPI api, String column) {
		long id = api.getColumnId(column);
		try (var stream = api.getRange(0, id, null, null, false, RequestType.<KV>allInRange())) {
			var result = new HashSet<KV>();
			stream.forEach(kv -> assertTrue(result.add(kv), "duplicate " + kv.keys()));
			return result;
		}
	}

	@Test
	void copiesTheSelectedColumnsToTheClusterAndVerifiesThem() throws Exception {
		var source = sourceDb.getSyncApi(RequestContext.batch(TIMEOUT));
		populate(source);
		var leader = cluster.awaitLeader(TIMEOUT);
		var target = leader.connection().getSyncApi(RequestContext.batch(TIMEOUT));

		// A dry run writes nothing, not even the columns.
		var dry = copy("--dry-run", "--exclude-column", "skipped");
		assertEquals(0, dry.code(), dry.out() + dry.err());
		assertTrue(dry.out().contains("column fixed: ~") && dry.out().contains("column bucketed: ~"), dry.out());
		assertTrue(dry.out().contains("DRY-RUN OK"), dry.out());
		assertTrue(target.getAllColumnDefinitions().isEmpty(), "a dry run created columns");

		long start = System.nanoTime();
		var run = copy("--exclude-column", "skipped", "--verify");
		double seconds = (System.nanoTime() - start) / 1e9;
		assertEquals(0, run.code(), run.out() + run.err());
		assertTrue(run.out().contains("column fixed: 3000 keys"), run.out());
		assertTrue(run.out().contains("column bucketed: "), run.out());
		assertTrue(run.out().contains("column fixed: verified") && run.out().contains("column bucketed: verified"),
				run.out());
		assertTrue(run.out().stripTrailing().endsWith("OK"), run.out());
		System.out.println("copy-columns of 5500 keys, one excluded column, with verify: " + seconds + " s");

		var definitions = target.getAllColumnDefinitions();
		assertEquals(Set.of("fixed", "bucketed"), definitions.keySet(), "the excluded column is absent");
		assertEquals(FIXED, definitions.get("fixed"));
		assertEquals(BUCKETED, definitions.get("bucketed"));
		for (var column : List.of("fixed", "bucketed")) {
			var expected = contents(source, column);
			assertEquals(column.equals("fixed") ? 3000 : 2500, expected.size());
			assertEquals(expected, contents(target, column), column);
		}
		// The big value and the empty one came through.
		var fixedId = target.getColumnId("fixed");
		assertEquals(300 * 1024, target.get(0, fixedId, new Keys(longBuf(7L)), RequestType.current()).size());
		assertEquals(0, target.get(0, fixedId, new Keys(longBuf(0L)), RequestType.current()).size());

		// A second run refuses the columns that hold data, and changes nothing.
		var refused = copy("--exclude-column", "skipped");
		assertEquals(1, refused.code(), refused.out() + refused.err());
		assertTrue(refused.out().contains("column bucketed already holds data")
				|| refused.out().contains("column fixed already holds data"), refused.out());

		// With --allow-non-empty it overwrites the same keys with the same values (a retry), and then copies the rest.
		var again = copy("--exclude-column", "skipped", "--allow-non-empty", "--verify");
		assertEquals(0, again.code(), again.out() + again.err());
		assertEquals(contents(source, "fixed"), contents(target, "fixed"));

		var rest = copy("--column", "skipped", "--verify");
		assertEquals(0, rest.code(), rest.out() + rest.err());
		assertEquals(Set.of("fixed", "bucketed", "skipped"), target.getAllColumnDefinitions().keySet());
		assertEquals(300, contents(target, "skipped").size());
		assertEquals(contents(source, "skipped"), contents(target, "skipped"));
	}

	@Test
	void usageErrorsAndSchemaConflictsEndBeforeAnythingIsWritten() throws Exception {
		var source = sourceDb.getSyncApi(RequestContext.batch(TIMEOUT));
		populate(source);
		var leader = cluster.awaitLeader(TIMEOUT);
		var target = leader.connection().getSyncApi(RequestContext.batch(TIMEOUT));

		assertEquals(2, copy("--column", "nope").code());
		assertEquals(2, copy("--exclude-column", "nope").code());
		assertEquals(2, copy("--column", "fixed", "--exclude-column", "fixed").code());
		assertEquals(2, copy("--column", "_column_schemas_").code());
		assertEquals(2, run("copy-columns", "--from", sourceAddress()).code());
		assertEquals(2, copy("--batch-bytes", "0").code());
		assertEquals(2, copy("--verify", "yes").code());
		assertTrue(target.getAllColumnDefinitions().isEmpty());

		// The same name with another schema on the target is refused, and nothing else is created.
		target.createColumn("fixed", BUCKETED);
		var conflict = copy();
		assertEquals(1, conflict.code(), conflict.out() + conflict.err());
		assertTrue(conflict.out().contains("column fixed exists on the target with another schema"), conflict.out());
		assertEquals(Set.of("fixed"), target.getAllColumnDefinitions().keySet());
		assertNull(conflict.out().lines().filter(l -> l.startsWith("column ") && l.contains(" keys,")).findAny().orElse(null));
		assertFalse(conflict.out().contains("\nOK"));
	}
}
