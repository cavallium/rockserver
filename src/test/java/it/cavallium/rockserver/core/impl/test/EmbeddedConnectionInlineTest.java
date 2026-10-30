package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.Get;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** {@link EmbeddedConnection#requestInline} counts the timeout from the start time its caller passes. */
@Timeout(60)
class EmbeddedConnectionInlineTest {

	@TempDir
	Path tempDir;

	private EmbeddedConnection connection;
	private long columnId;

	@BeforeEach
	void setUp() throws IOException {
		System.setProperty("rockserver.core.print-config", "false");
		Path configFile = tempDir.resolve("rockserver.conf");
		Files.writeString(configFile, "database: { global: { ingest-behind: false, optimistic: true } }");
		connection = new EmbeddedConnection(tempDir.resolve("db"), "inline", configFile);
		columnId = connection.getSyncApi(RequestContext.batch())
				.createColumn("inline", ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
		connection.getSyncApi(RequestContext.batch()).put(0, columnId, key(), Buf.wrap(new byte[] {7}),
				RequestType.none());
	}

	@AfterEach
	void tearDown() throws IOException {
		if (connection != null) {
			connection.closeTesting();
		}
	}

	@Test
	void runsWithinTheDeadlineCountedFromTheGivenStart() {
		var context = RequestContext.latency(Duration.ofSeconds(5));
		Buf value = connection.requestInline(context, System.nanoTime(), get());
		assertEquals(Buf.wrap(new byte[] {7}), value);
	}

	@Test
	void failsWithoutRunningWhenTheDeadlinePassedSinceTheGivenStart() {
		var context = RequestContext.latency(Duration.ofMillis(50));
		long startedNanos = System.nanoTime() - Duration.ofMillis(200).toNanos();
		var error = assertThrows(RocksDBException.class, () -> connection.requestInline(context, startedNanos, get()));
		assertEquals(RocksDBErrorType.READ_DEADLINE_EXCEEDED, error.getErrorUniqueId());
	}

	private Get<Buf> get() {
		return new Get<>(0, columnId, key(), RequestType.current());
	}

	private static Keys key() {
		return new Keys(Buf.wrap(new byte[] {0, 0, 0, 0, 0, 0, 0, 1}));
	}
}
