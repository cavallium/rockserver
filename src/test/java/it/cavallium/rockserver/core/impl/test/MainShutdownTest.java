package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.Utils;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(90)
public class MainShutdownTest {

	private static final Duration START_TIMEOUT = Duration.ofSeconds(30);

	@TempDir
	Path tempDir;

	@Test
	void sigtermWaitsForServersAndDatabaseToClose() throws Exception {
		Assumptions.assumeTrue(System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("linux"),
				"Process.destroy() is used as the production SIGTERM signal on Linux");

		Path databasePath = tempDir.resolve("database");
		Path configPath = writeConfig();
		Path childLog = tempDir.resolve("child.log");
		Path shutdownComplete = tempDir.resolve("shutdown-complete");
		int thriftPort = reserveLoopbackPort();
		int grpcPort;
		do {
			grpcPort = reserveLoopbackPort();
		} while (grpcPort == thriftPort);

		String classPath = System.getProperty("surefire.test.class.path",
				System.getProperty("java.class.path"));
		var command = List.of(
				Path.of(System.getProperty("java.home"), "bin", "java").toString(),
				"--enable-native-access=ALL-UNNAMED",
				"-Drockserver.core.print-config=false",
				"-Dit.cavallium.rockserver.db.shutdown-pending-ops-timeout-ms=2000",
				"-Dit.cavallium.rockserver.grpc.server.shutdown-graceful-timeout-ms=5000",
				"-Dit.cavallium.rockserver.grpc.server.shutdown-forced-timeout-ms=5000",
				"-Dit.cavallium.rockserver.grpc.server.scheduler-shutdown-timeout-ms=5000",
				"-XX:ErrorFile=" + tempDir.resolve("hs_err_pid%p.log"),
				"-cp",
				classPath,
				ShutdownProcess.class.getName(),
				shutdownComplete.toString(),
				"--database-url",
				databasePath.toUri().toString(),
				"--thrift-listen-url",
				"http://127.0.0.1:" + thriftPort,
				"--grpc-listen-url",
				"http://127.0.0.1:" + grpcPort,
				"--name",
				"main-shutdown-test",
				"--config",
				configPath.toString()
		);

		Process child = null;
		try {
			child = new ProcessBuilder(command)
					.directory(tempDir.toFile())
					.redirectErrorStream(true)
					.redirectOutput(childLog.toFile())
					.start();
			awaitListening(child, childLog, grpcPort, START_TIMEOUT);

			try (var client = GrpcConnection.forHostAndPort("main-shutdown-client",
					new Utils.HostAndPort("127.0.0.1", grpcPort))) {
				var api = client.getSyncApi();
				long columnId = api.createColumn("shutdown-column",
						ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
				api.put(0, columnId, key(1), value(1), RequestType.none());
				api.flush();
				api.openTransaction(TimeUnit.MINUTES.toMillis(5));
			}

			child.destroy();
			if (!child.waitFor(30, TimeUnit.SECONDS)) {
				fail("Rockserver did not finish SIGTERM shutdown:\n" + childDetails(child, childLog));
			}
			assertEquals(128 + 15, child.exitValue(), childDetails(child, childLog));
			assertEquals("complete", Files.readString(shutdownComplete),
					"the JVM exited before Main completed every resource scope\n" + childDetails(child, childLog));
		} finally {
			stopChild(child);
		}

		try (var reopened = new EmbeddedConnection(databasePath, "main-shutdown-reopen", configPath)) {
			var api = reopened.getSyncApi();
			long columnId = api.getColumnId("shutdown-column");
			assertEquals(value(1), api.get(0, columnId, key(1), RequestType.current()));
			api.put(0, columnId, key(2), value(2), RequestType.none());
			assertEquals(value(2), api.get(0, columnId, key(2), RequestType.current()));
			api.flush();
		}
	}

	private Path writeConfig() throws Exception {
		Path configPath = tempDir.resolve("rockserver.conf");
		Files.writeString(configPath, """
				database: {
				  metrics: {
				    database-name: "main-shutdown-test"
				    jmx: { enabled: false }
				  }
				  parallelism: { read: 2, write: 1 }
				  global: {
				    enable-fast-get: false
				    ingest-behind: false
				    optimistic: false
				    use-direct-io: false
				    maximum-open-files: 128
				    block-cache: "16MiB"
				    write-buffer-manager: "8MiB"
				    log-path: "%s"
				    wal-path: "%s"
				    temp-sst-path: "%s"
				    fallback-column-options: {
				      write-buffer-size: "4MiB"
				      memtable-memory-budget-bytes: "8MiB"
				      volumes: [{ volume-path: "%s", target-size: "1TiB" }]
				    }
				  }
				}
				""".formatted(
				escapeHocon(tempDir.resolve("logs")),
				escapeHocon(tempDir.resolve("wal")),
				escapeHocon(tempDir.resolve("temp-sst")),
				escapeHocon(tempDir.resolve("volume"))));
		return configPath;
	}

	private static int reserveLoopbackPort() throws Exception {
		try (var socket = new ServerSocket()) {
			socket.bind(new InetSocketAddress("127.0.0.1", 0));
			return socket.getLocalPort();
		}
	}

	private static void awaitListening(Process child, Path log, int port, Duration timeout) throws Exception {
		long deadline = System.nanoTime() + timeout.toNanos();
		while (System.nanoTime() < deadline) {
			if (!child.isAlive()) {
				fail("Rockserver exited before startup completed:\n" + childDetails(child, log));
			}
			try (var socket = new java.net.Socket()) {
				socket.connect(new InetSocketAddress("127.0.0.1", port), 100);
				return;
			} catch (java.io.IOException ignored) {
				// The server has not bound the port yet.
			}
			Thread.sleep(25);
		}
		fail("Timed out waiting for the gRPC server to listen:\n" + childDetails(child, log));
	}

	private static void stopChild(Process child) throws Exception {
		if (child == null || !child.isAlive()) {
			return;
		}
		child.destroy();
		if (!child.waitFor(5, TimeUnit.SECONDS)) {
			child.destroyForcibly();
			child.waitFor(5, TimeUnit.SECONDS);
		}
	}

	private static String childDetails(Process child, Path log) throws Exception {
		String state = child.isAlive() ? "alive" : "exit=" + child.exitValue();
		return state + "\n" + readLog(log);
	}

	private static String readLog(Path log) throws Exception {
		return Files.exists(log) ? Files.readString(log) : "";
	}

	private static String escapeHocon(Path path) {
		return path.toAbsolutePath().toString().replace("\\", "\\\\").replace("\"", "\\\"");
	}

	private static Keys key(long id) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(id).array()));
	}

	private static Buf value(long id) {
		return Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(id).array());
	}

	public static final class ShutdownProcess {

		private ShutdownProcess() {
		}

		public static void main(String[] args) throws Exception {
			Path completionMarker = Path.of(args[0]);
			String[] mainArgs = Arrays.copyOfRange(args, 1, args.length);
			Runnable onShutdownComplete = () -> {
				try {
					Files.writeString(completionMarker, "complete", StandardOpenOption.CREATE_NEW);
				} catch (java.io.IOException ex) {
					throw new java.io.UncheckedIOException(ex);
				}
			};
			var run = Class.forName("it.cavallium.rockserver.core.Main")
					.getMethod("run", String[].class, Runnable.class);
			try {
				run.invoke(null, mainArgs, onShutdownComplete);
			} catch (java.lang.reflect.InvocationTargetException ex) {
				if (ex.getCause() instanceof Exception cause) {
					throw cause;
				}
				if (ex.getCause() instanceof Error cause) {
					throw cause;
				}
				throw ex;
			}
		}
	}
}
