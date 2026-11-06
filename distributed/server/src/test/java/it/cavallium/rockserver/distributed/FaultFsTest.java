package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The fault-injecting shim of {@link FaultFs}, in a small JVM ({@link Probe}) started with it: writes and syncs under
 * the faulty directory fail with the trigger's error while the trigger exists, and only those; the trigger's operation
 * filter works; removing the trigger ends the faults. The probe JVM uses the same calls as Ratis' log and the JDK
 * file channels (write, pwrite, fsync, fdatasync).
 */
class FaultFsTest {

	@TempDir
	Path temp;

	@Test
	void failsWritesAndSyncsUnderTheDirectoryWhileTriggered() throws Exception {
		var library = FaultFs.build(temp.resolve("lib"));
		var faulty = Files.createDirectories(temp.resolve("faulty"));
		var other = Files.createDirectories(temp.resolve("other"));
		var trigger = temp.resolve("trigger");
		assertEquals(EXPECTED, runProbe(faulty, other, trigger, FaultFs.environment(library, faulty, trigger)));
	}

	/**
	 * The faulty directory is created only after the JVM started (as a node's data directory on its first start) and
	 * named through {@code ..}: as {@link FaultFs#environment} passes it (made canonical in Java), and as given (the
	 * shim resolves it once it exists).
	 */
	@Test
	void failsUnderADirectoryCreatedLaterAndNamedThroughDotDot() throws Exception {
		var library = FaultFs.build(temp.resolve("lib"));
		var trigger = temp.resolve("trigger");
		var viaJava = temp.resolve("lib").resolve("..").resolve("late-java").resolve("faulty");
		var other = Files.createDirectories(temp.resolve("other"));
		assertEquals(EXPECTED, runProbe(viaJava, other, trigger, FaultFs.environment(library, viaJava, trigger)));
		var asGiven = temp.resolve("lib").resolve("..").resolve("late-raw").resolve("faulty").toAbsolutePath();
		var raw = Map.of("LD_PRELOAD", library.toAbsolutePath().toString(), "FAULTFS_DIR", asGiven.toString(),
				"FAULTFS_TRIGGER", trigger.toAbsolutePath().toString());
		assertEquals(EXPECTED, runProbe(asGiven, other, trigger, raw));
	}

	private static final List<String> EXPECTED = List.of(
				"none faulty write OK", "none faulty force OK", "none other write OK",
				"EIO faulty write Input/output error", "EIO faulty force Input/output error",
				"EIO faulty pwrite Input/output error", "EIO other write OK", "EIO other force OK",
				"ENOSPC-write faulty write No space left on device", "ENOSPC-write faulty force OK",
				"EIO-sync faulty write OK", "EIO-sync faulty force Input/output error",
				"cleared faulty write OK", "cleared faulty force OK");

	private List<String> runProbe(Path faulty, Path other, Path trigger, Map<String, String> environment)
			throws Exception {
		var command = List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
				"-Xmx32m", "-XX:TieredStopAtLevel=1", "-cp", System.getProperty("java.class.path"),
				Probe.class.getName(), faulty.toString(), other.toString(), trigger.toString());
		var builder = new ProcessBuilder(command).redirectErrorStream(true);
		builder.environment().putAll(environment);
		var process = builder.start();
		var output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
		assertTrue(process.waitFor(60, TimeUnit.SECONDS), output);
		assertEquals(0, process.exitValue(), output);
		return output.lines().filter(line -> line.startsWith("> ")).map(line -> line.substring(2)).toList();
	}

	/** The JVM under the shim: sets the trigger itself (outside the faulty directory) and reports each call. */
	public static final class Probe {

		public static void main(String[] args) throws Exception {
			var faulty = Path.of(args[0]);
			var other = Path.of(args[1]);
			var trigger = Path.of(args[2]);
			// A directory that does not exist yet when the JVM loads the shim is created here.
			Files.createDirectories(faulty);
			var results = new ArrayList<String>();
			step(results, "none", faulty, other);
			step(results, "EIO", faulty, other, trigger, FaultFs.Errno.EIO, FaultFs.Ops.ALL);
			step(results, "ENOSPC-write", faulty, null, trigger, FaultFs.Errno.ENOSPC, FaultFs.Ops.WRITE);
			step(results, "EIO-sync", faulty, null, trigger, FaultFs.Errno.EIO, FaultFs.Ops.SYNC);
			FaultFs.clear(trigger);
			Thread.sleep(20);
			results.add("cleared faulty write " + write(faulty.resolve("c")));
			results.add("cleared faulty force " + force(faulty.resolve("c")));
			// Through System.out, which the shim leaves alone (not under the faulty directory).
			results.forEach(line -> System.out.println("> " + line));
		}

		private static void step(List<String> results, String name, Path faulty, Path other) {
			results.add(name + " faulty write " + write(faulty.resolve("a")));
			results.add(name + " faulty force " + force(faulty.resolve("a")));
			results.add(name + " other write " + write(other.resolve("a")));
		}

		private static void step(List<String> results, String name, Path faulty, Path other, Path trigger,
				FaultFs.Errno errno, FaultFs.Ops ops) throws Exception {
			FaultFs.trigger(trigger, errno, ops);
			Thread.sleep(20);
			results.add(name + " faulty write " + write(faulty.resolve("b")));
			results.add(name + " faulty force " + force(faulty.resolve("a")));
			if (other != null) {
				results.add(name + " faulty pwrite " + pwrite(faulty.resolve("a")));
				results.add(name + " other write " + write(other.resolve("b")));
				results.add(name + " other force " + force(other.resolve("a")));
			}
		}

		/** A plain write through a stream (libc write). */
		private static String write(Path file) {
			try (var out = Files.newOutputStream(file, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
				out.write("data".getBytes(StandardCharsets.UTF_8));
				return "OK";
			} catch (IOException e) {
				return e.getMessage().replaceAll(".*: ", "");
			}
		}

		/** A positional write through a file channel (libc pwrite). */
		private static String pwrite(Path file) {
			try (var channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
				channel.write(ByteBuffer.wrap(new byte[] {1}), 0);
				return "OK";
			} catch (IOException e) {
				return e.getMessage().replaceAll(".*: ", "");
			}
		}

		/** fdatasync and fsync, as Ratis' log and RocksDB call them. */
		private static String force(Path file) {
			try (var channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
				channel.force(false);
				channel.force(true);
				return "OK";
			} catch (IOException e) {
				return e.getMessage().replaceAll(".*: ", "");
			}
		}
	}
}
