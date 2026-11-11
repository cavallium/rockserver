package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * R62-1: a halt ends the process while RocksDB is writing. A JVM of its own merges into a database whose merge
 * operator runs in Java, so that its flush and compaction threads are inside calls into Java nearly all the time, and
 * then halts as a crash point does. The process must be gone within {@link #EXIT_SECONDS} s, with the status the halt
 * asked for. With {@link Runtime#halt} it stayed for good: the VM's exit ran the destructors of the RocksDB library,
 * which joined a background thread that waited for the safepoint of that same exit.
 */
@Timeout(180)
class ProcessHaltTest {

	private static final int EXIT_SECONDS = 15;
	private static final int STATUS = 77;

	@TempDir
	Path root;

	@RepeatedTest(3)
	void aHaltEndsAProcessWhoseDatabaseIsWriting() throws Exception {
		var tmp = Files.createDirectories(root.resolve("tmp"));
		var output = root.resolve("output.log");
		var command = List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
				"-cp", System.getProperty("java.class.path"),
				"--enable-native-access=ALL-UNNAMED",
				"--add-modules=jdk.incubator.vector",
				"-Xmx512m",
				"-Djava.io.tmpdir=" + tmp,
				ProcessHaltMain.class.getName(), root.resolve("db").toString(), "3000", Integer.toString(STATUS));
		var process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(output.toFile()).start();
		try {
			long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
			while (!Files.readString(output).contains("HALTING")) {
				assertTrue(process.isAlive(), () -> "the child ended before its halt:\n" + read(output));
				assertTrue(System.nanoTime() < deadline, () -> "the child did not reach its halt:\n" + read(output));
				Thread.sleep(50);
			}
			assertTrue(process.waitFor(EXIT_SECONDS, TimeUnit.SECONDS), () -> "the process is still running "
					+ EXIT_SECONDS + " s after its halt:\n" + read(output));
			assertEquals(STATUS, process.exitValue(), () -> read(output));
		} finally {
			process.destroyForcibly();
			process.waitFor(30, TimeUnit.SECONDS);
		}
	}

	private static String read(Path file) {
		try {
			return Files.readString(file);
		} catch (Exception ex) {
			return ex.toString();
		}
	}
}
