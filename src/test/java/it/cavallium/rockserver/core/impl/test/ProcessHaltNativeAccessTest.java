package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import it.cavallium.rockserver.core.impl.ProcessHalt;
import it.cavallium.rockserver.core.impl.rocksdb.RocksDBLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * R66-5, review of R67: which JVM flags decide whether a halt ({@link ProcessHalt}) uses libc's {@code _exit} or
 * falls back to {@link Runtime#halt}, which can hang a database that is writing (R62-1). A process each, on the JDK
 * that runs the tests.
 *
 * <p>Without {@code --enable-native-access} the JDK only warns (JEP 472) and {@code _exit} is linked all the same. It
 * is not linked only where native access is denied ({@code --illegal-native-access=deny}, announced as a future
 * default), and there the RocksDB library cannot be loaded either: such a server never opens a database, so it never
 * reaches a halt with one writing. On the day a JDK denies by default, the first case fails and the flag becomes
 * required for the halt, as it then is for the server itself.</p>
 */
@Timeout(120)
public class ProcessHaltNativeAccessTest {

	@TempDir
	Path tempDir;

	@BeforeEach
	void setUp() {
		Assumptions.assumeTrue(System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("linux"),
				"_exit is linked from libc on Linux");
	}

	@Test
	void withoutTheNativeAccessFlagTheHaltStillUsesExit() throws Exception {
		String log = run(List.of());
		assertTrue(log.contains("EXIT-LINKED true"), log);
		assertTrue(log.contains("ROCKSDB-LOADED true"), log);
	}

	@Test
	void withTheNativeAccessFlagTheHaltUsesExit() throws Exception {
		String log = run(List.of("--enable-native-access=ALL-UNNAMED"));
		assertTrue(log.contains("EXIT-LINKED true"), log);
		assertTrue(log.contains("ROCKSDB-LOADED true"), log);
	}

	@Test
	void whereNativeAccessIsDeniedTheHaltFallsBackButNoDatabaseCanOpen() throws Exception {
		String log = run(List.of("--illegal-native-access=deny"));
		assertTrue(log.contains("EXIT-LINKED false"), log);
		assertTrue(log.contains("ROCKSDB-LOADED false java.lang.IllegalCallerException"), log);
	}

	private String run(List<String> jvmFlags) throws Exception {
		String classPath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
		var command = new ArrayList<String>();
		command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
		command.addAll(jvmFlags);
		command.addAll(List.of("-XX:ErrorFile=" + tempDir.resolve("hs_err_pid%p.log"), "-cp", classPath,
				Probe.class.getName()));
		Path childLog = tempDir.resolve("child.log");
		var child = new ProcessBuilder(command)
				.directory(tempDir.toFile())
				.redirectErrorStream(true)
				.redirectOutput(childLog.toFile())
				.start();
		try {
			if (!child.waitFor(60, TimeUnit.SECONDS)) {
				fail("still running after 60 s: " + Files.readString(childLog));
			}
		} finally {
			child.destroyForcibly().waitFor();
		}
		String log = Files.readString(childLog);
		assertEquals(0, child.exitValue(), log);
		return log;
	}

	/** The child JVM: whether {@code _exit} is linked, and whether the RocksDB library loads. */
	public static final class Probe {

		private Probe() {
		}

		public static void main(String[] args) {
			System.out.println("EXIT-LINKED " + ProcessHalt.prepare());
			try {
				RocksDBLoader.loadLibrary();
				System.out.println("ROCKSDB-LOADED true");
			} catch (Throwable notLoaded) {
				System.out.println("ROCKSDB-LOADED false " + notLoaded);
			}
			System.out.flush();
		}
	}
}
