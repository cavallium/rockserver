package it.cavallium.rockserver.distributed;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assumptions;

/**
 * Tests only (RT-23, RT-64): the fault-injecting storage shim {@code faultfs.c} (a test resource), loaded into a node's
 * JVM with {@code LD_PRELOAD}. While a trigger file exists, every write, fsync, fdatasync and fallocate of a file under
 * one directory (the node's data directory) fails with EIO or ENOSPC, as a failing or full disk would; nothing else
 * changes, and no root is needed. See the comment at the top of {@code faultfs.c} for what it covers.
 *
 * <p>{@link #build} compiles it with {@code gcc}; a test without {@code gcc} on the path is skipped, not failed.</p>
 */
final class FaultFs {

	private static final String SOURCE = "/faultfs/faultfs.c";
	private static final long COMPILE_TIMEOUT_SECONDS = 60;

	/** The error the faulty calls return. */
	enum Errno {
		EIO, ENOSPC
	}

	/** Which calls fail: all of them, the writes (and fallocate) only, as on a full disk, or the syncs only. */
	enum Ops {
		ALL, WRITE, SYNC
	}

	private FaultFs() {}

	/**
	 * Compiles the shim into {@code directory} (once per directory) and returns the library. Skips the calling test
	 * (a JUnit assumption) when {@code gcc} is missing.
	 */
	static synchronized Path build(Path directory) throws IOException, InterruptedException {
		var library = directory.resolve("libfaultfs.so");
		if (Files.exists(library)) {
			return library;
		}
		Files.createDirectories(directory);
		var source = directory.resolve("faultfs.c");
		try (InputStream in = FaultFs.class.getResourceAsStream(SOURCE)) {
			if (in == null) {
				throw new IOException("Test resource " + SOURCE + " not found");
			}
			Files.copy(in, source, StandardCopyOption.REPLACE_EXISTING);
		}
		Process gcc;
		try {
			gcc = new ProcessBuilder("gcc", "-shared", "-fPIC", "-O2", "-o", library.toString(), source.toString(), "-ldl")
					.redirectErrorStream(true)
					.start();
		} catch (IOException e) {
			Assumptions.abort("gcc is not available to build the fault-injecting shim: " + e.getMessage());
			throw e;
		}
		var output = new String(gcc.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
		if (!gcc.waitFor(COMPILE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
			gcc.destroyForcibly();
			throw new IOException("gcc did not finish building " + source);
		}
		if (gcc.exitValue() != 0) {
			throw new IOException("gcc failed to build " + source + ":\n" + output);
		}
		return library;
	}

	/** Environment variables that load the shim, with faults under {@code faultyDir} while {@code trigger} exists. */
	static Map<String, String> environment(Path library, Path faultyDir, Path trigger) {
		return Map.of("LD_PRELOAD", library.toAbsolutePath().toString(),
				"FAULTFS_DIR", faultyDir.toAbsolutePath().toString(),
				"FAULTFS_TRIGGER", trigger.toAbsolutePath().toString());
	}

	/** Starts the faults: the shim reads the trigger at most 2 ms later. Written atomically. */
	static void trigger(Path trigger, Errno errno, Ops ops) throws IOException {
		var temp = trigger.resolveSibling(trigger.getFileName() + ".tmp");
		Files.writeString(temp, errno.name() + " " + ops.name().toLowerCase(java.util.Locale.ROOT),
				StandardCharsets.UTF_8);
		Files.move(temp, trigger, StandardCopyOption.ATOMIC_MOVE);
	}

	/** Ends the faults ("the space came back"). */
	static void clear(Path trigger) throws IOException {
		Files.deleteIfExists(trigger);
	}
}
