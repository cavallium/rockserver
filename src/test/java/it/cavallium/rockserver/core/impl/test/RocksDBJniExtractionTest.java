package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import it.cavallium.rockserver.core.impl.rocksdb.RocksDBLoader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The native library is extracted through a temporary file and an atomic rename, never copied in place. */
class RocksDBJniExtractionTest {

	@TempDir
	Path dir;

	private static URL resource(Path source) throws Exception {
		return source.toUri().toURL();
	}

	private static byte[] library(int size) {
		var bytes = new byte[size];
		new Random(42).nextBytes(bytes);
		return bytes;
	}

	@Test
	void concurrentExtractionsLeaveOneCompleteFileAndNoTemporaries() throws Exception {
		byte[] content = library(4 << 20);
		Path source = Files.write(Files.createTempDirectory(dir, "src").resolve("lib.so"), content);
		Path target = Files.createDirectory(dir.resolve("out"));
		int threads = 8;
		var barrier = new CyclicBarrier(threads);
		try (var executor = Executors.newFixedThreadPool(threads)) {
			var futures = new ArrayList<Future<Path>>();
			for (int i = 0; i < threads; i++) {
				futures.add(executor.submit((Callable<Path>) () -> {
					barrier.await();
					return RocksDBLoader.extractLibrary(target, "librocksdbjni.so", resource(source));
				}));
			}
			for (var future : futures) {
				assertEquals(target.resolve("librocksdbjni.so"), future.get());
			}
		}
		assertArrayEquals(content, Files.readAllBytes(target.resolve("librocksdbjni.so")));
		try (Stream<Path> files = Files.list(target)) {
			assertEquals(1, files.count(), "temporary files must not be left behind");
		}
	}

	@Test
	void truncatedFileFromAnEarlierCrashIsReplaced() throws Exception {
		byte[] content = library(64 << 10);
		Path source = Files.write(Files.createTempDirectory(dir, "src").resolve("lib.so"), content);
		Path target = Files.createDirectory(dir.resolve("out"));
		Files.write(target.resolve("librocksdbjni.so"), new byte[100]);
		RocksDBLoader.extractLibrary(target, "librocksdbjni.so", resource(source));
		assertArrayEquals(content, Files.readAllBytes(target.resolve("librocksdbjni.so")));
	}

	@Test
	void completeFileIsReusedWithoutRewriting() throws Exception {
		byte[] content = library(64 << 10);
		Path source = Files.write(Files.createTempDirectory(dir, "src").resolve("lib.so"), content);
		Path target = Files.createDirectory(dir.resolve("out"));
		Path extracted = RocksDBLoader.extractLibrary(target, "librocksdbjni.so", resource(source));
		var inode = Files.getAttribute(extracted, "unix:ino");
		RocksDBLoader.extractLibrary(target, "librocksdbjni.so", resource(source));
		assertEquals(inode, Files.getAttribute(extracted, "unix:ino"), "a complete library must not be replaced");
	}
}
