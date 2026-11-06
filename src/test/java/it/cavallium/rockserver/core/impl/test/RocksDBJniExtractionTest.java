package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Stream;
import java.util.zip.CRC32;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The native library is extracted through a temporary file and an atomic rename, never copied in place, into a
 * directory named after its content.
 */
class RocksDBJniExtractionTest {

	@TempDir
	Path dir;

	private static URL resource(Path source) throws Exception {
		return source.toUri().toURL();
	}

	private static byte[] library(int size) {
		return library(size, 42);
	}

	private static byte[] library(int size, long seed) {
		var bytes = new byte[size];
		new Random(seed).nextBytes(bytes);
		return bytes;
	}

	private static Path contentDir(Path dir, byte[] content) {
		var crc = new CRC32();
		crc.update(content);
		return dir.resolve(String.format("%d-%08x", content.length, crc.getValue()));
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
				assertEquals(contentDir(target, content).resolve("librocksdbjni.so"), future.get());
			}
		}
		assertArrayEquals(content, Files.readAllBytes(contentDir(target, content).resolve("librocksdbjni.so")));
		try (Stream<Path> files = Files.list(contentDir(target, content))) {
			assertEquals(1, files.count(), "temporary files must not be left behind");
		}
	}

	@Test
	void truncatedFileFromAnEarlierCrashIsReplaced() throws Exception {
		byte[] content = library(64 << 10);
		Path source = Files.write(Files.createTempDirectory(dir, "src").resolve("lib.so"), content);
		Path target = Files.createDirectory(dir.resolve("out"));
		Files.createDirectories(contentDir(target, content));
		Files.write(contentDir(target, content).resolve("librocksdbjni.so"), new byte[100]);
		var extracted = RocksDBLoader.extractLibrary(target, "librocksdbjni.so", resource(source));
		assertArrayEquals(content, Files.readAllBytes(extracted));
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

	@Test
	void rebuiltLibraryOfTheSameSizeIsExtractedAgain() throws Exception {
		byte[] first = library(64 << 10, 1);
		byte[] rebuilt = library(64 << 10, 2);
		Path sources = Files.createTempDirectory(dir, "src");
		Path target = Files.createDirectory(dir.resolve("out"));
		Path old = RocksDBLoader.extractLibrary(target, "librocksdbjni.so",
				resource(Files.write(sources.resolve("first.so"), first)));
		Path fresh = RocksDBLoader.extractLibrary(target, "librocksdbjni.so",
				resource(Files.write(sources.resolve("rebuilt.so"), rebuilt)));
		assertNotEquals(old, fresh);
		assertArrayEquals(rebuilt, Files.readAllBytes(fresh));
		assertArrayEquals(first, Files.readAllBytes(old));
	}

	@Test
	void jarEntryIsKeyedByItsCrcAndReused() throws Exception {
		byte[] content = library(64 << 10);
		Path jar = Files.createTempDirectory(dir, "src").resolve("natives.jar");
		try (var out = new JarOutputStream(Files.newOutputStream(jar))) {
			out.putNextEntry(new JarEntry("librocksdbjni.so"));
			out.write(content);
			out.closeEntry();
		}
		var url = new URL("jar:" + jar.toUri() + "!/librocksdbjni.so");
		Path target = Files.createDirectory(dir.resolve("out"));
		Path extracted = RocksDBLoader.extractLibrary(target, "librocksdbjni.so", url);
		assertEquals(contentDir(target, content).resolve("librocksdbjni.so"), extracted);
		assertArrayEquals(content, Files.readAllBytes(extracted));
		var inode = Files.getAttribute(extracted, "unix:ino");
		RocksDBLoader.extractLibrary(target, "librocksdbjni.so", url);
		assertEquals(inode, Files.getAttribute(extracted, "unix:ino"));
	}

	@Test
	void staleTemporaryFilesAreRemovedAndRecentOnesKept() throws Exception {
		byte[] content = library(64 << 10);
		Path source = Files.write(Files.createTempDirectory(dir, "src").resolve("lib.so"), content);
		Path target = Files.createDirectory(dir.resolve("out"));
		Path contentDir = Files.createDirectories(contentDir(target, content));
		Path stale = Files.write(contentDir.resolve("librocksdbjni.so.123.tmp"), new byte[10]);
		Files.setLastModifiedTime(stale, FileTime.from(Instant.now().minus(1, ChronoUnit.HOURS)));
		Path recent = Files.write(contentDir.resolve("librocksdbjni.so.456.tmp"), new byte[10]);
		RocksDBLoader.extractLibrary(target, "librocksdbjni.so", resource(source));
		assertFalse(Files.exists(stale), "a temporary file left by a killed process must be removed");
		assertTrue(Files.exists(recent), "another process may still be writing a recent temporary file");
	}
}
