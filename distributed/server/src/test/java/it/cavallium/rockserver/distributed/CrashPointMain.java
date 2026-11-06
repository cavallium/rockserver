package it.cavallium.rockserver.distributed;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * {@link ReplicatedServerMain} with {@link CrashPoints} that a test arms from outside the JVM: a {@code ProcessCluster}
 * started with this main class runs each node with a thread that watches {@link #armFile} of the node and arms the
 * point it names. The file holds {@code <POINT> [skip]}, or {@code DISARM}; the node deletes it as it takes it, so a
 * restarted node starts unarmed.
 */
public final class CrashPointMain {

	private CrashPointMain() {}

	/** The file that arms a crash point in the node whose {@code --data-dir} is {@code dataDir}. */
	static Path armFile(Path dataDir) {
		return dataDir.resolveSibling("crash-point");
	}

	/** Arms {@code point} in the running node of {@code dataDir}, at its {@code skip + 1}-th pass. */
	static void arm(Path dataDir, CrashPoints.Point point, int skip) throws IOException {
		var file = armFile(dataDir);
		var temp = file.resolveSibling("crash-point.tmp");
		Files.writeString(temp, point.name() + " " + skip, StandardCharsets.UTF_8);
		Files.move(temp, file, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
	}

	/** Disarms whatever point the running node of {@code dataDir} has armed. */
	static void disarm(Path dataDir) throws IOException {
		var file = armFile(dataDir);
		var temp = file.resolveSibling("crash-point.tmp");
		Files.writeString(temp, DISARM, StandardCharsets.UTF_8);
		Files.move(temp, file, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
	}

	private static final String DISARM = "DISARM";

	/** Whether the node took the file of {@link #arm} (it deletes it when it arms the point). */
	static boolean armed(Path dataDir) {
		return !Files.exists(armFile(dataDir));
	}

	public static void main(String[] args) throws Exception {
		var list = List.of(args);
		int dataDirOption = list.indexOf("--data-dir");
		if (!list.isEmpty() && list.getFirst().equals("run") && dataDirOption >= 0 && dataDirOption + 1 < args.length) {
			var file = armFile(Path.of(args[dataDirOption + 1]));
			// A file left from before this start arms nothing.
			Files.deleteIfExists(file);
			Thread.ofPlatform().daemon().name("crash-point-watcher").start(() -> watch(file));
		}
		ReplicatedServerMain.main(args);
	}

	private static void watch(Path file) {
		while (true) {
			try {
				if (Files.exists(file)) {
					var words = Files.readString(file, StandardCharsets.UTF_8).strip().split("\\s+");
					if (words[0].equals(DISARM)) {
						CrashPoints.disarm();
						Files.delete(file);
						System.err.println("CRASH POINT DISARMED");
						continue;
					}
					var point = CrashPoints.Point.valueOf(words[0]);
					int skip = words.length > 1 ? Integer.parseInt(words[1]) : 0;
					CrashPoints.arm(point, skip);
					Files.delete(file);
					System.err.println("CRASH POINT ARMED " + point + " skip " + skip);
				}
				Thread.sleep(10);
			} catch (InterruptedException e) {
				return;
			} catch (IOException | RuntimeException e) {
				System.err.println("crash point watcher: " + e);
			}
		}
	}
}
