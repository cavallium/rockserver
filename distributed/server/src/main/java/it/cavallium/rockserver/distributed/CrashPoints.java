package it.cavallium.rockserver.distributed;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;
import org.jetbrains.annotations.Nullable;

/**
 * Tests only: named points of the replication protocol where a test can stop this JVM at once, as a SIGKILL would,
 * but at a chosen step instead of a random moment. Nothing is armed in production: {@link #reach} is then one volatile
 * read. A test arms one point in a node's JVM (the process tests do it through {@code CrashPointMain}); the next time
 * the node passes it, after {@code skip} passes, the node prints a line naming the point and halts with
 * {@link #EXIT_CODE}, without shutdown hooks, flushes or a leadership hand-off.
 */
final class CrashPoints {

	/** Exit code of a node halted at a crash point, so that a test can tell it from any other end. */
	static final int EXIT_CODE = 99;

	/** Where a node can be halted. */
	enum Point {
		/** On the leader, in {@code preAppendTransaction}: a proposal admitted, before Ratis appends it to the log. */
		BEFORE_APPEND,
		/**
		 * A group of committed entries handed to the apply thread, before it applies them: on the leader, entries that a
		 * majority holds but that no replica may have applied, and whose callers have no answer.
		 */
		BEFORE_APPLY,
		/** A group applied and committed to the local database, before its replies and applied index are published. */
		AFTER_APPLY_BEFORE_REPLY,
		/** This node just learned that it won an election, before it is a ready leader. */
		ELECTED,
		/** A snapshot's checkpoint written and synced in its temporary directory, before it is published. */
		SNAPSHOT_BUILT,
		/** A snapshot published in the snapshot directory, before Ratis learns of it (and can purge its log). */
		SNAPSHOT_TAKEN,
		/** A snapshot received from the leader, before this replica closes its database to load it. */
		INSTALL_RECEIVED,
		/** During the install of a received snapshot: the old database closed, the new one not restored yet. */
		INSTALL_RESTORING,
		/** A configuration entry with a previous configuration (a joint configuration) applied here. */
		JOINT_CONFIGURATION_APPLIED
	}

	private record Armed(Point point, AtomicInteger skip) {}

	private static volatile @Nullable Armed armed;
	/** Halts the JVM; tests of this class replace it. */
	static volatile IntConsumer halt = code -> Runtime.getRuntime().halt(code);

	private CrashPoints() {}

	/** Halts at the {@code skip + 1}-th pass of {@code point} from now on; replaces what was armed before. */
	static void arm(Point point, int skip) {
		if (skip < 0) {
			throw new IllegalArgumentException("skip must not be negative");
		}
		armed = new Armed(Objects.requireNonNull(point, "point"), new AtomicInteger(skip));
	}

	static void disarm() {
		armed = null;
	}

	/** Called at {@code point}: halts the JVM if that point is armed and its passes to skip are used up. */
	static void reach(Point point) {
		var current = armed;
		if (current == null || current.point() != point || current.skip().getAndDecrement() > 0) {
			return;
		}
		armed = null;
		// The line the tests look for; stderr is unbuffered, and halt runs no shutdown hook that could flush it.
		System.err.println("CRASH POINT " + point);
		System.err.flush();
		halt.accept(EXIT_CODE);
	}
}
