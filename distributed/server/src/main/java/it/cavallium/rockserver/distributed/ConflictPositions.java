package it.cavallium.rockserver.distributed;

import it.unimi.dsi.fastutil.ints.IntList;
import org.jetbrains.annotations.Nullable;

/**
 * The text of a {@link RockserverStateMachine.ConflictException}: which commits of a log entry conflict.
 *
 * <p>Ratis carries a state machine's refusal to the proposer as an exception that it rebuilds from its class name
 * and message, so the message is the only channel: the leader {@link #format formats} the positions of the
 * conflicting commits in the entry, and the proposer {@link #parse parses} them back. A message that does not parse
 * makes the whole entry fail as a conflict, which is safe: a conflict is a refusal before the append.</p>
 */
final class ConflictPositions {

	static final String PREFIX = "Transaction conflict, conflicting positions: ";

	private ConflictPositions() {}

	/** The message listing {@code positions}, ascending, of an entry's requests. */
	static String format(IntList positions) {
		if (positions.isEmpty()) {
			throw new IllegalArgumentException("No conflicting position");
		}
		var text = new StringBuilder(PREFIX.length() + positions.size() * 4).append(PREFIX);
		for (int i = 0; i < positions.size(); i++) {
			text.append(i == 0 ? "" : ",").append(positions.getInt(i));
		}
		return text.toString();
	}

	/**
	 * The positions listed in {@code message}, or null if it lists none, or if any of them is not a position of an
	 * entry of {@code requests} requests, or is listed twice or out of order.
	 */
	static int @Nullable [] parse(@Nullable String message, int requests) {
		int at = message == null ? -1 : message.indexOf(PREFIX);
		if (at < 0) {
			return null;
		}
		var parts = message.substring(at + PREFIX.length()).strip().split(",", -1);
		var positions = new int[parts.length];
		for (int i = 0; i < parts.length; i++) {
			try {
				positions[i] = Integer.parseInt(parts[i]);
			} catch (NumberFormatException ex) {
				return null;
			}
			if (positions[i] < 0 || positions[i] >= requests || (i > 0 && positions[i] <= positions[i - 1])) {
				return null;
			}
		}
		return positions;
	}
}
