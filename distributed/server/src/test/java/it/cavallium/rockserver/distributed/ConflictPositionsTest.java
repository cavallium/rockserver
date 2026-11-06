package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.junit.jupiter.api.Test;

class ConflictPositionsTest {

	@Test
	void positionsSurviveTheExceptionThatRatisRebuilds() {
		for (var positions : new int[][] {{0}, {3}, {0, 1, 2}, {1, 7, 4095}}) {
			var exception = RockserverStateMachine.ConflictException.of(IntList.of(positions));
			// Ratis rebuilds the cause on the proposer from its class name and message.
			var rebuilt = new RockserverStateMachine.ConflictException(exception.getMessage());
			assertArrayEquals(positions, ConflictPositions.parse(rebuilt.getMessage(), 4096));
		}
	}

	@Test
	void anyPrefixBeforeThePositionsIsIgnored() {
		var message = "it.cavallium.Something: " + ConflictPositions.format(IntList.of(2, 5));
		assertArrayEquals(new int[] {2, 5}, ConflictPositions.parse(message, 6));
	}

	@Test
	void whatDoesNotParseFailsTheWholeEntryAsAConflict() {
		assertNull(ConflictPositions.parse(null, 4));
		assertNull(ConflictPositions.parse("Transaction conflict", 4));
		assertNull(ConflictPositions.parse(ConflictPositions.PREFIX, 4));
		assertNull(ConflictPositions.parse(ConflictPositions.PREFIX + "1,x", 4));
		assertNull(ConflictPositions.parse(ConflictPositions.PREFIX + "1,,2", 4));
		// Not a position of this entry: the batch would otherwise fail with an unknown outcome.
		assertNull(ConflictPositions.parse(ConflictPositions.PREFIX + "4", 4));
		assertNull(ConflictPositions.parse(ConflictPositions.PREFIX + "-1", 4));
		assertNull(ConflictPositions.parse(ConflictPositions.PREFIX + "2,2", 4));
		assertNull(ConflictPositions.parse(ConflictPositions.PREFIX + "2,1", 4));
	}

	@Test
	void noPositionIsNoConflict() {
		assertThrows(IllegalArgumentException.class, () -> ConflictPositions.format(new IntArrayList()));
	}
}
