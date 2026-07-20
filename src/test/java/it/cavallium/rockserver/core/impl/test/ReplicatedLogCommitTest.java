package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.rockserver.core.cluster.command.ReplicatedLogCommit;

import org.junit.jupiter.api.Test;

class ReplicatedLogCommitTest {

    @Test
    void comparesAndHashesResponsePayloadByValue() {
        var payload = new byte[]{1, 2, 3};
        var commit = new ReplicatedLogCommit(1, true, payload);
        payload[0] = 9;

        assertArrayEquals(new byte[]{1, 2, 3}, commit.responsePayload());
        assertEquals(new ReplicatedLogCommit(1, true, new byte[]{1, 2, 3}), commit);
        assertEquals(new ReplicatedLogCommit(1, true, new byte[]{1, 2, 3}).hashCode(), commit.hashCode());

        var returnedPayload = commit.responsePayload();
        returnedPayload[0] = 9;
        assertArrayEquals(new byte[]{1, 2, 3}, commit.responsePayload());
    }

    @Test
    void committedResultsRequirePositiveLogIndex() {
        var failure = assertThrows(IllegalArgumentException.class,
                () -> new ReplicatedLogCommit(0, true));

        assertEquals("committed log index must be > 0", failure.getMessage());
        assertFalse(new ReplicatedLogCommit(0, false).committed());
    }
}
