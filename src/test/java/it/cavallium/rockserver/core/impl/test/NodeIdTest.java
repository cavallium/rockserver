package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.rockserver.core.cluster.NodeId;

import org.junit.jupiter.api.Test;

class NodeIdTest {

    @Test
    void preservesLiteralNodeIdentifiers() {
        assertEquals("node/1", NodeId.of("node/1").value());
        assertEquals("node/1", NodeId.of("node/1").toString());
    }

    @Test
    void rejectsBlankOrLineBrokenNodeIds() {
        assertThrows(IllegalArgumentException.class, () -> NodeId.of(" "));
        assertThrows(IllegalArgumentException.class, () -> NodeId.of("n1\ninjected=true"));
        assertThrows(IllegalArgumentException.class, () -> NodeId.of("n1\rinjected=true"));
    }
}
