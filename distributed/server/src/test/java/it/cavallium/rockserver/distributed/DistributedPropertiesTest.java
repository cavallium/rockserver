package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

/** Property names of a replicated node: one prefix, the old names still read (CR-17). */
@ResourceLock(Resources.SYSTEM_PROPERTIES)
class DistributedPropertiesTest {

	private static final String NEW = DistributedProperties.PREFIX + "test-property";
	private static final String OLD = "rockserver.test-property";

	@AfterEach
	void clear() {
		System.clearProperty(NEW);
		System.clearProperty(OLD);
	}

	@Test
	void theNewNameWinsAndTheOldOneIsAnAlias() {
		assertNull(DistributedProperties.get(NEW, OLD));
		assertEquals(7, DistributedProperties.getLong(7, NEW, OLD));
		System.setProperty(OLD, "11");
		assertEquals("11", DistributedProperties.get(NEW, OLD));
		assertEquals(11, DistributedProperties.getLong(7, NEW, OLD));
		System.setProperty(NEW, "13");
		assertEquals(13, DistributedProperties.getLong(7, NEW, OLD));
	}

	@Test
	void everyReplicatedPropertyHasThePrefix() {
		for (var name : new String[] {Proposer.WRITE_ADMISSION_BYTES_PROPERTY, ReplicatedConnection.SESSION_TIMEOUT_PROPERTY,
				Router.FOLLOWER_READS_PROPERTY}) {
			assertTrue(name.startsWith(DistributedProperties.PREFIX), name);
		}
	}
}
