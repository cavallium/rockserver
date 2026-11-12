package it.cavallium.rockserver.distributed;

import org.junit.jupiter.api.Tag;

/**
 * {@link RandomFaultTest} with overlapping faults: a second fault starts while the first one holds, for example a
 * pause plus a one-way cut, or two kills (the majority lost, then back). Availability is not asserted, as the
 * majority may be gone for a while; linearizability, the bank invariant, the list-append checker, monotonic terms
 * and identical replicas still are.
 */
@Tag("slow")
class OverlappingRandomFaultTest extends RandomFaultTest {

	@Override
	boolean overlapping() {
		return true;
	}
}
