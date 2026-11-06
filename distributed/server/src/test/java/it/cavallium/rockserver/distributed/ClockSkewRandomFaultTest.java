package it.cavallium.rockserver.distributed;

import java.util.List;

/**
 * {@link RandomFaultTest} with skewed wall clocks: node 1 runs {@value #SKEW_MILLIS} ms ahead, node 2 as much
 * behind, node 3 on time (see {@code RockserverStateMachine.CLOCK_SKEW_PROPERTY}). Every leader change moves the
 * replicated clock that session expiry reads forward by the skew, or freezes it until the new leader's clock catches
 * up; reads, leases and timeouts run on monotonic time and must not care. The skew stays below the shortest client
 * session timeout (10 minutes), so no session may expire, and every check of the base test must still hold.
 */
class ClockSkewRandomFaultTest extends RandomFaultTest {

	static final long SKEW_MILLIS = 90_000;

	@Override
	List<String> jvmOptions(String nodeId) {
		long skew = switch (nodeId) {
			case "1" -> SKEW_MILLIS;
			case "2" -> -SKEW_MILLIS;
			default -> 0;
		};
		return List.of("-D" + RockserverStateMachine.CLOCK_SKEW_PROPERTY + "=" + skew);
	}
}
