package it.cavallium.rockserver.core.common;

/**
 * The identity of one write of a client session on a replicated server, which makes a resent write take effect at
 * most once (Raft dissertation, section 6.3). The session is opened through the replicated log; its writes are
 * numbered by the client, and every copy of a write keeps its number, so the server returns the result of the first
 * copy that took effect instead of applying it again.
 *
 * @param sessionId the session, as returned by the server when it was opened
 * @param sequence number of this write within the session
 * @param completedBelow every write of the session numbered below this has its final answer: the client never sends
 *                       any of them again, so the server may forget their results
 */
public record ClientRequestId(long sessionId, long sequence, long completedBelow) {

	public ClientRequestId {
		if (sessionId <= 0 || sequence < 0 || completedBelow < 0 || completedBelow > sequence) {
			throw new IllegalArgumentException("Invalid client request id: session " + sessionId + ", sequence "
					+ sequence + ", completed below " + completedBelow);
		}
	}
}
