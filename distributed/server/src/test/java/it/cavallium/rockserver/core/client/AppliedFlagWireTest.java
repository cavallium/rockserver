package it.cavallium.rockserver.core.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.Metadata;
import io.grpc.Status;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * How the client reads whether a failed write was applied (CR-2): from the server's
 * {@link RocksDBException#APPLIED_METADATA_KEY} trailer, and only from a server without it, from the message. The
 * legacy messages are pinned here against the server's: rewording one must update this list on purpose. Lives in the
 * core client's package on the class path, like {@link LeaderHintRouterTest}.
 */
class AppliedFlagWireTest {

	private static final Metadata.Key<String> KEY = Metadata.Key.of(RocksDBException.APPLIED_METADATA_KEY,
			Metadata.ASCII_STRING_MARSHALLER);

	private static RocksDBException mapped(Status status, String applied) {
		var description = "RocksDBError: [uid:READ_DEADLINE_EXCEEDED] The request deadline came before the write was "
				+ "sent; it was not applied";
		Metadata trailers = null;
		if (applied != null) {
			trailers = new Metadata();
			trailers.put(KEY, applied);
		}
		var error = status.withDescription(description).asRuntimeException(trailers);
		return assertInstanceOf(RocksDBException.class, GrpcConnectionDelegate.mapGrpcStatusError(error));
	}

	@Test
	void theTrailerDecidesAndTheMessageOnlyWithoutIt() {
		var flagged = mapped(Status.DEADLINE_EXCEEDED, RocksDBException.NOT_APPLIED);
		assertEquals(RocksDBErrorType.READ_DEADLINE_EXCEEDED, flagged.getErrorUniqueId());
		assertTrue(flagged.isNotApplied());
		// A server with the trailer that says "maybe": its message does not matter.
		assertFalse(mapped(Status.DEADLINE_EXCEEDED, RocksDBException.MAYBE_APPLIED).isNotApplied());
		// An older server, without the trailer: its message.
		assertTrue(mapped(Status.DEADLINE_EXCEEDED, null).isNotApplied());
	}

	@Test
	void grpcsOwnDeadlineSaysNothing() {
		var mapped = assertInstanceOf(RocksDBException.class,
				GrpcConnectionDelegate.mapGrpcStatusError(Status.DEADLINE_EXCEEDED.asRuntimeException()));
		assertEquals(RocksDBErrorType.READ_DEADLINE_EXCEEDED, mapped.getErrorUniqueId());
		assertFalse(mapped.isNotApplied(), "the copy may have been applied");
	}

	/** The messages by which servers before the trailer said "not applied": pinned, and matched only without it. */
	@Test
	void legacyMessagesArePinned() {
		assertEquals(List.of("not applied", "already expired", "before downstream dispatch"),
				GrpcConnectionDelegate.LEGACY_NOT_APPLIED_MESSAGES);
		for (var message : List.of("Request deadline already expired",
				"Request deadline expired before downstream dispatch",
				"The request deadline came before a leader accepted the write; it was not applied")) {
			assertTrue(GrpcConnectionDelegate.saysNotApplied(null, message), message);
			var maybe = new Metadata();
			maybe.put(KEY, RocksDBException.MAYBE_APPLIED);
			assertFalse(GrpcConnectionDelegate.saysNotApplied(maybe, message), message);
		}
		assertFalse(GrpcConnectionDelegate.saysNotApplied(null, "Deadline exceeded"));
	}
}
