package it.cavallium.rockserver.distributed;

import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.RocksDBRetryException;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.TimeDuration;

/**
 * Submits log entries to the current leader through a Ratis client, from any node.
 *
 * <p>The Ratis client finds the leader and retries on leader changes with the same call id, so the leader's retry
 * cache turns a retried request into one log entry. The reply carries the {@link ApplyResult} computed when the
 * leader applied the entry.</p>
 */
final class Proposer implements Closeable {

	/** The leader rejected a transaction commit because one of its keys changed after its snapshot. */
	static final class ConflictException extends RocksDBRetryException {}

	private static final long NOT_READY_RETRY_MS = 50;

	private final RaftClient client;
	private final long requestTimeoutNanos;

	Proposer(RaftGroup group, RaftProperties properties, NodeConfig config) {
		this.requestTimeoutNanos = config.requestTimeout().toNanos();
		long sleepMs = 100;
		int attempts = (int) Math.max(1, config.requestTimeout().toMillis() / sleepMs);
		this.client = RaftClient.newBuilder()
				.setClientId(ClientId.randomId())
				.setRaftGroup(group)
				.setProperties(properties)
				.setRetryPolicy(RetryPolicies.retryUpToMaximumCountWithFixedSleep(attempts,
						TimeDuration.valueOf(sleepMs, TimeUnit.MILLISECONDS)))
				.build();
	}

	RaftClient client() {
		return client;
	}

	/**
	 * Proposes {@code entry} and completes with its result. Fails with a {@link RocksDBException}: the result's own
	 * error, {@link ConflictException} for transaction conflicts, {@link RocksDBRetryException} for stale
	 * transactions, or
	 * {@code SERVER_OVERLOADED} when no leader accepted it in time (then the outcome is unknown).
	 */
	CompletableFuture<Object> propose(LogEntry entry) {
		byte[] data;
		try {
			data = LogEntryCodec.encode(entry);
		} catch (IllegalArgumentException ex) {
			return CompletableFuture.failedFuture(RocksDBException.of(RocksDBErrorType.INVALID_REQUEST, ex.getMessage()));
		}
		if (data.length > RaftSettings.MAX_ENTRY_BYTES) {
			return CompletableFuture.failedFuture(RocksDBException.of(RocksDBErrorType.INVALID_REQUEST,
					"Write too large for one replicated entry: " + data.length + " bytes"));
		}
		var message = Message.valueOf(ByteString.copyFrom(data));
		return send(message, System.nanoTime() + requestTimeoutNanos);
	}

	private CompletableFuture<Object> send(Message message, long deadlineNanos) {
		return client.async().send(message).handle((reply, error) -> {
			// Ratis reports state machine refusals either in the reply or as the future's failure.
			var refusal = error != null ? stateMachineException(error)
					: !reply.isSuccess() && reply.getException() instanceof StateMachineException e ? e : null;
			if (refusal != null && isNotReady(refusal) && System.nanoTime() - deadlineNanos < 0) {
				// Refused before being appended: resending cannot apply it twice.
				return CompletableFuture.supplyAsync(() -> send(message, deadlineNanos),
						CompletableFuture.delayedExecutor(NOT_READY_RETRY_MS, TimeUnit.MILLISECONDS)).thenCompose(f -> f);
			}
			if (refusal != null) {
				throw new CompletionException(admissionFailure(refusal));
			}
			if (error != null) {
				throw new CompletionException(unavailable(error));
			}
			return CompletableFuture.completedFuture(toResult(reply));
		}).thenCompose(f -> f);
	}

	private static StateMachineException stateMachineException(Throwable error) {
		for (Throwable t = error; t != null; t = t.getCause()) {
			if (t instanceof StateMachineException exception) {
				return exception;
			}
		}
		return null;
	}

	private static boolean isNotReady(StateMachineException exception) {
		return exception.getCause() != null
				&& exception.getCause().getClass().getName().equals(RockserverStateMachine.NotReadyException.class.getName());
	}

	private static Object toResult(RaftClientReply reply) {
		if (!reply.isSuccess()) {
			var exception = reply.getException();
			if (exception instanceof StateMachineException stateMachineException) {
				throw new CompletionException(admissionFailure(stateMachineException));
			}
			throw new CompletionException(unavailable(exception));
		}
		var result = ResultCodec.decode(reply.getMessage().getContent().toByteArray());
		return switch (result) {
			case ApplyResult.Ok ok -> ok.value();
			case ApplyResult.Failed failed -> throw new CompletionException(failed.toException());
		};
	}

	/** The leader refused to append the entry: a transaction conflict, a stale transaction, or a malformed entry. */
	private static RocksDBException admissionFailure(StateMachineException exception) {
		var cause = exception.getCause();
		String type = cause != null ? cause.getClass().getName() : "";
		String message = cause != null ? String.valueOf(cause.getMessage()) : String.valueOf(exception.getMessage());
		if (type.equals(RockserverStateMachine.ConflictException.class.getName())) {
			return new ConflictException();
		}
		if (type.equals(RockserverStateMachine.RetryableException.class.getName())
				|| type.equals(RockserverStateMachine.NotReadyException.class.getName())) {
			return new RocksDBRetryException();
		}
		return RocksDBException.of(RocksDBErrorType.INVALID_REQUEST, message);
	}

	static RocksDBException unavailable(Throwable error) {
		var cause = error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
		return RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED,
				"Replicated write not acknowledged, its outcome is unknown: " + cause);
	}

	@Override
	public void close() throws IOException {
		client.close();
	}
}
