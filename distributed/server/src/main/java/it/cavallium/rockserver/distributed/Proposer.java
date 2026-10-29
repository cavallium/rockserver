package it.cavallium.rockserver.distributed;

import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.RocksDBRetryException;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Submits log entries to the current leader through a Ratis client, from any node.
 *
 * <p>The Ratis client finds the leader and retries on leader changes with the same call id, so the leader's retry
 * cache turns a retried request into one log entry. The reply carries the {@link ApplyResult} computed when the
 * leader applied the entry.</p>
 *
 * <p>Ratis sends the async requests of one client as an ordered stream, and fails that stream for good as soon as
 * one request completes exceptionally (its retries ran out, for example while this node is cut off from the
 * leader): every later request of that client then fails at once. So a client whose request failed is replaced by
 * a new one, and closed once the earlier requests it may still be retrying have had their time.</p>
 */
final class Proposer implements Closeable {

	/** The leader rejected a transaction commit because one of its keys changed after its snapshot. */
	static final class ConflictException extends RocksDBRetryException {}

	private static final Logger LOG = LoggerFactory.getLogger(Proposer.class);
	private static final long NOT_READY_RETRY_MS = 50;
	private static final long RETRY_SLEEP_MS = 100;

	private final RaftGroup group;
	private final RaftProperties properties;
	private final NodeConfig config;
	private final long requestTimeoutNanos;
	/** Replaced clients, not closed yet. */
	private final Set<RaftClient> retired = ConcurrentHashMap.newKeySet();
	private volatile RaftClient client;
	private boolean closed;

	Proposer(RaftGroup group, RaftProperties properties, NodeConfig config) {
		this.group = group;
		this.properties = properties;
		this.config = config;
		this.requestTimeoutNanos = config.requestTimeout().toNanos();
		this.client = newClient();
	}

	private RaftClient newClient() {
		int attempts = (int) Math.max(1, config.requestTimeout().toMillis() / RETRY_SLEEP_MS);
		return RaftClient.newBuilder()
				.setClientId(ClientId.randomId())
				.setRaftGroup(group)
				.setProperties(properties)
				.setRetryPolicy(RetryPolicies.retryUpToMaximumCountWithFixedSleep(attempts,
						TimeDuration.valueOf(RETRY_SLEEP_MS, TimeUnit.MILLISECONDS)))
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
		var sender = client;
		return sender.async().send(message).handle((reply, error) -> {
			if (error != null) {
				// The failure also ended the client's ordered request stream: later requests need a new client.
				replace(sender, error);
			}
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

	/** Replaces {@code failed} with a new client, unless that happened already. */
	private synchronized void replace(RaftClient failed, Throwable error) {
		if (closed || client != failed) {
			return;
		}
		var cause = error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
		LOG.warn("{}: Raft client {} failed a request ({}), replacing it", config.nodeId(), failed.getId(),
				cause.toString());
		client = newClient();
		retired.add(failed);
		// Earlier requests of the failed client may still be retrying: give them their time before closing it.
		CompletableFuture.runAsync(() -> closeRetired(failed),
				CompletableFuture.delayedExecutor(2 * requestTimeoutNanos, TimeUnit.NANOSECONDS));
	}

	private void closeRetired(RaftClient retiredClient) {
		if (!retired.remove(retiredClient)) {
			return;
		}
		try {
			retiredClient.close();
		} catch (IOException | RuntimeException ex) {
			LOG.warn("{}: failed to close replaced Raft client {}", config.nodeId(), retiredClient.getId(), ex);
		}
	}

	@Override
	public void close() throws IOException {
		synchronized (this) {
			closed = true;
		}
		List.copyOf(retired).forEach(this::closeRetired);
		client.close();
	}
}
