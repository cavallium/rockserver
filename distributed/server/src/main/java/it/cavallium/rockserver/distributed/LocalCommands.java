package it.cavallium.rockserver.distributed;

import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.CloseFailedUpdate;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.CloseTransaction;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.Delete;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.Get;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.Merge;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.OpenTransaction;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.Put;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandStream;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.RocksDBRetryException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import static it.cavallium.rockserver.distributed.ReplicatedConnection.unwrap;

/**
 * Runs commands on this node's own database, and tells whether this node leads: the steps that {@link Router},
 * {@link Forwarder}, {@link UpdateProtocol} and {@link CommitOutcomes} share.
 */
final class LocalCommands {

	private final RockserverStateMachine stateMachine;
	private final ReadBarrier barrier;
	private final NodeMetrics.Series localTime;
	/** Runs short local commands, each on a virtual thread of its own ({@link NodeThreads#local()}); see {@link #local}. */
	private final ExecutorService inline;

	LocalCommands(RockserverStateMachine stateMachine, ReadBarrier barrier, NodeMetrics metrics,
			ExecutorService inline) {
		this.stateMachine = stateMachine;
		this.barrier = barrier;
		this.inline = inline;
		this.localTime = metrics.duration("local");
	}

	/**
	 * Runs a command on the local database. Point reads, single-key staged writes and the opening of native
	 * transactions run each on a virtual thread of its own ({@link #inline}), straight on the database: the requests
	 * were admitted already ({@code GrpcServer}'s proxy scheduler), and the embedded workload scheduler's queues cost
	 * milliseconds per request under load. (A pool of 4 platform threads per core, tried instead to keep RocksDB
	 * calls off the virtual threads' carriers, stalled for seconds once slow disk reads blocked all its threads.)
	 * The command's timeout counts from here: one that waited past its deadline fails instead of running. Longer
	 * commands (ranges, multi-key reads and staged writes, iterator calls) and maintenance keep the workload
	 * scheduler, for its admission and fairness. Rollbacks never come here: {@link UpdateProtocol#closeTransaction} and
	 * {@link UpdateProtocol#closeFailedUpdate} roll back at once, on the caller's thread.
	 */
	CompletableFuture<?> local(RequestContext context, RocksDBAPICommandSingle<?> command) {
		long start = System.nanoTime();
		var connection = stateMachine.store().connection();
		CompletableFuture<?> result;
		if (isShort(command)) {
			result = CompletableFuture.supplyAsync(() -> connection.requestInline(context, start, command), inline);
		} else {
			result = (CompletableFuture<?>) connection.getAsyncApi(context).requestAsync(command);
		}
		result.whenComplete((_, _) -> localTime.since(start));
		return result;
	}

	private static boolean isShort(RocksDBAPICommandSingle<?> command) {
		return switch (command) {
			case Get<?> _, Put<?> _, Delete<?> _, Merge<?> _, OpenTransaction _ -> true;
			// CloseTransaction and CloseFailedUpdate never come here: rollbacks run at once on the caller's thread.
			default -> false;
		};
	}

	@SuppressWarnings("unchecked")
	Flux<Object> localStream(RequestContext context, RocksDBAPICommandStream<?> command) {
		var stream = Flux.from((Publisher<Object>) stateMachine.store().connection().getAsyncApi(context).requestAsync(command));
		// A column deletion ends the streams of the column instead of waiting for them (DD-7a).
		Long column = switch (command) {
			case RocksDBAPICommandStream.GetRange<?> c -> c.columnId();
			case RocksDBAPICommandStream.ScanRaw c -> c.columnId();
			case RocksDBAPICommandStream.ScanRawResumable c -> c.columnId();
			default -> null;
		};
		return column == null ? stream : stateMachine.columnStreams().track(column, stream);
	}

	/**
	 * {@link ReadBarrier#awaitLeadership}, completed on a virtual thread of {@link #inline}: the barrier completes its
	 * futures on the node's few control threads, which must never wait for the database, and its continuations here
	 * read the database, open transactions or start streams.
	 */
	CompletableFuture<Void> leadership() {
		return barrier.awaitLeadership().whenCompleteAsync((_, _) -> {}, inline);
	}

	long currentTerm() {
		var division = stateMachine.division();
		return division != null ? division.getInfo().getCurrentTerm() : -1;
	}

	LeaderSession currentSession() {
		var division = stateMachine.division();
		if (division == null || !division.getInfo().isLeader() || !division.getInfo().isLeaderReady()) {
			throw new NotLeader();
		}
		var session = stateMachine.leaderSession(division.getInfo().getCurrentTerm());
		if (session == null) {
			throw new NotLeader();
		}
		session.expireIfDue();
		return session;
	}

	/** This node is the leader and may serve leader-only requests. */
	boolean isLeader() {
		var division = stateMachine.division();
		return division != null && division.getInfo().isLeader() && division.getInfo().isLeaderReady()
				&& !stateMachine.isFailed();
	}

	/** Thrown when leadership was lost while routing a leader-bound request. */
	static final class NotLeader extends RocksDBRetryException {}

	static RuntimeException barrierFailure(Throwable error) {
		var cause = unwrap(error);
		if (cause instanceof ReadBarrier.NotLeaderException) {
			return new RocksDBRetryException();
		}
		if (cause instanceof TimeoutException) {
			return RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED, "Could not confirm leadership in time");
		}
		if (cause instanceof ReadBarrier.ApplyLagException lag) {
			return RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED, lag.getMessage());
		}
		if (cause instanceof RuntimeException runtime) {
			return runtime;
		}
		return RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, String.valueOf(cause));
	}
}
