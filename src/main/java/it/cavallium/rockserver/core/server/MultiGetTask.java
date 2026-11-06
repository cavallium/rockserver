package it.cavallium.rockserver.core.server;


import io.grpc.Context;
import io.grpc.stub.ServerCallStreamObserver;
import it.cavallium.rockserver.core.common.MultiGetPage;
import it.cavallium.rockserver.core.common.RangeBudget;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.api.proto.*;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jetbrains.annotations.Nullable;
import reactor.core.Disposable;

/** Cancellation never releases a result while the worker or transport is borrowing it. */
final class MultiGetTask implements Runnable, RWScheduler.EstimatedWork, RWScheduler.RejectionAwareTask {
	private final GrpcServer server;
	private final GetMultiRequest request;
	final ServerCallStreamObserver<MultiGetResponse> observer;
	private final Context callContext = Context.current();
	private final AtomicBoolean claimed = new AtomicBoolean();
	private final AtomicBoolean terminal = new AtomicBoolean();
	private volatile boolean cancelled;
	private volatile @Nullable Disposable scheduled;
	private ResolvedRequestContext context;
	private RocksDBAPICommand.RocksDBAPICommandSingle.GetMulti command;
	private WorkloadProfile profile;

	MultiGetTask(GrpcServer server, GetMultiRequest request, ServerCallStreamObserver<MultiGetResponse> observer) {
		this.server = server;
		this.request = request;
		this.observer = observer;
	}

	void schedule() {
		try {
			context = server.contexts.mapRequestContext(request.getContext());
			command = mapGetMultiRequest(request);
			profile = server.contexts.preAdmit(context, OperationFamily.BOUNDED_FAN_OUT, command);
			if (isCancelled()) {
				return;
			}
			var task = server.workloadScheduler(server.commandWorkloadScheduler(command), profile, command.operationFamily(),
					context.localMonotonicDeadlineNanos()).schedule(this);
			scheduled = task;
			if (isCancelled()) {
				task.dispose();
			}
		} catch (Throwable error) {
			if (claimed.compareAndSet(false, true)) {
				fail(error);
			}
		}
	}

	private boolean isCancelled() {
		return cancelled || observer.isCancelled() || callContext.isCancelled();
	}

	void cancel() {
		cancelled = true;
		var task = scheduled;
		if (task != null) {
			task.dispose();
		}
	}

	@Override
	public void run() {
		if (!claimed.compareAndSet(false, true) || isCancelled()) {
			return;
		}
		Context previous = callContext.attach();
		try {
			try (var response = createResponse()) {
				if (!isCancelled()) {
					// sendMessage consumes the marshaller stream before onNext returns.
					observer.onNext(response);
				}
			}
			if (!isCancelled() && terminal.compareAndSet(false, true)) {
				observer.onCompleted();
			}
		} catch (Throwable error) {
			fail(error);
		} finally {
			callContext.detach(previous);
		}
	}

	private MultiGetResponse createResponse() {
		if (server.embeddedDatabase == null) {
			return new MultiGetResponse(command.handleSync(server.contexts.syncApi(context)), null);
		}
		var owner = server.embeddedDatabase.getMultiPinned(command.columnId(), command.keys(), command.budget(),
				profile, context.localMonotonicDeadlineNanos(), this::isCancelled);
		try {
			return new MultiGetResponse(owner.page(), owner);
		} catch (Throwable error) {
			owner.close();
			throw error;
		}
	}

	private void fail(Throwable error) {
		if (!isCancelled() && terminal.compareAndSet(false, true)) {
			observer.onError(server.grpc.mapRequestError("getMulti", request, error));
		}
	}

	@Override
	public long estimatedBytes() {
		return command.estimatedBytes();
	}

	@Override
	public void reject(RuntimeException failure) {
		if (claimed.compareAndSet(false, true)) {
			Context previous = callContext.attach();
			try {
				fail(failure);
			} finally {
				callContext.detach(previous);
			}
		}
	}

	static RocksDBAPICommand.RocksDBAPICommandSingle.GetMulti mapGetMultiRequest(GetMultiRequest request) {
		if (!request.hasBudget()) {
			throw RocksDBException.of(RocksDBErrorType.INVALID_REQUEST, "MultiGet budget is required");
		}
		final RangeBudget budget;
		try {
			budget = new RangeBudget(
					request.getBudget().getMaxItems(), request.getBudget().getMaxBytes());
		} catch (IllegalArgumentException invalid) {
			throw RocksDBException.of(RocksDBErrorType.INVALID_REQUEST, invalid.getMessage(), invalid);
		}
		MultiGetPage.validateLimits(request.getKeysMultiCount(), budget);
		long keyBytes = 0;
		for (var tuple : request.getKeysMultiList()) {
			keyBytes += MultiGetPage.KEY_PART_BYTES * tuple.getKeysCount();
			MultiGetPage.validateKeyBytes(keyBytes);
			for (var part : tuple.getKeysList()) {
				keyBytes += part.size();
				MultiGetPage.validateKeyBytes(keyBytes);
			}
		}
		var keys = request.getKeysMultiList().stream()
				.map(tuple -> GrpcProtoMapping.mapKeys(tuple.getKeysList())).toList();
		return new RocksDBAPICommand.RocksDBAPICommandSingle.GetMulti(request.getColumnId(), keys, budget);
	}
}
