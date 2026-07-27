package it.cavallium.rockserver.core.client;

import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType.RequestTypeId;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.UpdateContext;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Enforces the workload profile inherited by server-side transactions, update handles,
 * and iterators. The registry lives on the connection rather than an API view, so a
 * caller cannot change a resource's profile by asking the same connection for another
 * view.
 */
final class ResourceProfileBindings {

	private final ConcurrentHashMap<Long, WorkloadProfile> transactions = new ConcurrentHashMap<>();
	private final ConcurrentHashMap<Long, WorkloadProfile> updates = new ConcurrentHashMap<>();
	private final ConcurrentHashMap<Long, WorkloadProfile> iterators = new ConcurrentHashMap<>();

	void before(RequestContext context, RocksDBAPICommand<?, ?, ?> command) {
		Objects.requireNonNull(context, "context");
		Objects.requireNonNull(command, "command");
		switch (command) {
			case RocksDBAPICommand.RocksDBAPICommandSingle.CloseTransaction _ -> {
				// Rollback is protected CONTROL work and must remain idempotent even when
				// the caller is recovering a handle that this connection did not open.
				// Commit is validated by the server against the real transaction state.
			}
			case RocksDBAPICommand.RocksDBAPICommandSingle.Get<?> get ->
					requireTransactionOrUpdate(get.transactionOrUpdateId(), context.profile());
			case RocksDBAPICommand.RocksDBAPICommandSingle.Put<?> put ->
					requireTransactionOrUpdate(put.transactionOrUpdateId(), context.profile());
			case RocksDBAPICommand.RocksDBAPICommandSingle.Delete<?> delete ->
					requireTransactionOrUpdate(delete.transactionOrUpdateId(), context.profile());
			case RocksDBAPICommand.RocksDBAPICommandSingle.DeleteMulti<?> delete ->
					requireTransactionOrUpdate(delete.transactionOrUpdateId(), context.profile());
			case RocksDBAPICommand.RocksDBAPICommandSingle.PutMulti<?> put ->
					requireTransactionOrUpdate(put.transactionOrUpdateId(), context.profile());
			case RocksDBAPICommand.RocksDBAPICommandSingle.Merge<?> merge ->
					requireTransactionOrUpdate(merge.transactionOrUpdateId(), context.profile());
			case RocksDBAPICommand.RocksDBAPICommandSingle.MergeMulti<?> merge ->
					requireTransactionOrUpdate(merge.transactionOrUpdateId(), context.profile());
			case RocksDBAPICommand.RocksDBAPICommandSingle.OpenIterator open ->
					require(transactions, open.transactionId(), context.profile(), "transaction");
			case RocksDBAPICommand.RocksDBAPICommandSingle.SeekTo seek ->
					require(iterators, seek.iterationId(), context.profile(), "iterator");
			case RocksDBAPICommand.RocksDBAPICommandSingle.Subsequent<?> subsequent ->
					require(iterators, subsequent.iterationId(), context.profile(), "iterator");
			case RocksDBAPICommand.RocksDBAPICommandSingle.ReduceRange<?> reduce ->
					require(transactions, reduce.transactionId(), context.profile(), "transaction");
			case RocksDBAPICommand.RocksDBAPICommandStream.GetRange<?> range ->
					require(transactions, range.transactionId(), context.profile(), "transaction");
			default -> {
				// Stateless commands have no inherited profile to check.
			}
		}
	}

	void after(RequestContext context, RocksDBAPICommand<?, ?, ?> command, Object result) {
		switch (command) {
			case RocksDBAPICommand.RocksDBAPICommandSingle.OpenTransaction _ ->
					bind(transactions, (Long) result, context.profile(), "transaction");
			case RocksDBAPICommand.RocksDBAPICommandSingle.CloseTransaction close -> {
				if (Boolean.TRUE.equals(result) || !close.commit()) {
					transactions.remove(close.transactionId());
				}
			}
			case RocksDBAPICommand.RocksDBAPICommandSingle.Get<?> get -> {
				if (get.requestType().getRequestTypeId() == RequestTypeId.FOR_UPDATE
						&& result instanceof UpdateContext<?> update) {
					bind(updates, update.updateId(), context.profile(), "update");
				}
			}
			case RocksDBAPICommand.RocksDBAPICommandSingle.CloseFailedUpdate close ->
					updates.remove(close.updateId());
			case RocksDBAPICommand.RocksDBAPICommandSingle.OpenIterator _ ->
					bind(iterators, (Long) result, context.profile(), "iterator");
			case RocksDBAPICommand.RocksDBAPICommandSingle.CloseIterator close ->
					iterators.remove(close.iteratorId());
			default -> {
				// No lifecycle transition.
			}
		}
	}

	private void requireTransactionOrUpdate(long id, WorkloadProfile requested) {
		if (id == 0L) {
			return;
		}
		var transaction = transactions.get(id);
		var update = updates.get(id);
		if (transaction != null && update != null && transaction != update) {
			throw mismatch("resource", id, requested, transaction);
		}
		var bound = transaction != null ? transaction : update;
		if (bound == null) {
			throw RocksDBException.of(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST,
					"Unknown transaction or update handle " + id);
		}
		if (bound != requested) {
			throw mismatch("transaction or update", id, requested, bound);
		}
	}

	private static void require(ConcurrentHashMap<Long, WorkloadProfile> bindings,
			long id,
			WorkloadProfile requested,
			String resourceType) {
		if (id == 0L) {
			return;
		}
		var bound = bindings.get(id);
		if (bound == null) {
			throw RocksDBException.of(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST,
					"Unknown " + resourceType + " " + id);
		}
		if (bound != requested) {
			throw mismatch(resourceType, id, requested, bound);
		}
	}

	private static void bind(ConcurrentHashMap<Long, WorkloadProfile> bindings,
			long id,
			WorkloadProfile profile,
			String resourceType) {
		var previous = bindings.putIfAbsent(id, profile);
		if (previous != null && previous != profile) {
			throw mismatch(resourceType, id, profile, previous);
		}
	}

	private static RocksDBException mismatch(String resourceType,
			long id,
			WorkloadProfile requested,
			WorkloadProfile bound) {
		return RocksDBException.of(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST,
				"Cannot change " + resourceType + " " + id + " from " + bound + " to " + requested);
	}
}
