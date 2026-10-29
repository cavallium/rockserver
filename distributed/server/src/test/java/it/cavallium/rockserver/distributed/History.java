package it.cavallium.rockserver.distributed;

import it.cavallium.rockserver.distributed.LinearizabilityChecker.Operation;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.jetbrains.annotations.Nullable;

/**
 * Thread-safe recorder of register operations for {@link LinearizabilityChecker}. Call {@link #invokePut} or
 * {@link #invokeGet} right before sending a request and complete the returned {@link Pending} right after its
 * reply. Timestamps come from {@link System#nanoTime()}. Operations never completed are reported as
 * {@link OpStatus#UNKNOWN}.
 */
public final class History {

	private final List<Pending> pending = new ArrayList<>();

	public Pending invokePut(String key, String value) {
		Objects.requireNonNull(value, "value");
		return register(new Pending(key, Operation.Kind.PUT, value, System.nanoTime()));
	}

	public Pending invokeGet(String key) {
		return register(new Pending(key, Operation.Kind.GET, null, System.nanoTime()));
	}

	/** Every recorded operation, in invocation order. */
	public synchronized List<Operation> operations() {
		return pending.stream().map(Pending::toOperation).toList();
	}

	private synchronized Pending register(Pending operation) {
		pending.add(operation);
		return operation;
	}

	/** An invoked operation, completed exactly once. */
	public static final class Pending {

		private final String key;
		private final Operation.Kind kind;
		private final long invokeNanos;
		private @Nullable String value;
		private long completeNanos;
		private @Nullable OpStatus status;

		private Pending(String key, Operation.Kind kind, @Nullable String value, long invokeNanos) {
			this.key = Objects.requireNonNull(key, "key");
			this.kind = kind;
			this.value = value;
			this.invokeNanos = invokeNanos;
		}

		/** Completes a put. */
		public void complete(OpStatus status) {
			if (kind != Operation.Kind.PUT) {
				throw new IllegalStateException("A get completes with the value it read");
			}
			finish(status, value);
		}

		/** Completes a get that read {@code readValue} (null = absent); the value is ignored unless {@code OK}. */
		public void complete(OpStatus status, @Nullable String readValue) {
			if (kind != Operation.Kind.GET) {
				throw new IllegalStateException("A put completes without a value");
			}
			finish(status, status == OpStatus.OK ? readValue : null);
		}

		private synchronized void finish(OpStatus status, @Nullable String value) {
			long now = System.nanoTime();
			if (this.status != null) {
				throw new IllegalStateException("Operation already completed");
			}
			this.status = Objects.requireNonNull(status, "status");
			this.value = value;
			this.completeNanos = now;
		}

		private synchronized Operation toOperation() {
			var effectiveStatus = status != null ? status : OpStatus.UNKNOWN;
			long complete = effectiveStatus == OpStatus.UNKNOWN ? Long.MAX_VALUE : completeNanos;
			return new Operation(key, kind, value, invokeNanos, complete, effectiveStatus);
		}
	}
}
