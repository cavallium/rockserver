package it.cavallium.rockserver.core.server;


import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.api.proto.*;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.util.context.ContextView;

/** Maps request failures to gRPC statuses, and logs the ones that arrive after their call has ended. */
final class GrpcRequestErrors {

	private static final Logger LOG = LoggerFactory.getLogger(GrpcServer.class.getName());

	private GrpcRequestErrors() {
	}

	static final String GRPC_LATE_ERROR_HANDLER_CONTEXT_KEY
			= GrpcServer.class.getName() + ".lateErrorHandler";

	// Reactor exposes sequence-local dropped-error handling through this documented context key, while
	// keeping Hooks.KEY_ON_ERROR_DROPPED package-private.
	static final String REACTOR_ON_ERROR_DROPPED_CONTEXT_KEY = "reactor.onErrorDropped.local";

	static final Consumer<Throwable> UNCONTEXTUALIZED_LATE_ERROR_HANDLER
			= GrpcRequestErrors::logUncontextualizedLateError;

	static final long LATE_READ_DEADLINE_LOG_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(1);

	static final ConcurrentMap<String, LateReadDeadlineLogState> LATE_READ_DEADLINE_LOG_STATES
			= new ConcurrentHashMap<>();

	static void logUncontextualizedLateError(Throwable error) {
		var rocksError = findRocksDBException(error);
		var status = Status.fromThrowable(error);
		if (rocksError != null) {
			var statusCode = lateErrorStatusCode(status, rocksError);
			Long suppressed = rocksError.getErrorUniqueId() == RocksDBErrorType.READ_DEADLINE_EXCEEDED
					? claimLateReadDeadlineLog("<uncontextualized>")
					// Boxed: beside a primitive 0L the claim is unboxed, and its null (a suppressed line) threw.
					: Long.valueOf(0L);
			if (suppressed == null) {
				return;
			}
			if (suppressed == 0L) {
				LOG.warn("Late gRPC request failure after call termination without request context: "
						+ "errorType={}, grpcStatus={}, message={}",
						rocksError.getErrorUniqueId(),
						statusCode,
						sanitizeForLog(rocksError.getMessage()));
			} else {
				LOG.warn("Late gRPC request failure after call termination without request context: "
						+ "errorType={}, grpcStatus={}, message={}, suppressedSimilarFailures={}",
						rocksError.getErrorUniqueId(),
						statusCode,
						sanitizeForLog(rocksError.getMessage()),
						suppressed);
			}
			LOG.debug("Late gRPC request failure stack without request context", error);
			return;
		}
		if (status.getCode() == Code.CANCELLED
				|| error instanceof CancellationException) {
			LOG.debug("Late gRPC cancellation after call termination without request context: description={}",
					sanitizeForLog(status.getDescription()));
			return;
		}
		if (status.getCode() != Code.UNKNOWN && status.getCode() != Code.INTERNAL) {
			LOG.warn("Late gRPC transport failure after call termination without request context: "
					+ "grpcStatus={}, description={}",
					status.getCode(),
					sanitizeForLog(status.getDescription()));
			LOG.debug("Late gRPC transport failure stack without request context", error);
			return;
		}
		LOG.error("Unexpected late gRPC request failure after call termination; request context is unavailable",
				error);
	}

	static @Nullable Long claimLateReadDeadlineLog(String operation) {
		var state = LATE_READ_DEADLINE_LOG_STATES.computeIfAbsent(operation,
				_ -> new LateReadDeadlineLogState());
		long now = System.nanoTime();
		long previous = state.lastLogNanos.get();
		if ((previous == 0L || now - previous >= LATE_READ_DEADLINE_LOG_INTERVAL_NANOS)
				&& state.lastLogNanos.compareAndSet(previous, now)) {
			return state.suppressed.getAndSet(0L);
		}
		state.suppressed.incrementAndGet();
		return null;
	}

	static Code lateErrorStatusCode(Status status, RocksDBException rocksError) {
		if (rocksError.getErrorUniqueId() == RocksDBErrorType.READ_DEADLINE_EXCEEDED) {
			return Code.DEADLINE_EXCEEDED;
		}
		return status.getCode() == Code.UNKNOWN ? Code.INTERNAL : status.getCode();
	}

	static final class LateReadDeadlineLogState {

		private final AtomicLong lastLogNanos = new AtomicLong();
		private final AtomicLong suppressed = new AtomicLong();
	}

	static Consumer<Throwable> lateErrorHandler(ContextView context) {
		return context.getOrDefault(GRPC_LATE_ERROR_HANDLER_CONTEXT_KEY,
				UNCONTEXTUALIZED_LATE_ERROR_HANDLER);
	}

	static Consumer<Throwable> lateRequestErrorHandler(String requestName, Message request) {
		return error -> logLateRequestError(requestName, request, error);
	}

	static void logLateRequestError(String requestName, Message request, Throwable error) {
		var requestType = request.getDescriptorForType().getFullName();
		var requestSummary = summarizeRequest(request);
		var rocksDBException = findRocksDBException(error);
		var status = Status.fromThrowable(error);

		if (rocksDBException != null) {
			var statusCode = lateErrorStatusCode(status, rocksDBException);
			Long suppressed = rocksDBException.getErrorUniqueId() == RocksDBErrorType.READ_DEADLINE_EXCEEDED
					? claimLateReadDeadlineLog(requestName)
					// Boxed, see logUncontextualizedLateError.
					: Long.valueOf(0L);
			if (suppressed == null) {
				return;
			}
			if (suppressed == 0L) {
				LOG.warn("Late gRPC request failure after call termination: operation={}, requestType={}, "
						+ "request={}, errorType={}, grpcStatus={}, message={}",
						requestName,
						requestType,
						requestSummary,
						rocksDBException.getErrorUniqueId(),
						statusCode,
						sanitizeForLog(rocksDBException.getMessage()));
			} else {
				LOG.warn("Late gRPC request failure after call termination: operation={}, requestType={}, "
						+ "request={}, errorType={}, grpcStatus={}, message={}, suppressedSimilarFailures={}",
						requestName,
						requestType,
						requestSummary,
						rocksDBException.getErrorUniqueId(),
						statusCode,
						sanitizeForLog(rocksDBException.getMessage()),
						suppressed);
			}
			LOG.debug("Late gRPC request failure stack: operation={}, requestType={}",
					requestName,
					requestType,
					error);
			return;
		}

		if (status.getCode() == Code.CANCELLED
				|| error instanceof CancellationException) {
			LOG.debug("Late gRPC cancellation after call termination: operation={}, requestType={}, request={}, "
					+ "description={}",
					requestName,
					requestType,
					requestSummary,
					sanitizeForLog(status.getDescription()));
			return;
		}

		if (status.getCode() != Code.UNKNOWN && status.getCode() != Code.INTERNAL) {
			LOG.warn("Late gRPC transport failure after call termination: operation={}, requestType={}, request={}, "
					+ "grpcStatus={}, description={}",
					requestName,
					requestType,
					requestSummary,
					status.getCode(),
					sanitizeForLog(status.getDescription()));
			LOG.debug("Late gRPC transport failure stack: operation={}, requestType={}",
					requestName,
					requestType,
					error);
			return;
		}

		LOG.error("Unexpected late gRPC request failure after call termination: operation={}, requestType={}, "
				+ "request={}, grpcStatus={}, description={}",
				requestName,
				requestType,
				requestSummary,
				status.getCode(),
				sanitizeForLog(status.getDescription()),
				error);
	}

	@Nullable
	static RocksDBException findRocksDBException(Throwable error) {
		var current = error;
		for (int depth = 0; current != null && depth < 32; depth++) {
			if (current instanceof RocksDBException rocksDBException) {
				return rocksDBException;
			}
			var cause = current.getCause();
			if (cause == current) {
				break;
			}
			current = cause;
		}
		return null;
	}

	static String summarizeRequest(Message request) {
		try {
			return summarizeMessage(request, 0);
		} catch (Throwable summaryError) {
			return "{summaryUnavailable=" + summaryError.getClass().getSimpleName() + "}";
		}
	}

	static String summarizeMessage(Message message, int depth) {
		var fields = new StringJoiner(", ", "{", "}");
		for (FieldDescriptor field : message.getDescriptorForType().getFields()) {
			var fieldName = field.getJsonName();
			var value = message.getField(field);
			if (field.isRepeated()) {
				fields.add(fieldName + ".count=" + ((List<?>) value).size());
				continue;
			}
			if (field.hasPresence() && !message.hasField(field)) {
				continue;
			}
			fields.add(fieldName + "=" + summarizeFieldValue(field, value, depth));
		}
		return fields.toString();
	}

	static String summarizeFieldValue(FieldDescriptor field, Object value, int depth) {
		return switch (field.getJavaType()) {
			case BYTE_STRING -> "bytes(" + ((ByteString) value).size() + ")";
			case MESSAGE -> depth < 1
					? summarizeMessage((Message) value, depth + 1)
					: "message(" + ((Message) value).getDescriptorForType().getFullName() + ")";
			case STRING -> "\"" + sanitizeForLog(String.valueOf(value)) + "\"";
			default -> String.valueOf(value);
		};
	}

	static String sanitizeForLog(@Nullable String value) {
		if (value == null) {
			return "<none>";
		}
		var sanitized = value
				.replace("\\", "\\\\")
				.replace("\r", "\\r")
				.replace("\n", "\\n");
		return sanitized.length() <= 256 ? sanitized : sanitized.substring(0, 253) + "...";
	}

	static Status handleError(Throwable ex) {
		if (ex instanceof StatusRuntimeException e && e.getStatus().getCode().equals(Status.CANCELLED.getCode())) {
			return e.getStatus();
		}

		var rocksError = findRocksDBException(ex);
		if (rocksError != null) {
			return switch (rocksError.getErrorUniqueId()) {
					case COMPACTION_CONFLICT -> Status.ABORTED
							.withDescription(rocksError.getLocalizedMessage()).withCause(rocksError);
					case PUT_INVALID_REQUEST, INVALID_REQUEST -> Status.INVALID_ARGUMENT
							.withDescription(rocksError.getLocalizedMessage()).withCause(rocksError);
					case TRANSACTION_NOT_FOUND -> Status.NOT_FOUND
							.withDescription(rocksError.getLocalizedMessage()).withCause(rocksError);
					case READ_DEADLINE_EXCEEDED -> Status.DEADLINE_EXCEEDED
							.withDescription(rocksError.getLocalizedMessage()).withCause(rocksError);
					case SERVER_OVERLOADED -> Status.RESOURCE_EXHAUSTED
							.withDescription(rocksError.getLocalizedMessage()).withCause(rocksError);
					// Not UNAVAILABLE nor RESOURCE_EXHAUSTED: a client that does not know these types must not take
					// them for a failure it can resend blindly.
					case OUTCOME_UNKNOWN -> Status.UNKNOWN
							.withDescription(rocksError.getLocalizedMessage()).withCause(rocksError);
					case SESSION_EXPIRED -> Status.FAILED_PRECONDITION
							.withDescription(rocksError.getLocalizedMessage()).withCause(rocksError);
					default -> Status.INTERNAL
							.withDescription(rocksError.getLocalizedMessage()).withCause(rocksError);
			};
		}
		return switch (ex) {
			case CompletionException exx -> handleError(exx.getCause());
			case StatusException ex2 -> ex2.getStatus();
			case StatusRuntimeException ex3 -> ex3.getStatus();
			case null, default -> Status.INTERNAL.withCause(ex);
		};
	}
}
