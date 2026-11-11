package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.google.protobuf.Message;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.api.proto.PutRequest;
import java.lang.reflect.Method;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

/**
 * The log lines of request failures that arrive after their call ended (R62-7). {@code GrpcRequestErrors} is not
 * public, and the tests are a module of their own, which cannot hold a class of its package ("package exists in
 * another module"): its methods are called by reflection, as {@code GrpcServerDeadlineErrorTest} reaches its logger.
 */
class GrpcRequestErrorsTest {

	private static final RocksDBException LATE_READ = RocksDBException.of(RocksDBErrorType.READ_DEADLINE_EXCEEDED,
			"the read ran out of time");
	private static final Method LOG_LATE;
	private static final Method LOG_UNCONTEXTUALIZED;
	private static final Method CLAIM;

	static {
		try {
			var errors = Class.forName("it.cavallium.rockserver.core.server.GrpcRequestErrors");
			LOG_LATE = errors.getDeclaredMethod("logLateRequestError", String.class, Message.class, Throwable.class);
			LOG_UNCONTEXTUALIZED = errors.getDeclaredMethod("logUncontextualizedLateError", Throwable.class);
			CLAIM = errors.getDeclaredMethod("claimLateReadDeadlineLog", String.class);
			LOG_LATE.setAccessible(true);
			LOG_UNCONTEXTUALIZED.setAccessible(true);
			CLAIM.setAccessible(true);
		} catch (ReflectiveOperationException ex) {
			throw new ExceptionInInitializerError(ex);
		}
	}

	/** Throws what the method threw, wrapped by the reflection: a test that reaches here failed. */
	private static void logLate(String operation, Throwable error) throws Exception {
		LOG_LATE.invoke(null, operation, PutRequest.getDefaultInstance(), error);
	}

	private static Long claim(String operation) throws Exception {
		return (Long) CLAIM.invoke(null, operation);
	}

	/**
	 * A late READ_DEADLINE_EXCEEDED is logged once a minute per operation; the lines between are counted, not
	 * written: their claim is null. That null was unboxed (a conditional of a {@code Long} and a {@code 0L}) and the
	 * handler threw a NullPointerException, on the thread that dropped the error.
	 */
	@Test
	void aSuppressedLateReadDeadlineLineDoesNotThrow() throws Exception {
		var operation = "test-" + UUID.randomUUID();
		// The first line of the minute is written, with nothing suppressed before it.
		logLate(operation, LATE_READ);
		// The following are suppressed: these are the calls that threw.
		logLate(operation, LATE_READ);
		logLate(operation, new CompletionException(LATE_READ));
		// They were suppressed indeed: a further claim within the minute is null too.
		assertNull(claim(operation));
	}

	/** The same for a failure that carries no request: all of them share one line a minute. */
	@Test
	void aSuppressedLateReadDeadlineLineWithoutRequestContextDoesNotThrow() throws Exception {
		for (int i = 0; i < 3; i++) {
			LOG_UNCONTEXTUALIZED.invoke(null, LATE_READ);
		}
		assertNull(claim("<uncontextualized>"));
	}

	/** The first claim of an operation writes its line with nothing suppressed; the claims after it are null. */
	@Test
	void theFirstClaimOfAnOperationIsNotSuppressed() throws Exception {
		var operation = "test-" + UUID.randomUUID();
		assertEquals(0L, claim(operation));
		assertNull(claim(operation));
		assertNull(claim(operation));
	}

	/** Other late failures are not rate limited, and do not throw either. */
	@Test
	void aLateFailureOfAnotherKindIsLoggedEveryTime() throws Exception {
		var other = RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, "failed");
		for (int i = 0; i < 3; i++) {
			logLate("put", other);
			LOG_UNCONTEXTUALIZED.invoke(null, other);
		}
	}
}
