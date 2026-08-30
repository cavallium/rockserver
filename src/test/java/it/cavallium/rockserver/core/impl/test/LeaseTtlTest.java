package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.rockserver.core.common.LeaseTtl;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class LeaseTtlTest {

	@Test
	void embeddedMillisecondCeilingAndWireNanosecondsShareOneBound() {
		assertParity(Duration.ofNanos(1L), 1L, 1L);
		assertParity(Duration.ofMillis(7L), 7_000_000L, 7L);
		assertParity(Duration.ofNanos(Long.MAX_VALUE),
				Long.MAX_VALUE, Long.MAX_VALUE / 1_000_000L + 1L);
		assertParity(Duration.ofSeconds(Long.MAX_VALUE),
				Long.MAX_VALUE, Long.MAX_VALUE / 1_000_000L + 1L);
	}

	@Test
	void nonPositiveLeaseTtlsFailBeforeDispatch() {
		assertThrows(IllegalArgumentException.class,
				() -> LeaseTtl.toNanos(Duration.ZERO, "lease"));
		assertThrows(IllegalArgumentException.class,
				() -> LeaseTtl.toMillisCeil(Duration.ofNanos(-1L), "lease"));
	}

	private static void assertParity(Duration duration, long expectedNanos, long expectedMillis) {
		assertEquals(expectedNanos, LeaseTtl.toNanos(duration, "lease"));
		assertEquals(expectedMillis, LeaseTtl.toMillisCeil(duration, "lease"));
		var transaction = it.cavallium.rockserver.core.common.api.proto.OpenTransactionRequest
				.newBuilder().setTransactionLeaseTtlNanos(LeaseTtl.toNanos(duration, "lease")).build();
		var iterator = it.cavallium.rockserver.core.common.api.proto.OpenIteratorRequest
				.newBuilder().setIteratorLeaseTtlNanos(LeaseTtl.toNanos(duration, "lease")).build();
		assertEquals(expectedNanos, transaction.getTransactionLeaseTtlNanos());
		assertEquals(expectedNanos, iterator.getIteratorLeaseTtlNanos());
	}
}
