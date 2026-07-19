package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.ThriftTransportLimits;
import it.cavallium.rockserver.core.common.cdc.CDCEvent;
import it.cavallium.rockserver.core.common.cdc.CdcBatch;
import it.cavallium.rockserver.core.server.ThriftCdcResponseBudget;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.jupiter.api.Test;

class ThriftCdcResponseBudgetTest {

	@Test
	void exactSerializedBoundaryIncludesTheCompleteBatch() throws TException {
		var first = event(11, "first-key", "first-value");
		var second = event(12, "second-key", "second-value");
		var batch = new CdcBatch(List.of(first, second), 99);
		var unbounded = ThriftCdcResponseBudget.build(batch, Integer.MAX_VALUE);
		int exactSize = serializedSize(unbounded);

		var atBoundary = ThriftCdcResponseBudget.build(batch, exactSize);

		assertEquals(unbounded, atBoundary);
		assertEquals(exactSize, serializedSize(atBoundary));
		assertEquals(2, atBoundary.getEventsSize());
		assertEquals(batch.nextSeq(), atBoundary.getNextSeq());
	}

	@Test
	void paginationNeverSplitsACompleteSequenceGroup() throws TException {
		var prefix = event(20, "prefix", "p");
		var sameSequenceFirst = event(21, "same-a", "large-value-a");
		var sameSequenceSecond = event(21, "same-b", "large-value-b");
		var suffix = event(22, "suffix", "s");
		int budgetThatFitsTheCompleteGroupAlone = serializedSize(ThriftCdcResponseBudget.build(
				new CdcBatch(List.of(sameSequenceFirst, sameSequenceSecond), suffix.seq()),
				Integer.MAX_VALUE));

		var firstPage = ThriftCdcResponseBudget.build(
				new CdcBatch(List.of(prefix, sameSequenceFirst, sameSequenceSecond, suffix), 23),
				budgetThatFitsTheCompleteGroupAlone);
		var secondPage = ThriftCdcResponseBudget.build(
				new CdcBatch(List.of(sameSequenceFirst, sameSequenceSecond, suffix), 23),
				budgetThatFitsTheCompleteGroupAlone);
		var thirdPage = ThriftCdcResponseBudget.build(
				new CdcBatch(List.of(suffix), 23),
				budgetThatFitsTheCompleteGroupAlone);

		assertEquals(List.of(prefix.seq()),
				firstPage.getEvents().stream().map(event -> event.getSeq()).toList());
		assertEquals(sameSequenceFirst.seq(), firstPage.getNextSeq());
		assertEquals(List.of(sameSequenceFirst.seq(), sameSequenceSecond.seq()),
				secondPage.getEvents().stream().map(event -> event.getSeq()).toList());
		assertEquals(suffix.seq(), secondPage.getNextSeq());
		assertEquals(List.of(suffix.seq()),
				thirdPage.getEvents().stream().map(event -> event.getSeq()).toList());
		assertEquals(23, thirdPage.getNextSeq());
		assertTrue(serializedSize(firstPage) <= budgetThatFitsTheCompleteGroupAlone);
		assertTrue(serializedSize(secondPage) <= budgetThatFitsTheCompleteGroupAlone);
		assertTrue(serializedSize(thirdPage) <= budgetThatFitsTheCompleteGroupAlone);
	}

	@Test
	void oversizedFirstGroupReturnsTheTypedCdcError() throws TException {
		var first = event(31, "same-1", "value-1");
		var second = event(31, "same-2", "value-2");
		var batch = new CdcBatch(List.of(first, second), 32);
		int requiredBytes = serializedSize(ThriftCdcResponseBudget.build(batch, Integer.MAX_VALUE));

		var error = assertThrows(RocksDBException.class,
				() -> ThriftCdcResponseBudget.build(batch, requiredBytes - 1));

		assertEquals(RocksDBException.RocksDBErrorType.CDC_RESPONSE_TOO_LARGE,
				error.getErrorUniqueId());
		assertTrue(error.getMessage().contains("seq " + first.seq()));
		assertTrue(error.getMessage().contains("requires " + requiredBytes + " serialized Thrift bytes"));
		assertTrue(error.getMessage().contains(
				ThriftTransportLimits.CLIENT_MAX_CDC_RESPONSE_SIZE_PROPERTY));
	}

	private static int serializedSize(
			it.cavallium.rockserver.core.common.api.CdcPollBatchResult result) throws TException {
		return new TSerializer(new TBinaryProtocol.Factory()).serialize(result).length;
	}

	private static CDCEvent event(long sequence, String key, String value) {
		return new CDCEvent(sequence,
				7,
				Buf.wrap(key.getBytes(StandardCharsets.UTF_8)),
				Buf.wrap(value.getBytes(StandardCharsets.UTF_8)),
				CDCEvent.Op.PUT);
	}
}
