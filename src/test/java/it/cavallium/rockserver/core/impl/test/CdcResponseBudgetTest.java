package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.api.proto.CdcPollResponse;
import it.cavallium.rockserver.core.common.cdc.CDCEvent;
import it.cavallium.rockserver.core.common.cdc.CdcBatch;
import it.cavallium.rockserver.core.server.CdcResponseBudget;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

class CdcResponseBudgetTest {

	@Test
	void exactSerializedBoundaryIncludesCompleteBatchAndPreservesCursor() {
		var first = event(11, 7, "first-key", "first-value", CDCEvent.Op.PUT);
		var second = event(12, 7, "second-key", "second-value", CDCEvent.Op.MERGE);
		var batch = new CdcBatch(List.of(first, second), 99);
		var unbounded = CdcResponseBudget.build(batch, Integer.MAX_VALUE);

		var atBoundary = CdcResponseBudget.build(batch, unbounded.getSerializedSize());

		assertEquals(unbounded, atBoundary);
		assertEquals(2, atBoundary.getEventsCount());
		assertEquals(99, atBoundary.getNextSeq());
		assertEquals("first-key", atBoundary.getEvents(0).getKey().toStringUtf8());
		assertEquals("second-value", atBoundary.getEvents(1).getValue().toStringUtf8());
		assertEquals(it.cavallium.rockserver.core.common.api.proto.CDCEvent.Op.MERGE,
				atBoundary.getEvents(1).getOp());
	}

	@Test
	void oneByteBelowFullResponsePaginatesAtFirstUnsentSequenceWithoutSkipping() {
		var first = event(21, 3, "a", "first-page", CDCEvent.Op.PUT);
		var second = event(22, 3, "b", "second-page", CDCEvent.Op.DELETE);
		var batch = new CdcBatch(List.of(first, second), 1000);
		int fullSize = CdcResponseBudget.build(batch, Integer.MAX_VALUE).getSerializedSize();

		var firstPage = CdcResponseBudget.build(batch, fullSize - 1);
		assertEquals(1, firstPage.getEventsCount());
		assertEquals(21, firstPage.getEvents(0).getSeq());
		assertEquals(22, firstPage.getNextSeq());

		var secondPage = CdcResponseBudget.build(new CdcBatch(List.of(second), batch.nextSeq()), fullSize - 1);
		assertEquals(1, secondPage.getEventsCount());
		assertEquals(22, secondPage.getEvents(0).getSeq());
		assertEquals(batch.nextSeq(), secondPage.getNextSeq());
	}

	@Test
	void budgetIncludesProtobufEventWrappersAndNextSequence() {
		var domainEvent = event(31, 9, "key", "value", CDCEvent.Op.PUT);
		var batch = new CdcBatch(List.of(domainEvent), 1234);
		var response = CdcResponseBudget.build(batch, Integer.MAX_VALUE);
		var protoEvent = response.getEvents(0);
		int expectedSize = CdcPollResponse.newBuilder()
				.addEvents(protoEvent)
				.setNextSeq(batch.nextSeq())
				.build()
				.getSerializedSize();
		int rawPayloadSize = domainEvent.key().size() + domainEvent.value().size();

		assertEquals(expectedSize, response.getSerializedSize());
		assertTrue(response.getSerializedSize() > rawPayloadSize);

		var error = assertThrows(StatusRuntimeException.class,
				() -> CdcResponseBudget.build(batch, rawPayloadSize));
		assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
		String description = error.getStatus().getDescription();
		assertNotNull(description);
		assertTrue(description.contains("requires " + expectedSize + " serialized bytes"));
	}

	@Test
	void paginationNeverSplitsAContiguousSameSequenceGroup() {
		var prefix = event(40, 2, "prefix", "p", CDCEvent.Op.PUT);
		var sameSeqFirst = event(41, 2, "same-a", "large-value-a", CDCEvent.Op.PUT);
		var sameSeqSecond = event(41, 2, "same-b", "large-value-b", CDCEvent.Op.PUT);
		var suffix = event(42, 2, "suffix", "s", CDCEvent.Op.PUT);

		int budgetThatWouldFitOnlyPartOfGroup = CdcResponseBudget.build(
				new CdcBatch(List.of(prefix, sameSeqFirst), sameSeqFirst.seq()),
				Integer.MAX_VALUE).getSerializedSize();
		var page = CdcResponseBudget.build(
				new CdcBatch(List.of(prefix, sameSeqFirst, sameSeqSecond, suffix), 43),
				budgetThatWouldFitOnlyPartOfGroup);

		assertEquals(1, page.getEventsCount());
		assertEquals(prefix.seq(), page.getEvents(0).getSeq());
		assertEquals(sameSeqFirst.seq(), page.getNextSeq());
	}

	@Test
	void oversizedFirstGroupFailsFastAndSucceedsAtRequiredBoundary() {
		var first = event(51, 4, "same-1", "value-1", CDCEvent.Op.PUT);
		var second = event(51, 4, "same-2", "value-2", CDCEvent.Op.PUT);
		var batch = new CdcBatch(List.of(first, second), 52);
		int requiredBytes = CdcResponseBudget.build(batch, Integer.MAX_VALUE).getSerializedSize();
		int tooSmall = requiredBytes - 1;

		var error = assertThrows(StatusRuntimeException.class,
				() -> CdcResponseBudget.build(batch, tooSmall));
		assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
		String description = error.getStatus().getDescription();
		assertNotNull(description);
		assertTrue(description.contains("seq " + first.seq()));
		assertTrue(description.contains("requires " + requiredBytes + " serialized bytes"));
		assertTrue(description.contains("limit " + tooSmall));
		assertTrue(description.contains(CdcResponseBudget.CLIENT_MAX_INBOUND_MESSAGE_SIZE_PROPERTY));

		var response = CdcResponseBudget.build(batch, requiredBytes);
		assertEquals(2, response.getEventsCount());
		assertEquals(batch.nextSeq(), response.getNextSeq());
	}

	@Test
	void emptyBatchPreservesCursorAndBudgetMustBePositive() {
		var empty = CdcResponseBudget.build(new CdcBatch(List.of(), 77), Integer.MAX_VALUE);
		assertEquals(0, empty.getEventsCount());
		assertEquals(77, empty.getNextSeq());

		assertThrows(IllegalArgumentException.class,
				() -> CdcResponseBudget.build(new CdcBatch(List.of(), 0), 0));
		assertThrows(IllegalArgumentException.class,
				() -> CdcResponseBudget.build(new CdcBatch(List.of(), 0), -1));
	}

	@Test
	void streamedEventUsesExactSerializedBoundary() {
		var event = event(81, 6, "stream-key", "stream-value", CDCEvent.Op.PUT);
		var unbounded = CdcResponseBudget.buildEvent(event, Integer.MAX_VALUE);
		int exactSize = unbounded.getSerializedSize();

		assertEquals(unbounded, CdcResponseBudget.buildEvent(event, exactSize));
		var error = assertThrows(StatusRuntimeException.class,
				() -> CdcResponseBudget.buildEvent(event, exactSize - 1));
		assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
		assertNotNull(error.getStatus().getDescription());
		assertTrue(error.getStatus().getDescription().contains("seq " + event.seq()));
		assertTrue(error.getStatus().getDescription().contains("requires " + exactSize + " serialized bytes"));
		assertTrue(error.getStatus().getDescription().contains(CdcResponseBudget.CLIENT_MAX_INBOUND_MESSAGE_SIZE_PROPERTY));
	}

	private static CDCEvent event(long seq, long columnId, String key, String value, CDCEvent.Op op) {
		return new CDCEvent(
				seq,
				columnId,
				Buf.wrap(key.getBytes(StandardCharsets.UTF_8)),
				Buf.wrap(value.getBytes(StandardCharsets.UTF_8)),
				op);
	}
}
