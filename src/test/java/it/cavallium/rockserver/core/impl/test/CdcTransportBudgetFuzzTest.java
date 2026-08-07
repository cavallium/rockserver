package it.cavallium.rockserver.core.impl.test;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.api.CDCOperation;
import it.cavallium.rockserver.core.common.api.CdcPollBatchResult;
import it.cavallium.rockserver.core.common.api.proto.CdcPollResponse;
import it.cavallium.rockserver.core.common.cdc.CDCEvent;
import it.cavallium.rockserver.core.common.cdc.CdcBatch;
import it.cavallium.rockserver.core.server.CdcResponseBudget;
import it.cavallium.rockserver.core.server.ThriftCdcResponseBudget;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CdcTransportBudgetFuzzTest {

	private static final int MAX_GROUPS = 8;
	private static final int MAX_EVENTS_PER_GROUP = 4;
	private static final int MAX_KEY_SIZE = 512;
	private static final int MAX_VALUE_SIZE = 2_048;

	@FuzzTest(maxDuration = "30s")
	void protobufPaginationMatchesSerializedSequenceGroupModel(FuzzedDataProvider data) {
		CdcBatch batch = batch(data);
		int fullSize = protoResponse(batch.events(), batch.nextSeq()).getSerializedSize();
		int budget = data.consumeInt(1, Math.max(1, fullSize + 128));
		ProtoOutcome expected = modelProto(batch, budget);

		try {
			CdcPollResponse actual = CdcResponseBudget.build(batch, budget);
			if (expected.response == null) {
				fail("Expected the first protobuf sequence group to exceed " + budget + " bytes");
			}
			assertEquals(expected.response, actual);
			assertTrue(actual.getSerializedSize() <= budget);
		} catch (StatusRuntimeException actualFailure) {
			if (expected.response != null) {
				throw actualFailure;
			}
			assertEquals(Status.Code.FAILED_PRECONDITION, actualFailure.getStatus().getCode());
		}

		if (!batch.events().isEmpty()) {
			CDCEvent event = batch.events().get(data.consumeInt(0, batch.events().size() - 1));
			var mapped = mapProto(event);
			int exactSize = mapped.getSerializedSize();
			assertEquals(mapped, CdcResponseBudget.buildEvent(event, exactSize));
			if (exactSize > 1) {
				assertThrows(StatusRuntimeException.class,
						() -> CdcResponseBudget.buildEvent(event, exactSize - 1));
			}
		}
	}

	@FuzzTest(maxDuration = "30s")
	void thriftPaginationMatchesBinaryProtocolSequenceGroupModel(FuzzedDataProvider data) throws TException {
		CdcBatch batch = batch(data);
		int fullSize = thriftSize(thriftResponse(batch.events(), batch.nextSeq()));
		int budget = data.consumeInt(1, Math.max(1, fullSize + 128));
		ThriftOutcome expected = modelThrift(batch, budget);

		try {
			CdcPollBatchResult actual = ThriftCdcResponseBudget.build(batch, budget);
			if (expected.response == null) {
				fail("Expected the first Thrift sequence group to exceed " + budget + " bytes");
			}
			assertEquals(expected.response, actual);
			assertTrue(thriftSize(actual) <= budget);
		} catch (RocksDBException actualFailure) {
			if (expected.response != null) {
				throw actualFailure;
			}
			assertEquals(RocksDBException.RocksDBErrorType.CDC_RESPONSE_TOO_LARGE,
					actualFailure.getErrorUniqueId());
		}
	}

	private static ProtoOutcome modelProto(CdcBatch batch, int budget) {
		if (batch.events().isEmpty()) {
			CdcPollResponse empty = protoResponse(List.of(), batch.nextSeq());
			return new ProtoOutcome(empty.getSerializedSize() <= budget ? empty : null);
		}

		int groupStart = 0;
		while (groupStart < batch.events().size()) {
			int groupEnd = endOfSequenceGroup(batch.events(), groupStart);
			long candidateNextSeq = groupEnd < batch.events().size()
					? batch.events().get(groupEnd).seq()
					: batch.nextSeq();
			CdcPollResponse candidate = protoResponse(batch.events().subList(0, groupEnd), candidateNextSeq);
			if (candidate.getSerializedSize() > budget) {
				return groupStart == 0
						? new ProtoOutcome(null)
						: new ProtoOutcome(protoResponse(
						batch.events().subList(0, groupStart),
						batch.events().get(groupStart).seq()));
			}
			groupStart = groupEnd;
		}
		return new ProtoOutcome(protoResponse(batch.events(), batch.nextSeq()));
	}

	private static ThriftOutcome modelThrift(CdcBatch batch, int budget) throws TException {
		if (batch.events().isEmpty()) {
			CdcPollBatchResult empty = thriftResponse(List.of(), batch.nextSeq());
			return new ThriftOutcome(thriftSize(empty) <= budget ? empty : null);
		}

		int groupStart = 0;
		while (groupStart < batch.events().size()) {
			int groupEnd = endOfSequenceGroup(batch.events(), groupStart);
			long candidateNextSeq = groupEnd < batch.events().size()
					? batch.events().get(groupEnd).seq()
					: batch.nextSeq();
			CdcPollBatchResult candidate = thriftResponse(batch.events().subList(0, groupEnd), candidateNextSeq);
			if (thriftSize(candidate) > budget) {
				return groupStart == 0
						? new ThriftOutcome(null)
						: new ThriftOutcome(thriftResponse(
						batch.events().subList(0, groupStart),
						batch.events().get(groupStart).seq()));
			}
			groupStart = groupEnd;
		}
		return new ThriftOutcome(thriftResponse(batch.events(), batch.nextSeq()));
	}

	private static int endOfSequenceGroup(List<CDCEvent> events, int groupStart) {
		long sequence = events.get(groupStart).seq();
		int groupEnd = groupStart + 1;
		while (groupEnd < events.size() && events.get(groupEnd).seq() == sequence) {
			groupEnd++;
		}
		return groupEnd;
	}

	private static CdcPollResponse protoResponse(List<CDCEvent> events, long nextSeq) {
		var builder = CdcPollResponse.newBuilder().setNextSeq(nextSeq);
		for (CDCEvent event : events) {
			builder.addEvents(mapProto(event));
		}
		return builder.build();
	}

	private static it.cavallium.rockserver.core.common.api.proto.CDCEvent mapProto(CDCEvent event) {
		var builder = it.cavallium.rockserver.core.common.api.proto.CDCEvent.newBuilder()
				.setSeq(event.seq())
				.setColumnId(event.columnId())
				.setKey(ByteString.copyFrom(event.key().asArray()))
				.setOp(switch (event.op()) {
					case PUT -> it.cavallium.rockserver.core.common.api.proto.CDCEvent.Op.PUT;
					case DELETE -> it.cavallium.rockserver.core.common.api.proto.CDCEvent.Op.DELETE;
					case MERGE -> it.cavallium.rockserver.core.common.api.proto.CDCEvent.Op.MERGE;
				});
		if (event.value() != null && !event.value().isEmpty()) {
			builder.setValue(ByteString.copyFrom(event.value().asArray()));
		}
		return builder.build();
	}

	private static CdcPollBatchResult thriftResponse(List<CDCEvent> events, long nextSeq) {
		return new CdcPollBatchResult(events.stream().map(CdcTransportBudgetFuzzTest::mapThrift).toList(), nextSeq);
	}

	private static it.cavallium.rockserver.core.common.api.CDCEvent mapThrift(CDCEvent event) {
		var mapped = new it.cavallium.rockserver.core.common.api.CDCEvent()
				.setSeq(event.seq())
				.setColumnId(event.columnId())
				.setKey(ByteBuffer.wrap(event.key().asArray()))
				.setOp(CDCOperation.valueOf(event.op().name()));
		if (event.value() != null) {
			mapped.setValue(ByteBuffer.wrap(event.value().asArray()));
		}
		return mapped;
	}

	private static int thriftSize(CdcPollBatchResult response) throws TException {
		return new TSerializer(new TBinaryProtocol.Factory()).serialize(response).length;
	}

	private static CdcBatch batch(FuzzedDataProvider data) {
		int groups = data.consumeInt(0, MAX_GROUPS);
		List<CDCEvent> events = new ArrayList<>(groups * MAX_EVENTS_PER_GROUP);
		long sequence = data.consumeLong(1, 1_000_000_000L);
		for (int group = 0; group < groups; group++) {
			if (group > 0) {
				sequence += data.consumeInt(1, 16);
			}
			int groupSize = data.consumeInt(1, MAX_EVENTS_PER_GROUP);
			for (int eventIndex = 0; eventIndex < groupSize; eventIndex++) {
				Buf key = Buf.wrap(bytes(data, data.consumeInt(0, MAX_KEY_SIZE)));
				Buf value = data.consumeBoolean()
						? null
						: Buf.wrap(bytes(data, data.consumeInt(0, MAX_VALUE_SIZE)));
				events.add(new CDCEvent(
						sequence,
						data.consumeLong(0, 1_000_000L),
						key,
						value,
						data.pickValue(CDCEvent.Op.values())));
			}
		}
		long nextSeq = groups == 0
				? data.consumeLong(0, 1_000_000_000L)
				: sequence + data.consumeInt(1, 16);
		return new CdcBatch(List.copyOf(events), nextSeq);
	}

	private static byte[] bytes(FuzzedDataProvider data, int size) {
		byte[] result = new byte[size];
		byte[] consumed = data.consumeBytes(size);
		System.arraycopy(consumed, 0, result, 0, consumed.length);
		return result;
	}

	private record ProtoOutcome(CdcPollResponse response) {
	}

	private record ThriftOutcome(CdcPollBatchResult response) {
	}
}
