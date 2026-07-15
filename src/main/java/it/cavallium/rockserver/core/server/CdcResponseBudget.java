package it.cavallium.rockserver.core.server;

import com.google.protobuf.CodedOutputStream;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.common.api.proto.CdcPollResponse;
import it.cavallium.rockserver.core.common.cdc.CDCEvent;
import it.cavallium.rockserver.core.common.cdc.CdcBatch;
import java.util.ArrayList;
import java.util.List;

/** Builds cursor-safe CDC responses that fit a client's inbound protobuf message budget. */
public final class CdcResponseBudget {

	public static final String CLIENT_MAX_INBOUND_MESSAGE_SIZE_PROPERTY =
			GrpcConnection.MAX_INBOUND_MESSAGE_SIZE_PROPERTY;

	private CdcResponseBudget() {}

	public static CdcPollResponse build(CdcBatch batch, int maxSerializedBytes) {
		if (maxSerializedBytes <= 0) {
			throw new IllegalArgumentException("maxSerializedBytes must be positive: " + maxSerializedBytes);
		}

		List<CDCEvent> events = batch.events();
		if (events.isEmpty()) {
			var response = CdcPollResponse.newBuilder()
					.setNextSeq(batch.nextSeq())
					.build();
			if (response.getSerializedSize() > maxSerializedBytes) {
				throw responseTooLarge(batch.nextSeq(), response.getSerializedSize(), maxSerializedBytes);
			}
			return response;
		}

		var response = CdcPollResponse.newBuilder();
		long acceptedEventsSize = 0;
		int groupStart = 0;
		while (groupStart < events.size()) {
			long groupSeq = events.get(groupStart).seq();
			int groupEnd = groupStart;
			while (groupEnd < events.size() && events.get(groupEnd).seq() == groupSeq) {
				groupEnd++;
			}

			long groupSerializedSize = 0;
			var mappedGroup = new ArrayList<it.cavallium.rockserver.core.common.api.proto.CDCEvent>(
					groupEnd - groupStart);
			for (int eventIndex = groupStart; eventIndex < groupEnd; eventIndex++) {
				var mappedEvent = mapEvent(events.get(eventIndex));
				mappedGroup.add(mappedEvent);
				groupSerializedSize += serializedRepeatedEventSize(mappedEvent);
			}

			long candidateNextSeq = groupEnd < events.size()
					? events.get(groupEnd).seq()
					: batch.nextSeq();
			long candidateSize = acceptedEventsSize
					+ groupSerializedSize
					+ serializedNextSeqSize(candidateNextSeq);

			if (candidateSize > maxSerializedBytes) {
				if (groupStart == 0) {
					throw responseTooLarge(groupSeq, candidateSize, maxSerializedBytes);
				}
				return response
						.setNextSeq(groupSeq)
						.build();
			}

			response.addAllEvents(mappedGroup);
			acceptedEventsSize += groupSerializedSize;
			groupStart = groupEnd;
		}

		return response
				.setNextSeq(batch.nextSeq())
				.build();
	}

	private static long serializedRepeatedEventSize(
			it.cavallium.rockserver.core.common.api.proto.CDCEvent event) {
		return CodedOutputStream.computeMessageSize(CdcPollResponse.EVENTS_FIELD_NUMBER, event);
	}

	private static int serializedNextSeqSize(long nextSeq) {
		return nextSeq == 0
				? 0
				: CodedOutputStream.computeInt64Size(CdcPollResponse.NEXTSEQ_FIELD_NUMBER, nextSeq);
	}

	private static StatusRuntimeException responseTooLarge(long seq, long requiredBytes, int limitBytes) {
		return Status.FAILED_PRECONDITION
				.withDescription("CDC response sequence group at seq " + seq
						+ " requires " + requiredBytes + " serialized bytes, exceeding limit " + limitBytes
						+ "; increase system property " + CLIENT_MAX_INBOUND_MESSAGE_SIZE_PROPERTY)
				.asRuntimeException();
	}

	private static it.cavallium.rockserver.core.common.api.proto.CDCEvent mapEvent(CDCEvent event) {
		var builder = it.cavallium.rockserver.core.common.api.proto.CDCEvent.newBuilder()
				.setSeq(event.seq())
				.setColumnId(event.columnId())
				.setKey(Utils.toByteString(event.key()));
		if (event.value() != null && !event.value().isEmpty()) {
			builder.setValue(Utils.toByteString(event.value()));
		}
		builder.setOp(switch (event.op()) {
			case PUT -> it.cavallium.rockserver.core.common.api.proto.CDCEvent.Op.PUT;
			case DELETE -> it.cavallium.rockserver.core.common.api.proto.CDCEvent.Op.DELETE;
			case MERGE -> it.cavallium.rockserver.core.common.api.proto.CDCEvent.Op.MERGE;
		});
		return builder.build();
	}
}
