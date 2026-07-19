package it.cavallium.rockserver.core.server;

import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.ThriftTransportLimits;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.common.api.CDCOperation;
import it.cavallium.rockserver.core.common.api.CdcPollBatchResult;
import it.cavallium.rockserver.core.common.cdc.CDCEvent;
import it.cavallium.rockserver.core.common.cdc.CdcBatch;
import java.util.ArrayList;
import java.util.Collections;

/** Builds cursor-safe CDC results that fit a client's serialized Thrift response budget. */
public final class ThriftCdcResponseBudget {
	private static final long RESULT_FIXED_SERIALIZED_BYTES = 20L;
	private static final long EVENT_FIXED_SERIALIZED_BYTES = 37L;
	private static final long OPTIONAL_BINARY_FIELD_OVERHEAD_BYTES = 7L;

	private ThriftCdcResponseBudget() {}

	public static CdcPollBatchResult build(CdcBatch batch, int maxSerializedBytes) {
		if (maxSerializedBytes <= 0) {
			throw new IllegalArgumentException("maxSerializedBytes must be positive: " + maxSerializedBytes);
		}

		if (batch.events().isEmpty()) {
			if (RESULT_FIXED_SERIALIZED_BYTES > maxSerializedBytes) {
				throw responseTooLarge(batch.nextSeq(), RESULT_FIXED_SERIALIZED_BYTES, maxSerializedBytes);
			}
			return new CdcPollBatchResult(Collections.emptyList(), batch.nextSeq());
		}

		var acceptedEvents = new ArrayList<it.cavallium.rockserver.core.common.api.CDCEvent>();
		long acceptedEventBytes = 0L;
		int groupStart = 0;
		while (groupStart < batch.events().size()) {
			long groupSequence = batch.events().get(groupStart).seq();
			int groupEnd = groupStart;
			long groupBytes = 0L;
			while (groupEnd < batch.events().size()
					&& batch.events().get(groupEnd).seq() == groupSequence) {
				groupBytes = saturatingAdd(groupBytes, serializedEventSize(batch.events().get(groupEnd)));
				groupEnd++;
			}

			long candidateBytes = saturatingAdd(RESULT_FIXED_SERIALIZED_BYTES,
					saturatingAdd(acceptedEventBytes, groupBytes));
			if (candidateBytes > maxSerializedBytes) {
				if (acceptedEvents.isEmpty()) {
					throw responseTooLarge(groupSequence, candidateBytes, maxSerializedBytes);
				}
				return new CdcPollBatchResult(acceptedEvents, groupSequence);
			}

			for (int eventIndex = groupStart; eventIndex < groupEnd; eventIndex++) {
				acceptedEvents.add(mapEvent(batch.events().get(eventIndex)));
			}
			acceptedEventBytes = saturatingAdd(acceptedEventBytes, groupBytes);
			groupStart = groupEnd;
		}

		return new CdcPollBatchResult(acceptedEvents, batch.nextSeq());
	}

	private static long serializedEventSize(CDCEvent event) {
		long size = saturatingAdd(EVENT_FIXED_SERIALIZED_BYTES, event.key().size());
		if (event.value() != null) {
			size = saturatingAdd(size,
					saturatingAdd(OPTIONAL_BINARY_FIELD_OVERHEAD_BYTES, event.value().size()));
		}
		return size;
	}

	private static long saturatingAdd(long left, long right) {
		return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
	}

	private static it.cavallium.rockserver.core.common.api.CDCEvent mapEvent(CDCEvent event) {
		var mapped = new it.cavallium.rockserver.core.common.api.CDCEvent()
				.setSeq(event.seq())
				.setColumnId(event.columnId())
				.setKey(Utils.asByteBuffer(event.key()))
				.setOp(CDCOperation.valueOf(event.op().name()));
		if (event.value() != null) {
			mapped.setValue(Utils.asByteBuffer(event.value()));
		}
		return mapped;
	}

	private static RocksDBException responseTooLarge(long sequence,
			long requiredBytes,
			int limitBytes) {
		return RocksDBException.of(RocksDBException.RocksDBErrorType.CDC_RESPONSE_TOO_LARGE,
				"CDC response sequence group at seq " + sequence
						+ " requires " + requiredBytes + " serialized Thrift bytes, exceeding limit "
						+ limitBytes + "; increase system property "
						+ ThriftTransportLimits.CLIENT_MAX_CDC_RESPONSE_SIZE_PROPERTY);
	}
}
