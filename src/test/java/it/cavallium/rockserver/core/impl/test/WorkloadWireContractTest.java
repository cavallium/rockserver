package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.protobuf.ByteString;
import it.cavallium.rockserver.core.common.api.RocksDB;
import it.cavallium.rockserver.core.common.api.proto.PutBatchInitialRequest;
import it.cavallium.rockserver.core.common.api.proto.PutRequest;
import java.nio.ByteBuffer;
import java.util.List;
import org.junit.jupiter.api.Test;

class WorkloadWireContractTest {

	@Test
	void allSevenProfileNumbersAreExplicitAndStableAcrossJavaProtobufAndThrift() {
		var profiles = List.of(
				it.cavallium.rockserver.core.common.WorkloadProfile.CONTROL,
				it.cavallium.rockserver.core.common.WorkloadProfile.LATENCY,
				it.cavallium.rockserver.core.common.WorkloadProfile.ANALYTICAL,
				it.cavallium.rockserver.core.common.WorkloadProfile.INGEST,
				it.cavallium.rockserver.core.common.WorkloadProfile.CDC,
				it.cavallium.rockserver.core.common.WorkloadProfile.BATCH,
				it.cavallium.rockserver.core.common.WorkloadProfile.PHYSICAL_MAINTENANCE);
		for (int index = 0; index < profiles.size(); index++) {
			int wireValue = index + 1;
			var profile = profiles.get(index);
			assertEquals(wireValue, profile.wireValue());
			assertEquals(profile,
					it.cavallium.rockserver.core.common.WorkloadProfile.fromWireValue(wireValue));
			assertEquals(wireValue,
					it.cavallium.rockserver.core.common.api.proto.WorkloadProfile.forNumber(wireValue).getNumber());
			assertEquals(wireValue,
					it.cavallium.rockserver.core.common.api.WorkloadProfile.findByValue(wireValue).getValue());
		}
		assertThrows(IllegalArgumentException.class,
				() -> it.cavallium.rockserver.core.common.WorkloadProfile.fromWireValue(0));
		assertThrows(IllegalArgumentException.class,
				() -> it.cavallium.rockserver.core.common.WorkloadProfile.fromWireValue(8));
	}

	@Test
	void protobufRetainsProfileAndAbsoluteDeadlineOnUnaryAndStreamInitialRequests() throws Exception {
		var context = it.cavallium.rockserver.core.common.api.proto.RequestContext.newBuilder()
				.setProfile(it.cavallium.rockserver.core.common.api.proto.WorkloadProfile.ANALYTICAL)
				.setDeadlineEpochMillis(123_456L)
				.build();
		var unary = PutRequest.newBuilder()
				.setColumnId(1)
				.setData(it.cavallium.rockserver.core.common.api.proto.KV.newBuilder()
						.addKeys(ByteString.copyFrom(new byte[] {1}))
						.setValue(ByteString.copyFrom(new byte[] {2})))
				.setContext(context)
				.build();
		assertEquals(context, PutRequest.parseFrom(unary.toByteArray()).getContext());

		var streaming = PutBatchInitialRequest.newBuilder().setColumnId(1).setContext(context).build();
		assertEquals(context, PutBatchInitialRequest.parseFrom(streaming.toByteArray()).getContext());
	}

	@Test
	void thriftGenericMethodsRequireRequestContextAndProtectedMethodsDoNot() throws Exception {
		assertNotNull(RocksDB.Iface.class.getMethod("put",
				long.class, long.class, List.class, ByteBuffer.class,
				it.cavallium.rockserver.core.common.api.RequestContext.class));
		assertNotNull(RocksDB.Iface.class.getMethod("deleteRange",
				long.class, List.class, List.class,
				it.cavallium.rockserver.core.common.api.RequestContext.class));
		assertNotNull(RocksDB.Iface.class.getMethod("closeIterator", long.class));
		assertNotNull(RocksDB.Iface.class.getMethod("flush"));
	}
}
