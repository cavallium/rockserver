package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.protobuf.ByteString;
import it.cavallium.rockserver.core.common.api.RocksDB;
import it.cavallium.rockserver.core.common.api.proto.PutBatchInitialRequest;
import it.cavallium.rockserver.core.common.api.proto.PutRequest;
import java.nio.ByteBuffer;
import java.util.List;
import org.junit.jupiter.api.Test;

class WorkloadWireContractTest {

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
