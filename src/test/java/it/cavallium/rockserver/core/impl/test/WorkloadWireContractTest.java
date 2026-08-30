package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import io.grpc.protobuf.ProtoFileDescriptorSupplier;
import it.cavallium.rockserver.core.common.api.RocksDB;
import it.cavallium.rockserver.core.common.api.proto.PutBatchInitialRequest;
import it.cavallium.rockserver.core.common.api.proto.PutRequest;
import java.nio.ByteBuffer;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.server.ThriftServer;

class WorkloadWireContractTest {

	private static final Set<String> CONTEXT_FREE_METHODS = Set.of(
			"getCapabilities",
			"closeFailedUpdate",
			"closeIterator",
			"flush",
			"compact",
			"cdcCreate",
			"cdcDelete",
			"cdcGetEarliestAvailableSequence",
			"cdcGetLastCommittedSequence",
			"cdcPoll",
			"cdcPollBatch",
			"cdcCommit");

	@Test
	void allSevenProfileNumbersAreExplicitAndStableAcrossJavaProtobufAndThrift() {
		var profiles = List.of(
				new ProfileWireValue(it.cavallium.rockserver.core.common.WorkloadProfile.CONTROL, 1),
				new ProfileWireValue(it.cavallium.rockserver.core.common.WorkloadProfile.LATENCY, 2),
				new ProfileWireValue(it.cavallium.rockserver.core.common.WorkloadProfile.ANALYTICAL, 3),
				new ProfileWireValue(it.cavallium.rockserver.core.common.WorkloadProfile.INGEST, 4),
				new ProfileWireValue(it.cavallium.rockserver.core.common.WorkloadProfile.CDC, 5),
				new ProfileWireValue(it.cavallium.rockserver.core.common.WorkloadProfile.BATCH, 6),
				new ProfileWireValue(it.cavallium.rockserver.core.common.WorkloadProfile.PHYSICAL_MAINTENANCE, 7));
		for (var expected : profiles) {
			int wireValue = expected.wireValue();
			var profile = expected.profile();
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

	private record ProfileWireValue(
			it.cavallium.rockserver.core.common.WorkloadProfile profile,
			int wireValue) {
	}

	@Test
	void workloadV2CapabilitiesAndRangeRequestValuesAreExplicitAndStable() throws Exception {
		var capabilities = it.cavallium.rockserver.core.common.api.proto.CapabilitiesResponse.newBuilder()
				.setWorkloadContractVersion(2)
				.setBoundedRange(true)
				.build();
		var decoded = it.cavallium.rockserver.core.common.api.proto.CapabilitiesResponse.parseFrom(
				capabilities.toByteArray());
		assertEquals(2, decoded.getWorkloadContractVersion());
		org.junit.jupiter.api.Assertions.assertTrue(decoded.getBoundedRange());

		assertEquals(0, it.cavallium.rockserver.core.common.api.proto.RangeRequestType
				.RANGE_REQUEST_TYPE_UNSPECIFIED.getNumber());
		assertEquals(1, it.cavallium.rockserver.core.common.api.proto.RangeRequestType.ALL_IN_RANGE.getNumber());
		assertEquals(2, it.cavallium.rockserver.core.common.api.proto.RangeRequestType.ALL_IN_RANGE_NO_CACHE.getNumber());
		assertEquals(1, it.cavallium.rockserver.core.common.api.RangeRequestType.ALL_IN_RANGE.getValue());
		assertEquals(2, it.cavallium.rockserver.core.common.api.RangeRequestType.ALL_IN_RANGE_NO_CACHE.getValue());
		assertEquals(44,
				it.cavallium.rockserver.core.common.api.RocksDBErrorType.RANGE_ITEM_TOO_LARGE.getValue());
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
		assertNotNull(RocksDB.Iface.class.getMethod("getCapabilities"));
		assertNotNull(RocksDB.Iface.class.getMethod("put",
				long.class, long.class, List.class, ByteBuffer.class,
				it.cavallium.rockserver.core.common.api.RequestContext.class));
		assertNotNull(RocksDB.Iface.class.getMethod("deleteRange",
				long.class, List.class, List.class,
				it.cavallium.rockserver.core.common.api.RequestContext.class));
		assertNotNull(RocksDB.Iface.class.getMethod("closeIterator", long.class));
		assertNotNull(RocksDB.Iface.class.getMethod("getRangePage",
				long.class, long.class, List.class, List.class, boolean.class, List.class,
				it.cavallium.rockserver.core.common.api.RangeRequestType.class,
				long.class,
				it.cavallium.rockserver.core.common.api.RangeBudget.class,
				it.cavallium.rockserver.core.common.api.RequestContext.class));
		assertNotNull(RocksDB.Iface.class.getMethod("flush"));
	}

	@Test
	void everyGrpcAndThriftRequestVariantHasExactlyOneDeclaredContextPolicy() {
		var schemaSupplier = org.junit.jupiter.api.Assertions.assertInstanceOf(
				ProtoFileDescriptorSupplier.class,
				it.cavallium.rockserver.core.common.api.proto.RocksDBServiceGrpc
						.getServiceDescriptor().getSchemaDescriptor());
		var service = schemaSupplier.getFileDescriptor().findServiceByName("RocksDBService");
		assertNotNull(service);
		var grpcNames = new HashSet<String>();
		for (var method : service.getMethods()) {
			grpcNames.add(method.getName());
			boolean hasContext = containsRequestContext(method.getInputType(), new HashSet<>());
			if (CONTEXT_FREE_METHODS.contains(method.getName())) {
				assertFalse(hasContext, method.getFullName() + " must remain server-owned/context-free");
			} else {
				assertTrue(hasContext, method.getFullName() + " has no request context in its unary/stream initial shape");
			}
		}
		assertTrue(grpcNames.containsAll(CONTEXT_FREE_METHODS),
				"the explicit protected/capability set contains a stale or missing gRPC variant");

		assertThriftContextPolicy(RocksDB.Iface.class);
		assertThriftContextPolicy(RocksDB.AsyncIface.class);
	}

	private static boolean containsRequestContext(Descriptors.Descriptor descriptor,
			Set<Descriptors.Descriptor> visited) {
		if (!visited.add(descriptor)) return false;
		var direct = descriptor.findFieldByName("context");
		if (direct != null) {
			return direct.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE
					&& direct.getMessageType().getFullName().endsWith(".RequestContext");
		}
		for (var field : descriptor.getFields()) {
			if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE
					&& containsRequestContext(field.getMessageType(), visited)) {
				return true;
			}
		}
		return false;
	}

	private static void assertThriftContextPolicy(Class<?> api) {
		var names = new HashSet<String>();
		for (Method method : api.getDeclaredMethods()) {
			names.add(method.getName());
			long contexts = java.util.Arrays.stream(method.getParameterTypes())
					.filter(type -> type == it.cavallium.rockserver.core.common.api.RequestContext.class)
					.count();
			if (CONTEXT_FREE_METHODS.contains(method.getName())) {
				assertEquals(0L, contexts, api.getSimpleName() + "." + method.getName());
			} else {
				assertEquals(1L, contexts, api.getSimpleName() + "." + method.getName());
			}
		}
		assertTrue(names.contains("getCapabilities"));
		assertTrue(names.contains("closeFailedUpdate"));
		assertTrue(names.contains("closeIterator"));
		assertTrue(names.contains("flush"));
		assertTrue(names.contains("compact"));
	}

	@Test
	void thriftServerRejectsEveryProtectedProfileSpoofAtTheWireBoundary() throws Exception {
		var mapper = ThriftServer.class.getDeclaredMethod("mapRequestContext",
				it.cavallium.rockserver.core.common.api.RequestContext.class);
		mapper.setAccessible(true);
		for (var profile : List.of(
				it.cavallium.rockserver.core.common.api.WorkloadProfile.CONTROL,
				it.cavallium.rockserver.core.common.api.WorkloadProfile.CDC,
				it.cavallium.rockserver.core.common.api.WorkloadProfile.PHYSICAL_MAINTENANCE)) {
			var wire = new it.cavallium.rockserver.core.common.api.RequestContext(
					profile,
					it.cavallium.rockserver.core.common.RequestContext.NO_DEADLINE);
			var invocation = assertThrows(InvocationTargetException.class,
					() -> mapper.invoke(null, wire));
			var failure = org.junit.jupiter.api.Assertions.assertInstanceOf(
					RocksDBException.class, invocation.getCause());
			assertEquals(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST,
					failure.getErrorUniqueId());
		}

		var expired = new it.cavallium.rockserver.core.common.api.RequestContext(
				it.cavallium.rockserver.core.common.api.WorkloadProfile.BATCH,
				1L);
		var invocation = assertThrows(InvocationTargetException.class,
				() -> mapper.invoke(null, expired));
		var failure = org.junit.jupiter.api.Assertions.assertInstanceOf(
				RocksDBException.class, invocation.getCause());
		assertEquals(RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED,
				failure.getErrorUniqueId());
	}
}
