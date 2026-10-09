package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.*;

import it.cavallium.rockserver.core.common.RangeBudget;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RockserverCapabilities;
import it.cavallium.rockserver.core.common.RockserverCapabilities.BackendMode;
import it.cavallium.rockserver.core.common.RockserverCapabilities.RawScanMode;
import it.cavallium.rockserver.core.common.api.proto.CapabilitiesResponse;
import java.lang.reflect.InvocationTargetException;
import org.junit.jupiter.api.Test;

class RockserverCapabilitiesTest {
	@Test void transportMaskPreservesActualBackendAndRangeLimits() {
		var backend = new RockserverCapabilities(BackendMode.DISTRIBUTED,
				RawScanMode.IMMUTABLE_EXPORT_JOBS, 123, 456);
		var transport = backend.withoutRawScan();
		assertEquals(BackendMode.DISTRIBUTED, transport.backendMode());
		assertEquals(RawScanMode.UNSUPPORTED, transport.rawScanMode());
		assertEquals(123, transport.maximumRangePageKeys());
		assertEquals(456, transport.maximumRangePageBytes());
		assertThrows(RocksDBException.class, transport::requirePhysicalSst);
		assertThrows(RocksDBException.class, backend::requirePhysicalSst);
		assertDoesNotThrow(RockserverCapabilities.STANDALONE::requirePhysicalSst);
	}
	@Test void pageBudgetUsesAdvertisedLimits() {
		var caps = new RockserverCapabilities(BackendMode.STANDALONE, RawScanMode.UNSUPPORTED, 2, 3);
		assertDoesNotThrow(() -> caps.requireRangeBudget(new RangeBudget(2, 3)));
		assertThrows(RocksDBException.class, () -> caps.requireRangeBudget(new RangeBudget(3, 3)));
		assertThrows(RocksDBException.class, () -> caps.requireRangeBudget(new RangeBudget(2, 4)));
		assertThrows(IllegalArgumentException.class, () -> new RockserverCapabilities(
				BackendMode.DISTRIBUTED, RawScanMode.PHYSICAL_SST, 1, 1));
		assertThrows(IllegalArgumentException.class, () -> new RockserverCapabilities(
				BackendMode.STANDALONE, RawScanMode.UNSUPPORTED, 0, 1));
	}
	@Test void transportLimitReservesCursorAndEnvelope() throws Exception {
		long transportBytes = 4L*1024*1024;
		var caps = RockserverCapabilities.STANDALONE.withTransportLimit(transportBytes);
		assertEquals((transportBytes-64*1024)/2, caps.maximumRangePageBytes());
		assertEquals(RockserverCapabilities.STANDALONE,
				RockserverCapabilities.STANDALONE.withTransportLimit(64L*1024*1024));
		assertThrows(IllegalArgumentException.class,
				() -> caps.withTransportLimit(64*1024));
		var key = new byte[(int)caps.maximumRangePageBytes()-22];
		var logical = new it.cavallium.rockserver.core.common.KV(
				new it.cavallium.rockserver.core.common.Keys(it.cavallium.buffer.Buf.wrap(key)), null);
		assertEquals(caps.maximumRangePageBytes(), RangeBudget.itemBytes(logical));
		var item = it.cavallium.rockserver.core.common.api.proto.KV.newBuilder()
				.addKeys(com.google.protobuf.ByteString.copyFrom(key)).build();
		var response = it.cavallium.rockserver.core.common.api.proto.RangePage.newBuilder().addItems(item)
				.setResumeAfter(it.cavallium.rockserver.core.common.api.proto.RangeKey.newBuilder()
						.addKeys(com.google.protobuf.ByteString.copyFrom(key))).build();
		assertTrue(response.getSerializedSize() <= transportBytes);
		assertEquals(response, it.cavallium.rockserver.core.common.api.proto.RangePage.parseFrom(response.toByteArray()));
	}
	@Test void emptyKeyComponentsStillConsumeEncodedPageBudget() {
		var parts = new it.cavallium.buffer.Buf[1024];
		java.util.Arrays.fill(parts, it.cavallium.buffer.Buf.wrap(new byte[0]));
		var item = new it.cavallium.rockserver.core.common.KV(
				new it.cavallium.rockserver.core.common.Keys(parts), null);
		assertEquals(16L+6L*parts.length, RangeBudget.itemBytes(item));
	}
	@Test void grpcDecodingRejectsUnknownModesMissingModesAndInvalidLimits() throws Exception {
		var decoder = Class.forName("it.cavallium.rockserver.core.client.GrpcConnectionDelegate")
				.getDeclaredMethod("decodeCapabilities", CapabilitiesResponse.class);
		decoder.setAccessible(true);
		var valid = CapabilitiesResponse.newBuilder().setBackendModeValue(1).setRawScanModeValue(2)
				.setMaximumRangePageKeys(4096).setMaximumRangePageBytes(8L*1024*1024).build();
		assertEquals(RockserverCapabilities.STANDALONE, decoder.invoke(null, valid));
		for (var invalid : java.util.List.of(CapabilitiesResponse.getDefaultInstance(),
				valid.toBuilder().setBackendModeValue(999).build(),
				valid.toBuilder().setRawScanModeValue(0).build(),
				valid.toBuilder().setRawScanModeValue(999).build(),
				valid.toBuilder().setMaximumRangePageBytes(-1).build(),
				valid.toBuilder().setBackendModeValue(2).build())) {
			var wrapper = assertThrows(InvocationTargetException.class, () -> decoder.invoke(null, invalid));
			var failure = assertInstanceOf(RocksDBException.class, wrapper.getCause());
			assertEquals(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED, failure.getErrorUniqueId());
		}
	}
}
