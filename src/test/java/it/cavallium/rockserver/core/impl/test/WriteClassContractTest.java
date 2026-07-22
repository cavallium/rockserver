package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.protobuf.ByteString;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.MergeBatchMode;
import it.cavallium.rockserver.core.common.PutBatchMode;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.WriteClass;
import it.cavallium.rockserver.core.common.api.RocksDB;
import it.cavallium.rockserver.core.common.api.RocksDBWriteClass;
import it.cavallium.rockserver.core.common.api.proto.DeleteMultiInitialRequest;
import it.cavallium.rockserver.core.common.api.proto.PutBatchInitialRequest;
import it.cavallium.rockserver.core.common.api.proto.PutRequest;
import java.nio.ByteBuffer;
import java.util.List;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

class WriteClassContractTest {

	private static final Keys KEYS = new Keys(Buf.wrap(new byte[] {1}));
	private static final Buf VALUE = Buf.wrap(new byte[] {2});
	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(1), ObjectList.of(), true);

	@Test
	void legacyCommandConstructorsDefaultEveryCallerClassifiedMutationToForeground() {
		List<RocksDBAPICommand<?, ?, ?>> commands = List.of(
				new RocksDBAPICommand.RocksDBAPICommandSingle.CloseTransaction(1, true),
				new RocksDBAPICommand.RocksDBAPICommandSingle.CreateColumn("column", SCHEMA),
				new RocksDBAPICommand.RocksDBAPICommandSingle.DeleteColumn(1),
				new RocksDBAPICommand.RocksDBAPICommandSingle.DeleteColumnIfExists("column"),
				new RocksDBAPICommand.RocksDBAPICommandSingle.Put<>(0, 1, KEYS, VALUE, RequestType.none()),
				new RocksDBAPICommand.RocksDBAPICommandSingle.Delete<>(0, 1, KEYS, RequestType.none()),
				new RocksDBAPICommand.RocksDBAPICommandSingle.DeleteMulti<>(0, 1, List.of(KEYS), RequestType.none()),
				new RocksDBAPICommand.RocksDBAPICommandSingle.DeleteRange(1, KEYS, KEYS),
				new RocksDBAPICommand.RocksDBAPICommandSingle.PutMulti<>(
						0, 1, List.of(KEYS), List.of(VALUE), RequestType.none()),
				new RocksDBAPICommand.RocksDBAPICommandSingle.PutBatch(
						1, Flux.empty(), PutBatchMode.WRITE_BATCH),
				new RocksDBAPICommand.RocksDBAPICommandSingle.Merge<>(
						0, 1, KEYS, VALUE, RequestType.none()),
				new RocksDBAPICommand.RocksDBAPICommandSingle.MergeMulti<>(
						0, 1, List.of(KEYS), List.of(VALUE), RequestType.none()),
				new RocksDBAPICommand.RocksDBAPICommandSingle.MergeBatch(
						1, Flux.empty(), MergeBatchMode.MERGE_WRITE_BATCH));

		commands.forEach(command -> assertEquals(WriteClass.FOREGROUND, command.writeClass(),
				command.getClass().getSimpleName()));
	}

	@Test
	void explicitCommandsRetainMaintenance() {
		assertEquals(WriteClass.MAINTENANCE,
				new RocksDBAPICommand.RocksDBAPICommandSingle.Put<>(
						0, 1, KEYS, VALUE, RequestType.none(), WriteClass.MAINTENANCE).writeClass());
		assertEquals(WriteClass.MAINTENANCE,
				new RocksDBAPICommand.RocksDBAPICommandSingle.DeleteRange(
						1, KEYS, KEYS, WriteClass.MAINTENANCE).writeClass());
		assertEquals(WriteClass.MAINTENANCE,
				new RocksDBAPICommand.RocksDBAPICommandSingle.CloseTransaction(
						1, true, WriteClass.MAINTENANCE).writeClass());
	}

	@Test
	void protobufDefaultsLegacyRequestsAndRetainsExplicitOrUnknownValues() throws Exception {
		var legacy = PutRequest.parseFrom(PutRequest.newBuilder()
				.setColumnId(1)
				.setData(it.cavallium.rockserver.core.common.api.proto.KV.newBuilder()
						.addKeys(ByteString.copyFrom(new byte[] {1}))
						.setValue(ByteString.copyFrom(new byte[] {2})))
				.build()
				.toByteArray());
		assertEquals(it.cavallium.rockserver.core.common.api.proto.WriteClass.FOREGROUND,
				legacy.getWriteClass());

		var maintenance = legacy.toBuilder()
				.setWriteClass(it.cavallium.rockserver.core.common.api.proto.WriteClass.MAINTENANCE)
				.build();
		assertEquals(1, PutRequest.parseFrom(maintenance.toByteArray()).getWriteClassValue());

		var unknown = legacy.toBuilder().setWriteClassValue(99).build();
		assertEquals(99, PutRequest.parseFrom(unknown.toByteArray()).getWriteClassValue());

		assertEquals(0, PutBatchInitialRequest.getDefaultInstance().getWriteClassValue());
		assertEquals(0, DeleteMultiInitialRequest.getDefaultInstance().getWriteClassValue());
	}

	@Test
	void thriftKeepsLegacyGeneratedSignaturesAndAddsClassifiedService() throws Exception {
		assertNotNull(RocksDB.Iface.class.getMethod("put",
				long.class, long.class, List.class, ByteBuffer.class));
		assertNotNull(RocksDB.Iface.class.getMethod("deleteRange",
				long.class, List.class, List.class));
		assertNotNull(RocksDBWriteClass.Iface.class.getMethod("putWithWriteClass",
				long.class, long.class, List.class, ByteBuffer.class, int.class));
		assertNotNull(RocksDBWriteClass.Iface.class.getMethod("deleteRangeWithWriteClass",
				long.class, List.class, List.class, int.class));
	}
}
