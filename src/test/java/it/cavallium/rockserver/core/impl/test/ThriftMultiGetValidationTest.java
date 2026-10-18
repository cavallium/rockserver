package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RockserverCapabilities;
import it.cavallium.rockserver.core.common.api.RangeBudget;
import it.cavallium.rockserver.core.common.api.RocksDB;
import it.cavallium.rockserver.core.common.api.RocksDBErrorType;
import it.cavallium.rockserver.core.common.api.RocksDBThriftException;
import it.cavallium.rockserver.core.common.api.WorkloadProfile;
import it.cavallium.rockserver.core.server.ThriftServer;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
class ThriftMultiGetValidationTest {

	@Test
	void malformedWireRequestPreservesGenericErrorAndConnectionRemainsUsable(@TempDir Path directory)
			throws Exception {
		int port;
		try (var socket = new ServerSocket(0)) {
			port = socket.getLocalPort();
		}
		try (var embedded = new EmbeddedConnection(directory.resolve("db"), "thrift-multi-get-validation", null);
				var server = new ThriftServer(embedded, "127.0.0.1", port)) {
			long column = embedded.getSyncApi(RequestContext.batch()).createColumn("entries",
					ColumnSchema.of(IntList.of(1), ObjectList.of(), true));
			server.start();
			try (var transport = new TFramedTransport(new TSocket("127.0.0.1", port, 5_000))) {
				transport.open();
				var client = new RocksDB.Client(new TBinaryProtocol(transport));
				var context = new it.cavallium.rockserver.core.common.api.RequestContext()
						.setProfile(WorkloadProfile.BATCH).setTimeoutNanos(Long.MAX_VALUE)
						.setWorkloadContractVersion(RockserverCapabilities.REQUIRED_WORKLOAD_CONTRACT_VERSION);
				var keys = List.of(List.of(ByteBuffer.wrap(new byte[]{1})));
				// Bypass the public API's local validation to exercise the serialized error enum.
				var failure = assertThrows(RocksDBThriftException.class,
						() -> client.getMulti(column, keys, new RangeBudget(129, 64), context));
				assertEquals(RocksDBErrorType.INVALID_REQUEST, failure.getErrorType());
				var page = client.getMulti(column, keys, new RangeBudget(1, 64), context);
				assertEquals(1, page.getValuesSize());
				assertEquals(false, page.getValues().getFirst().isSetValue());
				assertEquals(false, page.isHasMore());
				assertEquals(0, embedded.getInternalDB().getPendingOpsCount());
			}
		}
	}
}
