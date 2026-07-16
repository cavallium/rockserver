package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.server.GrpcServer;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.ThreadPoolExecutor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(60)
class GrpcIteratorSchedulingTest {

	@TempDir
	Path tempDir;

	@Test
	void nonMaterializingRemoteAdvanceUsesLargeBoundedSteps() throws Exception {
		final int entries = 4_200;
		try (var embedded = new EmbeddedConnection(tempDir.resolve("db"), "grpc-iterator-scheduling", null)) {
			var backend = embedded.getSyncApi();
			long columnId = backend.createColumn("entries",
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			for (int i = 0; i < entries; i++) {
				backend.put(0, columnId, key(i), value(i), RequestType.none());
			}

			try (var server = new GrpcServer(embedded, new InetSocketAddress("127.0.0.1", 0))) {
				server.start();
				try (var client = GrpcConnection.forHostAndPort("grpc-iterator-scheduling",
						new Utils.HostAndPort("127.0.0.1", server.getPort()))) {
					var api = client.getSyncApi();
					long iteratorId = api.openIterator(0, columnId, null, null, false, 10_000);
					try {
						var readExecutor = (ThreadPoolExecutor) embedded.getInternalDB()
								.getScheduler()
								.readExecutor();
						long tasksBefore = readExecutor.getTaskCount();

						api.subsequent(iteratorId, 0, entries, RequestType.none());

						long scheduledTasks = readExecutor.getTaskCount() - tasksBefore;
						assertEquals(2L, scheduledTasks,
								"4,200 non-materializing entries should use two 4,096-entry read steps");
					} finally {
						api.closeIterator(iteratorId);
					}
				}
			}
			assertEquals(0, embedded.getInternalDB().getOpenIteratorsCount());
			assertEquals(0L, embedded.getInternalDB().getPendingOpsCount());
		}
	}

	private static Keys key(int value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(value).array()));
	}

	private static Buf value(int value) {
		return Buf.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
	}
}
