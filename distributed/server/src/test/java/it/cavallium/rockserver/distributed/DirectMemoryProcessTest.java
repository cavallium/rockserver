package it.cavallium.rockserver.distributed;

import org.junit.jupiter.api.Tag;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * CR2-1, in real processes: a client message larger than a node's direct memory fails that request, on every node the
 * client tries, and halts none of them. Before, the direct memory error on the API's event loop stopped the node with
 * exit code 3, and the client's retry carried it to the next node.
 */
@Timeout(240)
@Tag("slow")
class DirectMemoryProcessTest {

	private static final Duration READY = Duration.ofSeconds(60);
	private static final RequestContext CONTEXT = RequestContext.batch(Duration.ofSeconds(30));

	@TempDir
	Path root;

	@Test
	void aClientMessageLargerThanDirectMemoryHaltsNoNode() throws Exception {
		try (var cluster = ProcessCluster.create(root, 3, 0, List.of())) {
			for (var id : cluster.members()) {
				// The API accepts a message larger than the node's direct memory; the client sends one.
				cluster.setJvmOptions(id, List.of("-XX:MaxDirectMemorySize=96m",
						"-Dit.cavallium.rockserver.grpc.server.max-inbound-message-bytes=" + (256 << 20)));
				cluster.start(id);
			}
			cluster.waitReady(READY);
			var addresses = new ArrayList<HostAndPort>();
			cluster.members().forEach(id -> addresses.add(cluster.apiAddress(id)));
			try (var client = GrpcConnection.forHostsAndPorts("direct-memory-client", addresses)) {
				var api = client.getSyncApi(CONTEXT);
				long column = api.createColumn("c", ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
				var huge = Buf.wrap(new byte[160 << 20]);
				assertThrows(RuntimeException.class, () -> api.put(0, column, key(1), huge, RequestType.none()));
				// The JFR events arrive within about a second: a node that would halt has done it by now.
				Thread.sleep(5_000);
				for (var id : cluster.members()) {
					assertTrue(cluster.isRunning(id), () -> "node " + id + " stopped:\n" + cluster.logTail(id));
					assertTrue(!cluster.output(id).contains("ran out of direct memory, exiting"), () -> cluster.logTail(id));
				}
				api.put(0, column, key(2), Buf.wrap(new byte[] {7}), RequestType.none());
				assertArrayEquals(new byte[] {7}, api.get(0, column, key(2), RequestType.current()).toByteArray());
			}
		}
	}

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}
}
