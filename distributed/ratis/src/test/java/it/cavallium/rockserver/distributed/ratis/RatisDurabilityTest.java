package it.cavallium.rockserver.distributed.ratis;

import java.nio.file.Path;
import it.cavallium.rockserver.core.common.distributed.NativeSstImportManifest;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RatisDurabilityTest {
    @Test void smallOwnedWriteBufferPreservesLegalEntryAndSynchronousDurability() {
        var properties=RatisDurability.properties(Path.of("raft-config-fixture"));
        assertFalse(RaftServerConfigKeys.ThreadPool.proxyCached(properties));
        assertEquals(1,RaftServerConfigKeys.ThreadPool.proxySize(properties));
        assertEquals(64L*1024,RaftServerConfigKeys.Log.writeBufferSize(properties).getSize());
        assertEquals(NativeSstImportManifest.MAX_FILE_BYTES+4096L,RaftServerConfigKeys.Log.Appender.bufferByteLimit(properties).getSize());
        long messageBytes=org.apache.ratis.grpc.GrpcConfigKeys.messageSizeMax(properties,ignored->{}).getSize();
        assertTrue(messageBytes-RaftServerConfigKeys.Log.Appender.bufferByteLimit(properties).getSize()>=org.apache.ratis.util.SizeInBytes.ONE_MB.getSize());
        assertTrue(messageBytes>=RatisTopologySourceSnapshotRpc.MAX_REPLY_BYTES+4096L);
        assertFalse(RaftServerConfigKeys.Log.unsafeFlushEnabled(properties));
        assertFalse(RaftServerConfigKeys.Log.asyncFlushEnabled(properties));
        assertTrue(RaftServerConfigKeys.Log.StateMachineData.sync(properties));
        assertFalse(RaftServerConfigKeys.Log.StateMachineData.cachingEnabled(properties));
    }
}
