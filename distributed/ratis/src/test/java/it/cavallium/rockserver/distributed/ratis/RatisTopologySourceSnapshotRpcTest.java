package it.cavallium.rockserver.distributed.ratis;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySourceSnapshotRpc.*;

class RatisTopologySourceSnapshotRpcTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void canonicalImageMetadataRoundtripNeverCarriesAHostPath()throws Exception {
        try(var fixture=new RatisTopologySourceSnapshotFixture(root);var service=new RatisTopologySourceSnapshotService(fixture,RatisTopologySourceSnapshotService.Limits.defaults(8*1024*1024))){
            var request=fixture.request(50);var prepared=service.prepare(request);
            for(int kind:new int[]{PREPARE,TRANSFER,RELEASE})assertArrayEquals(request(kind,request),request(kind,request(request(kind,request))));
            var bytes=reply(PREPARE,prepared);var decoded=reply(bytes,PREPARE,request);
            assertArrayEquals(bytes,reply(PREPARE,decoded));assertArrayEquals(prepared.image().sha256(),decoded.image().sha256());assertArrayEquals(prepared.sourceCfg().data(),decoded.sourceCfg().data());assertTrue(decoded.schemaRows().isEmpty());
            assertFalse(new String(bytes,java.nio.charset.StandardCharsets.ISO_8859_1).contains(root.toString()));
            assertThrows(IllegalArgumentException.class,()->reply(bytes,TRANSFER,request));assertThrows(IllegalArgumentException.class,()->reply(bytes,PREPARE,fixture.request(51)));
            assertThrows(IllegalArgumentException.class,()->reply(Arrays.copyOf(bytes,bytes.length-1),PREPARE,request));
        }
    }
    @Test void malformedRequestLengthsAndKindsAreRejectedBeforeHostWork()throws Exception {
        try(var fixture=new RatisTopologySourceSnapshotFixture(root)){
            byte[] bytes=request(PREPARE,fixture.request(50));var oversized=bytes.clone();ByteBuffer.wrap(oversized).putInt(ScopedTransactionRpc.HEADER_BYTES+32,Integer.MAX_VALUE);
            assertThrows(IllegalArgumentException.class,()->request(oversized));assertThrows(IllegalArgumentException.class,()->request(Arrays.copyOf(bytes,bytes.length-1)));
            assertThrows(IllegalArgumentException.class,()->request(108,fixture.request(50)));assertEquals(0,fixture.captures.get());
        }
    }
}
