package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** Borrowed proxy transport only; returned artifacts never imply configured-replica readiness. */
class RatisTopologyFreezeClientTest {
    final class Fixture {
        final RatisTopologyFreezeRecord record=RatisTopologyFreezeRpcTest.record();
        final List<byte[]> writes=new ArrayList<>(),reads=new ArrayList<>();
        final RaftClient raft;
        final RatisTopologyFreezeClient client;
        boolean loseReply,crossGroup,badRecord,oversized;
        int closes;
        Fixture(UUID group) {
            var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args)->{
                byte[] bytes=((Message)args[0]).getContent().toByteArray(); byte[] output;
                if(method.getName().equals("send")) { writes.add(bytes); var decoded=RatisTopologyFreezeRecord.decode(bytes); output=RatisTopologyFreezeRpcTest.artifact(decoded).encode(); if(loseReply) { loseReply=false; throw new IOException("Original freeze reply lost after local commit"); } }
                else if(method.getName().equals("sendReadOnly")) {
                    reads.add(bytes); var decoded=RatisTopologyFreezeRpc.request(bytes); int kind=ScopedTransactionRpc.kind(bytes,RatisTopologyFreezeRpc.MAX_REQUEST_BYTES);
                    if(badRecord) { byte[] digest=decoded.admissionDigest(); digest[0]^=1; decoded=new RatisTopologyFreezeRecord(decoded.definition(),decoded.groupId(),digest); }
                    output=kind==RatisTopologyFreezeRpc.CANCEL ? RatisTopologyFreezeRpc.cancelledReply(decoded) : RatisTopologyFreezeRpc.reply(kind,new RatisTopologyFreezeRpc.State(decoded,writes.isEmpty() ? null : RatisTopologyFreezeRpcTest.artifact(decoded),0,0,0,true));
                } else throw new AssertionError(method);
                if(oversized) output=new byte[RatisTopologyFreezeRpc.MAX_REPLY_BYTES+1];
                return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf("local-fixture")).setGroupId(RaftGroupId.valueOf(crossGroup ? UUID.randomUUID() : group)).setCallId(1).setSuccess(true).setMessage(Message.valueOf(ByteString.copyFrom(output))).build();
            });
            raft=(RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args)->switch(method.getName()) { case "getGroupId" -> RaftGroupId.valueOf(group); case "io" -> io; case "close" -> { closes++; yield null; } default -> throw new AssertionError(method); }); client=new RatisTopologyFreezeClient(raft);
        }
    }
    @Test void readyPrepareUnknownAppendExactRetryStateAndCancelPreserveOriginalArtifact() throws Exception {
        var record=RatisTopologyFreezeRpcTest.record(); var f=new Fixture(record.groupId());
        var prepared=f.client.prepare(f.record); assertTrue(prepared.ready()); assertNull(prepared.freeze()); f.loseReply=true; assertThrows(IOException.class,()->f.client.append(f.record));
        f.client.prepare(f.record); var artifact=f.client.append(f.record); assertArrayEquals(f.writes.getFirst(),f.writes.getLast()); assertEquals(RatisTopologyFreezeRpcTest.artifact(f.record),artifact); f.record.requireFreeze(artifact);
        assertEquals(artifact,f.client.state(f.record).freeze()); f.client.cancel(f.record); assertEquals(0,f.closes);
    }
    @Test void wrongBoundClientCrossedGroupExactRecordAndOversizedRepliesNeverSucceed() throws Exception {
        var wrong=new Fixture(UUID.randomUUID()); assertThrows(IOException.class,()->wrong.client.prepare(wrong.record)); assertThrows(IOException.class,()->wrong.client.append(wrong.record)); assertTrue(wrong.reads.isEmpty()); assertTrue(wrong.writes.isEmpty());
        var f=new Fixture(RatisTopologyFreezeRpcTest.record().groupId()); f.crossGroup=true; assertThrows(IOException.class,()->f.client.state(f.record)); assertThrows(IOException.class,()->f.client.append(f.record)); f.crossGroup=false;
        f.badRecord=true; assertThrows(IOException.class,()->f.client.prepare(f.record)); assertThrows(IOException.class,()->f.client.cancel(f.record)); f.badRecord=false; f.oversized=true;
        assertThrows(IOException.class,()->f.client.state(f.record)); assertThrows(IOException.class,()->f.client.append(f.record)); assertEquals(0,f.closes);
    }
}
