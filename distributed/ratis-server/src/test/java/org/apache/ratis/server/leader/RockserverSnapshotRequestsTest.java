package org.apache.ratis.server.leader;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.*;
import java.util.*;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.*;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.statemachine.*;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.util.MD5FileUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Actual released chunk generator/file reader; controlled Division only, no RaftServer or socket. */
class RockserverSnapshotRequestsTest {
    @TempDir Path root;
    @SuppressWarnings("unchecked") static <T>T proxy(Class<T> type,java.lang.reflect.InvocationHandler handler){return (T)Proxy.newProxyInstance(type.getClassLoader(),new Class<?>[]{type},handler);}
    RaftServer.Division source(Path file){
        var group=RaftGroupId.valueOf(new UUID(10,1));var peer=RaftPeerId.valueOf("source");var voter=RaftPeer.newBuilder().setId(peer).setAddress("127.0.0.1:9901").build();
        var storage=proxy(StateMachineStorage.class,(p,m,a)->{if(m.getName().equals("getSnapshotDir"))return file.getParent().toFile();throw new AssertionError(m);});var machine=proxy(StateMachine.class,(p,m,a)->{if(m.getName().equals("getStateMachineStorage"))return storage;throw new AssertionError(m);});
        var configuration=proxy(RaftConfiguration.class,(p,m,a)->switch(m.getName()){case "getLogEntryIndex"->4L;case "getCurrentPeers"->a==null || a.length==0 || a[0]==RaftPeerRole.FOLLOWER?List.of(voter):List.of();case "getPreviousPeers"->List.of();default->throw new AssertionError(m);});
        var info=proxy(DivisionInfo.class,(p,m,a)->{if(m.getName().equals("getCurrentTerm"))return 2L;throw new AssertionError(m);});
        return proxy(RaftServer.Division.class,(p,m,a)->switch(m.getName()){case "getStateMachine"->machine;case "getRaftConf"->configuration;case "getMemberId"->RaftGroupMemberId.valueOf(peer,group);case "getInfo"->info;case "getId"->peer;default->throw new AssertionError(m);});
    }
    long readers(Path file)throws IOException{try(var descriptors=Files.list(Path.of("/proc/self/fd"))){return descriptors.filter(fd->{try{return Files.readSymbolicLink(fd).equals(file.toAbsolutePath());}catch(IOException raced){return false;}}).count();}}
    @Test void earlyRpcFailureClosesActualReaderAndExactRetryGeneratesSameChunks()throws Exception{
        org.junit.jupiter.api.Assumptions.assumeTrue(Files.isDirectory(Path.of("/proc/self/fd")));byte[] contents=new byte[3073];for(int i=0;i<contents.length;i++)contents[i]=(byte)i;var file=Files.write(root.resolve("snapshot.1_7"),contents);var snapshot=new SingleFileSnapshotInfo(new FileInfo(file,MD5FileUtil.computeMd5ForFile(file.toFile())),TermIndex.valueOf(1,7));var source=source(file);byte[] original;
        assertEquals(0,readers(file));try(var requests=RockserverSnapshotRequests.open(source,RaftPeerId.valueOf("receiver"),"fixed-transfer",snapshot,1024)){var first=requests.next();original=first.toByteArray();assertEquals(1,readers(file));assertFalse(first.getSnapshotChunk().getDone());assertEquals(1024,first.getSnapshotChunk().getFileChunks(0).getData().size());assertThrows(IOException.class,()->{throw new IOException("Controlled RPC failed after first chunk");});}
        assertEquals(0,readers(file));var assembled=new java.io.ByteArrayOutputStream();int index=0;try(var requests=RockserverSnapshotRequests.open(source,RaftPeerId.valueOf("receiver"),"fixed-transfer",snapshot,1024)){while(requests.hasNext()){var request=requests.next();if(index==0)assertArrayEquals(original,request.toByteArray());assertEquals(index++,request.getSnapshotChunk().getRequestIndex());assertEquals(2,request.getLeaderTerm());assertEquals(7,request.getSnapshotChunk().getTermIndex().getIndex());var chunk=request.getSnapshotChunk().getFileChunks(0);assertEquals(assembled.size(),chunk.getOffset());assertEquals(contents.length,request.getSnapshotChunk().getTotalSize());assembled.write(chunk.getData().toByteArray());assertEquals(!requests.hasNext(),request.getSnapshotChunk().getDone());}}assertEquals(4,index);assertArrayEquals(contents,assembled.toByteArray());assertEquals(0,readers(file));
    }
    @Test void sourceOwnedImageMapsCanonicalWireNameAndClosesActualReader()throws Exception{
        org.junit.jupiter.api.Assumptions.assumeTrue(Files.isDirectory(Path.of("/proc/self/fd")));var owned=Files.createDirectories(root.resolve("source-owned"));byte[] contents=new byte[2305];new Random(9).nextBytes(contents);var file=Files.write(owned.resolve(UUID.randomUUID()+".image"),contents);var snapshot=new SingleFileSnapshotInfo(new FileInfo(file,MD5FileUtil.computeMd5ForFile(file.toFile())),TermIndex.valueOf(1,9));var managed=Files.createDirectories(root.resolve("raft-managed")).resolve("snapshot.1_9");var source=source(managed);byte[] first;
        try(var chunks=RockserverSnapshotRequests.openSource(source,RaftPeerId.valueOf("receiver"),"owned-source",snapshot,1024)){var chunk=chunks.next();first=chunk.toByteArray();assertEquals("sm/snapshot.1_9",chunk.getSnapshotChunk().getFileChunks(0).getFilename());assertEquals(1,readers(file));}assertEquals(0,readers(file));
        var assembled=new java.io.ByteArrayOutputStream();try(var chunks=RockserverSnapshotRequests.openSource(source,RaftPeerId.valueOf("receiver"),"owned-source",snapshot,1024)){int index=0;while(chunks.hasNext()){var chunk=chunks.next();if(index++==0)assertArrayEquals(first,chunk.toByteArray());assertEquals("sm/snapshot.1_9",chunk.getSnapshotChunk().getFileChunks(0).getFilename());assembled.write(chunk.getSnapshotChunk().getFileChunks(0).getData().toByteArray());}}assertArrayEquals(contents,assembled.toByteArray());assertEquals(0,readers(file));assertFalse(Files.exists(managed));
    }
}
