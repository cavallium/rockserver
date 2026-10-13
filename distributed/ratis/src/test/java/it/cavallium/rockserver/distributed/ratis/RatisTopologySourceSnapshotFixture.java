package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.*;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.*;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.statemachine.*;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.slf4j.LoggerFactory;
import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

/** Real native image and released chunk producer; only the division/receiver transport is controlled. */
final class RatisTopologySourceSnapshotFixture implements RatisTopologySourceSnapshotService.Host,AutoCloseable {
    static UUID id(int n){return new UUID(0,n);}
    final UUID group=id(4),column=id(5);
    final ReplicaDescriptor donor=new ReplicaDescriptor(id(10),id(11),id(12),"donor","127.0.0.1:7001",ReplicaDescriptor.Role.VOTER);
    final ReplicaDescriptor receiver=new ReplicaDescriptor(id(20),id(21),id(22),"receiver","127.0.0.1:7002",ReplicaDescriptor.Role.LISTENER);
    final DistributedSchema schema=new DistributedSchema(id(1),id(2),id(3),0,1,List.of(new DistributedSchema.Column(1,column,"data",ColumnSchema.of(IntArrayList.of(1),ObjectArrayList.of(),true),null)));
    final TopologyChange.Definition definition;
    final Path root,image;
    final DistributedStoragePool pool;
    final ReplicatedRangeStore store;
    final RaftServer.Division division=mock(RaftServer.Division.class);
    final DivisionInfo info=mock(DivisionInfo.class);
    final AtomicInteger captures=new AtomicInteger(),pinCloses=new AtomicInteger(),calls=new AtomicInteger(),proofs=new AtomicInteger(),durableReleases=new AtomicInteger(),releaseLookups=new AtomicInteger();
    final ByteArrayOutputStream received=new ByteArrayOutputStream();
    volatile CountDownLatch entered,unblock;
    volatile boolean failFirst,unknownReply,alreadyInstalled,installed=true,owner=true,failRelease;
    RatisTopologySourceSnapshotFixture(Path root)throws Exception {this(root,false);}
    RatisTopologySourceSnapshotFixture(Path root,boolean bootstrap)throws Exception {this(root,bootstrap,1);}
    RatisTopologySourceSnapshotFixture(Path root,boolean bootstrap,int voters)throws Exception {
        this.root=root;Files.createDirectories(root.resolve("snapshots"));image=root.resolve("snapshots/native.image");
        var members=new ArrayList<ReplicaDescriptor>();members.add(donor);for(int n=1;n<voters;n++)members.add(new ReplicaDescriptor(id(60+n),id(70+n),id(80+n),"donor"+n,"127.0.0.1:"+(7010+n),ReplicaDescriptor.Role.VOTER));
        var parent=new RangeDescriptor(id(6),group,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,members);var joined=new ArrayList<>(members);joined.add(receiver);
        var target=new RangeDescriptor(parent.rangeId(),group,1,1,null,null,RangeDescriptor.TopologyState.ACTIVE,joined);
        var source=new RangeDirectory(id(1),id(2),id(3),0,schema.digest(),0,Set.of(column),1,List.of(parent));
        var next=new RangeDirectory(id(1),id(2),id(3),0,schema.digest(),1,Set.of(column),1,List.of(target));
        definition=new TopologyChange.Definition(id(30),source,next);
        pool=DistributedStoragePool.open(root.resolve("pool"),ConfigParser.parseDefault(),LoggerFactory.getLogger(getClass()));
        store=pool.openRange(new RangeOwnership.Owner(group,1),new RangeOwnership.Interval(id(3),Set.of(column),null,null),schema.digest(),Map.of(column,"data"),Set.of());store.enableLogicalSchema(schema);
        var peer=RaftPeer.newBuilder().setId(donor.peerId()).setAddress(donor.address()).build();
        var peers=members.stream().map(r->RaftPeer.newBuilder().setId(r.peerId()).setAddress(r.address()).build()).toList();var builder=RaftConfigurationProto.newBuilder();peers.forEach(p->builder.addPeers(p.getRaftPeerProto()));var configuration=builder.build();if(bootstrap){store.initializeConfiguration(configuration.toByteArray());store.applyNoop(new ApplyPosition(1,0));}else store.applyNoop(new ApplyPosition(1,0),configuration.toByteArray());
        byte[] value=new byte[2*1024*1024-512];for(int n=0;n<value.length;n++)value[n]=(byte)n;
        store.apply(new ApplyPosition(1,1),new DistributedCommand(group,1,schema.digest(),new OperationId(id(31),1,0),List.of(new Mutation(Mutation.Kind.PUT,column,new byte[]{1},value))));
        when(division.getMemberId()).thenReturn(RaftGroupMemberId.valueOf(RaftPeerId.valueOf(donor.peerId()),RaftGroupId.valueOf(group)));when(division.getId()).thenReturn(RaftPeerId.valueOf(donor.peerId()));when(division.getInfo()).thenReturn(info);when(info.getCurrentTerm()).thenReturn(1L);
        var conf=mock(RaftConfiguration.class);when(conf.getLogEntryIndex()).thenReturn(0L);when(conf.getCurrentPeers()).thenReturn(peers);when(conf.getPreviousPeers()).thenReturn(List.of());when(conf.getCurrentPeers(RaftPeerRole.FOLLOWER)).thenReturn(peers);when(conf.getCurrentPeers(RaftPeerRole.LISTENER)).thenReturn(List.of());when(conf.getPreviousPeers(RaftPeerRole.FOLLOWER)).thenReturn(List.of());when(conf.getPreviousPeers(RaftPeerRole.LISTENER)).thenReturn(List.of());when(division.getRaftConf()).thenReturn(conf);
        var machine=mock(StateMachine.class);var storage=mock(StateMachineStorage.class);when(storage.getSnapshotDir()).thenReturn(image.getParent().toFile());when(machine.getStateMachineStorage()).thenReturn(storage);when(division.getStateMachine()).thenReturn(machine);
    }
    RatisTopologySourceSnapshotRpc.Request request(int n){return new RatisTopologySourceSnapshotRpc.Request(id(n),definition.header(),id(40),receiver,RatisTopologySourceSnapshotRpc.Purpose.BOOTSTRAP);}
    @Override public RatisTopologySourceSnapshotService.Lease capture(RatisTopologySourceSnapshotRpc.Request request)throws Exception {
        captures.incrementAndGet();var pin=store.capture();boolean adopted=false;
        try {
            var manifest=NativeRangeImage.write(image,pin,pin.interval(),new NativeRangeImage.Limits(4*1024*1024,8*1024*1024,16));
            var prepared=new RatisTopologySourceSnapshotRpc.Prepared(request,manifest,schema,donor,pin.configuration(),Map.of());
            var snapshot=new SingleFileSnapshotInfo(new FileInfo(image,MD5Hash.newInstance(MessageDigest.getInstance("MD5").digest(Files.readAllBytes(image)))),manifest.position().term(),manifest.position().index());
            var lease=new RatisTopologySourceSnapshotService.Lease(prepared,division,snapshot,()->{pin.close();pinCloses.incrementAndGet();});adopted=true;return lease;
        }finally{if(!adopted)pin.close();}
    }
    @Override public void requireOwner(RatisTopologySourceSnapshotService.Lease lease)throws Exception {if(!owner)throw new IOException("controlled owner loss");assertEquals(store.applied(),lease.prepared().image().position());assertEquals(store.configuration().term(),lease.prepared().sourceCfg().term());assertEquals(store.configuration().index(),lease.prepared().sourceCfg().index());assertArrayEquals(store.configuration().data(),lease.prepared().sourceCfg().data());}
    @Override public void requireReceiverInstalled(RatisTopologySourceSnapshotService.Lease lease)throws Exception {proofs.incrementAndGet();if(!installed || !Arrays.equals(MessageDigest.getInstance("SHA-256").digest(received.toByteArray()),lease.prepared().image().sha256()))throw new IOException("Receiver has not qualified the exact installed image");}
    @Override public boolean isReleased(RatisTopologySourceSnapshotRpc.Request request){releaseLookups.incrementAndGet();return false;}
    @Override public void released(RatisTopologySourceSnapshotService.Lease lease)throws Exception{durableReleases.incrementAndGet();if(failRelease)throw new IOException("controlled durable release failure");}
    @Override public InstallSnapshotReplyProto send(InstallSnapshotRequestProto request)throws IOException {
        int call=calls.incrementAndGet();if(entered!=null){entered.countDown();try{if(!unblock.await(5,TimeUnit.SECONDS))throw new IOException("controlled chunk wait expired");}catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new IOException(interrupted);}}
        if(failFirst && call==1)throw new IOException("controlled RPC failure");
        if(unknownReply)return InstallSnapshotReplyProto.getDefaultInstance();
        var chunk=request.getSnapshotChunk();if(!alreadyInstalled){if(chunk.getRequestIndex()==0)received.reset();for(var file:chunk.getFileChunksList())file.getData().writeTo(received);}
        var rpc=request.getServerRequest();return InstallSnapshotReplyProto.newBuilder().setServerReply(RaftRpcReplyProto.newBuilder().setRequestorId(rpc.getRequestorId()).setReplyId(rpc.getReplyId()).setRaftGroupId(rpc.getRaftGroupId()).setSuccess(true)).setTerm(request.getLeaderTerm()).setRequestIndex(chunk.getRequestIndex()).setResult(alreadyInstalled?InstallSnapshotResult.ALREADY_INSTALLED:InstallSnapshotResult.SUCCESS).build();
    }
    int imageDescriptors()throws IOException {var proc=Path.of("/proc/self/fd");if(!Files.isDirectory(proc))return 0;try(var paths=Files.list(proc)){int count=0;for(var path:paths.toList())try{if(Files.readSymbolicLink(path).equals(image))count++;}catch(IOException ignored){}return count;}}
    @Override public void close()throws Exception{pool.close();}
}
