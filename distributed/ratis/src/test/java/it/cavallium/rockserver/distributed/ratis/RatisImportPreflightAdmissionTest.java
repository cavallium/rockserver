package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.lang.reflect.*;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** The actual native qualifier is paused at its pool semaphore, independently of CONTROL proof work. */
class RatisImportPreflightAdmissionTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void blockedNativeSstQualifierKeepsOwnedChargeWhileUnrelatedNativeControlProgresses()throws Exception{
        var helper=new RatisRangeImportApplyTest();helper.root=root;var input=helper.input("preflight",false);var ordinary=new RatisProofQueryService(RatisProofFixtures.admission());var preflight=RatisProofQueryService.importPreflightLane(RatisProofFixtures.admission());boolean held=false;
        try(var pool=helper.pool(root.resolve("native"));var files=new RatisReferencedFiles(root.resolve("files"),helper.group,RatisReferencedFiles.Limits.defaults(),Runnable::run,request->RatisRangeImportRpc.requireFile(input.manifest(),request));var storage=RaftStorage.newBuilder().setDirectory(root.resolve("raft").toFile()).setOption(RaftStorage.StartupOption.FORMAT).setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.ONE_MB).setLogCorruptionPolicy(org.apache.ratis.server.RaftServerConfigKeys.Log.CorruptionPolicy.EXCEPTION).build()){
            storage.initialize();var store=pool.openNewFencedRange(input.manifest(),input.schema());var install=helper.publish(files,input);var owner=input.definition().sourceDirectory().ranges().getFirst().replicas().getFirst();var peer=RaftPeer.newBuilder().setId(owner.peerId()).setAddress(owner.address()).build();var group=RaftGroup.valueOf(RaftGroupId.valueOf(helper.group),peer);var machine=new RatisRangeStateMachine(store,group,new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,1),command->{},command->{},()->new RatisRangeStateMachine.Leadership(1,true));
            machine.rangeImportApply(new RatisRangeImportApply(pool,store,files,input.definition()),definition->{throw new AssertionError("INSTALL preflight must not invent publication authority");},owner);machine.referencedFiles(files);machine.proofQueries(ordinary);machine.importPreflightQueries(preflight);
            var server=(RaftServer)Proxy.newProxyInstance(RaftServer.class.getClassLoader(),new Class<?>[]{RaftServer.class},(p,m,a)->switch(m.getName()){case "getId"->peer.getId();case "toString"->"controlled-import-preflight";default->throw new AssertionError(m);});machine.initialize(server,group.getGroupId(),storage);machine.notifyConfigurationChanged(1,0,RaftConfigurationProto.newBuilder().addPeers(peer.getRaftPeerProto()).build());machine.notifyTermIndexUpdated(1,0);machine.notifyTermIndexUpdated(1,1);
            var otherGroup=new UUID(300,1);var otherColumn=new UUID(300,2);var control=pool.openRange(new RangeOwnership.Owner(otherGroup,1),new RangeOwnership.Interval(new UUID(300,3),Set.of(otherColumn),null,null),new byte[32],Map.of(otherColumn,"other-control"),Set.of());control.initializeConfiguration(RaftConfigurationProto.newBuilder().addPeers(peer.getRaftPeerProto()).build().toByteArray());control.applyNoop(new ApplyPosition(1,0));
            var field=DistributedStoragePool.class.getDeclaredField("nativeMutation");field.setAccessible(true);var nativeQualifier=(Semaphore)field.get(pool);nativeQualifier.acquire();held=true;var before=store.applied();
            var query=machine.queryLinearizable(helper.message(RatisRangeImportRpc.prepare(install)));
            try{long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);while(!nativeQualifier.hasQueuedThreads() && System.nanoTime()<deadline)Thread.sleep(1);assertTrue(nativeQualifier.hasQueuedThreads(),"Actual validateSstImport must reach the native qualifier boundary");assertEquals(1,preflight.stats().jobs());assertEquals(RatisProofQueryService.IMPORT_PREFLIGHT_BYTES,preflight.stats().bytes());assertEquals(0,ordinary.stats().bytes());
                ordinary.submit(otherGroup,new byte[]{3},128,false,owned->{control.applyNoop(new ApplyPosition(1,1));return Message.EMPTY;}).get(5,TimeUnit.SECONDS);assertEquals(new ApplyPosition(1,1),control.applied());assertEquals(before,store.applied());query.cancel(false);var drain=preflight.stop();assertFalse(drain.isDone());assertEquals(RatisProofQueryService.IMPORT_PREFLIGHT_BYTES,preflight.stats().bytes());nativeQualifier.release();held=false;drain.get(10,TimeUnit.SECONDS);assertEquals(0,preflight.stats().jobs());assertEquals(0,preflight.stats().bytes());assertEquals(before,store.applied(),"Preflight completes no native import effects");assertNull(store.importReceipt());
            }finally{if(held){nativeQualifier.release();held=false;}preflight.stop().get(10,TimeUnit.SECONDS);machine.close();}
        }finally{preflight.stop().get(10,TimeUnit.SECONDS);ordinary.stop().get(10,TimeUnit.SECONDS);}
    }
}
