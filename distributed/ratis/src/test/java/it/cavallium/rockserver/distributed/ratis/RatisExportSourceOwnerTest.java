package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeSstExport;
import java.io.IOException;
import java.nio.file.*;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Native source ownership through the actual stale-query dispatch; no listener or quorum. */
class RatisExportSourceOwnerTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;

    @Test void lostReplyRecoversOriginalNativeArtifactAfterPinReleaseAndLeadershipLoss() throws Exception {
        var helper=new RatisSnapshotPinsTest(); helper.root=root;
        var owner=new AtomicReference<RatisSnapshotPinsTest.Fixture>();
        var builds=new AtomicInteger(); var loseReply=new AtomicBoolean(true);
        var scheduler=RWScheduler.forTesting(1,1,1,4,1,"source-owner-fixture");
        var service=new RatisExportSourceService(root.resolve("exports"),RatisExportSourceRegistry.Budgets.defaults(),
                new RatisExportSourceRegistry.NativeBuilder() {
                    public NativeRangeSstExport.Chunk build(RatisExportSourceRpc.JobDefinition definition,
                            RatisExportJournal.ChunkIntent intent,Path artifact,RatisExportSourceRegistry.Completion completion) throws Exception {
                        var f=owner.get(); f.data.f.machine.requireExportPin(definition); builds.incrementAndGet();
                        return f.data.f.machine.exportSnapshotPinChunk(definition.capture(),pin -> {
                            var chunk=f.data.f.pool.exportSstChunk(artifact,pin,intent.column(),intent.scopeStart(),
                                    definition.scope(intent.column()).upperExclusive(),intent.limits());
                            completion.persist(chunk); return chunk;
                        });
                    }
                    public boolean tokenUsable(RatisExportSourceRpc.JobDefinition definition) throws Exception {
                        try { owner.get().data.f.machine.requireExportPin(definition); return true; }
                        catch (RatisSnapshotLostException lost) { return false; }
                    }
                },scheduler);
        helper.helper.helper.dataInitializer=machine -> machine.exportSources(service);
        try (var f=helper.new Fixture("native-source")) {
            owner.set(f); var machine=f.data.f.machine;
            var token=machine.captureSnapshotPin(f.home.homes,f.fence,f.owner,f.snapshot,helper.ttl); f.complete(token);
            var route=f.data.directory.ranges().getFirst(); var peer=route.replicas().getFirst();
            var limits=new RatisExportJournal.Limits(8,16L*1024*1024,8L*1024*1024,
                    new NativeRangeSstExport.Limits(1,1024,1024*1024,Duration.ofSeconds(5)));
            var definition=new RatisExportSourceRpc.JobDefinition(UUID.randomUUID(),f.fence,route.groupId(),List.of(f.data.f.column),token,limits);
            var request=new RatisExportSourceRpc.BuildRequest(definition,
                    new RatisExportJournal.ChunkIntent(route.groupId(),f.data.f.column,token.position(),new byte[]{1},0,limits.chunk()));
            try (var data=new RatisRoutedClient(f.data.directory,new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor r,UUID c,byte[] k) { throw new AssertionError("No fresh reads"); }
                public CommandResult command(RangeDescriptor r,DistributedCommand c) { throw new AssertionError("No mutations"); }
                public byte[] snapshot(RangeDescriptor r,ReplicaDescriptor replica,byte[] bytes) throws IOException {
                    assertEquals(route,r); assertEquals(peer,replica);
                    try {
                        byte[] reply=machine.queryStale(Message.valueOf(ByteString.copyFrom(bytes)),0).get(5,TimeUnit.SECONDS).getContent().toByteArray();
                        if (ScopedTransactionRpc.kind(bytes,ScopedTransactionRpc.MAX_REQUEST_BYTES)==ScopedTransactionRpc.EXPORT_BUILD && loseReply.getAndSet(false))
                            throw new IOException("Lost completed BUILD reply");
                        return reply;
                    } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException(interrupted); }
                    catch (ExecutionException | TimeoutException failure) { throw new IOException(failure); }
                }
            })) {
                var target=data.restoreSnapshotTarget(f.fence,route,peer,token.ownerIncarnation());
                assertThrows(IOException.class,() -> data.exportBuild(target,request)); assertEquals(1,builds.get());
                machine.releaseSnapshotPin(token); machine.notifyNotLeader(List.of());
                var descriptor=data.exportBuild(target,request); assertEquals(1,builds.get()); assertTrue(descriptor.hasMore());
                var bytes=data.exportRead(target,new RatisExportSourceRpc.ReadRequest(definition,0,descriptor.digest(),0,1024*1024));
                assertEquals(descriptor.fileBytes(),bytes.length);
                assertArrayEquals(descriptor.contentHash(),MessageDigest.getInstance("SHA-256").digest(bytes));
                var next=new RatisExportSourceRpc.BuildRequest(definition,new RatisExportJournal.ChunkIntent(
                        route.groupId(),f.data.f.column,token.position(),descriptor.upperExclusive(),1,limits.chunk()));
                assertThrows(RatisSnapshotLostException.class,() -> data.exportBuild(target,next));
                var framed=ScopedTransactionRpc.frame(ScopedTransactionRpc.EXPORT_BUILD,request.encode());
                assertThrows(ExecutionException.class,() -> machine.queryStale(Message.valueOf(ByteString.copyFrom(framed)),1).get(5,TimeUnit.SECONDS));
                data.releaseExport(target,new RatisExportSourceRpc.ReleaseRequest(definition));
                assertEquals(0,machine.snapshotPinStats().pins());
            }
        } finally { service.stop().get(10,TimeUnit.SECONDS); scheduler.disposeGracefully().block(Duration.ofSeconds(5)); }
    }
}
