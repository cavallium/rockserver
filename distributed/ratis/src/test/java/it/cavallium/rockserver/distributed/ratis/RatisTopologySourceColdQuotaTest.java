package it.cavallium.rockserver.distributed.ratis;

import java.nio.ByteBuffer;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.*;
import java.lang.reflect.Proxy;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Actual Runtime Host/native capture; divisions, catalogue authority and receiver IMAGE replies are controlled. */
class RatisTopologySourceColdQuotaTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static Set<String> files(Path directory)throws IOException {try(var files=Files.list(directory)){return new TreeSet<>(files.map(path->path.getFileName().toString()).toList());}}
    final class Fixture implements AutoCloseable {
        final DistributedSchema schema; final RatisNodeRuntimeTest.Env env; final RangeDescriptor parent;
        final ReplicaDescriptor receiver; final TopologyChange.Definition definition; final TopologyChange.State state;
        final RatisTopologySourceSnapshotRpc.Request request,fresh; final RatisNodeRuntime.Settings settings;
        final ByteArrayOutputStream received=new ByteArrayOutputStream();
        Fixture()throws Exception {
            var shape=RatisTopologySeedRuntimeTest.schema();schema=new DistributedSchema(shape.clusterId(),shape.databaseId(),shape.domainId(),shape.schemaEpoch(),0,shape.columns());
            env=new RatisNodeRuntimeTest.Env(root,false,schema.digest());var source=env.plan.domains().getFirst().directory();parent=source.ranges().getFirst();
            receiver=new ReplicaDescriptor(new UUID(160,1),new UUID(160,2),new UUID(160,3),"cold-quota-learner","127.0.0.1:9561",ReplicaDescriptor.Role.LISTENER);
            var owners=new ArrayList<>(parent.replicas());owners.add(receiver);
            var target=new RangeDescriptor(parent.rangeId(),parent.groupId(),parent.rangeEpoch(),parent.configurationVersion()+1,parent.lowerInclusive(),parent.upperExclusive(),parent.topologyState(),owners);
            var ranges=new ArrayList<>(source.ranges());ranges.set(0,target);
            var after=new RangeDirectory(source.clusterId(),source.databaseId(),source.domainId(),source.schemaEpoch(),source.schemaDigest(),source.generation()+1,source.columns(),source.routingPrefixBytes(),ranges);
            definition=new TopologyChange.Definition(new UUID(160,4),source,after);state=TopologyChange.State.begin(definition.header()).beginJoining();
            request=new RatisTopologySourceSnapshotRpc.Request(new UUID(160,5),definition.header(),env.plan.catalogue().groupId(),receiver,RatisTopologySourceSnapshotRpc.Purpose.BOOTSTRAP);
            fresh=new RatisTopologySourceSnapshotRpc.Request(new UUID(160,6),definition.header(),request.catalogue(),receiver,request.purpose());
            var defaults=env.settings();var resources=defaults.resources();
            // Each declared pool receives exactly one worst-case pair of managed+artifact images.
            var constrained=new RatisNodeRuntime.ResourceLimits(resources.node(),resources.pool(),Math.multiplyExact(2L*defaults.images().maxImageBytes(),env.plan.pools().size()),resources.maximumDurableMetadataBytes(),resources.maximumOperatorCacheBytes(),resources.maximumNativeCacheBytes(),RatisCommittedApplyDispatcher.Policy.defaults(),RatisDiskRunway.Policy.defaults(),RatisMigrationController.Policy.defaults());
            settings=new RatisNodeRuntime.Settings(defaults.images(),defaults.retention(),defaults.clockPeriod(),defaults.propagationVisits(),defaults.maxPropagationRanges(),defaults.shutdownTimeout(),defaults.exports(),constrained);
        }
        Owned open(boolean recover)throws Exception {
            var fake=new RatisNodeRuntimeTest.FakeServer();var rpc=mock(RaftServerRpc.class);
            when(rpc.installSnapshot(any())).thenAnswer(call->{
                InstallSnapshotRequestProto input=call.getArgument(0);var chunk=input.getSnapshotChunk();if(chunk.getRequestIndex()==0)received.reset();for(var file:chunk.getFileChunksList())file.getData().writeTo(received);
                var request=input.getServerRequest();return InstallSnapshotReplyProto.newBuilder().setServerReply(RaftRpcReplyProto.newBuilder().setRequestorId(request.getRequestorId()).setReplyId(request.getReplyId()).setRaftGroupId(request.getRaftGroupId()).setSuccess(true)).setTerm(input.getLeaderTerm()).setRequestIndex(chunk.getRequestIndex()).setResult(InstallSnapshotResult.SUCCESS).build();
            });
            var node=RatisNodeRuntime.open(env.plan,env.nodeRoot,ConfigParser.parseDefault(),env.catalogue.epochs,Map.of(env.routed.directory().domainId(),env.routed),env.scheduler,env.timer,()->20L,settings,recover?RatisNodeRuntime.Startup.RECOVER:RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,
                    (plan,logs,machines)->{try {
                        var base=RatisTopologySourceHostTest.controlled(fake,plan,logs,machines,new java.util.concurrent.atomic.AtomicBoolean());
                        var server=(RaftServer)Proxy.newProxyInstance(RaftServer.class.getClassLoader(),new Class<?>[]{RaftServer.class},(p,m,a)->m.getName().equals("getServerRpc")?rpc:RatisTopologySourceHostTest.invoke(base,m,a));fake.server=server;return server;
                    }catch(Exception failure){throw new IOException(failure);}},List.of(schema));
            boolean adopted=false;
            try {
                var authority=(RatisNodeRuntime.JoinAuthorityReader)input->{var view=new RatisTopologyJoinAuthority.View(request.catalogue(),new ApplyPosition(1,8),definition,state);view.require(input);return view;};
                RatisTopologySeedRuntimeTest.field(node,"joinAuthority",authority);
                for(var machine:fake.machines.values())RatisTopologySeedRuntimeTest.field(machine,"relocationAuthority",(RatisRangeStateMachine.RelocationAuthorityReader)authority::read);
                node.start();var machine=fake.machines.get(RaftGroupId.valueOf(parent.groupId()));var store=RatisTopologySeedRuntimeTest.field(machine,"store",ReplicatedRangeStore.class);
                var old=RatisTopologySeedRuntimeTest.field(node,"relocationClients",RatisRelocationDataClient.class);old.close();
                var clients=new RatisRelocationDataClient(schema.clusterId(),schema.databaseId(),request.catalogue(),range->{throw new IOException("No leader RPC permitted");},(range,owner)->ownerClient(owner,store));
                RatisTopologySeedRuntimeTest.field(node,"relocationClients",clients);
                var pools=RatisTopologySeedRuntimeTest.field(node,"pools",Map.class);var pool=pools.get(env.plan.poolId(parent.groupId()));
                var artifacts=RatisTopologySeedRuntimeTest.field(pool,"artifacts",RatisTopologySourceSnapshotArtifacts.class);
                adopted=true;return new Owned(node,machine,store,artifacts);
            }finally{if(!adopted)node.close();}
        }
        RaftClient ownerClient(ReplicaDescriptor owner,ReplicatedRangeStore store) {
            assertEquals(receiver,owner);
            var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(p,m,a)->{
                assertEquals("sendStaleRead",m.getName());var query=RatisRelocationOwnerRpc.decode(((Message)a[0]).getContent().toByteArray());assertEquals(RatisRelocationOwnerRpc.Expected.IMAGE,query.expected());assertEquals(receiver.replicaId(),query.replica());
                assertArrayEquals(query.imageHash(),OperatorUpload.sha256(received.toByteArray()),"Configured receiver proof only accepts bytes actually streamed by the source");
                var evidence=new RatisRelocationOwnerRpc.Evidence(query.digest(),TopologyChange.Owner.of(receiver),query.requiredCut(),store.configuration(),new byte[32],true,false);
                return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(receiver.peerId())).setGroupId(RaftGroupId.valueOf(parent.groupId())).setCallId(1).setSuccess(true).setMessage(RatisTopologySourceHostTest.message(RatisRelocationOwnerRpc.reply(evidence))).build();
            });
            return (RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(p,m,a)->switch(m.getName()){case "getGroupId"->RaftGroupId.valueOf(parent.groupId());case "io"->io;case "close"->null;default->throw new AssertionError(m);});
        }
        Path artifactRoot(){return env.nodeRoot.resolve("topology-source-artifacts").resolve(env.plan.poolId(parent.groupId()).toString());}
        Set<Path> imageFiles()throws IOException {
            var images=new TreeSet<Path>();
            try(var paths=Files.walk(artifactRoot(),2)) {for(var file:paths.toList()) {
                if(!Files.isRegularFile(file,LinkOption.NOFOLLOW_LINKS))continue;
                try {NativeRangeImage.inspect(file,settings.images());images.add(file.toAbsolutePath().normalize());}catch(IOException metadata) { /* Bounded intent/receipt/lock files are not native images. */ }
            }}
            return images;
        }
        Path leasedImage(Owned owner)throws Exception {
            var service=RatisTopologySeedRuntimeTest.field(owner.machine,"sourceSnapshots",RatisTopologySourceSnapshotService.class);
            var entries=RatisTopologySeedRuntimeTest.field(service,"entries",Map.class);
            var lease=RatisTopologySeedRuntimeTest.field(entries.get(request.transferId()),"lease",RatisTopologySourceSnapshotService.Lease.class);
            return lease.snapshot().getFiles().getFirst().getPath().toAbsolutePath().normalize();
        }
        void authorize(Owned owner)throws Exception {
            byte[] source=owner.store.configuration().data(),target=RatisRelocationDataClient.targetConfiguration(definition.targets().getFirst(),source);
            var record=new RatisTopologyRelocationRecord(RatisTopologyRelocationRecord.Action.AUTHORIZE,definition,request.catalogue(),new ApplyPosition(1,8),source,target);
            RatisTopologySourceHostTest.appendCompletion(owner.machine,owner.store,record,env.plan.peerId());
        }
        public void close()throws Exception {env.close();}
    }
    record Owned(RatisNodeRuntime node,RatisRangeStateMachine machine,ReplicatedRangeStore store,RatisTopologySourceSnapshotArtifacts artifacts) implements AutoCloseable {
        byte[] action(int kind,RatisTopologySourceSnapshotRpc.Request request)throws Exception {return machine.queryLinearizable(RatisTopologySourceHostTest.message(RatisTopologySourceSnapshotRpc.request(kind,request))).get(5,TimeUnit.SECONDS).getContent().toByteArray();}
        public void close()throws Exception {node.close();}
    }
    @Test void coldOriginalClaimsDiskBeforeNewCaptureAndStillReplaysExactPreparedImage()throws Exception {
        try(var f=new Fixture()) {
            byte[] original;Set<Path> originalImages;
            try(var owner=f.open(false)) {
                owner.machine.notifyTermIndexUpdated(1,owner.store.applied().index()+1);var raftFiles=files(owner.machine.sourceSnapshotDirectory());
                original=owner.action(RatisTopologySourceSnapshotRpc.PREPARE,f.request);assertEquals(1,owner.store.activeSnapshotPins());
                originalImages=f.imageFiles();assertEquals(2,originalImages.size(),"Original and retained copies must both be artifact-owned");
                assertTrue(f.leasedImage(owner).startsWith(f.artifactRoot()));assertEquals(raftFiles,files(owner.machine.sourceSnapshotDirectory()),"Source capture must not create Raft snapshot/recovery files");
                owner.close();assertEquals(0,owner.store.activeSnapshotPins());assertEquals(originalImages,f.imageFiles());
            }
            try(var owner=f.open(true)) {
                owner.machine.notifyTermIndexUpdated(1,owner.store.applied().index()+1);var managed=files(owner.machine.sourceSnapshotDirectory());var cut=owner.store.applied();assertEquals(0,owner.store.activeSnapshotPins());
                assertThrows(java.util.concurrent.ExecutionException.class,()->owner.action(RatisTopologySourceSnapshotRpc.PREPARE,f.fresh));
                assertEquals(managed,files(owner.machine.sourceSnapshotDirectory()),"Cold quota rejection precedes writing a new native image");assertEquals(originalImages,f.imageFiles());assertEquals(cut,owner.store.applied());assertEquals(0,owner.store.activeSnapshotPins());
                Path artifacts=f.env.nodeRoot.resolve("topology-source-artifacts").resolve(f.env.plan.poolId(f.parent.groupId()).toString());assertTrue(files(artifacts).stream().noneMatch(name->name.startsWith(f.fresh.transferId().toString())));
                assertArrayEquals(original,owner.action(RatisTopologySourceSnapshotRpc.PREPARE,f.request));assertEquals(originalImages,f.imageFiles());assertEquals(1,owner.store.activeSnapshotPins());
            }
        }
    }
    @Test void actualTransferAndOwnerQualifiedReleaseDrainPinAndRestoreSourceAdmission()throws Exception {
        try(var f=new Fixture();var owner=f.open(false)) {
            f.authorize(owner);var raftFiles=files(owner.machine.sourceSnapshotDirectory());var original=owner.action(RatisTopologySourceSnapshotRpc.PREPARE,f.request);long charge=owner.artifacts.reservedImageBytes();assertTrue(charge>0);
            var originalImages=f.imageFiles();assertEquals(2,originalImages.size());assertTrue(f.leasedImage(owner).startsWith(f.artifactRoot()));assertEquals(raftFiles,files(owner.machine.sourceSnapshotDirectory()));
            assertThrows(java.util.concurrent.ExecutionException.class,()->owner.action(RatisTopologySourceSnapshotRpc.RELEASE,f.request),"No transfer/receiver IMAGE proof means no release authority");
            assertEquals(1,owner.store.activeSnapshotPins());assertEquals(charge,owner.artifacts.reservedImageBytes());
            var prepared=RatisTopologySourceSnapshotRpc.reply(original,RatisTopologySourceSnapshotRpc.PREPARE,f.request);
            var transferred=RatisTopologySourceSnapshotRpc.reply(owner.action(RatisTopologySourceSnapshotRpc.TRANSFER,f.request),RatisTopologySourceSnapshotRpc.TRANSFER,f.request);
            assertArrayEquals(original,RatisTopologySourceSnapshotRpc.reply(RatisTopologySourceSnapshotRpc.PREPARE,transferred));assertArrayEquals(prepared.image().sha256(),OperatorUpload.sha256(f.received.toByteArray()));
            owner.action(RatisTopologySourceSnapshotRpc.RELEASE,f.request);assertEquals(0,owner.store.activeSnapshotPins());assertEquals(0,owner.artifacts.reservedImageBytes());
            assertTrue(f.imageFiles().isEmpty());for(var image:originalImages)assertFalse(Files.exists(image),"Release must physically reclaim both source-owned copies");assertEquals(raftFiles,files(owner.machine.sourceSnapshotDirectory()));
            owner.action(RatisTopologySourceSnapshotRpc.RELEASE,f.request);assertEquals(0,owner.artifacts.reservedImageBytes());
            assertThrows(java.util.concurrent.ExecutionException.class,()->owner.action(RatisTopologySourceSnapshotRpc.PREPARE,f.request));
            assertNotNull(owner.action(RatisTopologySourceSnapshotRpc.PREPARE,f.fresh));assertEquals(2,f.imageFiles().size());assertEquals(1,owner.store.activeSnapshotPins());
        }
    }
    @Test void failedDeletionKeepsOriginalReservationUntilActualReleaseCleanupRetries()throws Exception {releaseFailure(false);}
    @Test void unknownParentForceKeepsOriginalReservationAcrossColdReleaseRetry()throws Exception {releaseFailure(true);}
    void releaseFailure(boolean force)throws Exception {
        try(var f=new Fixture()) {
            long charge;
            try(var owner=f.open(false)) {
                f.authorize(owner);owner.action(RatisTopologySourceSnapshotRpc.PREPARE,f.request);owner.action(RatisTopologySourceSnapshotRpc.TRANSFER,f.request);charge=owner.artifacts.reservedImageBytes();assertEquals(2,f.imageFiles().size());
                owner.artifacts.removalObserver(new SessionJournalFiles.Removal(){public void delete(Path file)throws IOException {if(!force)throw new IOException("Controlled source cleanup delete failure");Files.delete(file);}public void force(Path directory)throws IOException {throw new IOException("Controlled unknown source cleanup parent force");}});
                assertThrows(java.util.concurrent.ExecutionException.class,()->owner.action(RatisTopologySourceSnapshotRpc.RELEASE,f.request));assertEquals(charge,owner.artifacts.reservedImageBytes());assertEquals(1,owner.store.activeSnapshotPins());
                assertEquals(force ? 0 : 2,f.imageFiles().size(),"Failed force retains reservation even after both physical deletions; failed deletion retains originals");
                assertThrows(java.util.concurrent.ExecutionException.class,()->owner.action(RatisTopologySourceSnapshotRpc.PREPARE,f.fresh));assertEquals(charge,owner.artifacts.reservedImageBytes());
            }
            try(var owner=f.open(true)) {
                assertEquals(charge,owner.artifacts.reservedImageBytes());
                var changed=new RatisTopologySourceSnapshotRpc.Request(f.request.transferId(),f.request.header(),new UUID(999,1),f.request.receiver(),f.request.purpose());
                assertThrows(java.util.concurrent.ExecutionException.class,()->owner.action(RatisTopologySourceSnapshotRpc.RELEASE,changed));assertEquals(charge,owner.artifacts.reservedImageBytes());
                owner.action(RatisTopologySourceSnapshotRpc.RELEASE,f.request);assertEquals(0,owner.artifacts.reservedImageBytes());assertEquals(0,owner.store.activeSnapshotPins());assertTrue(f.imageFiles().isEmpty());
                assertNotNull(owner.action(RatisTopologySourceSnapshotRpc.PREPARE,f.fresh));
            }
        }
    }
    @Test void incompleteForcedCaptureAcknowledgesOriginalAbandonmentAndOnlySuccessorMayPublishLaterCut()throws Exception {
        try(var f=new Fixture()) {
            var intent=new AtomicReference<Path>();long charge;ApplyPosition originalCut;byte[] originalConfiguration,abandonedReply,successorReply;
            RatisTopologySourceSnapshotRpc.Request successor;
            try(var owner=f.open(false)) {
                owner.machine.notifyTermIndexUpdated(1,owner.store.applied().index()+1);originalCut=owner.store.applied();var configuration=owner.store.configuration();originalConfiguration=ByteBuffer.allocate(16+configuration.data().length).putLong(configuration.term()).putLong(configuration.index()).put(configuration.data()).array();var managed=files(owner.machine.sourceSnapshotDirectory());
                owner.artifacts.captureForceObserver(path->{intent.set(path);throw new IOException("Controlled failure after original-cut force before image write");});
                assertThrows(java.util.concurrent.ExecutionException.class,()->owner.action(RatisTopologySourceSnapshotRpc.PREPARE,f.request));assertEquals(0,owner.store.activeSnapshotPins());assertEquals(managed,files(owner.machine.sourceSnapshotDirectory()));assertTrue(f.imageFiles().isEmpty());
                assertTrue(Files.isRegularFile(intent.get()));charge=owner.artifacts.reservedImageBytes();assertTrue(charge>0);owner.machine.notifyTermIndexUpdated(1,originalCut.index()+1);
            }
            try(var owner=f.open(true);var unrelated=owner.store.captureForRecovery()) {
                assertTrue(owner.store.applied().index()>originalCut.index());assertEquals(charge,owner.artifacts.reservedImageBytes());var managed=files(owner.machine.sourceSnapshotDirectory());
                abandonedReply=owner.action(RatisTopologySourceSnapshotRpc.PREPARE,f.request);
                var abandoned=assertThrows(RatisTopologySourceSnapshotRpc.AbandonedException.class,()->RatisTopologySourceSnapshotRpc.reply(abandonedReply,RatisTopologySourceSnapshotRpc.PREPARE,f.request)).receipt();
                assertEquals(originalCut,abandoned.cut());assertArrayEquals(OperatorUpload.sha256(originalConfiguration),abandoned.configurationDigest());
                assertEquals(f.parent.replicas().stream().filter(replica->replica.nodeId().equals(f.env.plan.nodeId()) && replica.nodeIncarnation().equals(f.env.plan.nodeIncarnation())).findFirst().orElseThrow(),abandoned.sourceOwner());
                assertArrayEquals(RatisTopologySourceSnapshotRpc.request(RatisTopologySourceSnapshotRpc.PREPARE,f.request),RatisTopologySourceSnapshotRpc.request(RatisTopologySourceSnapshotRpc.PREPARE,abandoned.original()));
                successor=abandoned.successorRequest();assertNotEquals(f.request.transferId(),successor.transferId());
                assertEquals(managed,files(owner.machine.sourceSnapshotDirectory()));assertEquals(1,owner.store.activeSnapshotPins(),"Unrelated native pin remains held through A abandonment");assertTrue(f.imageFiles().isEmpty());
                assertFalse(Files.exists(intent.get()),"Original capture intent is reclaimed only after forced abandonment cleanup");assertEquals(0,owner.artifacts.reservedImageBytes());
                assertArrayEquals(abandonedReply,owner.action(RatisTopologySourceSnapshotRpc.PREPARE,f.request),"A repeats its terminal receipt, never a later READY image");
                successorReply=owner.action(RatisTopologySourceSnapshotRpc.PREPARE,successor);
                var ready=RatisTopologySourceSnapshotRpc.reply(successorReply,RatisTopologySourceSnapshotRpc.PREPARE,successor);
                assertEquals(successor.transferId(),ready.request().transferId());assertTrue(ready.image().position().index()>originalCut.index());assertEquals(2,f.imageFiles().size());assertEquals(2,owner.store.activeSnapshotPins(),"B owns its pin independently of the unrelated reader");assertTrue(owner.artifacts.reservedImageBytes()>0);
                assertArrayEquals(abandonedReply,owner.action(RatisTopologySourceSnapshotRpc.PREPARE,f.request));assertEquals(2,f.imageFiles().size());
                assertTrue(f.imageFiles().stream().noneMatch(path->path.toString().contains(f.request.transferId().toString())),"Neither old A image may be substituted by B");
            }
            try(var owner=f.open(true)) {
                owner.machine.notifyTermIndexUpdated(1,owner.store.applied().index()+1);
                assertArrayEquals(abandonedReply,owner.action(RatisTopologySourceSnapshotRpc.PREPARE,f.request));
                assertArrayEquals(successorReply,owner.action(RatisTopologySourceSnapshotRpc.PREPARE,successor),"B retains its own fixed READY cut across a later cold prefix");
                assertEquals(2,f.imageFiles().size());assertEquals(1,owner.store.activeSnapshotPins());
            }
        }
    }
}
