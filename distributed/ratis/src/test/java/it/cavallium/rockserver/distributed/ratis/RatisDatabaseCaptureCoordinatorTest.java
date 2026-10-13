package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Capture-only barrier over actual native cohorts and controlled HOME journals; no archive/SST writing. */
class RatisDatabaseCaptureCoordinatorTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final class Prefix implements AutoCloseable {
        final RatisDatabaseBackupRealCohortFixture fixture;final RatisDatabaseBackupJournal log;
        final RatisDatabaseBackupDriver.CohortTransport transport;final RatisSnapshotCohortService combined;
        final List<RatisRoutedClient> routes=new ArrayList<>();final RatisDatabaseCaptureCoordinator coordinator;
        Prefix(String name,boolean shared)throws Exception {
            fixture=new RatisDatabaseBackupRealCohortFixture(root.resolve(name),1,1,shared);log=fixture.base.journal(false,false);
            var bindings=new HashMap<UUID,RatisDatabaseCaptureCoordinator.CaptureBinding>();
            if(!shared){combined=null;transport=fixture.cohorts;fixture.bindings.forEach((id,b)->bindings.put(id,new RatisDatabaseCaptureCoordinator.CaptureBinding(b.journal(),b.coordinator(),b.routed())));}
            else {
                var first=fixture.endpoints.getFirst();assertSame(first.state.nativePool,fixture.endpoints.getLast().state.nativePool);assertEquals(first.state.poolId,fixture.endpoints.getLast().state.poolId);
                var byGroup=new HashMap<UUID,RatisDatabaseBackupRealCohortFixture.Endpoint>();fixture.endpoints.forEach(endpoint->endpoint.state.stores.keySet().forEach(group->byGroup.put(group,endpoint)));
                var host=new RatisSnapshotCohortService.Host(){
                    public UUID nodeId(){return first.nodeId();}public UUID nodeIncarnation(){return first.nodeIncarnation();}public UUID processIncarnation(){return first.processIncarnation();}public UUID poolId(){return first.poolId();}public DistributedStoragePool pool(){return first.pool();}public PoolSnapshotAdmission.Limits poolLimits(){return first.poolLimits();}
                    public RatisSnapshotCohortService.Member member(UUID group,SnapshotFence fence,boolean ready)throws Exception{var endpoint=byGroup.get(group);if(endpoint==null)throw new IOException("Unknown native group");return endpoint.member(group,fence,ready);}
                    public boolean originalGone(RatisSnapshotCohortRpc.Reference reference){return false;}
                };
                var graph=new RatisNodeResourceBudget(RatisNodeRuntime.ResourceLimits.defaults().node(),Map.of(first.poolId(),RatisNodeRuntime.ResourceLimits.defaults().pool()));combined=new RatisSnapshotCohortService(host,first.scheduler,RatisSnapshotCohortService.Limits.defaults(),System::nanoTime,graph);
                var groups=new HashMap<UUID,RatisRoutedClient>();
                for(var endpoint:fixture.endpoints){
                    var routed=new RatisRoutedClient(endpoint.state.directory,new RatisRoutedClient.Transport(){
                        public byte[] read(RangeDescriptor route,UUID column,byte[] key){throw new AssertionError("No ordinary reads in capture prefix");}public CommandResult command(RangeDescriptor route,DistributedCommand command){throw new AssertionError("No ordinary writes in capture prefix");}
                        public byte[] transaction(RangeDescriptor route,byte[] request,boolean fresh)throws IOException{try{var frame=ScopedTransactionRpc.parse(request,ScopedTransactionRpc.MAX_REQUEST_BYTES);assertEquals(ScopedTransactionRpc.SNAPSHOT_OWNER,frame.kind());return ScopedTransactionRpc.frame(frame.kind(),combined.owner(route.groupId(),null).encode());}catch(Exception failure){throw new IOException(failure);}}
                        public CompletableFuture<byte[]> snapshot(RangeDescriptor route,ReplicaDescriptor owner,byte[] request)throws IOException{try{var frame=ScopedTransactionRpc.parse(request,RatisSnapshotCohortRpc.MAX_REQUEST_BYTES);if(owner==null){assertEquals(ScopedTransactionRpc.SNAPSHOT_OWNER,frame.kind());return CompletableFuture.completedFuture(ScopedTransactionRpc.frame(frame.kind(),combined.owner(route.groupId(),null).encode()));}if(frame.kind()==RatisSnapshotCohortRpc.CAPTURE){assertTrue(fixture.base.pools.stream().allMatch(pool->pool.gateChecks>0),"Every HOME gate must be checked before any native capture");assertTrue(log.snapshot().phase()==RatisDatabaseBackupJournal.Phase.REQUESTS || log.snapshot().phase()==RatisDatabaseBackupJournal.Phase.CAPTURED);assertEquals(0,fixture.base.reports.get());}return combined.query(route.groupId(),frame.kind(),frame.body()).thenApply(response->{if(frame.kind()==RatisSnapshotCohortRpc.CAPTURE && fixture.loseReply){fixture.loseReply=false;throw new CompletionException(new IOException("Lost actual shared-pool capture response"));}return ScopedTransactionRpc.frame(frame.kind(),response);});}catch(IOException failure){throw failure;}catch(Exception failure){throw new IOException(failure);}}

                    });routes.add(routed);for(var group:endpoint.state.stores.keySet())groups.put(group,routed);bindings.put(endpoint.state.fence.fenceId(),new RatisDatabaseCaptureCoordinator.CaptureBinding(endpoint.state.durable,endpoint.state.coordinator,routed));
                }
                transport=new RatisSnapshotCohortClient(groups,fixture.frozen::get);
            }
            coordinator=new RatisDatabaseCaptureCoordinator(log,()->new RatisDatabaseBackupDriver.Vector(fixture.base.definition.roster(),fixture.base.definition.domains()),transport,bindings);
        }
        Map<UUID,SnapshotFenceStatus.Capture> tokens()throws IOException{var tokens=new HashMap<UUID,SnapshotFenceStatus.Capture>();log.snapshot().captured().forEach(status->status.progress().forEach(p->tokens.put(p.groupId(),p.capture())));return Map.copyOf(tokens);}
        int physicalPins(){var pools=Collections.newSetFromMap(new IdentityHashMap<DistributedStoragePool,Boolean>());fixture.base.pools.forEach(state->pools.add(state.nativePool));return pools.stream().mapToInt(pool->pool.snapshotAdmissionStats().pins()).sum();}
        void release()throws Exception{for(var request:log.snapshot().requests())transport.cancelAndDrain(request).get(20,TimeUnit.SECONDS);}
        public void close()throws Exception{if(combined!=null)combined.close();for(var route:routes)route.close();log.close();fixture.close();}
    }
    void captureAndRead(String name,boolean shared)throws Exception {
        try(var prefix=new Prefix(name,shared)){
            assertEquals(0,prefix.physicalPins());var progress=prefix.coordinator.step(RequestContext.batch());assertEquals(RatisDatabaseBackupJournal.Phase.REQUESTS,progress.phase());assertEquals(shared?1:2,prefix.log.snapshot().requests().size());assertTrue(prefix.fixture.base.pools.stream().allMatch(pool->pool.gateChecks>0));assertEquals(0,prefix.physicalPins());
            prefix.fixture.loseReply=true;assertThrows(IOException.class,()->prefix.coordinator.step(RequestContext.batch()));assertEquals(RatisDatabaseBackupJournal.Phase.REQUESTS,prefix.log.snapshot().phase());assertEquals(1,prefix.physicalPins());assertEquals(0,prefix.fixture.base.reports.get());assertFalse(prefix.fixture.base.vectorForced.get());
            progress=prefix.coordinator.step(RequestContext.batch());assertEquals(RatisDatabaseBackupJournal.Phase.CAPTURED,progress.phase());assertEquals(shared?1:2,prefix.physicalPins());var tokens=prefix.tokens();assertEquals(2,tokens.size());assertTrue(prefix.fixture.base.vectorForced.get());assertEquals(0,prefix.fixture.base.reports.get());
            for(var home:prefix.log.snapshot().captured()){assertEquals(SnapshotFenceStatus.Phase.INSTALLING,home.phase());assertTrue(home.allReady());assertTrue(home.progress().stream().allMatch(p->p.capture()!=null));}
            for(var request:prefix.log.snapshot().requests()){var prepared=new HashMap<UUID,RatisRoutedClient.PreparedSnapshotCapture>();request.members().forEach(member->prepared.put(member.groupId(),prefix.fixture.frozen.get(member.groupId())));var same=prefix.transport.capture(request,prepared).get(20,TimeUnit.SECONDS);same.forEach((group,result)->assertEquals(tokens.get(group),result.token()));}
            assertEquals(shared?1:2,prefix.physicalPins());for(int n=0;n<16 && !progress.released();n++)progress=prefix.coordinator.step(RequestContext.batch());assertTrue(progress.released());assertEquals(tokens,prefix.tokens());assertTrue(prefix.fixture.base.reports.get()>0);
            for(var endpoint:prefix.fixture.endpoints){var state=endpoint.state;for(var group:state.stores.keySet()){
                var store=state.stores.get(group);assertFalse(store.snapshotGate(state.fence.fenceId()).active());var original=endpoint.registries.get(group).get(tokens.get(group),state.column,RatisDatabaseBackupDriverFixture.key(0));assertArrayEquals(new byte[]{0},original);
                var command=new DistributedCommand(group,1,state.schema.digest(),new OperationId(UUID.randomUUID(),1,0),List.of(new Mutation(Mutation.Kind.PUT,state.column,RatisDatabaseBackupDriverFixture.key(0),new byte[]{77})));assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(store.apply(new ApplyPosition(1,store.applied().index()+1),command)).status());assertArrayEquals(new byte[]{77},store.readCurrentPoint(state.column,RatisDatabaseBackupDriverFixture.key(0)));assertArrayEquals(original,endpoint.registries.get(group).get(tokens.get(group),state.column,RatisDatabaseBackupDriverFixture.key(0)));
            }}
            assertEquals(shared?1:2,prefix.physicalPins());assertEquals(progress,prefix.coordinator.step(RequestContext.batch()));assertEquals(tokens,prefix.tokens());assertFalse(Files.exists(prefix.fixture.base.root.resolve("archive")));assertFalse(Files.exists(prefix.fixture.base.root.resolve("export-0")));
            prefix.release();assertEquals(0,prefix.physicalPins());
        }
    }
    @Test void twoDomainsTwoActualPoolsForceWholeVectorBeforeHomeReportsAndKeepOriginalReads()throws Exception{captureAndRead("two-pool",false);}
    @Test void twoDomainsOneActualPoolShareOneNativeLeaseAndRetainOriginalReadsAfterGateRelease()throws Exception{captureAndRead("shared-pool",true);}
    @Test void explicitOriginalPinLossRejectsReportAndNeverRecaptures()throws Exception{
        try(var prefix=new Prefix("pin-loss",false)){
            prefix.coordinator.step(RequestContext.batch());prefix.coordinator.step(RequestContext.batch());assertEquals(RatisDatabaseBackupJournal.Phase.CAPTURED,prefix.log.snapshot().phase());var tokens=prefix.tokens();var request=prefix.log.snapshot().requests().getFirst();var endpoint=prefix.fixture.endpoints.stream().filter(e->e.state.poolId.equals(request.poolId())).findFirst().orElseThrow();endpoint.restartAfterDrain(request);assertEquals(1,prefix.physicalPins());
            assertThrows(RatisSnapshotLostException.class,()->prefix.coordinator.step(RequestContext.batch()));assertEquals(RatisDatabaseBackupJournal.Phase.LOST,prefix.log.snapshot().phase());assertEquals(tokens,prefix.tokens());assertEquals(0,prefix.fixture.base.reports.get());assertThrows(RatisSnapshotLostException.class,()->prefix.coordinator.step(RequestContext.batch()));assertEquals(1,prefix.physicalPins());assertEquals(2,prefix.fixture.frozen.size());
            prefix.release();assertEquals(0,prefix.physicalPins());
        }
    }
    @Test void laterBackupPhasesCannotBeRewrittenByCaptureCoordinator()throws Exception {
        try(var prefix=new Prefix("terminal-owner",false)){
            var progress=prefix.coordinator.step(RequestContext.batch());for(int n=0;n<16 && !progress.released();n++)progress=prefix.coordinator.step(RequestContext.batch());assertTrue(progress.released());
            // Exercise phase ownership only; this test does not publish or claim a complete archive.
            for(var phase:List.of(RatisDatabaseBackupJournal.Phase.MATERIALIZING,RatisDatabaseBackupJournal.Phase.COMPLETE)){prefix.log.phase(phase);var before=prefix.log.snapshot();assertThrows(IOException.class,()->prefix.coordinator.step(RequestContext.batch()));assertEquals(before.phase(),prefix.log.snapshot().phase());assertEquals(before.captured(),prefix.log.snapshot().captured());assertEquals(before.requests(),prefix.log.snapshot().requests());}prefix.release();
        }
    }

}
