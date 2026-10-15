package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisLocalNoneDiscoveryTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    private static RatisConfiguredRecoveryGraph.Snapshot graph(RatisLocalCurrentDiscoveryTest.Fixture f,ApplyPosition cut) {
        return new RatisConfiguredRecoveryGraph.Snapshot(new RatisConfiguredRecoveryDiscovery.Plan(cut,List.of(new RatisConfiguredRecoveryDiscovery.Root(f.directory,f.directory.ranges().getFirst())),List.of(),List.of(),List.of()),List.of(),List.of(),Map.of());
    }
    @Test void absentNativePrefixAndUninstalledCurrentImageCanRemainInertWithoutInventedProgress()throws Exception {
        for(var mode:List.of(RatisLocalCurrentDiscoveryTest.Mode.EMPTY,RatisLocalCurrentDiscoveryTest.Mode.UNREADY)) {
            var helper=new RatisLocalCurrentDiscoveryTest();helper.root=root;
            try(var f=helper.new Fixture("none-"+mode,mode)) {
                var id=f.machine.getGroupId();var entry=new RatisRuntimeGroups(f.plan).require(id.getUuid());var division=f.server.getDivision(id);var graph=graph(f,f.graphCut);
                var stamp=f.machine.readinessStamp();var token=new RatisLocalNoneDiscovery(graph,entry,f.store,f.machine,division,f.plan.catalogue().groupId());
                assertTrue(token.current(graph,entry,f.store,f.machine,division,stamp));assertNull(f.store.applied());assertEquals(f.before,f.store.applied());
                assertTrue(f.machine.recoveryEligibilityGate().discoveryPending());assertFalse(f.store.admissionOpen());assertEquals(0,f.store.activeSnapshotPins());
                var source=new RatisNodeReadiness.GroupSource(entry.group(),entry.descriptor(),entry.directory().domainId(),entry.directory().schemaDigest(),entry.role(),division.getId(),false,f.store,f.machine,()->division,
                        ()->new RatisNodeReadiness.MachineStamp(stamp.admissionGeneration(),stamp.configurationStamp(),stamp.configurationPending(),RatisNodeReadiness.Discovery.NONE_AT_CUT,stamp.storageFailure(),stamp.observerFailure()));
                var report=RatisNodeReadiness.evaluate(f.plan.nodeId(),f.plan.nodeIncarnation(),()->new RatisNodeReadiness.NodeStamp(RatisNodeRuntime.State.RUNNING,false,graph,new RatisNodeReadiness.Recovery(true,0,0,null),null),List.of(source));
                assertTrue(report.ready(),report.toString());var status=report.groups().getFirst();assertEquals(RatisNodeReadiness.Discovery.NONE_AT_CUT,status.discovery());assertTrue(status.recoverySettled());assertFalse(status.servingReady());assertNull(status.nativeCut());assertNull(status.raftApplied());
                assertThrows(IllegalStateException.class,()->f.machine.recoveryEligibilityGate().snapshotPermit());assertTrue(f.machine.query(org.apache.ratis.protocol.Message.EMPTY).isCompletedExceptionally());
                assertFalse(token.current(graph(f,f.graphCut),entry,f.store,f.machine,division,stamp),"Equal graph contents do not preserve a replaced discovery owner");
                f.machine.recoveryEligibilityGate().publish(org.apache.ratis.server.impl.RecoveryEligibilityStateMachine.Evidence.missing());
                assertFalse(token.current(graph,entry,f.store,f.machine,division,f.machine.readinessStamp()));
            }
        }
    }
    @Test void warmCurrentAndMetadataSettlementFailuresReleasePrivateWorkBeforeNone()throws Exception {
        var helper=new RatisLocalCurrentDiscoveryTest();helper.root=root;
        try(var f=helper.new Fixture("none-warm-current",RatisLocalCurrentDiscoveryTest.Mode.READY)) {
            var candidate=f.machine.prepareLocalCurrentDiscovery(f.graphCut,f.directory);f.machine.reloadLocalCurrentDiscovery(candidate).get(10,java.util.concurrent.TimeUnit.SECONDS);
            assertThrows(IOException.class,()->f.machine.settleLocalCurrentDiscovery(candidate,publication->{throw new IOException("Lost Node publication owner");}));
            assertFalse(f.machine.localDiscoveryIdle());assertTrue(f.store.activeSnapshotPins()>0);var a=f.store.applied();
            f.machine.abandonLocalDiscovery().get(10,java.util.concurrent.TimeUnit.SECONDS);assertTrue(f.machine.localDiscoveryIdle());assertEquals(0,f.store.activeSnapshotPins());assertEquals(0,f.resources.stats().protectedLane().bytes());assertEquals(0,f.runway.stats().owners());
            assertEquals(a,f.store.applied());assertFalse(f.store.admissionOpen());assertTrue(f.machine.recoveryEligibilityGate().discoveryPending());
            assertThrows(IOException.class,()->f.machine.settleLocalCurrentDiscovery(candidate,publication->publication.publish()));
            var entry=new RatisRuntimeGroups(f.plan).require(f.machine.getGroupId().getUuid());assertNotNull(new RatisLocalNoneDiscovery(graph(f,f.graphCut),entry,f.store,f.machine,f.server.getDivision(f.machine.getGroupId()),f.plan.catalogue().groupId()));
        }
        var metadata=new RatisLocalMetadataDiscoveryTest();metadata.root=root;
        try(var f=metadata.new Fixture("none-warm-meta",true,false)) {
            var candidate=f.machine.prepareLocalMetadataDiscovery(f.graphCut,f.consuming);f.machine.reloadLocalMetadataDiscovery(candidate).get(10,java.util.concurrent.TimeUnit.SECONDS);
            assertThrows(IOException.class,()->f.machine.settleLocalMetadataDiscovery(candidate,publication->{throw new IOException("Lost Node publication owner");}));
            assertFalse(f.machine.localDiscoveryIdle());assertTrue(f.store.activeSnapshotPins()>0);var a=f.store.applied();
            f.machine.abandonLocalDiscovery().get(10,java.util.concurrent.TimeUnit.SECONDS);assertTrue(f.machine.localDiscoveryIdle());assertEquals(0,f.store.activeSnapshotPins());assertEquals(0,f.resources.stats().protectedLane().bytes());assertEquals(0,f.runway.stats().owners());
            assertEquals(a,f.store.applied());assertFalse(f.store.admissionOpen());assertTrue(f.machine.recoveryEligibilityGate().discoveryPending());
            assertThrows(IOException.class,()->f.machine.settleLocalMetadataDiscovery(candidate,publication->publication.publish()));
        }
    }
    @Test void pausedCurrentPreparationKeepsItsOriginalReloadPermitWhenNoneIsRefused()throws Exception {
        var helper=new RatisLocalCurrentDiscoveryTest();helper.root=root;
        try(var f=helper.new Fixture("none-paused",RatisLocalCurrentDiscoveryTest.Mode.READY)) {
            var candidate=f.machine.prepareLocalCurrentDiscovery(f.graphCut,f.directory);var a=f.store.applied();var revision=f.machine.recoveryEligibilityGate().revision();
            assertEquals(org.apache.ratis.util.LifeCycle.State.PAUSED,f.machine.getLifeCycleState());assertTrue(f.store.activeSnapshotPins()>0);
            assertThrows(java.util.concurrent.ExecutionException.class,()->f.machine.abandonLocalDiscovery().get(10,java.util.concurrent.TimeUnit.SECONDS));
            assertEquals(revision,f.machine.recoveryEligibilityGate().revision());assertEquals(a,f.store.applied());assertTrue(f.store.activeSnapshotPins()>0);
            f.machine.reloadLocalCurrentDiscovery(candidate).get(10,java.util.concurrent.TimeUnit.SECONDS);assertTrue(f.machine.settleLocalCurrentDiscovery(candidate,publication->publication.publish()));
            assertEquals(a,f.store.applied());assertTrue(f.store.admissionOpen());assertEquals(0,f.store.activeSnapshotPins());assertEquals(0,f.resources.stats().protectedLane().bytes());
        }
    }
    @Test void noneCannotUseCatalogueCutBeforeRetainedCurrentRegistrationOrAnotherCatalogue()throws Exception {
        var helper=new RatisLocalCurrentDiscoveryTest();helper.root=root;
        try(var f=helper.new Fixture("none-cut",RatisLocalCurrentDiscoveryTest.Mode.UNREADY)) {
            var entry=new RatisRuntimeGroups(f.plan).require(f.machine.getGroupId().getUuid());var division=f.server.getDivision(f.machine.getGroupId());var cut=f.registration.catalogueCut();
            var old=graph(f,new ApplyPosition(cut.term(),cut.index()-1));assertThrows(IOException.class,()->new RatisLocalNoneDiscovery(old,entry,f.store,f.machine,division,f.plan.catalogue().groupId()));
            assertThrows(IOException.class,()->new RatisLocalNoneDiscovery(graph(f,f.graphCut),entry,f.store,f.machine,division,new UUID(12040,1)));
            assertNull(f.store.applied());assertFalse(f.store.admissionOpen());
        }
    }
}
