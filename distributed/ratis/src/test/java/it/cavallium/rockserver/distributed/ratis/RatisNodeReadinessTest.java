package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.ApplyPosition;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.server.DivisionInfo;
import org.apache.ratis.server.RaftConfiguration;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisNodeReadiness.*;

class RatisNodeReadinessTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    private static final UUID NODE=new UUID(0,200),INCARNATION=new UUID(0,201);
    private static NodeStamp node(Object placement,Recovery recovery) {
        return new NodeStamp(RatisNodeRuntime.State.RUNNING,false,placement,recovery,null);
    }
    private static final Recovery SETTLED=new Recovery(true,0,0,null);
    private static boolean has(Report report,Reason reason) {
        return report.issues().stream().anyMatch(i->i.reason()==reason)
                || report.groups().stream().flatMap(g->g.issues().stream()).anyMatch(i->i.reason()==reason);
    }
    private static final class Evidence {
        final AtomicLong term=new AtomicLong(1),committed=new AtomicLong(0);
        final RaftServer.Division division;
        Evidence(RatisTopologyFreezeTest.Fixture f) {
            var info=proxy(DivisionInfo.class,(method,args)->switch(method) {
                case "getCurrentTerm" -> term.get();
                case "getLastAppliedIndex" -> f.machine.getLastAppliedTermIndex().getIndex();
                case "getLifeCycleState" -> LifeCycle.State.RUNNING;
                case "isAlive", "isLeaderReady" -> true;
                case "getCurrentRole" -> RaftPeerRole.FOLLOWER;
                default -> throw new AssertionError(method);
            });
            var configuration=proxy(RaftConfiguration.class,(method,args)->switch(method) {
                case "getLogEntryIndex" -> -1L;
                case "getCurrentPeers" -> args[0]==RaftPeerRole.FOLLOWER ? List.of(f.peer) : List.of();
                case "getPreviousPeers" -> List.of();
                default -> throw new AssertionError(method);
            });
            var log=org.mockito.Mockito.mock(RaftLog.class);
            org.mockito.Mockito.when(log.getLastCommittedIndex()).thenAnswer(ignored -> committed.get());
            division=proxy(RaftServer.Division.class,(method,args)->switch(method) {
                case "getInfo" -> info;
                case "getRaftConf" -> configuration;
                case "getRaftLog" -> log;
                case "getStateMachine" -> f.machine;
                case "getGroup" -> f.group;
                case "getId" -> f.peer.getId();
                default -> throw new AssertionError(method);
            });
        }
    }
    @FunctionalInterface private interface Call {Object invoke(String method,Object[] args);}
    private static <T>T proxy(Class<T> type,Call call) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(),new Class<?>[]{type},(object,method,args)-> {
            if(method.getName().equals("toString"))return "readiness-"+type.getSimpleName();
            return call.invoke(method.getName(),args);
        }));
    }
    private static GroupSource source(RatisTopologyFreezeTest.Fixture f,boolean required,DivisionReader division,MachineReader stamp) {
        return new GroupSource(f.group,f.parent,f.schema.domainId(),f.schema.digest(),RatisNodePlan.Role.DATA,
                f.peer.getId(),required,f.store,f.machine,division,stamp);
    }
    private static Report report(NodeReader node,GroupSource source) {return evaluate(NODE,INCARNATION,node,List.of(source));}

    @Test void stableNativePrefixIsReadyWithoutSnapshotOrAuthorityLookup() throws Exception {
        try(var f=new RatisTopologyFreezeTest.Fixture(root)) {
            var evidence=new Evidence(f);var source=source(f,true,()->evidence.division,f.machine::readinessStamp);
            var result=report(()->node("placement",SETTLED),source);
            assertTrue(result.ready(),result.toString()); assertTrue(result.recoverySettled());
            assertEquals(new ApplyPosition(1,0),result.groups().getFirst().nativeCut());
            assertEquals(0,f.store.activeSnapshotPins());assertEquals(0,f.reads.get());
            evidence.committed.set(1);
            result=report(()->node("placement",SETTLED),source);
            assertTrue(has(result,Reason.APPLY_LAG));assertFalse(result.ready());assertFalse(result.recoverySettled());
            evidence.committed.set(0);evidence.term.set(2);
            result=report(()->node("placement",SETTLED),source);
            assertTrue(has(result,Reason.CURRENT_TERM_NOT_APPLIED));assertFalse(result.recoverySettled());
        }
    }
    @Test void failedNonServingGroupSampleNeverSettlesRecovery() throws Exception {
        try(var f=new RatisTopologyFreezeTest.Fixture(root)) {
            var source=source(f,false,()->{throw new IOException("division unavailable");},f.machine::readinessStamp);
            var result=report(()->node("placement",SETTLED),source);
            assertTrue(result.localServingReady());assertFalse(result.recoverySettled());assertFalse(result.ready());
            assertFalse(result.groups().getFirst().recoverySampled());assertTrue(has(result,Reason.SAMPLE_FAILED));
        }
    }
    @Test void unstableOrFailedNodeResampleCannotCertifyRecovery() throws Exception {
        try(var f=new RatisTopologyFreezeTest.Fixture(root)) {
            var evidence=new Evidence(f);var source=source(f,true,()->evidence.division,f.machine::readinessStamp);
            var calls=new AtomicInteger();
            var changed=report(()->node(calls.incrementAndGet(),SETTLED),source);
            assertTrue(has(changed,Reason.NODE_CHANGED));assertFalse(changed.recoverySettled());
            calls.set(0);
            var failed=report(()->{if(calls.incrementAndGet()==2)throw new IOException("node scan failed");return node("placement",SETTLED);},source);
            assertTrue(has(failed,Reason.SAMPLE_FAILED));assertFalse(failed.recoverySettled());
        }
    }
    @Test void pendingConfigurationStorageFailureAndChangedOwnerFailClosed() throws Exception {
        try(var f=new RatisTopologyFreezeTest.Fixture(root)) {
            var evidence=new Evidence(f);var actual=f.machine.readinessStamp();
            for(var sample:List.of(new MachineStamp(actual.admissionGeneration(),actual.configurationStamp(),true,null,null),
                    new MachineStamp(actual.admissionGeneration(),actual.configurationStamp(),false,new IOException("native write failed"),null))) {
                var result=report(()->node("placement",SETTLED),source(f,true,()->evidence.division,()->sample));
                assertFalse(result.ready());assertFalse(result.recoverySettled());
                assertTrue(has(result,sample.configurationPending() ? Reason.CONFIGURATION_PENDING : Reason.STORAGE_FAILED));
            }
            var calls=new AtomicInteger();
            var changed=report(()->node("placement",SETTLED),source(f,true,()->evidence.division,
                    ()->new MachineStamp(actual.admissionGeneration()+calls.getAndIncrement(),actual.configurationStamp(),false,null,null)));
            assertTrue(has(changed,Reason.OWNER_CHANGED));assertFalse(changed.recoverySettled());
        }
    }
    @Test void registryRecoveryIsDistinctFromLocalServingReadiness() throws Exception {
        try(var f=new RatisTopologyFreezeTest.Fixture(root)) {
            var evidence=new Evidence(f);var source=source(f,true,()->evidence.division,f.machine::readinessStamp);
            for(var recovery:List.of(new Recovery(false,0,0,null),new Recovery(true,1,0,null),
                    new Recovery(true,0,1,null),new Recovery(true,0,0,new IOException("registry recovery failed")))) {
                var result=report(()->node("placement",recovery),source);
                assertTrue(result.localServingReady(),result.toString());assertFalse(result.recoverySettled());assertFalse(result.ready());
            }
        }
    }
}
