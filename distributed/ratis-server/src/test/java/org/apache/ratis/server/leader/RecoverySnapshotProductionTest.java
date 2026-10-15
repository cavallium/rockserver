package org.apache.ratis.server.leader;

import java.lang.reflect.InvocationTargetException;
import java.nio.file.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ratis.proto.RaftProtos.RaftConfigurationProto;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.impl.RecoveryEligibilityStateMachine;
import org.apache.ratis.server.impl.RecoveryEligibilityStateMachine.Evidence;
import org.apache.ratis.server.impl.RecoveryEligibilityStateMachine.Gate;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.util.MD5FileUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Actual shared chunk producer and file reader; controlled cached SM evidence, without sockets. */
class RecoverySnapshotProductionTest {
    @TempDir Path root;
    private final RockserverSnapshotRequestsTest files=new RockserverSnapshotRequestsTest();
    private final AtomicInteger gateLookups=new AtomicInteger();
    private SnapshotInfo latest;
    private RaftServer.Division source(Path file,Gate gate) {
        var ordinary=files.source(file);var storage=ordinary.getStateMachine().getStateMachineStorage();
        var machine=RockserverSnapshotRequestsTest.proxy(RecoveryEligibilityStateMachine.class,(proxy,method,args)->switch(method.getName()){
            case "recoveryEligibilityGate"->{gateLookups.incrementAndGet();yield gate;}
            case "getStateMachineStorage"->storage;
            case "getLatestSnapshot"->latest;
            default->throw new AssertionError(method);
        });
        return RockserverSnapshotRequestsTest.proxy(RaftServer.Division.class,(proxy,method,args)->{
            if(method.getName().equals("getStateMachine"))return machine;
            try{return method.invoke(ordinary,args);}catch(InvocationTargetException failed){throw failed.getCause();}
        });
    }
    private SingleFileSnapshotInfo snapshot(Path file,long index)throws Exception{return new SingleFileSnapshotInfo(new FileInfo(file,MD5FileUtil.computeMd5ForFile(file.toFile())),TermIndex.valueOf(1,index));}
    private void readers(Path file,long expected)throws Exception{if(Files.isDirectory(Path.of("/proc/self/fd")))assertEquals(expected,files.readers(file));}
    private static Evidence recovered(long nativeIndex){return Evidence.recovered(TermIndex.valueOf(1,7),TermIndex.valueOf(1,nativeIndex),TermIndex.valueOf(1,7),0,-1,RaftConfigurationProto.getDefaultInstance());}
    private static int drain(RockserverSnapshotRequests.Requests requests){int bytes=0;while(requests.hasNext())bytes+=requests.next().getSnapshotChunk().getFileChunks(0).getData().size();return bytes;}
    @Test void discoveryFenceClosesExistingProducerAndRejectsNewExportsDespiteOrdinaryReadyPublication()throws Exception {
        var file=Files.write(root.resolve("snapshot.1_7"),new byte[3073]);var snapshot=snapshot(file,7);latest=snapshot;
        var gate=new Gate();gate.publish(Evidence.normal());var source=source(file,gate);var receiver=RaftPeerId.valueOf("receiver");
        var permit=gate.snapshotPermit();
        try(var requests=RockserverSnapshotRequests.openSource(source,receiver,"before-discovery",snapshot,1024,permit)) {
            requests.next();readers(file,1);gate.deferDiscovery();gate.publish(Evidence.normal());
            assertThrows(IllegalStateException.class,requests::next);assertFalse(requests.hasNext());readers(file,0);
        }
        assertThrows(IllegalStateException.class,()->RockserverSnapshotRequests.open(source,receiver,"pending",snapshot,1024));
        assertThrows(IllegalStateException.class,()->RockserverSnapshotRequests.openSource(source,receiver,"pending-source",snapshot,1024,permit));readers(file,0);
        assertTrue(gate.settleDiscovery(gate.revision(),Evidence.normal()));
        try(var requests=RockserverSnapshotRequests.open(source,receiver,"classified",snapshot,1024)){assertEquals(3073,drain(requests));}
        readers(file,0);
    }
    @Test void missingRejectsBothOutboundEntryPointsAndExistingIteratorBeforeOpeningAReader()throws Exception {
        var file=Files.write(root.resolve("snapshot.1_7"),new byte[3073]);var snapshot=snapshot(file,7);latest=snapshot;var gate=new Gate();var source=source(file,gate);var receiver=RaftPeerId.valueOf("receiver");
        assertThrows(IllegalStateException.class,gate::snapshotPermit);gate.publish(Evidence.normal());var permit=gate.snapshotPermit();gate.publish(Evidence.missing());
        readers(file,0);assertThrows(IllegalStateException.class,()->RockserverSnapshotRequests.open(source,receiver,"missing",snapshot,1024));assertThrows(IllegalStateException.class,()->RockserverSnapshotRequests.openSource(source,receiver,"missing-source",snapshot,1024,permit));readers(file,0);
        gate.publish(Evidence.normal());
        try(var requests=new InstallSnapshotRequests(source,receiver,"revoked-before-first",snapshot,1024).iterator()) {
            gate.publish(Evidence.missing());assertThrows(IllegalStateException.class,requests::next);assertFalse(requests.hasNext());readers(file,0);
        }
    }
    @Test void missedMissingPublicationRevokesBothExistingProducersAndClosesTheirReaders()throws Exception {
        var file=Files.write(root.resolve("snapshot.1_7"),new byte[3073]);var snapshot=snapshot(file,7);latest=snapshot;
        for(boolean sourceArtifact:new boolean[]{false,true}) {
            var gate=new Gate();gate.publish(Evidence.normal());var source=source(file,gate);int lookups=gateLookups.get();
            try(var requests=sourceArtifact?RockserverSnapshotRequests.openSource(source,RaftPeerId.valueOf("receiver"),"source-cycle",snapshot,1024,gate.snapshotPermit()):RockserverSnapshotRequests.open(source,RaftPeerId.valueOf("receiver"),"default-cycle",snapshot,1024)) {
                assertFalse(requests.next().getSnapshotChunk().getDone());readers(file,1);assertEquals(lookups+1,gateLookups.get(),"The producer retains the constant cached Gate");
                gate.publish(Evidence.missing());gate.publish(recovered(7));assertThrows(IllegalStateException.class,requests::next);readers(file,0);assertFalse(requests.hasNext());
                gate.publish(Evidence.normal());assertFalse(requests.hasNext());assertEquals(lookups+1,gateLookups.get());
            }
        }
    }
    @Test void nativeEvidenceAndLatestCheckpointProgressDoNotRevokeAnAdmittedArtifact()throws Exception {
        var file=Files.write(root.resolve("snapshot.1_7"),new byte[3073]);var snapshot=snapshot(file,7);var nextFile=Files.write(root.resolve("snapshot.1_8"),new byte[4097]);var next=snapshot(nextFile,8);
        for(boolean sourceArtifact:new boolean[]{false,true}) {
            latest=snapshot;var gate=new Gate();gate.publish(Evidence.normal());var source=source(file,gate);var permit=gate.snapshotPermit();
            try(var requests=sourceArtifact?RockserverSnapshotRequests.openSource(source,RaftPeerId.valueOf("receiver"),"source-progress",snapshot,1024,permit):RockserverSnapshotRequests.open(source,RaftPeerId.valueOf("receiver"),"default-progress",snapshot,1024)) {
                assertEquals(1024,requests.next().getSnapshotChunk().getFileChunks(0).getData().size());latest=next;gate.publish(Evidence.normal());gate.publish(recovered(8));permit.requireCurrent();assertEquals(2049,drain(requests));readers(file,0);
            }
            latest=snapshot;try(var requests=RockserverSnapshotRequests.open(source,RaftPeerId.valueOf("receiver"),"recovered",snapshot,1024)){assertEquals(3073,drain(requests));}readers(file,0);
        }
        try(var requests=RockserverSnapshotRequests.open(files.source(file),RaftPeerId.valueOf("receiver"),"ordinary",snapshot,1024)){assertEquals(3073,drain(requests));}readers(file,0);
    }
    @Test void equalCutReplacedArtifactCannotBeSelectedBeforeRevocationAndConstructedAfterRecovery()throws Exception {
        var file=Files.write(root.resolve("snapshot.1_7"),new byte[3073]);var old=snapshot(file,7);latest=old;var gate=new Gate();gate.publish(Evidence.normal());var source=source(file,gate);
        gate.publish(Evidence.missing());byte[] replacement=new byte[3073];replacement[0]=1;Files.write(file,replacement);latest=snapshot(file,7);gate.publish(recovered(7));
        assertEquals(old.getTermIndex(),latest.getTermIndex());assertNotEquals(old.getFiles().getFirst().getFileDigest(),latest.getFiles().getFirst().getFileDigest());assertThrows(IllegalStateException.class,()->RockserverSnapshotRequests.open(source,RaftPeerId.valueOf("receiver"),"stale-default",old,1024));readers(file,0);
        var noDigest=new SingleFileSnapshotInfo(new FileInfo(file,null),TermIndex.valueOf(1,7));latest=noDigest;assertThrows(IllegalStateException.class,()->RockserverSnapshotRequests.open(source,RaftPeerId.valueOf("receiver"),"unknown-digest",noDigest,1024));
        latest=snapshot(file,7);try(var requests=RockserverSnapshotRequests.open(source,RaftPeerId.valueOf("receiver"),"new-default",latest,1024)){assertEquals(3073,drain(requests));}readers(file,0);
    }
    @Test void sourceArtifactRequiresAFreshPermitFromItsExactGateWithoutCanonicalLatestEquality()throws Exception {
        var canonical=Files.write(root.resolve("snapshot.1_7"),new byte[1]);latest=snapshot(canonical,7);var artifact=Files.write(root.resolve("qualified-source.image"),new byte[2305]);var snapshot=snapshot(artifact,9);var gate=new Gate();gate.publish(Evidence.normal());var source=source(canonical,gate);var old=gate.snapshotPermit();
        gate.publish(Evidence.missing());gate.publish(recovered(9));assertThrows(IllegalStateException.class,()->RockserverSnapshotRequests.openSource(source,RaftPeerId.valueOf("receiver"),"stale-source",snapshot,1024,old));readers(artifact,0);
        var other=new Gate();other.publish(Evidence.normal());assertThrows(IllegalStateException.class,()->RockserverSnapshotRequests.openSource(source,RaftPeerId.valueOf("receiver"),"foreign-source",snapshot,1024,other.snapshotPermit()));assertThrows(NullPointerException.class,()->RockserverSnapshotRequests.openSource(source,RaftPeerId.valueOf("receiver"),"unqualified-source",snapshot,1024,null));
        try(var requests=RockserverSnapshotRequests.openSource(source,RaftPeerId.valueOf("receiver"),"fresh-source",snapshot,1024,gate.snapshotPermit())){assertEquals(2305,drain(requests));}readers(artifact,0);
    }
}
