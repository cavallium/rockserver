package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import static org.junit.jupiter.api.Assertions.*;

/** Real JNI participants/images and one frozen HOME authority; no client orchestration or quorum claim. */
class NativeCrossDomainAuthorityTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    static UUID id(long n){return new UUID(0,n);}
    final UUID cluster=id(1),database=id(2),alpha=id(3),beta=id(4),ga=id(5),gb=id(6),ca=id(7),cb=id(8),transaction=id(9),home=id(10);
    final OperationId business=new OperationId(id(20),1,0);
    final NativeRangeImage.Limits images=new NativeRangeImage.Limits(4<<20,32L<<20,2);
    DistributedSchema schema(UUID domain,UUID column){return new DistributedSchema(cluster,database,domain,1,0,List.of(new DistributedSchema.Column(1,column,"data",ColumnSchema.of(IntArrayList.of(1),new ObjectArrayList<>(),true),null)));}
    RangeDirectory directory(DistributedSchema schema,UUID group){var replica=new ReplicaDescriptor(id(30),id(31),id(32),"native","unused:1",ReplicaDescriptor.Role.VOTER);return new RangeDirectory(cluster,database,schema.domainId(),1,schema.digest(),7,schema.columnIds(),0,List.of(new RangeDescriptor(group,group,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(replica))));}
    TransactionScope scope(UUID column){return new TransactionScope(List.of(TransactionScope.Span.point(column,new byte[]{1})));}
    ReplicatedRangeStore open(DistributedStoragePool pool,DistributedSchema schema,UUID group,String name)throws Exception {
        var store=pool.openRange(new RangeOwnership.Owner(group,1),new RangeOwnership.Interval(schema.domainId(),schema.columnIds(),null,null),schema.digest(),Map.of(schema.columnIds().iterator().next(),name),Set.of());store.enableLogicalSchema(schema);return store;
    }
    void initialize(ReplicatedRangeStore store)throws Exception{store.initializeConfiguration(new byte[]{1});store.enableScopedHistory(RangeMutationHistory.Limits.defaults());store.applyNoop(new ApplyPosition(1,0));}
    DistributedStoragePool pool(Path path)throws Exception{return DistributedStoragePool.open(path,ConfigParser.parseDefault(),LoggerFactory.getLogger(getClass()));}
    DistributedCommand accepted(ReplicatedRangeStore store,DistributedSchema schema,UUID group,UUID column,int value)throws Exception {
        var command=new DistributedCommand(group,1,schema.digest(),business,List.of(new Mutation(Mutation.Kind.PUT,column,new byte[]{1},new byte[]{(byte)value})));
        try(var pin=store.captureTracked(scope(column))){return store.planScopedCommit(pin,command,0,id(40));}
    }
    TransactionStatus.Participant participant(DistributedSchema schema,UUID group,DistributedCommand accepted){return new TransactionStatus.Participant(schema.domainId(),group,group,1,schema.digest(),accepted.payloadDigest(),null,false);}

    @Test void oneDecisionAndCompleteBindingVectorSurvivePrepareImageColdOpenAndFinalization()throws Exception {
        var sa=schema(alpha,ca);var sb=schema(beta,cb);var da=DomainBinding.of(directory(sa,ga));var db=DomainBinding.of(directory(sb,gb));var source=root.resolve("source");var image=root.resolve("prepared.group");
        PreparedParticipant saved;ParticipantFinalization finalA;ParticipantFinalization finalB;
        try(var pool=pool(source)) {
            var a=open(pool,sa,ga,"alpha");var b=open(pool,sb,gb,"beta");initialize(a);initialize(b);
            var aa=accepted(a,sa,ga,ca,11);var ab=accepted(b,sb,gb,cb,22);var participants=List.of(participant(sa,ga,aa),participant(sb,gb,ab));
            var registration=TransactionStatus.begin(cluster,database,transaction,business,List.of(db,da),new byte[32],participants);
            var pa=new PreparedParticipant.Preparation(home,1,new OperationId(id(21),1,1),registration,aa);var pb=new PreparedParticipant.Preparation(home,1,new OperationId(id(21),1,2),registration,ab);
            var swapped=List.of(new DomainBinding(alpha,7,da.directoryDigest(),1,sb.digest()),new DomainBinding(beta,7,db.directoryDigest(),1,sa.digest()));
            var wrong=TransactionStatus.begin(cluster,database,id(99),business,swapped,new byte[32],List.of(new TransactionStatus.Participant(beta,ga,ga,1,sa.digest(),aa.payloadDigest(),null,false),new TransactionStatus.Participant(alpha,gb,gb,1,sb.digest(),ab.payloadDigest(),null,false)));
            assertThrows(IllegalArgumentException.class,()->a.applyPrepared(new ApplyPosition(1,1),new PreparedParticipant.Preparation(home,1,new OperationId(id(21),1,3),wrong,aa)));assertEquals(new ApplyPosition(1,0),a.applied());
            assertTrue(a.applyPrepared(new ApplyPosition(1,1),pa).prepared());assertTrue(b.applyPrepared(new ApplyPosition(1,1),pb).prepared());saved=a.preparedParticipant(transaction);assertEquals(List.of(da,db),saved.preparation().registration().domains());
            assertThrows(IllegalStateException.class,()->a.captureReadable(scope(ca)));assertThrows(IllegalStateException.class,()->b.captureReadable(scope(cb)));
            NativeRangeImage.write(image,a,images);assertNotNull(NativeRangeImage.inspect(image,images));
            var decision=registration.prepared(ga,aa.payloadDigest(),new ApplyPosition(1,1)).prepared(gb,ab.payloadDigest(),new ApplyPosition(1,1)).decide(TransactionStatus.Phase.COMMIT);
            finalA=new ParticipantFinalization(home,1,ga,new OperationId(id(22),1,1),decision);finalB=new ParticipantFinalization(home,1,gb,new OperationId(id(22),1,2),decision);
        }
        try(var pool=pool(source)) {
            var a=open(pool,sa,ga,"alpha");var b=open(pool,sb,gb,"beta");assertEquals(saved,a.preparedParticipant(transaction));assertEquals(2,b.preparedParticipant(transaction).preparation().registration().domains().size());
            a.applyFinalization(new ApplyPosition(1,2),finalA);b.applyFinalization(new ApplyPosition(1,2),finalB);
            assertEquals(finalA.decision(),a.committedParticipant(transaction).finalization().decision());assertEquals(finalB.decision(),b.committedParticipant(transaction).finalization().decision());
            try(var pin=a.capture()){assertArrayEquals(new byte[]{11},pin.get(ca,new byte[]{1}));}try(var pin=b.capture()){assertArrayEquals(new byte[]{22},pin.get(cb,new byte[]{1}));}
        }
        try(var pool=pool(root.resolve("portable"))) {
            var a=open(pool,sa,ga,"alpha");a.fence();a.installSnapshot(image,images);a.activate();assertEquals(saved,a.preparedParticipant(transaction));assertThrows(IllegalStateException.class,()->a.captureReadable(scope(ca)));
            a.applyFinalization(new ApplyPosition(1,2),finalA);assertEquals(2,a.committedParticipant(transaction).finalization().decision().domains().size());try(var pin=a.capture()){assertArrayEquals(new byte[]{11},pin.get(ca,new byte[]{1}));}
        }
    }
    @Test void oneSnapshotFenceInstallsEveryDomainBeforeCaptureAndReleasesOnlyTheGlobalCompleteDecision()throws Exception {
        var sa=schema(alpha,ca);var sb=schema(beta,cb);
        try(var pool=pool(root.resolve("gates"))) {
            var a=open(pool,sa,ga,"alpha");var b=open(pool,sb,gb,"beta");initialize(a);initialize(b);
            var fence=new SnapshotFence(cluster,database,id(50),home,1,new OperationId(id(51),1,0),List.of(DomainBinding.of(directory(sb,gb)),DomainBinding.of(directory(sa,ga))),List.of(new SnapshotFence.Participant(alpha,ga,ga,1,sa.digest(),new OperationId(id(52),1,0),new OperationId(id(52),1,1),scope(ca)),new SnapshotFence.Participant(beta,gb,gb,1,sb.digest(),new OperationId(id(52),1,2),new OperationId(id(52),1,3),scope(cb))));
            var ia=a.applyGateInstall(new ApplyPosition(1,1),fence).gate();var ib=b.applyGateInstall(new ApplyPosition(1,1),fence).gate();
            var home=SnapshotFenceStatus.begin(fence).ready(ga,ia.installed());assertFalse(home.allReady());home=home.ready(gb,ib.installed());assertTrue(home.allReady());
            try(var pa=a.captureGate(fence);var pb=b.captureGate(fence)){
                var capturing=home.capture(ga,new SnapshotFenceStatus.Capture(id(60),id(61),pa.position()));var incomplete=capturing;assertThrows(IllegalArgumentException.class,()->a.applyGateRelease(new ApplyPosition(1,2),incomplete));
                var complete=capturing.capture(gb,new SnapshotFenceStatus.Capture(id(62),id(63),pb.position())).complete();a.applyGateRelease(new ApplyPosition(1,2),complete);b.applyGateRelease(new ApplyPosition(1,2),complete);assertFalse(a.snapshotGate(fence.fenceId()).active());assertFalse(b.snapshotGate(fence.fenceId()).active());assertEquals(2,complete.fence().domains().size());
            }
            assertEquals(0,a.activeSnapshotPins());assertEquals(0,b.activeSnapshotPins());
        }
    }
}
