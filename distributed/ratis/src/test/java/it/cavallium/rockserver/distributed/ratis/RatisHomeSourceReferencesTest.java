package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Actual JNI HOME rows/fences; controlled page transport, no network or quorum acceptance claim. */
class RatisHomeSourceReferencesTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final RatisHomeSourceClosingTest helper=new RatisHomeSourceClosingTest();
    RatisNodePlan.Home expected(RatisRetentionAdmissionTest.Fixture home){
        var replica=new ReplicaDescriptor(new UUID(601,1),new UUID(601,2),new UUID(601,3),home.peer.getId().toString(),"localhost:47001",ReplicaDescriptor.Role.VOTER);
        var range=new RangeDescriptor(home.groupId,home.groupId,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(replica));
        var origin=sourceOrigin;var directory=new RangeDirectory(origin.clusterId(),origin.databaseId(),home.store.ownership().interval().domainId(),0,new byte[32],0,Set.of(home.column),0,List.of(range));
        return new RatisNodePlan.Home(RatisNodePlan.Role.TRANSACTION_HOME,directory,home.column,"data");
    }
    void configure(RatisRetentionAdmissionTest.Fixture home)throws Exception{
        // Genuine native configuration progression; the controlled page fixture does not
        // assert a live Raft configuration change or remote endpoint acceptance.
        var cfg=org.apache.ratis.proto.RaftProtos.RaftConfigurationProto.newBuilder().addPeers(home.peer.getRaftPeerProto().toBuilder().setAddress("localhost:47001")).build();
        home.store.applyNoop(new ApplyPosition(1,1),cfg.toByteArray());home.machine.notifyTermIndexUpdated(1,1);
    }
    OperationRerouteOrigin sourceOrigin;
    @Test void actualForcedHomeEmptyScanRoundTripsAndQualificationKeepsPaidEvidence()throws Exception{
        try(var source=helper.source.helper.new Fixture(root.resolve("source"),false)){
            sourceOrigin=source.request.origin();var closed=helper.source.close(source,helper.source.record(source));
            try(var home=helper.home(source,root.resolve("home"),new UUID(601,4),new UUID(601,5))){
                configure(home);var fence=helper.close(home,closed,2);var request=new RatisHomeSourceReferencesRpc.Request(fence,null,16);var decoded=RatisHomeSourceReferencesRpc.request(RatisHomeSourceReferencesRpc.request(request));assertArrayEquals(request.digest(),decoded.digest());
                var reply=new RatisHomeSourceReferencesRpc.Reply(request,home.store.homeSourceReferences(fence,null,16),home.store.configuration());var encoded=RatisHomeSourceReferencesRpc.reply(reply);assertTrue(encoded.length<=RatisHomeSourceReferencesRpc.MAX_REPLY_BYTES);assertFalse(RatisHomeSourceReferencesRpc.reply(encoded,request).page().more());
                assertThrows(IllegalArgumentException.class,()->RatisHomeSourceReferencesRpc.reply(Arrays.copyOf(encoded,encoded.length+1),request));
                var wrong=new RatisHomeSourceReferencesRpc.Request(fence,new byte[16],16);assertThrows(IllegalArgumentException.class,()->RatisHomeSourceReferencesRpc.reply(encoded,wrong));
                var oversizedCfg=encoded.clone();ByteBuffer.wrap(oversizedCfg).putInt(ScopedTransactionRpc.HEADER_BYTES+64,65537);assertThrows(IllegalArgumentException.class,()->RatisHomeSourceReferencesRpc.reply(oversizedCfg,request));
                var oversizedFence=RatisHomeSourceReferencesRpc.request(request);ByteBuffer.wrap(oversizedFence).putInt(ScopedTransactionRpc.HEADER_BYTES,Integer.MAX_VALUE);assertThrows(IllegalArgumentException.class,()->RatisHomeSourceReferencesRpc.request(oversizedFence));
                var contradictory=new ReplicatedRangeStore.HomeSourceReferencePage(home.store.applied(),new byte[16],true,null,null);assertThrows(IllegalArgumentException.class,()->new RatisHomeSourceReferencesRpc.Reply(new RatisHomeSourceReferencesRpc.Request(fence,new byte[16],16),contradictory,home.store.configuration()));
                var malformed=encoded.clone();malformed[ScopedTransactionRpc.HEADER_BYTES]^=1;assertThrows(IllegalArgumentException.class,()->RatisHomeSourceReferencesRpc.reply(malformed,request));
                assertThrows(IllegalArgumentException.class,()->new RatisHomeSourceReferencesRpc.Request(fence,new byte[15],16));assertThrows(IllegalArgumentException.class,()->new RatisHomeSourceReferencesRpc.Request(fence,null,17));
                var owners=new AtomicInteger();RatisSourceReferenceClosure.Admission admission=bytes->{assertTrue(bytes>0);owners.incrementAndGet();return owners::decrementAndGet;};
                var readerCalls=new AtomicInteger();RatisSourceReferenceClosure.Reader reader=(query,credit)->{readerCalls.incrementAndGet();try(var workspace=credit.reserve(RatisHomeSourceReferencesRpc.WORKSPACE_BYTES)){return RatisHomeSourceReferencesRpc.reply(RatisHomeSourceReferencesRpc.reply(new RatisHomeSourceReferencesRpc.Reply(query,home.store.homeSourceReferences(query.closing(),query.after(),query.scans()),home.store.configuration())),query);}catch(org.rocksdb.RocksDBException failure){throw new IOException(failure);}};
                var qualification=RatisSourceReferenceClosure.scan(closed,List.of(expected(home)),List.of(fence),reader,admission);assertEquals(1,owners.get());assertEquals(1,readerCalls.get());assertEquals(1,qualification.witnesses().size());assertEquals(home.groupId,qualification.witnesses().getFirst().home());assertEquals(32,qualification.digest().length);qualification.close();assertEquals(0,owners.get());assertThrows(IllegalStateException.class,qualification::digest);
                var transactionHome=expected(home);var snapshotId=new UUID(601,20);var snapshotColumn=new UUID(601,21);var snapshotRange=new RangeDescriptor(snapshotId,snapshotId,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,transactionHome.directory().ranges().getFirst().replicas());
                var snapshotDirectory=new RangeDirectory(sourceOrigin.clusterId(),sourceOrigin.databaseId(),new UUID(601,22),0,new byte[32],0,Set.of(snapshotColumn),0,List.of(snapshotRange));var snapshotHome=new RatisNodePlan.Home(RatisNodePlan.Role.SNAPSHOT_HOME,snapshotDirectory,snapshotColumn,"snapshot");
                assertThrows(IOException.class,()->RatisSourceReferenceClosure.scan(closed,List.of(transactionHome,snapshotHome),List.of(fence),reader,admission),"A zero-current-credit snapshot HOME still requires its exact forced fence");assertEquals(1,readerCalls.get());assertEquals(0,owners.get());
                assertThrows(IOException.class,()->RatisSourceReferenceClosure.scan(closed,List.of(expected(home)),List.of(),reader,admission));assertEquals(1,readerCalls.get(),"Missing HOME fence fails before any page RPC");assertEquals(0,owners.get());
                assertThrows(IOException.class,()->RatisSourceReferenceClosure.scan(closed,List.of(expected(home)),List.of(fence),reader,bytes->{throw new IOException("No workspace");}));assertEquals(1,readerCalls.get());
            }
        }
    }
    @Test void acceptedNativeReservationIsNotMistakenForReferenceClosure()throws Exception{
        try(var source=helper.source.helper.new Fixture(root.resolve("referenced-source"),false)){
            sourceOrigin=source.request.origin();var column=new UUID(601,6);
            try(var home=helper.home(source,root.resolve("referenced-home"),column,new UUID(601,7))){
                configure(home);var reserved=source.base.status();byte[] key=ByteBuffer.allocate(16).putLong(reserved.transactionId().getMostSignificantBits()).putLong(reserved.transactionId().getLeastSignificantBits()).array();
                var command=new DistributedCommand(home.groupId,1,new byte[32],reserved.origin().firstControl(),List.of(new Mutation(Mutation.Kind.PUT,column,key,reserved.encode())),List.of(new KeyPredicate(column,key,null)));
                assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(home.store.apply(new ApplyPosition(1,2),command)).status());home.machine.notifyTermIndexUpdated(1,2);
                var closed=helper.source.close(source,helper.source.record(source));var fence=helper.close(home,closed,3);var owners=new AtomicInteger();
                RatisSourceReferenceClosure.Admission admission=bytes->{owners.incrementAndGet();return owners::decrementAndGet;};
                var outstanding=assertThrows(RatisSourceReferenceClosure.OutstandingReference.class,()->RatisSourceReferenceClosure.scan(closed,List.of(expected(home)),List.of(fence),(request,credit)->{
                    try(var workspace=credit.reserve(RatisHomeSourceReferencesRpc.WORKSPACE_BYTES)){return RatisHomeSourceReferencesRpc.reply(RatisHomeSourceReferencesRpc.reply(new RatisHomeSourceReferencesRpc.Reply(request,home.store.homeSourceReferences(fence,request.after(),request.scans()),home.store.configuration())),request);}catch(org.rocksdb.RocksDBException failure){throw new IOException(failure);}
                },admission));assertEquals(home.groupId,outstanding.home());assertEquals(reserved.transactionId(),outstanding.identity());assertFalse(outstanding.snapshot());assertEquals(0,owners.get());assertEquals(reserved,TransactionStatus.decode(home.store.readCurrentPoint(column,key)),"Qualification performs no abort, expiry or consumption");
            }
        }
    }
}
