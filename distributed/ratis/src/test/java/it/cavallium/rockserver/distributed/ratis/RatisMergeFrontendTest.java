package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.jar.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Frontend commands over real qualified native operators and controlled DATA/HOME dispatch. */
class RatisMergeFrontendTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    public static class AppendOperator extends FFMByteArrayMergeOperator {
        public AppendOperator(String identity) { super(identity); }
        @Override public byte[] merge(byte[] key,byte[] existing,List<byte[]> operands) {
            var out=new ByteArrayOutputStream(); if (existing!=null) out.writeBytes(existing); operands.forEach(out::writeBytes); return out.toByteArray();
        }
    }
    static byte[] jar() throws Exception {
        var out=new ByteArrayOutputStream(); String name=AppendOperator.class.getName().replace('.','/')+".class";
        try (var jar=new JarOutputStream(out); var input=AppendOperator.class.getClassLoader().getResourceAsStream(name)) { jar.putNextEntry(new JarEntry(name)); jar.write(Objects.requireNonNull(input).readAllBytes()); jar.closeEntry(); } return out.toByteArray();
    }
    static Keys key(int prefix) { return RatisAtomicClientTest.key(prefix); }
    static Keys member(int prefix,String tail) { return RatisAtomicClientTest.member(prefix,tail); }
    static Buf value(int... bytes) { byte[] result=new byte[bytes.length]; for (int i=0;i<bytes.length;i++) result[i]=(byte)bytes[i]; return Buf.wrap(result); }
    static <T> T await(CompletableFuture<T> value) throws Exception { return value.get(15,TimeUnit.SECONDS); }
    static void bytes(Buf actual,int... expected) { assertNotNull(actual); assertArrayEquals(value(expected).toByteArray(),actual.toByteArray()); }
    final class Fixture implements AutoCloseable {
        final RatisAtomicClientTest.Fixture f;
        final DistributedSchema schema;
        final RatisTransactionClient transactions;
        final RatisRocksDBAdapter.AllocationSource allocations;
        final SessionEpochLedger issuance;
        final SessionEpochLedger.Grant grant;
        final RatisDurableSessionClient session;
        final RatisRoutedClient routed;
        final RatisRocksDBDirect direct;
        final RatisRocksDBAdapter adapter;
        final AtomicBoolean lose=new AtomicBoolean(); final List<DistributedCommand> sent=new ArrayList<>();
        final Path window,journal;
        boolean closedDirect;
        SchemaChange.Definition lastDefinition; NativeSchemaActivationProof lastActivation;
        Fixture(Path path) throws Exception {
            f=new RatisAtomicClientTest.Fixture(path,true); byte[] jar=jar();
            var operator=new DistributedSchema.Operator("frontend-append",1,AppendOperator.class.getName(),OperatorUpload.sha256(jar),1,1,"append-contract",1);
            DistributedStoragePool.stageOperator(f.root.resolve("catalogue/catalogue"),operator,new MergeOperatorRegistry.DeterministicContract("append-contract",1,true),jar);
            var current=install(f.schema,75,new UUID(310,75),"fixed-merge",operator,false);
            schema=install(current,76,new UUID(310,76),"bucket-merge",operator,true);
            transactions=new RatisTransactionClient(schema,f.snapshots,f.homes,f.data,f.scheduler,RatisTransactionClient.Limits.defaults()); allocations=new RatisRocksDBAdapterTest.Allocations(f);
            issuance=f.ledger.issue(new UUID(310,1),1000); grant=issuance.grants().getLast();
            assertEquals(CommandResult.Status.SUCCESS,f.catalogue.epochs.execute(f.catalogue.epochs.prepare(f.outer.operation(2),f.ledger,issuance)).status());
            int n=0; for (var group:f.groups) assertEquals(CommandResult.Status.SUCCESS,f.data.execute(f.data.prepareSessionControl(f.catalogue.epochs.prepareSessionBinding(f.data.directory(),group,new OperationId(f.admin,2,n++),grant,32,-1))).status());
            var transport=f.transport(); routed=new RatisRoutedClient(f.data.directory(),new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID column,byte[] key) throws IOException { return transport.read(route,column,key); }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) throws IOException { synchronized (sent) { sent.add(command); } var result=transport.command(route,command); if (lose.getAndSet(false)) throw new IOException("Lost native MERGE reply"); return result; }
                public byte[] transaction(RangeDescriptor route,byte[] bytes,boolean fresh) throws IOException { return transport.transaction(route,bytes,fresh); }
            });
            window=f.root.resolve("merge-window"); journal=f.root.resolve("merge-request"); session=RatisDurableSessionClient.create(window,routed,issuance,grant,RatisSessionClient.Limits.defaults());
            direct=new RatisRocksDBDirect(schema,routed,session,f.scheduler,journal); adapter=new RatisRocksDBAdapter(schema,f.data.directory(),transactions,allocations,direct,null,new AtomicLong()::incrementAndGet);
        }
        DistributedSchema install(DistributedSchema source,long api,UUID column,String name,DistributedSchema.Operator operator,boolean bucket) throws Exception {
            var shape=ColumnSchema.of(IntArrayList.of(1),bucket ? ObjectArrayList.of(ColumnHashType.ALLSAME8) : ObjectArrayList.of(),true,operator.name(),operator.version(),operator.className());
            var columns=new ArrayList<>(source.columns()); columns.add(new DistributedSchema.Column(api,column,name,shape,operator));
            var target=new DistributedSchema(source.clusterId(),source.databaseId(),source.domainId(),source.schemaEpoch()+1,source.routingPrefixBytes(),columns);
            var definition=new SchemaChange.Definition(new UUID(311,api),new OperationId(f.admin,1,api),f.data.directory(),source,target); NativeSchemaActivationProof activated=null;
            for (var group:f.groups) {
                var store=f.stores.get(group); long index=store.applied().index(); store.applySchemaFence(new ApplyPosition(1,++index),definition.header());
                var names=new HashMap<>(f.catalogue.pool.schemaPhysicalNames(store)); names.put(column,name); var merges=new HashSet<>(store.deterministicMergeColumns()); merges.add(column);
                var installed=f.catalogue.pool.installSchema(store,new ApplyPosition(1,++index),definition,names,merges);
                var record=new RatisSchemaActivateRecord(definition.header(),group,installed.position(),f.outer.catalogue,new ApplyPosition(1,10));
                var context=org.apache.ratis.statemachine.TransactionContext.newBuilder().setStateMachine(f.machines.get(group)).setServerRole(org.apache.ratis.proto.RaftProtos.RaftPeerRole.FOLLOWER)
                        .setLogEntry(org.apache.ratis.proto.RaftProtos.LogEntryProto.newBuilder().setTerm(1).setIndex(++index).setStateMachineLogEntry(org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto.newBuilder().setLogData(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(record.encode()))).build()).build();
                activated=NativeSchemaActivationProof.decode(await(f.machines.get(group).applyTransaction(context)).getContent().toByteArray());
            }
            f.data.refreshSchema(definition,activated); lastDefinition=definition; lastActivation=activated; return target;
        }
        @Override public void close() throws Exception { adapter.close(); if (!closedDirect) { direct.close(); session.close(); } routed.close(); transactions.close(); f.close(); }
    }
    @Test void qualifiedFixedAndCollisionBucketsSupportNativeMergedAndOrdinaryCallbacks() throws Exception {
        try (var x=new Fixture(root.resolve("direct"))) {
            var api=x.adapter.api(RequestContext.batch());
            await(api.putAsync(0,75,key(2),value(1),RequestType.<Buf>none())); bytes(await(api.mergeAsync(0,75,key(2),value(2),RequestType.<Buf>merged())),1,2);
            assertNull(await(api.mergeAsync(0,75,key(2),value(3),RequestType.<Buf>none()))); bytes(await(api.getAsync(0,75,key(2),RequestType.<Buf>current())),1,2,3);
            var delta=await(api.putAsync(0,75,key(2),value(4),RequestType.<Buf>delta())); bytes(delta.previous(),1,2,3); bytes(delta.current(),4);
            assertTrue(await(api.deleteAsync(0,75,key(2),RequestType.<Buf>previousPresence()))); assertNull(await(api.getAsync(0,75,key(2),RequestType.<Buf>current())));
            var a=member(2,"alpha"); var b=member(2,"beta"); await(api.putAsync(0,76,a,value(5),RequestType.<Buf>none())); await(api.putAsync(0,76,b,value(9),RequestType.<Buf>none()));
            var result=await(api.mergeMultiAsync(0,76,List.of(a,a),List.of(value(6),value(7)),RequestType.<Buf>merged())); bytes(result.get(0),5,6); bytes(result.get(1),5,6,7);
            bytes(await(api.getAsync(0,76,b,RequestType.<Buf>current())),9); bytes(await(api.deleteAsync(0,76,a,RequestType.<Buf>previous())),5,6,7); bytes(await(api.getAsync(0,76,b,RequestType.<Buf>current())),9);
            assertTrue(x.sent.stream().anyMatch(command -> command.mutations().getFirst().kind()==Mutation.Kind.BUCKET_MERGE));
        }
    }
    @Test void fullStagedPrefixMergesOnceWithPutDeleteSnapshotRollbackAndCommit() throws Exception {
        try (var x=new Fixture(root.resolve("transaction"))) {
            var api=x.adapter.api(RequestContext.batch()); var a=member(2,"alpha"); var b=member(2,"beta");
            await(api.putAsync(0,76,a,value(1),RequestType.<Buf>none())); await(api.putAsync(0,76,b,value(9),RequestType.<Buf>none()));
            long tx=await(api.openTransactionAsync(Duration.ofSeconds(30))); bytes(await(api.mergeAsync(tx,76,a,value(2),RequestType.<Buf>merged())),1,2);
            bytes(await(api.mergeAsync(tx,76,a,value(3),RequestType.<Buf>merged())),1,2,3); bytes(await(api.putAsync(tx,76,b,value(8),RequestType.<Buf>previous())),9);
            bytes(await(api.mergeAsync(tx,76,a,value(4),RequestType.<Buf>merged())),1,2,3,4); bytes(await(api.getAsync(tx,76,a,RequestType.<Buf>current())),1,2,3,4);
            var page=await(api.getRangePageAsync(tx,76,key(2),key(3),false,null,RequestType.<KV>allInRange(),RangeBudget.DEFAULT)); assertEquals(List.of(a,b),page.items().stream().map(KV::keys).toList()); bytes(page.items().getFirst().value(),1,2,3,4);
            bytes(await(api.getAsync(0,76,a,RequestType.<Buf>current())),1); assertTrue(await(api.closeTransactionAsync(tx,false))); bytes(await(api.getAsync(0,76,a,RequestType.<Buf>current())),1);
            tx=await(api.openTransactionAsync(Duration.ofSeconds(30))); await(api.mergeAsync(tx,76,a,value(2),RequestType.<Buf>none())); bytes(await(api.mergeAsync(tx,76,a,value(3),RequestType.<Buf>merged())),1,2,3);
            bytes(await(api.putAsync(tx,76,b,value(8),RequestType.<Buf>previous())),9); bytes(await(api.mergeAsync(tx,76,a,value(4),RequestType.<Buf>merged())),1,2,3,4); assertTrue(await(api.closeTransactionAsync(tx,true)));
            bytes(await(api.getAsync(0,76,a,RequestType.<Buf>current())),1,2,3,4); bytes(await(api.getAsync(0,76,b,RequestType.<Buf>current())),8);
            var update=await(api.getAsync(0,76,a,RequestType.<Buf>forUpdate())); var invalid=assertThrows(ExecutionException.class,() -> await(api.mergeAsync(update.updateId(),76,a,value(5),RequestType.<Buf>none()))); assertInstanceOf(RocksDBException.class,invalid.getCause()); assertNull(await(api.closeFailedUpdateAsync(update.updateId())));
        }
    }
    @Test void unknownNativeMergeReopensExactOperandIdentityAndSavedMergedResult() throws Exception {
        try (var x=new Fixture(root.resolve("reopen"))) {
            await(x.adapter.api(RequestContext.batch()).putAsync(0,75,key(2),value(1),RequestType.<Buf>none())); x.lose.set(true);
            assertThrows(ExecutionException.class,() -> await(x.direct.write(75,key(2),value(2),RatisRocksDBDirect.WriteKind.MERGE,RequestType.RequestTypeId.MERGED,RequestContext.batch())));
            var pending=x.session.pending().getFirst(); byte[] original=DistributedCommandCodec.encode(pending.command()); assertEquals(RatisRocksDBDirect.RecoveryState.PENDING,x.direct.recoveryState());
            x.direct.close(); x.session.close(); x.closedDirect=true;
            try (var session=RatisDurableSessionClient.reopen(x.window,x.routed); var direct=new RatisRocksDBDirect(x.schema,x.routed,session,x.f.scheduler,x.journal)) {
                assertThrows(ExecutionException.class,() -> await(direct.write(75,key(2),value(3),RatisRocksDBDirect.WriteKind.MERGE,RequestType.RequestTypeId.MERGED,RequestContext.batch())));
                bytes((Buf)await(direct.resumeRetained(RequestContext.batch())),1,2); assertTrue(session.pending().isEmpty()); assertEquals(RatisRocksDBDirect.RecoveryState.NONE,direct.recoveryState());
                synchronized (x.sent) { var retry=x.sent.getLast(); assertArrayEquals(original,DistributedCommandCodec.encode(retry)); }
                bytes((Buf)await(direct.get(75,key(2),RequestType.RequestTypeId.CURRENT,RequestContext.batch())),1,2);
            }
        }
    }
    @Test void advancedSchemaRejectsUnsubmittedRetainedSuffixWithoutNewIdentityOrLostPrefix() throws Exception {
        try (var x=new Fixture(root.resolve("unsubmitted"))) {
            await(x.adapter.api(RequestContext.batch()).putAsync(0,75,key(2),value(1),RequestType.<Buf>none())); x.direct.close();
            var original=x.routed.directory(x.schema); var items=new ArrayList<DistributedCommand>();
            for (var key:List.of(key(2),key(200))) {
                var logical=RatisLogicalClient.plan(x.schema,List.of(RatisLogicalClient.Change.merge(75,key,value(2),Mutation.ReturnMode.CURRENT)),List.of());
                var route=original.route(logical.mutations().getFirst().columnId(),logical.mutations().getFirst().key());
                items.add(new DistributedCommand(route.groupId(),route.rangeEpoch(),x.schema.digest(),new OperationId(x.grant.sessionId(),x.grant.epoch(),0),logical.mutations()));
            }
            var digest=java.security.MessageDigest.getInstance("SHA-256"); digest.update(x.schema.digest()); digest.update(x.routed.sessionIdentityDigest());
            digest.update(java.nio.ByteBuffer.allocate(40).putLong(x.grant.sessionId().getMostSignificantBits()).putLong(x.grant.sessionId().getLeastSignificantBits()).putLong(x.grant.epoch()).putLong(x.grant.issuedAtMillis()).putLong(x.grant.expiresAtMillis()).array()); byte[] binding=digest.digest();
            try (var journal=new RatisDirectRequestJournal(x.journal,binding)) {
                journal.begin(new RatisDirectRequestJournal.Request(RatisRocksDBDirect.WriteKind.MERGE,RequestType.RequestTypeId.MERGED,true,items));
                var pending=x.session.prepareAt(original,items.getFirst().mutations(),List.of()); journal.bind(pending.command().operationId()); journal.result(x.session.execute(pending)); x.session.acknowledge(pending); journal.acknowledged();
            }
            assertTrue(x.session.pending().stream().allMatch(RatisDurableSessionClient.Pending::consumed)); x.session.close(); x.closedDirect=true;
            var operator=x.schema.column(75).operator(); x.install(x.schema,77,new UUID(310,77),"later-merge",operator,false); x.routed.refreshSchema(x.lastDefinition,x.lastActivation);
            int before=x.sent.size();
            try (var session=RatisDurableSessionClient.reopen(x.window,x.routed); var direct=new RatisRocksDBDirect(x.schema,x.routed,session,x.f.scheduler,x.journal)) {
                assertEquals(RatisRocksDBDirect.RecoveryState.PENDING,direct.recoveryState());
                var failure=assertThrows(ExecutionException.class,() -> await(direct.resumeRetained(RequestContext.batch())));
                assertEquals(RocksDBException.RocksDBErrorType.COMMIT_FAILED,assertInstanceOf(RocksDBException.class,failure.getCause()).getErrorUniqueId());
                assertTrue(failure.getCause().getMessage().contains("1 applied prefix items")); assertEquals(before,x.sent.size()); assertEquals(RatisRocksDBDirect.RecoveryState.NONE,direct.recoveryState());
            }
            try (var journal=new RatisDirectRequestJournal(x.journal,binding)) {
                assertEquals(CommandResult.Status.STALE_SCHEMA,journal.request().unsubmittedStatus); assertEquals(1,journal.request().cursor()); assertArrayEquals(new byte[]{1,2},journal.request().results.getFirst().value(0)); assertNull(journal.request().pending);
            }
            var encoded=x.schema.encodeLogicalKey(x.schema.column(75).columnId(),key(2)); assertArrayEquals(new byte[]{1,2},x.routed.read(x.schema.column(75).columnId(),encoded.physicalKey()));
            assertNull(x.routed.read(x.schema.column(75).columnId(),x.schema.encodeLogicalKey(x.schema.column(75).columnId(),key(200)).physicalKey()));
        }
    }

}
