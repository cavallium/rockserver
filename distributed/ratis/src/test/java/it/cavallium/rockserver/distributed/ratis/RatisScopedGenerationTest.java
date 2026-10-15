package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisRocksDBAdapterTest.*;

/** Native snapshot ownership across a same-schema endpoint relocation; transport dispatch is controlled. */
class RatisScopedGenerationTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void newOpenUsesRelocatedParticipantsWhileOldHandleAndColdBindingsKeepTheirOriginalPlacement() throws Exception {
        try(var f=new RatisAtomicClientTest.Fixture(root.resolve("native"),true);var replicas=new RatisSchemaReplicaClient(1)) {
            var captures=new CopyOnWriteArrayList<RangeDescriptor>();var base=f.transport();
            var transport=new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID column,byte[] key)throws IOException{return base.read(route,column,key);}
                public CommandResult command(RangeDescriptor route,DistributedCommand command)throws IOException{return base.command(route,command);}
                public byte[] transaction(RangeDescriptor route,byte[] request,boolean fresh)throws IOException{return base.transaction(route,request,fresh);}
                public byte[] participant(RangeDescriptor route,byte[] record)throws IOException{return base.participant(route,record);}
                public RangeStateView stateView(RangeDescriptor route)throws IOException{return base.stateView(route);}
                public CompletableFuture<byte[]> snapshot(RangeDescriptor route,ReplicaDescriptor owner,byte[] request)throws IOException {
                    if(ScopedTransactionRpc.kind(request,ScopedTransactionRpc.MAX_REQUEST_BYTES)==ScopedTransactionRpc.SNAPSHOT_CAPTURE)captures.add(route);
                    return base.snapshot(route,owner,request);
                }
            };
            try(var routed=new RatisRoutedClient(f.directory,transport)) {
                var binding=new RatisNodePlan.Catalogue(f.outer.catalogue,1,new byte[32],f.outer.directoryColumn,f.outer.sessions,f.directory.ranges().getFirst().replicas());
                var schemas=new RatisSchemaCatalogue(f.catalogue.client,f.outer.cluster,f.outer.database,f.outer.catalogue,1,new byte[32],f.outer.directoryColumn);
                assertEquals(CommandResult.Status.SUCCESS,schemas.execute(schemas.prepare(f.outer.operation(3),f.schema)).status());
                var directories=new RatisCatalogue(f.catalogue.client,f.outer.cluster,f.outer.database,f.outer.catalogue,1,new byte[32],f.outer.directoryColumn);
                assertEquals(CommandResult.Status.SUCCESS,directories.execute(directories.prepare(f.outer.operation(4),null,f.directory)).status());
                var allocations=new Allocations(f);var created=new CopyOnWriteArrayList<RangeDirectory>();
                RatisRocksDBConnection.GenerationFactory factory=(schema,directory,path,ids)->{
                    created.add(directory);
                    var transactions=RatisTransactionClient.journalOwned(schema,directory,f.snapshots,f.homes,routed,f.scheduler,RatisTransactionClient.Limits.defaults(),RatisJournalAccess.standalone());
                    var snapshots=RatisSnapshotClient.journalOwned(schema,directory,f.snapshots,routed,f.scheduler,RatisSnapshotClient.Limits.defaults(),RatisSnapshotCaptureRequest.Mode.READ_ONLY,RatisJournalAccess.standalone());
                    var plan=snapshots.plan(new RatisSnapshotClient.Identity(new UUID(900,1),new OperationId(f.session,1,0),new OperationId(f.session,1,1)),
                            new TransactionScope(List.of(TransactionScope.Span.range(schema.column(71).columnId(),null,null))));
                    assertEquals(Set.copyOf(directory.ranges()),Set.copyOf(plan.routes()),"Cold historical clients keep their explicitly frozen placement");
                    var adapter=new RatisRocksDBAdapter(schema,directory,transactions,allocations,null,null,ids);
                    return new RatisRocksDBConnection.Generation(adapter,null,null,transactions,snapshots,List.of());
                };
                var relocated=new RangeDirectory(f.directory.clusterId(),f.directory.databaseId(),f.directory.domainId(),f.directory.schemaEpoch(),f.directory.schemaDigest(),f.directory.generation()+1,f.directory.columns(),f.directory.routingPrefixBytes(),f.directory.ranges().stream().map(range->new RangeDescriptor(range.rangeId(),range.groupId(),range.rangeEpoch(),range.configurationVersion(),range.lowerInclusive(),range.upperExclusive(),range.topologyState(),range.replicas().stream().map(peer->new ReplicaDescriptor(peer.nodeId(),peer.nodeIncarnation(),peer.replicaId(),peer.peerId(),"relocated-"+peer.replicaId()+":12345",peer.role())).toList())).toList());
                Path owned=root.resolve("connection");var declaration=scope(71,72);var uri=URI.create("ratis://controlled/placement");
                seed(f,71,key(2),value(1),1900);
                try(var connection=new RatisRocksDBConnection(uri,owned,Map.of(f.schema.domainId(),routed),(name,shape)->f.schema.domainId(),f.scheduler,binding,f.catalogue.client,replicas,null,null,factory,RatisRocksDBConnection.Limits.defaults(),List.of(),RatisJournalAccess.standalone())) {
                    var api=connection.getAsyncApi(CONTEXT);long original=await(api.openTransactionAsync(declaration,Duration.ofSeconds(30)));
                    assertEquals(Set.copyOf(f.directory.ranges()),Set.copyOf(captures));captures.clear();
                    assertEquals(CommandResult.Status.SUCCESS,directories.execute(directories.prepare(f.outer.operation(5),f.directory,relocated)).status());
                    routed.refresh(relocated);seed(f,71,key(2),value(2),1901);
                    long current=await(api.openTransactionAsync(declaration,Duration.ofSeconds(30)));
                    assertEquals(Set.copyOf(relocated.ranges()),Set.copyOf(captures));assertEquals(2,created.size());assertNotEquals(original,current);
                    bytes(1,await(api.getAsync(original,71,key(2),RequestType.<it.cavallium.buffer.Buf>current())));
                    bytes(2,await(api.getAsync(current,71,key(2),RequestType.<it.cavallium.buffer.Buf>current())));
                    assertTrue(await(api.closeTransactionAsync(original,false)));assertTrue(await(api.closeTransactionAsync(current,false)));
                    for(var store:f.stores.values())assertEquals(0,store.activeSnapshotPins());
                }
                var oldBinding=owned.resolve("generations").resolve(RatisGenerationRegistry.key(f.schema,f.directory)).resolve("binding");
                var newBinding=owned.resolve("generations").resolve(RatisGenerationRegistry.key(f.schema,relocated)).resolve("binding");
                byte[] originalBytes=Files.readAllBytes(oldBinding),currentBytes=Files.readAllBytes(newBinding);created.clear();captures.clear();
                try(var reopened=new RatisRocksDBConnection(uri,owned,Map.of(f.schema.domainId(),routed),(name,shape)->f.schema.domainId(),f.scheduler,binding,f.catalogue.client,replicas,null,null,factory,RatisRocksDBConnection.Limits.defaults(),List.of(),RatisJournalAccess.standalone())) {
                    assertEquals(Set.of(RatisGenerationRegistry.key(f.schema,f.directory),RatisGenerationRegistry.key(f.schema,relocated)),created.stream().map(directory->RatisGenerationRegistry.key(f.schema,directory)).collect(java.util.stream.Collectors.toSet()));
                    long current=await(reopened.getAsyncApi(CONTEXT).openTransactionAsync(declaration,Duration.ofSeconds(30)));
                    assertEquals(Set.copyOf(relocated.ranges()),Set.copyOf(captures));assertTrue(await(reopened.getAsyncApi(CONTEXT).closeTransactionAsync(current,false)));
                    assertArrayEquals(originalBytes,Files.readAllBytes(oldBinding));assertArrayEquals(currentBytes,Files.readAllBytes(newBinding));
                }
            }
        }
    }
}
