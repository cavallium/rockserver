package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Complete connection generation/root recovery across an actual native split, with controlled transport only. */
class RatisTopologyConnectionRecoveryTest {
    @TempDir(factory=RatisDiskTestDirectory.class)Path root;
    static <T>T await(CompletableFuture<T> value)throws Exception{return value.get(20,TimeUnit.SECONDS);}
    static RemoteMutationToken write(RatisRocksDBConnection connection,RemoteMutationOwner owner,int key,int value)throws Exception {
        var api=connection.getRemoteMutationApi();var token=await(api.preparePoint(owner,UUID.randomUUID(),owner.nextRequestSequence(),1,List.of(RatisAtomicClientTest.key(key)),List.of(RatisAtomicClientTest.value(value)),Mutation.Kind.PUT,RequestType.RequestTypeId.NOTHING,false,RequestContext.batch()));var status=await(api.execute(token,RequestContext.batch()));assertEquals(CommandResult.Status.SUCCESS,status.outcome());await(api.acknowledge(token,status.resultDigest(),RequestContext.batch()));return token;
    }
    static RemoteMutationOwner owner(RatisRocksDBConnection connection)throws Exception{return await(connection.getRemoteMutationApi().owners(1,RequestContext.batch())).stream().filter(value->value.kind()==RemoteMutationToken.Kind.POINT).findFirst().orElseThrow();}
    @Test void connectionColdReopensOriginalGenerationAndRenewedOwnersAfterActualSplit()throws Exception {
        var helper=new RatisOperationRerouteCoordinatorTest();helper.root=root.resolve("native");
        try(var f=helper.new Fixture(false,true);var budget=new RatisRequestBudget(new RemoteMutationOwnerLimits(1,8,8,8,4,128L<<20,512L<<20,512,16L<<20,8L<<20,1));var replicas=new RatisSchemaReplicaClient(1)) {
            var schema=f.source.schema;var initial=f.source.directory;var binding=new RatisNodePlan.Catalogue(f.outer.catalogue,1,new byte[32],f.outer.directoryColumn,f.outer.sessions,initial.ranges().getFirst().replicas());
            var schemas=new RatisSchemaCatalogue(f.catalogue.client,f.outer.cluster,f.outer.database,f.outer.catalogue,1,new byte[32],f.outer.directoryColumn);assertEquals(CommandResult.Status.SUCCESS,schemas.execute(schemas.prepare(f.outer.operation(900),schema)).status());
            var directories=f.catalogue.epochs.catalogue(f.outer.directoryColumn);var bootstrap=new RangeDirectory(initial.clusterId(),initial.databaseId(),initial.domainId(),initial.schemaEpoch(),initial.schemaDigest(),0,initial.columns(),initial.routingPrefixBytes(),initial.ranges());assertEquals(CommandResult.Status.SUCCESS,directories.execute(directories.prepare(f.outer.operation(901),null,bootstrap)).status());assertEquals(CommandResult.Status.SUCCESS,directories.execute(directories.prepare(f.outer.operation(903),bootstrap,initial)).status());
            var sessions=new RatisRocksDBConnection.SessionFactory(List.of(f.data,f.homeClient),(path,selected,plan)->RatisBackendSessions.window(path,f.data,f.catalogue.epochs,1000,RatisSessionClient.Limits.defaults(),plan.targets(), budget));
            var factory=RatisRocksDBConnection.durableGenerationFactory(f.data,f.scheduler,new RatisSnapshotFences(f.homeClient,f.homeColumn),f.homes,f.catalogue.epochs,directories,header->{var definition=TopologyChange.Definition.decode(f.point(RatisTopologyCatalogue.definitionKey(header.definitionDigest())));header.requireDefinition(definition);return definition.sourceDirectory();},context->CompletableFuture.failedFuture(new IOException("This point connection workflow does not request snapshot captures")),sessions,List.of(),budget);
            Path owned=root.resolve("connection");RemoteMutationToken original,renewedToken;RemoteMutationOwner renewed;
            try(var connection=new RatisRocksDBConnection(URI.create("ratis://controlled/split-connection"),owned,Map.of(schema.domainId(),f.data),(name,shape)->schema.domainId(),f.scheduler,binding,f.catalogue.client,replicas,null,null,factory,RatisRocksDBConnection.Limits.defaults(),List.of(), budget)) {
                var first=owner(connection);original=write(connection,first,10,7);assertArrayEquals(new byte[]{7},f.data.read(f.source.column,new byte[]{10}));f.activateSplit();assertArrayEquals(f.source.target.payloadDigest(),f.data.directory().payloadDigest());assertArrayEquals(new byte[]{7},f.data.read(f.source.column,new byte[]{10}));
                var before=f.catalogue.epochs.read();var grant=before.grants().stream().filter(value->value.sessionId().equals(first.ownerId())).findFirst().orElseThrow();var expired=before.advanceTime(grant.expiresAtMillis());assertEquals(CommandResult.Status.SUCCESS,f.catalogue.epochs.execute(f.catalogue.epochs.prepare(f.outer.operation(902),before,expired)).status());renewed=owner(connection);assertNotEquals(first.ownerId(),renewed.ownerId());assertArrayEquals(f.source.target.payloadDigest(),renewed.directoryDigest());renewedToken=write(connection,renewed,200,8);assertEquals(RemoteMutationStatus.State.ACKNOWLEDGED,await(connection.getRemoteMutationApi().status(original,RequestContext.batch())).state());
            }
            Path bindingFile=owned.resolve("generations").resolve(HexFormat.of().formatHex(schema.digest())).resolve("binding");byte[] originalBinding=Files.readAllBytes(bindingFile);long issued=f.catalogue.epochs.read().nextEpoch();
            byte[] schemaBytes=DistributedSchemaCodec.encode(schema),changedDirectory=RangeDirectoryCodec.encode(f.source.target);SessionJournalFiles.write(bindingFile,java.nio.ByteBuffer.allocate(8+schemaBytes.length+changedDirectory.length).putInt(schemaBytes.length).put(schemaBytes).putInt(changedDirectory.length).put(changedDirectory).array(),8*1024*1024);
            try{assertThrows(IOException.class,()->new RatisRocksDBConnection(URI.create("ratis://controlled/split-connection"),owned,Map.of(schema.domainId(),f.data),(name,shape)->schema.domainId(),f.scheduler,binding,f.catalogue.client,replicas,null,null,factory,RatisRocksDBConnection.Limits.defaults(),List.of(), budget));assertEquals(issued,f.catalogue.epochs.read().nextEpoch(),"Changed generation binding must fail before another native grant");}finally{Files.write(bindingFile,originalBinding);SessionJournalFiles.force(bindingFile);SessionJournalFiles.force(bindingFile.getParent());}

            try(var cold=new RatisRocksDBConnection(URI.create("ratis://controlled/split-connection"),owned,Map.of(schema.domainId(),f.data),(name,shape)->schema.domainId(),f.scheduler,binding,f.catalogue.client,replicas,null,null,factory,RatisRocksDBConnection.Limits.defaults(),List.of(), budget)) {
                assertArrayEquals(originalBinding,Files.readAllBytes(bindingFile));assertEquals(issued,f.catalogue.epochs.read().nextEpoch(),"Cold generation/root recovery must not allocate replacement grants");var api=cold.getRemoteMutationApi();assertEquals(RemoteMutationStatus.State.ACKNOWLEDGED,await(api.status(original,RequestContext.batch())).state());assertEquals(RemoteMutationStatus.State.ACKNOWLEDGED,await(api.status(renewedToken,RequestContext.batch())).state());var current=owner(cold);assertEquals(renewed.ownerId(),current.ownerId());write(cold,current,201,9);assertArrayEquals(new byte[]{9},f.data.read(f.source.column,new byte[]{(byte)201}));assertEquals(issued,f.catalogue.epochs.read().nextEpoch());
            }
        }
    }
}
