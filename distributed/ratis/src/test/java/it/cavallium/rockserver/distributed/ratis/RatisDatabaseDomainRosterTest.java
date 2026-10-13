package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Actual native catalogue CAS with controlled transport; no servers or sockets. */
class RatisDatabaseDomainRosterTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    RatisCatalogue catalogue(RatisSessionEpochsTest outer,RatisSessionEpochsTest.Fixture fixture) { return new RatisCatalogue(fixture.client,outer.cluster,outer.database,outer.catalogue,1,new byte[32],outer.directoryColumn); }
    RatisDatabaseDomainRoster rosters(RatisSessionEpochsTest outer,RaftClient client) { return new RatisDatabaseDomainRoster(client,outer.cluster,outer.database,outer.catalogue,1,new byte[32],outer.directoryColumn); }
    RangeDirectory directory(RatisSessionEpochsTest outer,int n) {
        var initial=new RatisCatalogueGenesisTest().plan(outer).domains().getFirst().directory(); var r=initial.ranges().getFirst(); var range=new RangeDescriptor(new UUID(850,n),new UUID(851,n),1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,r.replicas());
        return new RangeDirectory(outer.cluster,outer.database,new UUID(852,n),initial.schemaEpoch(),initial.schemaDigest(),0,initial.columns(),0,List.of(range));
    }
    @Test void concurrentDomainAdditionsConflictAtomicallyAndNeverPublishAnUnrosteredDirectory() throws Exception {
        var outer=new RatisSessionEpochsTest(); try(var fixture=outer.new Fixture(root)) {
            var catalogue=catalogue(outer,fixture); var reader=rosters(outer,fixture.client); var first=directory(outer,1); assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(catalogue.prepare(outer.operation(0),null,first)).status()); var before=reader.read();
            var second=directory(outer,2); var third=directory(outer,3); var a=catalogue.prepare(outer.operation(1),null,second,before); var b=catalogue.prepare(outer.operation(2),null,third,before); assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(a).status()); assertEquals(CommandResult.Status.CONFLICT,catalogue.execute(b).status());
            assertEquals(Set.of(first.domainId(),second.domainId()),Set.copyOf(reader.read().domainIds())); assertNull(catalogue.read(third.domainId())); assertEquals(2,reader.readComplete().directories().size());
            assertEquals(CommandResult.Status.CONFLICT,catalogue.execute(catalogue.prepare(outer.operation(3),null,third)).status()); assertNull(catalogue.read(third.domainId()));
            var missing=a.mutations().subList(0,1); assertThrows(IllegalArgumentException.class,()->fixture.validate.accept(new DistributedCommand(a.groupId(),a.rangeEpoch(),a.schemaDigest(),outer.operation(4),missing,a.predicates().subList(0,4))));
        }
    }
    @Test void lostAppliedGenesisThenLaterAdditionReopensOriginalBitsAndSavedOutcome() throws Exception {
        var outer=new RatisSessionEpochsTest(); try(var fixture=outer.new Fixture(root)) {
            var helper=new RatisCatalogueGenesisTest(); var plan=helper.plan(outer); var after=DatabaseDomainRoster.initial(outer.cluster,outer.database,List.of(plan.domains().getFirst().directory().domainId())); Path journal=root.resolve("genesis.bin"); byte[] original;
            try(var genesis=new RatisCatalogueGenesis(journal,fixture.client,plan,plan.domains().getFirst().directory().domainId(),10,null,null,after)) { original=DistributedCommandCodec.encode(genesis.preparedCommand()); fixture.writeOverride=new byte[4]; assertThrows(IOException.class,genesis::execute); }
            fixture.writeOverride=null; var second=directory(outer,2); var catalogue=catalogue(outer,fixture); assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(catalogue.prepare(outer.operation(0),null,second,after)).status()); var extended=rosters(outer,fixture.client).read();
            try(var genesis=new RatisCatalogueGenesis(journal,fixture.client,plan,plan.domains().getFirst().directory().domainId(),10,null,null,after)) { assertArrayEquals(original,DistributedCommandCodec.encode(genesis.preparedCommand())); assertEquals(CommandResult.Status.SUCCESS,genesis.execute().status()); }
            assertEquals(extended,rosters(outer,fixture.client).read()); assertEquals(2,rosters(outer,fixture.client).readComplete().directories().size());
        }
    }
    @Test void advertisedIncompleteDomainBlocksCompleteViewUntilFrozenLaterPublication() throws Exception {
        var outer=new RatisSessionEpochsTest(); try(var fixture=outer.new Fixture(root)) {
            var helper=new RatisCatalogueGenesisTest(); var first=helper.plan(outer); var second=helper.secondDomain(outer); var after=DatabaseDomainRoster.initial(outer.cluster,outer.database,List.of(first.domains().getFirst().directory().domainId(),second.domains().getFirst().directory().domainId())); var reader=rosters(outer,fixture.client);
            try(var genesis=new RatisCatalogueGenesis(root.resolve("first.bin"),fixture.client,first,first.domains().getFirst().directory().domainId(),10,null,null,after)) { assertEquals(CommandResult.Status.SUCCESS,genesis.execute().status()); } assertEquals(2,reader.read().domainIds().size()); assertThrows(IOException.class,reader::readComplete);
            try(var genesis=new RatisCatalogueGenesis(root.resolve("second.bin"),fixture.client,second,second.domains().getFirst().directory().domainId(),0,fixture.epochs.read(),after,after)) { assertEquals(1,genesis.preparedCommand().mutations().size()); assertEquals(CommandResult.Status.SUCCESS,genesis.execute().status()); } assertEquals(2,reader.readComplete().directories().size());
        }
    }
    @Test void staleForeignShrinkingOrStandaloneRosterCannotHideIssuedDomains() throws Exception {
        var outer=new RatisSessionEpochsTest(); try(var fixture=outer.new Fixture(root)) {
            var catalogue=catalogue(outer,fixture); var first=directory(outer,1); catalogue.execute(catalogue.prepare(outer.operation(0),null,first)); var reader=rosters(outer,fixture.client); var before=reader.read(); var second=directory(outer,2); var valid=catalogue.prepare(outer.operation(1),null,second,before);
            for(var roster:List.of(new DatabaseDomainRoster(outer.cluster,outer.database,1,List.of(second.domainId())),new DatabaseDomainRoster(new UUID(99,99),outer.database,1,List.of(first.domainId(),second.domainId())),new DatabaseDomainRoster(outer.cluster,outer.database,0,List.of(first.domainId(),second.domainId())))) {
                var mutations=new ArrayList<>(valid.mutations()); mutations.set(1,new Mutation(Mutation.Kind.PUT,outer.directoryColumn,RatisDatabaseDomainRoster.key(outer.database),roster.encode())); assertThrows(IllegalArgumentException.class,()->fixture.validate.accept(new DistributedCommand(valid.groupId(),valid.rangeEpoch(),valid.schemaDigest(),valid.operationId(),mutations,valid.predicates())));
            }
            assertThrows(IllegalArgumentException.class,()->fixture.validate.accept(new DistributedCommand(valid.groupId(),valid.rangeEpoch(),valid.schemaDigest(),valid.operationId(),List.of(valid.mutations().get(1)),List.of(valid.predicates().get(4)))));
            catalogue.execute(valid); assertThrows(IOException.class,()->reader.requireUnchanged(before)); assertEquals(2,reader.readComplete().directories().size());
        }
    }    @Test void completeObservationRechecksRosterAndDetectsDomainCreationBetweenDirectoryReads() throws Exception {
        var outer=new RatisSessionEpochsTest(); try(var fixture=outer.new Fixture(root)) {
            var catalogue=catalogue(outer,fixture); var first=directory(outer,1); var second=directory(outer,2); catalogue.execute(catalogue.prepare(outer.operation(0),null,first)); var before=rosters(outer,fixture.client).read(); var once=new java.util.concurrent.atomic.AtomicBoolean(true);
            var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args)->{
                assertEquals("sendReadOnly",method.getName()); var message=(Message)args[0]; var packet=ScopedTransactionRpc.parse(message.getContent().toByteArray(),ScopedTransactionRpc.MAX_REQUEST_BYTES); var in=ByteBuffer.wrap(packet.body()); ScopedTransactionRpc.uuid(in); var key=ScopedTransactionRpc.blob(in,65536,false);
                if(Arrays.equals(key,RatisSessionEpochsTest.bytes(first.domainId())) && once.compareAndSet(true,false)) assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(catalogue.prepare(outer.operation(1),null,second,before)).status());
                return fixture.client.io().sendReadOnly(message);
            });
            var client=(RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args)->switch(method.getName()){case "getGroupId"->fixture.client.getGroupId();case "io"->io;default->throw new AssertionError(method);});
            assertThrows(IOException.class,()->rosters(outer,client).readComplete()); assertFalse(once.get()); assertEquals(2,rosters(outer,fixture.client).readComplete().directories().size());
        }
    }

}
