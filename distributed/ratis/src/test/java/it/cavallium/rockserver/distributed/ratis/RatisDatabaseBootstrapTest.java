package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.buffer.Buf;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/** Full logical create-to-session-write workflow on native stores; no network/quorum/process acceptance. */
class RatisDatabaseBootstrapTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    RatisNodePlan plan(RatisSessionPropagationTest.Fixture f) {
        var node=f.directory.ranges().getFirst().replicas().getFirst();
        var catalogue=new ReplicaDescriptor(node.nodeId(),node.nodeIncarnation(),new UUID(8,8),node.peerId(),node.address(),ReplicaDescriptor.Role.VOTER);
        return new RatisNodePlan(node.nodeId(), node.nodeIncarnation(), List.of(new RatisNodePlan.Domain(f.directory, Map.of(f.outer.data,"data"), Set.of())), new RatisNodePlan.Catalogue(f.outer.catalogue,1,new byte[32],f.outer.directoryColumn,f.outer.sessions,List.of(catalogue)), List.of(), List.of(), Map.of(), 2);
    }
    RatisDatabaseBootstrap bootstrap(Path path,RatisSessionPropagationTest.Fixture f,RatisNodePlan plan) throws IOException {
        return new RatisDatabaseBootstrap(path,List.of(new RatisDatabaseBootstrap.Domain(plan,f.routed,null,null)),f.catalogue.client,null,null,10,() -> 20L,2);
    }
    void ready(RatisDatabaseBootstrap bootstrap) throws Exception {
        for (int i=0;i<20;i++) if (bootstrap.step(1).phase()==RatisDatabaseBootstrap.Phase.READY) return;
        fail("Bootstrap did not complete bounded stages");
    }
    @Test void atomicGenesisClockPropagationFreshReadinessThenIssuedSessionWritesAndReads() throws Exception {
        try (var f=new RatisSessionPropagationTest.Fixture(root,false)) {
            var plan=plan(f); Path journals=Files.createDirectory(root.resolve("bootstrap"));
            try (var boot=bootstrap(journals,f,plan)) {
                assertEquals(RatisDatabaseBootstrap.Phase.PREFLIGHT,boot.progress().phase());
                assertEquals(RatisDatabaseBootstrap.Phase.PREFLIGHT,boot.step(1).phase()); assertEquals(1,f.metadataReads); assertEquals(0,f.catalogue.next.get());
                assertEquals(RatisDatabaseBootstrap.Phase.GENESIS,boot.step(1).phase()); assertEquals(0,f.catalogue.next.get());
                assertEquals(RatisDatabaseBootstrap.Phase.CLOCK,boot.step(1).phase());
                assertNotNull(f.catalogue.epochs.read()); ready(boot);
                assertEquals(20,boot.progress().target().logicalTimeMillis()); assertEquals(0,f.customerReads);
                assertThrows(IOException.class,() -> bootstrap(journals,f,plan));
            }
            var ledger=f.catalogue.epochs.read(); var grant=f.catalogue.epochs.prepareGrant(f.outer.operation(77),ledger,f.outer.a,100);
            assertEquals(CommandResult.Status.SUCCESS,f.catalogue.epochs.execute(grant.command()).status());
            var issued=f.catalogue.epochs.read(); var session=new RatisSessionClient(f.routed,issued,grant.grant(),new RatisSessionClient.Limits(2,1024,1024));
            var pending=session.prepare(List.of(new Mutation(Mutation.Kind.PUT,f.outer.data,new byte[]{7},new byte[]{9})));
            var control=session.prepareBinding(f.catalogue.epochs,new OperationId(f.outer.maintenance,issued.minimumEpoch(),100),pending,-1);
            f.routed.execute(control); assertEquals(CommandResult.Status.SUCCESS,session.execute(pending).status()); session.acknowledge(pending);
            assertArrayEquals(new byte[]{9},f.routed.read(f.outer.data,new byte[]{7})); assertEquals(0,session.outstanding());
            for (var store:f.stores.values()) assertEquals(0,store.activeSnapshotPins());
        }
    }
    @Test void preflightFailuresRemainFairAndReopenRechecksRatherThanTrustingOldReadiness() throws Exception {
        try (var f=new RatisSessionPropagationTest.Fixture(root,false)) {
            var plan=plan(f); Path journals=Files.createDirectory(root.resolve("restart")); var first=f.groups.getFirst(); f.failed.add(first);
            try (var boot=bootstrap(journals,f,plan)) {
                assertEquals(1,boot.step(1).failures().size()); assertEquals(0,f.catalogue.next.get());
                assertEquals(1,boot.step(1).checked()); assertEquals(0,f.catalogue.next.get());
                f.failed.clear(); ready(boot);
            }
            long submitted=f.catalogue.next.get(); f.failed.add(first);
            try (var boot=bootstrap(journals,f,plan)) {
                assertEquals(RatisDatabaseBootstrap.Phase.PREFLIGHT,boot.progress().phase());
                assertEquals(1,boot.step(1).failures().size()); assertEquals(submitted,f.catalogue.next.get());
                f.failed.clear(); ready(boot); assertEquals(submitted,f.catalogue.next.get(),"Cached genesis and unchanged clock do not replay creation mutations");
                assertEquals(RatisDatabaseBootstrap.Phase.VERIFY,boot.step(1).phase(),"Repeated checks do not return permanent cached READY");
            }
        }
    }
    @Test void conflictingGenesisNeverProceedsToClockOrDataAuthorityAndPublishedScopeCannotRebase() throws Exception {
        try (var f=new RatisSessionPropagationTest.Fixture(root,false)) {
            var plan=plan(f); var ledger=SessionEpochLedger.initial(f.outer.cluster,f.outer.database,10);
            f.catalogue.epochs.execute(f.catalogue.epochs.prepare(f.outer.operation(0),null,ledger));
            Path journals=Files.createDirectory(root.resolve("conflict"));
            try (var boot=bootstrap(journals,f,plan)) {
                boot.step(2); assertEquals(RatisDatabaseBootstrap.Phase.CONFLICT,boot.step(2).phase()); long submitted=f.catalogue.next.get();
                assertEquals(RatisDatabaseBootstrap.Phase.CONFLICT,boot.step(2).phase()); assertEquals(submitted,f.catalogue.next.get()); assertEquals(0,f.submissions);
            }
        }
    }
    @Test void failedPollAfterReadyInvalidatesReadinessBeforeCatalogueOrScopeGuards() throws Exception {
        try (var f=new RatisSessionPropagationTest.Fixture(root,false)) {
            var plan=plan(f); Path journals=Files.createDirectory(root.resolve("ready-poll"));
            try (var boot=bootstrap(journals,f,plan)) {
                ready(boot); f.catalogue.unavailable=true;
                assertThrows(IOException.class,() -> boot.step(1)); assertEquals(RatisDatabaseBootstrap.Phase.VERIFY,boot.progress().phase());
                f.catalogue.unavailable=false; ready(boot);
                var catalogue=new RatisCatalogue(f.catalogue.client,f.outer.cluster,f.outer.database,f.outer.catalogue,1,new byte[32],f.outer.directoryColumn);
                catalogue.execute(catalogue.prepare(f.outer.operation(89),f.directory,f.updated(1,1)));
                assertThrows(IOException.class,() -> boot.step(1)); assertEquals(RatisDatabaseBootstrap.Phase.VERIFY,boot.progress().phase());
            } finally { f.catalogue.unavailable=false; }
        }
    }
    @Test void propagationFailuresReachCallerAndOtherRangesContinueUnderTheVisitBudget() throws Exception {
        try (var f=new RatisSessionPropagationTest.Fixture(root,false)) {
            var plan=plan(f); Path journals=Files.createDirectory(root.resolve("propagation-error"));
            try (var boot=bootstrap(journals,f,plan)) {
                boot.step(2); boot.step(1); boot.step(1);
                assertEquals(RatisDatabaseBootstrap.Phase.PROPAGATE,boot.progress().phase());
                var first=f.groups.getFirst(); f.failed.add(first);
                var blocked=boot.step(1); assertEquals(RatisDatabaseBootstrap.Phase.PROPAGATE,blocked.phase());
                assertTrue(blocked.failures().containsKey(first)); assertEquals(1,f.submissions);
                assertTrue(boot.step(1).failures().isEmpty()); assertEquals(2,f.submissions);
                f.failed.clear(); ready(boot); assertEquals(0,f.customerReads);
            }
        }
    }
    @Test void metadataPublicationChangesBlockOldScopeWithoutResettingIt() throws Exception {
        try (var f=new RatisSessionPropagationTest.Fixture(root,false)) {
            var plan=plan(f); Path journals=Files.createDirectory(root.resolve("scope"));
            try (var boot=bootstrap(journals,f,plan)) {
                boot.step(2); boot.step(1); assertEquals(RatisDatabaseBootstrap.Phase.CLOCK,boot.progress().phase());
                var next=f.updated(1,1);
                var catalogue=new RatisCatalogue(f.catalogue.client,f.outer.cluster,f.outer.database,f.outer.catalogue,1,new byte[32],f.outer.directoryColumn);
                catalogue.execute(catalogue.prepare(f.outer.operation(88),f.directory,next)); long submitted=f.catalogue.next.get();
                assertThrows(IOException.class,() -> boot.step(1)); assertEquals(submitted,f.catalogue.next.get()); assertEquals(0,f.submissions);
                assertEquals(RatisDatabaseBootstrap.Phase.CLOCK,boot.progress().phase());
            }
        }
    }
    DistributedSchema schema() {
        var outer=new RatisSessionEpochsTest();
        return new DistributedSchema(outer.cluster,outer.database,new UUID(6,1),1,0,List.of(
                new DistributedSchema.Column(71,outer.data,"data",ColumnSchema.of(IntArrayList.of(1),new ObjectArrayList<>(),true),null)));
    }
    RatisDatabaseBootstrap bootstrap(Path path,RatisSessionPropagationTest.Fixture f,DistributedSchema schema) throws IOException {
        return new RatisDatabaseBootstrap(path,List.of(new RatisDatabaseBootstrap.Domain(plan(f),f.routed,schema,null)),f.catalogue.client,null,null,10,() -> 20L,2);
    }
    RatisSchemaCatalogue schemas(RatisSessionPropagationTest.Fixture f) {
        return new RatisSchemaCatalogue(f.catalogue.client,f.outer.cluster,f.outer.database,f.outer.catalogue,1,new byte[32],f.outer.directoryColumn);
    }
    @Test void publishedSchemaBootstrapReopenAndLogicalSessionWorkflow() throws Exception {
        var schema=schema();
        try (var f=new RatisSessionPropagationTest.Fixture(root,false,schema.digest())) {
            Path journals=Files.createDirectory(root.resolve("schema-bootstrap"));
            try (var boot=bootstrap(journals,f,schema)) {
                assertEquals(RatisDatabaseBootstrap.Phase.SCHEMA,boot.step(2).phase()); assertEquals(0,f.catalogue.next.get());
                assertNull(schemas(f).read(schema.digest()));
                assertEquals(RatisDatabaseBootstrap.Phase.GENESIS,boot.step(1).phase()); assertEquals(1,f.catalogue.next.get());
                assertArrayEquals(DistributedSchemaCodec.encode(schema),DistributedSchemaCodec.encode(schemas(f).read(schema.digest())));
                assertNull(f.catalogue.epochs.read(),"Schema publication alone does not create database/session authority"); ready(boot);
            }
            long submitted=f.catalogue.next.get();
            try (var boot=bootstrap(journals,f,schema)) {
                assertEquals(RatisDatabaseBootstrap.Phase.PREFLIGHT,boot.progress().phase()); ready(boot);
                assertEquals(submitted,f.catalogue.next.get(),"Recovered publication/genesis do not allocate new mutation identities");
            }
            var ledger=f.catalogue.epochs.read(); var issuance=f.catalogue.epochs.prepareGrant(f.outer.operation(77),ledger,f.outer.a,100);
            f.catalogue.epochs.execute(issuance.command()); ledger=f.catalogue.epochs.read();
            var logical=new RatisLogicalClient(schemas(f).read(schema.digest()),f.routed,ledger,issuance.grant(),RatisSessionClient.Limits.defaults());
            var key=new Keys(Buf.wrap(new byte[]{7})); var pending=logical.preparePut(logical.getColumnId("data"),key,Buf.wrap(new byte[]{9}),Mutation.ReturnMode.NONE);
            f.routed.execute(logical.prepareBinding(f.catalogue.epochs,new OperationId(f.outer.maintenance,ledger.minimumEpoch(),100),pending,-1));
            int reads=f.catalogue.reads.get(); assertEquals(CommandResult.Status.SUCCESS,logical.execute(pending).status()); logical.acknowledge(pending);
            assertArrayEquals(new byte[]{9},logical.get(71,key).toByteArray()); assertEquals(reads,f.catalogue.reads.get());
        }
    }
    @Test void laterDomainSchemaBootstrapUsesFrozenGlobalCounterAndPreservesHighWaterAfterReopen() throws Exception {
        var schema=schema();
        try (var f=new RatisSessionPropagationTest.Fixture(root,false,schema.digest())) {
            var other=new DistributedSchema(schema.clusterId(),schema.databaseId(),new UUID(91,1),1,0,List.of(
                    new DistributedSchema.Column(70,new UUID(91,2),"other",schema.columns().getFirst().schema(),null)));
            assertEquals(CommandResult.Status.SUCCESS,schemas(f).execute(schemas(f).prepare(f.outer.operation(99),other)).status());
            var allocation=new RatisColumnAllocation(f.catalogue.client,f.outer.catalogue,1,new byte[32],f.outer.directoryColumn);
            byte[] witness=allocation.readCounter(); assertEquals(70,RatisColumnAllocation.counter(witness));
            Path journals=Files.createDirectory(root.resolve("later-domain")); byte[] frozen;
            try (var boot=new RatisDatabaseBootstrap(journals,List.of(new RatisDatabaseBootstrap.Domain(plan(f),f.routed,schema,witness)),f.catalogue.client,null,null,10,() -> 20L,2)) {
                assertEquals(RatisDatabaseBootstrap.Phase.SCHEMA,boot.step(2).phase());
                f.catalogue.writeOverride=new byte[4]; assertThrows(IOException.class,() -> boot.step(1));
                assertEquals(RatisDatabaseBootstrap.Phase.SCHEMA,boot.progress().phase()); frozen=Files.readAllBytes(journals.resolve("domains").resolve(schema.domainId().toString()).resolve("schema.bin"));
                assertEquals(71,RatisColumnAllocation.counter(allocation.readCounter())); assertNotNull(schemas(f).read(schema.digest()));
            } finally { f.catalogue.writeOverride=null; }
            assertEquals(CommandResult.Status.SUCCESS,allocation.execute(allocation.prepare(f.outer.operation(100),new UUID(91,3),other.domainId(),new UUID(91,4),allocation.readCounter())).status());
            assertEquals(72,RatisColumnAllocation.counter(allocation.readCounter()));
            assertThrows(IllegalArgumentException.class,() -> new RatisDatabaseBootstrap(journals,List.of(new RatisDatabaseBootstrap.Domain(plan(f),f.routed,schema,allocation.readCounter())),f.catalogue.client,null,null,10,() -> 20L,2),"Current counter cannot replace the journal's original witness");
            try (var boot=new RatisDatabaseBootstrap(journals,List.of(new RatisDatabaseBootstrap.Domain(plan(f),f.routed,schema,witness)),f.catalogue.client,null,null,10,() -> 20L,2)) {
                assertArrayEquals(frozen,Files.readAllBytes(journals.resolve("domains").resolve(schema.domainId().toString()).resolve("schema.bin")),"Reopen preserves the original unknown command");
                ready(boot); assertEquals(72,RatisColumnAllocation.counter(allocation.readCounter()));
                assertArrayEquals(DistributedSchemaCodec.encode(other),DistributedSchemaCodec.encode(schemas(f).read(other.digest())));
            }
        }
    }
    @Test void identicalSchemaPublicationConflictRequiresFreshContentVerificationBeforeGenesis() throws Exception {
        var schema=schema();
        try (var f=new RatisSessionPropagationTest.Fixture(root,false,schema.digest())) {
            schemas(f).execute(schemas(f).prepare(f.outer.operation(99),schema));
            Path journals=Files.createDirectory(root.resolve("existing-schema"));
            try (var boot=bootstrap(journals,f,schema)) {
                boot.step(2); f.catalogue.missingSchema=true;
                assertThrows(IOException.class,() -> boot.step(1)); assertEquals(RatisDatabaseBootstrap.Phase.SCHEMA,boot.progress().phase());
                assertNull(f.catalogue.epochs.read()); long submitted=f.catalogue.next.get(); f.catalogue.missingSchema=false;
                assertEquals(RatisDatabaseBootstrap.Phase.GENESIS,boot.step(1).phase()); assertEquals(submitted,f.catalogue.next.get());
                f.catalogue.missingSchema=true; assertThrows(IOException.class,() -> boot.step(1));
                assertEquals(RatisDatabaseBootstrap.Phase.GENESIS,boot.progress().phase()); assertEquals(submitted,f.catalogue.next.get());
                f.catalogue.missingSchema=false; ready(boot);
            } finally { f.catalogue.missingSchema=false; }
        }
    }
    @Test void failedSchemaPollInvalidatesReadyAndSchemaMismatchNeverCreatesIntent() throws Exception {
        var schema=schema();
        try (var f=new RatisSessionPropagationTest.Fixture(root,false,schema.digest())) {
            Path journals=Files.createDirectory(root.resolve("schema-poll"));
            var wrong=new DistributedSchema(schema.clusterId(),schema.databaseId(),schema.domainId(),2,0,schema.columns());
            assertThrows(IllegalArgumentException.class,() -> bootstrap(journals,f,wrong)); try (var files=Files.list(journals)) { assertEquals(0,files.count()); }
            try (var boot=bootstrap(journals,f,schema)) {
                ready(boot); f.catalogue.schemaUnavailable=true;
                assertThrows(IOException.class,() -> boot.step(1)); assertEquals(RatisDatabaseBootstrap.Phase.VERIFY,boot.progress().phase());
                f.catalogue.schemaUnavailable=false; f.catalogue.missingSchema=true;
                assertThrows(IOException.class,() -> boot.step(1)); assertEquals(RatisDatabaseBootstrap.Phase.VERIFY,boot.progress().phase());
                f.catalogue.missingSchema=false; ready(boot);
            } finally { f.catalogue.schemaUnavailable=false; f.catalogue.missingSchema=false; }
        }
    }

}
