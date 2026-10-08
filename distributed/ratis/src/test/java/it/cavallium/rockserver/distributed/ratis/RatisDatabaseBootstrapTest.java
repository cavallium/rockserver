package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
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
        return new RatisNodePlan(node.nodeId(),node.nodeIncarnation(),f.directory,
                new RatisNodePlan.Catalogue(f.outer.catalogue,1,new byte[32],f.outer.directoryColumn,f.outer.sessions,List.of(catalogue)),Map.of(f.outer.data,"data"),Set.of(),2);
    }
    RatisDatabaseBootstrap bootstrap(Path path,RatisSessionPropagationTest.Fixture f,RatisNodePlan plan) throws IOException {
        return new RatisDatabaseBootstrap(path,plan,f.catalogue.client,f.routed,10,() -> 20L,2);
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
}
