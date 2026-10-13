package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Two actual native databases, with controlled Raft divisions only. */
class RatisNodePoolTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void explicitPoolPlacementWritesAndColdServiceReopensBothConfiguredDatabases()throws Exception {
        var helper=new RatisNodeMultiDomainTest();helper.root=root;
        try(var f=helper.new Fixture()){
            UUID fast=new UUID(991,1),archive=new UUID(991,2);var one=RatisPoolFixtures.spec(fast,false);var two=RatisPoolFixtures.spec(archive,true);
            var plan=new RatisNodePlan(f.plan.nodeId(),f.plan.nodeIncarnation(),f.plan.domains(),f.plan.catalogue(),f.plan.homes(),f.plan.imports(),f.plan.streamAddresses(),f.plan.maxLocalGroups(),List.of(one,two),Map.of(f.first.domainId(),fast,f.second.domainId(),archive),Map.of(f.plan.catalogue().groupId(),fast));
            var fake=new RatisNodeRuntimeTest.FakeServer();
            try(var node=RatisNodeRuntime.open(plan,f.env.nodeRoot,ConfigParser.parseDefault(),f.env.catalogue.epochs,f.clients,f.env.scheduler,f.env.timer,()->20L,f.settings(),RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,fake::create,f.schemas)){
                node.start();assertNotSame(node.nativePool(fast),node.nativePool(archive));assertFalse(one.config().global().spinning());assertTrue(two.config().global().spinning());
                for(var schema:f.schemas){var range=plan.directory(schema.domainId()).ranges().getFirst();assertEquals(schema==f.first?fast:archive,node.poolId(range.groupId()));var store=f.store(fake,range.groupId());var command=new DistributedCommand(range.groupId(),range.rangeEpoch(),schema.digest(),new OperationId(UUID.randomUUID(),1,0),List.of(new Mutation(Mutation.Kind.PUT,schema.columnIds().iterator().next(),new byte[]{1},new byte[]{(byte)(schema==f.first?7:9)})));assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(store.apply(new ApplyPosition(1,store.applied().index()+1),command)).status());}
                assertTrue(Files.isRegularFile(one.nativePath(f.env.nodeRoot).resolve("CURRENT")));assertTrue(Files.isRegularFile(two.nativePath(f.env.nodeRoot).resolve("CURRENT")));assertFalse(Files.exists(f.env.nodeRoot.resolve("native/CURRENT")));
            }
            f.publish();var manifest=RatisNodeManifest.decode(RatisNodeManifest.encode(new RatisNodeManifest(f.schemas,plan,f.settings(),3,3)));assertArrayEquals(plan.identityDigest(),manifest.plan().identityDigest());var recovered=new RatisNodeRuntimeTest.FakeServer();var resources=new RatisNodeService.Resources(f.env.catalogue.client,f.clients,f.env.scheduler,f.env.timer);f.env.resourcesTransferred=true;
            try(var service=RatisNodeService.recover(manifest,f.env.nodeRoot,ConfigParser.parseDefault(),resources,recovered::create)){
                service.start();service.verifyCatalogue();for(var schema:f.schemas){var range=plan.directory(schema.domainId()).ranges().getFirst();assertArrayEquals(new byte[]{(byte)(schema==f.first?7:9)},f.store(recovered,range.groupId()).readCurrentPoint(schema.columnIds().iterator().next(),new byte[]{1}));}
            }
        }
    }
    @Test void oneGlobalNativePinLimitRejectsTheOtherPoolUntilActualRelease()throws Exception {
        var helper=new RatisNodeMultiDomainTest();helper.root=root;
        try(var f=helper.new Fixture()){
            UUID fast=new UUID(992,1),archive=new UUID(992,2);var plan=new RatisNodePlan(f.plan.nodeId(),f.plan.nodeIncarnation(),f.plan.domains(),f.plan.catalogue(),f.plan.homes(),f.plan.imports(),f.plan.streamAddresses(),f.plan.maxLocalGroups(),List.of(RatisPoolFixtures.spec(fast,false),RatisPoolFixtures.spec(archive,true)),Map.of(f.first.domainId(),fast,f.second.domainId(),archive),Map.of(f.plan.catalogue().groupId(),fast));var base=f.settings();var pins=new it.cavallium.rockserver.core.impl.rocksdb.PoolSnapshotAdmission.Limits(1,1L<<30,java.time.Duration.ofSeconds(60),0);var settings=new RatisNodeRuntime.Settings(base.images(),base.retention(),base.clockPeriod(),base.propagationVisits(),base.maxPropagationRanges(),base.shutdownTimeout(),new RatisNodeRuntime.ExportResources(pins,base.exports().sources(),base.exports().io()),base.resources());var fake=new RatisNodeRuntimeTest.FakeServer();
            try(var node=RatisNodeRuntime.open(plan,f.env.nodeRoot,ConfigParser.parseDefault(),f.env.catalogue.epochs,f.clients,f.env.scheduler,f.env.timer,()->20L,settings,RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,fake::create,f.schemas)){
                node.start();var first=f.store(fake,plan.directory(f.first.domainId()).ranges().getFirst().groupId());var second=f.store(fake,plan.directory(f.second.domainId()).ranges().getFirst().groupId());try(var pin=first.capture()){assertThrows(RuntimeException.class,second::capture);assertEquals(1,node.nativePool(fast).snapshotAdmissionStats().pins());assertEquals(0,node.nativePool(archive).snapshotAdmissionStats().pins());}try(var pin=second.capture()){assertEquals(1,node.nativePool(archive).snapshotAdmissionStats().pins());}
            }
        }
    }

}
