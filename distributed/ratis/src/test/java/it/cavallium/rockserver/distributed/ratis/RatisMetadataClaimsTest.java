package it.cavallium.rockserver.distributed.ratis;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisMetadataClaimsTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static final UUID POOL=new UUID(9910,1),GROUP=new UUID(9910,2);
    static RatisMetadataClaims.Claim claim(UUID group){return RatisMetadataClaims.claim(POOL,group,RatisNodePlan.Role.DATA,RatisRangeStateMachine.RetentionLimits.defaults(),RatisSchemaReplicaService.Limits.defaults());}
    @Test void unknownForceRetryAndColdRetainedGroupsDoNotDuplicateOrRefundClaims()throws Exception {
        var path=root.resolve("claims");var fail=new AtomicBoolean(true);var retained=claim(GROUP);long ceiling=RatisMetadataClaims.JOURNAL_BYTES+RatisMetadataClaims.POOL_BYTES+retained.bytes();
        var budget=new RatisMetadataClaims(path,ceiling,Map.of(POOL,ceiling),(file,bytes)->{SessionJournalFiles.write(file,bytes,8<<20);if(fail.getAndSet(false))throw new IOException("Unknown force reply");});
        assertThrows(IOException.class,()->budget.reserve(List.of(retained)));assertTrue(Files.exists(path));budget.reserve(List.of(retained));assertEquals(1,budget.groups());assertEquals(ceiling,budget.bytes());
        var reopened=new RatisMetadataClaims(path,ceiling,Map.of(POOL,ceiling));assertEquals(1,reopened.groups());assertThrows(IOException.class,()->reopened.reserve(List.of(claim(new UUID(9910,3)))));assertEquals(ceiling,reopened.bytes());assertThrows(IOException.class,()->new RatisMetadataClaims(path,ceiling-1,Map.of(POOL,ceiling)));assertTrue(Files.exists(path));
    }
    @Test void intactCommittedLedgerDiscardsPartialSidecarButPromotesCompletePromises()throws Exception {
        var path=root.resolve("sidecar");var original=claim(GROUP);var budget=new RatisMetadataClaims(path,1L<<40,Map.of(POOL,1L<<40));budget.reserve(List.of(original));byte[] committed=Files.readAllBytes(path);
        var pending=path.resolveSibling("sidecar.pending");Files.write(pending,Arrays.copyOf(committed,committed.length/2));var recovered=new RatisMetadataClaims(path,1L<<40,Map.of(POOL,1L<<40));assertEquals(1,recovered.groups());assertFalse(Files.exists(pending));assertArrayEquals(committed,Files.readAllBytes(path));recovered.reserve(List.of(original));assertEquals(1,recovered.groups());
        var full=root.resolve("complete");var expanded=new RatisMetadataClaims(full,1L<<40,Map.of(POOL,1L<<40));expanded.reserve(List.of(original,claim(new UUID(9910,5))));Files.copy(full,pending);var promoted=new RatisMetadataClaims(path,1L<<40,Map.of(POOL,1L<<40));assertEquals(2,promoted.groups());assertFalse(Files.exists(pending));assertArrayEquals(Files.readAllBytes(full),Files.readAllBytes(path));
        byte[] damaged=Files.readAllBytes(path);damaged[damaged.length-1]^=1;Files.write(path,damaged);Files.write(pending,new byte[]{1,2,3});assertThrows(IOException.class,()->new RatisMetadataClaims(path,1L<<40,Map.of(POOL,1L<<40)));assertArrayEquals(new byte[]{1,2,3},Files.readAllBytes(pending));
    }
    @Test void retainedPolicyAndPlacementAreImmutableEvenIfNewBudgetHasRoom()throws Exception {
        var budget=new RatisMetadataClaims(root.resolve("policy"),1L<<40,Map.of(POOL,1L<<40));var original=claim(GROUP);budget.reserve(List.of(original));
        assertThrows(IOException.class,()->budget.reserve(List.of(new RatisMetadataClaims.Claim(new UUID(9910,4),GROUP,original.role(),original.bytes(),original.policy()))));
        var smaller=new RatisRangeStateMachine.RetentionLimits(1,1024,1,1024);assertThrows(IOException.class,()->budget.reserve(List.of(RatisMetadataClaims.claim(POOL,GROUP,original.role(),smaller,RatisSchemaReplicaService.Limits.defaults()))));assertEquals(1,budget.groups());
    }
    @Test void nativeRetiredParentAndUnactivatedChildrenRetainClaimsBeyondLiveOwners()throws Exception {
        var helper=new RatisTopologySeedRuntimeTest();helper.root=root;try(var f=helper.new Fixture()){
            f.node.seedTopology(f.request());var inventory=f.pool().retainedMetadataOwners();assertEquals(3,inventory.size());assertEquals(1,inventory.stream().filter(it.cavallium.rockserver.core.impl.rocksdb.DistributedStoragePool.RetainedMetadataOwner::retired).count());assertEquals(2,inventory.stream().filter(it.cavallium.rockserver.core.impl.rocksdb.DistributedStoragePool.RetainedMetadataOwner::pending).count());
            var path=root.resolve("retained-claims");var budget=new RatisMetadataClaims(path,1L<<40,Map.of(RatisPoolFixtures.ID,256L<<30));var all=new ArrayList<RatisMetadataClaims.Claim>();for(var owner:inventory)all.add(RatisMetadataClaims.claim(RatisPoolFixtures.ID,owner.owner().groupId(),RatisNodePlan.Role.DATA,f.env.settings().retention(),RatisPoolFixtures.pools().getFirst().replicas()));budget.reserve(all);long bytes=budget.bytes();assertEquals(3,budget.groups());
            f.node.close();var cold=new RatisMetadataClaims(path,1L<<40,Map.of(RatisPoolFixtures.ID,256L<<30));assertEquals(bytes,cold.bytes());assertEquals(3,cold.groups());assertThrows(IOException.class,()->cold.reserve(List.of(RatisMetadataClaims.claim(RatisPoolFixtures.ID,new UUID(9910,10),RatisNodePlan.Role.DATA,f.env.settings().retention(),RatisPoolFixtures.pools().getFirst().replicas()),new RatisMetadataClaims.Claim(new UUID(9910,11),inventory.getFirst().owner().groupId(),RatisNodePlan.Role.DATA,all.getFirst().bytes(),all.getFirst().policy()))));assertEquals(bytes,cold.bytes());assertEquals(3,cold.groups());
        }
    }
    @Test void aggregateRoleClaimsRejectBeforeNativeOpenAndRemainAcrossActualColdRuntime()throws Exception {
        try(var env=new RatisNodeRuntimeTest.Env(root)){
            var base=env.settings();long required=RatisMetadataClaims.JOURNAL_BYTES+RatisMetadataClaims.POOL_BYTES;for(var group:env.plan.localGroups())required+=RatisMetadataClaims.claim(RatisPoolFixtures.ID,group.getGroupId().getUuid(),env.plan.role(group.getGroupId().getUuid()),base.retention(),RatisPoolFixtures.pools().getFirst().replicas()).bytes();
            var resources=base.resources();var denied=new RatisNodeRuntime.Settings(base.images(),base.retention(),base.clockPeriod(),base.propagationVisits(),base.maxPropagationRanges(),base.shutdownTimeout(),base.exports(),new RatisNodeRuntime.ResourceLimits(resources.node(),resources.pool(),resources.maximumSourceImageDiskBytes(),required-1,resources.maximumOperatorCacheBytes(),resources.maximumNativeCacheBytes(),RatisCommittedApplyDispatcher.Policy.defaults(),RatisDiskRunway.Policy.defaults()));var unopened=new RatisNodeRuntimeTest.FakeServer();
            assertThrows(IOException.class,()->RatisNodeRuntime.open(env.plan,env.nodeRoot,it.cavallium.rockserver.core.config.ConfigParser.parseDefault(),env.catalogue.epochs,Map.of(env.routed.directory().domainId(),env.routed),env.scheduler,env.timer,()->20L,denied,RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,unopened::create));assertNull(unopened.machines);assertFalse(Files.exists(RatisPoolFixtures.pools().getFirst().nativePath(env.nodeRoot).resolve("CURRENT")));
            try(var node=env.open(RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,new RatisNodeRuntimeTest.FakeServer())){node.start();}
            var cold=new RatisMetadataClaims(env.nodeRoot.resolve("metadata-claims"),resources.maximumDurableMetadataBytes(),Map.of(RatisPoolFixtures.ID,RatisPoolFixtures.pools().getFirst().maximumDurableMetadataBytes()));assertEquals(env.plan.localGroups().size(),cold.groups());assertEquals(required,cold.bytes());assertThrows(IOException.class,()->RatisNodeRuntime.open(env.plan,env.nodeRoot,it.cavallium.rockserver.core.config.ConfigParser.parseDefault(),env.catalogue.epochs,Map.of(env.routed.directory().domainId(),env.routed),env.scheduler,env.timer,()->20L,denied,RatisNodeRuntime.Startup.RECOVER,new RatisNodeRuntimeTest.FakeServer()::create));assertEquals(required,new RatisMetadataClaims(env.nodeRoot.resolve("metadata-claims"),resources.maximumDurableMetadataBytes(),Map.of(RatisPoolFixtures.ID,RatisPoolFixtures.pools().getFirst().maximumDurableMetadataBytes())).bytes());
        }
    }
}
