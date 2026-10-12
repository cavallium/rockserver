package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RatisDatabaseRestorePlannerTest {
    @Test void sixtyFiveGroupsKeepCompleteVectorAndActivateInTwoFreshBatches() {
        var f=new RatisDatabaseBackupFixture(65,1024);var complete=DatabaseBackupManifest.decode(f.manifest(f.emptyChunks()).encode());
        var fresh=UUID.randomUUID();var session=UUID.randomUUID();var plan=RatisDatabaseRestorePlanner.plan(complete,fresh,List.of(f.fresh()),session,100);
        assertEquals(fresh,plan.roster().clusterId());assertEquals(f.database,plan.roster().databaseId());assertEquals(0,plan.authority().generation());assertEquals(1,plan.authority().minimumEpoch());
        var d=plan.domains().getFirst();assertEquals(17,d.schema().column(f.column).apiId());assertEquals(65,d.initialDirectory().ranges().size());assertTrue(d.initialDirectory().ranges().stream().allMatch(r->r.topologyState()==RangeDescriptor.TopologyState.FENCED));
        assertEquals(2,d.imports().size());assertEquals(64,d.imports().getFirst().manifests().size());assertEquals(1,d.imports().getLast().manifests().size());
        assertArrayEquals(RangeDirectoryCodec.encode(d.imports().getFirst().targetDirectory()),RangeDirectoryCodec.encode(d.imports().getLast().sourceDirectory()));
        assertEquals(2,d.finalDirectory().generation());assertTrue(d.finalDirectory().ranges().stream().allMatch(r->r.topologyState()==RangeDescriptor.TopologyState.ACTIVE));
        var old=f.directory.ranges().stream().map(RangeDescriptor::groupId).collect(java.util.stream.Collectors.toSet());assertTrue(d.finalDirectory().ranges().stream().noneMatch(r->old.contains(r.groupId())));
        assertNotEquals(d.imports().getFirst().importId(),d.imports().getLast().importId());assertEquals(new OperationId(session,1,1),d.imports().getLast().operation());
    }
    @Test void definitionAndCompleteRequireExactRosterCutsAndFullCoverage() {
        var f=new RatisDatabaseBackupFixture(1,1024);assertArrayEquals(f.definition.encode(),DatabaseBackupDefinition.decode(f.definition.encode()).encode());
        assertThrows(IllegalArgumentException.class,()->f.definition.requireVector(new DatabaseDomainRoster(f.cluster,f.database,1,List.of(f.domain)),f.definition.domains()));
        assertThrows(IllegalArgumentException.class,()->f.manifest(List.of()));
        var whole=f.emptyChunks().getFirst();var repeated=new DatabaseBackupManifest.Chunk(f.domain,whole.groupId(),f.column,1,f.cut,null,null,null,0,new byte[0],0,null,null);
        assertThrows(IllegalArgumentException.class,()->f.manifest(List.of(whole,repeated)));
        var a=new DatabaseBackupManifest.Chunk(f.domain,whole.groupId(),f.column,0,f.cut,null,new byte[]{10},null,0,new byte[0],0,null,null);
        var b=new DatabaseBackupManifest.Chunk(f.domain,whole.groupId(),f.column,1,f.cut,new byte[]{10},null,null,0,new byte[0],0,null,null);
        var wrap=new DatabaseBackupManifest.Chunk(f.domain,whole.groupId(),f.column,2,f.cut,null,null,null,0,new byte[0],0,null,null);
        assertThrows(IllegalArgumentException.class,()->f.manifest(List.of(a,b,wrap)));
        var changed=new DatabaseBackupManifest.Chunk(f.domain,whole.groupId(),f.column,0,new ApplyPosition(1,5),null,null,null,0,new byte[0],0,null,null);
        assertThrows(IllegalArgumentException.class,()->f.manifest(List.of(changed)));
        assertThrows(IllegalArgumentException.class,()->new DatabaseBackupManifest(f.definition,f.destination,List.of(f.held.getFirst().complete()),List.of(f.cohort),List.of(whole),List.of()));
    }
    @Test void sourceMembershipClusterAndSessionCannotBeRestoredAsLiveAuthority() {
        var f=new RatisDatabaseBackupFixture(1,1024);var manifest=f.manifest(f.emptyChunks());
        assertThrows(IllegalArgumentException.class,()->RatisDatabaseRestorePlanner.plan(manifest,f.cluster,List.of(f.fresh()),UUID.randomUUID(),0));
        assertThrows(IllegalArgumentException.class,()->RatisDatabaseRestorePlanner.plan(manifest,UUID.randomUUID(),List.of(f.owner),UUID.randomUUID(),0));
        assertThrows(IllegalArgumentException.class,()->RatisDatabaseRestorePlanner.plan(manifest,UUID.randomUUID(),List.of(f.fresh()),f.session,0));
        byte[] malformed=f.definition.encode();java.nio.ByteBuffer.wrap(malformed).putInt(0,0);assertThrows(IllegalArgumentException.class,()->DatabaseBackupDefinition.decode(malformed));
        var limits=new DatabaseBackupDefinition.Limits(1,1,1,1,1);assertThrows(IllegalArgumentException.class,()->new DatabaseBackupDefinition(UUID.randomUUID(),f.definition.roster(),f.definition.domains(),limits));
    }
    @Test void legalLargeArchivePlansSameUnsplittableIntervalWithoutTruncation() {
        var f=new RatisDatabaseBackupFixture(1,1024);var range=f.directory.ranges().getFirst();var pieces=new ArrayList<DatabaseBackupManifest.Chunk>();
        for(int i=0;i<65;i++)pieces.add(new DatabaseBackupManifest.Chunk(f.domain,range.groupId(),f.column,i,f.cut,i==0?null:new byte[]{(byte)i},i==64?null:new byte[]{(byte)(i+1)},UUID.randomUUID(),9L*1024*1024,new byte[32],1,new byte[]{(byte)i},new byte[]{(byte)i}));
        var definition=new DatabaseBackupDefinition(f.definition.backupId(),f.definition.roster(),f.definition.domains(),new DatabaseBackupDefinition.Limits(1,1,65,1024L*1024*1024,4*1024*1024));
        var complete=new DatabaseBackupManifest(definition,f.destination,f.held,List.of(f.cohort),pieces,List.of());assertEquals(65,complete.chunks().size());
        var plan=RatisDatabaseRestorePlanner.plan(complete,UUID.randomUUID(),List.of(f.fresh()),UUID.randomUUID(),0);
        var restored=plan.domains().getFirst();assertEquals(1,restored.initialDirectory().ranges().size());
        var manifest=restored.imports().getFirst().manifests().getFirst();assertEquals(65,manifest.columns().getFirst().files().size());
        assertEquals(RatisRangeImportRecord.DISTRIBUTED_LIMITS,manifest.limits());assertEquals(65,complete.chunks().size());assertTrue(manifest.columns().getFirst().files().stream().mapToLong(NativeSstImportManifest.File::bytes).sum()>512L*1024*1024);

    }
    @Test void executablePlanRequiresCommittedFreshIssuedGrant() {
        var f=new RatisDatabaseBackupFixture(1,1024);var cluster=UUID.randomUUID();var session=UUID.randomUUID();var plan=RatisDatabaseRestorePlanner.plan(f.manifest(f.emptyChunks()),cluster,List.of(f.fresh()),session,10);
        var initial=SessionEpochLedger.initial(cluster,f.database,10);assertThrows(IllegalArgumentException.class,()->plan.requireExecutionLedger(initial));
        assertEquals(1,plan.requireExecutionLedger(initial.issue(session,100)).generation());assertThrows(IllegalArgumentException.class,()->plan.requireExecutionLedger(initial.issue(UUID.randomUUID(),100)));
    }

    @Test void databaseVectorRejectsSharedRangeColumnAndApiIdentitiesAcrossDomains() {
        var f=new RatisDatabaseBackupFixture(1,1024);
        for(int variant=0;variant<3;variant++){
            UUID domain=UUID.randomUUID(),column=variant==1?f.column:UUID.randomUUID();long api=variant==2?17:18;
            var schema=new DistributedSchema(f.cluster,f.database,domain,0,0,List.of(new DistributedSchema.Column(api,column,"other",f.schema.column(f.column).schema(),null)));
            var original=f.directory.ranges().getFirst();var range=new RangeDescriptor(variant==0?original.rangeId():UUID.randomUUID(),UUID.randomUUID(),1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(f.owner));
            var directory=new RangeDirectory(f.cluster,f.database,domain,0,schema.digest(),0,Set.of(column),0,List.of(range));
            var participant=new SnapshotFence.Participant(range.rangeId(),range.groupId(),1,schema.digest(),new OperationId(UUID.randomUUID(),1,0),new OperationId(UUID.randomUUID(),1,0),new TransactionScope(List.of(TransactionScope.Span.range(column,null,null))));
            var fence=new SnapshotFence(f.cluster,f.database,domain,UUID.randomUUID(),UUID.randomUUID(),1,new OperationId(UUID.randomUUID(),1,0),0,0,schema.digest(),List.of(participant));
            var second=new DatabaseBackupDefinition.Domain(directory,schema,List.of(fence));
            assertThrows(IllegalArgumentException.class,()->new DatabaseBackupDefinition(UUID.randomUUID(),new DatabaseDomainRoster(f.cluster,f.database,0,List.of(f.domain,domain)),List.of(f.definition.domains().getFirst(),second),new DatabaseBackupDefinition.Limits(2,2,2,1024,1024*1024)));
        }
    }

}
