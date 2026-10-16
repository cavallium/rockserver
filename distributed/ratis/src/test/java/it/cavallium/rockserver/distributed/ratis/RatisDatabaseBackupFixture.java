package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.distributed.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.*;

final class RatisDatabaseBackupFixture {
    final UUID cluster=UUID.randomUUID(),database=UUID.randomUUID(),domain=UUID.randomUUID(),column=UUID.randomUUID(),session=UUID.randomUUID();
    final ReplicaDescriptor owner=new ReplicaDescriptor(UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),"source","localhost:19001",ReplicaDescriptor.Role.VOTER);
    final DistributedSchema schema;
    final RangeDirectory directory;
    final DatabaseBackupDefinition definition;
    final List<SnapshotFenceStatus> held;
    final DatabaseBackupManifest.Destination destination=new DatabaseBackupManifest.Destination(UUID.randomUUID(),UUID.randomUUID());
    final DatabaseBackupManifest.Cohort cohort;
    final ApplyPosition cut=new ApplyPosition(1,4);
    RatisDatabaseBackupFixture(int count,long archiveBytes) {
        schema=new DistributedSchema(cluster,database,domain,0,0,List.of(new DistributedSchema.Column(17,column,"data",ColumnSchema.of(IntArrayList.of(1),new ObjectArrayList<>(),true),null)));
        var ranges=new ArrayList<RangeDescriptor>();for(int i=0;i<count;i++)ranges.add(new RangeDescriptor(UUID.randomUUID(),UUID.randomUUID(),1,0,i==0?null:new byte[]{(byte)i},i==count-1?null:new byte[]{(byte)(i+1)},RangeDescriptor.TopologyState.ACTIVE,List.of(owner)));
        directory=new RangeDirectory(cluster,database,domain,0,schema.digest(),0,Set.of(column),0,ranges);
        var fences=new ArrayList<SnapshotFence>();var statuses=new ArrayList<SnapshotFenceStatus>();long sequence=0;
        for(int start=0;start<count;start+=64){var participants=new ArrayList<SnapshotFence.Participant>();var operation=new OperationId(session,1,sequence++);
            for(var range:ranges.subList(start,Math.min(count,start+64)))participants.add(new SnapshotFence.Participant(domain, range.rangeId(), range.groupId(), 1, schema.digest(), new OperationId(session,1,sequence++), new OperationId(session,1,sequence++), new TransactionScope(List.of(TransactionScope.Span.range(column,range.lowerInclusive(),range.upperExclusive())))));
            var fence=new SnapshotFence(cluster, database, UUID.randomUUID(), UUID.randomUUID(), 1, operation, List.of(DomainBinding.of(directory)), participants);fences.add(fence);
            var status=SnapshotFenceStatus.begin(fence);for(var participant:participants)status=status.ready(participant.groupId(),cut);for(var participant:participants)status=status.capture(participant.groupId(),new SnapshotFenceStatus.Capture(owner.nodeIncarnation(),UUID.randomUUID(),cut));statuses.add(status);
        }
        definition=new DatabaseBackupDefinition(UUID.randomUUID(),new DatabaseDomainRoster(cluster,database,0,List.of(domain)),List.of(new DatabaseBackupDefinition.Domain(directory,schema,fences)),new DatabaseBackupDefinition.Limits(count,count,Math.max(count,4),archiveBytes,4*1024*1024));
        held=List.copyOf(statuses);cohort=new DatabaseBackupManifest.Cohort(owner.nodeIncarnation(),UUID.randomUUID(),UUID.randomUUID(),directory.ranges().stream().map(RangeDescriptor::groupId).collect(java.util.stream.Collectors.toSet()));
    }
    List<DatabaseBackupManifest.Chunk> emptyChunks(){return directory.ranges().stream().map(r->new DatabaseBackupManifest.Chunk(domain,r.groupId(),column,0,cut,r.lowerInclusive(),r.upperExclusive(),null,0,new byte[0],0,null,null)).toList();}
    DatabaseBackupManifest manifest(List<DatabaseBackupManifest.Chunk> chunks){return new DatabaseBackupManifest(definition,destination,held,List.of(cohort),chunks,List.of());}
    ReplicaDescriptor fresh(){return new ReplicaDescriptor(UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),"fresh","localhost:19002",ReplicaDescriptor.Role.VOTER);}
}
