package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.util.*;

/** A fresh-cluster USER-only import plan. Execution still needs native qualification and catalogue owner proofs. */
public final class RatisDatabaseRestorePlanner {
    public record Domain(DistributedSchema schema, RangeDirectory initialDirectory, List<RangeImportDefinition> imports) {
        public Domain { imports=List.copyOf(imports); }
        public RangeDirectory finalDirectory() { return imports.getLast().targetDirectory(); }
    }
    public record Plan(UUID archiveId, DatabaseDomainRoster roster, SessionAuthority authority,
                       UUID sessionId, List<Domain> domains) {
        public Plan { domains=List.copyOf(domains); }
        /** The plan is not session authorization. Execution requires a real committed fresh catalogue grant. */
        public SessionAuthority requireExecutionLedger(SessionEpochLedger issued) {
            if(!roster.clusterId().equals(issued.clusterId()) || !roster.databaseId().equals(issued.databaseId())
                    || issued.generation()<1 || issued.grants().stream().noneMatch(g->g.sessionId().equals(sessionId) && g.epoch()==1 && SessionAuthority.from(issued).permits(g)))
                throw new IllegalArgumentException("Restore execution lacks its committed fresh session grant");
            return SessionAuthority.from(issued);
        }
    }
    private RatisDatabaseRestorePlanner() {}

    /** Membership and cluster/session identity must be newly provisioned; no old operational state is imported. */
    public static Plan plan(DatabaseBackupManifest complete, UUID cluster, List<ReplicaDescriptor> membership,
                            UUID session, long logicalTimeMillis) {
        Objects.requireNonNull(complete); Objects.requireNonNull(cluster); Objects.requireNonNull(session);
        var old=complete.definition().roster();
        if(cluster.equals(old.clusterId())) throw new IllegalArgumentException("Restore requires a new cluster identity");
        var oldNodes=new HashSet<UUID>(); var oldIncarnations=new HashSet<UUID>(); var oldReplicas=new HashSet<UUID>();
        var oldPeers=new HashSet<String>(); var oldSessions=new HashSet<UUID>();
        for(var domain:complete.definition().domains()) {
            for(var range:domain.directory().ranges()) for(var replica:range.replicas()) {
                oldNodes.add(replica.nodeId()); oldIncarnations.add(replica.nodeIncarnation()); oldReplicas.add(replica.replicaId()); oldPeers.add(replica.peerId());
            }
            for(var fence:domain.fences()) { oldSessions.add(fence.operation().sessionId()); for(var p:fence.participants()) { oldSessions.add(p.installOperation().sessionId()); oldSessions.add(p.releaseOperation().sessionId()); } }
        }
        if(oldSessions.contains(session) || membership.isEmpty()) throw new IllegalArgumentException("Restore requires fresh session and membership");
        for(var r:membership) if(oldNodes.contains(r.nodeId()) || oldIncarnations.contains(r.nodeIncarnation()) || oldReplicas.contains(r.replicaId()) || oldPeers.contains(r.peerId()))
            throw new IllegalArgumentException("Restore membership reuses source operational identity");
        var domains=new ArrayList<Domain>(); long sequence=0;
        for(var source:complete.definition().domains()) {
            var schema=new DistributedSchema(cluster,old.databaseId(),source.schema().domainId(),0,
                    source.schema().routingPrefixBytes(),source.schema().columns());
            var ranges=new ArrayList<RangeDescriptor>(); var sourceGroups=new HashMap<UUID,UUID>();
            for(var range:source.directory().ranges()) {
                UUID group=UUID.randomUUID(); sourceGroups.put(group,range.groupId());
                ranges.add(new RangeDescriptor(UUID.randomUUID(),group,0,0,range.lowerInclusive(),range.upperExclusive(),RangeDescriptor.TopologyState.FENCED,membership));
            }
            var initial=new RangeDirectory(cluster,old.databaseId(),schema.domainId(),0,schema.digest(),0,schema.columnIds(),schema.routingPrefixBytes(),ranges);
            var current=initial; var imports=new ArrayList<RangeImportDefinition>();
            int start=0;
            while(start<ranges.size()) {
                UUID importId=UUID.randomUUID();var manifests=new ArrayList<NativeSstImportManifest>();
                long encodedBytes=60L+RangeDirectoryCodec.encode(current).length, sourceBytes=0;int fileCount=0;
                while(start<ranges.size() && manifests.size()<RangeImportDefinition.MAX_GROUPS) {
                    var range=ranges.get(start);var columns=new ArrayList<NativeSstImportManifest.Column>();
                    for(var column:schema.columns()) {
                        var files=complete.chunks().stream().filter(c->c.groupId().equals(sourceGroups.get(range.groupId())) && c.columnId().equals(column.columnId()) && c.entries()>0)
                                .map(c->new NativeSstImportManifest.File(c.objectId(),c.bytes(),c.sha256(),c.entries(),c.firstKey(),c.lastKey())).toList();
                        columns.add(new NativeSstImportManifest.Column(column.columnId(),"restored-"+column.columnId(),column.operator()!=null,files));
                    }
                    // Materialized PUT tables carry the physical codec, not old consensus/receipt identities.
                    var manifest=new NativeSstImportManifest(importId,range.groupId(),0,schema.domainId(),schema.digest(),range.lowerInclusive(),range.upperExclusive(),columns,RatisRangeImportRecord.DISTRIBUTED_LIMITS);
                    int count=columns.stream().mapToInt(c->c.files().size()).sum();long bytes=manifest.encode().length;
                    for(var c:columns)for(var file:c.files())bytes=Math.addExact(bytes,file.bytes());
                    long nextEncoded=encodedBytes+4L+manifest.encode().length;
                    if(nextEncoded>RangeImportDefinition.MAX_BYTES || bytes>RatisRangeImportController.MAX_SOURCE_BYTES-sourceBytes || count>RatisRangeImportController.MAX_SOURCE_FILES-fileCount) {
                        if(manifests.isEmpty())throw new IllegalArgumentException("Restore interval exceeds declared distributed job envelope");
                        break;
                    }
                    manifests.add(manifest);encodedBytes=nextEncoded;sourceBytes+=bytes;fileCount+=count;start++;
                }
                var batch=new RangeImportDefinition(importId,new OperationId(session,1,sequence++),current,manifests);
                imports.add(batch);current=batch.targetDirectory();
            }
            domains.add(new Domain(schema,initial,imports));
        }
        return new Plan(complete.destination().archiveId(),new DatabaseDomainRoster(cluster,old.databaseId(),0,old.domainIds()),
                new SessionAuthority(cluster,old.databaseId(),0,logicalTimeMillis,1),session,domains);
    }
}
