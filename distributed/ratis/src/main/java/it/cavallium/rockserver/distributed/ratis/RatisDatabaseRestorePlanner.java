package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.util.*;
import java.io.*;
import java.nio.ByteBuffer;

/** A fresh-cluster USER-only import plan. Execution still needs native qualification and catalogue owner proofs. */
public final class RatisDatabaseRestorePlanner {
    public static final int MAX_BYTES=32*1024*1024,MAX_IMPORTS=RangeDirectoryCodec.MAX_RANGES;
    public record Domain(DistributedSchema schema, RangeDirectory initialDirectory, List<RangeImportDefinition> imports) {
        public Domain { imports=List.copyOf(imports); }
        public RangeDirectory finalDirectory() { return imports.getLast().targetDirectory(); }
    }
    public record Plan(UUID archiveId, DatabaseDomainRoster roster, SessionAuthority authority,
                       UUID sessionId,UUID issuerId,RatisNodePlan.Catalogue catalogue, List<Domain> domains) {
        public Plan {
            Objects.requireNonNull(archiveId);Objects.requireNonNull(roster);Objects.requireNonNull(authority);Objects.requireNonNull(sessionId);Objects.requireNonNull(issuerId);Objects.requireNonNull(catalogue);domains=List.copyOf(domains);
            if(domains.isEmpty() || domains.size()>DatabaseDomainRoster.MAX_DOMAINS || !authority.clusterId().equals(roster.clusterId()) || !authority.databaseId().equals(roster.databaseId()) || issuerId.equals(sessionId))throw new IllegalArgumentException("Invalid fresh restore plan namespace");
            var ids=new HashSet<UUID>();var groups=new HashSet<UUID>();int jobs=0;long sequence=0;
            for(var domain:domains){domain.schema().requireMatches(domain.initialDirectory());if(!ids.add(domain.schema().domainId()) || domain.imports().isEmpty())throw new IllegalArgumentException("Restore domain duplicate/empty import chain");var directory=domain.initialDirectory();for(var range:directory.ranges())if(!groups.add(range.groupId()))throw new IllegalArgumentException("Restore group reused");for(var imported:domain.imports()){if(!Arrays.equals(directory.payloadDigest(),imported.sourceDirectory().payloadDigest()) || !imported.operation().equals(new OperationId(sessionId,1,sequence)))throw new IllegalArgumentException("Restore import chain/operation changed");sequence+=2;directory=imported.targetDirectory();jobs++;}}
            if(!ids.equals(Set.copyOf(roster.domainIds())) || groups.size()>RangeDirectoryCodec.MAX_RANGES || jobs>MAX_IMPORTS)throw new IllegalArgumentException("Restore exceeds executable domain/group/job scope");
        }
        public RatisColumnAllocation.Reservation reservation(UUID domain,DistributedSchema.Column column){return new RatisColumnAllocation.Reservation(UUID.nameUUIDFromBytes(ByteBuffer.allocate(32).putLong(issuerId.getMostSignificantBits()).putLong(issuerId.getLeastSignificantBits()).putLong(column.columnId().getMostSignificantBits()).putLong(column.columnId().getLeastSignificantBits()).array()),domain,column.columnId(),column.apiId());}
        public RatisNodePlan nodePlan(ReplicaDescriptor local,Map<String,String> streams,Collection<RatisNativePoolSpec> pools,Map<UUID,UUID> domainPools,Map<UUID,UUID> groupPools){var inputs=new ArrayList<RatisNodePlan.Domain>();var jobs=new ArrayList<RangeImportDefinition>();for(var domain:domains){var manifest=domain.imports().getFirst().manifests().getFirst();inputs.add(new RatisNodePlan.Domain(domain.initialDirectory(),manifest.physicalNames(),manifest.mergeColumns()));jobs.addAll(domain.imports());}return new RatisNodePlan(local.nodeId(),local.nodeIncarnation(),inputs,catalogue,List.of(),jobs,streams,RatisNodePlan.MAX_DATA_GROUPS+1,pools,domainPools,groupPools);}
        public byte[] encode(){return encodePlan(this);}
        public static Plan decode(byte[] bytes){return decodePlan(bytes);}
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
                var batch=new RangeImportDefinition(importId,new OperationId(session,1,sequence),current,manifests);
                imports.add(batch);current=batch.targetDirectory();sequence+=2;
            }
            domains.add(new Domain(schema,initial,imports));
        }
        var catalogue=new RatisNodePlan.Catalogue(UUID.randomUUID(),0,new byte[32],UUID.randomUUID(),UUID.randomUUID(),membership);
        return new Plan(complete.destination().archiveId(),new DatabaseDomainRoster(cluster,old.databaseId(),0,old.domainIds()),
                new SessionAuthority(cluster,old.databaseId(),0,logicalTimeMillis,1),session,UUID.randomUUID(),catalogue,domains);
    }    private static byte[] encodePlan(Plan plan){
        try{var bytes=new ByteArrayOutputStream();var out=new DataOutputStream(bytes);uuid(out,plan.archiveId());blob(out,plan.roster().encode());out.writeLong(plan.authority().logicalTimeMillis());uuid(out,plan.sessionId());uuid(out,plan.issuerId());var c=plan.catalogue();uuid(out,c.directoryColumn());uuid(out,c.sessionsColumn());var range=new RangeDescriptor(c.groupId(),c.groupId(),c.epoch(),0,null,null,RangeDescriptor.TopologyState.ACTIVE,c.replicas());blob(out,RangeDirectoryCodec.encode(new RangeDirectory(plan.roster().clusterId(),plan.roster().databaseId(),c.groupId(),c.epoch(),c.schemaDigest(),0,Set.of(c.directoryColumn(),c.sessionsColumn()),0,List.of(range))));out.writeInt(plan.domains().size());for(var domain:plan.domains()){blob(out,DistributedSchemaCodec.encode(domain.schema()));blob(out,RangeDirectoryCodec.encode(domain.initialDirectory()));out.writeInt(domain.imports().size());for(var imported:domain.imports()){blob(out,imported.encode());if(bytes.size()>MAX_BYTES)throw new IllegalArgumentException("Restore plan metadata exceeds bound");}}out.flush();if(bytes.size()>MAX_BYTES)throw new IllegalArgumentException("Restore plan exceeds bound");return bytes.toByteArray();}catch(IOException impossible){throw new IllegalStateException(impossible);}
    }
    private static Plan decodePlan(byte[] bytes){
        if(bytes==null || bytes.length>MAX_BYTES)throw new IllegalArgumentException("Restore plan exceeds bound");try{var in=new DataInputStream(new ByteArrayInputStream(bytes));UUID archive=uuid(in);var roster=DatabaseDomainRoster.decode(blob(in,DatabaseDomainRoster.MAX_BYTES));long time=in.readLong();UUID session=uuid(in),issuer=uuid(in),directoryColumn=uuid(in),sessionsColumn=uuid(in);var metadata=RangeDirectoryCodec.decode(blob(in,RangeDirectoryCodec.MAX_ENCODED_BYTES));var c=new RatisNodePlan.Catalogue(metadata.domainId(),metadata.schemaEpoch(),metadata.schemaDigest(),directoryColumn,sessionsColumn,metadata.ranges().getFirst().replicas());int count=count(in,DatabaseDomainRoster.MAX_DOMAINS);var domains=new ArrayList<Domain>();int jobs=0;for(int i=0;i<count;i++){var schema=DistributedSchemaCodec.decode(blob(in,DistributedSchemaCodec.MAX_BYTES));var initial=RangeDirectoryCodec.decode(blob(in,RangeDirectoryCodec.MAX_ENCODED_BYTES));int n=count(in,MAX_IMPORTS);jobs+=n;if(jobs>MAX_IMPORTS)throw new IOException("Restore import count exceeded");var imports=new ArrayList<RangeImportDefinition>();for(int j=0;j<n;j++)imports.add(RangeImportDefinition.decode(blob(in,RangeImportDefinition.MAX_BYTES)));domains.add(new Domain(schema,initial,imports));}if(in.available()!=0)throw new IOException("Trailing restore plan");var plan=new Plan(archive,roster,new SessionAuthority(roster.clusterId(),roster.databaseId(),0,time,1),session,issuer,c,domains);if(!Arrays.equals(bytes,plan.encode()))throw new IOException("Noncanonical restore plan");return plan;}catch(IOException|IndexOutOfBoundsException bad){throw new IllegalArgumentException("Invalid restore plan",bad);}
    }
    private static int count(DataInputStream in,int max)throws IOException{int n=in.readInt();if(n<1 || n>max || n>in.available())throw new IOException("Invalid restore count");return n;}
    private static void blob(DataOutputStream out,byte[] bytes)throws IOException{out.writeInt(bytes.length);out.write(bytes);}
    private static byte[] blob(DataInputStream in,int max)throws IOException{int n=in.readInt();if(n<0 || n>max || n>in.available())throw new IOException("Invalid restore blob");return in.readNBytes(n);}
    private static void uuid(DataOutputStream out,UUID id)throws IOException{out.writeLong(id.getMostSignificantBits());out.writeLong(id.getLeastSignificantBits());}
    private static UUID uuid(DataInputStream in)throws IOException{return new UUID(in.readLong(),in.readLong());}

}
