package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/** Validated static join configuration; it grants no authority to create placements or discard recovery data. */
public final class RatisNodePlan {
    public static final int MAX_DOMAINS=DatabaseDomainRoster.MAX_DOMAINS, MAX_DATA_GROUPS=RangeDirectoryCodec.MAX_RANGES, MAX_DOMAIN_BYTES=16*1024*1024, MAX_METADATA_BYTES=32*1024*1024, MAX_PHYSICAL_NAME_BYTES=4096;
    public static final int MAX_HOME_GROUPS=64, MAX_HOME_DIRECTORY_BYTES=2*1024*1024, MAX_LOCAL_GROUPS=RangeDirectoryCodec.MAX_RANGES+1+MAX_HOME_GROUPS;
    public enum Role { DATA, CATALOGUE, TRANSACTION_HOME, SNAPSHOT_HOME }
    public record Home(Role role,RangeDirectory directory,UUID column,String physicalName) {
        public Home {
            Objects.requireNonNull(role); Objects.requireNonNull(directory); Objects.requireNonNull(column); requirePhysicalName(physicalName);
            if (role!=Role.TRANSACTION_HOME && role!=Role.SNAPSHOT_HOME || !directory.columns().equals(Set.of(column)) || directory.routingPrefixBytes()!=0
                    || directory.ranges().size()>MAX_HOME_GROUPS || directory.ranges().stream().anyMatch(range -> range.topologyState()!=RangeDescriptor.TopologyState.ACTIVE)) throw new IllegalArgumentException("Invalid dedicated home placement");
        }
    }
    public record Domain(RangeDirectory directory,Map<UUID,String> columns,Set<UUID> merges) {
        public Domain {
            Objects.requireNonNull(directory); if(columns.size()>RangeDirectoryCodec.MAX_COLUMNS || merges.size()>RangeDirectoryCodec.MAX_COLUMNS) throw new IllegalArgumentException("DATA bindings exceed column capacity"); columns=Map.copyOf(columns); merges=Set.copyOf(merges);
            if(!columns.keySet().equals(directory.columns()) || !columns.keySet().containsAll(merges) || new HashSet<>(columns.values()).size()!=columns.size()) throw new IllegalArgumentException("DATA domain physical bindings differ from its directory"); columns.values().forEach(RatisNodePlan::requirePhysicalName);
        }
        public UUID domainId() { return directory.domainId(); }
    }
    private static final Comparator<UUID> UUID_ORDER=(a,b) -> {
        int first=Long.compareUnsigned(a.getMostSignificantBits(),b.getMostSignificantBits());
        return first!=0 ? first : Long.compareUnsigned(a.getLeastSignificantBits(),b.getLeastSignificantBits());
    };
    private static final Set<String> RESERVED_COLUMNS=Set.of("default","_column_schemas_","_merge_operators_","_cdc_meta_",
            "_distributed_range_state_","_distributed_range_receipts_","catalogue-directory","catalogue-sessions");
    public record Catalogue(UUID groupId,long epoch,byte[] schemaDigest,UUID directoryColumn,UUID sessionsColumn,
                            List<ReplicaDescriptor> replicas) {
        public Catalogue {
            Objects.requireNonNull(groupId); Objects.requireNonNull(directoryColumn); Objects.requireNonNull(sessionsColumn);
            if (epoch<0 || schemaDigest.length!=32 || directoryColumn.equals(sessionsColumn)) {
                throw new IllegalArgumentException("Invalid catalogue schema/columns");
            }
            schemaDigest=schemaDigest.clone();
            replicas=new RangeDescriptor(groupId,groupId,epoch,0,null,null,RangeDescriptor.TopologyState.ACTIVE,replicas).replicas();
        }
        @Override public byte[] schemaDigest() { return schemaDigest.clone(); }
        public RaftGroup raftGroup() { return group(groupId,replicas); }
    }
    private record Server(UUID node,UUID incarnation,String peer,String address) {}
    private final UUID nodeId,nodeIncarnation;
    private final UUID clusterId,databaseId;
    private final List<Domain> domains; private final Map<UUID,Domain> domainIndex,groupDomains;
    private final int maxLocalGroups;
    private final Catalogue catalogue;
    private final Map<UUID,String> columns;
    private final Set<UUID> merges;
    private final List<Home> homes;
    private final List<RangeImportDefinition> imports;
    private final Map<UUID,RangeImportDefinition> groupImports;
    private final Map<String,String> streamAddresses;
    private final Map<UUID,Role> roles;
    private final Map<UUID,Home> groupHomes;
    private final String peerId,address;
    private final List<RaftGroup> localGroups;
    private final byte[] identity;
    private final Map<UUID,RatisNativePoolSpec> pools;private final Map<UUID,UUID> domainPools,groupPools;

    public RatisNodePlan(UUID nodeId,UUID nodeIncarnation,Collection<Domain> domains,Catalogue catalogue,List<Home> homes,
            Collection<RangeImportDefinition> imports,Map<String,String> streamAddresses,int maxLocalGroups,Collection<RatisNativePoolSpec> pools,Map<UUID,UUID> domainPools,Map<UUID,UUID> groupPools) {
        this.nodeId=Objects.requireNonNull(nodeId); this.nodeIncarnation=Objects.requireNonNull(nodeIncarnation); this.catalogue=Objects.requireNonNull(catalogue); this.maxLocalGroups=maxLocalGroups;
        if(pools.isEmpty() || pools.size()>RatisNodeResourceBudget.MAX_POOLS || groupPools.size()>MAX_DATA_GROUPS+1+MAX_HOME_GROUPS)throw new IllegalArgumentException("Invalid bounded physical pool placement");var declared=new TreeMap<UUID,RatisNativePoolSpec>(UUID_ORDER);long poolBytes=0;for(var spec:pools){if(declared.put(spec.id(),spec)!=null || (poolBytes+=spec.encode().length)>4L*1024*1024)throw new IllegalArgumentException("Native pool declarations duplicate/exceed metadata budget");}this.pools=Collections.unmodifiableMap(declared);this.domainPools=Map.copyOf(domainPools);this.groupPools=Map.copyOf(groupPools);if(!declared.keySet().containsAll(domainPools.values()) || !declared.keySet().containsAll(groupPools.values()))throw new IllegalArgumentException("Placement names an undeclared native pool");
        if(domains.size()<1 || domains.size()>MAX_DOMAINS || homes.size()>2 || imports.size()>RangeImportDefinition.MAX_GROUPS || streamAddresses.size()>4096) throw new IllegalArgumentException("Node aggregate count capacity exceeded");
        this.domains=domains.stream().sorted(Comparator.comparing(Domain::domainId,UUID_ORDER)).toList();
        if(this.domains.isEmpty() || this.domains.size()>MAX_DOMAINS) throw new IllegalArgumentException("Invalid bounded DATA domain count");
        var first=this.domains.getFirst().directory(); clusterId=first.clusterId(); databaseId=first.databaseId();
        var index=new TreeMap<UUID,Domain>(UUID_ORDER); var byGroup=new HashMap<UUID,Domain>(); var allColumns=new TreeMap<UUID,String>(UUID_ORDER); var allMerges=new HashSet<UUID>(); var physicalNames=new HashSet<String>(); var ranges=new HashSet<UUID>(); ranges.add(catalogue.groupId()); long domainBytes=0,metadataBytes=poolBytes; int dataGroups=0;
        for(var domain:this.domains) {
            var directory=domain.directory(); domainBytes+=RangeDirectoryCodec.encode(directory).length;
            if(!clusterId.equals(directory.clusterId()) || !databaseId.equals(directory.databaseId()) || directory.domainId().equals(catalogue.groupId()) || index.put(directory.domainId(),domain)!=null || domainBytes>MAX_DOMAIN_BYTES) throw new IllegalArgumentException("DATA domain namespace/identity/metadata budget differs");
            for(var binding:domain.columns().entrySet()) { if(allColumns.put(binding.getKey(),binding.getValue())!=null || !physicalNames.add(binding.getValue())) throw new IllegalArgumentException("DATA domains overlap column or physical identities"); metadataBytes+=20L+binding.getValue().length(); }
            allMerges.addAll(domain.merges());
            for(var range:directory.ranges()) if(!ranges.add(range.rangeId()) || byGroup.put(range.groupId(),domain)!=null) throw new IllegalArgumentException("DATA domains overlap range/group identities");
            dataGroups+=directory.ranges().size();
        }
        if(allColumns.size()>RangeDirectoryCodec.MAX_COLUMNS || dataGroups>MAX_DATA_GROUPS) throw new IllegalArgumentException("Aggregate DATA column/group capacity exceeded");
        domainIndex=Collections.unmodifiableMap(index); groupDomains=Map.copyOf(byGroup); columns=Collections.unmodifiableMap(allColumns); merges=Set.copyOf(allMerges); metadataBytes+=domainBytes;
        this.homes=homes.stream().sorted(Comparator.comparing(Home::role)).toList();
        var imported=new HashMap<UUID,RangeImportDefinition>(); var jobs=new HashSet<UUID>(); long importBytes=0;
        this.imports=imports.stream().sorted(Comparator.comparing((RangeImportDefinition definition)->definition.sourceDirectory().domainId(),UUID_ORDER).thenComparingLong(definition->definition.sourceDirectory().generation()).thenComparing(RangeImportDefinition::importId,UUID_ORDER)).toList();
        if(this.imports.size()>RangeImportDefinition.MAX_GROUPS) throw new IllegalArgumentException("Node import definition count exceeds bound");
        var importDirectories=new HashMap<UUID,RangeDirectory>();domainIndex.forEach((id,domain)->importDirectories.put(id,domain.directory()));
        for(var definition:this.imports) {
            var domain=domainIndex.get(definition.sourceDirectory().domainId()); importBytes+=definition.encode().length;
            if(domain==null || importBytes>RangeImportDefinition.MAX_BYTES || !jobs.add(definition.importId()) || !Arrays.equals(importDirectories.get(domain.domainId()).payloadDigest(),definition.sourceDirectory().payloadDigest())) throw new IllegalArgumentException("Node import differs from its exact frozen DATA domain");
            importDirectories.put(domain.domainId(),definition.targetDirectory());
            for(var manifest:definition.manifests()) if(!manifest.physicalNames().equals(domain.columns()) || !manifest.mergeColumns().equals(domain.merges()) || groupDomains.get(manifest.groupId())!=domain || imported.put(manifest.groupId(),definition)!=null) throw new IllegalArgumentException("Node import physical bindings/target ownership differ");
        }
        groupImports=Map.copyOf(imported); metadataBytes+=importBytes;
        int homeGroups=0; long homeBytes=0; var homeRoles=EnumSet.noneOf(Role.class); var namespaceDomains=new HashSet<>(domainIndex.keySet()); namespaceDomains.add(catalogue.groupId()); var columnIds=new HashSet<>(columns.keySet());
        if(!columnIds.add(catalogue.directoryColumn()) || !columnIds.add(catalogue.sessionsColumn())) throw new IllegalArgumentException("Metadata/DATA column identities overlap");
        for(var home:this.homes) {
            if(!homeRoles.add(home.role()) || !namespaceDomains.add(home.directory().domainId()) || !columnIds.add(home.column()) || !physicalNames.add(home.physicalName()) || !home.directory().clusterId().equals(clusterId) || !home.directory().databaseId().equals(databaseId)) throw new IllegalArgumentException("HOME role/domain/column/database identity differs or overlaps");
            for(var range:home.directory().ranges()) if(!ranges.add(range.rangeId())) throw new IllegalArgumentException("HOME/DATA range identity overlaps");
            homeGroups+=home.directory().ranges().size(); homeBytes+=RangeDirectoryCodec.encode(home.directory()).length; metadataBytes+=25L+home.physicalName().length();
        }
        metadataBytes+=homeBytes;
        if(homeGroups>MAX_HOME_GROUPS || homeBytes>MAX_HOME_DIRECTORY_BYTES || maxLocalGroups<1 || maxLocalGroups>MAX_DATA_GROUPS+1+homeGroups) throw new IllegalArgumentException("Invalid node HOME/local group budget");
        var nodes=new HashMap<UUID,Server>(); var peers=new HashMap<String,Server>(); var endpoints=new HashMap<String,Server>(); var groups=new TreeMap<UUID,RaftGroup>(UUID_ORDER); var roles=new HashMap<UUID,Role>(); var groupHomes=new HashMap<UUID,Home>();
        roles.put(catalogue.groupId(),Role.CATALOGUE); inspect(catalogue.groupId(),catalogue.replicas(),nodes,peers,endpoints,groups);
        for(var domain:this.domains) for(var range:domain.directory().ranges()) {
            if(range.topologyState()!=RangeDescriptor.TopologyState.ACTIVE && (range.topologyState()!=RangeDescriptor.TopologyState.FENCED || !groupImports.containsKey(range.groupId())) || roles.putIfAbsent(range.groupId(),Role.DATA)!=null) throw new IllegalArgumentException("Static DATA join requires active or exactly qualified import groups"); inspect(range.groupId(),range.replicas(),nodes,peers,endpoints,groups);
        }
        for(var home:this.homes) for(var range:home.directory().ranges()) { if(roles.putIfAbsent(range.groupId(),home.role())!=null) throw new IllegalArgumentException("Raft group has multiple roles"); groupHomes.put(range.groupId(),home); inspect(range.groupId(),range.replicas(),nodes,peers,endpoints,groups); }
        this.roles=Map.copyOf(roles); this.groupHomes=Map.copyOf(groupHomes); Server local=nodes.get(nodeId);
        if(local==null || !local.incarnation().equals(nodeIncarnation) || groups.isEmpty() || groups.size()>maxLocalGroups) throw new IllegalArgumentException("Node lacks current placement or exceeds configured local group capacity");
        this.streamAddresses=Map.copyOf(streamAddresses);
        if(!this.streamAddresses.isEmpty()) {
            if(this.streamAddresses.size()>4096 || !this.streamAddresses.keySet().equals(peers.keySet())) throw new IllegalArgumentException("Stream endpoints must cover every frozen peer"); var streams=new HashSet<String>();
            for(var entry:this.streamAddresses.entrySet()) { var server=peers.get(entry.getKey()); new ReplicaDescriptor(server.node(),server.incarnation(),server.node(),server.peer(),entry.getValue(),ReplicaDescriptor.Role.VOTER); if(!streams.add(entry.getValue()) || endpoints.containsKey(entry.getValue())) throw new IllegalArgumentException("Stream endpoint overlaps another stream or Raft endpoint"); metadataBytes+=8L+entry.getKey().length()+entry.getValue().length(); }
            groups.replaceAll((id,group)->streaming(group));
        }
        if(metadataBytes>MAX_METADATA_BYTES) throw new IllegalArgumentException("Aggregate node metadata exceeds byte capacity");
        if(!this.domainPools.keySet().equals(domainIndex.keySet()) || !this.groupPools.containsKey(catalogue.groupId()))throw new IllegalArgumentException("Pool placements must cover all DATA domains and catalogue");for(var home:this.homes)for(var range:home.directory().ranges())if(!this.groupPools.containsKey(range.groupId()))throw new IllegalArgumentException("Dedicated HOME pool placement missing");var counts=new HashMap<UUID,Integer>();for(var group:groups.values()){var assigned=poolId(group.getGroupId().getUuid());if(counts.merge(assigned,1,Integer::sum)>this.pools.get(assigned).maximumGroups())throw new IllegalArgumentException("Physical pool group ceiling exceeded");}
        peerId=local.peer(); address=local.address(); localGroups=List.copyOf(groups.values()); identity=digest();
    }
    private void inspect(UUID group,List<ReplicaDescriptor> replicas,Map<UUID,Server> nodes,Map<String,Server> peers,
            Map<String,Server> endpoints,Map<UUID,RaftGroup> localGroups) {
        int local=0;
        for (var replica:replicas) {
            var server=new Server(replica.nodeId(),replica.nodeIncarnation(),replica.peerId(),replica.address());
            var sameNode=nodes.putIfAbsent(server.node(),server);
            var samePeer=peers.putIfAbsent(server.peer(),server);
            var sameEndpoint=endpoints.putIfAbsent(server.address(),server);
            if ((sameNode!=null && !sameNode.equals(server)) || (samePeer!=null && !samePeer.equals(server))
                    || (sameEndpoint!=null && !sameEndpoint.equals(server))) {
                throw new IllegalArgumentException("Conflicting global Ratis node/incarnation/peer/endpoint identity");
            }
            if (replica.nodeId().equals(nodeId) && replica.nodeIncarnation().equals(nodeIncarnation)) local++;
        }
        if (local>1) throw new IllegalArgumentException("Duplicate local placement in group");
        if (local==1) localGroups.put(group,group(group,replicas));
    }
    private static RaftGroup group(UUID group,List<ReplicaDescriptor> replicas) {
        return RaftGroup.valueOf(RaftGroupId.valueOf(group),replicas.stream().map(replica -> RaftPeer.newBuilder()
                .setId(replica.peerId()).setAddress(replica.address())
                .setStartupRole(replica.role()==ReplicaDescriptor.Role.VOTER ? RaftPeerRole.FOLLOWER : RaftPeerRole.LISTENER).build()).toList());
    }
    public Map<UUID,RatisNativePoolSpec> pools(){return pools;}public Map<UUID,UUID> domainPools(){return domainPools;}public Map<UUID,UUID> groupPools(){return groupPools;}
    public UUID poolId(UUID group){var override=groupPools.get(group);if(override!=null)return override;var domain=groupDomains.get(group);if(domain==null)throw new IllegalArgumentException("Group has no declared pool placement");return domainPools.get(domain.domainId());}
    public UUID incomingPoolId(UUID domain,UUID group){domain(domain);return groupPools.getOrDefault(group,domainPools.get(domain));}
    public UUID nodeId() { return nodeId; }
    public UUID nodeIncarnation() { return nodeIncarnation; }
    public UUID clusterId() { return clusterId; } public UUID databaseId() { return databaseId; }
    public List<Domain> domains() { return domains; }
    public Domain domain(UUID id) { var domain=domainIndex.get(id); if(domain==null) throw new IllegalArgumentException("Unplanned DATA domain"); return domain; }
    public RangeDirectory directory(UUID domain) { return domain(domain).directory(); }
    public int maxLocalGroups() { return maxLocalGroups; }
    public Catalogue catalogue() { return catalogue; }
    public Map<UUID,String> columns() { return columns; }
    public Set<UUID> merges() { return merges; }
    public List<Home> homes() { return homes; }
    public List<RangeImportDefinition> imports() { return imports; }
    public Map<String,String> streamAddresses() { return streamAddresses; }
    private RaftGroup streaming(RaftGroup group) {
        if(streamAddresses.isEmpty())return group;
        return RaftGroup.valueOf(group.getGroupId(),group.getPeers().stream().map(peer->RaftPeer.newBuilder(peer)
                .setDataStreamAddress(streamAddresses.get(peer.getId().toString())).build()).toList());
    }
    public RangeImportDefinition importDefinition(UUID group) { role(group); return groupImports.get(group); }
    public Role role(UUID group) { var role=roles.get(group); if (role==null) throw new IllegalArgumentException("Unplanned Raft group"); return role; }
    public Home home(UUID group) { role(group); return groupHomes.get(group); }
    public Home home(Role role) { return homes.stream().filter(home -> home.role()==role).findFirst().orElse(null); }
    public RangeDirectory groupDirectory(UUID group) {
        return switch (role(group)) { case DATA -> groupDomains.get(group).directory(); case TRANSACTION_HOME, SNAPSHOT_HOME -> home(group).directory(); case CATALOGUE -> throw new IllegalArgumentException("Catalogue has no physical group directory"); };
    }
    public RangeDescriptor groupDescriptor(UUID group) { return groupDirectory(group).ranges().stream().filter(range -> range.groupId().equals(group)).findFirst().orElseThrow(); }
    public String peerId() { return peerId; }
    public String address() { return address; }
    public List<RaftGroup> localGroups() { return localGroups; }
    public RaftGroup catalogueGroup() { return streaming(catalogue.raftGroup()); }
    public byte[] identityDigest() { return identity.clone(); }

    private byte[] digest() {
        try {
            var bytes=new ByteArrayOutputStream(); var output=new DataOutputStream(bytes);
            output.writeInt(pools.size());for(var spec:pools.values()){var encoded=spec.encode();output.writeInt(encoded.length);output.write(encoded);}for(var placement:List.of(domainPools,groupPools)){output.writeInt(placement.size());for(var id:placement.keySet().stream().sorted(UUID_ORDER).toList()){uuid(output,id);uuid(output,placement.get(id));}}
            uuid(output,clusterId); uuid(output,databaseId); output.writeInt(maxLocalGroups); output.writeInt(domains.size());
            for(var domain:domains) { var directory=domain.directory(); uuid(output,directory.domainId()); output.write(new RangeDirectory(directory.clusterId(),directory.databaseId(),directory.domainId(),directory.schemaEpoch(),directory.schemaDigest(),0,directory.columns(),directory.routingPrefixBytes(),directory.ranges()).payloadDigest()); }
            uuid(output,catalogue.groupId()); output.writeLong(catalogue.epoch()); output.write(catalogue.schemaDigest());
            uuid(output,catalogue.directoryColumn()); uuid(output,catalogue.sessionsColumn()); uuid(output,nodeId); uuid(output,nodeIncarnation);
            output.writeUTF(peerId); output.writeUTF(address); output.writeInt(columns.size());
            for (UUID column:columns.keySet().stream().sorted(UUID_ORDER).toList()) {
                uuid(output,column); byte[] name=columns.get(column).getBytes(StandardCharsets.US_ASCII);
                output.writeInt(name.length); output.write(name); output.writeBoolean(merges.contains(column));
            }
            output.writeInt(homes.size());
            for (var home:homes) {
                output.writeByte(home.role().ordinal()); byte[] placement=RangeDirectoryCodec.encode(home.directory()); output.writeInt(placement.length); output.write(placement);
                uuid(output,home.column()); byte[] name=home.physicalName().getBytes(StandardCharsets.US_ASCII); output.writeInt(name.length); output.write(name);
            }
            output.writeInt(imports.size()); for(var definition:imports) output.write(definition.digest());
            output.writeInt(streamAddresses.size()); for(var peer:streamAddresses.keySet().stream().sorted().toList()){output.writeUTF(peer);output.writeUTF(streamAddresses.get(peer));}
            output.writeInt(localGroups.size());
            for (var group:localGroups) {
                UUID id=group.getGroupId().getUuid(); uuid(output,id); output.writeByte(role(id).ordinal());
                List<ReplicaDescriptor> replicas;
                if (id.equals(catalogue.groupId())) {
                    output.writeLong(catalogue.epoch()); uuid(output,id); bound(output,null); bound(output,null); replicas=catalogue.replicas();
                } else {
                    var range=groupDescriptor(id);
                    output.writeLong(range.rangeEpoch()); uuid(output,range.rangeId()); bound(output,range.lowerInclusive()); bound(output,range.upperExclusive()); replicas=range.replicas();
                }
                var local=replicas.stream().filter(replica -> replica.nodeId().equals(nodeId) && replica.nodeIncarnation().equals(nodeIncarnation)).findFirst().orElseThrow();
                uuid(output,local.replicaId());
            }
            output.flush(); return MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray());
        } catch (IOException impossible) { throw new UncheckedIOException(impossible); }
        catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
    private static void requirePhysicalName(String name) {
        if (name==null || name.isEmpty() || name.length()>MAX_PHYSICAL_NAME_BYTES || name.indexOf('\0')>=0 || RESERVED_COLUMNS.contains(name) || !StandardCharsets.US_ASCII.newEncoder().canEncode(name)) throw new IllegalArgumentException("Invalid physical column name");
    }
    private static void bound(DataOutputStream output,byte[] bytes) throws IOException {
        output.writeInt(bytes==null ? -1 : bytes.length); if (bytes!=null) output.write(bytes);
    }
    private static void uuid(DataOutputStream output,UUID id) throws IOException {
        output.writeLong(id.getMostSignificantBits()); output.writeLong(id.getLeastSignificantBits());
    }
}
