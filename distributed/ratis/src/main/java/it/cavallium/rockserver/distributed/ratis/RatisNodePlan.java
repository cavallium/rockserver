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
    public static final int MAX_HOME_GROUPS=64, MAX_HOME_DIRECTORY_BYTES=2*1024*1024, MAX_LOCAL_GROUPS=RangeDirectoryCodec.MAX_RANGES+1+MAX_HOME_GROUPS;
    public enum Role { DATA, CATALOGUE, TRANSACTION_HOME, SNAPSHOT_HOME }
    public record Home(Role role,RangeDirectory directory,UUID column,String physicalName) {
        public Home {
            Objects.requireNonNull(role); Objects.requireNonNull(directory); Objects.requireNonNull(column); requirePhysicalName(physicalName);
            if (role!=Role.TRANSACTION_HOME && role!=Role.SNAPSHOT_HOME || !directory.columns().equals(Set.of(column)) || directory.routingPrefixBytes()!=0
                    || directory.ranges().size()>MAX_HOME_GROUPS || directory.ranges().stream().anyMatch(range -> range.topologyState()!=RangeDescriptor.TopologyState.ACTIVE)) throw new IllegalArgumentException("Invalid dedicated home placement");
        }
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
    private final RangeDirectory directory;
    private final Catalogue catalogue;
    private final Map<UUID,String> columns;
    private final Set<UUID> merges;
    private final List<Home> homes;
    private final Map<UUID,Role> roles;
    private final Map<UUID,Home> groupHomes;
    private final String peerId,address;
    private final List<RaftGroup> localGroups;
    private final byte[] identity;

    public RatisNodePlan(UUID nodeId,UUID nodeIncarnation,RangeDirectory directory,Catalogue catalogue,
            Map<UUID,String> columns,Set<UUID> merges,int maxLocalGroups) {
        this(nodeId,nodeIncarnation,directory,catalogue,columns,merges,List.of(),maxLocalGroups);
    }
    public RatisNodePlan(UUID nodeId,UUID nodeIncarnation,RangeDirectory directory,Catalogue catalogue,
            Map<UUID,String> columns,Set<UUID> merges,List<Home> homes,int maxLocalGroups) {
        this.nodeId=Objects.requireNonNull(nodeId); this.nodeIncarnation=Objects.requireNonNull(nodeIncarnation);
        this.directory=Objects.requireNonNull(directory); this.catalogue=Objects.requireNonNull(catalogue);
        this.columns=Map.copyOf(columns); this.merges=Set.copyOf(merges); this.homes=homes.stream().sorted(Comparator.comparing(Home::role)).toList();
        int homeGroups=0; long homeBytes=0; var homeRoles=EnumSet.noneOf(Role.class); var domains=new HashSet<>(Set.of(directory.domainId())); domains.add(catalogue.groupId());
        var columnIds=new HashSet<>(directory.columns());
        if (!columnIds.add(catalogue.directoryColumn()) || !columnIds.add(catalogue.sessionsColumn())) throw new IllegalArgumentException("Metadata/data column identities overlap");
        var physicalNames=new HashSet<>(this.columns.values());
        if (!this.columns.keySet().equals(directory.columns()) || !this.columns.keySet().containsAll(this.merges) || physicalNames.size()!=this.columns.size()) throw new IllegalArgumentException("Invalid physical data bindings");
        this.columns.values().forEach(RatisNodePlan::requirePhysicalName);
        for (var home:this.homes) {
            if (!homeRoles.add(home.role()) || !domains.add(home.directory().domainId()) || !columnIds.add(home.column()) || !physicalNames.add(home.physicalName())
                    || !home.directory().clusterId().equals(directory.clusterId()) || !home.directory().databaseId().equals(directory.databaseId())) throw new IllegalArgumentException("Home role/domain/column/database identity overlaps or differs");
            homeGroups+=home.directory().ranges().size(); homeBytes+=RangeDirectoryCodec.encode(home.directory()).length;
        }
        if (homeGroups>MAX_HOME_GROUPS || homeBytes>MAX_HOME_DIRECTORY_BYTES || maxLocalGroups<1 || maxLocalGroups>RangeDirectoryCodec.MAX_RANGES+1+homeGroups) throw new IllegalArgumentException("Invalid node home/local group budget");
        var nodes=new HashMap<UUID,Server>(); var peers=new HashMap<String,Server>(); var endpoints=new HashMap<String,Server>();
        var groups=new TreeMap<UUID,RaftGroup>(UUID_ORDER); var roles=new HashMap<UUID,Role>(); var groupHomes=new HashMap<UUID,Home>();
        roles.put(catalogue.groupId(),Role.CATALOGUE); inspect(catalogue.groupId(),catalogue.replicas(),nodes,peers,endpoints,groups);
        for (var range:directory.ranges()) {
            if (range.topologyState()!=RangeDescriptor.TopologyState.ACTIVE || roles.putIfAbsent(range.groupId(),Role.DATA)!=null) throw new IllegalArgumentException("Static node join requires distinct active data groups");
            inspect(range.groupId(),range.replicas(),nodes,peers,endpoints,groups);
        }
        for (var home:this.homes) for (var range:home.directory().ranges()) {
            if (roles.putIfAbsent(range.groupId(),home.role())!=null) throw new IllegalArgumentException("Raft group is assigned to multiple roles");
            groupHomes.put(range.groupId(),home); inspect(range.groupId(),range.replicas(),nodes,peers,endpoints,groups);
        }
        this.roles=Map.copyOf(roles); this.groupHomes=Map.copyOf(groupHomes);
        Server local=nodes.get(nodeId);
        if (local==null || !local.incarnation().equals(nodeIncarnation) || groups.isEmpty() || groups.size()>maxLocalGroups) {
            throw new IllegalArgumentException("Node has no current placement or exceeds local group budget");
        }
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
    public UUID nodeId() { return nodeId; }
    public UUID nodeIncarnation() { return nodeIncarnation; }
    public RangeDirectory directory() { return directory; }
    public Catalogue catalogue() { return catalogue; }
    public Map<UUID,String> columns() { return columns; }
    public Set<UUID> merges() { return merges; }
    public List<Home> homes() { return homes; }
    public Role role(UUID group) { var role=roles.get(group); if (role==null) throw new IllegalArgumentException("Unplanned Raft group"); return role; }
    public Home home(UUID group) { role(group); return groupHomes.get(group); }
    public Home home(Role role) { return homes.stream().filter(home -> home.role()==role).findFirst().orElse(null); }
    public RangeDirectory groupDirectory(UUID group) {
        return switch (role(group)) { case DATA -> directory; case TRANSACTION_HOME, SNAPSHOT_HOME -> home(group).directory(); case CATALOGUE -> throw new IllegalArgumentException("Catalogue has no physical group directory"); };
    }
    public RangeDescriptor groupDescriptor(UUID group) { return groupDirectory(group).ranges().stream().filter(range -> range.groupId().equals(group)).findFirst().orElseThrow(); }
    public String peerId() { return peerId; }
    public String address() { return address; }
    public List<RaftGroup> localGroups() { return localGroups; }
    public RaftGroup catalogueGroup() { return catalogue.raftGroup(); }
    public byte[] identityDigest() { return identity.clone(); }

    private byte[] digest() {
        try {
            var bytes=new ByteArrayOutputStream(); var output=new DataOutputStream(bytes);
            uuid(output,directory.clusterId()); uuid(output,directory.databaseId()); uuid(output,directory.domainId());
            output.writeLong(directory.schemaEpoch()); output.write(directory.schemaDigest()); output.writeInt(directory.routingPrefixBytes());
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
        if (name==null || name.isEmpty() || name.indexOf('\0')>=0 || RESERVED_COLUMNS.contains(name) || !StandardCharsets.US_ASCII.newEncoder().canEncode(name)) throw new IllegalArgumentException("Invalid physical column name");
    }
    private static void bound(DataOutputStream output,byte[] bytes) throws IOException {
        output.writeInt(bytes==null ? -1 : bytes.length); if (bytes!=null) output.write(bytes);
    }
    private static void uuid(DataOutputStream output,UUID id) throws IOException {
        output.writeLong(id.getMostSignificantBits()); output.writeLong(id.getLeastSignificantBits());
    }
}
