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
    private final String peerId,address;
    private final List<RaftGroup> localGroups;
    private final byte[] identity;

    public RatisNodePlan(UUID nodeId,UUID nodeIncarnation,RangeDirectory directory,Catalogue catalogue,
            Map<UUID,String> columns,Set<UUID> merges,int maxLocalGroups) {
        this.nodeId=Objects.requireNonNull(nodeId); this.nodeIncarnation=Objects.requireNonNull(nodeIncarnation);
        this.directory=Objects.requireNonNull(directory); this.catalogue=Objects.requireNonNull(catalogue);
        this.columns=Map.copyOf(columns); this.merges=Set.copyOf(merges);
        if (maxLocalGroups<1 || maxLocalGroups>RangeDirectoryCodec.MAX_RANGES+1
                || !this.columns.keySet().equals(directory.columns()) || !this.columns.keySet().containsAll(this.merges)
                || new HashSet<>(this.columns.values()).size()!=this.columns.size()
                || directory.columns().contains(catalogue.directoryColumn()) || directory.columns().contains(catalogue.sessionsColumn())) {
            throw new IllegalArgumentException("Invalid node group budget or physical column bindings");
        }
        for (String name:this.columns.values()) {
            if (name.isEmpty() || name.indexOf('\0')>=0 || RESERVED_COLUMNS.contains(name)
                    || !StandardCharsets.US_ASCII.newEncoder().canEncode(name)) throw new IllegalArgumentException("Invalid physical column name");
        }
        var nodes=new HashMap<UUID,Server>(); var peers=new HashMap<String,Server>(); var endpoints=new HashMap<String,Server>();
        var groups=new TreeMap<UUID,RaftGroup>(UUID_ORDER);
        inspect(catalogue.groupId(),catalogue.replicas(),nodes,peers,endpoints,groups);
        for (var range:directory.ranges()) {
            if (range.topologyState()!=RangeDescriptor.TopologyState.ACTIVE || range.groupId().equals(catalogue.groupId())) {
                throw new IllegalArgumentException("Static node join requires active data groups distinct from catalogue");
            }
            inspect(range.groupId(),range.replicas(),nodes,peers,endpoints,groups);
        }
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
    public String peerId() { return peerId; }
    public String address() { return address; }
    public List<RaftGroup> localGroups() { return localGroups; }
    public RaftGroup catalogueGroup() { return catalogue.raftGroup(); }
    public byte[] identityDigest() { return identity.clone(); }

    private byte[] digest() {
        try {
            var bytes=new ByteArrayOutputStream(); var output=new DataOutputStream(bytes);
            output.writeInt(1); uuid(output,directory.clusterId()); uuid(output,directory.databaseId()); uuid(output,directory.domainId());
            output.writeLong(directory.schemaEpoch()); output.write(directory.schemaDigest()); output.writeInt(directory.routingPrefixBytes());
            uuid(output,catalogue.groupId()); output.writeLong(catalogue.epoch()); output.write(catalogue.schemaDigest());
            uuid(output,catalogue.directoryColumn()); uuid(output,catalogue.sessionsColumn()); uuid(output,nodeId); uuid(output,nodeIncarnation);
            output.writeUTF(peerId); output.writeUTF(address); output.writeInt(columns.size());
            for (UUID column:columns.keySet().stream().sorted(UUID_ORDER).toList()) {
                uuid(output,column); byte[] name=columns.get(column).getBytes(StandardCharsets.US_ASCII);
                output.writeInt(name.length); output.write(name); output.writeBoolean(merges.contains(column));
            }
            output.writeInt(localGroups.size());
            for (var group:localGroups) {
                UUID id=group.getGroupId().getUuid(); uuid(output,id);
                List<ReplicaDescriptor> replicas;
                if (id.equals(catalogue.groupId())) {
                    output.writeLong(catalogue.epoch()); uuid(output,id); bound(output,null); bound(output,null); replicas=catalogue.replicas();
                } else {
                    var range=directory.ranges().stream().filter(candidate -> candidate.groupId().equals(id)).findFirst().orElseThrow();
                    output.writeLong(range.rangeEpoch()); uuid(output,range.rangeId()); bound(output,range.lowerInclusive()); bound(output,range.upperExclusive()); replicas=range.replicas();
                }
                var local=replicas.stream().filter(replica -> replica.nodeId().equals(nodeId) && replica.nodeIncarnation().equals(nodeIncarnation)).findFirst().orElseThrow();
                uuid(output,local.replicaId());
            }
            output.flush(); return MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray());
        } catch (IOException impossible) { throw new UncheckedIOException(impossible); }
        catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
    private static void bound(DataOutputStream output,byte[] bytes) throws IOException {
        output.writeInt(bytes==null ? -1 : bytes.length); if (bytes!=null) output.write(bytes);
    }
    private static void uuid(DataOutputStream output,UUID id) throws IOException {
        output.writeLong(id.getMostSignificantBits()); output.writeLong(id.getLeastSignificantBits());
    }
}
