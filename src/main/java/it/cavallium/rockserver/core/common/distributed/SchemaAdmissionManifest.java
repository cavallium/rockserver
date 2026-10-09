package it.cavallium.rockserver.core.common.distributed;

import java.nio.*;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.*;

/** Complete frozen replica admissions, committing local bindings without repeating schema or node metadata. */
public final class SchemaAdmissionManifest {
    public static final int MAX_BYTES=512*1024;
    private static final int MAGIC=0x53414d46;
    public record Binding(UUID groupId,String changedPhysicalName,boolean merge) {
        public Binding { Objects.requireNonNull(groupId); if(changedPhysicalName!=null && (changedPhysicalName.isEmpty() || changedPhysicalName.length()>4096 || changedPhysicalName.indexOf('\0')>=0 || !StandardCharsets.US_ASCII.newEncoder().canEncode(changedPhysicalName))) throw new IllegalArgumentException("Invalid admission physical name"); }
    }
    public record ReplicaAdmission(UUID groupId,UUID replicaId,NativeSchemaCapacityReservation reservation) {
        public ReplicaAdmission { Objects.requireNonNull(groupId); Objects.requireNonNull(replicaId); Objects.requireNonNull(reservation); }
    }
    private record Owner(UUID group,UUID replica) {}
    private final SchemaChange.Header header;
    private final RangeDirectory directory;
    private final List<Binding> bindings;
    private final List<ReplicaAdmission> replicas;
    private final Map<UUID,Binding> byGroup;
    private final Map<Owner,ReplicaAdmission> byReplica;
    public SchemaAdmissionManifest(SchemaChange.Definition definition,Collection<Binding> bindings,Collection<ReplicaAdmission> replicas) {
        this(Objects.requireNonNull(definition).header(),definition.sourceDirectory(),bindings,replicas); requireDefinition(definition);
    }
    private SchemaAdmissionManifest(SchemaChange.Header header,RangeDirectory directory,Collection<Binding> bindings,Collection<ReplicaAdmission> replicas) {
        this.header=Objects.requireNonNull(header); this.directory=Objects.requireNonNull(directory); requireDirectory(header,directory);
        if(bindings.size()!=header.participants().size() || replicas.size()>SchemaChange.MAX_PARTICIPANTS*RangeDirectoryCodec.MAX_REPLICAS) throw new IllegalArgumentException("Incomplete/excess admission coverage");
        var groups=new HashMap<UUID,Binding>(); var owners=new HashMap<Owner,ReplicaAdmission>(); var descriptors=new HashMap<UUID,RangeDescriptor>();
        for(var descriptor:directory.ranges()) descriptors.put(descriptor.groupId(),descriptor);
        for(var binding:bindings) {
            if(!descriptors.containsKey(binding.groupId()) || groups.put(binding.groupId(),binding)!=null) throw new IllegalArgumentException("Duplicate/foreign admission binding");
            if(header.kind()==SchemaChange.Kind.DELETE_COLUMN ? binding.changedPhysicalName()!=null || binding.merge() : binding.changedPhysicalName()==null) throw new IllegalArgumentException("Admission binding differs from schema change kind");
        }
        int expected=0; for(var descriptor:directory.ranges()) expected+=descriptor.replicas().size();
        if(replicas.size()!=expected) throw new IllegalArgumentException("Every frozen voter/listener requires admission");
        for(var replica:replicas) {
            var descriptor=descriptors.get(replica.groupId());
            if(descriptor==null || descriptor.replicas().stream().noneMatch(owner -> owner.replicaId().equals(replica.replicaId())) || owners.put(new Owner(replica.groupId(),replica.replicaId()),replica)!=null) throw new IllegalArgumentException("Duplicate/foreign replica admission");
            replica.reservation().requireHeader(header);
            if(!replica.reservation().participant().groupId().equals(replica.groupId())) throw new IllegalArgumentException("Admission proof belongs to another group");
        }
        for(var descriptor:directory.ranges()) for(var owner:descriptor.replicas()) if(!owners.containsKey(new Owner(descriptor.groupId(),owner.replicaId()))) throw new IllegalArgumentException("Missing frozen replica admission");
        this.bindings=groups.values().stream().sorted(Comparator.comparing(Binding::groupId,RangeDirectoryCodec.UUID_ORDER)).toList();
        this.replicas=owners.values().stream().sorted(Comparator.comparing(ReplicaAdmission::groupId,RangeDirectoryCodec.UUID_ORDER).thenComparing(ReplicaAdmission::replicaId,RangeDirectoryCodec.UUID_ORDER)).toList();
        byGroup=Map.copyOf(groups); byReplica=Map.copyOf(owners);
        if(encodedSize()>MAX_BYTES) throw new IllegalArgumentException("Admission manifest exceeds byte budget");
    }
    private static void requireDirectory(SchemaChange.Header header,RangeDirectory directory) {
        byte[] digest=directory.payloadDigest();
        if(!Arrays.equals(digest,header.sourceDirectoryDigest()) && !Arrays.equals(digest,header.targetDirectoryDigest())) throw new IllegalArgumentException("Admission directory is not the exact source/target authority");
        if(directory.ranges().size()!=header.participants().size()) throw new IllegalArgumentException("Admission participant coverage differs");
        for(var participant:header.participants()) {
            var range=directory.ranges().stream().filter(r -> r.groupId().equals(participant.groupId())).findFirst().orElseThrow(() -> new IllegalArgumentException("Missing admission group"));
            if(!range.rangeId().equals(participant.rangeId()) || range.rangeEpoch()!=participant.rangeEpoch() || range.configurationVersion()!=participant.configurationVersion() || range.topologyState()!=RangeDescriptor.TopologyState.ACTIVE) throw new IllegalArgumentException("Admission directory participant stamp changed");
        }
    }
    public SchemaChange.Header header() { return header; }
    public List<Binding> bindings() { return bindings; }
    public List<ReplicaAdmission> replicas() { return replicas; }
    public Binding binding(UUID group) { return Objects.requireNonNull(byGroup.get(group),"Group outside admission manifest"); }
    public ReplicaAdmission replica(UUID group,UUID replica) { return Objects.requireNonNull(byReplica.get(new Owner(group,replica)),"Replica outside admission manifest"); }
    public ReplicaDescriptor owner(UUID group,UUID replica) {
        replica(group,replica); return directory.ranges().stream().filter(r -> r.groupId().equals(group)).findFirst().orElseThrow().replicas().stream().filter(r -> r.replicaId().equals(replica)).findFirst().orElseThrow();
    }
    public void requireDefinition(SchemaChange.Definition definition) {
        if(!Arrays.equals(header.encode(),definition.header().encode())) throw new IllegalArgumentException("Admission definition commitment changed");
        requireDirectory(header,definition.sourceDirectory());
        if(header.kind()==SchemaChange.Kind.CREATE_COLUMN) {
            boolean merge=definition.targetSchema().column(header.changedColumnId()).operator()!=null;
            for(var binding:bindings) if(binding.merge()!=merge) throw new IllegalArgumentException("Admission CREATE merge differs from changed operator");
        }
    }
    private int encodedSize() { long size=16L+header.encode().length+204L*replicas.size(); for(var binding:bindings) size+=21L+(binding.changedPhysicalName()==null ? 0 : binding.changedPhysicalName().length()); return Math.toIntExact(size); }
    public byte[] encode() {
        var out=ByteBuffer.allocate(encodedSize()).putInt(MAGIC); byte[] encoded=header.encode(); out.putInt(encoded.length).put(encoded).putInt(bindings.size());
        for(var binding:bindings) { uuid(out,binding.groupId()); if(binding.changedPhysicalName()==null) out.putInt(-1); else { byte[] name=binding.changedPhysicalName().getBytes(StandardCharsets.US_ASCII); out.putInt(name.length).put(name); } out.put((byte)(binding.merge() ? 1 : 0)); }
        out.putInt(replicas.size()); for(var replica:replicas) { uuid(out,replica.groupId()); uuid(out,replica.replicaId()); out.put(replica.reservation().encode()); } return out.array();
    }
    public byte[] digest() { try { return MessageDigest.getInstance("SHA-256").digest(encode()); } catch(NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); } }
    public static SchemaAdmissionManifest decode(byte[] bytes,SchemaChange.Definition definition) { var result=decode(bytes,definition.header(),definition.sourceDirectory()); result.requireDefinition(definition); return result; }
    public static SchemaAdmissionManifest decode(byte[] bytes,SchemaChange.Header expected,RangeDirectory directory) {
        Objects.requireNonNull(bytes); if(bytes.length<16 || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid admission manifest size");
        try {
            var in=ByteBuffer.wrap(bytes); if(in.getInt()!=MAGIC) throw new IllegalArgumentException("Invalid admission manifest tag");
            int headerLength=in.getInt(); if(headerLength<1 || headerLength>in.remaining()) throw new IllegalArgumentException("Invalid admission header length"); byte[] headerBytes=new byte[headerLength]; in.get(headerBytes); var header=SchemaChange.Header.decode(headerBytes);
            if(!Arrays.equals(headerBytes,expected.encode())) throw new IllegalArgumentException("Admission header commitment changed");
            int count=in.getInt(); if(count<1 || count>SchemaChange.MAX_PARTICIPANTS) throw new IllegalArgumentException("Invalid admission group count"); var bindings=new ArrayList<Binding>(count);
            for(int i=0;i<count;i++) {
                UUID group=uuid(in); int length=in.getInt(); if(length < -1 || length>4096 || length>in.remaining()-1) throw new IllegalArgumentException("Invalid admission name length"); String name=null;
                if(length>=0) { byte[] encoded=new byte[length]; in.get(encoded); for(byte value:encoded) if(value<0) throw new IllegalArgumentException("Admission name is not ASCII"); name=new String(encoded,StandardCharsets.US_ASCII); }
                int merge=in.get()&255; if(merge>1) throw new IllegalArgumentException("Invalid admission merge flag"); bindings.add(new Binding(group,name,merge==1));
            }
            int owners=in.getInt(); if(owners<1 || owners>SchemaChange.MAX_PARTICIPANTS*RangeDirectoryCodec.MAX_REPLICAS || in.remaining()!=204L*owners) throw new IllegalArgumentException("Invalid admission owner count"); var replicas=new ArrayList<ReplicaAdmission>(owners);
            for(int i=0;i<owners;i++) { UUID group=uuid(in),replica=uuid(in); byte[] proof=new byte[NativeSchemaCapacityReservation.BYTES]; in.get(proof); replicas.add(new ReplicaAdmission(group,replica,NativeSchemaCapacityReservation.decode(proof))); }
            var result=new SchemaAdmissionManifest(header,directory,bindings,replicas); if(!Arrays.equals(bytes,result.encode())) throw new IllegalArgumentException("Noncanonical admission manifest"); return result;
        } catch(BufferUnderflowException truncated) { throw new IllegalArgumentException("Truncated admission manifest",truncated); }
    }
    private static void uuid(ByteBuffer out,UUID id) { out.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()); }
    private static UUID uuid(ByteBuffer in) { return new UUID(in.getLong(),in.getLong()); }
}
