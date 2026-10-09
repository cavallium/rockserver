package it.cavallium.rockserver.core.common.distributed;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/** Durable install intent and original completion proof, with metadata but no native handles. */
public final class NativeSchemaInstallation {
    public static final int MAX_BYTES=SchemaChange.MAX_DEFINITION_BYTES+128*1024;
    private final SchemaChange.Definition definition;
    private final NativeSchemaFence fence;
    private final ApplyPosition position;
    private final Map<UUID,String> sourceNames,targetNames;
    private final Set<UUID> sourceMerges,targetMerges;
    public NativeSchemaInstallation(SchemaChange.Definition definition,NativeSchemaFence fence,ApplyPosition position,
            Map<UUID,String> sourceNames,Map<UUID,String> targetNames,Set<UUID> sourceMerges,Set<UUID> targetMerges) {
        this.definition=Objects.requireNonNull(definition); this.fence=Objects.requireNonNull(fence); this.position=Objects.requireNonNull(position);
        fence.requireHeader(definition.header());
        fence.requireCut(position);
        if (position.index()<=fence.position().index()) throw new IllegalArgumentException("Install must follow fence");
        this.sourceNames=names(sourceNames,definition.sourceSchema().columnIds()); this.targetNames=names(targetNames,definition.targetSchema().columnIds());
        this.sourceMerges=Set.copyOf(sourceMerges); this.targetMerges=Set.copyOf(targetMerges);
        if (!sourceNames.keySet().containsAll(sourceMerges) || !targetNames.keySet().containsAll(targetMerges)) throw new IllegalArgumentException("Merge outside schema");
        for (var id:sourceNames.keySet()) if (targetNames.containsKey(id)
                && (!sourceNames.get(id).equals(targetNames.get(id)) || sourceMerges.contains(id)!=targetMerges.contains(id))) throw new IllegalArgumentException("Unchanged column binding changed");
    }
    private static Map<UUID,String> names(Map<UUID,String> names,Set<UUID> expected) {
        if (!names.keySet().equals(expected) || new HashSet<>(names.values()).size()!=names.size()) throw new IllegalArgumentException("Invalid physical coverage");
        names.values().forEach(name -> { if (name.isEmpty() || name.length()>4096 || name.indexOf('\0')>=0 || !StandardCharsets.US_ASCII.newEncoder().canEncode(name)) throw new IllegalArgumentException("Invalid physical name"); });
        return Map.copyOf(names);
    }
    public SchemaChange.Definition definition() { return definition; }
    public NativeSchemaFence fence() { return fence; }
    public ApplyPosition position() { return position; }
    public NativeSchemaInstallProof proof() { return new NativeSchemaInstallProof(fence,definition.targetSchema().digest(),position); }
    public void requireRange(UUID group,long epoch,UUID domain,byte[] lowerInclusive,byte[] upperExclusive) {
        var participant=fence.participant();
        var descriptor=definition.sourceDirectory().ranges().stream().filter(range -> range.groupId().equals(group)).findFirst().orElseThrow(() -> new IllegalArgumentException("Install group is outside source directory"));
        if(!participant.groupId().equals(group) || participant.rangeEpoch()!=epoch || !definition.sourceSchema().domainId().equals(domain)
                || descriptor.rangeEpoch()!=epoch || !descriptor.rangeId().equals(participant.rangeId()) || descriptor.configurationVersion()!=participant.configurationVersion()
                || !Arrays.equals(descriptor.lowerInclusive(),lowerInclusive) || !Arrays.equals(descriptor.upperExclusive(),upperExclusive)) throw new IllegalArgumentException("Install differs from exact native range lineage/bounds");
    }
    public void requireCut(ApplyPosition cut) {
        if(cut==null || position.index()>cut.index() || position.term()>cut.term() || position.index()==cut.index() && position.term()!=cut.term()) throw new IllegalArgumentException("Installation exceeds completed cut");
    }
    public Map<UUID,String> sourceNames() { return sourceNames; }
    public Map<UUID,String> targetNames() { return targetNames; }
    public Set<UUID> sourceMerges() { return sourceMerges; }
    public Set<UUID> targetMerges() { return targetMerges; }
    public void requireDefinition(SchemaChange.Definition expected) { if (!Arrays.equals(definition.encode(),expected.encode())) throw new IllegalArgumentException("Install definition changed"); }
    /** Validates the single current lifecycle, including retained original proofs from predecessors. */
    public static List<NativeSchemaInstallation> validateLifecycle(NativeSchemaInstallation current,Collection<NativeSchemaInstallation> history,
            NativeSchemaActivationProof currentActivation,Map<UUID,NativeSchemaActivationProof> activations,NativeSchemaFence pending,ApplyPosition cut) {
        var ordered=history.stream().sorted(Comparator.comparingLong(record -> record.position().index())).toList();
        if(current==null) {
            if(!ordered.isEmpty() || currentActivation!=null || !activations.isEmpty()) throw new IllegalArgumentException("Schema history lacks current generation");
            if(pending!=null) pending.requireCut(cut);
            return ordered;
        }
        current.requireCut(cut);
        if(ordered.isEmpty() || !Arrays.equals(current.encode(),ordered.getLast().encode())) throw new IllegalArgumentException("Current installation differs from latest immutable history");
        var records=new HashMap<UUID,NativeSchemaInstallation>(); NativeSchemaInstallation previous=null;
        for(var record:ordered) {
            record.requireCut(current.position()); var id=record.definition().changeId();
            if(records.put(id,record)!=null) throw new IllegalArgumentException("Duplicate schema history identity");
            if(previous!=null) {
                var activation=activations.get(previous.definition().changeId());
                if(activation==null || activation.position().index()>=record.fence().position().index() || activation.position().term()>record.fence().position().term()
                        || !Arrays.equals(previous.definition().targetSchema().digest(),record.definition().sourceSchema().digest())
                        || !previous.targetNames().equals(record.sourceNames()) || !previous.targetMerges().equals(record.sourceMerges())) throw new IllegalArgumentException("Schema lifecycle predecessor is missing or changed");
            }
            previous=record;
        }
        for(var entry:activations.entrySet()) {
            var activation=entry.getValue(); activation.requireCut(cut); var record=records.get(entry.getKey());
            if(record==null || !entry.getKey().equals(activation.installation().fence().changeId()) || !activation.installation().equals(record.proof())) throw new IllegalArgumentException("Activation differs from immutable INSTALL");
        }
        var savedActivation=activations.get(current.definition().changeId());
        if(!Objects.equals(currentActivation,savedActivation)) throw new IllegalArgumentException("Current activation differs from immutable history");
        if(currentActivation==null) {
            if(!current.fence().equals(pending)) throw new IllegalArgumentException("Unactivated installation lacks original pending fence");
        } else if(pending!=null) {
            var target=current.definition().targetSchema(); pending.requireCut(cut);
            if(records.containsKey(pending.changeId()) || !pending.clusterId().equals(target.clusterId()) || !pending.databaseId().equals(target.databaseId())
                    || !pending.domainId().equals(target.domainId()) || pending.sourceSchemaEpoch()!=target.schemaEpoch() || !Arrays.equals(pending.sourceSchemaDigest(),target.digest())
                    || !pending.participant().groupId().equals(current.fence().participant().groupId()) || pending.participant().rangeEpoch()!=current.fence().participant().rangeEpoch()
                    || pending.position().index()<=currentActivation.position().index() || pending.position().term()<currentActivation.position().term()) throw new IllegalArgumentException("Pending fence differs from activated generation");
        }
        return ordered;
    }
    public byte[] encode() {
        try {
            var bytes=new ByteArrayOutputStream(); var out=new DataOutputStream(bytes);
            out.writeInt(0x4e53494e); blob(out,definition.encode()); blob(out,fence.encode()); out.writeLong(position.term()); out.writeLong(position.index());
            writeNames(out,sourceNames,sourceMerges); writeNames(out,targetNames,targetMerges); out.flush();
            if (bytes.size()>MAX_BYTES) throw new IllegalArgumentException("Installation exceeds metadata budget"); return bytes.toByteArray();
        } catch (IOException impossible) { throw new AssertionError(impossible); }
    }
    private static void blob(DataOutputStream out,byte[] bytes) throws IOException { out.writeInt(bytes.length); out.write(bytes); }
    private static byte[] blob(DataInputStream in) throws IOException { int n=in.readInt(); if (n<0 || n>MAX_BYTES || n>in.available()) throw new IOException("Invalid metadata length"); return in.readNBytes(n); }
    private static void writeNames(DataOutputStream out,Map<UUID,String> names,Set<UUID> merges) throws IOException {
        out.writeInt(names.size());
        for (var id:names.keySet().stream().sorted().toList()) { out.writeLong(id.getMostSignificantBits()); out.writeLong(id.getLeastSignificantBits()); blob(out,names.get(id).getBytes(StandardCharsets.US_ASCII)); out.writeBoolean(merges.contains(id)); }
    }
    private record Names(Map<UUID,String> names,Set<UUID> merges) {}
    private static Names readNames(DataInputStream in) throws IOException {
        int count=in.readInt(); if (count<0 || count>RangeDirectoryCodec.MAX_COLUMNS) throw new IOException("Invalid column count");
        var names=new HashMap<UUID,String>(); var merges=new HashSet<UUID>();
        for (int i=0;i<count;i++) { var id=new UUID(in.readLong(),in.readLong()); var encoded=blob(in); if (encoded.length>4096) throw new IOException("Name exceeds budget");
            if (names.put(id,new String(encoded,StandardCharsets.US_ASCII))!=null) throw new IOException("Duplicate column"); if(in.readBoolean()) merges.add(id); }
        return new Names(names,merges);
    }
    public static NativeSchemaInstallation decode(byte[] encoded) {
        if (encoded.length>MAX_BYTES) throw new IllegalArgumentException("Installation exceeds metadata budget");
        try {
            var in=new DataInputStream(new ByteArrayInputStream(encoded)); if(in.readInt()!=0x4e53494e) throw new IOException("Invalid installation tag");
            var definition=SchemaChange.Definition.decode(blob(in)); var fence=NativeSchemaFence.decode(blob(in)); var position=new ApplyPosition(in.readLong(),in.readLong());
            var source=readNames(in); var target=readNames(in); if(in.available()!=0) throw new IOException("Trailing installation metadata");
            var result=new NativeSchemaInstallation(definition,fence,position,source.names,target.names,source.merges,target.merges);
            if (!Arrays.equals(encoded,result.encode())) throw new IOException("Noncanonical installation"); return result;
        } catch(IOException failure) { throw new IllegalArgumentException("Invalid installation",failure); }
    }
}
