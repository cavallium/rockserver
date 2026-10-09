package it.cavallium.rockserver.core.impl.rocksdb;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.*;

/** One bounded pool registry, with schema content shared by all local participants of a change. */
final class SchemaCapacityClaims {
    static final int MAX_CHANGES=64,MAX_BYTES=16*1024*1024;
    enum State { ACTIVE, FENCED, CONSUMED, ABORTED, RETIRED }
    record Group(NativeSchemaCapacityReservation proof,State state) { boolean outstanding() { return state==State.ACTIVE || state==State.FENCED; } }
    record Claim(SchemaChange.Definition definition,Map<UUID,String> sourceNames,Map<UUID,String> targetNames,Set<UUID> sourceMerges,Set<UUID> targetMerges,Map<UUID,Group> groups) {
        Claim { sourceNames=Map.copyOf(sourceNames); targetNames=Map.copyOf(targetNames); sourceMerges=Set.copyOf(sourceMerges); targetMerges=Set.copyOf(targetMerges); groups=Map.copyOf(groups); }
        boolean outstanding() { return groups.values().stream().anyMatch(Group::outstanding); }
        byte[] bindingDigest() { return digest(bindingBytes(sourceNames,targetNames,sourceMerges,targetMerges)); }
        int installBytes() { int size=Math.toIntExact(36L+NativeSchemaFence.BYTES+definition.encode().length+namesSize(sourceNames)+namesSize(targetNames)); if(size>NativeSchemaInstallation.MAX_BYTES) throw new IllegalArgumentException("Reserved INSTALL metadata exceeds budget"); return size; }
        long historyBytes() { return 3L*installBytes()+2L*NativeSchemaActivationProof.BYTES; }
        Claim group(UUID group,Group value) { var next=new HashMap<>(groups); next.put(group,value); return new Claim(definition,sourceNames,targetNames,sourceMerges,targetMerges,next); }
        void requireSame(Claim other) { if(!Arrays.equals(definition.encode(),other.definition.encode()) || !Arrays.equals(bindingDigest(),other.bindingDigest())) throw new IllegalArgumentException("Original schema capacity inputs changed"); }
    }
    private static long namesSize(Map<UUID,String> names) { long size=0; for(var name:names.values()) size+=21L+name.length(); return size; }
    static byte[] encode(Map<UUID,Claim> claims) {
        if(claims.size()>MAX_CHANGES) throw new IllegalStateException("Schema capacity evidence count exhausted");
        try {
            var bytes=new ByteArrayOutputStream(); var out=new DataOutputStream(bytes); out.writeInt(0x53435247); out.writeInt(claims.size());
            for(var id:claims.keySet().stream().sorted().toList()) {
                var claim=claims.get(id); blob(out,claim.definition.encode()); names(out,claim.sourceNames,claim.sourceMerges); names(out,claim.targetNames,claim.targetMerges);
                out.writeInt(claim.groups.size());
                for(var group:claim.groups.keySet().stream().sorted().toList()) { var value=claim.groups.get(group); out.write(value.proof.encode()); out.writeByte(value.state.ordinal()+1); }
            }
            out.flush(); if(bytes.size()>MAX_BYTES) throw new IllegalStateException("Schema capacity evidence byte budget exhausted"); return bytes.toByteArray();
        } catch(IOException impossible) { throw new AssertionError(impossible); }
    }
    static Map<UUID,Claim> decode(byte[] bytes) {
        if(bytes.length>MAX_BYTES) throw new IllegalArgumentException("Schema capacity registry exceeds budget");
        try {
            var in=new DataInputStream(new ByteArrayInputStream(bytes)); if(in.readInt()!=0x53435247) throw new IOException("Invalid schema capacity registry");
            int count=in.readInt(); if(count<0 || count>MAX_CHANGES) throw new IOException("Invalid schema capacity count"); var claims=new HashMap<UUID,Claim>();
            for(int i=0;i<count;i++) {
                var definition=SchemaChange.Definition.decode(blob(in,SchemaChange.MAX_DEFINITION_BYTES)); var source=names(in); var target=names(in); int groups=in.readInt();
                if(groups<1 || groups>SchemaChange.MAX_PARTICIPANTS) throw new IOException("Invalid schema capacity groups"); var members=new HashMap<UUID,Group>();
                var claim=new Claim(definition,source.names,target.names,source.merges,target.merges,Map.of());
                if(!source.names.keySet().equals(definition.sourceSchema().columnIds()) || !target.names.keySet().equals(definition.targetSchema().columnIds())) throw new IOException("Capacity physical coverage changed");
                for(int g=0;g<groups;g++) {
                    var proof=NativeSchemaCapacityReservation.decode(in.readNBytes(NativeSchemaCapacityReservation.BYTES)); proof.requireDefinition(definition); proof.requireBinding(claim.bindingDigest());
                    if(proof.historyBytes()!=claim.historyBytes()) throw new IOException("Capacity charge changed"); int state=in.readUnsignedByte(); if(state<1 || state>State.values().length) throw new IOException("Invalid capacity state");
                    if(members.put(proof.participant().groupId(),new Group(proof,State.values()[state-1]))!=null) throw new IOException("Duplicate capacity participant");
                }
                claim=new Claim(definition,source.names,target.names,source.merges,target.merges,members); if(claims.put(definition.changeId(),claim)!=null) throw new IOException("Duplicate capacity change");
            }
            if(in.available()!=0 || !Arrays.equals(bytes,encode(claims))) throw new IOException("Noncanonical capacity registry"); return Map.copyOf(claims);
        } catch(IOException failure) { throw new IllegalArgumentException("Invalid schema capacity registry",failure); }
    }
    private record Names(Map<UUID,String> names,Set<UUID> merges) {}
    private static Names names(DataInputStream in) throws IOException {
        int count=in.readInt(); if(count<0 || count>RangeDirectoryCodec.MAX_COLUMNS) throw new IOException("Invalid capacity binding count");
        var names=new HashMap<UUID,String>(); var merges=new HashSet<UUID>(); var physical=new HashSet<String>();
        for(int i=0;i<count;i++) {
            var id=new UUID(in.readLong(),in.readLong()); String name=new String(blob(in,4096),StandardCharsets.US_ASCII);
            if(name.isEmpty() || name.indexOf('\0')>=0 || !StandardCharsets.US_ASCII.newEncoder().canEncode(name) || DistributedStoragePool.isReservedColumn(name) || !physical.add(name) || names.put(id,name)!=null) throw new IOException("Invalid capacity binding");
            if(in.readBoolean()) merges.add(id);
        }
        return new Names(names,merges);
    }
    private static byte[] bindingBytes(Map<UUID,String> source,Map<UUID,String> target,Set<UUID> sourceMerges,Set<UUID> targetMerges) {
        try { var bytes=new ByteArrayOutputStream(); var out=new DataOutputStream(bytes); names(out,source,sourceMerges); names(out,target,targetMerges); out.flush(); return bytes.toByteArray(); }
        catch(IOException impossible) { throw new AssertionError(impossible); }
    }
    private static void names(DataOutputStream out,Map<UUID,String> names,Set<UUID> merges) throws IOException {
        out.writeInt(names.size()); for(var id:names.keySet().stream().sorted().toList()) { out.writeLong(id.getMostSignificantBits()); out.writeLong(id.getLeastSignificantBits()); blob(out,names.get(id).getBytes(StandardCharsets.US_ASCII)); out.writeBoolean(merges.contains(id)); }
    }
    private static void blob(DataOutputStream out,byte[] bytes) throws IOException { out.writeInt(bytes.length); out.write(bytes); }
    private static byte[] blob(DataInputStream in,int maximum) throws IOException { int size=in.readInt(); if(size<0 || size>maximum || size>in.available()) throw new IOException("Capacity field exceeds budget"); return in.readNBytes(size); }
    private static byte[] digest(byte[] bytes) { try { return MessageDigest.getInstance("SHA-256").digest(bytes); } catch(NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); } }
}
