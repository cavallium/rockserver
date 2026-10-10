package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeSstImport;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

/** Owner-addressed local durable proof; it is not a leader readiness or public DATA admission flag. */
public final class RatisRangeImportStatusRpc {
    public static final int KIND=79,MAX_REQUEST_BYTES=1024,MAX_REPLY_BYTES=512;
    private RatisRangeImportStatusRpc() {}
    public record Target(UUID cluster,UUID database,UUID domain,UUID importId,UUID group,UUID range,long epoch,long configuration,
                         byte[] definitionDigest,byte[] manifestDigest,ReplicaDescriptor owner) {
        public Target {
            Objects.requireNonNull(cluster);Objects.requireNonNull(database);Objects.requireNonNull(domain);Objects.requireNonNull(importId);Objects.requireNonNull(group);Objects.requireNonNull(range);Objects.requireNonNull(owner);
            if(epoch<0 || configuration<0 || definitionDigest.length!=32 || manifestDigest.length!=32)throw new IllegalArgumentException("Invalid import status target");definitionDigest=definitionDigest.clone();manifestDigest=manifestDigest.clone();
        }
        @Override public byte[] definitionDigest(){return definitionDigest.clone();}
        @Override public byte[] manifestDigest(){return manifestDigest.clone();}
        public static Target of(RangeImportDefinition definition,UUID group,ReplicaDescriptor owner) {
            var directory=definition.sourceDirectory(); var range=directory.ranges().stream().filter(r->r.groupId().equals(group)).findFirst().orElseThrow();
            var manifest=definition.manifest(group); if(!range.replicas().contains(owner))throw new IllegalArgumentException("Status owner is outside frozen membership");
            return new Target(directory.clusterId(),directory.databaseId(),directory.domainId(),definition.importId(),group,range.rangeId(),range.rangeEpoch(),range.configurationVersion(),definition.digest(),manifest.digest(),owner);
        }
        public void requireBinding(RangeImportDefinition definition,ReplicaDescriptor local) {
            if(!Arrays.equals(encode(),of(definition,group,local).encode()))throw new IllegalArgumentException("Status target differs from configured import owner");
        }
        byte[] encode() {
            try {var bytes=new ByteArrayOutputStream();var out=new DataOutputStream(bytes);for(var id:List.of(cluster,database,domain,importId,group,range))uuid(out,id);out.writeLong(epoch);out.writeLong(configuration);out.write(definitionDigest);out.write(manifestDigest);
                uuid(out,owner.nodeId());uuid(out,owner.nodeIncarnation());uuid(out,owner.replicaId());text(out,owner.peerId());text(out,owner.address());out.writeByte(owner.role().ordinal());return bytes.toByteArray();
            }catch(IOException impossible){throw new AssertionError(impossible);}
        }
    }
    public static byte[] request(Target target){return ScopedTransactionRpc.frame(KIND,target.encode());}
    public static Target decodeRequest(byte[] bytes) {
        var packet=ScopedTransactionRpc.parse(bytes,MAX_REQUEST_BYTES);if(packet.kind()!=KIND)throw new IllegalArgumentException("Wrong import status request kind");
        try {var in=new DataInputStream(new ByteArrayInputStream(packet.body()));UUID cluster=uuid(in),database=uuid(in),domain=uuid(in),job=uuid(in),group=uuid(in),range=uuid(in);long epoch=in.readLong(),configuration=in.readLong();byte[] definition=in.readNBytes(32),manifest=in.readNBytes(32);
            UUID node=uuid(in),incarnation=uuid(in),replica=uuid(in);String peer=text(in),address=text(in);int role=in.readUnsignedByte();if(role>=ReplicaDescriptor.Role.values().length)throw new IOException("Invalid status owner role");
            var target=new Target(cluster,database,domain,job,group,range,epoch,configuration,definition,manifest,new ReplicaDescriptor(node,incarnation,replica,peer,address,ReplicaDescriptor.Role.values()[role]));
            if(in.read()!=-1 || !Arrays.equals(packet.body(),target.encode()))throw new IOException("Noncanonical status target");return target;
        }catch(IOException|RuntimeException invalid){throw new IllegalArgumentException("Invalid status target",invalid);}
    }
    public static byte[] reply(Target target,RatisRangeImportApply.Status status) {
        requireStatus(target,status);var receipt=status.receipt();var activation=status.activation();
        var out=ByteBuffer.allocate(137+(receipt==null?0:88)+(activation==null?0:168)).put(RangeImportAdmission.sha256(target.encode()));uuid(out,status.groupId());out.putLong(status.rangeEpoch()).put(status.definitionDigest()).put(status.manifestDigest()).putLong(status.applied().term()).putLong(status.applied().index()).put((byte)status.phase().ordinal());
        if(receipt!=null)out.put(receipt.encode());if(activation!=null)out.put(activation.encode());return ScopedTransactionRpc.frame(KIND,out.array());
    }
    public static RatisRangeImportApply.Status decodeReply(byte[] bytes,Target target) {
        var packet=ScopedTransactionRpc.parse(bytes,MAX_REPLY_BYTES);if(packet.kind()!=KIND)throw new IllegalArgumentException("Wrong status reply kind");
        try {var in=ByteBuffer.wrap(packet.body());byte[] binding=new byte[32];in.get(binding);if(!Arrays.equals(binding,RangeImportAdmission.sha256(target.encode())))throw new IllegalArgumentException("Status reply crossed owner request");UUID group=uuid(in);long epoch=in.getLong();byte[] definition=new byte[32],manifest=new byte[32];in.get(definition).get(manifest);var cut=new ApplyPosition(in.getLong(),in.getLong());int phase=in.get()&255;
            if(phase>=RatisRangeImportApply.Phase.values().length || in.remaining()!=(phase==0?0:phase==1?88:256))throw new IllegalArgumentException("Invalid status phase/length");
            NativeSstImportManifest.Receipt receipt=null;NativeRangeSstImport.Activation activation=null;if(phase>0){byte[] encoded=new byte[88];in.get(encoded);receipt=NativeSstImportManifest.Receipt.decode(encoded);}if(phase==2){byte[] encoded=new byte[168];in.get(encoded);activation=NativeRangeSstImport.Activation.decode(encoded);}
            var status=new RatisRangeImportApply.Status(group,epoch,definition,manifest,cut,RatisRangeImportApply.Phase.values()[phase],receipt,activation);requireStatus(target,status);return status;
        }catch(java.nio.BufferUnderflowException malformed){throw new IllegalArgumentException("Truncated import status",malformed);}
    }
    private static void requireStatus(Target target,RatisRangeImportApply.Status status) {
        if(status.applied()==null || status.applied().term()<1 || !target.group().equals(status.groupId()) || target.epoch()!=status.rangeEpoch() || !Arrays.equals(target.definitionDigest(),status.definitionDigest()) || !Arrays.equals(target.manifestDigest(),status.manifestDigest()))throw new IllegalArgumentException("Status differs from target or lacks completed native cut");
        if(status.receipt()!=null && !target.importId().equals(status.receipt().importId()))throw new IllegalArgumentException("Status receipt belongs to another import");
    }
    private static void uuid(DataOutputStream out,UUID id)throws IOException{out.writeLong(id.getMostSignificantBits());out.writeLong(id.getLeastSignificantBits());}
    private static UUID uuid(DataInputStream in)throws IOException{return new UUID(in.readLong(),in.readLong());}
    private static void uuid(ByteBuffer out,UUID id){out.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits());}
    private static UUID uuid(ByteBuffer in){return new UUID(in.getLong(),in.getLong());}
    private static void text(DataOutputStream out,String value)throws IOException{byte[] bytes=value.getBytes(StandardCharsets.US_ASCII);out.writeInt(bytes.length);out.write(bytes);}
    private static String text(DataInputStream in)throws IOException{int count=in.readInt();if(count<1 || count>256 || count>in.available())throw new IOException("Invalid status owner text");byte[] bytes=in.readNBytes(count);for(byte b:bytes)if(b<0)throw new IOException("Non-ASCII status owner");return new String(bytes,StandardCharsets.US_ASCII);}
}
