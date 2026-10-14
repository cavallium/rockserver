package it.cavallium.rockserver.core.common.distributed;

import java.io.*;
import java.nio.ByteBuffer;
import java.security.*;
import java.util.*;

/** One forced future source receipt, retaining the exact frozen recovery input. */
public record NativeSourceOutcomeCredit(Request request,ApplyPosition position) {
    public static final int MAX_CREDITS=8,MAX_METADATA_BYTES=16*1024*1024,MAX_PUBLICATIONS=4096;
    public static final int MAX_BYTES=24+Request.MAX_BYTES;
    public NativeSourceOutcomeCredit { Objects.requireNonNull(request);Objects.requireNonNull(position);if(position.term()<1 || position.index()<=request.origin().absenceCut().index() || position.term()<request.origin().absenceCut().term())throw new IllegalArgumentException("Invalid source credit cut"); }
    public record Request(DistributedCommand original,OperationRerouteOrigin origin,RangeDirectory targetDirectory,NativeTopologyFreeze freeze,HomeIdentity home) {
        public static final int MAX_BYTES=24+DistributedCommandCodec.MAX_ENCODED_BYTES+OperationRerouteOrigin.MAX_BYTES+RangeDirectoryCodec.MAX_ENCODED_BYTES+NativeTopologyFreeze.MAX_BYTES+HomeIdentity.MAX_BYTES;
        public Request {
            Objects.requireNonNull(original);Objects.requireNonNull(origin);Objects.requireNonNull(targetDirectory);Objects.requireNonNull(freeze);Objects.requireNonNull(home);
            var header=freeze.header();var parent=header.participant(TopologyChange.Stage.FREEZE,freeze.groupId());var reserved=home.definition();
            if(original.kind()!=DistributedCommand.Kind.DATA && original.kind()!=DistributedCommand.Kind.COMMIT_ACCEPTED || original.kind()==DistributedCommand.Kind.DATA && original.mutations().isEmpty() && original.predicates().isEmpty() || !reserved.reserved() || !origin.equals(reserved.origin()) || !original.operationId().equals(origin.operation()) || !Arrays.equals(original.receiptDigest(),origin.originalReceiptDigest())
                    || !original.groupId().equals(freeze.groupId()) || original.rangeEpoch()!=parent.rangeEpoch() || !Arrays.equals(original.schemaDigest(),header.schemaDigest())
                    || !origin.clusterId().equals(header.clusterId()) || !origin.databaseId().equals(header.databaseId()) || !origin.domainId().equals(header.domainId())
                    || !origin.source().equals(new DistributedCdcCheckpoint.Lineage(parent.rangeId(),parent.groupId(),parent.rangeEpoch()))
                    || !Arrays.equals(origin.targetDirectoryDigest(),targetDirectory.payloadDigest())
                    || !targetDirectory.clusterId().equals(origin.clusterId()) || !targetDirectory.databaseId().equals(origin.databaseId()) || !targetDirectory.domainId().equals(origin.domainId())
                    || !Arrays.equals(targetDirectory.schemaDigest(),header.schemaDigest()) || targetDirectory.schemaEpoch()!=header.schemaEpoch() || reserved.schemaEpoch()!=header.schemaEpoch() || !Arrays.equals(reserved.schemaDigest(),header.schemaDigest()) || !Arrays.equals(origin.absenceCommitment(),hash(freeze.encode())))throw new IllegalArgumentException("Source credit changed original/HOME/frozen directory identity");
            freeze.requireCut(origin.absenceCut());
            for(var mutation:original.mutations())if(!targetDirectory.columns().contains(mutation.columnId()))throw new IllegalArgumentException("Source credit mutation projection changed");for(var predicate:original.predicates())if(!targetDirectory.columns().contains(predicate.columnId()))throw new IllegalArgumentException("Source credit predicate projection changed");
        }
        public int maximumResultBytes() { return original.kind()==DistributedCommand.Kind.COMMIT_ACCEPTED || original.mutations().stream().allMatch(m->m.returnMode()==Mutation.ReturnMode.NONE) ? 8 : CommandResult.MAX_BYTES; }
        public long reservedBytes() { return 80L+maximumResultBytes(); }
        public byte[] encode() { try { var bytes=new ByteArrayOutputStream();var out=new DataOutputStream(bytes);out.writeInt(0x534f5251);for(byte[] part:List.of(DistributedCommandCodec.encode(original),origin.encode(),RangeDirectoryCodec.encode(targetDirectory),freeze.encode(),home.encode())) { out.writeInt(part.length);out.write(part); }return bytes.toByteArray(); } catch(IOException impossible) { throw new AssertionError(impossible); } }
        public byte[] digest() { return hash(encode()); }
        public static Request decode(byte[] bytes) {
            if(bytes==null || bytes.length<24 || bytes.length>MAX_BYTES)throw new IllegalArgumentException("Invalid source credit request size");
            try { var in=new DataInputStream(new ByteArrayInputStream(bytes));if(in.readInt()!=0x534f5251)throw new IllegalArgumentException("Invalid source credit request tag");var request=new Request(DistributedCommandCodec.decode(blob(in,DistributedCommandCodec.MAX_ENCODED_BYTES)),OperationRerouteOrigin.decode(blob(in,OperationRerouteOrigin.MAX_BYTES)),RangeDirectoryCodec.decode(blob(in,RangeDirectoryCodec.MAX_ENCODED_BYTES)),NativeTopologyFreeze.decode(blob(in,NativeTopologyFreeze.MAX_BYTES)),HomeIdentity.decode(blob(in,HomeIdentity.MAX_BYTES)));if(in.available()!=0 || !Arrays.equals(bytes,request.encode()))throw new IllegalArgumentException("Noncanonical source credit request");return request; } catch(IOException invalid) { throw new IllegalArgumentException("Invalid source credit request",invalid); }
        }
        @Override public boolean equals(Object other) { return other instanceof Request request && Arrays.equals(encode(),request.encode()); }
        @Override public int hashCode() { return Arrays.hashCode(digest()); }
    }
    private static byte[] blob(DataInputStream in,int maximum)throws IOException { int size=in.readInt();if(size<1 || size>maximum || size>in.available())throw new IllegalArgumentException("Source credit input exceeds bound");return in.readNBytes(size); }
    public byte[] encode() { byte[] requestBytes=request.encode();return ByteBuffer.allocate(24+requestBytes.length).putInt(0x534f4352).putLong(position.term()).putLong(position.index()).putInt(requestBytes.length).put(requestBytes).array(); }
    public static NativeSourceOutcomeCredit decode(byte[] bytes) { if(bytes==null || bytes.length<24 || bytes.length>MAX_BYTES)throw new IllegalArgumentException("Invalid source credit size");var in=ByteBuffer.wrap(bytes);if(in.getInt()!=0x534f4352)throw new IllegalArgumentException("Invalid source credit tag");var position=new ApplyPosition(in.getLong(),in.getLong());int size=in.getInt();if(size!=in.remaining())throw new IllegalArgumentException("Invalid source credit input length");byte[] request=new byte[size];in.get(request);return new NativeSourceOutcomeCredit(Request.decode(request),position); }
    public void requireCut(ApplyPosition cut) { if(cut==null || position.index()>cut.index() || position.term()>cut.term() || position.index()==cut.index() && position.term()!=cut.term())throw new IllegalArgumentException("Source credit exceeds completed prefix"); }
    static byte[] hash(byte[] bytes) { try { return MessageDigest.getInstance("SHA-256").digest(bytes); } catch(NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); } }
}
