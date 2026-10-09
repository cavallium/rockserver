package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

/** One irreversible native DATA fence, retaining the original committed proof without installing a schema. */
public final class NativeSchemaFence {
    private static final int MAGIC=0x4e53464e;
    public static final int BYTES=244;
    private final UUID changeId,clusterId,databaseId,domainId;
    private final long sourceSchemaEpoch,sourceGeneration;
    private final byte[] sourceSchemaDigest,sourceDirectoryDigest,headerDigest;
    private final SchemaChange.Participant participant;
    private final ApplyPosition position;

    public NativeSchemaFence(SchemaChange.Header header,UUID group,ApplyPosition position) {
        this(header.changeId(),header.clusterId(),header.databaseId(),header.domainId(),header.sourceSchemaEpoch(),header.sourceGeneration(),
                header.sourceSchemaDigest(),header.sourceDirectoryDigest(),header.digest(),header.participants().stream()
                        .filter(p -> p.groupId().equals(group)).findFirst().orElseThrow(() -> new IllegalArgumentException("Group is outside schema change")),position);
    }
    private NativeSchemaFence(UUID changeId,UUID clusterId,UUID databaseId,UUID domainId,long epoch,long generation,
                              byte[] schemaDigest,byte[] directoryDigest,byte[] headerDigest,SchemaChange.Participant participant,ApplyPosition position) {
        this.changeId=Objects.requireNonNull(changeId); this.clusterId=Objects.requireNonNull(clusterId);
        this.databaseId=Objects.requireNonNull(databaseId); this.domainId=Objects.requireNonNull(domainId);
        if (epoch<0 || epoch==Long.MAX_VALUE || generation<0 || generation==Long.MAX_VALUE || position.term()<=0) throw new IllegalArgumentException("Invalid native schema fence bounds");
        this.sourceSchemaEpoch=epoch; this.sourceGeneration=generation;
        this.sourceSchemaDigest=schemaDigest.clone(); this.sourceDirectoryDigest=directoryDigest.clone(); this.headerDigest=headerDigest.clone();
        this.participant=Objects.requireNonNull(participant); this.position=Objects.requireNonNull(position);
    }
    public UUID changeId() { return changeId; }
    public UUID clusterId() { return clusterId; }
    public UUID databaseId() { return databaseId; }
    public UUID domainId() { return domainId; }
    public long sourceSchemaEpoch() { return sourceSchemaEpoch; }
    public long sourceGeneration() { return sourceGeneration; }
    public byte[] sourceSchemaDigest() { return sourceSchemaDigest.clone(); }
    public byte[] sourceDirectoryDigest() { return sourceDirectoryDigest.clone(); }
    public byte[] headerDigest() { return headerDigest.clone(); }
    public SchemaChange.Participant participant() { return participant; }
    public ApplyPosition position() { return position; }
    public void requireHeader(SchemaChange.Header header) {
        var expected=new NativeSchemaFence(header,participant.groupId(),position);
        if (!equals(expected)) throw new IllegalArgumentException("Native schema fence header or stored identity changed");
    }
    public void requireCut(ApplyPosition cut) {
        if (cut==null || position.index()>cut.index() || position.term()>cut.term() || position.index()==cut.index() && position.term()!=cut.term()) throw new IllegalArgumentException("Native schema fence exceeds completed cut");
    }
    public byte[] encode() {
        var out=ByteBuffer.allocate(BYTES).putInt(MAGIC);
        uuid(out,changeId); uuid(out,clusterId); uuid(out,databaseId); uuid(out,domainId);
        out.putLong(sourceSchemaEpoch).putLong(sourceGeneration).put(sourceSchemaDigest).put(sourceDirectoryDigest).put(headerDigest);
        uuid(out,participant.rangeId()); uuid(out,participant.groupId()); out.putLong(participant.rangeEpoch()).putLong(participant.configurationVersion());
        out.putLong(position.term()).putLong(position.index()); return out.array();
    }
    public static NativeSchemaFence decode(byte[] bytes) {
        if (bytes==null || bytes.length!=BYTES) throw new IllegalArgumentException("Invalid native schema fence size");
        var in=ByteBuffer.wrap(bytes); if (in.getInt()!=MAGIC) throw new IllegalArgumentException("Invalid native schema fence encoding");
        var change=uuid(in); var cluster=uuid(in); var database=uuid(in); var domain=uuid(in);
        long epoch=in.getLong(),generation=in.getLong(); byte[] schema=digest(in),directory=digest(in),header=digest(in);
        var participant=new SchemaChange.Participant(uuid(in),uuid(in),in.getLong(),in.getLong());
        return new NativeSchemaFence(change,cluster,database,domain,epoch,generation,schema,directory,header,participant,new ApplyPosition(in.getLong(),in.getLong()));
    }
    @Override public boolean equals(Object other) { return other instanceof NativeSchemaFence fence && Arrays.equals(encode(),fence.encode()); }
    @Override public int hashCode() { return Arrays.hashCode(encode()); }
    private static void uuid(ByteBuffer out,UUID id) { out.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()); }
    private static UUID uuid(ByteBuffer in) { return new UUID(in.getLong(),in.getLong()); }
    private static byte[] digest(ByteBuffer in) { var bytes=new byte[32]; in.get(bytes); return bytes; }
}
