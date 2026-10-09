package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.util.*;

/** Original forced capacity admission; contains no native handles or physical names. */
public final class NativeSchemaCapacityReservation {
    public static final int BYTES=172;
    private final UUID changeId;
    private final SchemaChange.Participant participant;
    private final byte[] headerDigest,definitionDigest,bindingDigest;
    private final long historyBytes;
    public NativeSchemaCapacityReservation(SchemaChange.Header header,UUID group,byte[] bindingDigest,long historyBytes) {
        this(header.changeId(),header.participants().stream().filter(p -> p.groupId().equals(group)).findFirst().orElseThrow(() -> new IllegalArgumentException("Group outside schema change")),header.digest(),header.definitionDigest(),bindingDigest,historyBytes);
    }
    private NativeSchemaCapacityReservation(UUID change,SchemaChange.Participant participant,byte[] header,byte[] definition,byte[] binding,long bytes) {
        this.changeId=Objects.requireNonNull(change); this.participant=Objects.requireNonNull(participant);
        if(header.length!=32 || definition.length!=32 || binding.length!=32 || bytes<=0 || bytes>16L*1024*1024) throw new IllegalArgumentException("Invalid schema capacity bounds");
        headerDigest=header.clone(); definitionDigest=definition.clone(); bindingDigest=binding.clone(); historyBytes=bytes;
    }
    public UUID changeId() { return changeId; }
    public SchemaChange.Participant participant() { return participant; }
    public byte[] bindingDigest() { return bindingDigest.clone(); }
    public long historyBytes() { return historyBytes; }
    public void requireHeader(SchemaChange.Header header) {
        var expected=new NativeSchemaCapacityReservation(header,participant.groupId(),bindingDigest,historyBytes);
        if(!equals(expected)) throw new IllegalArgumentException("Schema capacity header/participant changed");
    }
    public void requireDefinition(SchemaChange.Definition definition) { requireHeader(definition.header()); }
    public void requireBinding(byte[] digest) { if(!Arrays.equals(bindingDigest,digest)) throw new IllegalArgumentException("Schema capacity binding changed"); }
    public byte[] encode() {
        var out=ByteBuffer.allocate(BYTES).putInt(0x53434150); uuid(out,changeId); uuid(out,participant.rangeId()); uuid(out,participant.groupId());
        out.putLong(participant.rangeEpoch()).putLong(participant.configurationVersion()).put(headerDigest).put(definitionDigest).put(bindingDigest).putLong(historyBytes); return out.array();
    }
    public static NativeSchemaCapacityReservation decode(byte[] bytes) {
        if(bytes==null || bytes.length!=BYTES) throw new IllegalArgumentException("Invalid schema capacity evidence size");
        var in=ByteBuffer.wrap(bytes); if(in.getInt()!=0x53434150) throw new IllegalArgumentException("Invalid schema capacity tag");
        var change=uuid(in); var participant=new SchemaChange.Participant(uuid(in),uuid(in),in.getLong(),in.getLong());
        byte[] header=new byte[32],definition=new byte[32],binding=new byte[32]; in.get(header).get(definition).get(binding);
        return new NativeSchemaCapacityReservation(change,participant,header,definition,binding,in.getLong());
    }
    private static void uuid(ByteBuffer out,UUID id) { out.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()); }
    private static UUID uuid(ByteBuffer in) { return new UUID(in.getLong(),in.getLong()); }
    @Override public boolean equals(Object other) { return other instanceof NativeSchemaCapacityReservation proof && Arrays.equals(encode(),proof.encode()); }
    @Override public int hashCode() { return Arrays.hashCode(encode()); }
}
