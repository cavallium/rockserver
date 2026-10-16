package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.*;

/** Caller-selected identity bound to canonical input and every original domain generation. */
public record RemoteMutationToken(UUID clusterId, UUID databaseId, UUID ownerId, UUID requestId,
        long ownerEpoch, long requestSequence, Kind kind, List<DomainBinding> domains, byte[] inputDigest) {
    public enum Kind { POINT, RANGE, BULK, ATOMIC }
    private static final int HEADER_BYTES = 117;
    public static final int MIN_BYTES = HEADER_BYTES + DomainBinding.BYTES;
    public static final int MAX_BYTES = HEADER_BYTES + DomainBinding.MAX_DOMAINS * DomainBinding.BYTES;
    public RemoteMutationToken {
        if (requestSequence < 0 || ownerEpoch < 0) throw new IllegalArgumentException("Negative owner epoch or sequence");
        Objects.requireNonNull(clusterId); Objects.requireNonNull(databaseId); Objects.requireNonNull(ownerId); Objects.requireNonNull(requestId); Objects.requireNonNull(kind);
        domains = DomainBinding.canonical(domains);
        if (kind != Kind.ATOMIC && domains.size() != 1) throw new IllegalArgumentException("Single-domain token requires one binding");
        if (inputDigest.length != 32) throw new IllegalArgumentException("Digest must contain 32 bytes");
        inputDigest = inputDigest.clone();
    }
    @Override public byte[] inputDigest() { return inputDigest.clone(); }
    public int encodedSize() { return HEADER_BYTES + domains.size() * DomainBinding.BYTES; }
    public byte[] encode() {
        var out = ByteBuffer.allocate(encodedSize());
        for (var id : List.of(clusterId, databaseId, ownerId, requestId)) out.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits());
        out.putLong(ownerEpoch).putLong(requestSequence).put((byte)kind.ordinal()).putInt(domains.size());
        domains.forEach(domain -> DomainBinding.write(out, domain));
        return out.put(inputDigest).array();
    }
    /** Read one self-delimited token from a bounded journal buffer. */
    public static RemoteMutationToken read(ByteBuffer in) {
        if (in.remaining() < MIN_BYTES) throw new IllegalArgumentException("Truncated mutation token");
        var cluster = uuid(in); var database = uuid(in); var owner = uuid(in); var id = uuid(in);
        long epoch = in.getLong(), sequence = in.getLong(); int kind = in.get() & 255, count = in.getInt();
        if (kind >= Kind.values().length || count < 1 || count > DomainBinding.MAX_DOMAINS || (long) count * DomainBinding.BYTES + 32 > in.remaining()) throw new IllegalArgumentException("Invalid token kind/domain count");
        var domains = new ArrayList<DomainBinding>(count);
        for (int n = 0; n < count; n++) domains.add(DomainBinding.read(in));
        if (!domains.equals(DomainBinding.canonical(domains))) throw new IllegalArgumentException("Noncanonical token domains");
        byte[] input = new byte[32]; in.get(input);
        return new RemoteMutationToken(cluster, database, owner, id, epoch, sequence, Kind.values()[kind], domains, input);
    }
    public static RemoteMutationToken decode(byte[] bytes) {
        if (bytes.length < MIN_BYTES || bytes.length > MAX_BYTES) throw new IllegalArgumentException("Invalid token size");
        var in = ByteBuffer.wrap(bytes); var token = read(in);
        if (in.hasRemaining()) throw new IllegalArgumentException("Trailing token bytes");
        return token;
    }
    private static UUID uuid(ByteBuffer in) { return new UUID(in.getLong(), in.getLong()); }
    public static byte[] digest(byte[] bytes) { try { return MessageDigest.getInstance("SHA-256").digest(bytes); } catch (java.security.NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); } }
    @Override public boolean equals(Object other) { return other instanceof RemoteMutationToken token && Arrays.equals(encode(), token.encode()); }
    @Override public int hashCode() { return Arrays.hashCode(encode()); }
}
