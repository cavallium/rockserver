package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;

/** Typed range binding/clock controls; only proposal admission consults catalogue authority. */
public record RangeSessionControl(SessionAuthority authority, SessionEpochLedger.Grant grant, int outstandingWindow) {
    public static final int MAX_OUTSTANDING = 4096;
    public static final int ADVANCE_BYTES = 64, BIND_BYTES = 108;
    public RangeSessionControl {
        Objects.requireNonNull(authority);
        if (grant==null ? outstandingWindow!=0 : outstandingWindow<1 || outstandingWindow>MAX_OUTSTANDING || !authority.permits(grant)) {
            throw new IllegalArgumentException("Invalid range session control");
        }
    }
    public static RangeSessionControl advance(SessionEpochLedger ledger) { return new RangeSessionControl(SessionAuthority.from(ledger),null,0); }
    public static RangeSessionControl bind(SessionEpochLedger ledger,SessionEpochLedger.Grant grant,int outstandingWindow) {
        Objects.requireNonNull(grant);
        if (!ledger.isLive(new OperationId(grant.sessionId(),grant.epoch(),0))
                || !ledger.grants().get((int)(grant.epoch()-ledger.minimumEpoch())).equals(grant)) throw new IllegalArgumentException("Grant absent from authority image");
        return new RangeSessionControl(SessionAuthority.from(ledger),grant,outstandingWindow);
    }
    public boolean binding() { return grant!=null; }
    public int encodedSize() { return binding() ? BIND_BYTES : ADVANCE_BYTES; }
    public byte[] encode() {
        var output=ByteBuffer.allocate(encodedSize()).putInt(1).putInt(binding() ? 1 : 0);
        uuid(output,authority.clusterId()); uuid(output,authority.databaseId());
        output.putLong(authority.generation()).putLong(authority.logicalTimeMillis()).putLong(authority.minimumEpoch());
        if (binding()) {
            output.putLong(grant.epoch()); uuid(output,grant.sessionId());
            output.putLong(grant.issuedAtMillis()).putLong(grant.expiresAtMillis()).putInt(outstandingWindow);
        }
        return output.array();
    }
    public static RangeSessionControl decode(byte[] bytes) {
        if (bytes.length!=ADVANCE_BYTES && bytes.length!=BIND_BYTES) throw new IllegalArgumentException("Invalid range session control length");
        var input=ByteBuffer.wrap(bytes);
        if (input.getInt()!=1) throw new IllegalArgumentException("Unsupported range session control version");
        int kind=input.getInt();
        if (kind<0 || kind>1 || bytes.length!=(kind==1 ? BIND_BYTES : ADVANCE_BYTES)) throw new IllegalArgumentException("Invalid range session control kind");
        var authority=new SessionAuthority(uuid(input),uuid(input),input.getLong(),input.getLong(),input.getLong());
        var grant=kind==0 ? null : new SessionEpochLedger.Grant(input.getLong(),uuid(input),input.getLong(),input.getLong());
        return new RangeSessionControl(authority,grant,kind==0 ? 0 : input.getInt());
    }
    private static void uuid(ByteBuffer output,UUID id) { output.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()); }
    private static UUID uuid(ByteBuffer input) { return new UUID(input.getLong(),input.getLong()); }
}
