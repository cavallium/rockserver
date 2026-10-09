package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;

/** Current protected DATA snapshot-gate reclamation record. */
public final class RatisSnapshotRetirementRecord {
    private static final int MAGIC=0x52534743;
    private RatisSnapshotRetirementRecord() {}
    public static boolean isRetirement(byte[] bytes) { return bytes.length>=4 && ByteBuffer.wrap(bytes).getInt()==MAGIC; }
    public static byte[] encode(SnapshotGateRetirement retirement) { byte[] body=retirement.encode(); return ByteBuffer.allocate(4+body.length).putInt(MAGIC).put(body).array(); }
    public static SnapshotGateRetirement decode(byte[] bytes) { if (!isRetirement(bytes) || bytes.length>4+SnapshotGateRetirement.MAX_BYTES) throw new IllegalArgumentException("Invalid snapshot retirement record"); return SnapshotGateRetirement.decode(Arrays.copyOfRange(bytes,4,bytes.length)); }
    public static OperationId operation(SnapshotGateRetirement retirement) { return new OperationId(UUID.nameUUIDFromBytes(retirement.digest()),retirement.fence().operation().epoch(),0); }
}
