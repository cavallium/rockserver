package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;

/** Current protected snapshot-HOME USER reclamation record. */
public final class RatisSnapshotHomeRetirementRecord {
    private static final int MAGIC=0x52534843;
    private RatisSnapshotHomeRetirementRecord() {}
    public static boolean isRetirement(byte[] bytes) { return bytes.length>=4 && ByteBuffer.wrap(bytes).getInt()==MAGIC; }
    public static byte[] encode(SnapshotHomeRetirement retirement) { byte[] body=retirement.encode(); return ByteBuffer.allocate(4+body.length).putInt(MAGIC).put(body).array(); }
    public static SnapshotHomeRetirement decode(byte[] bytes) { if (!isRetirement(bytes) || bytes.length>4+SnapshotHomeRetirement.MAX_BYTES) throw new IllegalArgumentException("Invalid snapshot HOME retirement record"); return SnapshotHomeRetirement.decode(Arrays.copyOfRange(bytes,4,bytes.length)); }
    public static OperationId operation(SnapshotHomeRetirement retirement) { return new OperationId(UUID.nameUUIDFromBytes(retirement.digest()),retirement.identity().business().epoch(),0); }
}
