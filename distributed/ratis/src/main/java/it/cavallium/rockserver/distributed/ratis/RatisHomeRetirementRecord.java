package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;

/** Current protected HOME reclamation log record; no retained per-transaction GC receipt. */
public final class RatisHomeRetirementRecord {
    private static final int MAGIC=0x52485254;
    private RatisHomeRetirementRecord() {}
    public static boolean isRetirement(byte[] bytes) { return bytes.length>=4 && ByteBuffer.wrap(bytes).getInt()==MAGIC; }
    public static byte[] encode(HomeRetirement retirement) { byte[] body=retirement.encode(); return ByteBuffer.allocate(4+body.length).putInt(MAGIC).put(body).array(); }
    public static HomeRetirement decode(byte[] bytes) {
        if (!isRetirement(bytes) || bytes.length>4+HomeRetirement.MAX_BYTES) throw new IllegalArgumentException("Invalid HOME retirement record"); return HomeRetirement.decode(Arrays.copyOfRange(bytes,4,bytes.length));
    }
    public static OperationId operation(HomeRetirement retirement) { return new OperationId(UUID.nameUUIDFromBytes(retirement.digest()),retirement.identity().business().epoch(),0); }
}
