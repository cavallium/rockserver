package it.cavallium.rockserver.distributed.ratis;

import java.nio.ByteBuffer;
import java.util.Objects;
import org.rocksdb.NativeRawFlushLimits;
import org.rocksdb.NativeExtentEnv;

/** Declared native extent checkpoints and one shared raw/control working-memory policy. */
public record RatisNativeExtentPolicy(long filesystemOrdinaryBytes,long filesystemProtectedBytes,long nodeOrdinaryBytes,
                                     long nodeProtectedBytes,long quantum,int maximumFiles,int maximumAliases,long maximumStateBytes,NativeRawFlushLimits.Limits raw) {
    public static final int BYTES=112;
    public RatisNativeExtentPolicy {Objects.requireNonNull(raw);if(raw.maximumInputBytes()>RatisNodeResourceBudget.MAX_BYTES || raw.maximumOutputBytes()>RatisNodeResourceBudget.MAX_BYTES || raw.maximumWorkingBytes()>RatisNodeResourceBudget.MAX_BYTES)throw new IllegalArgumentException("Native raw byte policy exceeds node bound");new NativeExtentEnv.Limits(filesystemOrdinaryBytes,filesystemProtectedBytes,nodeOrdinaryBytes,nodeProtectedBytes,quantum,maximumFiles,maximumAliases,maximumStateBytes);}
    public static RatisNativeExtentPolicy defaults(){return new RatisNativeExtentPolicy(1L<<40,1L<<38,1L<<40,1L<<38,4096,65536,65536,64L<<20,new NativeRawFlushLimits.Limits(1_000_000,100_000,64L<<20,128L<<20,1L<<30,1_000_000,64_000_000));}
    public NativeExtentEnv.Limits limits(){return new NativeExtentEnv.Limits(filesystemOrdinaryBytes,filesystemProtectedBytes,nodeOrdinaryBytes,nodeProtectedBytes,quantum,maximumFiles,maximumAliases,maximumStateBytes);}
    public byte[] encode(){return ByteBuffer.allocate(BYTES).putLong(filesystemOrdinaryBytes).putLong(filesystemProtectedBytes).putLong(nodeOrdinaryBytes).putLong(nodeProtectedBytes).putLong(quantum).putInt(maximumFiles).putInt(maximumAliases).putLong(maximumStateBytes).putLong(raw.maximumPointEntries()).putLong(raw.maximumRangeEntries()).putLong(raw.maximumInputBytes()).putLong(raw.maximumOutputBytes()).putLong(raw.maximumWorkingBytes()).putLong(raw.maximumReplayBatches()).putLong(raw.maximumReplayEntries()).array();}
    public static RatisNativeExtentPolicy read(ByteBuffer input){return new RatisNativeExtentPolicy(input.getLong(),input.getLong(),input.getLong(),input.getLong(),input.getLong(),input.getInt(),input.getInt(),input.getLong(),new NativeRawFlushLimits.Limits(input.getLong(),input.getLong(),input.getLong(),input.getLong(),input.getLong(),input.getLong(),input.getLong()));}
}
