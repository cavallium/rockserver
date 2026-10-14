package it.cavallium.rockserver.distributed.ratis;

import java.nio.ByteBuffer;
import org.rocksdb.NativeExtentEnv;

/** Declared rounded native extent checkpoints. This grants neither protected job authority nor a physical-block quota. */
public record RatisNativeExtentPolicy(long filesystemOrdinaryBytes,long filesystemProtectedBytes,long nodeOrdinaryBytes,
                                     long nodeProtectedBytes,long quantum,int maximumFiles,int maximumAliases,long maximumStateBytes) {
    public static final int BYTES=56;
    public RatisNativeExtentPolicy {new NativeExtentEnv.Limits(filesystemOrdinaryBytes,filesystemProtectedBytes,nodeOrdinaryBytes,nodeProtectedBytes,quantum,maximumFiles,maximumAliases,maximumStateBytes);}
    public static RatisNativeExtentPolicy defaults(){return new RatisNativeExtentPolicy(1L<<40,1L<<38,1L<<40,1L<<38,4096,65536,65536,64L<<20);}
    public NativeExtentEnv.Limits limits(){return new NativeExtentEnv.Limits(filesystemOrdinaryBytes,filesystemProtectedBytes,nodeOrdinaryBytes,nodeProtectedBytes,quantum,maximumFiles,maximumAliases,maximumStateBytes);}
    public byte[] encode(){return ByteBuffer.allocate(BYTES).putLong(filesystemOrdinaryBytes).putLong(filesystemProtectedBytes).putLong(nodeOrdinaryBytes).putLong(nodeProtectedBytes).putLong(quantum).putInt(maximumFiles).putInt(maximumAliases).putLong(maximumStateBytes).array();}
    public static RatisNativeExtentPolicy read(ByteBuffer input){return new RatisNativeExtentPolicy(input.getLong(),input.getLong(),input.getLong(),input.getLong(),input.getLong(),input.getInt(),input.getInt(),input.getLong());}
}
