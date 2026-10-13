package it.cavallium.rockserver.core.impl.rocksdb;

import it.cavallium.rockserver.core.config.*;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import java.util.*;

/** Persistent RocksDB cache/WBM declarations, resolved without JNI allocation. */
public final class NativeCacheCapacity {
    public static final long DEFAULT_BLOCK_CACHE_BYTES=512L*1024*1024;
    private final Map<String,Long> blockCaches;
    private final long writeBufferManagerBytes,totalBytes;
    private NativeCacheCapacity(Map<String,Long> blockCaches,long writeBufferManagerBytes,long totalBytes){this.blockCaches=Collections.unmodifiableMap(new LinkedHashMap<>(blockCaches));this.writeBufferManagerBytes=writeBufferManagerBytes;this.totalBytes=totalBytes;}
    public Map<String,Long> blockCaches(){return blockCaches;}
    public long defaultCacheBytes(){return blockCaches.get("default");}
    public long writeBufferManagerBytes(){return writeBufferManagerBytes;}
    /** Conservative declared cache plus WBM capacity, not measured physical memory usage. */
    public long totalBytes(){return totalBytes;}
    public static NativeCacheCapacity resolve(DatabaseConfig config){try{return resolve(config.global());}catch(org.github.gestalt.config.exceptions.GestaltException invalid){throw RocksDBException.of(RocksDBErrorType.CONFIG_ERROR,"Cannot resolve native cache configuration",invalid);}}
    public static NativeCacheCapacity resolve(GlobalDatabaseConfig config) {
        Objects.requireNonNull(config);
        try {
            long configuredCache=Optional.ofNullable(config.blockCache()).map(DataSize::longValue).orElse(DEFAULT_BLOCK_CACHE_BYTES);
            long configuredWbm=Optional.ofNullable(config.writeBufferManager()).map(DataSize::longValue).orElse(0L);
            if(configuredCache<0 || configuredWbm<0)throw invalid("Block cache and write-buffer-manager must be nonnegative");
            long effectiveCache=Math.addExact(configuredCache,configuredWbm),effectiveWbm=configuredWbm>0 ? configuredWbm : effectiveCache/2;
            var caches=new LinkedHashMap<String,Long>();caches.put("default",effectiveCache);long total=Math.addExact(effectiveCache,effectiveWbm);
            for(var cache:Objects.requireNonNullElse(config.blockCaches(),new BlockCacheConfig[0])) {
                if(cache==null)throw invalid("Null named block cache declaration");String name=cache.name();
                if(name==null || name.isBlank() || name.equals("default"))throw invalid("Named block cache name must be nonblank and not reserved 'default'");
                var size=cache.size();if(size==null || size.longValue()<=0)throw invalid("Named block cache '"+name+"' size must be greater than zero");
                if(caches.putIfAbsent(name,size.longValue())!=null)throw invalid("Duplicate named block cache: "+name);
                total=Math.addExact(total,size.longValue());
            }
            return new NativeCacheCapacity(caches,effectiveWbm,total);
        }catch(org.github.gestalt.config.exceptions.GestaltException malformed){throw RocksDBException.of(RocksDBErrorType.CONFIG_ERROR,"Cannot resolve native cache configuration",malformed);}
        catch(ArithmeticException overflow){throw invalid("Native cache/WBM capacity overflow");}
    }
    private static RocksDBException invalid(String message){return RocksDBException.of(RocksDBErrorType.CONFIG_ERROR,message);}
}
