package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.config.*;
import it.cavallium.rockserver.core.impl.rocksdb.NativeCacheCapacity;
import it.cavallium.rockserver.core.common.RocksDBException;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class NativeCacheCapacityTest {
    private static final long MIB=1024L*1024;
    private static GlobalDatabaseConfig config(Long cache,Long wbm,BlockCacheConfig... named)throws Exception {
        var config=mock(GlobalDatabaseConfig.class);when(config.blockCache()).thenReturn(cache==null ? null : new DataSize(cache));when(config.writeBufferManager()).thenReturn(wbm==null ? null : new DataSize(wbm));when(config.blockCaches()).thenReturn(named);return config;
    }
    private static BlockCacheConfig named(String name,Long bytes)throws Exception {var value=mock(BlockCacheConfig.class);when(value.name()).thenReturn(name);when(value.size()).thenReturn(bytes==null ? null : new DataSize(bytes));return value;}
    @Test void absentAndZeroWbmUseHalfEffectiveDefaultCache()throws Exception {
        var disabled=NativeCacheCapacity.resolve(config(0L,0L));assertEquals(0,disabled.totalBytes());assertEquals(0,disabled.writeBufferManagerBytes());
        var wbmOnly=NativeCacheCapacity.resolve(config(0L,4*MIB));assertEquals(4*MIB,wbmOnly.defaultCacheBytes());assertEquals(8*MIB,wbmOnly.totalBytes());
        var fallback=NativeCacheCapacity.resolve(config(null,null));assertEquals(512*MIB,fallback.defaultCacheBytes());assertEquals(256*MIB,fallback.writeBufferManagerBytes());assertEquals(768*MIB,fallback.totalBytes());
        var explicit=NativeCacheCapacity.resolve(config(8*MIB,0L));assertEquals(8*MIB,explicit.defaultCacheBytes());assertEquals(4*MIB,explicit.writeBufferManagerBytes());assertEquals(12*MIB,explicit.totalBytes());
    }
    @Test void configuredWbmAndEveryNamedDeclarationCountOnceEvenWithoutAliases()throws Exception {
        var capacity=NativeCacheCapacity.resolve(config(8*MIB,4*MIB,named("shared",2*MIB),named("unused",3*MIB)));assertEquals(12*MIB,capacity.defaultCacheBytes());assertEquals(4*MIB,capacity.writeBufferManagerBytes());assertEquals(21*MIB,capacity.totalBytes());assertEquals(3,capacity.blockCaches().size());assertThrows(UnsupportedOperationException.class,()->capacity.blockCaches().put("extra",1L));
        var parser=new ConfigParser();parser.addSourceBytes("database.global { block-cache=8MiB, write-buffer-manager=4MiB, block-caches=[{name=shared,size=2MiB}] }\ndatabase.global.column-options=[{name=a,block-cache-name=shared},{name=b,block-cache-name=shared}]".getBytes(java.nio.charset.StandardCharsets.UTF_8));assertEquals(18*MIB,NativeCacheCapacity.resolve(parser.parse()).totalBytes());
    }
    @Test void invalidNamesSizesAndAllArithmeticOverflowBeforeAllocation()throws Exception {
        assertThrows(RocksDBException.class,()->NativeCacheCapacity.resolve(config(-1L,0L)));assertThrows(RocksDBException.class,()->NativeCacheCapacity.resolve(config(1L,-1L)));
        for(var value:new BlockCacheConfig[]{named("",1L),named("default",1L),named("missing",null),named("zero",0L)})assertThrows(RocksDBException.class,()->NativeCacheCapacity.resolve(config(8L,0L,value)));
        var duplicate=named("same",1L);assertThrows(RocksDBException.class,()->NativeCacheCapacity.resolve(config(8L,0L,duplicate,duplicate)));
        assertThrows(RocksDBException.class,()->NativeCacheCapacity.resolve(config(Long.MAX_VALUE,1L)));assertThrows(RocksDBException.class,()->NativeCacheCapacity.resolve(config(Long.MAX_VALUE,0L)));assertThrows(RocksDBException.class,()->NativeCacheCapacity.resolve(config(2L,1L,named("huge",Long.MAX_VALUE))));
    }
}
