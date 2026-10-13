package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.DistributedSchema;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class DistributedOperatorArtifactsTest {
    @TempDir Path root;
    private static Object cache(Path path) throws Exception {
        var type=Class.forName("it.cavallium.rockserver.core.impl.rocksdb.DistributedOperatorArtifacts");
        var constructor=type.getDeclaredConstructor(Path.class); constructor.setAccessible(true); return constructor.newInstance(path);
    }
    private static Object invoke(Object cache,String method,Class<?>[] types,Object... arguments) throws Exception {
        var target=cache.getClass().getDeclaredMethod(method,types); target.setAccessible(true);
        try { return target.invoke(cache,arguments); }
        catch(java.lang.reflect.InvocationTargetException failure) {
            if(failure.getCause() instanceof Exception cause) throw cause;
            if(failure.getCause() instanceof Error cause) throw cause;
            throw failure;
        }
    }
    private static void qualify(Object cache,DistributedSchema schema,Map<UUID,String> plain,boolean virgin) throws Exception {
        invoke(cache,"qualifyBootstrap",new Class<?>[]{Collection.class,Map.class,Map.class,boolean.class},List.of(schema),Map.of(),plain,virgin);
    }
    @Test void staleConcurrentCacheCannotOverwritePublishedBootstrapOrImmutableRows() throws Exception {
        var path=root.resolve("pool"); var schema=new DistributedSchema(UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),0,0,List.of());
        var first=cache(path); var stale=cache(path); UUID one=UUID.randomUUID(),two=UUID.randomUUID();
        qualify(first,schema,Map.of(one,"first"),true);
        var directory=(Path)invoke(first,"directory",new Class<?>[]{}); byte[] original=Files.readAllBytes(directory.resolve("bindings"));
        assertThrows(java.io.IOException.class,() -> qualify(stale,schema,Map.of(two,"second"),true));
        assertArrayEquals(original,Files.readAllBytes(directory.resolve("bindings")));
        var reopened=cache(path); qualify(reopened,schema,Map.of(one,"first"),false);
        assertEquals(true,invoke(reopened,"contains",new Class<?>[]{String.class},"first"));
        assertEquals(false,invoke(reopened,"contains",new Class<?>[]{String.class},"second"));
    }
    @Test void bucketBirthPersistsExactShapeAndReopensTheWrapperRatherThanRawOperator() throws Exception {
        var helper=new NativeOperatorInstallationTest(); byte[] jar=DistributedMergeOperatorRegistryTest.classes(NativeOperatorInstallationTest.AppendOperator.class);
        var fixed=helper.target(jar); var column=fixed.columns().getFirst(); var operator=column.operator();
        var shape=it.cavallium.rockserver.core.common.ColumnSchema.of(it.unimi.dsi.fastutil.ints.IntArrayList.of(1),it.unimi.dsi.fastutil.objects.ObjectArrayList.of(it.cavallium.rockserver.core.common.ColumnHashType.ALLSAME8),true,operator.name(),operator.version(),operator.className());
        var bucket=new DistributedSchema(fixed.clusterId(),fixed.databaseId(),fixed.domainId(),fixed.schemaEpoch(),0,List.of(new DistributedSchema.Column(column.apiId(),column.columnId(),column.name(),shape,operator)));
        Path path=root.resolve("bucket-shape");
        it.cavallium.rockserver.core.impl.rocksdb.DistributedStoragePool.stageOperator(path,operator,new it.cavallium.rockserver.core.impl.MergeOperatorRegistry.DeterministicContract("append-contract",1,true),jar);
        var cache=cache(path); var names=Map.of(column.columnId(),"bucket");
        invoke(cache,"qualifyBootstrap",new Class<?>[]{Collection.class,Map.class,Map.class,boolean.class},List.of(bucket),names,Map.of(),true);
        var reopened=cache(path);
        try(var callback=(it.cavallium.rockserver.core.impl.FFMAbstractMergeOperator)invoke(reopened,"resolve",new Class<?>[]{String.class},"bucket")) { assertInstanceOf(it.cavallium.rockserver.core.impl.rocksdb.DistributedBucketMergeOperator.class,callback); }
        assertThrows(java.io.IOException.class,() -> invoke(reopened,"requireSchemaBindings",new Class<?>[]{DistributedSchema.class,Map.class},fixed,names));
        var wrongShape=it.cavallium.rockserver.core.common.ColumnSchema.of(it.unimi.dsi.fastutil.ints.IntArrayList.of(1),it.unimi.dsi.fastutil.objects.ObjectArrayList.of(it.cavallium.rockserver.core.common.ColumnHashType.XXHASH8),true,operator.name(),operator.version(),operator.className());
        var changed=new DistributedSchema(bucket.clusterId(),bucket.databaseId(),bucket.domainId(),bucket.schemaEpoch(),0,List.of(new DistributedSchema.Column(column.apiId(),column.columnId(),column.name(),wrongShape,operator)));
        assertThrows(java.io.IOException.class,() -> invoke(reopened,"requireSchemaBindings",new Class<?>[]{DistributedSchema.class,Map.class},changed,names));
        assertThrows(RuntimeException.class,() -> it.cavallium.rockserver.core.impl.rocksdb.DistributedStoragePool.open(path,it.cavallium.rockserver.core.config.ConfigParser.parseDefault(),org.slf4j.LoggerFactory.getLogger(getClass()),List.of(changed),names,Map.of()));
        assertFalse(Files.exists(path.resolve("CURRENT")),"Wrong shape must be rejected before writable native open");
    }

}
