package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import static org.junit.jupiter.api.Assertions.*;

class NativeConfiguredSchemaDeletionTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;

    @Test void configuredDeletedColumnStaysAbsentBeforeAnyRangeReopens() throws Exception {
        Path configPath=root.resolve("named.conf"),dbPath=root.resolve("native");
        Files.writeString(configPath,Files.readString(Path.of("src/main/resources/it/cavallium/rockserver/core/resources/default.conf"))+"""
                database.global.column-options: [
                  ${database.global.fallback-column-options} { name: "default" },
                  ${database.global.fallback-column-options} { name: "data" }
                ]
                """);
        var config=ConfigParser.parse(configPath); var helper=new NativeSchemaInstallationTest(); var source=helper.source();
        var target=new DistributedSchema(source.clusterId(),source.databaseId(),source.domainId(),1,0,List.of());
        var change=helper.change(source,target,List.of(helper.descriptor(helper.group,null,null)));
        var logger=LoggerFactory.getLogger(getClass());
        try (var pool=DistributedStoragePool.open(dbPath,config,logger)) {
            assertFalse(hasColumn(dbPath,"data"),"Configuration supplies options; bootstrap creates the column explicitly");
            var store=helper.store(pool,helper.group,source,null,null); store.enableLogicalSchema(source);
            assertTrue(hasColumn(dbPath,"data")); store.applyNoop(new ApplyPosition(1,0));
            store.applySchemaFence(new ApplyPosition(1,1),change.header());
            pool.installSchema(store,new ApplyPosition(1,2),change,Map.of(),Set.of());
            assertFalse(hasColumn(dbPath,"data"));
        }
        try (var pool=DistributedStoragePool.open(dbPath,config,logger)) {
            assertFalse(hasColumn(dbPath,"data"),"Opening the configured pool must not resurrect the deleted CF");
            var recovered=helper.store(pool,helper.group,source,null,null);
            assertEquals(Set.of(),recovered.ownership().interval().columns());
            assertArrayEquals(target.digest(),recovered.schemaDigest()); assertFalse(hasColumn(dbPath,"data"));
        }
    }

    private static boolean hasColumn(Path path,String name) throws Exception {
        try (var options=new org.rocksdb.Options()) {
            return org.rocksdb.RocksDB.listColumnFamilies(options,path.toString()).stream()
                    .anyMatch(bytes -> Arrays.equals(bytes,name.getBytes(StandardCharsets.US_ASCII)));
        }
    }

    @Test void admittedLongPhysicalNameReopensAndCanBeDeleted() throws Exception {
        var helper=new NativeSchemaInstallationTest(); var source=helper.source(); var path=root.resolve("long-name");
        String name="n".repeat(1025);
        try (var pool=helper.open(path)) {
            var store=pool.openRange(new RangeOwnership.Owner(helper.group,1),new RangeOwnership.Interval(source.domainId(),source.columnIds(),null,null),
                    source.digest(),Map.of(helper.column,name),Set.of());
            store.enableLogicalSchema(source); store.applyNoop(new ApplyPosition(1,0));
        }
        try (var pool=helper.open(path)) {
            var store=pool.openRange(new RangeOwnership.Owner(helper.group,1),new RangeOwnership.Interval(source.domainId(),source.columnIds(),null,null),
                    source.digest(),Map.of(helper.column,name),Set.of());
            store.enableLogicalSchema(source);
            var target=new DistributedSchema(source.clusterId(),source.databaseId(),source.domainId(),1,0,List.of());
            var change=helper.change(source,target,List.of(helper.descriptor(helper.group,null,null)));
            store.applySchemaFence(new ApplyPosition(1,1),change.header());
            pool.installSchema(store,new ApplyPosition(1,2),change,Map.of(),Set.of());
            assertFalse(hasColumn(path,name));
        }
    }
}
