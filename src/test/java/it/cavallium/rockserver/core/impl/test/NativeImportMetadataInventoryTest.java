package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class NativeImportMetadataInventoryTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    @Test void importedBirthRetainsExactIdentityAfterCreateAndColdInventory()throws Exception {
        var helper=new NativeRangeSstImportTest();helper.root=root;var source=helper.schema();var added=UUID.randomUUID();var columns=new ArrayList<>(source.columns());columns.add(new DistributedSchema.Column(2,added,"added",source.columns().getFirst().schema(),null));
        var target=new DistributedSchema(source.clusterId(),source.databaseId(),source.domainId(),1,0,columns);var replica=new ReplicaDescriptor(UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),"local","127.0.0.1:9999",ReplicaDescriptor.Role.VOTER);
        var range=new RangeDescriptor(UUID.randomUUID(),helper.group,1,1,new byte[]{1},new byte[]{5},RangeDescriptor.TopologyState.ACTIVE,List.of(replica));
        var before=new RangeDescriptor(UUID.randomUUID(),UUID.randomUUID(),1,1,null,new byte[]{1},RangeDescriptor.TopologyState.ACTIVE,List.of(replica));var after=new RangeDescriptor(UUID.randomUUID(),UUID.randomUUID(),1,1,new byte[]{5},null,RangeDescriptor.TopologyState.ACTIVE,List.of(replica));
        var directory=new RangeDirectory(source.clusterId(),source.databaseId(),source.domainId(),0,source.digest(),0,source.columnIds(),0,List.of(before,range,after));var creation=new SchemaChange.Definition(UUID.randomUUID(),new OperationId(UUID.randomUUID(),1,0),directory,source,target);
        var filePath=root.resolve("original.sst");var file=helper.file(filePath,false);var manifest=helper.manifest(source,file);var db=root.resolve("pool");byte[] receipt;
        try(var pool=helper.open(db)){
            var store=pool.openNewFencedRange(manifest,source);var imported=pool.importSst(store,manifest,Map.of(file.fileId(),filePath),new ApplyPosition(1,0));receipt=imported.encode();pool.activateImport(store,new NativeRangeSstImport.Activation(imported,UUID.randomUUID(),new ApplyPosition(2,10),new byte[32],new ApplyPosition(1,1)));
            store.applySchemaFence(new ApplyPosition(1,2),creation.header());var installation=pool.installSchema(store,new ApplyPosition(1,3),creation,Map.of(helper.column,"data",added,"added-physical"),Set.of());store.applySchemaActivation(new ApplyPosition(1,4),installation.proof(),UUID.randomUUID(),new ApplyPosition(2,20));
            var write=new DistributedCommand(helper.group,1,target.digest(),new OperationId(UUID.randomUUID(),1,0),List.of(new Mutation(Mutation.Kind.PUT,added,new byte[]{2},new byte[]{9})));assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(store.apply(new ApplyPosition(1,5),write)).status());
            assertTrue(pool.retainedMetadataOwners().getFirst().interval().columns().containsAll(target.columnIds()));assertArrayEquals(receipt,store.importReceipt().encode());
        }
        try(var pool=helper.open(db)){
            var inventory=pool.retainedMetadataOwners();assertEquals(1,inventory.size());var owner=inventory.getFirst();assertEquals(new RangeOwnership.Owner(helper.group,1),owner.owner());assertEquals(target.columnIds(),owner.interval().columns());assertFalse(owner.pending());assertFalse(owner.retired());
            var store=pool.openRange(new RangeOwnership.Owner(helper.group,1),new RangeOwnership.Interval(helper.domain,source.columnIds(),new byte[]{1},new byte[]{5}),source.digest(),Map.of(helper.column,"data"),Set.of());assertArrayEquals(target.digest(),store.schemaDigest());assertArrayEquals(receipt,store.importReceipt().encode());assertArrayEquals(new byte[]{7},store.readCurrentPoint(helper.column,new byte[]{2}));assertArrayEquals(new byte[]{9},store.readCurrentPoint(added,new byte[]{2}));
        }
    }
}
