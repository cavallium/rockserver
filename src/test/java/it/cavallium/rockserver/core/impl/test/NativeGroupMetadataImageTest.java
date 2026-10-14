package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class NativeGroupMetadataImageTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final NativeTopologyCdcTest fixture=new NativeTopologyCdcTest();
    @Test void detachedSealedSourceExportsItsOwnReceiptAndCdcCutWithoutUserHandles()throws Exception {
        try(var f=fixture.new Fixture(root.resolve("source"))) {
            var command=f.write(f.parent,0,9);byte[] result=f.parent.apply(new ApplyPosition(1,1),command);f.freeze();f.seed();
            assertTrue(f.pool.detachRetiredUserHandles(f.parent));assertTrue(f.parent.userHandlesDetached());
            try(var pin=f.parent.captureMetadataForRecovery()) {
                assertEquals(f.definition.sourceDirectory().columns(),pin.columns());assertEquals(f.freeze.position(),pin.position());
                assertThrows(IllegalArgumentException.class,()->pin.visitColumn(fixture.helper.outer.fixed,null,null,(key,value)->fail("Metadata pin read USER rows")));
                var ack=new DistributedCommand(command.groupId(),1,command.schemaDigest(),new OperationId(command.operationId().sessionId(),1,1),List.of(),List.of(),0);
                f.parent.apply(new ApplyPosition(1,3),ack);assertArrayEquals(result,pin.operationResult(command.operationId(),command.receiptDigest()));
                Path image=root.resolve("metadata");var saved=NativeRangeImage.writeMetadataAdmitted(image,pin,pin.interval(),fixture.limits,NativeRangeImage.IoAdmission.UNLIMITED);
                assertEquals(f.freeze.position(),saved.position());assertEquals(saved.position(),NativeRangeImage.inspectMetadata(image,fixture.limits).position());
                assertThrows(IOException.class,()->NativeRangeImage.inspect(image,fixture.limits));
                int[] receipts={0},barriers={0};NativeRangeImage.visitMetadata(image,fixture.limits,(section,column,key,value)->{
                    assertNotEquals(NativeRangeImage.Section.USER,section);
                    if(section==NativeRangeImage.Section.RECEIPTS)receipts[0]++;
                    if(section==NativeRangeImage.Section.STATE && key.length>16){assertNotEquals(0x62,key[16]&255,"Foreign local seed pointer is not metadata recovery authority");if(key[16]==0x60){assertEquals(f.freeze,NativeTopologyFreeze.decode(value));barriers[0]++;}}
                });assertTrue(receipts[0]>0);assertEquals(1,barriers[0]);
                assertThrows(IllegalArgumentException.class,()->NativeRangeImage.write(root.resolve("wrong-kind"),pin,pin.interval(),fixture.limits));
            }
            Path latest=root.resolve("metadata-after-ack");assertEquals(new ApplyPosition(1,3),NativeRangeImage.writeMetadata(latest,f.parent,fixture.limits).position());
            assertEquals(0,f.parent.activeSnapshotPins());assertArrayEquals(fixture.helper.helper.key(9),f.child.readCurrentPoint(fixture.helper.outer.fixed,fixture.helper.helper.key(1)));
            Path data=root.resolve("child-user-image");NativeRangeImage.write(data,f.child,fixture.limits);assertThrows(IOException.class,()->NativeRangeImage.inspectMetadata(data,fixture.limits));
        }
    }
    @Test void unsealedUserOwnerCannotCreateMetadataRecoveryAuthority()throws Exception {
        try(var f=fixture.new Fixture(root.resolve("unsealed"))) {
            assertThrows(IllegalStateException.class,f.parent::captureMetadataForRecovery);
            try(var pin=f.parent.captureForRecovery()) {assertThrows(IllegalArgumentException.class,()->NativeRangeImage.writeMetadataAdmitted(root.resolve("unsealed-image"),pin,pin.interval(),fixture.limits,NativeRangeImage.IoAdmission.UNLIMITED));}
            assertEquals(0,f.parent.activeSnapshotPins());
        }
    }
}
