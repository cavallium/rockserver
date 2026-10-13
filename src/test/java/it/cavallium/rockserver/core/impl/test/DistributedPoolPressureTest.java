package it.cavallium.rockserver.core.impl.test;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class DistributedPoolPressureTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    @Test void nativePressureAndFreeSpaceAreSampledWithoutSnapshotOwnership() throws Exception {
        var helper=new NativeHomeRecoveryTest();var fixture=helper.new Fixture(root);
        try {
            var normal=fixture.pool.currentWorkloadPressure(0);
            assertFalse(normal.pressured());assertEquals(0,normal.reasonMask());assertTrue(normal.freeBytes()>0);
            assertTrue(System.nanoTime()-normal.sampledAtNanos()>=0);
            var limited=fixture.pool.currentWorkloadPressure(Long.MAX_VALUE);
            assertTrue(limited.pressured());assertTrue(limited.diskPressure());assertEquals(0,limited.reasonMask());
            assertEquals(0,fixture.store.activeSnapshotPins());
        } finally {fixture.close();}
        assertThrows(IOException.class,()->fixture.pool.currentWorkloadPressure(0));
    }
}
