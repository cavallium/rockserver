package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.*;

class NativeRangeStateViewTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    @Test void identityAppliedPositionAndAuthorityAlwaysBelongToTheSamePinnedCut() throws Exception {
        try (var f=new NativeSessionBindingTest.Fixture(root.resolve("native"))) {
            f.store.enableSessionControls(); var ledger=NativeSessionBindingTest.issued();
            assertEquals(CommandResult.Status.SUCCESS,f.apply(NativeSessionBindingTest.bind(ledger,0)));
            try (var old=f.store.capture()) {
                long sequence=f.db.getLatestSequenceNumber(); var before=old.stateView();
                assertEquals(new ApplyPosition(1,0),before.applied()); assertEquals(SessionAuthority.from(ledger),before.sessionAuthority());
                assertEquals(NativeSessionBindingTest.GROUP,before.groupId()); assertEquals(NativeSessionBindingTest.DOMAIN,before.domainId());
                assertEquals(sequence,f.db.getLatestSequenceNumber());
                var advanced=ledger.advanceTime(50);
                assertEquals(CommandResult.Status.SUCCESS,f.apply(NativeSessionBindingTest.control(new OperationId(NativeSessionBindingTest.ADMIN,2,1),RangeSessionControl.advance(advanced))));
                assertEquals(before,old.stateView(),"A later authority update cannot contaminate a pinned metadata reply");
                try (var current=f.store.capture()) {
                    var after=current.stateView(); assertEquals(new ApplyPosition(1,1),after.applied());
                    assertEquals(SessionAuthority.from(advanced),after.sessionAuthority());
                    assertThrows(IllegalStateException.class,() -> before.requireServing(SessionAuthority.from(advanced)));
                    assertDoesNotThrow(() -> after.requireServing(SessionAuthority.from(advanced)));
                }
            }
            assertEquals(0,f.store.activeSnapshotPins());
        }
    }
}
