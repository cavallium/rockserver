package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Actual native catalogue issuance and HOME/DATA bindings through the current public maintenance facades. */
class RatisBackendSessionsTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static List<RatisBackendSessions.Target> targets(RatisDatabaseBackupFactoryFixture fixture){var result=new ArrayList<RatisBackendSessions.Target>();for(var home:fixture.homes){for(var range:home.data.directory.ranges())result.add(new RatisBackendSessions.Target(fixture.access.get(home.data.domain).routed(),range.groupId()));result.add(new RatisBackendSessions.Target(home.routed,home.group));}return result;}
    @Test void twoDomainsDistinctPointBulkWindowsFreezeIssuanceAndRecoverLostAppliedReply()throws Exception{
        try(var fixture=new RatisDatabaseBackupFactoryFixture(root.resolve("sessions"))){
            var routed=fixture.access.get(fixture.homes.getFirst().data.domain).routed();var limits=RatisSessionClient.Limits.defaults();Path point=root.resolve("point"),bulk=root.resolve("bulk");var epochs=fixture.catalogue.epochs;
            long next=epochs.read().nextEpoch();fixture.loseIssuance=true;assertThrows(IOException.class,()->RatisBackendSessions.window(point,routed,epochs,60000,limits,targets(fixture)));assertEquals(next+1,epochs.read().nextEpoch());
            try(var p=RatisBackendSessions.window(point,routed,epochs,60000,limits,targets(fixture));var b=RatisBackendSessions.window(bulk,routed,epochs,60000,limits,targets(fixture))){assertNotEquals(p.grant().sessionId(),b.grant().sessionId());assertEquals(60000,p.grant().expiresAtMillis());assertEquals(next+2,epochs.read().nextEpoch());}
            long issued=epochs.read().nextEpoch();try(var p=RatisBackendSessions.window(point,routed,epochs,1,limits,targets(fixture))){assertEquals(60000,p.grant().expiresAtMillis());assertEquals(issued,epochs.read().nextEpoch());}
            Files.delete(point.resolve("window.bin"));assertThrows(IOException.class,()->RatisBackendSessions.window(point,routed,epochs,60000,limits,targets(fixture)));assertEquals(issued,epochs.read().nextEpoch());
        }
    }
    @Test void lostDataBindingReplyReopensExactMaintenanceBeforeExposingWindow()throws Exception{
        try(var fixture=new RatisDatabaseBackupFactoryFixture(root.resolve("binding"))){var routed=fixture.access.get(fixture.homes.getFirst().data.domain).routed();Path path=root.resolve("window");fixture.loseDataBinding=true;assertThrows(IOException.class,()->RatisBackendSessions.window(path,routed,fixture.catalogue.epochs,60000,RatisSessionClient.Limits.defaults(),targets(fixture)));long next=fixture.catalogue.epochs.read().nextEpoch();try(var owner=RatisBackendSessions.window(path,routed,fixture.catalogue.epochs,60000,RatisSessionClient.Limits.defaults(),targets(fixture))){assertEquals(next,fixture.catalogue.epochs.read().nextEpoch());assertTrue(owner.pristine());}}
    }
    @Test void retainedBindingCannotSubstituteGrantWindowOrRecreateMissingIdentity()throws Exception{
        try(var fixture=new RatisDatabaseBackupFactoryFixture(root.resolve("binding-identity"))){Path path=root.resolve("identity");var epochs=fixture.catalogue.epochs;var issued=RatisBackendSessions.issue(path,epochs,60000);var targets=targets(fixture);RatisBackendSessions.bind(path,epochs,issued,32,targets);
            assertThrows(IOException.class,()->RatisBackendSessions.bind(path,epochs,issued,33,targets));Path marker=path.resolve("binding-"+targets.getFirst().group()+".identity");Files.delete(marker);assertThrows(IOException.class,()->RatisBackendSessions.bind(path,epochs,issued,32,targets));
        }
    }

}
