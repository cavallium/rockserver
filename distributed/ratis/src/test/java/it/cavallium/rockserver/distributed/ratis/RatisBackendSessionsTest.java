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
    static List<RatisBackendSessions.Target> targets(RatisDatabaseBackupFactoryFixture fixture){var result=new ArrayList<RatisBackendSessions.Target>();for(var home:fixture.homes){for(var range:home.data.directory.ranges())result.add(new RatisBackendSessions.Target(fixture.access.get(home.data.domain).routed(),range.groupId(),home.data.directory));result.add(new RatisBackendSessions.Target(home.routed,home.group,home.routed.directory()));}return result;}
    @Test void twoDomainsDistinctPointBulkWindowsFreezeIssuanceAndRecoverLostAppliedReply()throws Exception{
        try(var fixture=new RatisDatabaseBackupFactoryFixture(root.resolve("sessions"))){
            var routed=fixture.access.get(fixture.homes.getFirst().data.domain).routed();var limits=RatisSessionClient.Limits.defaults();Path point=root.resolve("point"),bulk=root.resolve("bulk");var epochs=fixture.catalogue.epochs;
            long next=epochs.read().nextEpoch();fixture.loseIssuance=true;assertThrows(IOException.class,()->RatisBackendSessions.window(point,routed,epochs,60000,limits,targets(fixture),RatisJournalAccess.standalone()));assertEquals(next+1,epochs.read().nextEpoch());
            try(var p=RatisBackendSessions.window(point,routed,epochs,60000,limits,targets(fixture),RatisJournalAccess.standalone());var b=RatisBackendSessions.window(bulk,routed,epochs,60000,limits,targets(fixture),RatisJournalAccess.standalone())){assertNotEquals(p.grant().sessionId(),b.grant().sessionId());assertEquals(60000,p.grant().expiresAtMillis());assertEquals(next+2,epochs.read().nextEpoch());}
            long issued=epochs.read().nextEpoch();try(var p=RatisBackendSessions.window(point,routed,epochs,1,limits,targets(fixture),RatisJournalAccess.standalone())){assertEquals(60000,p.grant().expiresAtMillis());assertEquals(issued,epochs.read().nextEpoch());}
            Files.delete(point.resolve("window.bin"));assertThrows(IOException.class,()->RatisBackendSessions.window(point,routed,epochs,60000,limits,targets(fixture),RatisJournalAccess.standalone()));assertEquals(issued,epochs.read().nextEpoch());
        }
    }
    @Test void lostDataBindingReplyReopensExactMaintenanceBeforeExposingWindow()throws Exception{
        try(var fixture=new RatisDatabaseBackupFactoryFixture(root.resolve("binding"))){var routed=fixture.access.get(fixture.homes.getFirst().data.domain).routed();Path path=root.resolve("window");fixture.loseDataBinding=true;assertThrows(IOException.class,()->RatisBackendSessions.window(path,routed,fixture.catalogue.epochs,60000,RatisSessionClient.Limits.defaults(),targets(fixture),RatisJournalAccess.standalone()));long next=fixture.catalogue.epochs.read().nextEpoch();try(var owner=RatisBackendSessions.window(path,routed,fixture.catalogue.epochs,60000,RatisSessionClient.Limits.defaults(),targets(fixture),RatisJournalAccess.standalone())){assertEquals(next,fixture.catalogue.epochs.read().nextEpoch());assertTrue(owner.pristine());}}
    }
    @Test void retainedBindingCannotSubstituteGrantWindowOrRecreateMissingIdentity()throws Exception{
        try(var fixture=new RatisDatabaseBackupFactoryFixture(root.resolve("binding-identity"))){Path path=root.resolve("identity");var epochs=fixture.catalogue.epochs;var issued=RatisBackendSessions.issue(path,epochs,60000,RatisJournalAccess.standalone());var targets=targets(fixture);RatisBackendSessions.bind(path,epochs,issued,32,targets,RatisJournalAccess.standalone());
            assertThrows(IOException.class,()->RatisBackendSessions.bind(path,epochs,issued,33,targets,RatisJournalAccess.standalone()));Path marker=path.resolve("binding-"+targets.getFirst().group()+".identity");Files.delete(marker);assertThrows(IOException.class,()->RatisBackendSessions.bind(path,epochs,issued,32,targets,RatisJournalAccess.standalone()));
        }
    }

    @Test void retainedOldGenerationPreflightDeniesBeforeIssueAndColdRetryKeepsExactGrant()throws Exception {
        try(var fixture=new RatisDatabaseBackupFactoryFixture(root.resolve("native"))){
            Path backend=Files.createDirectories(root.resolve("backend")),old=Files.createDirectories(backend.resolve("generations/old")),fresh=backend.resolve("generations/new/session");
            SessionJournalFiles.write(old.resolve("input.released"),new byte[300],1024);
            var limits=new RemoteMutationOwnerLimits(1,2,1,1,0,4,128L<<20,16L<<20,8,16L<<20,8L<<20,1);
            var routed=fixture.access.get(fixture.homes.getFirst().data.domain).routed();var targets=targets(fixture);var epochs=fixture.catalogue.epochs;long before=epochs.read().nextEpoch();
            try(var denied=new RatisRequestBudget(limits)){RatisBackendJournalInventory.inventory(backend,denied);long memory=denied.chargedBytes(),disk=denied.chargedDiskBytes();assertThrows(RuntimeException.class,()->RatisBackendSessions.window(fresh,routed,epochs,60000,RatisSessionClient.Limits.defaults(),targets,denied));assertEquals(before,epochs.read().nextEpoch());assertFalse(Files.exists(fresh.resolve("issuer.bin")));assertEquals(memory,denied.chargedBytes());assertEquals(disk,denied.chargedDiskBytes());}
            limits=new RemoteMutationOwnerLimits(1,2,1,1,0,4,128L<<20,16L<<20,128,16L<<20,8L<<20,1);
            try(var denied=new RatisRequestBudget(limits)){RatisBackendJournalInventory.inventory(backend,denied);var filler=backend.resolve("retained-history");Files.write(filler,new byte[0]);try(var history=denied.disk(filler,16<<20)){history.reservePeak((16L<<20)-4096);}long memory=denied.chargedBytes(),disk=denied.chargedDiskBytes();assertThrows(RuntimeException.class,()->RatisBackendSessions.window(fresh,routed,epochs,60000,RatisSessionClient.Limits.defaults(),targets,denied));assertEquals(before,epochs.read().nextEpoch());assertFalse(Files.exists(fresh.resolve("issuer.bin")));assertEquals(memory,denied.chargedBytes());assertEquals(disk,denied.chargedDiskBytes());denied.fulfilled(new RatisJournalAccess.Reservation(filler,16<<20,(16L<<20)-4096));try(var accepted=RatisBackendSessions.window(fresh,routed,epochs,60000,RatisSessionClient.Limits.defaults(),targets,denied)){assertTrue(accepted.pristine());assertEquals(before+1,epochs.read().nextEpoch());}}
            Path replay=backend.resolve("generations/retry/session");long next=epochs.read().nextEpoch();SessionEpochLedger.Grant original;
            try(var budget=new RatisRequestBudget(limits)){RatisBackendJournalInventory.inventory(backend,budget);fixture.loseIssuance=true;assertThrows(IOException.class,()->RatisBackendSessions.window(replay,routed,epochs,60000,RatisSessionClient.Limits.defaults(),targets,budget));assertEquals(next+1,epochs.read().nextEpoch());original=epochs.read().grants().getLast();}
            try(var cold=new RatisRequestBudget(limits)){RatisBackendJournalInventory.inventory(backend,cold);try(var window=RatisBackendSessions.window(replay,routed,epochs,1,RatisSessionClient.Limits.defaults(),targets,cold)){assertEquals(original,window.grant());assertEquals(next+1,epochs.read().nextEpoch());assertTrue(window.pristine());assertTrue(cold.chargedDiskBytes()>332);}}
        }
    }

}
