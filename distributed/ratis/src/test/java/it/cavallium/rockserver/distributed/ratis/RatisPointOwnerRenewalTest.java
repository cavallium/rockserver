package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisPointOwnerRenewalTest {
    @TempDir(factory=RatisDiskTestDirectory.class)Path root;
    static <T>T await(CompletableFuture<T> value)throws Exception{return value.get(20,TimeUnit.SECONDS);}
    final class Fixture implements AutoCloseable {
        final RatisAtomicClientTest.Fixture f;
        final RatisRequestBudget budget;final RatisRocksDBConnection.SessionFactory sessions;final AtomicInteger opens=new AtomicInteger();
        final AtomicBoolean loseIssue=new AtomicBoolean(),loseBinding=new AtomicBoolean();final AtomicReference<SessionEpochLedger.Grant> lost=new AtomicReference<>();
        volatile CountDownLatch entered,release;final AtomicLong commands=new AtomicLong(900);final Path ownersRoot;
        Fixture(Path fixtureRoot,int history)throws Exception {
            f=new RatisAtomicClientTest.Fixture(fixtureRoot.resolve("native"),true);ownersRoot=fixtureRoot.resolve("owners");
            budget=new RatisRequestBudget(new RemoteMutationOwnerLimits(1,history,history,128L<<20,16L<<20,8L<<20,1));
            var directories=f.catalogue.epochs.catalogue(f.outer.directoryColumn);assertEquals(CommandResult.Status.SUCCESS,directories.execute(directories.prepare(f.outer.operation(commands.getAndIncrement()),null,f.directory)).status());
            sessions=new RatisRocksDBConnection.SessionFactory(List.of(f.data,f.homeClient),(path,schema,plan)->{
                opens.incrementAndGet();
                if(loseIssue.getAndSet(false)){lost.set(RatisBackendSessions.issue(path,f.catalogue.epochs,5).grant());throw new IOException("Lost original issue reply");}
                if(loseBinding.getAndSet(false)){var issued=RatisBackendSessions.issue(path,f.catalogue.epochs,5);lost.set(issued.grant());RatisBackendSessions.bind(path,f.catalogue.epochs,issued,32,plan.targets());throw new IOException("Lost original binding reply");}
                var window=RatisBackendSessions.window(path,f.data,f.catalogue.epochs,5,RatisSessionClient.Limits.defaults(),plan.targets());
                if(entered!=null){entered.countDown();try{if(!release.await(10,TimeUnit.SECONDS)){window.close();throw new IOException("Renewal fixture barrier timed out");}}catch(InterruptedException failure){window.close();throw new IOException(failure);}}
                return window;
            });
        }
        RatisPointOwnerManager open()throws IOException{return new RatisPointOwnerManager(f.schema,f.directory,f.data,f.scheduler,ownersRoot,budget,sessions,f.catalogue.epochs,f.catalogue.epochs.catalogue(f.outer.directoryColumn),header->{throw new IOException("No relocation in this fixture");},f.homes);}
        void expire(RemoteMutationOwner owner)throws Exception {var before=f.catalogue.epochs.read();var grant=before.grants().stream().filter(value->value.sessionId().equals(owner.ownerId()) && value.epoch()==owner.ownerEpoch()).findFirst().orElseThrow();var after=before.advanceTime(grant.expiresAtMillis());assertEquals(CommandResult.Status.SUCCESS,f.catalogue.epochs.execute(f.catalogue.epochs.prepare(f.outer.operation(commands.getAndIncrement()),before,after)).status());}
        RemoteMutationToken write(RatisPointOwnerManager manager,RemoteMutationOwner owner,int key,int value)throws Exception {
            var actor=manager.select(owner);var token=actor.prepareSequence(owner,UUID.randomUUID(),owner.nextRequestSequence(),71,List.of(RatisAtomicClientTest.key(key)),List.of(RatisAtomicClientTest.value(value)),RatisRocksDBDirect.WriteKind.PUT,RequestType.RequestTypeId.NOTHING,false,RequestContext.batch()).get(20,TimeUnit.SECONDS);await(actor.execute(token,RequestContext.batch()));actor.acknowledgeResult(token,actor.status(token).resultDigest());return token;
        }
        public void close()throws Exception{budget.close();f.close();}
    }
    @Test void expiredQuiescentGrantRenewsOnceAndOldTokenRemainsAckedAcrossColdReopen()throws Exception {
        try(var x=new Fixture(root,4)) {
            RemoteMutationToken oldToken,newToken;RemoteMutationOwner replacement;
            try(var manager=x.open()) {
                var original=await(manager.discover(RequestContext.batch())).getFirst();oldToken=x.write(manager,original,2,7);int grants=x.f.catalogue.epochs.read().grants().size();assertEquals(original.ownerId(),await(manager.discover(RequestContext.batch())).getFirst().ownerId());assertEquals(grants,x.f.catalogue.epochs.read().grants().size(),"Healthy quiescence must not rotate grants");
                x.expire(original);replacement=await(manager.discover(RequestContext.batch())).getFirst();assertNotEquals(original.ownerId(),replacement.ownerId());assertEquals(0,replacement.nextRequestSequence());assertEquals(RemoteMutationStatus.State.ACKNOWLEDGED,manager.select(oldToken).status(oldToken).state());assertThrows(ExecutionException.class,()->await(manager.select(original).prepareSequence(original,UUID.randomUUID(),1,71,List.of(RatisAtomicClientTest.key(4)),List.of(RatisAtomicClientTest.value(9)),RatisRocksDBDirect.WriteKind.PUT,RequestType.RequestTypeId.NOTHING,false,RequestContext.batch())));
                newToken=x.write(manager,replacement,3,8);assertArrayEquals(new byte[]{7},x.f.get(71,RatisAtomicClientTest.key(2)));assertArrayEquals(new byte[]{8},x.f.get(71,RatisAtomicClientTest.key(3)));assertEquals(2,manager.all().size());
            }
            int grants=x.f.catalogue.epochs.read().grants().size();try(var cold=x.open()){assertEquals(replacement.ownerId(),await(cold.discover(RequestContext.batch())).getFirst().ownerId());assertEquals(RemoteMutationStatus.State.ACKNOWLEDGED,cold.select(oldToken).status(oldToken).state());assertEquals(RemoteMutationStatus.State.ACKNOWLEDGED,cold.select(newToken).status(newToken).state());assertEquals(grants,x.f.catalogue.epochs.read().grants().size());}
        }
    }
    @Test void preparedAndTerminalUnackedRequestsPreventRenewalAndHistoryQuotaPrecedesIssuance()throws Exception {
        try(var x=new Fixture(root,1);var manager=x.open()) {
            var owner=manager.available().getFirst();var actor=manager.select(owner);var token=await(actor.prepareSequence(owner,UUID.randomUUID(),0,71,List.of(RatisAtomicClientTest.key(2)),List.of(RatisAtomicClientTest.value(9)),RatisRocksDBDirect.WriteKind.PUT,RequestType.RequestTypeId.NOTHING,false,RequestContext.batch()));x.expire(owner);int opens=x.opens.get();assertThrows(ExecutionException.class,()->await(manager.discover(RequestContext.batch())));assertEquals(RemoteMutationStatus.State.PREPARED,actor.status(token).state());assertEquals(opens,x.opens.get());
            try{await(actor.execute(token,RequestContext.batch()));}catch(ExecutionException expired){assertEquals(CommandResult.Status.SESSION_EXPIRED,actor.status(token).outcome());}assertEquals(RemoteMutationStatus.State.TERMINAL,actor.status(token).state());assertThrows(ExecutionException.class,()->await(manager.discover(RequestContext.batch())));assertEquals(opens,x.opens.get());actor.acknowledgeResult(token,actor.status(token).resultDigest());int grants=x.f.catalogue.epochs.read().grants().size();assertThrows(ExecutionException.class,()->await(manager.discover(RequestContext.batch())));assertEquals(opens,x.opens.get());assertEquals(grants,x.f.catalogue.epochs.read().grants().size());assertEquals(1,manager.all().size());assertTrue(manager.available().isEmpty());
        }
    }
    @Test void lostIssueAndBindingRepliesReopenTheSameForcedRenewalIntent()throws Exception {
        for(boolean binding:List.of(false,true))try(var x=new Fixture(root.resolve("lost-"+binding),4)) {
            SessionEpochLedger.Grant issued;
            try(var manager=x.open()){var owner=manager.available().getFirst();x.write(manager,owner,2,1);x.expire(owner);if(binding)x.loseBinding.set(true);else x.loseIssue.set(true);assertThrows(ExecutionException.class,()->await(manager.discover(RequestContext.batch())));issued=x.lost.get();assertNotNull(issued);assertEquals(1,manager.all().size());}
            try(var cold=x.open()){var owner=await(cold.discover(RequestContext.batch())).getFirst();assertEquals(issued.sessionId(),owner.ownerId());assertEquals(issued.epoch(),owner.ownerEpoch());int grants=x.f.catalogue.epochs.read().grants().size();x.write(cold,owner,3,2);assertEquals(grants,x.f.catalogue.epochs.read().grants().size());assertArrayEquals(new byte[]{2},x.f.get(71,RatisAtomicClientTest.key(3)));}
        }
    }
    @Test void canceledRenewalObserverKeepsNewOwnerCreditAndSelectorForceUncertaintyReopensExactly()throws Exception {
        try(var x=new Fixture(root,4)) {
            UUID replacement;
            try(var manager=x.open()){var owner=manager.available().getFirst();x.write(manager,owner,2,1);x.expire(owner);x.entered=new CountDownLatch(1);x.release=new CountDownLatch(1);var renewal=manager.discover(RequestContext.batch());assertTrue(x.entered.await(10,TimeUnit.SECONDS));long charged=x.budget.chargedBytes();assertTrue(renewal.cancel(false));assertEquals(charged,x.budget.chargedBytes());assertThrows(IOException.class,manager::close);x.release.countDown();await(x.budget.recover(()->null));replacement=manager.available().getFirst().ownerId();assertNotEquals(owner.ownerId(),replacement);x.entered=null;var current=manager.available().getFirst();x.write(manager,current,3,2);x.expire(current);manager.afterSelectorForce(()->{throw new IllegalStateException("Lost selector force reply");});assertThrows(ExecutionException.class,()->await(manager.discover(RequestContext.batch())));assertTrue(manager.available().isEmpty());}
            try(var cold=x.open()){var owner=await(cold.discover(RequestContext.batch())).getFirst();assertNotEquals(replacement,owner.ownerId());int grants=x.f.catalogue.epochs.read().grants().size();x.write(cold,owner,4,3);assertEquals(grants,x.f.catalogue.epochs.read().grants().size());}
        }
    }
    @Test void quiescentExpiredOwnerRenewsOntoActualActivatedSplitChildrenAndKeepsOldTokenAcrossColdReopen()throws Exception {
        var helper=new RatisOperationRerouteCoordinatorTest();helper.root=root;
        try(var f=helper.new Fixture();var stale=new RatisRoutedClient(f.source.directory,f.transport());var budget=new RatisRequestBudget(new RemoteMutationOwnerLimits(1,4,4,128L<<20,16L<<20,8L<<20,1))) {
            f.window.close();f.coordinator.close();
            var sessions=new RatisRocksDBConnection.SessionFactory(List.of(stale,f.homeClient),(path,schema,plan)->{
                if(Arrays.equals(plan.directory().payloadDigest(),f.source.directory.payloadDigest()))return RatisDurableSessionClient.reopen(f.windowPath,stale);
                return RatisBackendSessions.window(path,stale,f.catalogue.epochs,1000,RatisSessionClient.Limits.defaults(),plan.targets());
            });
            Path owners=root.resolve("split-owners");var directories=f.catalogue.epochs.catalogue(f.outer.directoryColumn);RemoteMutationToken originalToken,newToken;RemoteMutationOwner replacement;
            try(var manager=new RatisPointOwnerManager(f.source.schema,f.source.directory,stale,f.scheduler,owners,budget,sessions,f.catalogue.epochs,directories,header->{var definition=TopologyChange.Definition.decode(f.point(RatisTopologyCatalogue.definitionKey(header.definitionDigest())));header.requireDefinition(definition);return definition.sourceDirectory();},f.homes)) {
                var original=manager.available().getFirst();var actor=manager.select(original);originalToken=await(actor.prepareSequence(original,UUID.randomUUID(),0,1,List.of(RatisAtomicClientTest.key(10)),List.of(RatisAtomicClientTest.value(77)),RatisRocksDBDirect.WriteKind.PUT,RequestType.RequestTypeId.NOTHING,false,RequestContext.batch()));await(actor.execute(originalToken,RequestContext.batch()));actor.acknowledgeResult(originalToken,actor.status(originalToken).resultDigest());assertEquals(1,f.opens);assertEquals(1,f.consumes.size());assertArrayEquals(new byte[]{77},f.data.read(f.source.column,new byte[]{10}));
                var before=f.catalogue.epochs.read();var grant=before.grants().stream().filter(value->value.sessionId().equals(original.ownerId())).findFirst().orElseThrow();var expired=before.advanceTime(grant.expiresAtMillis());assertEquals(CommandResult.Status.SUCCESS,f.catalogue.epochs.execute(f.catalogue.epochs.prepare(f.outer.operation(901),before,expired)).status());
                replacement=await(manager.discover(RequestContext.batch())).getFirst();assertNotEquals(original.ownerId(),replacement.ownerId());assertArrayEquals(f.source.target.payloadDigest(),replacement.directoryDigest());assertFalse(Arrays.equals(original.directoryDigest(),replacement.directoryDigest()));var current=manager.select(replacement);newToken=await(current.prepareSequence(replacement,UUID.randomUUID(),0,1,List.of(RatisAtomicClientTest.key(200)),List.of(RatisAtomicClientTest.value(88)),RatisRocksDBDirect.WriteKind.PUT,RequestType.RequestTypeId.NOTHING,false,RequestContext.batch()));await(current.execute(newToken,RequestContext.batch()));current.acknowledgeResult(newToken,current.status(newToken).resultDigest());assertArrayEquals(new byte[]{88},f.data.read(f.source.column,new byte[]{(byte)200}));assertTrue(manager.contains(newToken));assertEquals(RemoteMutationStatus.State.ACKNOWLEDGED,manager.select(originalToken).status(originalToken).state());
            }
            int grants=f.catalogue.epochs.read().grants().size();try(var cold=new RatisPointOwnerManager(f.source.schema,f.source.directory,stale,f.scheduler,owners,budget,sessions,f.catalogue.epochs,directories,header->{var definition=TopologyChange.Definition.decode(f.point(RatisTopologyCatalogue.definitionKey(header.definitionDigest())));header.requireDefinition(definition);return definition.sourceDirectory();},f.homes)) {
                assertEquals(replacement.ownerId(),await(cold.discover(RequestContext.batch())).getFirst().ownerId());assertEquals(RemoteMutationStatus.State.ACKNOWLEDGED,cold.select(originalToken).status(originalToken).state());assertEquals(RemoteMutationStatus.State.ACKNOWLEDGED,cold.select(newToken).status(newToken).state());assertTrue(cold.contains(newToken));assertEquals(grants,f.catalogue.epochs.read().grants().size());
            }
        }
    }

}
