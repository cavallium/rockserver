package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.*;

/** Actual native original/HOME/children with controlled Raft dispatch; no source publication or socket/quorum claim. */
class RatisClosedRerouteCompletionTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    RatisOperationRerouteCoordinatorTest helper(){var helper=new RatisOperationRerouteCoordinatorTest();helper.root=root;return helper;}
    static void closeAtCredit(RatisOperationRerouteCoordinatorTest.Fixture fixture){fixture.beforeCredit=()->{try{fixture.closeSourceAndHome();}catch(Exception failure){throw new CompletionException(failure);}};}
    static byte[] blob(ByteBuffer b){int n=b.getInt();assertTrue(n>=0 && n<=b.remaining());byte[] result=new byte[n];b.get(result);return result;}
    record State(CommandResult result,RatisHomeSourceClosingRpc.ClosedReservation proof,HomeRecovery abort,boolean credited,boolean published,boolean sourceAcknowledged,boolean done){}
    static State state(byte[] body){var b=ByteBuffer.wrap(body);b.position(36);blob(b);blob(b);byte[] result=blob(b),proof=blob(b),abort=blob(b);boolean credited=b.get()!=0,published=b.get()!=0;for(int n=0;n<3;n++)if(b.get()!=0)b.position(b.position()+32);boolean sourceAck=b.get()!=0,done=b.get()!=0;return new State(result.length==0?null:CommandResult.decode(result),proof.length==0?null:RatisHomeSourceClosingRpc.ClosedReservation.decode(proof),abort.length==0?null:HomeRecovery.decode(abort),credited,published,sourceAck,done);}
    State state(){try{return state(SessionJournalFiles.read(root.resolve("reroutes/0/state.bin"),8<<20));}catch(IOException failure){throw new AssertionError(failure);}}
    void honest(State state){assertFalse(state.credited());assertFalse(state.published());assertFalse(state.sourceAcknowledged());assertNotNull(state.proof());}
    @Test void originalConflictWaitsForExplicitAckThenLostConsumeColdRetryReclaimsWithoutSource()throws Exception {
        var helper=helper();try(var fixture=helper.new Fixture()){
            closeAtCredit(fixture);var pending=fixture.window.prepareAt(fixture.source.directory,fixture.writes(),List.of());var original=pending.command();var result=fixture.execute(fixture.window,pending);
            assertEquals(CommandResult.Status.CONFLICT,result.status());assertEquals(0,result.valueCount());assertArrayEquals(result.encode(),pending.savedResult().encode());assertEquals(0,fixture.opens);assertEquals(1,fixture.closedAborts.size());var selected=state();honest(selected);assertEquals(TransactionStatus.NoEffectOutcome.SOURCE_CLOSED,selected.proof().terminal().recovery().next().noEffectOutcome());assertFalse(selected.done());assertNull(fixture.source.store.operationResult(original.operationId(),original.receiptDigest()));assertFalse(Files.exists(root.resolve("reroutes/0/input.bin.atomic")));
            fixture.sourceAvailable=false;fixture.cleanup(fixture.window,fixture.coordinator);assertTrue(Files.exists(root.resolve("reroutes/0/state.bin")));assertTrue(fixture.consumes.isEmpty());
            fixture.window.acknowledge(pending);fixture.loseConsume=true;assertThrows(ExecutionException.class,()->fixture.cleanup(fixture.window,fixture.coordinator));assertEquals(1,fixture.consumes.size());var consume=fixture.consumes.getFirst();assertTrue(Files.exists(root.resolve("reroutes/0/state.bin")));honest(state());assertTrue(fixture.ackCommands.isEmpty());fixture.window.close();fixture.coordinator.close();fixture.consumeUncertain=false;
            try(var window=RatisDurableSessionClient.reopen(fixture.windowPath,fixture.data);var cleanup=new RatisOperationRerouteCoordinator(root.resolve("reroutes"),fixture.source.schema,fixture.catalogue.epochs,fixture.data,fixture.homes,fixture.scheduler,window.grant(),window.limits(),fixture.catalogue.epochs.catalogue(fixture.outer.directoryColumn),header->fixture.source.definition.sourceDirectory(),RatisJournalAccess.standalone())){
                cleanup.beforeReclamation=directory->{var done=state();honest(done);assertTrue(done.done());assertEquals(selected.proof().terminal(),done.proof().terminal());};fixture.cleanup(window,cleanup);assertFalse(Files.exists(root.resolve("reroutes/0")));assertTrue(cleanup.quiescent());assertTrue(window.pending().isEmpty());assertEquals(1,fixture.consumes.size());assertEquals(consume,fixture.consumes.getFirst());assertTrue(fixture.ackCommands.isEmpty());
            }
        }finally{helper.closePointBudget();}
    }
    @Test void lostAbortAckReopensSamePendingOriginalAndAsyncRetryNeedsNoSource()throws Exception {
        var helper=helper();try(var fixture=helper.new Fixture()){
            closeAtCredit(fixture);fixture.loseClosedAbort=true;var pending=fixture.window.prepareAt(fixture.source.directory,List.of(fixture.writes().getFirst()),List.of());byte[] original=DistributedCommandCodec.encode(pending.command());assertThrows(ExecutionException.class,()->fixture.execute(fixture.window,pending));assertNull(pending.savedResult());var selected=state();honest(selected);assertNull(selected.proof().terminal());assertNotNull(selected.abort());assertEquals(1,fixture.closedAborts.size());assertEquals(selected.abort(),fixture.closedAborts.getFirst());fixture.window.close();fixture.coordinator.close();fixture.sourceAvailable=false;
            try(var window=RatisDurableSessionClient.reopen(fixture.windowPath,fixture.data)){
                window.installReroutes(root.resolve("reroutes"),fixture.source.schema,fixture.catalogue.epochs,fixture.data,fixture.homes,fixture.scheduler,fixture.catalogue.epochs.catalogue(fixture.outer.directoryColumn),header->fixture.source.definition.sourceDirectory(),RatisJournalAccess.standalone());var recovered=window.pending().getFirst();assertArrayEquals(original,DistributedCommandCodec.encode(recovered.command()));var result=window.executeAsync(recovered,RequestContext.batch(),()->{},Runnable::run,helper.pointBudget.shared).get(15,TimeUnit.SECONDS);assertEquals(CommandResult.Status.CONFLICT,result.status());assertArrayEquals(result.encode(),recovered.savedResult().encode());honest(state());assertEquals(selected.abort().operation(),state().proof().terminal().recovery().operation());assertEquals(0,fixture.opens);assertTrue(fixture.ackCommands.isEmpty());window.acknowledge(recovered);assertFalse(Files.exists(root.resolve("reroutes/0")));assertTrue(window.quiescent());
            }
        }finally{helper.closePointBudget();}
    }
    static final class ForceLoss implements RatisJournalAccess {
        final RatisJournalAccess delegate=RatisJournalAccess.standalone();final Map<Path,Reservation> promises=new HashMap<>();final boolean after;boolean failed;
        ForceLoss(boolean after){this.after=after;}
        public RootLease backendRoot(Path path)throws IOException{return delegate.backendRoot(path);}public void register(Path path,int maximum)throws IOException{delegate.register(path,maximum);}public byte[] read(Path path,int maximum)throws IOException{return delegate.read(path,maximum);}
        public void write(Path path,byte[] body,int maximum)throws IOException{boolean terminal=path.getFileName().toString().equals("state.bin") && state(body).proof()!=null && state(body).proof().terminal()!=null;var promise=promises.get(path);if(path.getFileName().toString().equals("state.bin")){assertNotNull(promise,"Future completion metadata must be admitted before force");assertTrue(promise.peakBytes()>=2L*(body.length+32),"Main+pending completion bytes exceed original admission");}if(terminal && !failed && !after){failed=true;throw new IOException("Lost terminal journal force before write");}delegate.write(path,body,maximum);if(terminal && !failed){failed=true;throw new IOException("Lost terminal journal force ACK after durable write");}}
        public void remove(Path path,byte[] body,int maximum,SessionJournalFiles.Removal removal)throws IOException{delegate.remove(path,body,maximum,removal);}public void deleted(Path path)throws IOException{delegate.deleted(path);}public void reclaimedTree(Path path)throws IOException{delegate.reclaimedTree(path);}public void reserve(Reservation reservation)throws IOException{promises.put(reservation.path(),reservation);delegate.reserve(reservation);}public void fulfilled(Reservation reservation)throws IOException{delegate.fulfilled(reservation);}
    }
    @ParameterizedTest @ValueSource(booleans={false,true}) void terminalJournalUnknownForceKeepsExactDebtUntilColdRetryAndExplicitAck(boolean after)throws Exception {
        var helper=helper();var io=new ForceLoss(after);try(var fixture=helper.new Fixture(false,false,io)){
            closeAtCredit(fixture);var pending=fixture.window.prepareAt(fixture.source.directory,fixture.writes(),List.of());assertThrows(ExecutionException.class,()->fixture.execute(fixture.window,pending));assertTrue(io.failed);assertNull(pending.savedResult());var selected=state();honest(selected);assertNotNull(selected.abort());assertEquals(1,fixture.closedAborts.size());assertEquals(0,fixture.opens);assertTrue(Files.exists(root.resolve("reroutes/0/input.bin")));fixture.window.close();fixture.coordinator.close();fixture.sourceAvailable=false;
            try(var window=RatisDurableSessionClient.reopen(fixture.windowPath,fixture.data);var cleanup=new RatisOperationRerouteCoordinator(root.resolve("reroutes"),fixture.source.schema,fixture.catalogue.epochs,fixture.data,fixture.homes,fixture.scheduler,window.grant(),window.limits(),fixture.catalogue.epochs.catalogue(fixture.outer.directoryColumn),header->fixture.source.definition.sourceDirectory(),io)){
                var original=window.pending().getFirst();var result=fixture.execute(window,original,cleanup);assertEquals(CommandResult.Status.CONFLICT,result.status());honest(state());assertEquals(selected.abort().operation(),state().proof().terminal().recovery().operation());assertEquals(1,fixture.closedAborts.size());window.acknowledge(original);fixture.cleanup(window,cleanup);assertFalse(Files.exists(root.resolve("reroutes/0")));assertTrue(cleanup.quiescent());assertTrue(fixture.ackCommands.isEmpty());
            }
        }finally{helper.closePointBudget();}
    }
}
