package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.security.*;
import java.util.*;
import java.util.concurrent.*;

/** Global read-only capture barrier. RELEASED leaves the original native captures owned by its caller. */
public final class RatisDatabaseCaptureCoordinator {
    public interface Log {
        DatabaseBackupDefinition definition();
        RatisDatabaseBackupDriver.Policy policy();
        RatisDatabaseBackupJournal.Snapshot snapshot()throws IOException;
        void freezeRequests(List<RatisDatabaseBackupJournal.CohortRequest> requests)throws IOException;
        void captured(List<SnapshotFenceStatus> statuses)throws IOException;
        void phase(RatisDatabaseBackupJournal.Phase phase)throws IOException;
    }
    public record CaptureBinding(RatisDurableSnapshot journal,RatisSnapshotCoordinator coordinator,RatisRoutedClient routed) {
        public CaptureBinding{Objects.requireNonNull(journal);Objects.requireNonNull(coordinator);Objects.requireNonNull(routed);}
    }
    public record Progress(RatisDatabaseBackupJournal.Phase phase,boolean released) {}
    private final Log journal;
    private final RatisDatabaseBackupDriver.Authority authority;
    private final RatisDatabaseBackupDriver.CohortTransport transport;
    private final Map<UUID,CaptureBinding> bindings;
    private boolean active,supported;
    public RatisDatabaseCaptureCoordinator(Log journal,RatisDatabaseBackupDriver.Authority authority,RatisDatabaseBackupDriver.CohortTransport transport,Map<UUID,CaptureBinding> bindings)throws IOException {
        this.journal=Objects.requireNonNull(journal);this.authority=Objects.requireNonNull(authority);this.transport=Objects.requireNonNull(transport);this.bindings=Map.copyOf(bindings);
        var expected=new HashSet<UUID>();for(var domain:journal.definition().domains())for(var fence:domain.fences()){
            var binding=this.bindings.get(fence.fenceId());if(!expected.add(fence.fenceId()) || binding==null)throw new IOException("Capture subfence coverage differs from complete roster");var saved=binding.journal().snapshot();
            if(!fence.equals(saved.initial().fence()) || saved.captureMode()!=RatisSnapshotCaptureRequest.Mode.READ_ONLY || !journal.policy().retention().equals(saved.requestedTtl()))throw new IOException("Capture binding changed original full-column fence/mode/TTL");
        }
        if(!expected.equals(this.bindings.keySet()))throw new IOException("Capture binding coverage differs from complete roster");
    }
    /** Caller-thread convenience; workload dispatchers must compose stepAsync instead of waiting. */
    public Progress step(RequestContext context)throws IOException{return await(stepAsync(context));}
    public CompletableFuture<Progress> stepAsync(RequestContext context){
        Objects.requireNonNull(context);synchronized(this){if(active)return CompletableFuture.failedFuture(new IOException("Database capture work active"));active=true;}
        CompletableFuture<Void> work;try{var saved=journal.snapshot();if(saved.phase()==RatisDatabaseBackupJournal.Phase.LOST)throw new RatisSnapshotLostException("Original database capture is durably lost; recapture prohibited");if(saved.phase()!=RatisDatabaseBackupJournal.Phase.GATES&&saved.phase()!=RatisDatabaseBackupJournal.Phase.REQUESTS&&saved.phase()!=RatisDatabaseBackupJournal.Phase.CAPTURED&&saved.phase()!=RatisDatabaseBackupJournal.Phase.REPORTED&&saved.phase()!=RatisDatabaseBackupJournal.Phase.RELEASED)throw new IOException("Database capture phase belongs to another owner");if(saved.phase()!=RatisDatabaseBackupJournal.Phase.GATES&&saved.phase()!=RatisDatabaseBackupJournal.Phase.RELEASED)checkDeadline(saved);switch(saved.phase()){
            case GATES->work=gatesAsync();
            case REQUESTS->work=captureAsync();
            case CAPTURED->work=reportAsync();
            case REPORTED->{releaseGates();work=CompletableFuture.completedFuture(null);}
            case RELEASED->work=CompletableFuture.completedFuture(null);
            default->throw new IOException("Database capture phase belongs to materializer or cleanup");
        }}catch(Throwable failed){work=CompletableFuture.failedFuture(failed);}
        var result=new CompletableFuture<Progress>();work.whenComplete((ignored,failure)->{Throwable problem=unwrap(failure);Progress progress=null;try{if(problem instanceof RatisSnapshotLostException){var phase=journal.snapshot().phase();if(phase!=RatisDatabaseBackupJournal.Phase.CANCELLED&&phase!=RatisDatabaseBackupJournal.Phase.LOST)journal.phase(RatisDatabaseBackupJournal.Phase.LOST);}if(problem==null){var phase=journal.snapshot().phase();progress=new Progress(phase,phase==RatisDatabaseBackupJournal.Phase.RELEASED);}}catch(Throwable save){if(problem!=null)problem.addSuppressed(save);else problem=save;}synchronized(this){active=false;notifyAll();}if(problem==null)result.complete(progress);else result.completeExceptionally(problem);});return result;
    }
    private void checkDeadline(RatisDatabaseBackupJournal.Snapshot saved)throws IOException{
        long now=System.currentTimeMillis(),started=saved.dispatchStartedMillis();if(started<=0 || now<started || now-started>journal.policy().maximumMaterialization().toMillis())throw new RatisSnapshotLostException("Capture exceeded original dispatch deadline; reopen never renews retention");
    }
    private void fresh()throws IOException{var current=authority.read();journal.definition().requireVector(current.roster(),current.domains());}
    private List<CaptureBinding> ordered(){return bindings.entrySet().stream().sorted(Map.Entry.comparingByKey(RangeDirectoryCodec.UUID_ORDER)).map(Map.Entry::getValue).toList();}
    private CompletableFuture<Void> gatesAsync()throws IOException{
        if(!supported){fresh();return transport.requireSupported(journal.definition(),journal.policy()).thenCompose(ignored->{supported=true;try{return gatesAsync();}catch(Throwable failed){return CompletableFuture.failedFuture(failed);}});}
        for(var binding:ordered()){var step=binding.coordinator().step(RatisSnapshotCoordinator.StopAt.GATES_READY);if(step.action()!=RatisSnapshotCoordinator.Action.GATES_READY){if(step.action()==RatisSnapshotCoordinator.Action.ABORTED||step.complete())throw new RatisSnapshotLostException("Database gate acquisition lost");return CompletableFuture.completedFuture(null);}}
        fresh();var prepared=new HashMap<UUID,RatisRoutedClient.PreparedSnapshotCapture>();CompletableFuture<Void> staged=CompletableFuture.completedFuture(null);
        for(var binding:ordered())for(var participant:binding.journal().snapshot().participants()){
            if(participant.captureLost()||participant.captureReleased()||participant.captureReleaseLost())throw new RatisSnapshotLostException("Original database capture unavailable");var known=participant.capture();if(known!=null){prepared.put(participant.route().groupId(),known);continue;}
            var fence=binding.journal().snapshot().initial().fence();staged=staged.thenCompose(ignored->binding.routed().snapshotTargetAsync(fence,participant.route().groupId()).thenApply(target->{try{var request=binding.routed().prepareSnapshotCapture(target,UUID.randomUUID(),journal.policy().retention());binding.journal().freezeCapture(request);prepared.put(participant.route().groupId(),request);return null;}catch(IOException failed){throw new CompletionException(failed);}}));
        }
        return staged.thenCompose(ignored->{try{return transport.qualify(journal.definition(),Map.copyOf(prepared),journal.policy());}catch(IOException failed){return CompletableFuture.failedFuture(failed);}}).thenApply(requests->{try{verifyRequests(requests,prepared);journal.freezeRequests(requests);return null;}catch(IOException failed){throw new CompletionException(failed);}});
    }
    private Map<UUID,RatisRoutedClient.PreparedSnapshotCapture> frozen()throws IOException{
        var map=new HashMap<UUID,RatisRoutedClient.PreparedSnapshotCapture>();for(var b:ordered())for(var p:b.journal().snapshot().participants()){
            if(p.capture()==null || p.captureLost() || p.captureReleased() || p.captureReleaseLost())throw new RatisSnapshotLostException("Frozen backup request lost; recapture prohibited");map.put(p.route().groupId(),p.capture());}
        verifyRequests(journal.snapshot().requests(),map);return Map.copyOf(map);
    }
    private static void verifyRequests(List<RatisDatabaseBackupJournal.CohortRequest> requests,Map<UUID,RatisRoutedClient.PreparedSnapshotCapture> prepared)throws IOException{
        var groups=new HashSet<UUID>();for(var c:requests)for(var m:c.members()){
            var p=prepared.get(m.groupId());if(p==null || !groups.add(m.groupId()) || !m.fenceId().equals(p.request().fence().fenceId()) || !m.snapshotId().equals(p.request().snapshotId())
                    || !m.owner().equals(p.request().owner()) || !m.replica().equals(p.target().replica().replicaId()) || m.ttlMillis()!=p.request().ttl().toMillis() || !MessageDigest.isEqual(m.preparedDigest(),sha(p.bytes())))throw new IOException("Cohort changed exact frozen request/owner");
        }if(!groups.equals(prepared.keySet()))throw new IOException("Cohort does not cover all frozen requests");
    }
    private CompletableFuture<Void> captureAsync()throws IOException{
        var prepared=frozen();var results=new HashMap<UUID,RatisSnapshotCaptureRequest.Result>();CompletableFuture<Void> captured=CompletableFuture.completedFuture(null);
        for(var request:journal.snapshot().requests()){var members=new HashMap<UUID,RatisRoutedClient.PreparedSnapshotCapture>();request.members().forEach(member->members.put(member.groupId(),prepared.get(member.groupId())));captured=captured.thenCompose(ignored->{try{return transport.capture(request,Map.copyOf(members)).thenApply(values->{var expected=request.members().stream().map(RatisDatabaseBackupJournal.Member::groupId).collect(java.util.stream.Collectors.toSet());if(!values.keySet().equals(expected))throw new CompletionException(new IOException("Cohort result coverage changed"));for(var entry:values.entrySet()){var original=prepared.get(entry.getKey()).request();var value=entry.getValue();if(value.handle()!=null||!value.token().ownerIncarnation().equals(original.owner())||!value.token().snapshotId().equals(original.snapshotId()))throw new CompletionException(new IOException("Cohort changed original read-only request"));results.put(entry.getKey(),value);}return null;});}catch(IOException failed){return CompletableFuture.failedFuture(failed);}});}
        return captured.thenCompose(ignored->{var tokens=new HashMap<UUID,SnapshotFenceStatus.Capture>();results.forEach((id,result)->tokens.put(id,result.token()));try{return live(journal.snapshot().requests(),tokens).thenApply(value->{try{fresh();var statuses=new ArrayList<SnapshotFenceStatus>();for(var binding:ordered()){var home=binding.journal().snapshot().observedHome();if(home==null||home.terminal()||!home.allReady())throw new IOException("HOME changed before complete vector force");for(var progress:home.progress())home=home.capture(progress.groupId(),tokens.get(progress.groupId()));statuses.add(home);}journal.captured(statuses);return null;}catch(IOException failed){throw new CompletionException(failed);}});}catch(IOException failed){return CompletableFuture.failedFuture(failed);}});
    }
    private CompletableFuture<Void> live(List<RatisDatabaseBackupJournal.CohortRequest> requests,Map<UUID,SnapshotFenceStatus.Capture> tokens){CompletableFuture<Void> work=CompletableFuture.completedFuture(null);for(var request:requests)work=work.thenCompose(ignored->{try{return transport.requireLive(request,Map.copyOf(tokens));}catch(IOException failed){return CompletableFuture.failedFuture(failed);}});return work;}
    private CompletableFuture<Void> reportAsync()throws IOException{
        var saved=journal.snapshot();var prepared=frozen();var tokens=new HashMap<UUID,SnapshotFenceStatus.Capture>();saved.captured().forEach(status->status.progress().forEach(progress->tokens.put(progress.groupId(),progress.capture())));
        return live(saved.requests(),tokens).thenApply(ignored->{try{for(var status:saved.captured()){var binding=bindings.get(status.fence().fenceId());for(var progress:status.progress())binding.journal().acceptExternalCapture(progress.groupId(),prepared.get(progress.groupId()),new RatisSnapshotCaptureRequest.Result(progress.capture(),null));var step=binding.coordinator().reportExternalCaptures(status);if(step.participant()!=null)return null;}journal.phase(RatisDatabaseBackupJournal.Phase.REPORTED);return null;}catch(IOException failed){throw new CompletionException(failed);}});
    }
    private void releaseGates()throws IOException{
        for(var b:ordered()){var step=b.coordinator().step(RatisSnapshotCoordinator.StopAt.RELEASED);if(!step.complete())return;}
        journal.phase(RatisDatabaseBackupJournal.Phase.RELEASED);
    }
    private static byte[] sha(byte[] bytes){try{return MessageDigest.getInstance("SHA-256").digest(bytes);}catch(NoSuchAlgorithmException impossible){throw new AssertionError(impossible);}}
    private static Throwable unwrap(Throwable failure){while(failure instanceof CompletionException&&failure.getCause()!=null)failure=failure.getCause();return failure;}
    private static <T>T await(CompletableFuture<T> actual)throws IOException{boolean interrupted=false;try{for(;;)try{return actual.get();}catch(InterruptedException retry){interrupted=true;}catch(ExecutionException failed){var cause=unwrap(failed.getCause());if(cause instanceof IOException io)throw io;if(cause instanceof RuntimeException runtime)throw runtime;throw new IOException("Database capture work failed",cause);}}finally{if(interrupted)Thread.currentThread().interrupt();}}

}
