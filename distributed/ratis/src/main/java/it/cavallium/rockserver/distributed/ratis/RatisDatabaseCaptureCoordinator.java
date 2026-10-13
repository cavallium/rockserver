package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.security.*;
import java.util.*;

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
    private boolean active;
    public RatisDatabaseCaptureCoordinator(Log journal,RatisDatabaseBackupDriver.Authority authority,RatisDatabaseBackupDriver.CohortTransport transport,Map<UUID,CaptureBinding> bindings)throws IOException {
        this.journal=Objects.requireNonNull(journal);this.authority=Objects.requireNonNull(authority);this.transport=Objects.requireNonNull(transport);this.bindings=Map.copyOf(bindings);
        var expected=new HashSet<UUID>();for(var domain:journal.definition().domains())for(var fence:domain.fences()){
            var binding=this.bindings.get(fence.fenceId());if(!expected.add(fence.fenceId()) || binding==null)throw new IOException("Capture subfence coverage differs from complete roster");var saved=binding.journal().snapshot();
            if(!fence.equals(saved.initial().fence()) || saved.captureMode()!=RatisSnapshotCaptureRequest.Mode.READ_ONLY || !journal.policy().retention().equals(saved.requestedTtl()))throw new IOException("Capture binding changed original full-column fence/mode/TTL");
        }
        if(!expected.equals(this.bindings.keySet()))throw new IOException("Capture binding coverage differs from complete roster");
        if(journal.snapshot().phase()==RatisDatabaseBackupJournal.Phase.GATES){fresh();transport.requireSupported(journal.definition(),journal.policy());}
    }
    public Progress step(RequestContext context)throws IOException {
        Objects.requireNonNull(context);synchronized(this){if(active)throw new IOException("Database capture work active");active=true;}
        try{var saved=journal.snapshot();if(saved.phase()==RatisDatabaseBackupJournal.Phase.LOST)throw new RatisSnapshotLostException("Original database capture is lost");if(!switch(saved.phase()){case GATES,REQUESTS,CAPTURED,REPORTED,RELEASED -> true;default -> false;})throw new IOException("Database capture barrier is no longer active");if(saved.phase()!=RatisDatabaseBackupJournal.Phase.GATES && saved.phase()!=RatisDatabaseBackupJournal.Phase.RELEASED)checkDeadline(saved);
            switch(saved.phase()){
                case GATES -> gates();
                case REQUESTS -> capture();
                case CAPTURED -> report();
                case REPORTED -> releaseGates();
                case RELEASED -> {}
                default -> throw new IOException("Database capture barrier is no longer active");
            }
            var phase=journal.snapshot().phase();return new Progress(phase,phase==RatisDatabaseBackupJournal.Phase.RELEASED);
        }catch(RatisSnapshotLostException lost){var phase=journal.snapshot().phase();if(phase!=RatisDatabaseBackupJournal.Phase.CANCELLED && phase!=RatisDatabaseBackupJournal.Phase.LOST)journal.phase(RatisDatabaseBackupJournal.Phase.LOST);throw lost;}
        finally{synchronized(this){active=false;notifyAll();}}
    }
    private void checkDeadline(RatisDatabaseBackupJournal.Snapshot saved)throws IOException{
        long now=System.currentTimeMillis(),started=saved.dispatchStartedMillis();if(started<=0 || now<started || now-started>journal.policy().maximumMaterialization().toMillis())throw new RatisSnapshotLostException("Capture exceeded original dispatch deadline; reopen never renews retention");
    }
    private void fresh()throws IOException{var current=authority.read();journal.definition().requireVector(current.roster(),current.domains());}
    private List<CaptureBinding> ordered(){return bindings.entrySet().stream().sorted(Map.Entry.comparingByKey(RangeDirectoryCodec.UUID_ORDER)).map(Map.Entry::getValue).toList();}
    private void gates()throws IOException{
        for(var b:ordered()){
            var step=b.coordinator().step(RatisSnapshotCoordinator.StopAt.GATES_READY);
            if(step.action()!=RatisSnapshotCoordinator.Action.GATES_READY){if(step.action()==RatisSnapshotCoordinator.Action.ABORTED || step.complete())throw new RatisSnapshotLostException("Backup gate acquisition lost");return;}

        }
        fresh();var prepared=new HashMap<UUID,RatisRoutedClient.PreparedSnapshotCapture>();
        for(var b:ordered())for(var p:b.journal().snapshot().participants()){
            if(p.captureLost() || p.captureReleased() || p.captureReleaseLost())throw new RatisSnapshotLostException("Original backup capture unavailable");
            var request=p.capture();if(request==null){var target=b.routed().snapshotTarget(b.journal().snapshot().initial().fence(),p.route().groupId());request=b.routed().prepareSnapshotCapture(target,UUID.randomUUID(),journal.policy().retention());b.journal().freezeCapture(request);}
            prepared.put(p.route().groupId(),request);
        }
        var requests=transport.qualify(journal.definition(),Map.copyOf(prepared),journal.policy());verifyRequests(requests,prepared);journal.freezeRequests(requests);
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
    private void capture()throws IOException{
        var prepared=frozen();var results=new HashMap<UUID,RatisSnapshotCaptureRequest.Result>();
        // Each exact backend retry recovers its single original physical cut, including lost replies.
        for(var request:journal.snapshot().requests()){
            var members=new HashMap<UUID,RatisRoutedClient.PreparedSnapshotCapture>();request.members().forEach(m->members.put(m.groupId(),prepared.get(m.groupId())));
            var captured=transport.capture(request,Map.copyOf(members));var expected=request.members().stream().map(RatisDatabaseBackupJournal.Member::groupId).collect(java.util.stream.Collectors.toSet());
            if(!captured.keySet().equals(expected))throw new IOException("Cohort result coverage changed");for(var entry:captured.entrySet()){
                var original=prepared.get(entry.getKey()).request();var result=entry.getValue();if(result.handle()!=null || !result.token().ownerIncarnation().equals(original.owner()) || !result.token().snapshotId().equals(original.snapshotId()))throw new IOException("Cohort result changed original read-only request");results.put(entry.getKey(),result);
            }
        }
        var tokens=new HashMap<UUID,SnapshotFenceStatus.Capture>();results.forEach((id,result)->tokens.put(id,result.token()));for(var request:journal.snapshot().requests())transport.requireLive(request,Map.copyOf(tokens));
        fresh();var statuses=new ArrayList<SnapshotFenceStatus>();for(var b:ordered()){
            var home=b.journal().snapshot().observedHome();if(home==null || home.terminal() || !home.allReady())throw new IOException("HOME changed before global vector force");
            for(var p:home.progress())home=home.capture(p.groupId(),tokens.get(p.groupId()));statuses.add(home);
        }
        // The only authority to start reporting: one forced COMPLETE token/cohort vector, still INSTALLING HOME images.
        journal.captured(statuses);
    }
    private void report()throws IOException{
        var saved=journal.snapshot();var prepared=frozen();var tokens=new HashMap<UUID,SnapshotFenceStatus.Capture>();saved.captured().forEach(s->s.progress().forEach(p->tokens.put(p.groupId(),p.capture())));
        for(var request:saved.requests())transport.requireLive(request,Map.copyOf(tokens));
        for(var status:saved.captured()){
            var b=bindings.get(status.fence().fenceId());for(var p:status.progress())b.journal().acceptExternalCapture(p.groupId(),prepared.get(p.groupId()),new RatisSnapshotCaptureRequest.Result(p.capture(),null));
            var step=b.coordinator().reportExternalCaptures(status);if(step.participant()!=null)return;
        }
        journal.phase(RatisDatabaseBackupJournal.Phase.REPORTED);
    }
    private void releaseGates()throws IOException{
        for(var b:ordered()){var step=b.coordinator().step(RatisSnapshotCoordinator.StopAt.RELEASED);if(!step.complete())return;}
        journal.phase(RatisDatabaseBackupJournal.Phase.RELEASED);
    }
    private static byte[] sha(byte[] bytes){try{return MessageDigest.getInstance("SHA-256").digest(bytes);}catch(NoSuchAlgorithmException impossible){throw new AssertionError(impossible);}}
}
