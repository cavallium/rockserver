package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.io.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;

/** Direct durable subjournal creation: ordinary SnapshotClient acquisition never participates in a global backup. */
public final class RatisDatabaseBackupFactory implements AutoCloseable {
    public record Limits(int jobs,int subjournals,long metadataBytes){public Limits{if(jobs<1 || jobs>8 || subjournals<1 || subjournals>1024 || metadataBytes<1 || metadataBytes>4L*1024*1024*1024)throw new IllegalArgumentException("Invalid global backup factory budgets");}}
    public record Domain(RatisSnapshotFences homes,RatisRoutedClient routed,RWScheduler scheduler){public Domain{Objects.requireNonNull(homes);Objects.requireNonNull(routed);Objects.requireNonNull(scheduler);}}
    /** The caller owns this exclusive sequence reservation in an actually issued grant. Servers still validate admission. */
    public record Reservation(SessionEpochLedger.Grant issued,RatisSnapshotClient.Identity identity){public Reservation{Objects.requireNonNull(issued);Objects.requireNonNull(identity);if(!issued.sessionId().equals(identity.business().sessionId()) || issued.epoch()!=identity.business().epoch())throw new IllegalArgumentException("Backup reservation differs from issued grant");}}
    private record Planned(DatabaseBackupDefinition.Domain definition,Domain access,SnapshotFence fence,RatisSnapshotPlan plan,Reservation reservation,long reserve){}
    private final Path root;private final Limits limits;private final FileChannel ownership;private final FileLock lock;
    private final Map<UUID,RatisDatabaseBackupOperation> jobs=new HashMap<>();private long metadata;private boolean closed;
    public RatisDatabaseBackupFactory(Path root,Limits limits)throws IOException{
        this.root=root.toAbsolutePath().normalize();this.limits=Objects.requireNonNull(limits);Files.createDirectories(this.root);if(!this.root.toRealPath().equals(this.root))throw new IOException("Backup factory root is foreign or indirect");
        ownership=FileChannel.open(this.root.resolve("factory.lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS);try{lock=ownership.tryLock();if(lock==null)throw new IOException("Backup factory already owned");}catch(IOException|RuntimeException failure){ownership.close();throw failure;}
    }
    public RatisDatabaseBackupOperation create(DatabaseBackupDefinition definition,Map<UUID,Domain> access,Map<UUID,Reservation> reservations,
            DatabaseBackupManifest.Destination destination,RatisDatabaseBackupDriver.Policy policy,RatisExportJournal.Limits exports,
            RatisDatabaseBackupDriver.Authority authority,RatisDatabaseBackupDriver.Operators operators)throws IOException{return open(false,definition,access,reservations,destination,policy,exports,authority,operators);}
    public RatisDatabaseBackupOperation reopen(DatabaseBackupDefinition definition,Map<UUID,Domain> access,Map<UUID,Reservation> reservations,
            DatabaseBackupManifest.Destination destination,RatisDatabaseBackupDriver.Policy policy,RatisExportJournal.Limits exports,
            RatisDatabaseBackupDriver.Authority authority,RatisDatabaseBackupDriver.Operators operators)throws IOException{return open(true,definition,access,reservations,destination,policy,exports,authority,operators);}
    private synchronized RatisDatabaseBackupOperation open(boolean reopen,DatabaseBackupDefinition definition,Map<UUID,Domain> access,Map<UUID,Reservation> reservations,
            DatabaseBackupManifest.Destination destination,RatisDatabaseBackupDriver.Policy policy,RatisExportJournal.Limits exports,
            RatisDatabaseBackupDriver.Authority authority,RatisDatabaseBackupDriver.Operators operators)throws IOException{
        if(closed || !lock.isValid() || jobs.containsKey(definition.backupId()) || jobs.size()>=limits.jobs())throw new IOException("Backup factory closed or job capacity exhausted");
        var planned=preflight(definition,access,reservations,exports);long charge=4L*definition.limits().maximumMetadataBytes();
        for(var p:planned)charge=Math.addExact(charge,Math.addExact(4L*p.reserve(),4L*exports.maximumJournalBytes()));if(charge>limits.metadataBytes()-metadata)throw new IOException("Global future snapshot metadata capacity exhausted before gates");
        Path job=root.resolve(definition.backupId().toString());if(reopen ? !Files.isDirectory(job,LinkOption.NOFOLLOW_LINKS) : Files.exists(job,LinkOption.NOFOLLOW_LINKS))throw new IOException("Backup owned layout differs; use exact create/reopen");
        if(!reopen){Files.createDirectory(job);SessionJournalFiles.force(root);}
        final long charged=charge;metadata+=charge;var owners=new ArrayList<AutoCloseable>();
        try{
            RatisDatabaseBackupJournal global=reopen?RatisDatabaseBackupJournal.reopen(job.resolve("global"),definition,destination,policy):RatisDatabaseBackupJournal.create(job.resolve("global"),definition,destination,policy);owners.add(global);
            var archive=new RatisDatabaseBackupArchive(job.resolve("archive"),definition,destination);owners.add(archive);
            Path initialized=job.resolve("initialized");if(Files.exists(job.resolve("initialized.pending"),LinkOption.NOFOLLOW_LINKS))throw new IOException("Uncertain backup initialization marker");boolean initializedBefore=Files.exists(initialized,LinkOption.NOFOLLOW_LINKS);if(initializedBefore){if(!Files.isRegularFile(initialized,LinkOption.NOFOLLOW_LINKS) || !Arrays.equals(SessionJournalFiles.read(initialized,64),definition.digest()))throw new IOException("Backup initialization identity differs");}
            var bindings=new HashMap<UUID,RatisDatabaseBackupDriver.Binding>();var groups=new HashMap<UUID,RatisRoutedClient>();var restored=new HashMap<UUID,RatisRoutedClient.PreparedSnapshotCapture>();var clients=new HashMap<UUID,RatisSnapshotClient>();
            for(var p:planned){
                var d=p.definition();var a=p.access();var client=clients.get(d.directory().domainId());if(client==null){client=new RatisSnapshotClient(RatisTransactionBindings.of(d.schema(),a.routed().directory(d.schema()),a.routed()),a.homes(),a.scheduler(),RatisSnapshotClient.Limits.defaults());clients.put(d.directory().domainId(),client);owners.add(client);}
                Path source=job.resolve(p.fence().fenceId()+".snapshot");var initial=SnapshotFenceStatus.begin(p.fence());var first=p.reservation().identity().firstControl();
                boolean existing=Files.exists(source,LinkOption.NOFOLLOW_LINKS);if(existing && !Files.isRegularFile(source,LinkOption.NOFOLLOW_LINKS))throw new IOException("Original backup subjournal is foreign");if(reopen && !existing && (initializedBefore || global.snapshot().phase()!=RatisDatabaseBackupJournal.Phase.GATES))throw new IOException("Original initialized snapshot journal lost; new capture prohibited");
                RatisDurableSnapshot durable=existing?RatisDurableSnapshot.reopen(source,a.homes(),RatisTransactionBindings.of(a.routed())):RatisDurableSnapshot.create(source,a.homes(),RatisTransactionBindings.of(a.routed()),initial,a.homes().prepare(new OperationId(first.sessionId(),first.epoch(),first.sequence()+128),null,initial),p.plan().routes(),policy.retention(),RatisSnapshotCaptureRequest.Mode.READ_ONLY);owners.add(durable);
                var saved=durable.snapshot();if(!saved.initial().equals(initial) || saved.captureMode()!=RatisSnapshotCaptureRequest.Mode.READ_ONLY || !policy.retention().equals(saved.requestedTtl()))throw new IOException("Original backup subjournal changed fence/mode/lifetime");boundedControl(first,saved.lastHome());if(!initializedBefore && reopen && saved.participants().stream().anyMatch(p2->p2.installFrozen() || p2.capture()!=null || p2.nativeGate()!=null))throw new IOException("Uninitialized backup has unexpected native work");
                for(var progress:saved.participants()){groups.put(progress.route().groupId(),a.routed());if(progress.capture()!=null)restored.put(progress.route().groupId(),progress.capture());}
                var coordinator=new RatisSnapshotCoordinator(durable,a.homes(),RatisTransactionBindings.of(a.routed()),last->{boundedControl(first,last);if(last.sequence()==first.sequence()+RatisSnapshotClient.CONTROL_SLOTS-1)throw new IOException("Backup HOME reservation exhausted");return new OperationId(last.sessionId(),last.epoch(),last.sequence()+1);},UUID::randomUUID,policy.retention());owners.add(coordinator);
                UUID exportId=UUID.nameUUIDFromBytes((definition.backupId()+"/export/"+p.fence().fenceId()).getBytes(java.nio.charset.StandardCharsets.UTF_8));
                var export=new RatisExportJournal.Definition(exportId,p.fence(),d.directory().columns().stream().sorted(RangeDirectoryCodec.UUID_ORDER).toList(),source,client.identityDigest(),exports,policy.retention());
                bindings.put(p.fence().fenceId(),new RatisDatabaseBackupDriver.Binding(durable,coordinator,a.routed(),job.resolve(p.fence().fenceId()+".export"),export,new RatisRemoteExportSource(client,a.routed(),RatisJournalAccess.standalone())));
            }
            if(!initializedBefore)SessionJournalFiles.write(initialized,definition.digest(),64);else{try(var file=FileChannel.open(initialized,StandardOpenOption.READ,LinkOption.NOFOLLOW_LINKS)){file.force(true);}SessionJournalFiles.force(job);}
            var backend=new RatisSnapshotCohortClient(groups,restored::get);var driver=new RatisDatabaseBackupDriver(global,archive,authority,backend,operators,bindings);
            var operation=new RatisDatabaseBackupOperation(driver,global,owners,()->refund(definition.backupId(),charged));jobs.put(definition.backupId(),operation);return operation;
        }catch(Throwable failure){for(int i=owners.size()-1;i>=0;i--)try{owners.get(i).close();}catch(Exception cleanup){failure.addSuppressed(cleanup);}metadata-=charge;if(failure instanceof IOException io)throw io;throw new IOException("Backup factory initialization failed; original owned layout retained",failure);}
    }
    private List<Planned> preflight(DatabaseBackupDefinition definition,Map<UUID,Domain> access,Map<UUID,Reservation> reservations,RatisExportJournal.Limits exports)throws IOException{
        if(!access.keySet().equals(new HashSet<>(definition.roster().domainIds())))throw new IOException("Backup domain access coverage differs");var planned=new ArrayList<Planned>();var expected=new HashSet<UUID>();var spans=new HashMap<String,List<long[]>>();
        for(var d:definition.domains()){
            var a=access.get(d.directory().domainId());a.homes().requireDatabaseBinding(d.directory().clusterId(),d.directory().databaseId());a.routed().requireKnownSchema(d.schema());
            if(!Arrays.equals(a.routed().directory(d.schema()).payloadDigest(),d.directory().payloadDigest()))throw new IOException("Backup cached directory differs from frozen vector");
            for(var f:d.fences()){
                expected.add(f.fenceId());var reservation=reservations.get(f.fenceId());if(reservation==null || !reservation.identity().fence().equals(f.fenceId()) || !reservation.identity().business().equals(f.operation()))throw new IOException("Backup caller reservation differs from original fence");
                var first=reservation.identity().firstControl();int i=0;for(var participant:f.participants())if(!participant.installOperation().equals(new OperationId(first.sessionId(),first.epoch(),first.sequence()+i)) || !participant.releaseOperation().equals(new OperationId(first.sessionId(),first.epoch(),first.sequence()+64+i++)))throw new IOException("Backup participant controls differ from exclusive reservation");
                var ranges=spans.computeIfAbsent(first.sessionId()+"/"+first.epoch(),key->new ArrayList<>());for(var current:new long[][]{{f.operation().sequence(),f.operation().sequence()},{first.sequence(),first.sequence()+RatisSnapshotClient.CONTROL_SLOTS-1}}){for(var prior:ranges)if(current[0]<=prior[1] && current[1]>=prior[0])throw new IOException("Backup caller control reservations overlap");ranges.add(current);}
                var plan=RatisSnapshotPlan.frozen(List.of(d.directory()),f);var home=a.homes().route(f.fenceId());if(!home.groupId().equals(f.homeGroup()) || home.rangeEpoch()!=f.homeEpoch())throw new IOException("Backup HOME placement changed");
                long routeBytes=futureRoute(home);for(var route:plan.routes())routeBytes=Math.addExact(routeBytes,futureRoute(route));
                long reserve=f.encode().length+routeBytes+65536L+plan.routes().size()*(1025L+ScopedTransactionRpc.HANDLE_BYTES)+32;
                if(routeBytes>16L*1024*1024 || reserve>RatisDurableSnapshot.MAX_BYTES || exports.maximumChunks()<plan.routes().size()*d.directory().columns().size())throw new IOException("Backup future durable/export envelope exceeds bounds before gates");
                planned.add(new Planned(d,a,f,plan,reservation,reserve));if(planned.size()>limits.subjournals())throw new IOException("Backup subjournal count exceeds declared capacity");
            }
        }
        if(!expected.equals(reservations.keySet()))throw new IOException("Backup reservation coverage differs");return List.copyOf(planned);
    }
    private static long futureRoute(RangeDescriptor route)throws IOException{return SessionWindowJournal.routeSize(route)+60L+(route.lowerInclusive()==null?0:route.lowerInclusive().length)+(route.upperExclusive()==null?0:route.upperExclusive().length)+RangeDirectoryCodec.MAX_REPLICAS*(57L+128+256);}
    private static void boundedControl(OperationId first,OperationId last)throws IOException{if(!first.sessionId().equals(last.sessionId()) || first.epoch()!=last.epoch() || last.sequence()<first.sequence()+128 || last.sequence()>first.sequence()+RatisSnapshotClient.CONTROL_SLOTS-1)throw new IOException("Backup HOME control is outside caller-issued reservation");}
    private synchronized void refund(UUID job,long bytes){jobs.remove(job);metadata-=bytes;}
    @Override public synchronized void close()throws IOException{if(closed)return;if(!jobs.isEmpty())throw new IOException("Backup operations still own actual cleanup/journals");closed=true;try{lock.close();}finally{ownership.close();}}
}
