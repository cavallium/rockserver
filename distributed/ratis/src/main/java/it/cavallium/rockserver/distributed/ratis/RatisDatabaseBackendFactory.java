package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.RWScheduler;
import org.apache.ratis.client.RaftClient;
import java.io.*;
import java.net.URI;
import java.nio.file.*;
import java.util.*;
import java.time.Duration;
import it.cavallium.rockserver.core.common.RequestContext;

/** Aggregate common connection composition. Node service catalogue/DATA/scheduler remain borrowed. */
public final class RatisDatabaseBackendFactory implements AutoCloseable {
    public record Policy(RatisRocksDBConnection.Limits connection,RatisSessionClient.Limits sessions,RemoteMutationOwnerLimits pointOwners,long grantLifetimeMillis,
                         int maximumAllocationGrants,int replicaClients,RatisOperatorCatalogue.Limits operatorLimits,RatisOperatorCatalogue.AdmissionPolicy operatorAdmission){
        public Policy{Objects.requireNonNull(connection);Objects.requireNonNull(sessions);Objects.requireNonNull(pointOwners);Objects.requireNonNull(operatorLimits);Objects.requireNonNull(operatorAdmission);if(grantLifetimeMillis<1 || grantLifetimeMillis>SessionEpochLedger.MAX_LIFETIME_MILLIS || maximumAllocationGrants<1 || maximumAllocationGrants>64 || replicaClients<1 || replicaClients>RatisSchemaReplicaClient.MAX_CALLS)throw new IllegalArgumentException("Invalid aggregate backend policy");}
        public static Policy defaults(){return new Policy(RatisRocksDBConnection.Limits.defaults(),RatisSessionClient.Limits.defaults(),RemoteMutationOwnerLimits.defaults(),60L*60*1000,64,16,new RatisOperatorCatalogue.Limits(64,512L*1024*1024),upload->{throw new IllegalArgumentException("Explicit trusted operator upload admission is required");});}
    }
    @FunctionalInterface interface Homes {RatisRoutedClient connect(RangeDirectory directory)throws IOException;}
    private final Path root;private final RatisNodePlan plan;private final RaftClient catalogue;private final Map<UUID,RatisRoutedClient> data;private final RWScheduler scheduler;
    private final RatisRocksDBConnection.DomainPlacement placement;private final RatisSchemaClient.TrustedOperatorResolver resolver;private final Policy policy;private final RatisRequestBudget pointBudget;private final RatisJournalAccess.RootLease rootOwnership;
    private final RatisRoutedClient snapshots,transactions;private final RatisSnapshotFences snapshotHomes;private final RatisTransactionStatuses transactionHomes;
    private final RatisSchemaReplicaClient replicas;private final RatisSessionEpochs epochs;private final RatisCatalogue directories;private final RatisTopologyCatalogue topology;private final RatisOperatorCatalogue operators;
    private RatisRocksDBConnection connection;private RatisExportBackend exports;private RatisBackendAllocations exportAllocations;private RatisExportClient exportDelivery;private boolean closed,exportAllocationOpening;
    private static final int MAX_EXPORT_BINDINGS=RatisExportClient.MAX_JOBS*RatisNodePlan.MAX_DATA_GROUPS;
    private final Map<UUID,ExportBinding> exportBindings=new HashMap<>();
    public RatisDatabaseBackendFactory(Path root,RatisNodePlan plan,RaftClient borrowedCatalogue,Map<UUID,RatisRoutedClient> borrowedData,RWScheduler borrowedScheduler,
            RatisRocksDBConnection.DomainPlacement trustedPlacement,RatisSchemaClient.TrustedOperatorResolver trustedResolver,Policy policy)throws IOException{
        this(root,plan,borrowedCatalogue,borrowedData,borrowedScheduler,trustedPlacement,trustedResolver,policy,RatisRoutedClient::connect,new RatisSchemaReplicaClient(policy.replicaClients()));
    }
    RatisDatabaseBackendFactory(Path root,RatisNodePlan plan,RaftClient catalogue,Map<UUID,RatisRoutedClient> data,RWScheduler scheduler,
            RatisRocksDBConnection.DomainPlacement placement,RatisSchemaClient.TrustedOperatorResolver resolver,Policy policy,Homes homes,RatisSchemaReplicaClient replicas)throws IOException{
        this.replicas=Objects.requireNonNull(replicas);RatisRoutedClient first=null,second=null;RatisRequestBudget createdBudget=null;RatisJournalAccess.RootLease acquiredRoot=null;
        try{this.root=root.toAbsolutePath().normalize();this.plan=Objects.requireNonNull(plan);this.catalogue=Objects.requireNonNull(catalogue);this.data=Map.copyOf(data);this.scheduler=Objects.requireNonNull(scheduler);this.placement=Objects.requireNonNull(placement);this.resolver=Objects.requireNonNull(resolver);this.policy=Objects.requireNonNull(policy);this.pointBudget=createdBudget=new RatisRequestBudget(policy.pointOwners());this.rootOwnership=acquiredRoot=pointBudget.backendRoot(this.root);
            RatisBackendJournalInventory.inventory(this.root,pointBudget);
            var domains=plan.domains().stream().map(RatisNodePlan.Domain::domainId).collect(java.util.stream.Collectors.toSet());if(!domains.equals(this.data.keySet()))throw new IOException("Aggregate backend requires every declared DATA domain");
            var snapshot=home(plan,RatisNodePlan.Role.SNAPSHOT_HOME);var transaction=home(plan,RatisNodePlan.Role.TRANSACTION_HOME);
            first=homes.connect(snapshot.directory());second=homes.connect(transaction.directory());snapshots=first;transactions=second;snapshotHomes=new RatisSnapshotFences(snapshots,snapshot.column());transactionHomes=new RatisTransactionStatuses(transactions,transaction.column());
            var binding=plan.catalogue();epochs=new RatisSessionEpochs(catalogue,plan.clusterId(),plan.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.sessionsColumn());directories=new RatisCatalogue(catalogue,plan.clusterId(),plan.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn());topology=new RatisTopologyCatalogue(catalogue,plan.clusterId(),plan.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn());operators=new RatisOperatorCatalogue(catalogue,plan.clusterId(),plan.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn(),policy.operatorLimits(),policy.operatorAdmission());
        }catch(Throwable failure){for(var owner:new AutoCloseable[]{second,first,replicas,createdBudget,acquiredRoot})if(owner!=null)try{owner.close();}catch(Exception cleanup){failure.addSuppressed(cleanup);}if(failure instanceof IOException io)throw io;throw new IOException("Aggregate HOME/replica ownership initialization failed",failure);}
    }
    private static RatisNodePlan.Home home(RatisNodePlan plan,RatisNodePlan.Role role)throws IOException{var homes=plan.homes().stream().filter(h->h.role()==role).toList();if(homes.size()!=1)throw new IOException("Aggregate backend requires one complete declared "+role+" directory");return homes.getFirst();}
    public synchronized RatisRocksDBConnection open(URI uri)throws IOException{
        if(closed || connection!=null)throw new IOException("Aggregate backend already opened or closed");Files.createDirectories(root);
        connection=new RatisRocksDBConnection(uri,root,data,placement,scheduler,plan.catalogue(),catalogue,replicas,operators,resolver,this::generation,policy.connection(),List.of(),pointBudget);return connection;
    }
    private RatisRocksDBConnection.Generation generation(DistributedSchema schema,RangeDirectory directory,Path generationRoot,java.util.function.LongSupplier ids)throws IOException{
        var routed=data.get(schema.domainId());if(routed==null)throw new IOException("Generation domain has no declared routed DATA client");schema.requireMatches(directory);routed.requireKnownSchema(schema);
        var allocationTargets=targets(routed,directory,true);var allocator=new RatisBackendAllocations(generationRoot.resolve("allocations"),epochs,scheduler,policy.grantLifetimeMillis(),policy.maximumAllocationGrants(),allocationTargets,pointBudget);
        var sessions=new RatisRocksDBConnection.SessionFactory(List.of(routed,transactions),(path,expectedSchema,plan)->RatisBackendSessions.window(path,routed,epochs,policy.grantLifetimeMillis(),policy.sessions(),plan.targets(),pointBudget));
        var factory=RatisRocksDBConnection.durableGenerationFactory(routed,scheduler,snapshotHomes,transactionHomes,epochs,directories,header->{var definition=topology.readDefinition(header.definitionDigest());if(definition==null)throw new IOException("Immutable ACK topology definition absent");header.requireDefinition(definition);return definition.sourceDirectory();},allocator,sessions,List.of(allocator),pointBudget);
        return factory.create(schema,directory,generationRoot,ids);
    }
    private List<RatisBackendSessions.Target> targets(RatisRoutedClient routed,RangeDirectory directory,boolean allocations){
        var targets=new ArrayList<RatisBackendSessions.Target>();for(var range:directory.ranges())targets.add(new RatisBackendSessions.Target(routed,range.groupId(),directory));var transactionDirectory=transactions.directory();for(var range:transactionDirectory.ranges())targets.add(new RatisBackendSessions.Target(transactions,range.groupId(),transactionDirectory));if(allocations){var snapshotDirectory=snapshots.directory();for(var range:snapshotDirectory.ranges())targets.add(new RatisBackendSessions.Target(snapshots,range.groupId(),snapshotDirectory));}return List.copyOf(targets);
    }
    /** Separate aggregate lifetime: public export coverage never consumes ordinary connection generations. */
    public synchronized RatisExportBackend exports()throws IOException {
        if(closed)throw new IOException("Aggregate backend closed");if(exports!=null)return exports;
        Files.createDirectories(root);
        if(exportDelivery==null)exportDelivery=new RatisExportClient(null,scheduler,null,RatisExportClient.Limits.defaults());
        var dependencies=new RatisAggregateExportDriver.Dependencies(this::exportVector,this::allocateExport,new RatisAggregateExportDriver.BindingFactory(){
            public RatisSnapshotPlan plan(DatabaseExportDefinition.Domain domain,RatisAggregateExportDriver.Allocation allocation,TransactionScope scope)throws IOException{var routed=exportRoute(domain);var home=snapshotHomes.route(allocation.identity().fence());return RatisSnapshotPlan.create(domain.directory(),scope,allocation.identity().fence(),allocation.identity().business(),home.groupId(),home.rangeEpoch(),allocation.identity().firstControl());}
            public RatisAggregateExportDriver.Binding bind(DatabaseExportDefinition.Domain domain,RatisAggregateExportJournal.Slot slot,Duration ttl)throws IOException{return bindExport(domain,slot,ttl);}
        },exportCohorts(),exportDelivery);
        exports=RatisExportBackend.journalOwned(root.resolve("exports"),scheduler,dependencies,RatisExportBackend.Limits.defaults(),pointBudget);return exports;
    }
    private java.util.concurrent.CompletableFuture<RatisAggregateExportDriver.Allocation> allocateExport(DatabaseExportDefinition input,RequestContext context) {
        var targets=new TreeMap<UUID,RatisBackendSessions.Target>(RangeDirectoryCodec.UUID_ORDER);RatisBackendAllocations allocator;
        try {
            for(var domain:input.domains()){var routed=exportRoute(domain);for(var range:domain.directory().ranges())targets.put(range.groupId(),new RatisBackendSessions.Target(routed,range.groupId(),domain.directory()));}
            var home=snapshots.directory();for(var range:home.ranges())targets.put(range.groupId(),new RatisBackendSessions.Target(snapshots,range.groupId(),home));
            synchronized(this){if(closed || exportAllocationOpening)throw new IOException("Export allocator closed or opening");allocator=exportAllocations;if(allocator==null)exportAllocationOpening=true;}
            if(allocator==null){try{allocator=new RatisBackendAllocations(root.resolve("export-allocations"),epochs,scheduler,policy.grantLifetimeMillis(),policy.maximumAllocationGrants(),List.of(),pointBudget);synchronized(this){exportAllocations=allocator;}}finally{synchronized(this){exportAllocationOpening=false;}}}
            return allocator.allocateIssued(List.copyOf(targets.values()),context);
        }catch(IOException failure){return java.util.concurrent.CompletableFuture.failedFuture(failure);}
    }
    private RatisAggregateExportDriver.Vector exportVector()throws IOException {
        var binding=plan.catalogue();var complete=new RatisDatabaseDomainRoster(catalogue,plan.clusterId(),plan.databaseId(),binding).readComplete();var schemas=new RatisSchemaCatalogue(catalogue,plan.clusterId(),plan.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn());var domains=new ArrayList<DatabaseExportDefinition.Domain>();
        for(var domain:complete.roster().domainIds()){var directory=complete.directories().get(domain);var schema=schemas.read(directory.schemaDigest());if(schema==null)throw new IOException("Export roster schema is absent");var value=new DatabaseExportDefinition.Domain(directory,schema,List.of());exportRoute(value);domains.add(value);}new RatisDatabaseDomainRoster(catalogue,plan.clusterId(),plan.databaseId(),binding).requireUnchanged(complete.roster());return new RatisAggregateExportDriver.Vector(complete.roster(),domains);
    }
    private RatisRoutedClient exportRoute(DatabaseExportDefinition.Domain domain)throws IOException {
        var routed=data.get(domain.schema().domainId());if(routed==null)throw new IOException("Complete export roster has no qualified borrowed DATA route");try{routed.requireKnownSchema(domain.schema());if(!Arrays.equals(routed.directory().payloadDigest(),domain.directory().payloadDigest()))throw new IllegalArgumentException("Frozen export directory changed");domain.schema().requireMatches(domain.directory());}catch(IllegalArgumentException stale){throw new IOException("Original qualified export DATA view unavailable",stale);}return routed;
    }
    private final class ExportBinding implements AutoCloseable {
        final UUID fence;final RatisDurableSnapshot journal;final RatisSnapshotCoordinator coordinator;final RatisSnapshotClient source;final RatisRoutedClient routed;final boolean ownsRoute;private boolean closed;
        ExportBinding(UUID fence,RatisDurableSnapshot journal,RatisSnapshotCoordinator coordinator,RatisSnapshotClient source,RatisRoutedClient routed,boolean ownsRoute){this.ownsRoute=ownsRoute;this.fence=fence;this.journal=journal;this.coordinator=coordinator;this.source=source;this.routed=routed;}
        public void close()throws IOException{if(closed)return;coordinator.close();source.close();journal.close();if(ownsRoute)routed.close();synchronized(exportBindings){exportBindings.remove(fence,this);}closed=true;}
    }
    private RatisAggregateExportDriver.Binding bindExport(DatabaseExportDefinition.Domain domain,RatisAggregateExportJournal.Slot slot,Duration ttl)throws IOException {
        var frozen=RatisSnapshotPlan.frozen(domain.directory(),slot.fence());var first=slot.fence().participants().stream().map(SnapshotFence.Participant::installOperation).min(Comparator.comparingLong(OperationId::sequence)).orElseThrow();
        synchronized(exportBindings){if(exportBindings.size()>=MAX_EXPORT_BINDINGS || exportBindings.containsKey(slot.fence().fenceId()))throw new IOException("Export binding already owned or capacity exhausted");}
        boolean retained=Files.exists(slot.snapshot(),LinkOption.NOFOLLOW_LINKS),ownsRoute=false;RatisRoutedClient routed;
        try{routed=exportRoute(domain);}catch(IOException changed){if(!retained)throw changed;var borrowed=data.get(domain.schema().domainId());if(borrowed==null)throw changed;routed=borrowed.borrowedSnapshotCleanupView(domain.directory(),slot.fence());ownsRoute=true;}
        RatisDurableSnapshot journal=null;RatisSnapshotCoordinator coordinator=null;RatisSnapshotClient source=null;
        try {
            if(retained)journal=RatisDurableSnapshot.reopenOwned(slot.snapshot(),snapshotHomes,routed,pointBudget);else{var initial=SnapshotFenceStatus.begin(slot.fence());journal=RatisDurableSnapshot.createOwned(slot.snapshot(),snapshotHomes,routed,initial,snapshotHomes.prepare(new OperationId(first.sessionId(),first.epoch(),first.sequence()+128),null,initial),frozen.routes(),ttl,RatisSnapshotCaptureRequest.Mode.READ_ONLY,pointBudget);}
            var saved=journal.snapshot();if(!saved.initial().fence().equals(slot.fence()) || !ttl.equals(saved.requestedTtl()) || saved.captureMode()!=RatisSnapshotCaptureRequest.Mode.READ_ONLY)throw new IOException("Saved export source journal binding changed");
            if(ownsRoute)routed.restoreSnapshotCleanupCaptures(saved);
            coordinator=new RatisSnapshotCoordinator(journal,snapshotHomes,routed,last->{if(!last.sessionId().equals(first.sessionId()) || last.epoch()!=first.epoch() || last.sequence()<first.sequence()+128 || last.sequence()>=first.sequence()+RatisSnapshotPlan.CONTROL_SLOTS-1)throw new IOException("Export HOME control block exhausted");return new OperationId(last.sessionId(),last.epoch(),last.sequence()+1);},UUID::randomUUID,ttl);
            source=RatisSnapshotClient.journalOwned(domain.schema(),snapshotHomes,routed,scheduler,RatisSnapshotClient.Limits.defaults(),RatisSnapshotCaptureRequest.Mode.READ_ONLY,pointBudget);var owned=new ExportBinding(slot.fence().fenceId(),journal,coordinator,source,routed,ownsRoute);synchronized(exportBindings){if(exportBindings.putIfAbsent(owned.fence,owned)!=null)throw new IOException("Export source already owned");}return new RatisAggregateExportDriver.Binding(new RatisDatabaseCaptureCoordinator.CaptureBinding(journal,coordinator,routed),new RatisRemoteExportSource(source,routed,pointBudget),source.identityDigest(),owned);
        }catch(Throwable failure){for(var owner:new AutoCloseable[]{coordinator,source,journal,ownsRoute?routed:null})if(owner!=null)try{owner.close();}catch(Exception cleanup){failure.addSuppressed(cleanup);}if(failure instanceof IOException io)throw io;throw new IOException("Export source binding failed",failure);}
    }
    private RatisSnapshotCohortClient cohortClient(DatabaseBackupDefinition definition,Map<UUID,RatisRoutedClient.PreparedSnapshotCapture> frozen)throws IOException {
        var groups=new HashMap<UUID,RatisRoutedClient>();for(var domain:definition.domains())for(var fence:domain.fences()){ExportBinding binding;synchronized(exportBindings){binding=exportBindings.get(fence.fenceId());}if(binding==null)throw new IOException("Original export cohort binding absent");for(var participant:fence.participants())groups.put(participant.groupId(),binding.routed);}return new RatisSnapshotCohortClient(groups,frozen::get);
    }
    private RatisSnapshotCohortClient cohortClient(RatisDatabaseBackupJournal.CohortRequest request)throws IOException {
        var groups=new HashMap<UUID,RatisRoutedClient>();var frozen=new HashMap<UUID,RatisRoutedClient.PreparedSnapshotCapture>();for(var member:request.members()){ExportBinding binding;synchronized(exportBindings){binding=exportBindings.get(member.fenceId());}if(binding==null)throw new IOException("Original export cohort source absent");var capture=binding.journal.snapshot().participant(member.groupId()).capture();if(capture==null)throw new IOException("Original export cohort request absent");groups.put(member.groupId(),binding.routed);frozen.put(member.groupId(),capture);}return new RatisSnapshotCohortClient(groups,frozen::get);
    }
    private RatisDatabaseBackupDriver.CohortTransport exportCohorts(){return new RatisDatabaseBackupDriver.CohortTransport(){
        public java.util.concurrent.CompletableFuture<Void> requireSupported(DatabaseBackupDefinition definition,RatisDatabaseBackupDriver.Policy policy)throws IOException{return cohortClient(definition,Map.of()).requireSupported(definition,policy);}
        public java.util.concurrent.CompletableFuture<List<RatisDatabaseBackupJournal.CohortRequest>> qualify(DatabaseBackupDefinition definition,Map<UUID,RatisRoutedClient.PreparedSnapshotCapture> frozen,RatisDatabaseBackupDriver.Policy policy)throws IOException{return cohortClient(definition,frozen).qualify(definition,frozen,policy);}
        public java.util.concurrent.CompletableFuture<Map<UUID,RatisSnapshotCaptureRequest.Result>> capture(RatisDatabaseBackupJournal.CohortRequest request,Map<UUID,RatisRoutedClient.PreparedSnapshotCapture> frozen)throws IOException{return cohortClient(request).capture(request,frozen);}
        public java.util.concurrent.CompletableFuture<Void> requireLive(RatisDatabaseBackupJournal.CohortRequest request,Map<UUID,SnapshotFenceStatus.Capture> original)throws IOException{return cohortClient(request).requireLive(request,original);}
        public java.util.concurrent.CompletableFuture<Void> cancelAndDrain(RatisDatabaseBackupJournal.CohortRequest request)throws IOException{return cohortClient(request).cancelAndDrain(request);}
    };}
    /** Actual parent capture/materialization/delivery must drain before any factory dependency is released. */
    public synchronized void drainExports()throws IOException{if(exports!=null)exports.close();}
    @Override public synchronized void close()throws IOException{
        if(closed)return;if(exportAllocationOpening)throw new IOException("Actual export allocator opening remains owned");drainExports();if(exportDelivery!=null)exportDelivery.close();if(exportAllocations!=null)exportAllocations.close();if(connection!=null)connection.close();pointBudget.close();IOException failure=null;for(var owner:List.of(replicas,transactions,snapshots))try{owner.close();}catch(Exception problem){if(failure==null)failure=new IOException("Owned aggregate transport close failed",problem);else failure.addSuppressed(problem);}if(failure!=null)throw failure;rootOwnership.close();closed=true;
    }
}
