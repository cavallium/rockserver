package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.RWScheduler;
import org.apache.ratis.client.RaftClient;
import java.io.*;
import java.net.URI;
import java.nio.file.*;
import java.util.*;

/** Aggregate common connection composition. Node service catalogue/DATA/scheduler remain borrowed. */
public final class RatisDatabaseBackendFactory implements AutoCloseable {
    public record Policy(RatisRocksDBConnection.Limits connection,RatisSessionClient.Limits sessions,long grantLifetimeMillis,
                         int maximumAllocationGrants,int replicaClients,RatisOperatorCatalogue.Limits operatorLimits,RatisOperatorCatalogue.AdmissionPolicy operatorAdmission){
        public Policy{Objects.requireNonNull(connection);Objects.requireNonNull(sessions);Objects.requireNonNull(operatorLimits);Objects.requireNonNull(operatorAdmission);if(grantLifetimeMillis<1 || grantLifetimeMillis>SessionEpochLedger.MAX_LIFETIME_MILLIS || maximumAllocationGrants<1 || maximumAllocationGrants>64 || replicaClients<1 || replicaClients>RatisSchemaReplicaClient.MAX_CALLS)throw new IllegalArgumentException("Invalid aggregate backend policy");}
        public static Policy defaults(){return new Policy(RatisRocksDBConnection.Limits.defaults(),RatisSessionClient.Limits.defaults(),60L*60*1000,64,16,new RatisOperatorCatalogue.Limits(64,512L*1024*1024),upload->{throw new IllegalArgumentException("Explicit trusted operator upload admission is required");});}
    }
    @FunctionalInterface interface Homes {RatisRoutedClient connect(RangeDirectory directory)throws IOException;}
    private final Path root;private final RatisNodePlan plan;private final RaftClient catalogue;private final Map<UUID,RatisRoutedClient> data;private final RWScheduler scheduler;
    private final RatisRocksDBConnection.DomainPlacement placement;private final RatisSchemaClient.TrustedOperatorResolver resolver;private final Policy policy;
    private final RatisRoutedClient snapshots,transactions;private final RatisSnapshotFences snapshotHomes;private final RatisTransactionStatuses transactionHomes;
    private final RatisSchemaReplicaClient replicas;private final RatisSessionEpochs epochs;private final RatisCatalogue directories;private final RatisTopologyCatalogue topology;private final RatisOperatorCatalogue operators;
    private RatisRocksDBConnection connection;private boolean closed;
    public RatisDatabaseBackendFactory(Path root,RatisNodePlan plan,RaftClient borrowedCatalogue,Map<UUID,RatisRoutedClient> borrowedData,RWScheduler borrowedScheduler,
            RatisRocksDBConnection.DomainPlacement trustedPlacement,RatisSchemaClient.TrustedOperatorResolver trustedResolver,Policy policy)throws IOException{
        this(root,plan,borrowedCatalogue,borrowedData,borrowedScheduler,trustedPlacement,trustedResolver,policy,RatisRoutedClient::connect,new RatisSchemaReplicaClient(policy.replicaClients()));
    }
    RatisDatabaseBackendFactory(Path root,RatisNodePlan plan,RaftClient catalogue,Map<UUID,RatisRoutedClient> data,RWScheduler scheduler,
            RatisRocksDBConnection.DomainPlacement placement,RatisSchemaClient.TrustedOperatorResolver resolver,Policy policy,Homes homes,RatisSchemaReplicaClient replicas)throws IOException{
        this.replicas=Objects.requireNonNull(replicas);RatisRoutedClient first=null,second=null;
        try{this.root=root.toAbsolutePath().normalize();this.plan=Objects.requireNonNull(plan);this.catalogue=Objects.requireNonNull(catalogue);this.data=Map.copyOf(data);this.scheduler=Objects.requireNonNull(scheduler);this.placement=Objects.requireNonNull(placement);this.resolver=Objects.requireNonNull(resolver);this.policy=Objects.requireNonNull(policy);
            var domains=plan.domains().stream().map(RatisNodePlan.Domain::domainId).collect(java.util.stream.Collectors.toSet());if(!domains.equals(this.data.keySet()))throw new IOException("Aggregate backend requires every declared DATA domain");
            var snapshot=home(plan,RatisNodePlan.Role.SNAPSHOT_HOME);var transaction=home(plan,RatisNodePlan.Role.TRANSACTION_HOME);
            first=homes.connect(snapshot.directory());second=homes.connect(transaction.directory());snapshots=first;transactions=second;snapshotHomes=new RatisSnapshotFences(snapshots,snapshot.column());transactionHomes=new RatisTransactionStatuses(transactions,transaction.column());
            var binding=plan.catalogue();epochs=new RatisSessionEpochs(catalogue,plan.clusterId(),plan.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.sessionsColumn());directories=new RatisCatalogue(catalogue,plan.clusterId(),plan.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn());topology=new RatisTopologyCatalogue(catalogue,plan.clusterId(),plan.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn());operators=new RatisOperatorCatalogue(catalogue,plan.clusterId(),plan.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn(),policy.operatorLimits(),policy.operatorAdmission());
        }catch(Throwable failure){for(var owner:new AutoCloseable[]{second,first,replicas})if(owner!=null)try{owner.close();}catch(Exception cleanup){failure.addSuppressed(cleanup);}if(failure instanceof IOException io)throw io;throw new IOException("Aggregate HOME/replica ownership initialization failed",failure);}
    }
    private static RatisNodePlan.Home home(RatisNodePlan plan,RatisNodePlan.Role role)throws IOException{var homes=plan.homes().stream().filter(h->h.role()==role).toList();if(homes.size()!=1)throw new IOException("Aggregate backend requires one complete declared "+role+" directory");return homes.getFirst();}
    public synchronized RatisRocksDBConnection open(URI uri)throws IOException{
        if(closed || connection!=null)throw new IOException("Aggregate backend already opened or closed");Files.createDirectories(root);
        connection=new RatisRocksDBConnection(uri,root,data,placement,scheduler,plan.catalogue(),catalogue,replicas,operators,resolver,this::generation,policy.connection(),List.of());return connection;
    }
    private RatisRocksDBConnection.Generation generation(DistributedSchema schema,RangeDirectory directory,Path generationRoot,java.util.function.LongSupplier ids)throws IOException{
        var routed=data.get(schema.domainId());if(routed==null)throw new IOException("Generation domain has no declared routed DATA client");schema.requireMatches(directory);routed.requireKnownSchema(schema);
        var allocationTargets=targets(routed,directory,true);var allocator=new RatisBackendAllocations(generationRoot.resolve("allocations"),epochs,scheduler,policy.grantLifetimeMillis(),policy.maximumAllocationGrants(),allocationTargets);
        RatisRocksDBConnection.SessionFactory sessions=(path,expectedSchema,expectedDirectory)->RatisBackendSessions.window(path,routed,epochs,policy.grantLifetimeMillis(),policy.sessions(),targets(routed,expectedDirectory,false));
        var factory=RatisRocksDBConnection.durableGenerationFactory(routed,scheduler,snapshotHomes,transactionHomes,epochs,directories,header->{var definition=topology.readDefinition(header.definitionDigest());if(definition==null)throw new IOException("Immutable ACK topology definition absent");header.requireDefinition(definition);return definition.sourceDirectory();},allocator,sessions,List.of(allocator));
        return factory.create(schema,directory,generationRoot,ids);
    }
    private List<RatisBackendSessions.Target> targets(RatisRoutedClient routed,RangeDirectory directory,boolean allocations){
        var targets=new ArrayList<RatisBackendSessions.Target>();for(var range:directory.ranges())targets.add(new RatisBackendSessions.Target(routed,range.groupId()));for(var range:transactions.directory().ranges())targets.add(new RatisBackendSessions.Target(transactions,range.groupId()));if(allocations)for(var range:snapshots.directory().ranges())targets.add(new RatisBackendSessions.Target(snapshots,range.groupId()));return List.copyOf(targets);
    }
    @Override public synchronized void close()throws IOException{
        if(closed)return;if(connection!=null)connection.close();IOException failure=null;for(var owner:List.of(replicas,transactions,snapshots))try{owner.close();}catch(Exception problem){if(failure==null)failure=new IOException("Owned aggregate transport close failed",problem);else failure.addSuppressed(problem);}if(failure!=null)throw failure;closed=true;
    }
}
