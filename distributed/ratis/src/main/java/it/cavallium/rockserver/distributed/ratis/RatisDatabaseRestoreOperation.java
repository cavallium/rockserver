package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.DatabaseConfig;
import it.cavallium.rockserver.core.impl.rocksdb.DistributedStoragePool;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import org.apache.ratis.conf.Parameters;

/** Service-owned execution of the frozen restore controller; the archive and operator policy remain borrowed. */
public final class RatisDatabaseRestoreOperation implements RatisDatabaseRestoreController.Provisioner,AutoCloseable {
    public record Placement(ReplicaDescriptor local,List<RatisNodePlan.Home> homes,Map<String,String> streams,
            List<RatisNativePoolSpec> pools,Map<UUID,UUID> domainPools,UUID cataloguePool,Map<UUID,UUID> groupPools,
            RatisNodeRuntime.Settings settings,int readThreads,int writeThreads,RatisSchemaReplicaService.Limits replicas) {
        public Placement {
            Objects.requireNonNull(local);homes=List.copyOf(homes);streams=Map.copyOf(streams);pools=List.copyOf(pools);
            domainPools=Map.copyOf(domainPools);Objects.requireNonNull(cataloguePool);groupPools=Map.copyOf(groupPools);
            Objects.requireNonNull(settings);Objects.requireNonNull(replicas);
            for(var role:List.of(RatisNodePlan.Role.TRANSACTION_HOME,RatisNodePlan.Role.SNAPSHOT_HOME))
                if(homes.stream().filter(h->h.role()==role).count()!=1)throw new IllegalArgumentException("Restore service requires caller-declared complete HOME placements");
        }
        RatisNodeManifest manifest(RatisDatabaseRestorePlanner.Plan original) {
            if(!original.catalogue().replicas().contains(local))throw new IllegalArgumentException("Restore local owner outside frozen membership");
            var assigned=new HashMap<>(groupPools);var previous=assigned.put(original.catalogue().groupId(),cataloguePool);
            if(previous!=null && !previous.equals(cataloguePool))throw new IllegalArgumentException("Catalogue pool changed");
            var node=original.nodePlan(local,homes,streams,pools,domainPools,assigned);
            var peers=new HashSet<String>();original.catalogue().replicas().forEach(r->peers.add(r.peerId()));
            homes.forEach(h->h.directory().ranges().forEach(r->r.replicas().forEach(owner->peers.add(owner.peerId()))));
            if(!streams.keySet().containsAll(peers))throw new IllegalArgumentException("Restore requires explicit streaming addresses for every declared owner");
            return new RatisNodeManifest(original.domains().stream().map(RatisDatabaseRestorePlanner.Domain::schema).toList(),node,settings,readThreads,writeThreads,replicas);
        }
    }
    record Data(RatisRangeImportController.DataPlane plane,AutoCloseable owner) {
        Data {Objects.requireNonNull(plane);Objects.requireNonNull(owner);}
    }
    @FunctionalInterface interface ResourcesFactory { RatisNodeService.Resources open(RatisNodeManifest manifest)throws IOException; }
    @FunctionalInterface interface DataFactory { Data open(RatisNodeManifest manifest,RatisNodeService.Resources resources)throws IOException; }
    record Dependencies(ResourcesFactory resources,RatisNodeRuntime.ServerFactory server,RatisNodeRuntime.HomeClientFactory homes,DataFactory data) {}
    private final Path operationRoot,nodeRoot;private final DatabaseConfig config;private final Placement placement;
    private final RatisSchemaReplicaService.OperatorPolicy policy;private final Dependencies dependencies;
    private RatisDatabaseRestoreController controller;private RatisNodeManifest manifest;private RatisNodeService service;private Data data;
    private boolean busy,closing,closed,closeRequested,controllerClosed,dataClosed,adopted;private Thread workThread;
    private RatisDatabaseRestoreOperation(Path root,Path nodeRoot,DatabaseConfig config,Placement placement,
            RatisSchemaReplicaService.OperatorPolicy policy,Dependencies dependencies)throws IOException {
        operationRoot=root.toAbsolutePath().normalize();this.nodeRoot=nodeRoot.toAbsolutePath().normalize();
        if(operationRoot.startsWith(this.nodeRoot) || this.nodeRoot.startsWith(operationRoot))throw new IOException("Restore journal and node storage must have separate roots");
        this.config=Objects.requireNonNull(config);this.placement=Objects.requireNonNull(placement);this.policy=Objects.requireNonNull(policy);this.dependencies=Objects.requireNonNull(dependencies);
    }
    private static Dependencies production(DatabaseConfig config,Parameters streams) {
        return new Dependencies(manifest->RatisNodeService.connectResources(manifest,config),null,RatisRoutedClient::connect,(manifest,resources)->{
            var client=new RatisRangeImportDataClient(manifest.plan(),resources.catalogue,streams);return new Data(client,client);
        });
    }
    /** Explicit authority to initialize only this newly frozen archive/cluster/placement operation. */
    public static RatisDatabaseRestoreOperation create(Path root,Path nodeRoot,RatisDatabaseBackupArchive archive,
            UUID cluster,List<ReplicaDescriptor> membership,UUID session,long logicalTime,DatabaseConfig config,
            Placement placement,RatisSchemaReplicaService.OperatorPolicy policy,Parameters streams)throws IOException {
        return create(root,nodeRoot,archive,cluster,membership,session,logicalTime,config,placement,policy,production(config,streams));
    }
    static RatisDatabaseRestoreOperation create(Path root,Path nodeRoot,RatisDatabaseBackupArchive archive,
            UUID cluster,List<ReplicaDescriptor> membership,UUID session,long logicalTime,DatabaseConfig config,
            Placement placement,RatisSchemaReplicaService.OperatorPolicy policy,Dependencies dependencies)throws IOException {
        if(Files.exists(nodeRoot.resolve("node.identity"),LinkOption.NOFOLLOW_LINKS))throw new IOException("Fresh restore cannot adopt an existing node identity");
        var owner=new RatisDatabaseRestoreOperation(root,nodeRoot,config,placement,policy,dependencies);
        owner.controller=RatisDatabaseRestoreController.create(root,archive,cluster,membership,session,logicalTime,owner);
        try{owner.freezeManifest(false);return owner;}catch(IOException|RuntimeException|Error failure){owner.abort(failure);throw failure;}
    }
    /** Resume the original journal and manifest, including an interrupted unexposed initialization; never regenerate identities/grants. */
    public static RatisDatabaseRestoreOperation reopen(Path root,Path nodeRoot,RatisDatabaseBackupArchive archive,
            DatabaseConfig config,Placement placement,RatisSchemaReplicaService.OperatorPolicy policy,Parameters streams)throws IOException {
        return reopen(root,nodeRoot,archive,config,placement,policy,production(config,streams));
    }
    static RatisDatabaseRestoreOperation reopen(Path root,Path nodeRoot,RatisDatabaseBackupArchive archive,
            DatabaseConfig config,Placement placement,RatisSchemaReplicaService.OperatorPolicy policy,Dependencies dependencies)throws IOException {
        var owner=new RatisDatabaseRestoreOperation(root,nodeRoot,config,placement,policy,dependencies);
        owner.controller=RatisDatabaseRestoreController.reopen(root,archive,owner);
        try{owner.freezeManifest(true);return owner;}catch(IOException|RuntimeException|Error failure){owner.abort(failure);throw failure;}
    }
    private void freezeManifest(boolean recovering)throws IOException {
        var expected=placement.manifest(controller.plan());var file=operationRoot.resolve("node.manifest");
        if(Files.exists(file,LinkOption.NOFOLLOW_LINKS)){
            var saved=RatisNodeManifest.read(file);if(!Arrays.equals(RatisNodeManifest.encode(saved),RatisNodeManifest.encode(expected)))throw new IOException("Restore placement/settings differ from original manifest");manifest=saved;
        }else{
            if(recovering && Files.exists(nodeRoot.resolve("node.identity"),LinkOption.NOFOLLOW_LINKS))throw new IOException("Established restore lost original manifest");
            expected.write(file);manifest=expected;
        }
        var rootBinding=operationRoot.resolve("node-root");byte[] path=nodeRoot.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        if(path.length>4096)throw new IOException("Restore node path exceeds bound");
        if(Files.exists(rootBinding,LinkOption.NOFOLLOW_LINKS)){
            if(!Arrays.equals(SessionJournalFiles.read(rootBinding,4130),path))throw new IOException("Restore node storage changed original root");
        }else{
            if(recovering && Files.exists(nodeRoot.resolve("node.identity"),LinkOption.NOFOLLOW_LINKS))throw new IOException("Established restore lost original node root binding");
            SessionJournalFiles.write(rootBinding,path,4130);
        }
        SessionJournalFiles.force(operationRoot);
    }
    private void requireOriginalIdentity()throws IOException {
        Path identity=nodeRoot.resolve("node.identity");if(!Files.exists(identity,LinkOption.NOFOLLOW_LINKS))return;
        if(!Files.isRegularFile(identity,LinkOption.NOFOLLOW_LINKS))throw new IOException("Restore node identity is not an original durable file");
        var bytes=SessionJournalFiles.read(identity,65+RatisNativeExtentPolicy.BYTES);
        if(bytes.length>0 && bytes[0]>=1 && !Files.exists(nodeRoot.resolve("restore.owner"),LinkOption.NOFOLLOW_LINKS))throw new IOException("Initialized restore lost durable original operation ownership");
        if(bytes.length!=33+RatisNativeExtentPolicy.BYTES || bytes[0]<0 || bytes[0]>2
                || !Arrays.equals(Arrays.copyOfRange(bytes,1,33),manifest.plan().identityDigest())
                || !Arrays.equals(Arrays.copyOfRange(bytes,33,bytes.length),manifest.settings().resources().nativeExtents().encode()))throw new IOException("Restore cannot adopt unrelated durable node identity");
    }
    @Override public RatisDatabaseRestoreController.Execution provision(RatisDatabaseRestorePlanner.Plan original,Map<UUID,Path> operators)throws IOException {
        synchronized(this){if(!busy || workThread!=Thread.currentThread() || closing || closed)throw new IOException("Provisioning requires its active restore operation owner");}
        if(controller==null || !Arrays.equals(controller.plan().encode(),original.encode()))throw new IOException("Provisioner requires its original frozen restore plan");
        if(service==null){
            freezeManifest(true);Files.createDirectories(nodeRoot);SessionJournalFiles.force(nodeRoot);SessionJournalFiles.force(nodeRoot.getParent());
            requireOriginalIdentity();
            var resources=dependencies.resources().open(manifest);
            service=RatisNodeService.openRestore(manifest,nodeRoot,config,resources,dependencies.server(),policy,dependencies.homes(),budget->{markOriginalOwner();stageOperators(operators,budget);});
            service.start();
        }
        if(service.state()!=RatisNodeRuntime.State.RUNNING)throw new IOException("Restore node initialization must drain before retrying its original operation");
        if(data==null)data=dependencies.data().open(manifest,serviceResources());
        return new RatisDatabaseRestoreController.Execution(manifest.plan(),serviceResources().catalogue,serviceResources().routed,data.plane());
    }
    /** Called only by Runtime while holding node.lock, before native binding/pools or phase2 exposure. */
    private void markOriginalOwner()throws IOException {
        byte[] root=operationRoot.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);if(root.length>4096)throw new IOException("Restore operation path exceeds bound");
        var planHash=sha().digest(controller.plan().encode());var manifestHash=sha().digest(RatisNodeManifest.encode(manifest));
        byte[] body=java.nio.ByteBuffer.allocate(68+root.length).put(planHash).put(manifestHash).putInt(root.length).put(root).array();
        var marker=nodeRoot.resolve("restore.owner");
        if(Files.exists(marker,LinkOption.NOFOLLOW_LINKS)){
            if(!Files.isRegularFile(marker,LinkOption.NOFOLLOW_LINKS) || !Arrays.equals(SessionJournalFiles.read(marker,4200),body))throw new IOException("Restore ownership differs from exact original operation/manifest");
        }else SessionJournalFiles.write(marker,body,4200);
        SessionJournalFiles.force(nodeRoot);
    }
    private RatisNodeService.Resources serviceResources(){return service.restoreResources();}
    private void stageOperators(Map<UUID,Path> assets,RatisNodeResourceBudget staging)throws IOException {
        for(var domain:manifest.schemas().values())for(var descriptor:domain.columns().stream().map(DistributedSchema.Column::operator).filter(Objects::nonNull).distinct().toList()){
            var pool=manifest.plan().pools().get(manifest.plan().domainPools().get(domain.domainId()));
            long graph=3L*OperatorUpload.MAX_ARTIFACT_BYTES+65536;
            if(graph>pool.resources().protectedLane().bytes())throw new IOException("Operator staging exceeds declared pool control envelope");
            try(var reservation=staging.reserve(pool.id(),RatisNodeResourceBudget.Kind.PROTECTED,graph,1)){
            byte[] artifact=null,buffer=new byte[OperatorUpload.MAX_ARTIFACT_BYTES];
            for(var path:assets.values()){
                try(var input=java.nio.channels.FileChannel.open(path,StandardOpenOption.READ,LinkOption.NOFOLLOW_LINKS)){
                    long size=input.size();if(size<1 || size>buffer.length)throw new IOException("Archived operator exceeds bounded artifact envelope");
                    var target=java.nio.ByteBuffer.wrap(buffer,0,(int)size);while(target.hasRemaining()){if(input.read(target)<0)throw new IOException("Original archived operator truncated");}
                    if(input.read(java.nio.ByteBuffer.allocate(1))!=-1 || input.size()!=size)throw new IOException("Archived operator changed bounded length");
                    var digest=sha();digest.update(buffer,0,(int)size);
                    if(Arrays.equals(digest.digest(),descriptor.artifactSha256())){artifact=Arrays.copyOf(buffer,(int)size);break;}
                }
            }
            if(artifact==null)throw new IOException("Original archived operator missing");
            try{
                var contract=Objects.requireNonNull(policy.admit(descriptor));
                DistributedStoragePool.stageOperator(pool.nativePath(nodeRoot),descriptor,contract,artifact);
            }catch(Exception failure){throw new IOException("Trusted archived operator qualification failed",failure);}
            }
        }
    }
    private static java.security.MessageDigest sha(){try{return java.security.MessageDigest.getInstance("SHA-256");}catch(java.security.NoSuchAlgorithmException impossible){throw new AssertionError(impossible);}}
    public RatisDatabaseRestorePlanner.Plan plan(){return controller.plan();}
    public RatisDatabaseRestoreController.Phase phase(){return controller.phase();}
    public RatisDatabaseRestoreController.Progress step(int budget)throws IOException {
        begin();try{var progress=controller.step(budget);if(progress.phase()==RatisDatabaseRestoreController.Phase.READY)adopt();return progress;}finally{end();}
    }
    private void adopt()throws IOException {if(!adopted){service.adoptRestore(controller.plan(),controller.issuedLedger());adopted=true;}}
    public RatisNodeService service()throws IOException {
        begin();try{if(controller.phase()!=RatisDatabaseRestoreController.Phase.READY)throw new IOException("Restore frontend remains closed until READY");controller.issuedLedger();adopt();service.verifyCatalogue();return service;}finally{end();}
    }
    public SessionEpochLedger issuedLedger()throws IOException {begin();try{return controller.issuedLedger();}finally{end();}}
    private synchronized void begin()throws IOException {if(closed || closeRequested || closing || busy)throw new IOException("Restore operation closed or actual work active");busy=true;workThread=Thread.currentThread();}
    private synchronized void end(){busy=false;workThread=null;}
    private void abort(Throwable failure){try{close();}catch(Exception cleanup){failure.addSuppressed(cleanup);}}
    @Override public void close()throws IOException {
        synchronized(this){if(closed)return;if(busy || closing)throw new IOException("Restore operation work still active");closing=true;closeRequested=true;}
        try{
            if(service!=null)service.requestRestoreClose();
            if(!controllerClosed){controller.close();controllerClosed=true;}
            if(data!=null && !dataClosed){try{data.owner().close();dataClosed=true;}catch(Exception failure){throw new IOException("Restore data/stream owners have not drained",failure);}}
            if(service!=null){service.finishRestoreOwnership();service.close();}
            synchronized(this){closed=true;}
        }finally{synchronized(this){closing=false;}}
    }
}
