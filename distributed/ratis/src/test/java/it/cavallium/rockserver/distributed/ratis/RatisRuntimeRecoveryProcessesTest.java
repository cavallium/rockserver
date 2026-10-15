package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.*;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.apache.ratis.client.*;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.*;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySeedRuntimeTest.field;

/** Three real Runtime JVMs/native pools/Raft roots over production loopback GRPC; one host, not failure domains. */
public class RatisRuntimeRecoveryProcessesTest {
    private static final UUID CLUSTER=id(1),DATABASE=id(2),DOMAIN=id(3),CAT=id(4),DIRECTORIES=id(5),SESSIONS=id(6),PARENT=id(7),DATA=id(8),POOL=id(9),OPERATIONS=id(10),LEFT=id(11),RIGHT=id(12);
    private static final Duration DEADLINE=Duration.ofSeconds(420);
    @TempDir(factory=RatisDiskTestDirectory.class,cleanup=org.junit.jupiter.api.io.CleanupMode.ON_SUCCESS) Path root;
    private static UUID id(long n){return new UUID(0x72756e74696d6500L,n);}
    private static byte[] bytes(String text){return text.getBytes(StandardCharsets.UTF_8);}
    private static OperationId operation(long sequence){return new OperationId(OPERATIONS,0,sequence);}
    private static OperationId business(SessionEpochLedger.Grant grant,long sequence){return new OperationId(grant.sessionId(),grant.epoch(),sequence);}
    private static DistributedSchema schema(){return new DistributedSchema(CLUSTER,DATABASE,DOMAIN,1,0,List.of(new DistributedSchema.Column(1,DATA,"data",ColumnSchema.of(IntArrayList.of(1),new ObjectArrayList<>(),true),null)));}
    private static List<ReplicaDescriptor> replicas(int[] ports,boolean catalogue){var result=new ArrayList<ReplicaDescriptor>();for(int n=0;n<3;n++)result.add(new ReplicaDescriptor(id(20+n),id(30+n),id((catalogue?40:50)+n),"n"+n,"127.0.0.1:"+ports[n],ReplicaDescriptor.Role.VOTER));return List.copyOf(result);}
    private static RangeDirectory genesis(int[] ports){var schema=schema();return new RangeDirectory(CLUSTER,DATABASE,DOMAIN,schema.schemaEpoch(),schema.digest(),0,schema.columnIds(),0,List.of(new RangeDescriptor(PARENT,PARENT,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,replicas(ports,false))));}
    private static RatisNodePlan.Catalogue catalogue(int[] ports){return new RatisNodePlan.Catalogue(CAT,1,new byte[32],DIRECTORIES,SESSIONS,replicas(ports,true));}
    private static RatisNodePlan plan(int node,int[] ports){var original=RatisPoolFixtures.spec(POOL,false);var images=new NativeRangeImage.Limits(4<<20,16L<<20,4);var pool=new RatisNativePoolSpec(POOL,original.configuration(),4,images,original.retention(),original.pins(),original.replicas(),original.resources(),original.maximumDurableMetadataBytes());var domain=new RatisNodePlan.Domain(genesis(ports),Map.of(DATA,"data"),Set.of());return new RatisNodePlan(id(20+node),id(30+node),List.of(domain),catalogue(ports),List.of(),List.of(),Map.of(),4,List.of(pool),Map.of(DOMAIN,POOL),Map.of(CAT,POOL));}
    private static RatisNodeRuntime.Settings settings(){return new RatisNodeRuntime.Settings(new NativeRangeImage.Limits(4<<20,16L<<20,4),RatisRangeStateMachine.RetentionLimits.defaults(),Duration.ofSeconds(1),4,4,Duration.ofSeconds(15),RatisNodeRuntime.ExportResources.defaults(),RatisNodeRuntime.ResourceLimits.defaults());}
    private static RaftGroup group(UUID group,int[] ports){return RaftGroup.valueOf(RaftGroupId.valueOf(group),replicas(ports,group.equals(CAT)).stream().map(r->RaftPeer.newBuilder().setId(r.peerId()).setAddress(r.address()).build()).toList());}
    private static RaftClient client(UUID group,int[] ports){var properties=new RaftProperties();org.apache.ratis.RaftConfigKeys.Rpc.setType(properties,SupportedRpcType.GRPC);RaftClientConfigKeys.Rpc.setRequestTimeout(properties,TimeDuration.valueOf(3,TimeUnit.SECONDS));return RaftClient.newBuilder().setRaftGroup(group(group,ports)).setProperties(properties).setRetryPolicy(RetryPolicies.retryUpToMaximumCountWithFixedSleep(20,TimeDuration.valueOf(100,TimeUnit.MILLISECONDS))).build();}
    private static TopologyChange.Definition split(int[] ports){var before=genesis(ports);var members=replicas(ports,false);var after=new RangeDirectory(CLUSTER,DATABASE,DOMAIN,before.schemaEpoch(),before.schemaDigest(),1,before.columns(),0,List.of(new RangeDescriptor(LEFT,LEFT,1,0,null,bytes("m"),RangeDescriptor.TopologyState.ACTIVE,members),new RangeDescriptor(RIGHT,RIGHT,1,0,bytes("m"),null,RangeDescriptor.TopologyState.ACTIVE,members)));return new TopologyChange.Definition(id(60),before,after);}

    @Test void allOriginalSourcesFencedRecoverMetadataAndDescendantsAcrossColdQuorumAndLeaderLoss() {
        assertTimeoutPreemptively(DEADLINE,()->{
            int[] ports={port(),port(),port()};var nodes=new Process[3];var runs=new int[3];var sequences=new int[3];
            try {
                launchAll(nodes,runs,ports);awaitAllReady(nodes,runs);
                try(var cat=client(CAT,ports);var routed=RatisRoutedClient.connect(genesis(ports))) {
                    awaitCatalogue(cat);var epochs=new RatisSessionEpochs(cat,CLUSTER,DATABASE,CAT,1,new byte[32],SESSIONS);assertEquals(CommandResult.Status.SUCCESS,epochs.execute(epochs.prepare(operation(0),null,SessionEpochLedger.initial(CLUSTER,DATABASE,10))).status());
                    // Original DATA must observe fresh unissued CAT and elect before directory issuance closes genesis.
                    broadcast(nodes,runs,sequences,"GENESIS");
                    try(var parentClient=client(PARENT,ports)) {
                        var reply=parentClient.io().sendReadOnly(Message.valueOf(ByteString.copyFrom(ScopedTransactionRpc.frame(ScopedTransactionRpc.STATE,new byte[0]))));
                        assertTrue(reply.isSuccess(),"Actual genesis STATE barrier failed: "+reply.getException());
                        assertNotNull(reply.getMessage());
                    }
                    var schemas=new RatisSchemaCatalogue(cat,CLUSTER,DATABASE,CAT,1,new byte[32],DIRECTORIES);assertEquals(CommandResult.Status.SUCCESS,schemas.execute(schemas.prepare(operation(1),schema())).status());var directories=new RatisCatalogue(cat,CLUSTER,DATABASE,CAT,1,new byte[32],DIRECTORIES);var roster=new RatisDatabaseDomainRoster(cat,CLUSTER,DATABASE,catalogue(ports));assertEquals(CommandResult.Status.SUCCESS,directories.execute(directories.prepare(operation(2),null,genesis(ports),roster.read())).status());
                    broadcast(nodes,runs,sequences,"BOOT");
                    var before=epochs.read();var issued=before.issue(OPERATIONS,100_000);var grant=issued.grants().getLast();
                    assertEquals(CommandResult.Status.SUCCESS,epochs.execute(epochs.prepare(operation(3),before,issued)).status());
                    bind(epochs,routed,PARENT,grant);
                    var original=routed.prepare(business(grant,10),List.of(new Mutation(Mutation.Kind.PUT,DATA,bytes("a"),bytes("original"),Mutation.ReturnMode.CURRENT)));byte[] receipt=execute(routed,original).encode();assertArrayEquals(bytes("original"),CommandResult.decode(receipt).value(0));execute(routed,routed.prepare(business(grant,11),List.of(new Mutation(Mutation.Kind.PUT,DATA,bytes("z"),bytes("right")))));
                    var registered=broadcast(nodes,runs,sequences,"PREPARE");for(var status:registered)assertEquals("REGISTER",status.getProperty("phase"));
                    Path journal=Files.createDirectory(root.resolve("topology-controller"));var definition=split(ports);
                    try(var owners=new RatisTopologyOwnerClient(CLUSTER,DATABASE,CAT);var topology=new RatisTopologyController(journal,CLUSTER,DATABASE,catalogue(ports),cat,owners.dataPlane(new RatisTopologyAuthority.Client(cat,CLUSTER,DATABASE,CAT)));var handle=topology.prepare(definition)) {
                        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(90);IOException unresolved=null;while(handle.phase()!=RatisTopologyController.Phase.RETIRED && System.nanoTime()<deadline){try{topology.advance(handle,8);}catch(IOException failure){unresolved=failure;Thread.sleep(50);}}assertEquals(RatisTopologyController.Phase.RETIRED,handle.phase(),"Original retained topology intent did not settle: "+unresolved);
                    }
                    routed.refresh(definition.targetDirectory());var fenced=broadcast(nodes,runs,sequences,"FENCE");for(var status:fenced){assertEquals("CLEANUP_OLD",status.getProperty("phase"));assertEquals("true",status.getProperty("fenced"));assertEquals("MISSING",status.getProperty("mode"));assertEquals("false",status.getProperty("localReady"));assertEquals("true",status.getProperty("noUser"));}
                    killAll(nodes);for(int n=0;n<3;n++){runs[n]++;sequences[n]=0;}launchAll(nodes,runs,ports);awaitAllReady(nodes,runs);
                    // Every peer starts its recovery before we wait for any peer's HEAD/quorum-dependent work.
                    broadcast(nodes,runs,sequences,"DRIVE");awaitRecovered(nodes,runs,sequences,fenced);assertArrayEquals(bytes("original"),read(routed,"a"));assertArrayEquals(bytes("right"),read(routed,"z"));assertArrayEquals(receipt,execute(routed,original).encode());
                    bind(epochs,routed,LEFT,grant);bind(epochs,routed,RIGHT,grant);
                    var durable=routed.prepare(business(grant,12),List.of(new Mutation(Mutation.Kind.PUT,DATA,bytes("b"),bytes("descendant"),Mutation.ReturnMode.CURRENT)));byte[] durableReceipt=execute(routed,durable).encode();assertArrayEquals(bytes("descendant"),read(routed,"b"));
                    int leader;try(var left=client(LEFT,ports)){leader=leader(left,LEFT);}nodes[leader].destroyForcibly();assertTrue(nodes[leader].waitFor(10,TimeUnit.SECONDS));assertArrayEquals(durableReceipt,execute(routed,durable).encode());var survivor=routed.prepare(business(grant,13),List.of(new Mutation(Mutation.Kind.PUT,DATA,bytes("c"),bytes("survivor"))));assertEquals(CommandResult.Status.SUCCESS,execute(routed,survivor).status());assertArrayEquals(bytes("survivor"),read(routed,"c"));
                    runs[leader]++;sequences[leader]=0;nodes[leader]=launch(leader,runs[leader],ports);awaitReady(nodes[leader],leader,runs[leader]);send(leader,runs,sequences,"DRIVE");awaitReply(nodes[leader],leader,runs[leader],sequences[leader],Duration.ofSeconds(20));
                    long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(90);boolean caught=false;Properties lastStatus=null;while(!caught && System.nanoTime()<deadline){send(leader,runs,sequences,"STATUS");var status=awaitReply(nodes[leader],leader,runs[leader],sequences[leader],Duration.ofSeconds(20));lastStatus=status;caught="true".equals(status.getProperty("done"));if(caught)assertRecovered(status,fenced.get(leader));else Thread.sleep(50);}if(!caught){var details=new StringBuilder("Restarted Runtime did not complete cold recovery: ").append(lastStatus);for(int n=0;n<3;n++)if(n!=leader){send(n,runs,sequences,"STATUS");details.append("; survivor ").append(n).append('=').append(awaitReply(nodes[n],n,runs[n],sequences[n],Duration.ofSeconds(20)));}throw failure(leader,runs[leader],details.toString());}
                    try(var left=client(LEFT,ports)){var query=Message.valueOf(ByteString.copyFrom(ScopedTransactionRpc.pointRequest(DATA,bytes("c"))));long deadline2=System.nanoTime()+TimeUnit.SECONDS.toNanos(30);boolean applied=false;while(!applied && System.nanoTime()<deadline2){try{var reply=left.io().sendStaleRead(query,0,RaftPeerId.valueOf("n"+leader));applied=reply.isSuccess() && Arrays.equals(bytes("survivor"),ScopedTransactionRpc.pointValue(reply.getMessage().getContent().toByteArray(),1024));}catch(IOException transientFailure){}if(!applied)Thread.sleep(50);}assertTrue(applied,"Restarted voter did not catch up through its actual updater");}
                    assertArrayEquals(receipt,execute(routed,original).encode());assertArrayEquals(durableReceipt,execute(routed,durable).encode());
                }
            }finally{killAll(nodes);}
        });
    }
    private void launchAll(Process[] nodes,int[] runs,int[] ports)throws Exception{for(int n=0;n<3;n++)nodes[n]=launch(n,runs[n],ports);}
    private Process launch(int node,int run,int[] ports)throws Exception{Path dir=Files.createDirectories(root.resolve("node"+node));var args=new ArrayList<>(List.of(Path.of(System.getProperty("java.home"),"bin/java").toString(),"--enable-native-access=ALL-UNNAMED","-XX:ActiveProcessorCount=4","-Dorg.slf4j.simpleLogger.defaultLogLevel=warn","-Dorg.slf4j.simpleLogger.log.org.apache.ratis.server.leader.LogAppenderDaemon=info","-cp",System.getProperty("java.class.path"),getClass().getName(),dir.toString(),Integer.toString(node),Integer.toString(run)));for(int port:ports)args.add(Integer.toString(port));return new ProcessBuilder(args).redirectErrorStream(true).redirectOutput(dir.resolve("process-"+run+".log").toFile()).start();}
    private void awaitAllReady(Process[] nodes,int[] runs)throws Exception{for(int n=0;n<3;n++)awaitReady(nodes[n],n,runs[n]);}
    private void awaitReady(Process process,int node,int run)throws Exception{Path ready=root.resolve("node"+node).resolve("ready-"+run);long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(30);while(process.isAlive() && !Files.exists(ready) && System.nanoTime()<deadline)Thread.sleep(20);if(!process.isAlive() || !Files.exists(ready))throw failure(node,run,"Runtime startup failed");}
    private static void killAll(Process[] nodes)throws InterruptedException{for(var process:nodes)if(process!=null)process.destroyForcibly();for(var process:nodes)if(process!=null)process.waitFor(10,TimeUnit.SECONDS);}
    private void send(int node,int[] runs,int[] sequences,String command)throws IOException{Path target=root.resolve("node"+node).resolve("command-"+runs[node]+"-"+(++sequences[node]));Path staging=target.resolveSibling(target.getFileName()+".pending");Files.writeString(staging,command);Files.move(staging,target,StandardCopyOption.ATOMIC_MOVE);}
    private List<Properties> broadcast(Process[] nodes,int[] runs,int[] sequences,String command)throws Exception{for(int n=0;n<3;n++)send(n,runs,sequences,command);var result=new ArrayList<Properties>();for(int n=0;n<3;n++)result.add(awaitReply(nodes[n],n,runs[n],sequences[n],Duration.ofSeconds(90)));return result;}
    private Properties awaitReply(Process process,int node,int run,int sequence,Duration timeout)throws Exception{Path reply=root.resolve("node"+node).resolve("reply-"+run+"-"+sequence);long deadline=System.nanoTime()+timeout.toNanos();while(process.isAlive() && !Files.exists(reply) && System.nanoTime()<deadline)Thread.sleep(20);if(!process.isAlive() || !Files.exists(reply))throw failure(node,run,"Child did not finish command "+sequence);var values=new Properties();try(var input=Files.newInputStream(reply)){values.load(input);}if(!"OK".equals(values.getProperty("result")))throw failure(node,run,values.getProperty("error","Unknown child failure"));return values;}
    private AssertionError failure(int node,int run,String message)throws IOException{Path log=root.resolve("node"+node).resolve("process-"+run+".log");String text=Files.exists(log)?Files.readString(log):"(missing log)";return new AssertionError(message+"; diagnostics retained at "+log+"\n"+text.substring(Math.max(0,text.length()-65536)));}
    private void awaitRecovered(Process[] nodes,int[] runs,int[] sequences,List<Properties> fenced)throws Exception{long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(150);var done=new boolean[3];while(System.nanoTime()<deadline){boolean all=true;for(int n=0;n<3;n++){if(!done[n])send(n,runs,sequences,"STATUS");}for(int n=0;n<3;n++)if(!done[n]){var status=awaitReply(nodes[n],n,runs[n],sequences[n],Duration.ofSeconds(20));if("true".equals(status.getProperty("done"))){assertRecovered(status,fenced.get(n));done[n]=true;}else all=false;}if(all)return;Thread.sleep(50);}throw new AssertionError("Concurrent Runtime recovery did not settle; logs under "+root);}
    private static void assertRecovered(Properties status,Properties fenced) {
        assertEquals("LOCAL_DONE",status.getProperty("phase"));
        assertEquals("true",status.getProperty("localReady"));
        assertEquals("RECOVERY",status.getProperty("mode"));
        assertEquals("true",status.getProperty("noUser"));
        assertEquals("true",status.getProperty("fenced"));
        assertEquals("1",status.getProperty("aliases"));
        assertEquals("3",status.getProperty("dispositions"));
        assertEquals("3",status.getProperty("obsolete"));
        assertEquals("4",status.getProperty("groups"));
        assertEquals("0",status.getProperty("sourceLeases"));
        assertEquals("0",status.getProperty("pins"));
        assertEquals("LOCAL_SEED",status.getProperty("originKind"));
        assertEquals(fenced.getProperty("fenceDigest"),status.getProperty("fenceDigest"));
        assertEquals(fenced.getProperty("seedDigest"),status.getProperty("originDigest"));
    }
    private static CommandResult execute(RatisRoutedClient routed,RatisRoutedClient.PreparedCommand command)throws Exception{long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(45);IOException last=null;do{try{var result=routed.execute(command);assertEquals(CommandResult.Status.SUCCESS,result.status());return result;}catch(IOException failure){if(last==null)failure.printStackTrace();last=failure;Thread.sleep(50);}}while(System.nanoTime()<deadline);throw new AssertionError("Original immutable command did not settle",last);}
    private static byte[] read(RatisRoutedClient routed,String key)throws Exception{long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(45);IOException last=null;do{try{return routed.read(DATA,bytes(key));}catch(IOException failure){last=failure;Thread.sleep(50);}}while(System.nanoTime()<deadline);throw new AssertionError("DATA read did not settle",last);}
    private static void awaitCatalogue(RaftClient client)throws Exception{long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(30);do{try{new RatisCatalogue(client,CLUSTER,DATABASE,CAT,1,new byte[32],DIRECTORIES).readView(DOMAIN);return;}catch(IOException retry){Thread.sleep(50);}}while(System.nanoTime()<deadline);throw new AssertionError("CAT did not elect/apply");}
    private static int leader(RaftClient client,UUID group)throws Exception{long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(30);do{for(int n=0;n<3;n++)try{var info=client.getGroupManagementApi(RaftPeerId.valueOf("n"+n)).info(RaftGroupId.valueOf(group));if(info.isSuccess() && info.getRoleInfoProto().getRole()==RaftPeerRole.LEADER)return n;}catch(IOException ignored){}Thread.sleep(50);}while(System.nanoTime()<deadline);throw new AssertionError("No actual DATA leader");}
    private static int port()throws IOException{try(var socket=new ServerSocket(0)){return socket.getLocalPort();}}

    /** Scalar file commands coordinate phases only; every data/authority/cleanup operation uses production Runtime/Ratis. */
    public static void main(String[] args)throws Exception {
        org.rocksdb.RocksDB.loadLibrary();
        Path dir=Path.of(args[0]);int me=Integer.parseInt(args[1]),run=Integer.parseInt(args[2]);int[] ports=Arrays.stream(args).skip(3).mapToInt(Integer::parseInt).toArray();var plan=plan(me,ports);var settings=settings();byte[] manifest=RatisNodeManifest.encode(new RatisNodeManifest(List.of(schema()),plan,settings,3,3));Path manifestFile=dir.resolve("manifest");if(Files.exists(manifestFile)){if(!Arrays.equals(manifest,Files.readAllBytes(manifestFile)))throw new IOException("Child changed its retained Runtime manifest");}else {Files.write(manifestFile,manifest);SessionJournalFiles.force(manifestFile);SessionJournalFiles.force(dir);}
        var timer=new RatisNodeRuntimeTest.ManualTimer();var scheduler=new RWScheduler(4,4,"runtime-recovery-process");
        try(var cat=client(CAT,ports);var route=RatisRoutedClient.connect(genesis(ports));var tasks=Executors.newSingleThreadExecutor();var maintenance=Executors.newSingleThreadScheduledExecutor()) {
            var epochs=new RatisSessionEpochs(cat,CLUSTER,DATABASE,CAT,1,new byte[32],SESSIONS);Path nodeRoot=Files.createDirectories(dir.resolve("runtime"));SessionJournalFiles.force(nodeRoot);SessionJournalFiles.force(dir);var startup=Files.exists(nodeRoot.resolve("node.identity"))?RatisNodeRuntime.Startup.RECOVER:RatisNodeRuntime.Startup.AUTHORIZED_GENESIS;
            try(var node=RatisNodeRuntime.open(plan,nodeRoot,ConfigParser.parseDefault(),epochs,Map.of(DOMAIN,route),scheduler,timer,()->20L,settings,startup,List.of(schema()))) {
                try {
                node.start();var census=field(node,"diskCensus",RatisDiskRunwayService.class);var pressure=field(node,"poolPressure",RatisPoolPressureService.class);census.initialCensus();pressure.refresh().get(5,TimeUnit.SECONDS);
                // Only real filesystem/pressure observations run periodically; recovery phases remain caller-driven.
                maintenance.scheduleWithFixedDelay(()->{try{for(int quantum=0;quantum<4;quantum++)census.refresh().get(5,TimeUnit.SECONDS);pressure.refresh().get(5,TimeUnit.SECONDS);}catch(Exception failure){failure.printStackTrace();}},1,1,TimeUnit.SECONDS);
                Files.writeString(dir.resolve("ready-"+run),"ready");CompletableFuture<Void> driving=null;var lastRecoveryFailure=new java.util.concurrent.atomic.AtomicReference<Throwable>();var reportedFailures=new HashSet<String>();
                for(int sequence=1;;sequence++) {
                    Path command=dir.resolve("command-"+run+"-"+sequence);while(!Files.exists(command))Thread.sleep(10);var reply=new Properties();
                    try {
                        switch(Files.readString(command).trim()) {
                            case "GENESIS"->awaitGenesis(node);
                            case "BOOT"->{long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(60);Throwable last=null;while(!parent(node).admissionOpen() && System.nanoTime()<deadline){try{node.bootstrapDeferredRecovery().get(15,TimeUnit.SECONDS);}catch(ExecutionException failure){last=failure.getCause();}Thread.sleep(20);}if(!parent(node).admissionOpen())throw new IOException("Genesis did not activate",last);}
                            case "PREPARE"->{var graph=field(node,"recoveryGraph",RatisConfiguredRecoveryGraph.class).snapshot();var request=graph.activeRequests().get(PARENT);if(request==null || request.metadataOnly())throw new IOException("Genesis lost frozen CURRENT selection");node.recoverConfigured(request).get(15,TimeUnit.SECONDS);long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(45);Throwable last=null;while(node.configuredRecoveryPhase(PARENT)==RatisConfiguredRecovery.Phase.PREPARE && System.nanoTime()<deadline){try{manager(node).step().get(20,TimeUnit.SECONDS);}catch(ExecutionException retry){last=retry.getCause();}Thread.sleep(20);}if(node.configuredRecoveryPhase(PARENT)!=RatisConfiguredRecovery.Phase.REGISTER)throw new IOException("Source PREPARE did not retain REGISTER job",last);}
                            case "FENCE"->awaitFence(node);
                            case "DRIVE"->{if(driving!=null)throw new IOException("Recovery already running");driving=CompletableFuture.runAsync(()->{try{drive(node,failure->{lastRecoveryFailure.set(failure);if(reportedFailures.size()<16 && reportedFailures.add(failure.toString()))failure.printStackTrace();});}catch(Exception failure){throw new CompletionException(failure);}},tasks);}
                            case "STATUS"->{}
                            default->throw new IOException("Unknown child phase command");
                        }
                        if(driving!=null && driving.isCompletedExceptionally())driving.join();reply.putAll(status(node));if(lastRecoveryFailure.get()!=null)reply.setProperty("lastRecoveryFailure",lastRecoveryFailure.get().toString());boolean done=driving!=null && driving.isDone();reply.setProperty("done",Boolean.toString(done));if(done)cleanupStatus(node,reply);reply.setProperty("result","OK");
                    }catch(Throwable failure){failure.printStackTrace();reply.setProperty("result","ERROR");reply.setProperty("error",failure.toString());}
                    Path output=dir.resolve("reply-"+run+"-"+sequence),temporary=output.resolveSibling(output.getFileName()+".pending");try(var stream=Files.newOutputStream(temporary)){reply.store(stream,"Runtime phase");}Files.move(temporary,output,StandardCopyOption.ATOMIC_MOVE);
                }
                }finally{maintenance.shutdownNow();maintenance.awaitTermination(10,TimeUnit.SECONDS);}
            }
        }finally{timer.dispose();scheduler.disposeGracefully().block(Duration.ofSeconds(15));}
    }
    private void bind(RatisSessionEpochs epochs,RatisRoutedClient routed,UUID group,SessionEpochLedger.Grant grant)throws Exception {
        try(var binding=new RatisSessionMaintenance(root.resolve("business-session-"+group),epochs,routed,group)) {
            binding.prepareBinding(grant,32);
            long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(30);IOException last=null;boolean bound=false;
            while(!bound && System.nanoTime()<deadline){try{assertEquals(CommandResult.Status.SUCCESS,binding.execute().status());bound=true;}catch(IOException failure){last=failure;Thread.sleep(50);}}
            assertTrue(bound,"Actual catalogue grant was not bound: "+last);
        }
    }
    private static void awaitFence(RatisNodeRuntime node)throws Exception {
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(60);Throwable last=null;
        while(node.configuredRecoveryPhase(PARENT)!=RatisConfiguredRecovery.Phase.CLEANUP_OLD && System.nanoTime()<deadline) {
            try {
                node.bootstrapDeferredRecovery().get(20,TimeUnit.SECONDS);
                for(int steps=0;steps<8 && node.configuredRecoveryPhase(PARENT)!=RatisConfiguredRecovery.Phase.CLEANUP_OLD;steps++)manager(node).step().get(20,TimeUnit.SECONDS);
            }catch(ExecutionException retry){last=retry.getCause();}
            Thread.sleep(20);
        }
        if(node.configuredRecoveryPhase(PARENT)!=RatisConfiguredRecovery.Phase.CLEANUP_OLD)throw new IOException("Receiver did not stop at cleanup boundary",last);
        if(parent(node).acknowledgedRecoverySupersession()==null || parent(node).localMetadataDiscoveryActive())throw new IOException("Receiver fence was not durable and uncompleted");
    }
    private static void awaitGenesis(RatisNodeRuntime node)throws Exception {
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(60);Throwable last=null;
        var machine=(RatisRangeStateMachine)field(node,"machines",Map.class).get(RaftGroupId.valueOf(PARENT));
        while(System.nanoTime()<deadline) {
            try{node.bootstrapDeferredRecovery().get(15,TimeUnit.SECONDS);}catch(ExecutionException failure){last=failure.getCause();}
            var applied=parent(node).applied();
            var division=machine.getServer().join().getDivision(machine.getGroupId());
            if(machine.genesisActive() && machine.recoveryEligibilityGate().snapshot().mode()==org.apache.ratis.server.impl.RecoveryEligibilityStateMachine.Mode.GENESIS
                    && applied!=null && applied.term()>0 && applied.term()==division.getInfo().getCurrentTerm()
                    && division.getRaftLog().getLastCommittedIndex()>=applied.index()
                    && RatisNodeGenesisBootstrapTest.actualUpdaterIndex(division)>=applied.index() && !parent(node).admissionOpen())return;
            Thread.sleep(20);
        }
        throw new IOException("Original genesis did not elect/apply metadata before CAT issuance",last);
    }
    private static RatisConfiguredRecoveries manager(RatisNodeRuntime node)throws Exception{return field(node,"configuredRecoveries",RatisConfiguredRecoveries.class);}
    private static ReplicatedRangeStore parent(RatisNodeRuntime node)throws Exception{return field(field(node,"machines",Map.class).get(RaftGroupId.valueOf(PARENT)),"store",ReplicatedRangeStore.class);}
    private static Properties status(RatisNodeRuntime node)throws Exception{var result=new Properties();var store=parent(node);var machine=(RatisRangeStateMachine)field(node,"machines",Map.class).get(RaftGroupId.valueOf(PARENT));result.setProperty("phase",String.valueOf(node.configuredRecoveryPhase(PARENT)));result.setProperty("fenced",Boolean.toString(store.acknowledgedRecoverySupersession()!=null));result.setProperty("noUser",Boolean.toString(store.userHandlesDetached()));result.setProperty("localReady",Boolean.toString(machine.localMetadataCompletionReady()));result.setProperty("mode",machine.recoveryEligibilityGate().snapshot().mode().name());metadataStatus(machine,store,result);result.setProperty("groups",Integer.toString(node.localGroups().size()));result.setProperty("pins",Integer.toString(store.activeSnapshotPins()));var owner=field(node,"pools",Map.class).get(POOL);result.setProperty("sourceLeases",Integer.toString(field(owner,"sources",RatisTopologySourceSnapshotService.class).stats().leases()));var fence=store.acknowledgedRecoverySupersession();if(fence!=null){result.setProperty("fenceDigest",HexFormat.of().formatHex(fence.digest()));try(var forbidden=store.capture()){throw new IOException("Retired USER capture was admitted");}catch(IllegalStateException expected){}try{store.readCurrentPoint(DATA,bytes("a"));throw new IOException("Retired USER read was admitted");}catch(IllegalStateException expected){}}var seed=node.nativePool(POOL).topologySeed(id(60));if(seed!=null)result.setProperty("seedDigest",HexFormat.of().formatHex(seed.digest()));return result;}
    private static void metadataStatus(RatisRangeStateMachine machine,ReplicatedRangeStore store,Properties result)throws Exception {
        result.setProperty("nativeApplied",String.valueOf(store.applied()));
        result.setProperty("smApplied",String.valueOf(machine.getLastAppliedTermIndex()));
        var cfg=store.configuration();if(cfg!=null){result.setProperty("nativeCfg",cfg.term()+"/"+cfg.index());result.setProperty("nativeCfgHash",HexFormat.of().formatHex(OperatorUpload.sha256(cfg.data())));}
        var division=machine.getServer().join().getDivision(machine.getGroupId());var log=division.getRaftLog();
        result.setProperty("coreApplied",Long.toString(RatisNodeGenesisBootstrapTest.actualUpdaterIndex(division)));
        result.setProperty("logSnapshot",Long.toString(log.getSnapshotIndex()));result.setProperty("logNext",Long.toString(log.getNextIndex()));result.setProperty("logCommitted",Long.toString(log.getLastCommittedIndex()));
        result.setProperty("actualCfgIndex",Long.toString(division.getRaftConf().getLogEntryIndex()));
        result.setProperty("actualCfgHash",HexFormat.of().formatHex(OperatorUpload.sha256(org.apache.ratis.server.raftlog.LogProtoUtils.toRaftConfigurationProtoBuilder(division.getRaftConf()).build().toByteArray())));
        result.setProperty("leader",String.valueOf(division.getInfo().getLeaderId()));result.setProperty("term",Long.toString(division.getInfo().getCurrentTerm()));
        var ready=division.getClass().getDeclaredMethod("isInitialImageReady");ready.setAccessible(true);result.setProperty("initialImageReady",String.valueOf(ready.invoke(division)));
        result.setProperty("discoveryPending",Boolean.toString(machine.recoveryEligibilityGate().discoveryPending()));
        if(machine.localMetadataBaselineReady())result.setProperty("localBaseline",machine.localMetadataImageCut().toString());
    }
    private static void drive(RatisNodeRuntime node,java.util.function.Consumer<Throwable> retryFailure)throws Exception{long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(140);Throwable last=null;while(System.nanoTime()<deadline){try{node.bootstrapDeferredRecovery().get(20,TimeUnit.SECONDS);manager(node).step().get(20,TimeUnit.SECONDS);boolean children=true;for(UUID group:List.of(LEFT,RIGHT)){var machine=(RatisRangeStateMachine)field(node,"machines",Map.class).get(RaftGroupId.valueOf(group));children&=machine!=null && field(machine,"store",ReplicatedRangeStore.class).admissionOpen();}if(node.configuredRecoveryPhase(PARENT)==RatisConfiguredRecovery.Phase.LOCAL_DONE && children)return;}catch(ExecutionException failure){last=failure.getCause();retryFailure.accept(last);}Thread.sleep(20);}throw new IOException("Local META/descendant recovery deadline expired",last);}
    private static void cleanupStatus(RatisNodeRuntime node,Properties output)throws Exception{var resources=field(node,"resources",RatisNodeResourceBudget.class);try(var credit=resources.reserve(POOL,RatisNodeResourceBudget.Kind.PROTECTED,4096,1)){manager(node).discover(()->{var retained=manager(node).retainedOnIo(PARENT,bytes->credit.growTo(Math.max(credit.bytes(),4096L+16L*bytes)));if(retained==null || retained.localCompletion()==null || retained.witness()==null)throw new IOException("LOCAL_DONE lacks actual completion/cleanup ownership");output.setProperty("originKind",retained.localCompletion().originKind().name());output.setProperty("originDigest",HexFormat.of().formatHex(retained.localCompletion().originDigest()));output.setProperty("aliases",Integer.toString(retained.witness().aliases().size()));output.setProperty("dispositions",Integer.toString(retained.witness().dispositions().size()));output.setProperty("obsolete",Long.toString(retained.witness().dispositions().stream().filter(value->value==RatisConfiguredRecovery.CleanupDisposition.OBSOLETE).count()));return null;}).get(20,TimeUnit.SECONDS);}}
}
