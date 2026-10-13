package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.MergeOperatorRegistry;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.jar.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisSchemaReplicaRpc.*;

/** Actual separate native pools and service work; catalogue authority is controlled, without sockets. */
class RatisSchemaReplicaColdQuotaTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    private static UUID id(int n) {return new UUID(0,n);}
    private static final RatisSchemaReplicaService.Limits POOL_LIMITS=new RatisSchemaReplicaService.Limits(4,16L<<20,192L<<20,4,4,0);
    private static RatisNodeResourceBudget.Limits graphLimits(long bytes) {
        return new RatisNodeResourceBudget.Limits(new RatisNodeResourceBudget.Lane(bytes,32),new RatisNodeResourceBudget.Lane(bytes,32));
    }
    private static RatisSchemaReplicaAdmission admission(int uploads,int tasks,RatisNodeResourceBudget graph) {
        return new RatisSchemaReplicaAdmission(new RatisSchemaReplicaService.Limits(uploads,16L<<20,192L<<20,tasks,4,0),graph);
    }
    private static byte[] artifact() throws Exception {
        var output=new ByteArrayOutputStream();
        try(var jar=new JarOutputStream(output)) {
            var type=RatisSchemaReplicaServiceTest.AppendOperator.class;
            String resource=type.getName().replace('.','/')+".class";
            jar.putNextEntry(new JarEntry(resource));
            try(var input=type.getClassLoader().getResourceAsStream(resource)) {jar.write(Objects.requireNonNull(input).readAllBytes());}
            jar.closeEntry();
            byte[] padding=new byte[6*1024*1024];new Random(70).nextBytes(padding);
            jar.putNextEntry(new JarEntry("padding"));jar.write(padding);jar.closeEntry();
        }
        return output.toByteArray();
    }
    private final class Pool implements AutoCloseable {
        final UUID poolId,group,column;
        final Path path,staging;
        final DistributedStoragePool nativePool;
        final ReplicatedRangeStore store;
        final RWScheduler scheduler=RWScheduler.forTesting(1,1,1,4,1,"schema-cold-quota");
        final DistributedSchema source;
        final RangeDirectory directory;
        final SchemaChange.Definition definition;
        final Target target;
        final OperatorUpload upload;
        final byte[] jar;
        SchemaChange.Status status;
        volatile Runnable beforeAuthority=()->{};
        RatisSchemaReplicaService service;
        Pool(String name,int number,byte[] jar,RatisSchemaReplicaAdmission admission) throws Exception {
            this.jar=jar;poolId=id(number);group=id(number+10);column=id(number+20);path=root.resolve(name).resolve("native");staging=root.resolve(name).resolve("staging");
            UUID domain=id(number+30);
            var local=new ReplicaDescriptor(id(1),id(2),id(number+40),"peer-"+number,"127.0.0.1:"+(10000+number),ReplicaDescriptor.Role.VOTER);
            source=new DistributedSchema(id(3),id(4),domain,0,0,List.of());
            directory=new RangeDirectory(id(3),id(4),domain,0,source.digest(),0,Set.of(),0,List.of(new RangeDescriptor(id(number+50),group,1,1,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(local))));
            var operator=new DistributedSchema.Operator("append",1,RatisSchemaReplicaServiceTest.AppendOperator.class.getName(),OperatorUpload.sha256(jar),1,1,"append-contract",1);
            var shape=ColumnSchema.of(IntArrayList.of(1),ObjectArrayList.of(),true);
            var targetSchema=new DistributedSchema(id(3),id(4),domain,1,0,List.of(new DistributedSchema.Column(number+20,column,"added",new ColumnSchema(shape.keys(),shape.variableTailKeys(),true,operator.name(),operator.version(),operator.className()),operator)));
            definition=new SchemaChange.Definition(id(number+60),new OperationId(id(number+70),1,1),directory,source,targetSchema);
            target=new Target(new Binding(definition,group,"added-physical-"+number,true),local);
            upload=OperatorUpload.create(id(number+80),id(3),id(4),operator,jar);status=SchemaChange.Status.begin(definition.header());
            nativePool=DistributedStoragePool.open(path,ConfigParser.parseDefault(),LoggerFactory.getLogger(getClass()),List.of(source),Map.of(),Map.of());
            store=nativePool.openRange(new RangeOwnership.Owner(group,1),new RangeOwnership.Interval(domain,Set.of(),null,null),source.digest(),Map.of(),Set.of());
            store.enableLogicalSchema(source);if(store.applied()==null)store.applyNoop(new ApplyPosition(1,0));
            start(admission);
        }
        void start(RatisSchemaReplicaAdmission admission) throws Exception {
            service=new RatisSchemaReplicaService(path,staging,nativePool,scheduler,operator->new MergeOperatorRegistry.DeterministicContract("append-contract",1,true),POOL_LIMITS,admission,poolId);
            service.register(group,store,target.replica(),header->{beforeAuthority.run();return new RatisSchemaAuthority.View(new RatisSchemaAuthority.Request(id(3),id(4),directory.domainId(),header,null,false),id(5),new ApplyPosition(1,10),directory,new RatisSchemaAdministration.Head(status,definition.header().targetMaximumApiId()),null);},(requested,current)->{
                if(!requested.equals(target) || !store.ownership().owner().groupId().equals(group) || !Arrays.equals(store.schemaDigest(),source.digest()))throw new IOException("Native source/target identity changed");return 1;
            });
        }
        Request request(int kind) {return new Request(kind,target,kind<=ScopedTransactionRpc.SCHEMA_ARTIFACT_STATE?upload:null,-1,null);}
        CompletableFuture<byte[]> submit(Request request) {return service.submit(group,request.encode());}
        Reply call(Request request) throws Exception {return decodeReply(submit(request).get(10,TimeUnit.SECONDS),request);}
        void upload() throws Exception {
            call(request(ScopedTransactionRpc.SCHEMA_ARTIFACT_BEGIN));
            for(int n=0;n<upload.chunks().size();n++) {
                int offset=n*OperatorUpload.CHUNK_BYTES;
                call(new Request(ScopedTransactionRpc.SCHEMA_ARTIFACT_CHUNK,target,upload,n,Arrays.copyOfRange(jar,offset,Math.min(jar.length,offset+OperatorUpload.CHUNK_BYTES))));
            }
            assertTrue(call(request(ScopedTransactionRpc.SCHEMA_ARTIFACT_FINISH)).artifactReady());
        }
        void abortAndRelease() throws Exception {status=SchemaChange.Status.begin(definition.header()).abort();call(request(ScopedTransactionRpc.SCHEMA_CAPACITY_RELEASE));}
        long directories() throws IOException {try(var paths=Files.list(staging)){return paths.filter(Files::isDirectory).count();}}
        void stop() throws Exception {service.stop().get(10,TimeUnit.SECONDS);}
        public void close() throws Exception {stop();scheduler.disposeGracefully().block(java.time.Duration.ofSeconds(5));nativePool.close();}
    }
    private static long chargedDisk(RatisSchemaReplicaAdmission admission) throws Exception {
        var field=RatisSchemaReplicaAdmission.class.getDeclaredField("disk");field.setAccessible(true);return field.getLong(admission);
    }
    private static int chargedUploads(RatisSchemaReplicaAdmission admission) throws Exception {
        var field=RatisSchemaReplicaAdmission.class.getDeclaredField("uploads");field.setAccessible(true);return ((Map<?,?>)field.get(admission)).size();
    }
    private static long diskCharge(Request begin) {return begin.upload().artifactLength()+2L*begin.encode().length+2L*OperatorUpload.CHUNK_BYTES+4096;}

    @Test void retainedNativeArtifactDeniesOtherPoolAndColdReconstructionKeepsExactChargeUntilCleanup() throws Exception {
        byte[] jar=artifact();var graph=new RatisNodeResourceBudget(graphLimits(256L<<20),Map.of(id(100),graphLimits(192L<<20),id(200),graphLimits(192L<<20)));var aggregate=admission(4,4,graph);
        long expected;
        NativeSchemaCapacityReservation original;
        try(var a=new Pool("a",100,jar,aggregate);var b=new Pool("b",200,jar,aggregate)) {
            a.upload();original=a.call(a.request(ScopedTransactionRpc.SCHEMA_CAPACITY_RESERVE)).capacity();assertNotNull(original);
            expected=diskCharge(a.request(ScopedTransactionRpc.SCHEMA_ARTIFACT_BEGIN));assertEquals(expected,chargedDisk(aggregate));
            assertThrows(ExecutionException.class,()->b.call(b.request(ScopedTransactionRpc.SCHEMA_ARTIFACT_BEGIN)));
            assertEquals(0,b.directories());assertEquals(1,chargedUploads(aggregate));assertEquals(expected,chargedDisk(aggregate));
            assertEquals(CapacityState.NONE,b.call(b.request(ScopedTransactionRpc.SCHEMA_CAPACITY_STATE)).capacityState());
        }
        assertEquals(0,graph.stats().protectedLane().bytes());
        var coldGraph=new RatisNodeResourceBudget(graphLimits(256L<<20),Map.of(id(100),graphLimits(192L<<20),id(200),graphLimits(192L<<20)));var cold=admission(4,4,coldGraph);
        try(var a=new Pool("a",100,jar,cold);var b=new Pool("b",200,jar,cold)) {
            assertTrue(a.call(a.request(ScopedTransactionRpc.SCHEMA_ARTIFACT_STATE)).artifactReady());
            assertEquals(original,a.call(a.request(ScopedTransactionRpc.SCHEMA_CAPACITY_STATE)).capacity());
            assertEquals(expected,chargedDisk(cold));assertEquals(1,chargedUploads(cold));
            assertThrows(ExecutionException.class,()->b.call(b.request(ScopedTransactionRpc.SCHEMA_ARTIFACT_BEGIN)));assertEquals(0,b.directories());
            a.abortAndRelease();assertEquals(0,chargedDisk(cold));assertEquals(0,chargedUploads(cold));
            b.upload();assertTrue(b.call(b.request(ScopedTransactionRpc.SCHEMA_CAPACITY_RESERVE)).capacityActive());
        }
    }

    @Test void canceledBlockedWorkRetainsNodeTaskAndHeapUntilActualServiceDrain() throws Exception {
        byte[] jar=artifact();var graph=new RatisNodeResourceBudget(graphLimits(256L<<20),Map.of(id(100),graphLimits(192L<<20),id(200),graphLimits(192L<<20)));var aggregate=admission(4,1,graph);
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
        try(var a=new Pool("blocked-a",100,jar,aggregate);var b=new Pool("blocked-b",200,jar,aggregate)) {
            a.beforeAuthority=()->{entered.countDown();try{if(!release.await(10,TimeUnit.SECONDS))throw new AssertionError("Authority timeout");}catch(InterruptedException failure){throw new AssertionError(failure);}};
            var observer=a.submit(a.request(ScopedTransactionRpc.SCHEMA_ARTIFACT_BEGIN));CompletableFuture<Void> stopped;
            try {
                assertTrue(entered.await(5,TimeUnit.SECONDS));assertTrue(observer.cancel(false));
                stopped=a.service.stop();assertFalse(stopped.isDone());assertTrue(graph.stats().protectedLane().bytes()>0);
                assertThrows(ExecutionException.class,()->b.call(b.request(ScopedTransactionRpc.SCHEMA_ARTIFACT_BEGIN)));assertEquals(0,b.directories());
            } finally {release.countDown();}
            stopped.get(10,TimeUnit.SECONDS);assertEquals(0,graph.stats().protectedLane().bytes());a.beforeAuthority=()->{};
            a.start(aggregate);a.abortAndRelease();b.call(b.request(ScopedTransactionRpc.SCHEMA_ARTIFACT_BEGIN));assertEquals(1,b.directories());
        }
    }

    @Test void unknownArtifactDeletionDoesNotDonateNodeQuotaAndColdRetryCompletesOriginalCleanup() throws Exception {
        byte[] jar=artifact();var graph=new RatisNodeResourceBudget(graphLimits(256L<<20),Map.of(id(100),graphLimits(192L<<20),id(200),graphLimits(192L<<20)));var aggregate=admission(4,4,graph);
        try(var a=new Pool("unknown-a",100,jar,aggregate);var b=new Pool("unknown-b",200,jar,aggregate)) {
            a.upload();a.stop();long expected=chargedDisk(aggregate);var fail=new AtomicBoolean(true);
            try(var registry=new RatisSchemaArtifactRegistry(a.staging,POOL_LIMITS,path->{
                if(fail.get() && Files.isDirectory(path) && Files.exists(path.resolve("released")))throw new IOException("Unknown release directory force");
                try(var channel=FileChannel.open(path,StandardOpenOption.READ)){channel.force(true);}
            },aggregate,a.poolId)) {
                assertThrows(IOException.class,()->registry.release(a.target));assertEquals(expected,chargedDisk(aggregate));
                assertThrows(ExecutionException.class,()->b.call(b.request(ScopedTransactionRpc.SCHEMA_ARTIFACT_BEGIN)));assertEquals(0,b.directories());
            }
            a.start(aggregate);assertEquals(expected,chargedDisk(aggregate));a.abortAndRelease();
            assertEquals(0,chargedDisk(aggregate));b.upload();assertEquals(1,chargedUploads(aggregate));
        }
    }

    @Test void poolGraphLimitRejectsBeforeAuthorityOrFilesWhileOtherPoolCanCompleteNativePreparation() throws Exception {
        byte[] jar=artifact();var graph=new RatisNodeResourceBudget(graphLimits(256L<<20),Map.of(id(100),graphLimits(64L<<20),id(200),graphLimits(192L<<20)));var aggregate=admission(4,4,graph);
        try(var a=new Pool("small-pool",100,jar,aggregate);var b=new Pool("large-pool",200,jar,aggregate)) {
            a.beforeAuthority=()->{throw new AssertionError("Rejected pool must not reach authority");};
            assertThrows(ExecutionException.class,()->a.call(a.request(ScopedTransactionRpc.SCHEMA_ARTIFACT_BEGIN)));
            assertEquals(0,a.directories());assertEquals(0,chargedDisk(aggregate));assertEquals(0,graph.stats(a.poolId).protectedLane().bytes());
            b.upload();assertTrue(b.call(b.request(ScopedTransactionRpc.SCHEMA_CAPACITY_RESERVE)).capacityActive());
        }
    }
}
