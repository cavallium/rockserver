package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.*;
import java.nio.file.*;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisExportSourceRpc.*;

/** Actual two-pool native IO and source SSTs; all Ratis servers/transport remain controlled. */
class NativeExtentNodeTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final UUID firstPool=new UUID(970,1),secondPool=new UUID(970,2);
    RatisNodePlan plan(RatisNodeMultiDomainTest.Fixture fixture){return new RatisNodePlan(fixture.plan.nodeId(),fixture.plan.nodeIncarnation(),fixture.plan.domains(),fixture.plan.catalogue(),List.of(),List.of(),Map.of(),8,List.of(RatisPoolFixtures.spec(firstPool,false),RatisPoolFixtures.spec(secondPool,false)),Map.of(fixture.first.domainId(),firstPool,fixture.second.domainId(),secondPool),Map.of(fixture.plan.catalogue().groupId(),firstPool));}
    RatisNodeRuntime.Settings settings(RatisNodeMultiDomainTest.Fixture fixture,RatisNativeExtentPolicy policy){var s=fixture.settings();var r=s.resources();return new RatisNodeRuntime.Settings(s.images(),s.retention(),s.clockPeriod(),s.propagationVisits(),s.maxPropagationRanges(),s.shutdownTimeout(),s.exports(),new RatisNodeRuntime.ResourceLimits(r.node(),r.pool(),r.maximumSourceImageDiskBytes(),r.maximumDurableMetadataBytes(),r.maximumOperatorCacheBytes(),r.maximumNativeCacheBytes(),r.committedApply(),r.diskRunway(),r.migration(),policy));}
    RatisNodeRuntime open(RatisNodeMultiDomainTest.Fixture fixture,RatisNodePlan plan,RatisNodeRuntimeTest.FakeServer server,RatisNodeRuntime.Startup startup,RatisNativeExtentPolicy policy)throws IOException{return RatisNodeRuntime.open(plan,fixture.env.nodeRoot,ConfigParser.parseDefault(),fixture.env.catalogue.epochs,fixture.clients,fixture.env.scheduler,fixture.env.timer,()->20L,settings(fixture,policy),startup,server::create,fixture.schemas);}
    UUID group(RatisNodePlan plan,DistributedSchema schema){return plan.directory(schema.domainId()).ranges().getFirst().groupId();}
    void put(ReplicatedRangeStore store,DistributedSchema schema,long sequence,byte key,byte[] value)throws Exception{var command=new DistributedCommand(store.ownership().owner().groupId(),store.ownership().owner().epoch(),schema.digest(),new OperationId(new UUID(978,1),1,sequence),List.of(new Mutation(Mutation.Kind.PUT,schema.columnIds().iterator().next(),new byte[]{key},value)));assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(store.apply(new ApplyPosition(1,store.applied().index()+1),command)).status());}
    static org.rocksdb.NativeExtentEnv nativeEnv(RatisNodeRuntime node)throws Exception{return RatisTopologySeedRuntimeTest.field(node,"nativeExtentEnv",org.rocksdb.NativeExtentEnv.class);}
    @Test void actualTwoPoolPutFlushColdReopenRetainsOriginalSharedEnvironment()throws Exception {
        var helper=new RatisNodeMultiDomainTest();helper.root=root;try(var f=helper.new Fixture()){
            var plan=plan(f);var policy=RatisNativeExtentPolicy.defaults();var server=new RatisNodeRuntimeTest.FakeServer();
            try(var node=open(f,plan,server,RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,policy)){node.start();assertTrue(nativeEnv(node).isOwningHandle());
                for(var schema:f.schemas){var store=f.store(server,group(plan,schema));put(store,schema,0,(byte)1,new byte[]{(byte)(schema==f.first?7:9)});var pool=node.nativePool(node.poolId(group(plan,schema)));pool.flushLocal(pool.maintenanceSession(),schema.columnIds().iterator().next());}
                assertEquals(2,SessionJournalFiles.read(f.env.nodeRoot.resolve("node.identity"),RatisNodeIdentity.MAX_BYTES)[0]);assertTrue(Files.isRegularFile(f.env.nodeRoot.resolve("native-extents.journal")));
            }
            server=new RatisNodeRuntimeTest.FakeServer();try(var node=open(f,plan,server,RatisNodeRuntime.Startup.RECOVER,policy)){node.start();for(var schema:f.schemas)assertArrayEquals(new byte[]{(byte)(schema==f.first?7:9)},f.store(server,group(plan,schema)).readCurrentPoint(schema.columnIds().iterator().next(),new byte[]{1}));}
        }
    }
    @Test void sharedNodeCeilingRejectsNativeGrowthAcrossBothPoolsBeforeDeniedWriteEffect()throws Exception {
        var helper=new RatisNodeMultiDomainTest();helper.root=root;try(var f=helper.new Fixture()){
            var original=plan(f);var compact=new ArrayList<RatisNativePoolSpec>();for(var spec:original.pools().values())compact.add(new RatisNativePoolSpec(spec.id(),"database.global.database-write-buffer-size=64KiB".getBytes(java.nio.charset.StandardCharsets.UTF_8),spec.maximumGroups(),spec.images(),spec.retention(),spec.pins(),spec.replicas(),spec.resources(),spec.maximumDurableMetadataBytes()));
            var plan=new RatisNodePlan(original.nodeId(),original.nodeIncarnation(),original.domains(),original.catalogue(),original.homes(),original.imports(),original.streamAddresses(),original.maxLocalGroups(),compact,original.domainPools(),original.groupPools());
            // RocksDB clamps WAL preallocation to db_write_buffer_size. Keep this fixture's
            // bootstrap small, then cross a finite shared ceiling with distinct native payloads.
            for(var spec:compact){assertEquals(64L<<10,spec.config().global().databaseWriteBufferSize().longValue());}
            var defaults=RatisNativeExtentPolicy.defaults();long ceiling=16L<<20;
            var policy=new RatisNativeExtentPolicy(defaults.filesystemOrdinaryBytes(),defaults.filesystemProtectedBytes(),ceiling,defaults.nodeProtectedBytes(),defaults.quantum(),defaults.maximumFiles(),defaults.maximumAliases(),defaults.maximumStateBytes(),defaults.raw());
            var server=new RatisNodeRuntimeTest.FakeServer();boolean denied=false;var successful=new HashSet<UUID>();
            org.rocksdb.NativeExtentEnv.Usage usage;
            try(var node=open(f,plan,server,RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,policy)){node.start();
                for(int n=0;n<128 && !denied;n++){
                    var schema=n%2==0?f.first:f.second;var store=f.store(server,group(plan,schema));var before=store.applied();var value=new byte[1<<20];new Random(n+1).nextBytes(value);byte key=(byte)(n/2+1);boolean applied=false;
                    try{put(store,schema,n/2,key,value);applied=true;successful.add(node.poolId(group(plan,schema)));var pool=node.nativePool(node.poolId(group(plan,schema)));pool.flushLocal(pool.maintenanceSession(),schema.columnIds().iterator().next());}
                    catch(org.rocksdb.RocksDBException failure){assertEquals(org.rocksdb.Status.SubCode.NoSpace,failure.getStatus().getSubCode(),failure::toString);if(!applied){assertEquals(before,store.applied());assertNull(store.readCurrentPoint(schema.columnIds().iterator().next(),new byte[]{key}));denied=true;}}
                }
                assertTrue(denied,"The aggregate native node ceiling must reject growth, not independently reset per pool");assertEquals(Set.of(firstPool,secondPool),successful);
                usage=nativeEnv(node).usage();
            }
            long observed=0;for(var spec:plan.pools().values())try(var files=Files.walk(spec.nativePath(f.env.nodeRoot))){for(var file:files.filter(path->Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS)).toList()){long size=Files.size(file);observed+=((size+policy.quantum()-1)/policy.quantum())*policy.quantum();}}
            assertTrue(usage.ordinaryBytes()<=ceiling,"Ordinary native requests exceed the shared ceiling");
            assertTrue(usage.protectedBytes()<=policy.nodeProtectedBytes(),"Protected native requests exceed their reserved ceiling");
            assertTrue(observed<=Math.addExact(usage.ordinaryBytes(),usage.protectedBytes()),"Observed rounded native files exceed admitted requests across both lanes");
        }
    }
    @Test void changedLimitsMissingOriginalJournalAndReplacedManagedRootRefuseBeforePoolEffects()throws Exception {
        var helper=new RatisNodeMultiDomainTest();helper.root=root;try(var f=helper.new Fixture()){
            var plan=plan(f);var policy=RatisNativeExtentPolicy.defaults();try(var node=open(f,plan,new RatisNodeRuntimeTest.FakeServer(),RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,policy)){node.start();}
            var changed=new RatisNativeExtentPolicy(policy.filesystemOrdinaryBytes(),policy.filesystemProtectedBytes(),policy.nodeOrdinaryBytes()-policy.quantum(),policy.nodeProtectedBytes(),policy.quantum(),policy.maximumFiles(),policy.maximumAliases(),policy.maximumStateBytes(),policy.raw());
            var denied=new RatisNodeRuntimeTest.FakeServer();assertThrows(IOException.class,()->open(f,plan,denied,RatisNodeRuntime.Startup.RECOVER,changed));assertFalse(denied.created);
            var raw=policy.raw();
            var changedRaw=new RatisNativeExtentPolicy(policy.filesystemOrdinaryBytes(),policy.filesystemProtectedBytes(),policy.nodeOrdinaryBytes(),policy.nodeProtectedBytes(),policy.quantum(),policy.maximumFiles(),policy.maximumAliases(),policy.maximumStateBytes(),new org.rocksdb.NativeRawFlushLimits.Limits(raw.maximumPointEntries(),raw.maximumRangeEntries(),raw.maximumInputBytes(),raw.maximumOutputBytes(),raw.maximumWorkingBytes()-1,raw.maximumReplayBatches(),raw.maximumReplayEntries()));
            var rawDenied=new RatisNodeRuntimeTest.FakeServer();assertThrows(IOException.class,()->open(f,plan,rawDenied,RatisNodeRuntime.Startup.RECOVER,changedRaw));assertFalse(rawDenied.created);
            var journal=f.env.nodeRoot.resolve("native-extents.journal");var saved=Files.readAllBytes(journal);Files.delete(journal);var missing=new RatisNodeRuntimeTest.FakeServer();assertThrows(IOException.class,()->open(f,plan,missing,RatisNodeRuntime.Startup.RECOVER,policy));assertFalse(missing.created);assertFalse(Files.exists(journal));Files.write(journal,saved);
            var managed=plan.pools().get(firstPool).nativePath(f.env.nodeRoot);var retired=managed.resolveSibling(managed.getFileName()+"-retained");Files.move(managed,retired);try(var files=Files.walk(retired)){for(var original:files.toList()){var destination=managed.resolve(retired.relativize(original));if(Files.isDirectory(original,LinkOption.NOFOLLOW_LINKS))Files.createDirectories(destination);else Files.copy(original,destination);}}
            try{var replaced=new RatisNodeRuntimeTest.FakeServer();assertThrows(IOException.class,()->open(f,plan,replaced,RatisNodeRuntime.Startup.RECOVER,policy));assertFalse(replaced.created);assertTrue(Files.exists(managed.resolve("CURRENT")),"Replacement has complete copied DB files; original native root identity must still reject it");}finally{try(var files=Files.walk(managed)){for(var copy:files.sorted(Comparator.reverseOrder()).toList())Files.delete(copy);}Files.move(retired,managed);}
            try(var node=open(f,plan,new RatisNodeRuntimeTest.FakeServer(),RatisNodeRuntime.Startup.RECOVER,policy)){node.start();}
        }
    }
    @Test void boundUnexposedJournalCannotBeRecreatedAfterInterruptedPoolInitialization()throws Exception {
        var helper=new RatisNodeMultiDomainTest();helper.root=root;try(var f=helper.new Fixture()){
            var plan=plan(f);var failed=new RatisNodeRuntimeTest.FakeServer();failed.failCreate=true;assertThrows(IOException.class,()->open(f,plan,failed,RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,RatisNativeExtentPolicy.defaults()));
            assertEquals(1,SessionJournalFiles.read(f.env.nodeRoot.resolve("node.identity"),RatisNodeIdentity.MAX_BYTES)[0]);var journal=f.env.nodeRoot.resolve("native-extents.journal");Files.delete(journal);
            var retry=new RatisNodeRuntimeTest.FakeServer();assertThrows(IOException.class,()->open(f,plan,retry,RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,RatisNativeExtentPolicy.defaults()));assertFalse(retry.created);assertFalse(Files.exists(journal));
        }
    }
    @Test void failedNativePinCloseRetainsEnvironmentAndNodeLockUntilActualDrain()throws Exception {
        var helper=new RatisNodeMultiDomainTest();helper.root=root;try(var f=helper.new Fixture()){
            var plan=plan(f);var server=new RatisNodeRuntimeTest.FakeServer();var node=open(f,plan,server,RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,RatisNativeExtentPolicy.defaults());node.start();var env=nativeEnv(node);
            try(var pin=f.store(server,group(plan,f.first)).capture()){assertThrows(IOException.class,node::close);assertTrue(env.isOwningHandle());assertThrows(IOException.class,()->open(f,plan,new RatisNodeRuntimeTest.FakeServer(),RatisNodeRuntime.Startup.RECOVER,RatisNativeExtentPolicy.defaults()));}
            node.close();assertFalse(env.isOwningHandle());try(var reopened=open(f,plan,new RatisNodeRuntimeTest.FakeServer(),RatisNodeRuntime.Startup.RECOVER,RatisNativeExtentPolicy.defaults())){reopened.start();}
        }
    }
    @Test void configuredNativeRootsCannotOverlapReservedNodeNamespacesBeforeBinding()throws Exception {
        var helper=new RatisNodeMultiDomainTest();helper.root=root;try(var f=helper.new Fixture()){
            var original=plan(f);var names=new ArrayList<>(List.of("raft","sessions","recovery","schema-replicas","referenced-files","topology-exposure","topology-source-artifacts","retirement","topology-relocations","exports"));
            for(var file:List.of("node.identity","node.lock","metadata-claims","native-extents.journal"))for(var suffix:List.of("",".pending",".lock"))names.add(file+suffix);
            names.add("native/"+firstPool+".operators");
            for(var name:names){
                var specs=new ArrayList<RatisNativePoolSpec>();for(var spec:original.pools().values()){byte[] config=spec.configuration();if(spec.id().equals(firstPool))config=("database.global.wal-path=\""+f.env.nodeRoot.resolve(name)+"\"").getBytes(java.nio.charset.StandardCharsets.UTF_8);specs.add(new RatisNativePoolSpec(spec.id(),config,spec.maximumGroups(),spec.images(),spec.retention(),spec.pins(),spec.replicas(),spec.resources(),spec.maximumDurableMetadataBytes()));}
                var unsafe=new RatisNodePlan(original.nodeId(),original.nodeIncarnation(),original.domains(),original.catalogue(),original.homes(),original.imports(),original.streamAddresses(),original.maxLocalGroups(),specs,original.domainPools(),original.groupPools());var server=new RatisNodeRuntimeTest.FakeServer();
                var failure=assertThrows(IOException.class,()->open(f,unsafe,server,RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,RatisNativeExtentPolicy.defaults()));assertTrue(failure.getMessage().contains("overlap"),failure.toString());assertFalse(server.created);assertFalse(Files.exists(f.env.nodeRoot.resolve("node.identity")));assertFalse(Files.exists(f.env.nodeRoot.resolve("native-extents.journal")));assertFalse(Files.exists(f.env.nodeRoot.resolve("exports")));
            }
        }
    }
    @Test void absentIdentityNeverAdoptsExistingExtentOrExportAuthority()throws Exception {
        var helper=new RatisNodeMultiDomainTest();helper.root=root;try(var f=helper.new Fixture()){
            var plan=plan(f);for(var name:List.of("exports","native-extents.journal","native-extents.journal.pending","native-extents.journal.lock")){
                var foreign=f.env.nodeRoot.resolve(name);if(name.equals("exports"))Files.createDirectory(foreign);else Files.write(foreign,new byte[]{7,8});
                try{var server=new RatisNodeRuntimeTest.FakeServer();assertThrows(IOException.class,()->open(f,plan,server,RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,RatisNativeExtentPolicy.defaults()));assertFalse(server.created);assertFalse(Files.exists(f.env.nodeRoot.resolve("node.identity")));if(!name.equals("exports"))assertArrayEquals(new byte[]{7,8},Files.readAllBytes(foreign));}
                finally{Files.delete(foreign);}
            }
        }
    }
    @Test void acceptedNativeSourceChunkDownloadReleaseAndColdReopenUseSharedEnv()throws Exception {
        var helper=new RatisNodeMultiDomainTest();helper.root=root;try(var f=helper.new Fixture()){
            var plan=plan(f);var server=new RatisNodeRuntimeTest.FakeServer();var policy=RatisNativeExtentPolicy.defaults();
            try(var node=open(f,plan,server,RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,policy)){node.start();var schema=f.first;var group=group(plan,schema);var column=schema.columnIds().iterator().next();var store=f.store(server,group);put(store,schema,0,(byte)1,new byte[]{7,8,9});
                var empty=RatisTopologySeedRuntimeTest.field(node,"exportSources",RatisExportSourceService.class);empty.stop().get(5,java.util.concurrent.TimeUnit.SECONDS);
                var range=plan.directory(schema.domainId()).ranges().getFirst();var scope=new TransactionScope(List.of(TransactionScope.Span.range(column,range.lowerInclusive(),range.upperExclusive())));
                try(var snapshot=store.capture(scope)){
                    var session=UUID.randomUUID();var participant=new SnapshotFence.Participant(range.rangeId(),group,range.rangeEpoch(),schema.digest(),new OperationId(session,1,1),new OperationId(session,1,2),scope);
                    var fence=new SnapshotFence(UUID.randomUUID(),plan.clusterId(),plan.databaseId(),schema.domainId(),plan.catalogue().groupId(),plan.catalogue().epoch(),new OperationId(session,1,0),1,1,new byte[32],List.of(participant));
                    var chunkLimits=new NativeRangeSstExport.Limits(8,1024,1L<<20,Duration.ofSeconds(5));var limits=new RatisExportJournal.Limits(8,16L<<20,8L<<20,chunkLimits);var definition=new JobDefinition(UUID.randomUUID(),fence,group,List.of(column),new SnapshotFenceStatus.Capture(UUID.randomUUID(),UUID.randomUUID(),snapshot.position()),limits);
                    var request=new BuildRequest(definition,new RatisExportJournal.ChunkIntent(group,column,snapshot.position(),range.lowerInclusive(),0,chunkLimits));
                    var builder=new RatisExportSourceRegistry.NativeBuilder(){public boolean tokenUsable(JobDefinition ignored){return !snapshot.releaseSettled();}public NativeRangeSstExport.Chunk build(JobDefinition def,RatisExportJournal.ChunkIntent intent,Path artifact,RatisExportSourceRegistry.Completion done)throws Exception{var chunk=node.nativePool(firstPool).exportSstChunk(artifact,snapshot,column,intent.scopeStart(),range.upperExclusive(),intent.limits());done.persist(chunk);return chunk;}};
                    try(var registry=new RatisExportSourceRegistry(f.env.nodeRoot.resolve("exports"),RatisExportSourceRegistry.Budgets.defaults(),RatisExportSourceService.IoLimits.defaults(),builder)){
                        var descriptor=registry.build(request);assertEquals(1,descriptor.rows());var downloaded=registry.read(new ReadRequest(definition,0,descriptor.digest(),0,MAX_READ_BYTES));assertEquals(descriptor.fileBytes(),downloaded.length);assertArrayEquals(descriptor.contentHash(),MessageDigest.getInstance("SHA-256").digest(downloaded));registry.release(new ReleaseRequest(definition));assertThrows(IOException.class,()->registry.build(request));
                    }
                }
            }
            try(var node=open(f,plan,new RatisNodeRuntimeTest.FakeServer(),RatisNodeRuntime.Startup.RECOVER,policy)){node.start();}
        }
    }
}
