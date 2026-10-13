package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeImage;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class RatisNodeManifestTest {
    @Test void currentManifestPreservesExplicitNodeAndPoolAdmissionLimits() {
        var original=manifest(false);var base=original.settings();
        var node=new RatisNodeResourceBudget.Limits(new RatisNodeResourceBudget.Lane(128L*1024*1024,64),new RatisNodeResourceBudget.Lane(64L*1024*1024,32));
        var pool=new RatisNodeResourceBudget.Limits(new RatisNodeResourceBudget.Lane(32L*1024*1024,16),new RatisNodeResourceBudget.Lane(16L*1024*1024,8));
        var limits=new RatisNodeRuntime.ResourceLimits(node,pool);
        var settings=new RatisNodeRuntime.Settings(base.images(),base.retention(),base.clockPeriod(),base.propagationVisits(),base.maxPropagationRanges(),base.shutdownTimeout(),base.exports(),limits);
        var value=new RatisNodeManifest(original.schemas().values(),original.plan(),settings,original.readThreads(),original.writeThreads());
        var cold=RatisNodeManifest.decode(RatisNodeManifest.encode(value));assertEquals(limits,cold.settings().resources());
        assertArrayEquals(RatisNodeManifest.encode(value),RatisNodeManifest.encode(cold));
    }
    @Test void explicitStreamEndpointSurvivesManifestAndConfiguresFiniteProviderWithoutStartingIt() throws Exception {
        var original=manifest(false); var base=original.plan(); var addresses=Map.of("local","127.0.0.1:9101");
        var plan=new RatisNodePlan(base.nodeId(),base.nodeIncarnation(),List.of(new RatisNodePlan.Domain(base.directory(DOMAIN),base.columns(),base.merges())),base.catalogue(),base.homes(),base.imports(),addresses,base.localGroups().size());
        var encoded=RatisNodeManifest.encode(new RatisNodeManifest(List.of(original.schema(DOMAIN)),plan,original.settings(),original.readThreads(),original.writeThreads()));
        var reopened=RatisNodeManifest.decode(encoded).plan(); assertEquals(addresses,reopened.streamAddresses()); assertArrayEquals(plan.identityDigest(),reopened.identityDigest());
        assertFalse(Arrays.equals(base.identityDigest(),plan.identityDigest()));
        for(var group:reopened.localGroups())for(var peer:group.getPeers())assertEquals(addresses.get(peer.getId().toString()),peer.getDataStreamAddress());
        var properties=new org.apache.ratis.conf.RaftProperties(); RatisStreamingTransport.server(reopened,properties);
        assertEquals(org.apache.ratis.datastream.SupportedDataStreamType.NETTY,org.apache.ratis.RaftConfigKeys.DataStream.type(properties,ignored->{}));
        assertEquals("127.0.0.1",org.apache.ratis.netty.NettyConfigKeys.DataStream.host(properties)); assertEquals(9101,org.apache.ratis.netty.NettyConfigKeys.DataStream.port(properties));
        assertEquals(2,org.apache.ratis.netty.NettyConfigKeys.DataStream.Server.workerGroupSize(properties));
        assertFalse(org.apache.ratis.server.RaftServerConfigKeys.DataStream.asyncRequestThreadPoolCached(properties)); assertEquals(2,org.apache.ratis.server.RaftServerConfigKeys.DataStream.asyncWriteThreadPoolSize(properties));
        assertEquals(1,org.apache.ratis.netty.NettyConfigKeys.DataStream.Client.workerGroupSize(properties));
        assertThrows(IllegalArgumentException.class,()->new RatisNodePlan(base.nodeId(),base.nodeIncarnation(),List.of(new RatisNodePlan.Domain(base.directory(DOMAIN),base.columns(),base.merges())),base.catalogue(),base.homes(),base.imports(),Map.of("other","127.0.0.1:9101"),base.localGroups().size()));
        assertThrows(IllegalArgumentException.class,()->new RatisNodePlan(base.nodeId(),base.nodeIncarnation(),List.of(new RatisNodePlan.Domain(base.directory(DOMAIN),base.columns(),base.merges())),base.catalogue(),base.homes(),base.imports(),Map.of("local",base.address()),base.localGroups().size()));
        var disabled=new org.apache.ratis.conf.RaftProperties(); RatisStreamingTransport.server(base,disabled);
        assertEquals(org.apache.ratis.datastream.SupportedDataStreamType.DISABLED,org.apache.ratis.RaftConfigKeys.DataStream.type(disabled,ignored->{}));
    }
    private static final UUID CLUSTER=new UUID(0,1), DATABASE=new UUID(0,2), DOMAIN=new UUID(0,3), A=new UUID(0,4), B=new UUID(Long.MIN_VALUE,5);
    private static final UUID NODE=new UUID(0,6), INCARNATION=new UUID(0,7), CATALOGUE=new UUID(0,8), DIRECTORY_COLUMN=new UUID(0,9), SESSIONS_COLUMN=new UUID(0,10);
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    private static DistributedSchema schema(boolean bucket,boolean operator) {
        var columns=new ArrayList<DistributedSchema.Column>();
        var first=ColumnSchema.of(IntArrayList.of(4),bucket ? ObjectArrayList.of(ColumnHashType.XXHASH8) : ObjectArrayList.of(),true);
        columns.add(new DistributedSchema.Column(7,A,"alpha",first,null));
        var second=ColumnSchema.of(IntArrayList.of(8),ObjectArrayList.of(),true,operator ? "sum" : null,operator ? 1L : null,operator ? "example.Sum" : null);
        columns.add(new DistributedSchema.Column(9,B,"beta",second,operator ? new DistributedSchema.Operator("sum",1,"example.Sum",new byte[32],1,1,"pure-sum",1) : null));
        return new DistributedSchema(CLUSTER,DATABASE,DOMAIN,1,0,columns);
    }
    private static RatisNodePlan plan(DistributedSchema schema,Map<UUID,String> bindings,Set<UUID> merges) {
        var data=new ReplicaDescriptor(NODE,INCARNATION,new UUID(1,1),"local","127.0.0.1:9001",ReplicaDescriptor.Role.VOTER);
        var cat=new ReplicaDescriptor(NODE,INCARNATION,new UUID(1,2),"local","127.0.0.1:9001",ReplicaDescriptor.Role.VOTER);
        var range=new RangeDescriptor(new UUID(1,3),new UUID(1,4),1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(data));
        var directory=new RangeDirectory(CLUSTER,DATABASE,DOMAIN,1,schema.digest(),5,schema.columnIds(),0,List.of(range));
        return new RatisNodePlan(NODE,INCARNATION,List.of(new RatisNodePlan.Domain(directory,bindings,merges)),new RatisNodePlan.Catalogue(CATALOGUE,2,new byte[32],DIRECTORY_COLUMN,SESSIONS_COLUMN,List.of(cat)),List.of(),List.of(),Map.of(),20);
    }
    private static RatisNodeRuntime.Settings settings() {
        return new RatisNodeRuntime.Settings(new NativeRangeImage.Limits(4*1024*1024,8L*1024*1024,2),
                new RatisRangeStateMachine.RetentionLimits(4096,16L*1024*1024,16,64*1024),Duration.ofMillis(1500),3,17,Duration.ofSeconds(25),RatisNodeRuntime.ExportResources.defaults(),RatisNodeRuntime.ResourceLimits.defaults());
    }
    private static RatisNodeManifest manifest(boolean bucket) {
        var schema=schema(bucket,false); var bindings=new LinkedHashMap<UUID,String>(); bindings.put(B,"physical-beta"); bindings.put(A,"physical-alpha");
        return new RatisNodeManifest(List.of(schema),plan(schema,bindings,Set.of()),settings(),3,5);
    }
    @Test void exactFencedImportPlacementAndDefinitionSurviveCurrentManifestRoundTrip() {
        var original=manifest(false); var active=original.plan().directory(DOMAIN); var range=active.ranges().getFirst();
        var fenced=new RangeDirectory(active.clusterId(),active.databaseId(),active.domainId(),active.schemaEpoch(),active.schemaDigest(),active.generation(),active.columns(),active.routingPrefixBytes(),List.of(
                new RangeDescriptor(range.rangeId(),range.groupId(),range.rangeEpoch(),range.configurationVersion(),null,null,RangeDescriptor.TopologyState.FENCED,range.replicas())));
        UUID job=UUID.randomUUID(); var columns=original.plan().columns().entrySet().stream().map(entry -> new NativeSstImportManifest.Column(entry.getKey(),entry.getValue(),false,List.of())).toList();
        var nativeManifest=new NativeSstImportManifest(job,range.groupId(),range.rangeEpoch(),fenced.domainId(),fenced.schemaDigest(),null,null,columns,RatisRangeImportRecord.DISTRIBUTED_LIMITS);
        var definition=new RangeImportDefinition(job,new OperationId(UUID.randomUUID(),1,0),fenced,List.of(nativeManifest));
        assertThrows(IllegalArgumentException.class,() -> new RatisNodePlan(NODE,INCARNATION,List.of(new RatisNodePlan.Domain(fenced,original.plan().columns(),Set.of())),original.plan().catalogue(),List.of(),List.of(),Map.of(),20));
        var configured=new RatisNodePlan(NODE,INCARNATION,List.of(new RatisNodePlan.Domain(fenced,original.plan().columns(),Set.of())),original.plan().catalogue(),List.of(),List.of(definition),Map.of(),20);
        assertThrows(IOException.class,()->RatisStreamingTransport.server(configured,new org.apache.ratis.conf.RaftProperties()));
        var value=new RatisNodeManifest(List.of(original.schema(DOMAIN)),configured,settings(),3,5); byte[] encoded=RatisNodeManifest.encode(value);
        var reopened=RatisNodeManifest.decode(encoded); assertArrayEquals(encoded,RatisNodeManifest.encode(reopened));
        assertArrayEquals(definition.encode(),reopened.plan().importDefinition(range.groupId()).encode());
        assertArrayEquals(configured.identityDigest(),reopened.plan().identityDigest());
        assertEquals(RangeDescriptor.TopologyState.FENCED,reopened.plan().directory(DOMAIN).ranges().getFirst().topologyState());
        assertThrows(IllegalArgumentException.class,() -> new RatisNodePlan(NODE,INCARNATION,List.of(new RatisNodePlan.Domain(active,original.plan().columns(),Set.of())),original.plan().catalogue(),List.of(),List.of(definition),Map.of(),20));
    }
    @Test void configuredPoolAndExportResourcesSurviveCanonicalManifestRoundTrip() {
        var original=manifest(false); var base=original.settings();
        var resources=new RatisNodeRuntime.ExportResources(
                new it.cavallium.rockserver.core.impl.rocksdb.PoolSnapshotAdmission.Limits(2,16L*1024*1024,Duration.ofSeconds(30),4096),
                new RatisExportSourceRegistry.Budgets(8,64,16L*1024*1024,16L*1024*1024,8L*1024*1024,1),
                new RatisExportSourceService.IoLimits(8192,1024*1024,1024*1024));
        var configured=new RatisNodeRuntime.Settings(base.images(),base.retention(),base.clockPeriod(),
                base.propagationVisits(),base.maxPropagationRanges(),base.shutdownTimeout(),resources,RatisNodeRuntime.ResourceLimits.defaults());
        var manifest=new RatisNodeManifest(List.of(original.schema(DOMAIN)),original.plan(),configured,3,5);
        byte[] encoded=RatisNodeManifest.encode(manifest); var reopened=RatisNodeManifest.decode(encoded);
        assertEquals(resources,reopened.settings().exports()); assertArrayEquals(encoded,RatisNodeManifest.encode(reopened));
    }

    @Test void configuredSchemaReplicaBudgetsUseOneCanonicalManifestLayout() {
        var original=manifest(false);
        var limits=new RatisSchemaReplicaService.Limits(5,128L*1024*1024,96L*1024*1024,7,9,8192);
        var configured=new RatisNodeManifest(List.of(original.schema(DOMAIN)),original.plan(),original.settings(),3,5,limits);
        byte[] encoded=RatisNodeManifest.encode(configured); var reopened=RatisNodeManifest.decode(encoded);
        assertEquals(limits,reopened.schemaReplicaLimits()); assertArrayEquals(encoded,RatisNodeManifest.encode(reopened));
        assertEquals(RatisSchemaReplicaService.Limits.defaults(),original.schemaReplicaLimits());
        assertFalse(Arrays.equals(encoded,RatisNodeManifest.encode(original)));
        ByteBuffer.wrap(encoded).putInt(encoded.length-16,0);
        assertThrows(IllegalArgumentException.class,() -> RatisNodeManifest.decode(encoded));
    }

    @Test void operatorBindingsAndEmptyUserSchemaSurviveCurrentManifestRoundTrip() {
        var merged=schema(false,true);
        var operatorManifest=new RatisNodeManifest(List.of(merged),plan(merged,Map.of(A,"a",B,"b"),Set.of(B)),settings(),3,3);
        var encoded=RatisNodeManifest.encode(operatorManifest); var decoded=RatisNodeManifest.decode(encoded);
        assertEquals(Set.of(B),decoded.plan().merges()); assertEquals(merged.column(B).operator(),decoded.schema(DOMAIN).column(B).operator());
        assertArrayEquals(operatorManifest.plan().identityDigest(),decoded.plan().identityDigest());
        assertArrayEquals(encoded,RatisNodeManifest.encode(decoded));
        var empty=new DistributedSchema(CLUSTER,DATABASE,DOMAIN,1,0,List.of());
        var emptyManifest=new RatisNodeManifest(List.of(empty),plan(empty,Map.of(),Set.of()),settings(),3,3);
        var reopened=RatisNodeManifest.decode(RatisNodeManifest.encode(emptyManifest));
        assertTrue(reopened.plan().columns().isEmpty()); assertTrue(reopened.plan().merges().isEmpty());
        assertEquals(emptyManifest.plan().localGroups().size(),reopened.plan().localGroups().size());
    }

    @Test void canonicalRoundTripRetainsSchemaPlanSettingsAndImmutableBindings() {
        var original=manifest(false); byte[] encoded=RatisNodeManifest.encode(original); var copy=RatisNodeManifest.decode(encoded);
        assertEquals(1,ByteBuffer.wrap(encoded).getInt(4)); assertEquals(DistributedSchemaCodec.encode(original.schema(DOMAIN)).length,ByteBuffer.wrap(encoded).getInt(8));
        assertArrayEquals(encoded,RatisNodeManifest.encode(copy)); assertArrayEquals(original.schema(DOMAIN).digest(),copy.schema(DOMAIN).digest());
        assertArrayEquals(original.plan().identityDigest(),copy.plan().identityDigest()); assertEquals(original.plan().directory(DOMAIN).generation(),copy.plan().directory(DOMAIN).generation());
        assertEquals(original.settings(),copy.settings()); assertEquals(3,copy.readThreads()); assertEquals(5,copy.writeThreads());
        assertEquals(original.plan().catalogue().directoryColumn(),copy.plan().catalogue().directoryColumn());
        assertEquals(original.plan().catalogue().sessionsColumn(),copy.plan().catalogue().sessionsColumn());
        assertThrows(UnsupportedOperationException.class,() -> copy.plan().columns().clear());
        copy.schema(DOMAIN).digest()[0]^=1; copy.plan().catalogue().schemaDigest()[0]^=1;
        assertArrayEquals(encoded,RatisNodeManifest.encode(copy));
        var bindings=new LinkedHashMap<UUID,String>(); bindings.put(A,"physical-alpha"); bindings.put(B,"physical-beta");
        var reordered=new RatisNodeManifest(List.of(original.schema(DOMAIN)),plan(original.schema(DOMAIN),bindings,Set.of()),settings(),3,5);
        assertArrayEquals(encoded,RatisNodeManifest.encode(reordered));
        var input=ByteBuffer.wrap(encoded); skipBlobs(input); input.position(input.position()+68); assertEquals(2,input.getInt());
        assertEquals(A,new UUID(input.getLong(),input.getLong()));
        assertArrayEquals(RatisNodeManifest.encode(manifest(true)),RatisNodeManifest.encode(RatisNodeManifest.decode(RatisNodeManifest.encode(manifest(true)))));
    }

    @Test void constructorRejectsScopeMismatchMergeBindingsAndUnsafeResourceBudgets() {
        var plain=manifest(false); var changed=schema(true,false);
        assertThrows(IllegalArgumentException.class,() -> new RatisNodeManifest(List.of(changed),plain.plan(),settings(),3,3));
        var merged=schema(false,true);
        assertDoesNotThrow(() -> new RatisNodeManifest(List.of(merged),plan(merged,Map.of(A,"a",B,"b"),Set.of(B)),settings(),3,3));
        assertThrows(IllegalArgumentException.class,() -> new RatisNodeManifest(List.of(merged),plan(merged,Map.of(A,"a",B,"b"),Set.of()),settings(),3,3));
        assertThrows(IllegalArgumentException.class,() -> new RatisNodeManifest(List.of(plain.schema(DOMAIN)),plan(plain.schema(DOMAIN),Map.of(A,"a",B,"b"),Set.of(A)),settings(),3,3));
        for (int threads:new int[] {0,2,257}) {
            assertThrows(IllegalArgumentException.class,() -> new RatisNodeManifest(List.of(plain.schema(DOMAIN)),plain.plan(),settings(),threads,3));
            assertThrows(IllegalArgumentException.class,() -> new RatisNodeManifest(List.of(plain.schema(DOMAIN)),plain.plan(),settings(),3,threads));
        }
        var oneColumn=new DistributedSchema(CLUSTER,DATABASE,DOMAIN,1,0,List.of(plain.schema(DOMAIN).columns().getFirst()));
        var oneImageColumn=new RatisNodeRuntime.Settings(new NativeRangeImage.Limits(4096,1024*1024,1),settings().retention(),
                settings().clockPeriod(),settings().propagationVisits(),settings().maxPropagationRanges(),settings().shutdownTimeout(),RatisNodeRuntime.ExportResources.defaults(),RatisNodeRuntime.ResourceLimits.defaults());
        assertThrows(IllegalArgumentException.class,() -> new RatisNodeManifest(List.of(oneColumn),plan(oneColumn,Map.of(A,"a"),Set.of()),oneImageColumn,3,3),
                "A local catalogue needs two snapshot columns even when data has one");
        var threeColumns=new ArrayList<>(plain.schema(DOMAIN).columns()); UUID third=new UUID(9,9);
        threeColumns.add(new DistributedSchema.Column(11,third,"gamma",ColumnSchema.of(IntArrayList.of(4),ObjectArrayList.of(),true),null));
        var wider=new DistributedSchema(CLUSTER,DATABASE,DOMAIN,1,0,threeColumns);
        var widerPlan=plan(wider,Map.of(A,"a",B,"b",third,"c"),Set.of());
        assertThrows(IllegalArgumentException.class,() -> new RatisNodeManifest(List.of(wider),widerPlan,settings(),3,3),
                "A local data group needs its complete logical-column snapshot scope");
        var threeImageColumns=new RatisNodeRuntime.Settings(new NativeRangeImage.Limits(4096,1024*1024,3),settings().retention(),
                settings().clockPeriod(),settings().propagationVisits(),settings().maxPropagationRanges(),settings().shutdownTimeout(),RatisNodeRuntime.ExportResources.defaults(),RatisNodeRuntime.ResourceLimits.defaults());
        assertDoesNotThrow(() -> new RatisNodeManifest(List.of(wider),widerPlan,threeImageColumns,3,3));
        var tooLong=new RatisNodeRuntime.Settings(settings().images(),settings().retention(),Duration.ofSeconds(Long.MAX_VALUE),1,1,Duration.ofSeconds(1),RatisNodeRuntime.ExportResources.defaults(),RatisNodeRuntime.ResourceLimits.defaults());
        assertThrows(IllegalArgumentException.class,() -> new RatisNodeManifest(List.of(plain.schema(DOMAIN)),plain.plan(),tooLong,3,3));
        assertThrows(IllegalArgumentException.class,() -> new RatisNodeManifest(List.of(plain.schema(DOMAIN)),plan(plain.schema(DOMAIN),Map.of(A,"a".repeat(4097),B,"b"),Set.of()),settings(),3,3));
    }
    @Test void decoderRejectsUnknownHeaderEveryTruncationLengthsCountsOrderNamesAndSettings() {
        byte[] valid=RatisNodeManifest.encode(manifest(false));
        for (int length=0;length<valid.length;length++) { byte[] truncated=Arrays.copyOf(valid,length); assertThrows(IllegalArgumentException.class,() -> RatisNodeManifest.decode(truncated)); }
        assertThrows(IllegalArgumentException.class,() -> RatisNodeManifest.decode(Arrays.copyOf(valid,valid.length+1)));
        assertThrows(IllegalArgumentException.class,() -> RatisNodeManifest.decode(new byte[RatisNodeManifest.MAX_BYTES+1]));
        for (int offset:new int[] {0,4,8}) { byte[] bad=valid.clone(); ByteBuffer.wrap(bad).putInt(offset,Integer.MAX_VALUE); assertThrows(IllegalArgumentException.class,() -> RatisNodeManifest.decode(bad)); }
        var input=ByteBuffer.wrap(valid); skipBlobs(input); int tail=input.position(), bindings=tail+72;
        for (int offset:new int[] {tail+64,tail+68,bindings+16}) {
            byte[] bad=valid.clone(); ByteBuffer.wrap(bad).putInt(offset,Integer.MAX_VALUE); assertThrows(IllegalArgumentException.class,() -> RatisNodeManifest.decode(bad));
        }
        byte[] malformed=valid.clone(); malformed[bindings+20]=(byte)0xff; assertThrows(IllegalArgumentException.class,() -> RatisNodeManifest.decode(malformed));
        int firstName=ByteBuffer.wrap(valid).getInt(bindings+16), second=bindings+20+firstName;
        byte[] duplicate=valid.clone(); System.arraycopy(duplicate,bindings,duplicate,second,16); assertThrows(IllegalArgumentException.class,() -> RatisNodeManifest.decode(duplicate));
        byte[] reversed=valid.clone(); System.arraycopy(valid,bindings,reversed,second,16); System.arraycopy(valid,second,reversed,bindings,16);
        assertThrows(IllegalArgumentException.class,() -> RatisNodeManifest.decode(reversed));
        byte[] groupBudget=valid.clone(); ByteBuffer.wrap(groupBudget).putInt(tail+64,1); assertThrows(IllegalArgumentException.class,() -> RatisNodeManifest.decode(groupBudget));
        int settingsStart=valid.length-248;
        for (int offset:new int[] {settingsStart+72,settingsStart+76}) { byte[] bad=valid.clone(); ByteBuffer.wrap(bad).putInt(offset,2); assertThrows(IllegalArgumentException.class,() -> RatisNodeManifest.decode(bad)); }
        byte[] duration=valid.clone(); ByteBuffer.wrap(duration).putLong(settingsStart+48,0); assertThrows(IllegalArgumentException.class,() -> RatisNodeManifest.decode(duration));
        for (int lane=0;lane<4;lane++) {
            int offset=settingsStart+200+12*lane;
            for (long bytes:new long[]{0,RatisNodeResourceBudget.MAX_BYTES+1}) {
                byte[] bad=valid.clone(); ByteBuffer.wrap(bad).putLong(offset,bytes);
                assertThrows(IllegalArgumentException.class,() -> RatisNodeManifest.decode(bad));
            }
            for (int owners:new int[]{0,RatisNodeResourceBudget.MAX_OWNERS+1}) {
                byte[] bad=valid.clone(); ByteBuffer.wrap(bad).putInt(offset+8,owners);
                assertThrows(IllegalArgumentException.class,() -> RatisNodeManifest.decode(bad));
            }
        }
    }

    @Test void syntheticCatalogueMustRemainExactlyTheDeclaredMetadataRange() {
        byte[] valid=RatisNodeManifest.encode(manifest(false)); var input=ByteBuffer.wrap(valid); input.position(8);
        for (int i=0;i<2;i++) { int length=input.getInt(); input.position(input.position()+length); }
        int size=input.getInt(), offset=input.position(); byte[] encoded=new byte[size]; input.get(encoded);
        var catalogue=RangeDirectoryCodec.decode(encoded); var range=catalogue.ranges().getFirst();
        var changedRange=new RangeDescriptor(range.rangeId(),range.groupId(),range.rangeEpoch(),1,null,null,range.topologyState(),range.replicas());
        var changed=new RangeDirectory(catalogue.clusterId(),catalogue.databaseId(),catalogue.domainId(),catalogue.schemaEpoch(),catalogue.schemaDigest(),0,catalogue.columns(),0,List.of(changedRange));
        byte[] replacement=RangeDirectoryCodec.encode(changed); assertEquals(size,replacement.length); System.arraycopy(replacement,0,valid,offset,size);
        assertThrows(IllegalArgumentException.class,() -> RatisNodeManifest.decode(valid));
    }

    @Test void durableFilesVerifyChecksumAndStableWriterLockWithoutStartingAnything() throws Exception {
        var manifest=manifest(false); Path path=root.resolve("node.manifest"); manifest.write(path);
        assertArrayEquals(RatisNodeManifest.encode(manifest),RatisNodeManifest.encode(RatisNodeManifest.read(path)));
        try (var channel=FileChannel.open(root.resolve("node.manifest.lock"),StandardOpenOption.WRITE); var lock=channel.lock()) {
            assertThrows(IOException.class,() -> manifest.write(path));
        }
        RatisNodeManifest.write(path,manifest);
        assertFalse(Files.exists(root.resolve("node.manifest.pending")));
        byte[] valid=Files.readAllBytes(path), damaged=valid.clone(); damaged[damaged.length/2]^=1; Files.write(path,damaged);
        assertThrows(IOException.class,() -> RatisNodeManifest.read(path)); Files.write(path,valid);
        SessionJournalFiles.write(path,new byte[172],RatisNodeManifest.MAX_BYTES); assertThrows(IOException.class,() -> RatisNodeManifest.read(path));
        manifest.write(path); assertEquals(3,RatisNodeManifest.read(path).readThreads());
        assertThrows(IOException.class,() -> manifest.write(root.resolve("missing/node.manifest")));
    }
    private static RatisNodePlan.Home home(RatisNodePlan.Role role,int id) {
        UUID column=new UUID(0,100+id),group=new UUID(0,200+id); byte[] schema=new byte[32]; schema[0]=(byte)id;
        var replica=new ReplicaDescriptor(NODE,INCARNATION,new UUID(2,id),"local","127.0.0.1:9001",ReplicaDescriptor.Role.VOTER);
        var range=new RangeDescriptor(group,group,3,4,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(replica));
        return new RatisNodePlan.Home(role,new RangeDirectory(CLUSTER,DATABASE,new UUID(0,300+id),7,schema,9,Set.of(column),0,List.of(range)),column,role==RatisNodePlan.Role.TRANSACTION_HOME ? "transaction-homes" : "snapshot-homes");
    }
    private static RatisNodeManifest withHomes(List<RatisNodePlan.Home> homes) {
        var original=manifest(false); var base=original.plan();
        var plan=new RatisNodePlan(NODE,INCARNATION,List.of(new RatisNodePlan.Domain(base.directory(DOMAIN),base.columns(),Set.of())),base.catalogue(),homes,List.of(),Map.of(),20);
        return new RatisNodeManifest(List.of(original.schema(DOMAIN)),plan,settings(),3,5);
    }
    @Test void currentManifestRoundTripsCanonicalDedicatedHomeRolesAndFullDefinitions() {
        var tx=home(RatisNodePlan.Role.TRANSACTION_HOME,1); var snapshot=home(RatisNodePlan.Role.SNAPSHOT_HOME,2);
        var manifest=withHomes(List.of(snapshot,tx)); byte[] encoded=RatisNodeManifest.encode(manifest); var decoded=RatisNodeManifest.decode(encoded);
        assertEquals(List.of(tx.role(),snapshot.role()),decoded.plan().homes().stream().map(RatisNodePlan.Home::role).toList());
        assertArrayEquals(encoded,RatisNodeManifest.encode(withHomes(List.of(tx,snapshot)))); assertArrayEquals(encoded,RatisNodeManifest.encode(decoded));
        assertArrayEquals(manifest.plan().identityDigest(),decoded.plan().identityDigest()); assertEquals(4,decoded.plan().localGroups().size());
        for (var home:manifest.plan().homes()) {
            var copy=decoded.plan().home(home.role()); assertEquals(home.column(),copy.column()); assertEquals(home.physicalName(),copy.physicalName());
            assertArrayEquals(RangeDirectoryCodec.encode(home.directory()),RangeDirectoryCodec.encode(copy.directory()));
        }
        assertThrows(UnsupportedOperationException.class,() -> decoded.plan().homes().clear());
    }
    @Test void homeRoleCountTagsEmbeddedDirectoriesAndTruncationsAreStrictlyBounded() {
        byte[] valid=RatisNodeManifest.encode(withHomes(List.of(home(RatisNodePlan.Role.TRANSACTION_HOME,1),home(RatisNodePlan.Role.SNAPSHOT_HOME,2))));
        for (int n=0;n<valid.length;n++) { byte[] truncated=Arrays.copyOf(valid,n); assertThrows(IllegalArgumentException.class,() -> RatisNodeManifest.decode(truncated)); }
        int count=homeOffset(valid),first=count+4;
        for (int value:new int[]{-1,3,Integer.MAX_VALUE}) { byte[] bytes=valid.clone(); ByteBuffer.wrap(bytes).putInt(count,value); assertThrows(IllegalArgumentException.class,() -> RatisNodeManifest.decode(bytes)); }
        for (int role:new int[]{0,1,3,4,255}) { byte[] bytes=valid.clone(); bytes[first]=(byte)role; assertThrows(IllegalArgumentException.class,() -> RatisNodeManifest.decode(bytes)); }
        byte[] size=valid.clone(); ByteBuffer.wrap(size).putInt(first+1,Integer.MAX_VALUE); assertThrows(IllegalArgumentException.class,() -> RatisNodeManifest.decode(size));
        var location=ByteBuffer.wrap(valid); location.position(first+1); int directoryLength=location.getInt(); int directoryStart=location.position(); byte[] directory=new byte[directoryLength]; location.get(directory);
        var home=RangeDirectoryCodec.decode(directory); var wrong=new RangeDirectory(CLUSTER,new UUID(0,999),home.domainId(),home.schemaEpoch(),home.schemaDigest(),home.generation(),home.columns(),0,home.ranges());
        byte[] bad=valid.clone(); System.arraycopy(RangeDirectoryCodec.encode(wrong),0,bad,directoryStart,directoryLength); assertThrows(IllegalArgumentException.class,() -> RatisNodeManifest.decode(bad));
    }
    @Test void localHomeOnlyPlacementUsesOneImageColumnEvenWithRemoteDataAndCatalogue() {
        var original=manifest(false); var base=original.plan();
        var remote=new ReplicaDescriptor(new UUID(0,400),new UUID(0,401),new UUID(2,10),"remote","127.0.0.1:9002",ReplicaDescriptor.Role.VOTER);
        var old=base.directory(DOMAIN).ranges().getFirst(); var range=new RangeDescriptor(old.rangeId(),old.groupId(),old.rangeEpoch(),old.configurationVersion(),null,null,old.topologyState(),List.of(remote));
        var data=new RangeDirectory(CLUSTER,DATABASE,DOMAIN,1,original.schema(DOMAIN).digest(),5,original.schema(DOMAIN).columnIds(),0,List.of(range));
        var catalogue=new RatisNodePlan.Catalogue(CATALOGUE,2,new byte[32],DIRECTORY_COLUMN,SESSIONS_COLUMN,List.of(remote));
        var plan=new RatisNodePlan(NODE,INCARNATION,List.of(new RatisNodePlan.Domain(data,base.columns(),Set.of())),catalogue,List.of(home(RatisNodePlan.Role.TRANSACTION_HOME,1)),List.of(),Map.of(),1);
        var one=new RatisNodeRuntime.Settings(new NativeRangeImage.Limits(4096,1024*1024,1),settings().retention(),settings().clockPeriod(),settings().propagationVisits(),settings().maxPropagationRanges(),settings().shutdownTimeout(),RatisNodeRuntime.ExportResources.defaults(),RatisNodeRuntime.ResourceLimits.defaults());
        var manifest=new RatisNodeManifest(List.of(original.schema(DOMAIN)),plan,one,3,5); assertEquals(1,manifest.plan().localGroups().size());
        assertEquals(RatisNodePlan.Role.TRANSACTION_HOME,plan.role(plan.localGroups().getFirst().getGroupId().getUuid()));
        assertArrayEquals(RatisNodeManifest.encode(manifest),RatisNodeManifest.encode(RatisNodeManifest.decode(RatisNodeManifest.encode(manifest))));
    }
    private static int homeOffset(byte[] bytes) {
        var input=ByteBuffer.wrap(bytes); skipBlobs(input); input.position(input.position()+64); input.getInt(); int columns=input.getInt();
        for (int i=0;i<columns;i++) { input.position(input.position()+16); int size=input.getInt(); input.position(input.position()+size); }
        return input.position();
    }
    private static void skipBlobs(ByteBuffer input) {
        input.position(4); int domains=input.getInt(); for(int i=0;i<domains*2+1;i++) { int size=input.getInt(); input.position(input.position()+size); }
    }    @Test void twoDomainCurrentCodecAndColdFileReadPreserveSchemasBindingsAndDeclaredCapacity() throws Exception {
        var first=manifest(false); UUID domain=new UUID(0,500),column=new UUID(0,501),group=new UUID(0,502); var shape=ColumnSchema.of(IntArrayList.of(4),ObjectArrayList.of(),true); var second=new DistributedSchema(CLUSTER,DATABASE,domain,1,0,List.of(new DistributedSchema.Column(99,column,"gamma",shape,null)));
        var original=first.plan().directory(DOMAIN).ranges().getFirst(); var range=new RangeDescriptor(group,group,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,original.replicas()); var directory=new RangeDirectory(CLUSTER,DATABASE,domain,1,second.digest(),0,Set.of(column),0,List.of(range)); var extra=new RatisNodePlan.Domain(directory,Map.of(column,"physical-gamma"),Set.of());
        var domains=new ArrayList<>(first.plan().domains()); domains.add(extra); Collections.reverse(domains); var plan=new RatisNodePlan(NODE,INCARNATION,domains,first.plan().catalogue(),List.of(),List.of(),Map.of(),20); var manifest=new RatisNodeManifest(List.of(second,first.schema(DOMAIN)),plan,settings(),3,5);
        var bytes=RatisNodeManifest.encode(manifest); var decoded=RatisNodeManifest.decode(bytes); assertArrayEquals(bytes,RatisNodeManifest.encode(decoded)); assertEquals(2,decoded.schemas().size()); assertEquals(20,decoded.plan().maxLocalGroups()); assertEquals(3,decoded.plan().localGroups().size()); assertArrayEquals(second.digest(),decoded.schemaForGroup(group).digest()); assertEquals(Set.of(A,B,column),decoded.plan().columns().keySet());
        Path path=root.resolve("multi.manifest"); manifest.write(path); var cold=RatisNodeManifest.read(path); assertArrayEquals(bytes,RatisNodeManifest.encode(cold)); assertArrayEquals(plan.identityDigest(),cold.plan().identityDigest()); assertThrows(UnsupportedOperationException.class,()->cold.schemas().clear());
        assertThrows(IllegalArgumentException.class,()->new RatisNodeManifest(List.of(second),plan,settings(),3,5)); assertThrows(IllegalArgumentException.class,()->new RatisNodeManifest(List.of(second,second),plan,settings(),3,5));
        var collision=new DistributedSchema(CLUSTER,DATABASE,domain,1,0,List.of(new DistributedSchema.Column(first.schema(DOMAIN).columns().getFirst().apiId(),column,"gamma",shape,null))); var collisionDirectory=new RangeDirectory(CLUSTER,DATABASE,domain,1,collision.digest(),0,Set.of(column),0,List.of(range)); var collisionPlan=new RatisNodePlan(NODE,INCARNATION,List.of(first.plan().domains().getFirst(),new RatisNodePlan.Domain(collisionDirectory,extra.columns(),Set.of())),plan.catalogue(),List.of(),List.of(),Map.of(),20); assertThrows(IllegalArgumentException.class,()->new RatisNodeManifest(List.of(first.schema(DOMAIN),collision),collisionPlan,settings(),3,5));
        var fencedRange=new RangeDescriptor(group,group,1,0,null,null,RangeDescriptor.TopologyState.FENCED,range.replicas()); var fencedDirectory=new RangeDirectory(CLUSTER,DATABASE,domain,1,second.digest(),0,Set.of(column),0,List.of(fencedRange)); UUID job=new UUID(0,600);
        var nativeManifest=new NativeSstImportManifest(job,group,1,domain,second.digest(),null,null,List.of(new NativeSstImportManifest.Column(column,"physical-gamma",false,List.of())),RatisRangeImportRecord.DISTRIBUTED_LIMITS); var definition=new RangeImportDefinition(job,new OperationId(new UUID(0,601),1,0),fencedDirectory,List.of(nativeManifest));
        var importedPlan=new RatisNodePlan(NODE,INCARNATION,List.of(first.plan().domains().getFirst(),new RatisNodePlan.Domain(fencedDirectory,extra.columns(),Set.of())),plan.catalogue(),List.of(),List.of(definition),Map.of(),20); var imported=new RatisNodeManifest(List.of(first.schema(DOMAIN),second),importedPlan,settings(),3,5); var importedCold=RatisNodeManifest.decode(RatisNodeManifest.encode(imported)); assertArrayEquals(definition.encode(),importedCold.plan().importDefinition(group).encode()); assertEquals(fencedDirectory.domainId(),importedCold.plan().groupDirectory(group).domainId());
        var count=bytes.clone(); ByteBuffer.wrap(count).putInt(4,65); assertThrows(IllegalArgumentException.class,()->RatisNodeManifest.decode(count));
    }

    @Test void batchedImportChainSurvivesCurrentColdManifestWithoutReorderedAuthority() {
        var original=manifest(false);var active=original.plan().directory(DOMAIN);var old=active.ranges().getFirst();
        var a=new RangeDescriptor(old.rangeId(),old.groupId(),old.rangeEpoch(),old.configurationVersion(),null,new byte[]{64},RangeDescriptor.TopologyState.FENCED,old.replicas());
        var b=new RangeDescriptor(new UUID(0,778),new UUID(0,777),old.rangeEpoch(),old.configurationVersion(),new byte[]{64},null,RangeDescriptor.TopologyState.FENCED,old.replicas());
        var initial=new RangeDirectory(active.clusterId(),active.databaseId(),active.domainId(),active.schemaEpoch(),active.schemaDigest(),active.generation(),active.columns(),active.routingPrefixBytes(),List.of(a,b));
        var columns=original.plan().columns().entrySet().stream().map(e->new NativeSstImportManifest.Column(e.getKey(),e.getValue(),false,List.of())).toList();UUID firstId=new UUID(5,2),secondId=new UUID(5,1),session=new UUID(5,3);
        var first=new RangeImportDefinition(firstId,new OperationId(session,1,0),initial,List.of(new NativeSstImportManifest(firstId,a.groupId(),a.rangeEpoch(),DOMAIN,initial.schemaDigest(),a.lowerInclusive(),a.upperExclusive(),columns,RatisRangeImportRecord.DISTRIBUTED_LIMITS)));
        var second=new RangeImportDefinition(secondId,new OperationId(session,1,1),first.targetDirectory(),List.of(new NativeSstImportManifest(secondId,b.groupId(),b.rangeEpoch(),DOMAIN,initial.schemaDigest(),b.lowerInclusive(),b.upperExclusive(),columns,RatisRangeImportRecord.DISTRIBUTED_LIMITS)));
        var plan=new RatisNodePlan(NODE,INCARNATION,List.of(new RatisNodePlan.Domain(initial,original.plan().columns(),Set.of())),original.plan().catalogue(),List.of(),List.of(second,first),Map.of(),20);
        var value=new RatisNodeManifest(List.of(original.schema(DOMAIN)),plan,settings(),3,5);byte[] encoded=RatisNodeManifest.encode(value);var cold=RatisNodeManifest.decode(encoded);
        assertArrayEquals(encoded,RatisNodeManifest.encode(cold));assertArrayEquals(first.encode(),cold.plan().imports().getFirst().encode());assertArrayEquals(second.encode(),cold.plan().importDefinition(b.groupId()).encode());assertArrayEquals(first.targetDirectory().payloadDigest(),cold.plan().importDefinition(b.groupId()).sourceDirectory().payloadDigest());
    }

}
