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
    private static final UUID CLUSTER=new UUID(0,1), DATABASE=new UUID(0,2), DOMAIN=new UUID(0,3), A=new UUID(0,4), B=new UUID(Long.MIN_VALUE,5);
    private static final UUID NODE=new UUID(0,6), INCARNATION=new UUID(0,7), CATALOGUE=new UUID(0,8), DIRECTORY_COLUMN=new UUID(0,9), SESSIONS_COLUMN=new UUID(0,10);
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    private static DistributedSchema schema(boolean bucket,boolean operator) {
        var columns=new ArrayList<DistributedSchema.Column>();
        var first=ColumnSchema.of(IntArrayList.of(4),bucket ? ObjectArrayList.of(ColumnHashType.XXHASH8) : ObjectArrayList.of(),true);
        columns.add(new DistributedSchema.Column(7,A,"alpha",first,null));
        var second=ColumnSchema.of(IntArrayList.of(8),ObjectArrayList.of(),true,operator ? "sum" : null,operator ? 1L : null,operator ? "example.Sum" : null);
        columns.add(new DistributedSchema.Column(9,B,"beta",second,operator ? new DistributedSchema.Operator("sum",1,"example.Sum",new byte[32],1,1) : null));
        return new DistributedSchema(CLUSTER,DATABASE,DOMAIN,1,0,columns);
    }
    private static RatisNodePlan plan(DistributedSchema schema,Map<UUID,String> bindings,Set<UUID> merges) {
        var data=new ReplicaDescriptor(NODE,INCARNATION,new UUID(1,1),"local","127.0.0.1:9001",ReplicaDescriptor.Role.VOTER);
        var cat=new ReplicaDescriptor(NODE,INCARNATION,new UUID(1,2),"local","127.0.0.1:9001",ReplicaDescriptor.Role.VOTER);
        var range=new RangeDescriptor(new UUID(1,3),new UUID(1,4),1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(data));
        var directory=new RangeDirectory(CLUSTER,DATABASE,DOMAIN,1,schema.digest(),5,schema.columnIds(),0,List.of(range));
        return new RatisNodePlan(NODE,INCARNATION,directory,new RatisNodePlan.Catalogue(CATALOGUE,2,new byte[32],DIRECTORY_COLUMN,SESSIONS_COLUMN,List.of(cat)),bindings,merges,20);
    }
    private static RatisNodeRuntime.Settings settings() {
        return new RatisNodeRuntime.Settings(new NativeRangeImage.Limits(4*1024*1024,8L*1024*1024,2),
                new RatisRangeStateMachine.RetentionLimits(4096,16L*1024*1024,16,64*1024),Duration.ofMillis(1500),3,17,Duration.ofSeconds(25));
    }
    private static RatisNodeManifest manifest(boolean bucket) {
        var schema=schema(bucket,false); var bindings=new LinkedHashMap<UUID,String>(); bindings.put(B,"physical-beta"); bindings.put(A,"physical-alpha");
        return new RatisNodeManifest(schema,plan(schema,bindings,Set.of()),settings(),3,5);
    }

    @Test void canonicalRoundTripRetainsSchemaPlanSettingsAndImmutableBindings() {
        var original=manifest(false); byte[] encoded=RatisNodeManifest.encode(original); var copy=RatisNodeManifest.decode(encoded);
        assertEquals(1,ByteBuffer.wrap(encoded).getInt(4));
        assertArrayEquals(encoded,RatisNodeManifest.encode(copy)); assertArrayEquals(original.schema().digest(),copy.schema().digest());
        assertArrayEquals(original.plan().identityDigest(),copy.plan().identityDigest()); assertEquals(original.plan().directory().generation(),copy.plan().directory().generation());
        assertEquals(original.settings(),copy.settings()); assertEquals(3,copy.readThreads()); assertEquals(5,copy.writeThreads());
        assertEquals(original.plan().catalogue().directoryColumn(),copy.plan().catalogue().directoryColumn());
        assertEquals(original.plan().catalogue().sessionsColumn(),copy.plan().catalogue().sessionsColumn());
        assertThrows(UnsupportedOperationException.class,() -> copy.plan().columns().clear());
        copy.schema().digest()[0]^=1; copy.plan().catalogue().schemaDigest()[0]^=1;
        assertArrayEquals(encoded,RatisNodeManifest.encode(copy));
        var bindings=new LinkedHashMap<UUID,String>(); bindings.put(A,"physical-alpha"); bindings.put(B,"physical-beta");
        var reordered=new RatisNodeManifest(original.schema(),plan(original.schema(),bindings,Set.of()),settings(),3,5);
        assertArrayEquals(encoded,RatisNodeManifest.encode(reordered));
        var input=ByteBuffer.wrap(encoded); skipBlobs(input); input.position(input.position()+68); assertEquals(2,input.getInt());
        assertEquals(A,new UUID(input.getLong(),input.getLong()));
        assertArrayEquals(RatisNodeManifest.encode(manifest(true)),RatisNodeManifest.encode(RatisNodeManifest.decode(RatisNodeManifest.encode(manifest(true)))));
    }

    @Test void constructorRejectsScopeMismatchOperatorsMergesAndUnsafeResourceBudgets() {
        var plain=manifest(false); var changed=schema(true,false);
        assertThrows(IllegalArgumentException.class,() -> new RatisNodeManifest(changed,plain.plan(),settings(),3,3));
        var merged=schema(false,true);
        assertThrows(IllegalArgumentException.class,() -> new RatisNodeManifest(merged,plan(merged,Map.of(A,"a",B,"b"),Set.of(B)),settings(),3,3));
        assertThrows(IllegalArgumentException.class,() -> new RatisNodeManifest(merged,plan(merged,Map.of(A,"a",B,"b"),Set.of()),settings(),3,3));
        assertThrows(IllegalArgumentException.class,() -> new RatisNodeManifest(plain.schema(),plan(plain.schema(),Map.of(A,"a",B,"b"),Set.of(A)),settings(),3,3));
        for (int threads:new int[] {0,2,257}) {
            assertThrows(IllegalArgumentException.class,() -> new RatisNodeManifest(plain.schema(),plain.plan(),settings(),threads,3));
            assertThrows(IllegalArgumentException.class,() -> new RatisNodeManifest(plain.schema(),plain.plan(),settings(),3,threads));
        }
        var oneColumn=new DistributedSchema(CLUSTER,DATABASE,DOMAIN,1,0,List.of(plain.schema().columns().getFirst()));
        var oneImageColumn=new RatisNodeRuntime.Settings(new NativeRangeImage.Limits(4096,1024*1024,1),settings().retention(),
                settings().clockPeriod(),settings().propagationVisits(),settings().maxPropagationRanges(),settings().shutdownTimeout());
        assertThrows(IllegalArgumentException.class,() -> new RatisNodeManifest(oneColumn,plan(oneColumn,Map.of(A,"a"),Set.of()),oneImageColumn,3,3),
                "A local catalogue needs two snapshot columns even when data has one");
        var threeColumns=new ArrayList<>(plain.schema().columns()); UUID third=new UUID(9,9);
        threeColumns.add(new DistributedSchema.Column(11,third,"gamma",ColumnSchema.of(IntArrayList.of(4),ObjectArrayList.of(),true),null));
        var wider=new DistributedSchema(CLUSTER,DATABASE,DOMAIN,1,0,threeColumns);
        var widerPlan=plan(wider,Map.of(A,"a",B,"b",third,"c"),Set.of());
        assertThrows(IllegalArgumentException.class,() -> new RatisNodeManifest(wider,widerPlan,settings(),3,3),
                "A local data group needs its complete logical-column snapshot scope");
        var threeImageColumns=new RatisNodeRuntime.Settings(new NativeRangeImage.Limits(4096,1024*1024,3),settings().retention(),
                settings().clockPeriod(),settings().propagationVisits(),settings().maxPropagationRanges(),settings().shutdownTimeout());
        assertDoesNotThrow(() -> new RatisNodeManifest(wider,widerPlan,threeImageColumns,3,3));
        var tooLong=new RatisNodeRuntime.Settings(settings().images(),settings().retention(),Duration.ofSeconds(Long.MAX_VALUE),1,1,Duration.ofSeconds(1));
        assertThrows(IllegalArgumentException.class,() -> new RatisNodeManifest(plain.schema(),plain.plan(),tooLong,3,3));
        assertThrows(IllegalArgumentException.class,() -> new RatisNodeManifest(plain.schema(),plan(plain.schema(),Map.of(A,"a".repeat(4097),B,"b"),Set.of()),settings(),3,3));
    }
    @Test void decoderRejectsUnknownFormatEveryTruncationLengthsCountsOrderNamesAndSettings() {
        byte[] valid=RatisNodeManifest.encode(manifest(false));
        for (int length=0;length<valid.length;length++) { byte[] truncated=Arrays.copyOf(valid,length); assertThrows(IllegalArgumentException.class,() -> RatisNodeManifest.decode(truncated)); }
        assertThrows(IllegalArgumentException.class,() -> RatisNodeManifest.decode(Arrays.copyOf(valid,valid.length+1)));
        assertThrows(IllegalArgumentException.class,() -> RatisNodeManifest.decode(new byte[RatisNodeManifest.MAX_BYTES+1]));
        for (int offset:new int[] {0,4,8}) { byte[] bad=valid.clone(); ByteBuffer.wrap(bad).putInt(offset,Integer.MAX_VALUE); assertThrows(IllegalArgumentException.class,() -> RatisNodeManifest.decode(bad)); }
        for (int version:new int[]{0,2,3}) { byte[] bad=valid.clone(); ByteBuffer.wrap(bad).putInt(4,version); assertThrows(IllegalArgumentException.class,() -> RatisNodeManifest.decode(bad)); }
        var input=ByteBuffer.wrap(valid); skipBlobs(input); int tail=input.position(), bindings=tail+72;
        for (int offset:new int[] {tail+64,tail+68,bindings+16}) {
            byte[] bad=valid.clone(); ByteBuffer.wrap(bad).putInt(offset,Integer.MAX_VALUE); assertThrows(IllegalArgumentException.class,() -> RatisNodeManifest.decode(bad));
        }
        byte[] malformed=valid.clone(); malformed[bindings+20]=(byte)0xff; assertThrows(IllegalArgumentException.class,() -> RatisNodeManifest.decode(malformed));
        int firstName=ByteBuffer.wrap(valid).getInt(bindings+16), second=bindings+20+firstName;
        byte[] duplicate=valid.clone(); System.arraycopy(duplicate,bindings,duplicate,second,16); assertThrows(IllegalArgumentException.class,() -> RatisNodeManifest.decode(duplicate));
        byte[] reversed=valid.clone(); System.arraycopy(valid,bindings,reversed,second,16); System.arraycopy(valid,second,reversed,bindings,16);
        assertThrows(IllegalArgumentException.class,() -> RatisNodeManifest.decode(reversed));
        byte[] groupBudget=valid.clone(); ByteBuffer.wrap(groupBudget).putInt(tail+64,3); assertThrows(IllegalArgumentException.class,() -> RatisNodeManifest.decode(groupBudget));
        for (int offset:new int[] {valid.length-8,valid.length-4}) { byte[] bad=valid.clone(); ByteBuffer.wrap(bad).putInt(offset,2); assertThrows(IllegalArgumentException.class,() -> RatisNodeManifest.decode(bad)); }
        byte[] duration=valid.clone(); ByteBuffer.wrap(duration).putLong(valid.length-32,0); assertThrows(IllegalArgumentException.class,() -> RatisNodeManifest.decode(duration));
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
    private static void skipBlobs(ByteBuffer input) {
        input.position(8); for (int i=0;i<3;i++) { int size=input.getInt(); input.position(input.position()+size); }
    }
}
