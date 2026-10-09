package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.WorkloadSettings;
import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeImage;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;

/** Bounded durable node configuration; it conveys no placement-creation, recovery-wipe or service-start authority. */
public final class RatisNodeManifest {
    /** Includes the file checksum; encoded bodies leave room for its 32 bytes. */
    public static final int MAX_BYTES=8*1024*1024;
    private static final int MAGIC=0x524e4d46, MAX_NAME_BYTES=4096, SETTINGS_BYTES=164;
    private static final Comparator<UUID> UUID_ORDER=(a,b) -> {
        int first=Long.compareUnsigned(a.getMostSignificantBits(),b.getMostSignificantBits());
        return first!=0 ? first : Long.compareUnsigned(a.getLeastSignificantBits(),b.getLeastSignificantBits());
    };
    private final DistributedSchema schema;
    private final RatisNodePlan plan;
    private final RatisNodeRuntime.Settings settings;
    private final int readThreads,writeThreads;

    public RatisNodeManifest(DistributedSchema schema,RatisNodePlan plan,RatisNodeRuntime.Settings settings,int readThreads,int writeThreads) {
        this.schema=Objects.requireNonNull(schema); this.plan=Objects.requireNonNull(plan); this.settings=Objects.requireNonNull(settings);
        schema.requireMatches(plan.directory());
        if (!plan.columns().keySet().equals(schema.columnIds()) || !plan.merges().equals(mergeColumns(schema))
                || readThreads<WorkloadSettings.MIN_PRODUCTION_DATA_THREADS || readThreads>256
                || writeThreads<WorkloadSettings.MIN_PRODUCTION_DATA_THREADS || writeThreads>256) {
            throw new IllegalArgumentException("Manifest schema/merge bindings or scheduler budget differ");
        }
        int requiredColumns=plan.localGroups().stream().mapToInt(group -> switch (plan.role(group.getGroupId().getUuid())) {
            case DATA -> schema.columnIds().size(); case CATALOGUE -> 2; case TRANSACTION_HOME, SNAPSHOT_HOME -> 1;
        }).max().orElseThrow();
        if (settings.images().maxColumns()<requiredColumns) throw new IllegalArgumentException("Manifest snapshot column budget is below local group scope");
        for (String name:plan.columns().values()) text(name);
        for (var home:plan.homes()) text(home.physicalName());
        nanos(settings.clockPeriod()); nanos(settings.shutdownTimeout());
        this.readThreads=readThreads; this.writeThreads=writeThreads;
    }
    public DistributedSchema schema() { return schema; }
    public RatisNodePlan plan() { return plan; }
    public RatisNodeRuntime.Settings settings() { return settings; }
    public int readThreads() { return readThreads; }
    public int writeThreads() { return writeThreads; }
    private static Set<UUID> mergeColumns(DistributedSchema schema) {
        var columns=new HashSet<UUID>();
        for (var column:schema.columns()) if (column.operator()!=null) columns.add(column.columnId());
        return Set.copyOf(columns);
    }

    public static byte[] encode(RatisNodeManifest manifest) {
        Objects.requireNonNull(manifest);
        byte[] schema=DistributedSchemaCodec.encode(manifest.schema), directory=RangeDirectoryCodec.encode(manifest.plan.directory());
        byte[] catalogue=RangeDirectoryCodec.encode(synthetic(manifest.plan));
        long size=4L+12+schema.length+directory.length+catalogue.length+72+4+SETTINGS_BYTES;
        for (var home:manifest.plan.homes()) size+=25L+RangeDirectoryCodec.encode(home.directory()).length+text(home.physicalName()).length;
        for (var name:manifest.plan.columns().values()) size+=20L+text(name).length;
        if (size>MAX_BYTES-32) throw new IllegalArgumentException("Node manifest exceeds byte budget");
        var output=ByteBuffer.allocate((int)size).putInt(MAGIC);
        blob(output,schema); blob(output,directory); blob(output,catalogue);
        var c=manifest.plan.catalogue(); uuid(output,c.directoryColumn()); uuid(output,c.sessionsColumn());
        uuid(output,manifest.plan.nodeId()); uuid(output,manifest.plan.nodeIncarnation());
        output.putInt(manifest.plan.localGroups().size()).putInt(manifest.plan.columns().size());
        for (UUID column:manifest.plan.columns().keySet().stream().sorted(UUID_ORDER).toList()) {
            uuid(output,column); blob(output,text(manifest.plan.columns().get(column)));
        }
        output.putInt(manifest.plan.homes().size());
        for (var home:manifest.plan.homes()) {
            output.put((byte)home.role().ordinal()); blob(output,RangeDirectoryCodec.encode(home.directory())); uuid(output,home.column()); blob(output,text(home.physicalName()));
        }
        var settings=manifest.settings; var images=settings.images(); var retention=settings.retention();
        output.putInt(images.maxRowBytes()).putLong(images.maxImageBytes()).putInt(images.maxColumns());
        output.putLong(retention.dataCount()).putLong(retention.dataBytes()).putLong(retention.maintenanceCount()).putLong(retention.maintenanceBytes());
        output.putLong(nanos(settings.clockPeriod())).putInt(settings.propagationVisits()).putInt(settings.maxPropagationRanges())
                .putLong(nanos(settings.shutdownTimeout())).putInt(manifest.readThreads).putInt(manifest.writeThreads);
        var exports=settings.exports(); var pins=exports.pins(); var source=exports.sources(); var io=exports.io();
        output.putInt(pins.maximumPins()).putLong(pins.maximumEstimatedBytes()).putLong(nanos(pins.maximumAge())).putLong(pins.minimumFreeBytes());
        output.putInt(source.maximumJobs()).putInt(source.maximumChunks()).putLong(source.maximumDiskBytes())
                .putLong(source.maximumMetadataBytes()).putLong(source.maximumBufferBytes()).putInt(source.concurrentBuilds());
        output.putLong(io.minimumFreeBytes()).putLong(io.readBytesPerSecond()).putInt(io.readBurstBytes());
        return output.array();
    }
    public static RatisNodeManifest decode(byte[] bytes) {
        Objects.requireNonNull(bytes);
        if (bytes.length<172 || bytes.length>MAX_BYTES-32) throw new IllegalArgumentException("Invalid node manifest size");
        var input=ByteBuffer.wrap(bytes);
        if (input.getInt()!=MAGIC) throw new IllegalArgumentException("Unsupported node manifest header");
        var schema=DistributedSchemaCodec.decode(blob(input,DistributedSchemaCodec.MAX_BYTES));
        var directory=RangeDirectoryCodec.decode(blob(input,RangeDirectoryCodec.MAX_ENCODED_BYTES));
        var catalogue=RangeDirectoryCodec.decode(blob(input,RangeDirectoryCodec.MAX_ENCODED_BYTES));
        require(input,72+4+SETTINGS_BYTES);
        UUID directoryColumn=uuid(input), sessionsColumn=uuid(input), node=uuid(input), incarnation=uuid(input);
        int groups=input.getInt(), count=input.getInt();
        if (groups<1 || groups>RatisNodePlan.MAX_LOCAL_GROUPS || count<0 || count>RangeDirectoryCodec.MAX_COLUMNS
                || (long)count*21>input.remaining()-SETTINGS_BYTES-4) throw new IllegalArgumentException("Invalid node manifest group/column counts");
        var columns=new HashMap<UUID,String>(); UUID previous=null;
        for (int i=0;i<count;i++) {
            require(input,16); UUID column=uuid(input);
            if (previous!=null && UUID_ORDER.compare(previous,column)>=0) throw new IllegalArgumentException("Noncanonical manifest physical column order");
            previous=column; String name=text(input); columns.put(column,name);
        }
        require(input,4+SETTINGS_BYTES); int homeCount=input.getInt();
        if (homeCount<0 || homeCount>2) throw new IllegalArgumentException("Invalid manifest home-role count");
        var homes=new ArrayList<RatisNodePlan.Home>(homeCount); int previousRole=-1; long homeBytes=0;
        for (int i=0;i<homeCount;i++) {
            require(input,1+4+16+4+1+SETTINGS_BYTES); int role=input.get() & 255;
            if (role>=RatisNodePlan.Role.values().length || role<=previousRole) throw new IllegalArgumentException("Invalid/noncanonical manifest home role"); previousRole=role;
            byte[] encoded=blob(input,RangeDirectoryCodec.MAX_ENCODED_BYTES); homeBytes+=encoded.length;
            if (homeBytes>RatisNodePlan.MAX_HOME_DIRECTORY_BYTES) throw new IllegalArgumentException("Manifest home directory aggregate exceeds bound");
            var homeDirectory=RangeDirectoryCodec.decode(encoded); require(input,16); UUID column=uuid(input); String name=text(input);
            homes.add(new RatisNodePlan.Home(RatisNodePlan.Role.values()[role],homeDirectory,column,name));
        }
        require(input,SETTINGS_BYTES);
        var images=new NativeRangeImage.Limits(input.getInt(),input.getLong(),input.getInt());
        final RatisRangeStateMachine.RetentionLimits retention;
        try { retention=new RatisRangeStateMachine.RetentionLimits(input.getLong(),input.getLong(),input.getLong(),input.getLong()); }
        catch (ArithmeticException overflow) { throw new IllegalArgumentException("Manifest retention limits overflow",overflow); }
        long clock=input.getLong(); int visits=input.getInt(), ranges=input.getInt(); long shutdown=input.getLong();
        int reads=input.getInt(), writes=input.getInt();
        var pins=new it.cavallium.rockserver.core.impl.rocksdb.PoolSnapshotAdmission.Limits(input.getInt(),input.getLong(),Duration.ofNanos(input.getLong()),input.getLong());
        var source=new RatisExportSourceRegistry.Budgets(input.getInt(),input.getInt(),input.getLong(),input.getLong(),input.getLong(),input.getInt());
        var io=new RatisExportSourceService.IoLimits(input.getLong(),input.getLong(),input.getInt());
        if (clock<=0 || shutdown<=0 || input.hasRemaining()) throw new IllegalArgumentException("Invalid node manifest durations/trailing data");
        var settings=new RatisNodeRuntime.Settings(images,retention,Duration.ofNanos(clock),visits,ranges,Duration.ofNanos(shutdown),new RatisNodeRuntime.ExportResources(pins,source,io));
        var recovered=catalogue(catalogue,directory,directoryColumn,sessionsColumn);
        var plan=new RatisNodePlan(node,incarnation,directory,recovered,columns,mergeColumns(schema),homes,groups);
        if (plan.localGroups().size()!=groups) throw new IllegalArgumentException("Manifest group budget is not canonical");
        var manifest=new RatisNodeManifest(schema,plan,settings,reads,writes);
        if (!Arrays.equals(bytes,encode(manifest))) throw new IllegalArgumentException("Noncanonical node manifest encoding");
        return manifest;
    }
    public static RatisNodeManifest read(Path path) throws IOException {
        try { return decode(SessionJournalFiles.read(path.toAbsolutePath(),MAX_BYTES)); }
        catch (IllegalArgumentException | ArithmeticException malformed) { throw new IOException("Invalid node manifest",malformed); }
    }
    public void write(Path path) throws IOException { write(path,this); }
    public static void write(Path path,RatisNodeManifest manifest) throws IOException {
        Path target=path.toAbsolutePath();
        if (!Files.isDirectory(target.getParent())) throw new IOException("Node manifest parent is absent");
        byte[] bytes=encode(manifest);
        try (var channel=FileChannel.open(target.resolveSibling(target.getFileName()+".lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE)) {
            final FileLock lock;
            try { lock=channel.tryLock(); } catch (OverlappingFileLockException owned) { throw new IOException("Node manifest writer already active",owned); }
            if (lock==null) throw new IOException("Node manifest writer already active");
            try (lock) { SessionJournalFiles.write(target,bytes,MAX_BYTES); }
        }
    }
    private static RangeDirectory synthetic(RatisNodePlan plan) {
        var c=plan.catalogue(); var d=plan.directory();
        var range=new RangeDescriptor(c.groupId(),c.groupId(),c.epoch(),0,null,null,RangeDescriptor.TopologyState.ACTIVE,c.replicas());
        return new RangeDirectory(d.clusterId(),d.databaseId(),c.groupId(),c.epoch(),c.schemaDigest(),0,
                Set.of(c.directoryColumn(),c.sessionsColumn()),0,List.of(range));
    }
    private static RatisNodePlan.Catalogue catalogue(RangeDirectory c,RangeDirectory d,UUID directoryColumn,UUID sessionsColumn) {
        if (!c.clusterId().equals(d.clusterId()) || !c.databaseId().equals(d.databaseId()) || c.generation()!=0 || c.routingPrefixBytes()!=0
                || directoryColumn.equals(sessionsColumn) || !c.columns().equals(Set.of(directoryColumn,sessionsColumn)) || c.ranges().size()!=1) {
            throw new IllegalArgumentException("Invalid synthetic catalogue directory");
        }
        var range=c.ranges().getFirst();
        if (!range.groupId().equals(c.domainId()) || !range.rangeId().equals(c.domainId()) || range.rangeEpoch()!=c.schemaEpoch()
                || range.configurationVersion()!=0 || range.lowerInclusive()!=null || range.upperExclusive()!=null
                || range.topologyState()!=RangeDescriptor.TopologyState.ACTIVE) throw new IllegalArgumentException("Invalid synthetic catalogue range");
        return new RatisNodePlan.Catalogue(c.domainId(),c.schemaEpoch(),c.schemaDigest(),directoryColumn,sessionsColumn,range.replicas());
    }
    private static long nanos(Duration duration) {
        try { long nanos=duration.toNanos(); if (nanos<=0) throw new IllegalArgumentException("Manifest duration must be positive"); return nanos; }
        catch (ArithmeticException overflow) { throw new IllegalArgumentException("Manifest duration does not fit nanoseconds",overflow); }
    }
    private static byte[] text(String value) {
        if (value==null || value.isEmpty() || value.length()>MAX_NAME_BYTES) throw new IllegalArgumentException("Invalid manifest physical column name");
        try {
            var encoded=StandardCharsets.UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT).encode(java.nio.CharBuffer.wrap(value));
            if (encoded.remaining()>MAX_NAME_BYTES) throw new IllegalArgumentException("Manifest physical column name exceeds byte budget");
            byte[] bytes=new byte[encoded.remaining()]; encoded.get(bytes); return bytes;
        } catch (CharacterCodingException malformed) { throw new IllegalArgumentException("Malformed manifest column name",malformed); }
    }
    private static String text(ByteBuffer input) {
        require(input,4); int size=input.getInt();
        if (size<1 || size>MAX_NAME_BYTES || size>input.remaining()-SETTINGS_BYTES) throw new IllegalArgumentException("Invalid manifest physical column name size");
        var bytes=input.slice(); bytes.limit(size);
        try {
            String value=StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT).decode(bytes).toString();
            input.position(input.position()+size); return value;
        } catch (CharacterCodingException malformed) { throw new IllegalArgumentException("Malformed manifest UTF-8",malformed); }
    }
    private static byte[] blob(ByteBuffer input,int maximum) {
        require(input,4); int size=input.getInt();
        if (size<1 || size>maximum || size>input.remaining()) throw new IllegalArgumentException("Invalid manifest embedded blob size");
        byte[] bytes=new byte[size]; input.get(bytes); return bytes;
    }
    private static void blob(ByteBuffer output,byte[] bytes) { output.putInt(bytes.length).put(bytes); }
    private static UUID uuid(ByteBuffer input) { return new UUID(input.getLong(),input.getLong()); }
    private static void uuid(ByteBuffer output,UUID id) { output.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()); }
    private static void require(ByteBuffer input,int bytes) { if (input.remaining()<bytes) throw new IllegalArgumentException("Truncated node manifest"); }
}
