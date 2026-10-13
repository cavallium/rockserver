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
    public static final int MAX_BYTES=RatisNodePlan.MAX_METADATA_BYTES;
    private static final int MAGIC=0x524e4d46, MAX_NAME_BYTES=4096, SETTINGS_BYTES=428;
    private static final Comparator<UUID> UUID_ORDER=(a,b) -> {
        int first=Long.compareUnsigned(a.getMostSignificantBits(),b.getMostSignificantBits());
        return first!=0 ? first : Long.compareUnsigned(a.getLeastSignificantBits(),b.getLeastSignificantBits());
    };
    private final Map<UUID,DistributedSchema> schemas;
    private final RatisNodePlan plan;
    private final RatisNodeRuntime.Settings settings;
    private final int readThreads,writeThreads;
    private final RatisSchemaReplicaService.Limits schemaReplicaLimits;

    public RatisNodeManifest(Collection<DistributedSchema> schemas,RatisNodePlan plan,RatisNodeRuntime.Settings settings,int readThreads,int writeThreads) {
        this(schemas,plan,settings,readThreads,writeThreads,RatisSchemaReplicaService.Limits.defaults());
    }
    public RatisNodeManifest(Collection<DistributedSchema> schemas,RatisNodePlan plan,RatisNodeRuntime.Settings settings,int readThreads,int writeThreads,RatisSchemaReplicaService.Limits replicaLimits) {
        schemaReplicaLimits=Objects.requireNonNull(replicaLimits);
        this.plan=Objects.requireNonNull(plan); this.settings=Objects.requireNonNull(settings);
        if(schemas.size()!=plan.domains().size()) throw new IllegalArgumentException("Manifest schemas must cover exactly every DATA domain");
        var byDomain=new TreeMap<UUID,DistributedSchema>(UUID_ORDER); var apiIds=new HashSet<Long>(); long bytes=0;
        for(var schema:schemas) {
            var domain=plan.domain(schema.domainId()); schema.requireMatches(domain.directory()); bytes+=DistributedSchemaCodec.encode(schema).length+RangeDirectoryCodec.encode(domain.directory()).length;
            if(byDomain.put(schema.domainId(),schema)!=null || !domain.merges().equals(mergeColumns(schema)) || bytes>RatisNodePlan.MAX_DOMAIN_BYTES) throw new IllegalArgumentException("Manifest DATA schema coverage/merges/metadata differs");
            for(var column:schema.columns()) if(!apiIds.add(column.apiId())) throw new IllegalArgumentException("DATA domains share a logical API column identity");
        }
        if(byDomain.size()!=plan.domains().size() || readThreads<WorkloadSettings.MIN_PRODUCTION_DATA_THREADS || readThreads>256 || writeThreads<WorkloadSettings.MIN_PRODUCTION_DATA_THREADS || writeThreads>256) throw new IllegalArgumentException("Manifest schema coverage or scheduler budget differs"); this.schemas=Collections.unmodifiableMap(byDomain);
        int requiredColumns=plan.localGroups().stream().mapToInt(group -> switch (plan.role(group.getGroupId().getUuid())) {
            case DATA -> plan.groupDirectory(group.getGroupId().getUuid()).columns().size(); case CATALOGUE -> 2; case TRANSACTION_HOME, SNAPSHOT_HOME -> 1;
        }).max().orElseThrow();
        if (settings.images().maxColumns()<requiredColumns) throw new IllegalArgumentException("Manifest snapshot column budget is below local group scope");
        for(var group:plan.localGroups()){var spec=plan.pools().get(plan.poolId(group.getGroupId().getUuid()));int count=plan.role(group.getGroupId().getUuid())==RatisNodePlan.Role.CATALOGUE?2:plan.groupDirectory(group.getGroupId().getUuid()).columns().size();if(spec.images().maxColumns()<count)throw new IllegalArgumentException("Assigned pool snapshot column budget below group scope");}
        for (String name:plan.columns().values()) text(name);
        for (var home:plan.homes()) text(home.physicalName());
        nanos(settings.clockPeriod()); nanos(settings.shutdownTimeout());
        this.readThreads=readThreads; this.writeThreads=writeThreads; encodedSize(this);
    }
    public Map<UUID,DistributedSchema> schemas() { return schemas; }
    public DistributedSchema schema(UUID domain) { var schema=schemas.get(domain); if(schema==null) throw new IllegalArgumentException("Manifest has no DATA domain schema"); return schema; }
    public DistributedSchema schemaForGroup(UUID group) { if(plan.role(group)!=RatisNodePlan.Role.DATA) throw new IllegalArgumentException("Group is not DATA"); return schema(plan.groupDirectory(group).domainId()); }
    public RatisNodePlan plan() { return plan; }
    public RatisNodeRuntime.Settings settings() { return settings; }
    public RatisSchemaReplicaService.Limits schemaReplicaLimits() { return schemaReplicaLimits; }
    public int readThreads() { return readThreads; }
    public int writeThreads() { return writeThreads; }
    private static Set<UUID> mergeColumns(DistributedSchema schema) {
        var columns=new HashSet<UUID>();
        for (var column:schema.columns()) if (column.operator()!=null) columns.add(column.columnId());
        return Set.copyOf(columns);
    }

    private static long placementBytes(RatisNodePlan plan){long size=12L+32L*(plan.domainPools().size()+plan.groupPools().size());for(var spec:plan.pools().values())size+=4L+spec.encode().length;return size;}
    private static Map<UUID,UUID> placements(ByteBuffer in,int maximum){require(in,4);int count=in.getInt();if(count<0 || count>maximum || count*32L>in.remaining())throw new IllegalArgumentException("Invalid pool placement count");var values=new LinkedHashMap<UUID,UUID>();UUID previous=null;for(int n=0;n<count;n++){var id=uuid(in);if(previous!=null && UUID_ORDER.compare(previous,id)>=0)throw new IllegalArgumentException("Noncanonical pool placements");previous=id;values.put(id,uuid(in));}return values;}
    private static long encodedSize(RatisNodeManifest manifest) {
        long size=placementBytes(manifest.plan)+4L+4+8L*manifest.plan.domains().size()+4+RangeDirectoryCodec.encode(synthetic(manifest.plan)).length+72+12+SETTINGS_BYTES;
        for(var domain:manifest.plan.domains()) size+=DistributedSchemaCodec.encode(manifest.schema(domain.domainId())).length+RangeDirectoryCodec.encode(domain.directory()).length;
        for(var home:manifest.plan.homes()) size+=25L+RangeDirectoryCodec.encode(home.directory()).length+text(home.physicalName()).length;
        for(var name:manifest.plan.columns().values()) size+=20L+text(name).length;
        for(var definition:manifest.plan.imports()) size+=4L+definition.encode().length;
        for(var entry:manifest.plan.streamAddresses().entrySet())size+=8L+text(entry.getKey()).length+text(entry.getValue()).length;
        if(size>MAX_BYTES-32)throw new IllegalArgumentException("Node manifest exceeds byte budget");return size;
    }
    public static byte[] encode(RatisNodeManifest manifest) {
        Objects.requireNonNull(manifest);
        var schemaBytes=new ArrayList<byte[]>(); var directoryBytes=new ArrayList<byte[]>(); long domainBytes=0;
        for(var domain:manifest.plan.domains()) { byte[] schema=DistributedSchemaCodec.encode(manifest.schema(domain.domainId())),directory=RangeDirectoryCodec.encode(domain.directory()); schemaBytes.add(schema); directoryBytes.add(directory); domainBytes+=schema.length+directory.length; }
        if(domainBytes>RatisNodePlan.MAX_DOMAIN_BYTES) throw new IllegalArgumentException("Manifest DATA metadata exceeds aggregate bound");
        byte[] catalogue=RangeDirectoryCodec.encode(synthetic(manifest.plan));
        long size=placementBytes(manifest.plan)+4L+4+8L*schemaBytes.size()+domainBytes+4+catalogue.length+72+12+SETTINGS_BYTES;
        for (var home:manifest.plan.homes()) size+=25L+RangeDirectoryCodec.encode(home.directory()).length+text(home.physicalName()).length;
        for (var name:manifest.plan.columns().values()) size+=20L+text(name).length;
        for(var definition:manifest.plan.imports()) size+=4L+definition.encode().length;
        for(var entry:manifest.plan.streamAddresses().entrySet())size+=8L+text(entry.getKey()).length+text(entry.getValue()).length;
        if (size>MAX_BYTES-32) throw new IllegalArgumentException("Node manifest exceeds byte budget");
        var output=ByteBuffer.allocate((int)size).putInt(MAGIC);
        output.putInt(manifest.plan.pools().size());for(var spec:manifest.plan.pools().values())blob(output,spec.encode());for(var placement:List.of(manifest.plan.domainPools(),manifest.plan.groupPools())){output.putInt(placement.size());for(var id:placement.keySet().stream().sorted(UUID_ORDER).toList()){uuid(output,id);uuid(output,placement.get(id));}}
        output.putInt(schemaBytes.size()); for(int i=0;i<schemaBytes.size();i++){blob(output,schemaBytes.get(i));blob(output,directoryBytes.get(i));} blob(output,catalogue);
        var c=manifest.plan.catalogue(); uuid(output,c.directoryColumn()); uuid(output,c.sessionsColumn());
        uuid(output,manifest.plan.nodeId()); uuid(output,manifest.plan.nodeIncarnation());
        output.putInt(manifest.plan.maxLocalGroups()).putInt(manifest.plan.columns().size());
        for (UUID column:manifest.plan.columns().keySet().stream().sorted(UUID_ORDER).toList()) {
            uuid(output,column); blob(output,text(manifest.plan.columns().get(column)));
        }
        output.putInt(manifest.plan.homes().size());
        for (var home:manifest.plan.homes()) {
            output.put((byte)home.role().ordinal()); blob(output,RangeDirectoryCodec.encode(home.directory())); uuid(output,home.column()); blob(output,text(home.physicalName()));
        }
        output.putInt(manifest.plan.imports().size()); for(var definition:manifest.plan.imports()) blob(output,definition.encode());
        output.putInt(manifest.plan.streamAddresses().size()); for(var peer:manifest.plan.streamAddresses().keySet().stream().sorted().toList()){blob(output,text(peer));blob(output,text(manifest.plan.streamAddresses().get(peer)));}
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
        var replicas=manifest.schemaReplicaLimits;
        output.putInt(replicas.maximumUploads()).putLong(replicas.maximumDiskBytes()).putLong(replicas.maximumHeapBytes()).putInt(replicas.maximumTasks()).putInt(replicas.maximumObservers()).putLong(replicas.minimumFreeBytes());
        for(var limits:List.of(settings.resources().node(),settings.resources().pool())) {
            for(var lane:List.of(limits.ordinary(),limits.protectedLane()))output.putLong(lane.bytes()).putInt(lane.owners());
        }
        output.putLong(settings.resources().maximumSourceImageDiskBytes()).putLong(settings.resources().maximumDurableMetadataBytes()).putLong(settings.resources().maximumOperatorCacheBytes()).putLong(settings.resources().maximumNativeCacheBytes());
        for(var value:List.of(settings.resources().committedApply().node(),settings.resources().committedApply().pool())){for(var lane:List.of(value.read(),value.ordinary(),value.protectedLane()))output.putLong(lane.bytes()).putInt(lane.owners());output.putInt(value.groups());}
        var disk=settings.resources().diskRunway();output.putLong(disk.warningFreeBytes()).putLong(disk.emergencyFreeBytes()).putLong(disk.resumeFreeBytes()).putLong(disk.maximumPendingBytes()).putLong(disk.maximumProtectedPendingBytes()).putLong(disk.recoveryHeadroomBytes()).putLong(disk.maximumCensusAge().toNanos()).putInt(disk.maximumScopes()).putInt(disk.maximumFiles()).putInt(disk.visits());return output.array();
    }
    public static RatisNodeManifest decode(byte[] bytes) {
        Objects.requireNonNull(bytes);
        if (bytes.length<172 || bytes.length>MAX_BYTES-32) throw new IllegalArgumentException("Invalid node manifest size");
        var input=ByteBuffer.wrap(bytes);
        if (input.getInt()!=MAGIC) throw new IllegalArgumentException("Unsupported node manifest header");
        require(input,4);int poolCount=input.getInt();if(poolCount<1 || poolCount>RatisNodeResourceBudget.MAX_POOLS)throw new IllegalArgumentException("Invalid physical pool count");var pools=new ArrayList<RatisNativePoolSpec>();long poolBytes=0;UUID previousPool=null;for(int n=0;n<poolCount;n++){var encoded=blob(input,RatisNativePoolSpec.MAX_BYTES);if((poolBytes+=encoded.length)>4L*1024*1024)throw new IllegalArgumentException("Pool declarations exceed aggregate limit");var spec=RatisNativePoolSpec.decode(encoded);if(previousPool!=null && UUID_ORDER.compare(previousPool,spec.id())>=0)throw new IllegalArgumentException("Noncanonical pool order");previousPool=spec.id();pools.add(spec);}var domainPools=placements(input,RatisNodePlan.MAX_DOMAINS);var groupPools=placements(input,RatisNodePlan.MAX_LOCAL_GROUPS);
        require(input,4); int domainCount=input.getInt();
        if(domainCount<1 || domainCount>RatisNodePlan.MAX_DOMAINS || domainCount*8L>input.remaining()) throw new IllegalArgumentException("Invalid manifest DATA domain count");
        var schemas=new ArrayList<DistributedSchema>(domainCount); var directories=new ArrayList<RangeDirectory>(domainCount); UUID previousDomain=null; long domainBytes=0;
        for(int i=0;i<domainCount;i++) {
            byte[] schemaBytes=blob(input,(int)Math.min(DistributedSchemaCodec.MAX_BYTES,RatisNodePlan.MAX_DOMAIN_BYTES-domainBytes)); domainBytes+=schemaBytes.length; byte[] directoryBytes=blob(input,(int)Math.min(RangeDirectoryCodec.MAX_ENCODED_BYTES,RatisNodePlan.MAX_DOMAIN_BYTES-domainBytes)); domainBytes+=directoryBytes.length;
            if(domainBytes>RatisNodePlan.MAX_DOMAIN_BYTES) throw new IllegalArgumentException("Manifest DATA metadata exceeds aggregate bound");
            var schema=DistributedSchemaCodec.decode(schemaBytes); var directory=RangeDirectoryCodec.decode(directoryBytes); schema.requireMatches(directory);
            if(previousDomain!=null && UUID_ORDER.compare(previousDomain,directory.domainId())>=0) throw new IllegalArgumentException("Noncanonical manifest DATA domain order"); previousDomain=directory.domainId(); schemas.add(schema); directories.add(directory);
        }
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
        require(input,4+SETTINGS_BYTES); int importCount=input.getInt();
        if(importCount<0 || importCount>RangeImportDefinition.MAX_GROUPS || 4L*importCount>input.remaining()-SETTINGS_BYTES) throw new IllegalArgumentException("Invalid node import count");
        var imports=new ArrayList<RangeImportDefinition>(importCount); var importIds=new HashSet<UUID>(); long importBytes=0;
        for(int i=0;i<importCount;i++) {
            byte[] encoded=blob(input,RangeImportDefinition.MAX_BYTES); importBytes+=encoded.length;
            if(importBytes>RangeImportDefinition.MAX_BYTES) throw new IllegalArgumentException("Node import definitions exceed byte bound");
            var definition=RangeImportDefinition.decode(encoded);
            if(!importIds.add(definition.importId()))throw new IllegalArgumentException("Duplicate node import identity");
            imports.add(definition);
        }
        require(input,4+SETTINGS_BYTES); int streamCount=input.getInt();
        if(streamCount<0 || streamCount>4096 || 8L*streamCount>input.remaining()-SETTINGS_BYTES)throw new IllegalArgumentException("Invalid stream endpoint count");
        var streamAddresses=new LinkedHashMap<String,String>(); String previousPeer=null;
        for(int i=0;i<streamCount;i++){String peer=text(input),endpoint=text(input);if(previousPeer!=null && previousPeer.compareTo(peer)>=0)throw new IllegalArgumentException("Noncanonical stream endpoint order");previousPeer=peer;streamAddresses.put(peer,endpoint);}
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
        var replicaLimits=new RatisSchemaReplicaService.Limits(input.getInt(),input.getLong(),input.getLong(),input.getInt(),input.getInt(),input.getLong());
        var nodeLimits=new RatisNodeResourceBudget.Limits(new RatisNodeResourceBudget.Lane(input.getLong(),input.getInt()),new RatisNodeResourceBudget.Lane(input.getLong(),input.getInt()));
        var poolLimits=new RatisNodeResourceBudget.Limits(new RatisNodeResourceBudget.Lane(input.getLong(),input.getInt()),new RatisNodeResourceBudget.Lane(input.getLong(),input.getInt()));
        long sourceImageDisk=input.getLong(),durableMetadata=input.getLong(),operatorCache=input.getLong(),nativeCache=input.getLong();
        var committed=new RatisCommittedApplyDispatcher.Policy(readCommittedLimits(input),readCommittedLimits(input));var disk=new RatisDiskRunway.Policy(input.getLong(),input.getLong(),input.getLong(),input.getLong(),input.getLong(),input.getLong(),Duration.ofNanos(input.getLong()),input.getInt(),input.getInt(),input.getInt());if (clock<=0 || shutdown<=0 || input.hasRemaining()) throw new IllegalArgumentException("Invalid node manifest durations/trailing data");
        var settings=new RatisNodeRuntime.Settings(images,retention,Duration.ofNanos(clock),visits,ranges,Duration.ofNanos(shutdown),new RatisNodeRuntime.ExportResources(pins,source,io),new RatisNodeRuntime.ResourceLimits(nodeLimits,poolLimits,sourceImageDisk,durableMetadata,operatorCache,nativeCache,committed,disk));
        var recovered=catalogue(catalogue,directories.getFirst(),directoryColumn,sessionsColumn); var domains=new ArrayList<RatisNodePlan.Domain>();
        for(int i=0;i<directories.size();i++) { var directory=directories.get(i); var bindings=new HashMap<UUID,String>(); for(var column:directory.columns()) { var name=columns.get(column); if(name==null) throw new IllegalArgumentException("Manifest DATA physical binding missing"); bindings.put(column,name); } domains.add(new RatisNodePlan.Domain(directory,bindings,mergeColumns(schemas.get(i)))); }
        var plan=new RatisNodePlan(node,incarnation,domains,recovered,homes,imports,streamAddresses,groups,pools,domainPools,groupPools);
        if(!plan.columns().equals(columns)) throw new IllegalArgumentException("Manifest has unrelated physical column bindings");
        var manifest=new RatisNodeManifest(schemas,plan,settings,reads,writes,replicaLimits);
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
        var c=plan.catalogue();
        var range=new RangeDescriptor(c.groupId(),c.groupId(),c.epoch(),0,null,null,RangeDescriptor.TopologyState.ACTIVE,c.replicas());
        return new RangeDirectory(plan.clusterId(),plan.databaseId(),c.groupId(),c.epoch(),c.schemaDigest(),0,
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
    private static RatisCommittedApplyDispatcher.Limits readCommittedLimits(ByteBuffer input){return new RatisCommittedApplyDispatcher.Limits(new RatisCommittedApplyDispatcher.Lane(input.getLong(),input.getInt()),new RatisCommittedApplyDispatcher.Lane(input.getLong(),input.getInt()),new RatisCommittedApplyDispatcher.Lane(input.getLong(),input.getInt()),input.getInt());}
    private static void require(ByteBuffer input,int bytes) { if (input.remaining()<bytes) throw new IllegalArgumentException("Truncated node manifest"); }
}
