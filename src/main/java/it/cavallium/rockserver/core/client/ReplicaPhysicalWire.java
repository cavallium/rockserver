package it.cavallium.rockserver.core.client;

import com.google.protobuf.*;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.api.proto.*;
import it.cavallium.rockserver.core.common.cdc.DistributedCdcWire;
import java.nio.charset.StandardCharsets;
import java.util.*;

/** Bounded, qualified node/operator wire values. No logical totals or routing authority. */
public final class ReplicaPhysicalWire {
    public static final int MAX_FRAME_BYTES=SstMaintenance.MAX_METADATA_RESPONSE_BYTES;
    public static final int MAX_REQUEST_BYTES=2*1024*1024,MAX_TARGETS=65536,MAX_FILES=SstMaintenance.MAX_INPUT_FILES;
    public static final int MAX_STRING_BYTES=65536,MAX_KEY_HEX_BYTES=4*1024*1024;
    private ReplicaPhysicalWire() {}
    public static <T extends MessageLite> T bounded(T value,int maximum){if(value.getSerializedSize()>maximum)throw new IllegalArgumentException("Physical protobuf exceeds byte budget");return value;}
    public static long timeout(ReplicaPhysicalContext value){if(value.getTimeoutNanos()<=0)throw new IllegalArgumentException("Physical timeout must be positive or Long.MAX_VALUE");return value.getTimeoutNanos();}
    public static ReplicaPhysicalContext context(long timeout){var result=ReplicaPhysicalContext.newBuilder().setTimeoutNanos(timeout).build();timeout(result);return result;}
    public static ReplicaPhysicalScope scope(ReplicaPhysicalAPI.Scope value){Objects.requireNonNull(value);var b=ReplicaPhysicalScope.newBuilder();if(value.pool()!=null)b.setPool(DistributedCdcWire.uuid(value.pool()));if(value.column()!=null)b.setColumn(DistributedCdcWire.uuid(value.column()));return b.build();}
    public static ReplicaPhysicalAPI.Scope scope(ReplicaPhysicalScope value){return new ReplicaPhysicalAPI.Scope(value.hasPool()?DistributedCdcWire.uuid(value.getPool()):null,value.hasColumn()?DistributedCdcWire.uuid(value.getColumn()):null);}
    public static ReplicaPhysicalTarget target(ReplicaPhysicalAPI.Target t){return ReplicaPhysicalTarget.newBuilder().setNode(DistributedCdcWire.uuid(t.node())).setIncarnation(DistributedCdcWire.uuid(t.incarnation())).setProcess(DistributedCdcWire.uuid(t.process())).setPool(DistributedCdcWire.uuid(t.pool())).setSession(DistributedCdcWire.uuid(t.session())).setColumn(DistributedCdcWire.uuid(t.column())).setNativeColumnId(t.nativeColumnId()).build();}
    public static ReplicaPhysicalAPI.Target target(ReplicaPhysicalTarget t){return new ReplicaPhysicalAPI.Target(DistributedCdcWire.uuid(t.getNode()),DistributedCdcWire.uuid(t.getIncarnation()),DistributedCdcWire.uuid(t.getProcess()),DistributedCdcWire.uuid(t.getPool()),DistributedCdcWire.uuid(t.getSession()),DistributedCdcWire.uuid(t.getColumn()),t.getNativeColumnId());}
    public static String session(ReplicaPhysicalAPI.Target t){return t.session()+"-"+t.nativeColumnId();}
    private static void inScope(ReplicaPhysicalAPI.Target t,ReplicaPhysicalAPI.Scope s){if(s.pool()!=null && !s.pool().equals(t.pool()) || s.column()!=null && !s.column().equals(t.column()))throw new IllegalArgumentException("Physical response differs from requested scope");}
    private static void count(int count,int maximum){if(count<0 || count>maximum)throw new IllegalArgumentException("Physical count exceeds budget");}
    private static void nonnegative(long value){if(value<0)throw new IllegalArgumentException("Negative physical scalar");}
    private static final class Budget {
        long used;
        void add(long bytes){used=Math.addExact(used,bytes);if(used>MAX_FRAME_BYTES)throw new IllegalArgumentException("Physical projection exceeds byte budget");}
        void string(String value,int maximum){Objects.requireNonNull(value);if(value.length()>maximum)throw new IllegalArgumentException("Physical string exceeds byte budget");int bytes=value.getBytes(StandardCharsets.UTF_8).length;if(bytes>maximum)throw new IllegalArgumentException("Physical string exceeds byte budget");add(16L+Math.max(bytes,2L*value.length()));}
    }
    public static ReplicaPhysicalTargets targets(List<ReplicaPhysicalAPI.Target> values,ReplicaPhysicalAPI.Scope scope){count(values.size(),MAX_TARGETS);var b=ReplicaPhysicalTargets.newBuilder();var seen=new HashSet<ReplicaPhysicalAPI.Target>();for(var t:values){inScope(t,scope);if(!seen.add(t))throw new IllegalArgumentException("Duplicate physical target");b.addTargets(target(t));}return bounded(b.build(),MAX_FRAME_BYTES);}
    public static List<ReplicaPhysicalAPI.Target> targets(ReplicaPhysicalTargets value,ReplicaPhysicalAPI.Scope scope){bounded(value,MAX_FRAME_BYTES);count(value.getTargetsCount(),MAX_TARGETS);var result=value.getTargetsList().stream().map(ReplicaPhysicalWire::target).toList();var seen=new HashSet<ReplicaPhysicalAPI.Target>();for(var t:result){inScope(t,scope);if(!seen.add(t))throw new IllegalArgumentException("Duplicate physical target");}return result;}
    private static void distribution(Map<?,Long> map,long tables,Budget budget){count(map.size(),MAX_FILES);long sum=0;for(var entry:map.entrySet()){if(entry.getValue()<=0)throw new IllegalArgumentException("Nonpositive table-property frequency");sum=Math.addExact(sum,entry.getValue());budget.add(48);if(entry.getKey() instanceof Long n)nonnegative(n);else budget.string((String)entry.getKey(),MAX_STRING_BYTES);}if(sum!=tables)throw new IllegalArgumentException("Table-property distribution differs from table count");}
    private static void properties(ColumnTableProperties p,Budget budget){
        budget.add(512);if(p.tableCount()>MAX_FILES)throw new IllegalArgumentException("Table count exceeds file budget");
        nonnegative(p.tableCount());
        nonnegative(p.dataSize());
        nonnegative(p.indexSize());
        nonnegative(p.indexPartitions());
        nonnegative(p.topLevelIndexSize());
        nonnegative(p.filterSize());
        nonnegative(p.rawKeySize());
        nonnegative(p.rawValueSize());
        nonnegative(p.numDataBlocks());
        nonnegative(p.numEntries());
        nonnegative(p.numDeletions());
        nonnegative(p.numMergeOperands());
        nonnegative(p.numRangeDeletions());
        nonnegative(p.slowCompressionEstimatedDataSize());
        nonnegative(p.fastCompressionEstimatedDataSize());
        nonnegative(p.oldestCreationTime());
        nonnegative(p.newestCreationTime());
        nonnegative(p.oldestKeyTime());
        distribution(p.formatVersions(),p.tableCount(),budget);
        distribution(p.fixedKeyLengths(),p.tableCount(),budget);
        distribution(p.indexKeysAreUserKeys(),p.tableCount(),budget);
        distribution(p.indexValuesAreDeltaEncoded(),p.tableCount(),budget);
        distribution(p.columnFamilyIds(),p.tableCount(),budget);
        distribution(p.filterPolicies(),p.tableCount(),budget);
        distribution(p.comparators(),p.tableCount(),budget);
        distribution(p.mergeOperators(),p.tableCount(),budget);
        distribution(p.prefixExtractors(),p.tableCount(),budget);
        distribution(p.propertyCollectors(),p.tableCount(),budget);
        distribution(p.compressions(),p.tableCount(),budget);
    }
    private static void properties(ColumnTablePropertiesResponse p,Budget budget){
        budget.add(512);if(p.getTableCount()>MAX_FILES)throw new IllegalArgumentException("Table count exceeds file budget");
        nonnegative(p.getTableCount());
        nonnegative(p.getDataSize());
        nonnegative(p.getIndexSize());
        nonnegative(p.getIndexPartitions());
        nonnegative(p.getTopLevelIndexSize());
        nonnegative(p.getFilterSize());
        nonnegative(p.getRawKeySize());
        nonnegative(p.getRawValueSize());
        nonnegative(p.getNumDataBlocks());
        nonnegative(p.getNumEntries());
        nonnegative(p.getNumDeletions());
        nonnegative(p.getNumMergeOperands());
        nonnegative(p.getNumRangeDeletions());
        nonnegative(p.getSlowCompressionEstimatedDataSize());
        nonnegative(p.getFastCompressionEstimatedDataSize());
        nonnegative(p.getOldestCreationTime());
        nonnegative(p.getNewestCreationTime());
        nonnegative(p.getOldestKeyTime());
        distribution(p.getFormatVersionsMap(),p.getTableCount(),budget);
        distribution(p.getFixedKeyLengthsMap(),p.getTableCount(),budget);
        distribution(p.getIndexKeysAreUserKeysMap(),p.getTableCount(),budget);
        distribution(p.getIndexValuesAreDeltaEncodedMap(),p.getTableCount(),budget);
        distribution(p.getColumnFamilyIdsMap(),p.getTableCount(),budget);
        distribution(p.getFilterPoliciesMap(),p.getTableCount(),budget);
        distribution(p.getComparatorsMap(),p.getTableCount(),budget);
        distribution(p.getMergeOperatorsMap(),p.getTableCount(),budget);
        distribution(p.getPrefixExtractorsMap(),p.getTableCount(),budget);
        distribution(p.getPropertyCollectorsMap(),p.getTableCount(),budget);
        distribution(p.getCompressionsMap(),p.getTableCount(),budget);
    }
    private static void observations(List<ReplicaPhysicalAPI.Observation> values,ReplicaPhysicalAPI.Scope scope,Budget budget){count(values.size(),MAX_TARGETS);var seen=new HashSet<ReplicaPhysicalAPI.Target>();for(var o:values){inScope(o.target(),scope);if(!seen.add(o.target()))throw new IllegalArgumentException("Duplicate physical observation");budget.add(256);nonnegative(o.estimatedKeys());budget.string(o.actualCFComparator(),MAX_STRING_BYTES);properties(o.tables(),budget);}}
    public static ReplicaPhysicalObservations observations(List<ReplicaPhysicalAPI.Observation> values,ReplicaPhysicalAPI.Scope scope){observations(values,scope,new Budget());var b=ReplicaPhysicalObservations.newBuilder();for(var o:values)b.addObservations(ReplicaPhysicalObservation.newBuilder().setTarget(target(o.target())).setEstimatedKeys(o.estimatedKeys()).setActualCFComparator(o.actualCFComparator()).setTables(GrpcPortableWire.properties(o.tables())));return bounded(b.build(),MAX_FRAME_BYTES);}
    public static List<ReplicaPhysicalAPI.Observation> observations(ReplicaPhysicalObservations value,ReplicaPhysicalAPI.Scope scope){
        bounded(value,MAX_FRAME_BYTES);count(value.getObservationsCount(),MAX_TARGETS);var budget=new Budget();var seen=new HashSet<ReplicaPhysicalAPI.Target>();
        // Validate the entire projection before copying the portable distribution maps.
        for(var o:value.getObservationsList()){if(!o.hasTarget() || !o.hasTables())throw new IllegalArgumentException("Missing qualified physical observation");var target=target(o.getTarget());inScope(target,scope);if(!seen.add(target))throw new IllegalArgumentException("Duplicate physical observation");budget.add(256);nonnegative(o.getEstimatedKeys());budget.string(o.getActualCFComparator(),MAX_STRING_BYTES);properties(o.getTables(),budget);}
        return value.getObservationsList().stream().map(o->new ReplicaPhysicalAPI.Observation(target(o.getTarget()),o.getEstimatedKeys(),o.getActualCFComparator(),GrpcPortableWire.properties(o.getTables()))).toList();
    }
    private static void basename(String name){if(!name.matches("[0-9]{1,20}\\.sst"))throw new IllegalArgumentException("Invalid SST basename");}
    private static void hex(String value,Budget budget){budget.string(value,MAX_KEY_HEX_BYTES);if((value.length()&1)!=0)throw new IllegalArgumentException("Odd physical key hex length");for(int i=0;i<value.length();i++){char c=value.charAt(i);if(!(c>='0' && c<='9' || c>='a' && c<='f'))throw new IllegalArgumentException("Invalid physical key hex");}}
    private static void validateMetadata(SstMaintenance.Metadata m,ReplicaPhysicalAPI.Target target,int level){
        if(level < -1 || m.columnId()!=target.nativeColumnId() || !m.session().equals(session(target)) || m.numLevels()<1 || m.baseLevel()<0 || m.baseLevel()>=m.numLevels())throw new IllegalArgumentException("Stale/invalid physical SST metadata");count(m.files().size(),MAX_FILES);count(m.paths().size(),MAX_FILES);if(m.paths().isEmpty())throw new IllegalArgumentException("Missing physical SST paths");var budget=new Budget();budget.add(256);budget.string(m.session(),128);budget.string(m.columnName(),MAX_STRING_BYTES);for(var path:m.paths())budget.string(path,MAX_STRING_BYTES);var names=new HashSet<String>();for(var f:m.files()){basename(f.name());if(!names.add(f.name()) || f.level()<0 || f.level()>=m.numLevels() || level!=-1 && f.level()!=level || f.pathId()<0 || f.pathId()>=m.paths().size())throw new IllegalArgumentException("Invalid SST file identity/level/path");nonnegative(f.sizeBytes());budget.add(128);hex(f.smallestKeyHex(),budget);hex(f.largestKeyHex(),budget);if(f.smallestKeyHex().compareTo(f.largestKeyHex())>0)throw new IllegalArgumentException("Reversed physical key bounds");}}
    public static ReplicaPhysicalMetadata metadata(SstMaintenance.Metadata value,ReplicaPhysicalAPI.Target target,int level){validateMetadata(value,target,level);return bounded(ReplicaPhysicalMetadata.newBuilder().setTarget(target(target)).setMetadata(GrpcPortableWire.metadata(value)).build(),MAX_FRAME_BYTES);}
    public static SstMaintenance.Metadata metadata(ReplicaPhysicalMetadata value,ReplicaPhysicalAPI.Target expected,int level){bounded(value,MAX_FRAME_BYTES);if(!value.hasTarget() || !value.hasMetadata() || !target(value.getTarget()).equals(expected))throw new IllegalArgumentException("Crossed physical metadata target");var m=value.getMetadata();count(m.getFilesCount(),MAX_FILES);count(m.getPathsCount(),MAX_FILES);var result=GrpcPortableWire.metadata(m);validateMetadata(result,expected,level);return result;}
    public static void request(ReplicaPhysicalAPI.Target target,SstMaintenance.Request request){if(request.columnId()!=target.nativeColumnId() || !request.session().equals(session(target)))throw new IllegalArgumentException("Stale SST/native CF session");count(request.files().size(),MAX_FILES);bounded(GrpcPortableWire.request(request),MAX_REQUEST_BYTES);}
    private static void result(SstMaintenance.Result result){count(result.inputFiles().size(),MAX_FILES);count(result.outputFiles().size(),MAX_FILES);nonnegative(result.validatedInputBytes());nonnegative(result.outputBytes());nonnegative(result.elapsedNanos());for(var f:result.inputFiles())basename(f);for(var f:result.outputFiles())basename(f);if(new HashSet<>(result.inputFiles()).size()!=result.inputFiles().size() || new HashSet<>(result.outputFiles()).size()!=result.outputFiles().size())throw new IllegalArgumentException("Duplicate compaction files");}
    private static void resultRequest(SstMaintenance.Result result,SstMaintenance.Request request){if(result.executed()!=request.execute())throw new IllegalArgumentException("Physical compaction execution differs from request");if(!result.executed() && (!new HashSet<>(result.inputFiles()).equals(new HashSet<>(request.files())) || !result.outputFiles().isEmpty() || result.outputBytes()!=0))throw new IllegalArgumentException("Invalid physical compaction dry run");}
    public static ReplicaPhysicalCompaction result(SstMaintenance.Result value,ReplicaPhysicalAPI.Target target,SstMaintenance.Request request){result(value);resultRequest(value,request);return bounded(ReplicaPhysicalCompaction.newBuilder().setTarget(target(target)).setResult(GrpcPortableWire.result(value)).build(),MAX_FRAME_BYTES);}
    public static SstMaintenance.Result result(ReplicaPhysicalCompaction value,ReplicaPhysicalAPI.Target expected,SstMaintenance.Request request){bounded(value,MAX_FRAME_BYTES);if(!value.hasTarget() || !value.hasResult() || !target(value.getTarget()).equals(expected))throw new IllegalArgumentException("Crossed physical compaction target");var r=value.getResult();count(r.getInputFilesCount(),MAX_FILES);count(r.getOutputFilesCount(),MAX_FILES);var result=GrpcPortableWire.result(r);result(result);resultRequest(result,request);return result;}
}
