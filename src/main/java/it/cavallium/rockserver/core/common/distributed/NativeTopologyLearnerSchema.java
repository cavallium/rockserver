package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.util.*;

/** Verified source history and receiver-local alias materialization; this allocates no historical capacity claim. */
public final class NativeTopologyLearnerSchema {
    public static final int MAX_BYTES=16*1024*1024,MAX_ROWS=131;
    private NativeTopologyLearnerSchema() {}
    public static boolean row(byte[] key) {return (key.length==17 || key.length==33) && (key[16]==0x27 || key[16]==0x28 || key[16]==0x2a || key[16]==0x2b || key[16]==0x2c);}
    public static Map<String,byte[]> copy(Map<String,byte[]> source) {var result=new TreeMap<String,byte[]>();long size=0;for(var entry:source.entrySet()){byte[] key=HexFormat.of().parseHex(entry.getKey()),value=entry.getValue();if(!row(key) || value==null || (size+=key.length+value.length)>MAX_BYTES || result.size()>=MAX_ROWS)throw new IllegalArgumentException("Learner source schema evidence exceeds bound");result.put(entry.getKey(),value.clone());}return Collections.unmodifiableMap(result);}
    public static Map<String,byte[]> localRows(NativeTopologyImageOrigin origin,RangeDescriptor descriptor,DistributedSchema schema,ApplyPosition cut,Map<UUID,String> names,Set<UUID> merges,Map<String,byte[]> source) {
        var group=descriptor.groupId();var prefix=ByteBuffer.allocate(16).putLong(group.getMostSignificantBits()).putLong(group.getLeastSignificantBits()).array();
        NativeSchemaInstallation current=null;NativeSchemaActivationProof activated=null;NativeSchemaFence pending=null;var history=new ArrayList<NativeSchemaInstallation>();var activations=new HashMap<UUID,NativeSchemaActivationProof>();var allNames=new HashMap<UUID,String>();
        for(var entry:source.entrySet()) {byte[] key=HexFormat.of().parseHex(entry.getKey()),value=entry.getValue();if(!Arrays.equals(key,0,16,prefix,0,16))throw new IllegalArgumentException("Source schema evidence group changed");
            if(key[16]==0x28 || key[16]==0x2a) {var record=NativeSchemaInstallation.decode(value);record.requireRange(group,descriptor.rangeEpoch(),schema.domainId(),descriptor.lowerInclusive(),descriptor.upperExclusive());allNames.putAll(record.sourceNames());allNames.putAll(record.targetNames());if(key[16]==0x28){if(key.length!=17 || current!=null)throw new IllegalArgumentException("Invalid current source installation");current=record;}else {if(key.length!=33 || !uuid(key).equals(record.definition().changeId()))throw new IllegalArgumentException("Source installation key changed");history.add(record);}}
            else if(key[16]==0x27){if(key.length!=17)throw new IllegalArgumentException("Invalid source fence key");pending=NativeSchemaFence.decode(value);}
            else {var proof=NativeSchemaActivationProof.decode(value);if(key[16]==0x2c){if(key.length!=17)throw new IllegalArgumentException("Invalid source activation key");activated=proof;}else if(key.length!=33 || !uuid(key).equals(proof.installation().fence().changeId()) || activations.put(proof.installation().fence().changeId(),proof)!=null)throw new IllegalArgumentException("Source activation key changed");}
        }
        var ordered=NativeSchemaInstallation.validateLifecycle(current,history,activated,activations,pending,cut);
        if(current==null) {if(!source.isEmpty() || origin!=null && (!Arrays.equals(schema.digest(),origin.seed().header().schemaDigest()) || !names.keySet().equals(origin.seed().physicalNames().keySet()) || !merges.equals(origin.seed().mergeColumns())))throw new IllegalArgumentException("Evolved learner source lacks verified schema history");return Map.of();}
        if(activated==null || pending!=null || !Arrays.equals(current.definition().targetSchema().digest(),schema.digest()) || !current.targetMerges().equals(merges))throw new IllegalArgumentException("Learner current schema is not activated");
        var first=ordered.getFirst();if(origin!=null && (!Arrays.equals(first.definition().sourceSchema().digest(),origin.seed().header().schemaDigest()) || !first.sourceNames().equals(origin.seed().physicalNames()) || !first.sourceMerges().equals(origin.seed().mergeColumns()) || first.fence().position().index()<=origin.activation().position().index()))throw new IllegalArgumentException("Learner schema history lacks original birth anchor");
        allNames.putAll(names);var result=new TreeMap<String,byte[]>();for(var entry:source.entrySet()){byte[] key=HexFormat.of().parseHex(entry.getKey()),value=entry.getValue();if(key[16]==0x28 || key[16]==0x2a){var record=NativeSchemaInstallation.decode(value);value=new NativeSchemaInstallation(record.definition(),record.fence(),record.position(),select(allNames,record.sourceNames().keySet()),select(allNames,record.targetNames().keySet()),record.sourceMerges(),record.targetMerges()).encode();}result.put(entry.getKey(),value.clone());}return copy(result);
    }
    private static UUID uuid(byte[] key){var bytes=ByteBuffer.wrap(key,17,16);return new UUID(bytes.getLong(),bytes.getLong());}
    private static Map<UUID,String> select(Map<UUID,String> names,Set<UUID> columns){var result=new HashMap<UUID,String>();for(UUID column:columns)result.put(column,Objects.requireNonNull(names.get(column)));return result;}
}
