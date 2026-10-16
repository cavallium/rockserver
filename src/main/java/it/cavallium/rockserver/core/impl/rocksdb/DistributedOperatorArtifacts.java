package it.cavallium.rockserver.core.impl.rocksdb;

import it.cavallium.rockserver.core.common.distributed.DistributedSchema;
import it.cavallium.rockserver.core.common.distributed.BucketMergeOperand;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.cavallium.rockserver.core.impl.FFMAbstractMergeOperator;
import it.cavallium.rockserver.core.impl.MergeOperatorRegistry;
import it.cavallium.rockserver.core.impl.MergeOperatorRegistry.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.*;
import java.util.*;

/** Bounded trusted local artifacts and CF birth identities, readable before RocksDB opens. */
public final class DistributedOperatorArtifacts {
    private static final Object FILES=new Object();
    private static final int MAX_ARTIFACTS=64,MAX_BINDINGS=4096,MAX_INDEX_BYTES=8*1024*1024,MAX_DESCRIPTOR_BYTES=64*1024;
    public static final long MAX_JAR_PAYLOAD_BYTES=128L*1024*1024;
    public static final long MAX_CACHE_METADATA_BYTES=2L*MAX_INDEX_BYTES;
    public static final long MAX_PENDING_BYTES=MAX_INDEX_BYTES;
    public static final int MAX_PENDING_FILES=1;
    public static final long MAX_CACHE_BYTES=MAX_JAR_PAYLOAD_BYTES+MAX_CACHE_METADATA_BYTES+MAX_PENDING_BYTES;
    private static final long MAX_ARTIFACT_BYTES=MAX_JAR_PAYLOAD_BYTES;
    @FunctionalInterface interface Force { void force(Path path) throws IOException; }
    private final Force forcing;
    private boolean pendingUncertain;
    public record Inventory(long jarPayloadBytes,long metadataBytes,long pendingBytes,int artifacts,int descriptors,int pendingFiles) {}
    /** Bounds every cache file, including unreferenced immutable artifacts and interrupted writes. */
    public static Inventory inventory(Path poolPath) throws IOException {
        Path absolute=poolPath.toAbsolutePath().normalize();
        if(absolute.getFileName()==null) throw new IllegalArgumentException("Pool needs a named path");
        synchronized(FILES) { return inventoryDirectory(absolute.resolveSibling(absolute.getFileName()+".operators")); }
    }
    private static Inventory inventoryDirectory(Path directory) throws IOException {
        if(!Files.exists(directory,LinkOption.NOFOLLOW_LINKS)) return new Inventory(0,0,0,0,0,0);
        if(Files.isSymbolicLink(directory) || !Files.isDirectory(directory,LinkOption.NOFOLLOW_LINKS)) throw new IOException("Invalid operator cache directory");
        long jars=0,descriptorsBytes=0,bindingsBytes=0,pendingBytes=0;int artifacts=0,descriptors=0,pending=0,files=0;
        try(var children=Files.newDirectoryStream(directory)) {
            for(var file:children) {
                if(++files>2*MAX_ARTIFACTS+MAX_PENDING_FILES+2 || Files.isSymbolicLink(file) || !Files.isRegularFile(file,LinkOption.NOFOLLOW_LINKS)) throw new IOException("Invalid or unbounded operator cache inventory");
                String name=file.getFileName().toString();long size=Files.size(file);
                if(name.matches("[0-9a-f]{64}\\.jar")) {
                    if(size<1 || size>MergeOperatorRegistry.MAX_DISTRIBUTED_JAR_BYTES || ++artifacts>MAX_ARTIFACTS || size>MAX_ARTIFACT_BYTES-jars) throw new IOException("Operator JAR payload exceeds cache ceiling");
                    if(!name.equals(fileDigest(file,size)+".jar")) throw new IOException("Operator JAR filename differs from immutable payload");
                    jars+=size;
                } else if(name.matches("[0-9a-f]{64}\\.operator")) {
                    if(size<1 || size>MAX_DESCRIPTOR_BYTES || ++descriptors>MAX_ARTIFACTS || size>MAX_INDEX_BYTES-descriptorsBytes) throw new IOException("Operator descriptor inventory exceeds cache ceiling");
                    descriptorsBytes+=size;
                    byte[] bytes=read(file,MAX_DESCRIPTOR_BYTES);
                    if(!name.equals(HexFormat.of().formatHex(sha(bytes))+".operator")) throw new IOException("Operator descriptor filename differs from canonical body");
                    var descriptor=decodeDescriptor(bytes);
                    Path artifact=directory.resolve(descriptor.artifactSha256()+".jar");
                    if(!Files.isRegularFile(artifact,LinkOption.NOFOLLOW_LINKS) || Files.isSymbolicLink(artifact)) throw new IOException("Operator descriptor lacks immutable JAR payload");
                } else if(name.equals("bindings")) {
                    if(size>MAX_INDEX_BYTES) throw new IOException("Operator bindings exceed cache ceiling");bindingsBytes=size;
                } else if(name.equals("lock")) {
                    if(size!=0) throw new IOException("Operator cache lock contains foreign data");
                } else if(name.matches("\\.operator-[0-9]{1,20}\\.tmp")) {
                    if(++pending>MAX_PENDING_FILES || size>MAX_PENDING_BYTES-pendingBytes) throw new IOException("Operator pending files exceed cache ceiling");pendingBytes+=size;
                } else throw new IOException("Foreign operator cache file: "+name);
            }
        }
        return new Inventory(jars,descriptorsBytes+bindingsBytes,pendingBytes,artifacts,descriptors,pending);
    }

    private static String fileDigest(Path file,long expected) throws IOException {
        MessageDigest digest;
        try { digest=MessageDigest.getInstance("SHA-256"); } catch(NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
        long total=0;byte[] buffer=new byte[8192];
        try(var input=Files.newInputStream(file,LinkOption.NOFOLLOW_LINKS)) {
            for(int n;(n=input.read(buffer))!=-1;) { total+=n;if(total>expected)throw new IOException("Operator cache file grew during inventory");digest.update(buffer,0,n); }
        }
        if(total!=expected)throw new IOException("Operator cache file changed during inventory");return HexFormat.of().formatHex(digest.digest());
    }
    private final Path directory,index;
    private byte[] bootstrap;
    private Map<String,Binding> bindings;
    private boolean dirty;
    private record BucketShape(List<Integer> keys,List<ColumnHashType> tails,boolean hasValue) {
        BucketShape { keys=List.copyOf(keys); tails=List.copyOf(tails); if(tails.isEmpty() || !hasValue) throw new IllegalArgumentException("Invalid bucket callback shape"); }
        ColumnSchema schema() { return new ColumnSchema(new IntArrayList(keys),new ObjectArrayList<>(tails),hasValue,null,null,null); }
    }
    private record Binding(UUID column,DistributedOperatorDescriptor operator,BucketShape bucket) {}
    private static BucketShape bucketShape(DistributedSchema.Column column) {
        var shape=column.schema(); return column.operator()==null || shape.variableLengthKeysCount()==0 ? null : new BucketShape(shape.keys(),shape.variableTailKeys(),shape.hasValue());
    }
    DistributedOperatorArtifacts(Path poolPath) throws IOException { this(poolPath,DistributedOperatorArtifacts::diskForce); }
    DistributedOperatorArtifacts(Path poolPath,Force forcing) throws IOException {
        this.forcing=Objects.requireNonNull(forcing);
        Path absolute=poolPath.toAbsolutePath().normalize();
        if(absolute.getFileName()==null) throw new IllegalArgumentException("Pool needs a named path");
        directory=absolute.resolveSibling(absolute.getFileName()+".operators"); index=directory.resolve("bindings");
        synchronized(FILES) { inventoryDirectory(directory);pendingUncertain=Files.exists(directory,LinkOption.NOFOLLOW_LINKS);bindings=readBindings(); }
    }
    boolean qualified() { return Files.exists(index,LinkOption.NOFOLLOW_LINKS); }
    Path directory() { return directory; }
    static DistributedOperatorDescriptor descriptor(DistributedSchema.Operator operator,DeterministicContract contract) {
        if(contract==null || !contract.admitted() || !operator.deterministicContract().equals(contract.identifier()) || operator.deterministicContractVersion()!=contract.version()) throw new IllegalArgumentException("Operator contract lacks matching trusted admission");
        return new DistributedOperatorDescriptor(operator.name(),operator.version(),HexFormat.of().formatHex(operator.artifactSha256()),operator.className(),operator.valueCodecVersion(),operator.operandCodecVersion(),contract);
    }
    void stage(DistributedSchema.Operator operator,DeterministicContract contract,byte[] input) throws IOException {
        var descriptor=descriptor(operator,contract);
        if(input.length==0 || input.length>MergeOperatorRegistry.MAX_DISTRIBUTED_JAR_BYTES) throw new IllegalArgumentException("Artifact size exceeds budget");
        byte[] jar=input.clone();
        try(var construction=FFMAbstractMergeOperator.trackConstruction()) { MergeOperatorRegistry.instantiateDistributed(descriptor,jar); }
        synchronized(FILES) {
            ensureDirectory();
            try(var lock=FileChannel.open(directory.resolve("lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE);var ignored=lock.lock()) {
                preparePending(0);
                int count=0; long bytes=0;
                try(var files=Files.newDirectoryStream(directory,"*.jar")) { for(var file:files) { count++; bytes=Math.addExact(bytes,Files.size(file)); } }
                Path artifact=artifact(descriptor); boolean exists=Files.exists(artifact,LinkOption.NOFOLLOW_LINKS);
                if(!exists && (count>=MAX_ARTIFACTS || jar.length>MAX_ARTIFACT_BYTES-bytes)) throw new IllegalStateException("Artifact cache capacity exhausted");
                int admissions=0; long metadata=0;
                try(var files=Files.newDirectoryStream(directory,"*.operator")) { for(var file:files) { admissions++; metadata=Math.addExact(metadata,Files.size(file)); } }
                byte[] encoded=encodeDescriptor(descriptor);
                if(!Files.exists(descriptorFile(descriptor),LinkOption.NOFOLLOW_LINKS) && (admissions>=MAX_ARTIFACTS || encoded.length>MAX_INDEX_BYTES-metadata)) throw new IllegalStateException("Operator admission cache capacity exhausted");
                immutable(artifact,jar,MergeOperatorRegistry.MAX_DISTRIBUTED_JAR_BYTES);
                immutable(descriptorFile(descriptor),encodeDescriptor(descriptor),MAX_DESCRIPTOR_BYTES);
            }
        }
    }
    private Path artifact(DistributedOperatorDescriptor descriptor) { return directory.resolve(descriptor.artifactSha256()+".jar"); }
    private Path descriptorFile(DistributedOperatorDescriptor descriptor) { return directory.resolve(HexFormat.of().formatHex(sha(encodeDescriptor(descriptor)))+".operator"); }
    DistributedOperatorDescriptor require(DistributedSchema.Operator operator) throws IOException {
        var descriptor=descriptor(operator,new DeterministicContract(operator.deterministicContract(),operator.deterministicContractVersion(),true));
        byte[] stored=read(descriptorFile(descriptor),MAX_DESCRIPTOR_BYTES);
        if(!Arrays.equals(stored,encodeDescriptor(descriptor))) throw new IOException("Trusted operator descriptor mismatch");
        byte[] jar=read(artifact(descriptor),MergeOperatorRegistry.MAX_DISTRIBUTED_JAR_BYTES);
        if(!descriptor.artifactSha256().equals(HexFormat.of().formatHex(sha(jar)))) throw new IOException("Operator artifact digest mismatch");
        return descriptor;
    }
    FFMAbstractMergeOperator instantiate(DistributedOperatorDescriptor descriptor) throws IOException {
        if(!Arrays.equals(read(descriptorFile(descriptor),MAX_DESCRIPTOR_BYTES),encodeDescriptor(descriptor))) throw new IOException("Operator descriptor missing or changed");
        return MergeOperatorRegistry.instantiateDistributed(descriptor,read(artifact(descriptor),MergeOperatorRegistry.MAX_DISTRIBUTED_JAR_BYTES));
    }
    FFMAbstractMergeOperator resolve(String name) throws IOException {
        var binding=bindings.get(name); if(binding==null) throw new IOException("Physical CF lacks qualified birth binding: "+name);
        return instantiate(binding);
    }
    private FFMAbstractMergeOperator instantiate(Binding binding) throws IOException {
        if(binding.operator()==null) return null;
        try(var construction=FFMAbstractMergeOperator.trackConstruction()) {
            var delegate=instantiate(binding.operator());
            var operator=binding.bucket()==null ? delegate : new DistributedBucketMergeOperator(binding.bucket().schema(),delegate);
            construction.adopt(operator); return operator;
        }
    }
    FFMAbstractMergeOperator instantiate(DistributedSchema.Column column) throws IOException {
        if(column.operator()==null) return null;
        return instantiate(new Binding(column.columnId(),require(column.operator()),bucketShape(column)));
    }
    boolean contains(String name) { return bindings.containsKey(name); }
    private static void add(Map<String,Binding> next,UUID id,String name,DistributedOperatorDescriptor operator) { add(next,id,name,operator,null); }
    private static void add(Map<String,Binding> next,UUID id,String name,DistributedOperatorDescriptor operator,BucketShape bucket) {
        Objects.requireNonNull(id); Objects.requireNonNull(name);
        if(name.isEmpty() || name.length()>4096 || name.indexOf('\0')>=0 || !StandardCharsets.US_ASCII.newEncoder().canEncode(name) || DistributedStoragePool.isReservedColumn(name)) throw new IllegalArgumentException("Invalid physical CF name");
        var binding=new Binding(id,operator,bucket); var previous=next.putIfAbsent(name,binding);
        if(previous!=null && !previous.equals(binding) || next.entrySet().stream().anyMatch(entry -> !entry.getKey().equals(name) && entry.getValue().column().equals(id))) throw new IllegalArgumentException("Qualified physical CF birth changed");
    }
    private Map<String,Binding> proposed(DistributedSchema schema,Map<UUID,String> names,Map<UUID,String> plain) throws IOException {
        if(!schema.columnIds().equals(names.keySet()) || !Collections.disjoint(names.keySet(),plain.keySet())) throw new IllegalArgumentException("Bootstrap schema/CF coverage mismatch");
        var next=new HashMap<>(bindings);
        // Validate all aliases and reserved names before inspecting artifacts or changing memory.
        for(var column:schema.columns()) add(next,column.columnId(),names.get(column.columnId()),column.operator()==null ? null : descriptor(column.operator(),new DeterministicContract(column.operator().deterministicContract(),column.operator().deterministicContractVersion(),true)),bucketShape(column));
        for(var entry:plain.entrySet()) add(next,entry.getKey(),entry.getValue(),null);
        for(var column:schema.columns()) if(column.operator()!=null) require(column.operator());
        encodedBindings(next,bootstrap); return Map.copyOf(next);
    }
    private static List<DistributedSchema> canonicalBootstrap(Collection<DistributedSchema> schemas,Map<UUID,String> names,Map<UUID,String> plain) {
        Objects.requireNonNull(schemas);Objects.requireNonNull(names);Objects.requireNonNull(plain);if(schemas.isEmpty() || schemas.size()>MAX_BINDINGS)throw new IllegalArgumentException("Invalid qualified bootstrap schema count");
        var ordered=schemas.stream().map(Objects::requireNonNull).sorted(Comparator.comparing(schema->schema.domainId().toString())).toList();var first=ordered.getFirst();var domains=new HashSet<UUID>();var columns=new HashSet<UUID>();var apiIds=new HashSet<Long>();var logicalNames=new HashSet<String>();
        for(var schema:ordered){if(!first.clusterId().equals(schema.clusterId()) || !first.databaseId().equals(schema.databaseId()) || !domains.add(schema.domainId()))throw new IllegalArgumentException("Bootstrap domain/database identity collision");for(var column:schema.columns())if(!columns.add(column.columnId()) || !apiIds.add(column.apiId()) || !logicalNames.add(column.name()))throw new IllegalArgumentException("Bootstrap column UUID/API ID/name collision");}
        if(!columns.equals(names.keySet()) || !Collections.disjoint(columns,plain.keySet()) || columns.size()+(long)plain.size()>MAX_BINDINGS)throw new IllegalArgumentException("Aggregate bootstrap schema/CF coverage mismatch");var physical=new HashSet<String>();for(var mapping:List.of(names,plain))for(var name:mapping.values())if(name==null || name.isEmpty() || name.length()>4096 || name.indexOf('\0')>=0 || !StandardCharsets.US_ASCII.newEncoder().canEncode(name) || DistributedStoragePool.isReservedColumn(name) || !physical.add(name))throw new IllegalArgumentException("Aggregate bootstrap physical-name collision");
        return ordered;
    }
    void qualifyBootstrap(Collection<DistributedSchema> schemas,Map<UUID,String> names,Map<UUID,String> plain,boolean virgin) throws IOException {
        names=Map.copyOf(names);plain=Map.copyOf(plain);var ordered=canonicalBootstrap(List.copyOf(schemas),names,plain);byte[] identity=bootstrapIdentity(ordered,names,plain);
        if(!virgin) {
            if(bootstrap==null || !Arrays.equals(bootstrap,identity))throw new IOException("Original qualified aggregate bootstrap identity changed");
            for(var schema:ordered){var domainNames=new HashMap<UUID,String>();for(var column:schema.columnIds())domainNames.put(column,names.get(column));requireSchemaBindings(schema,domainNames);}for(var entry:plain.entrySet())requireBirth(entry.getKey(),entry.getValue(),false);return;
        }
        if(bootstrap!=null && !Arrays.equals(bootstrap,identity))throw new IOException("Original qualified aggregate bootstrap identity changed");
        var next=new HashMap<>(bindings);
        for(var schema:ordered)for(var column:schema.columns())add(next,column.columnId(),names.get(column.columnId()),column.operator()==null ? null : descriptor(column.operator(),new DeterministicContract(column.operator().deterministicContract(),column.operator().deterministicContractVersion(),true)),bucketShape(column));for(var entry:plain.entrySet())add(next,entry.getKey(),entry.getValue(),null);
        for(var schema:ordered)for(var column:schema.columns())if(column.operator()!=null)require(column.operator());encodedBindings(next,identity);bootstrap=identity;bindings=Map.copyOf(next);dirty=true;forceBindings();
    }
    void validate(DistributedSchema schema,Map<UUID,String> names) throws IOException { proposed(schema,names,Map.of()); }
    void validateCapacity(Collection<SchemaCapacityClaims.Claim> claims,Map<UUID,String> permanentPlainBirths) throws IOException {
        var next=new HashMap<>(bindings);
        for(var entry:permanentPlainBirths.entrySet()) add(next,entry.getKey(),entry.getValue(),null);
        for(var claim:claims) if(claim.outstanding()) {
            for(var schema:List.of(claim.definition().sourceSchema(),claim.definition().targetSchema())) {
                var names=schema==claim.definition().sourceSchema() ? claim.sourceNames() : claim.targetNames();
                for(var column:schema.columns()) {
                    var operator=column.operator()==null ? null : require(column.operator()); add(next,column.columnId(),names.get(column.columnId()),operator,bucketShape(column));
                }
            }
        }
        encodedBindings(next,bootstrap);
    }
    void bindPermanentPlainBirths(Map<UUID,String> births) throws IOException {
        var next=new HashMap<>(bindings); for(var entry:births.entrySet()) add(next,entry.getKey(),entry.getValue(),null);
        encodedBindings(next,bootstrap); if(!next.equals(bindings)) { bindings=Map.copyOf(next); dirty=true; }
        if(dirty || !qualified()) forceBindings();
    }
    void bind(DistributedSchema schema,Map<UUID,String> names) throws IOException {
        var next=proposed(schema,names,Map.of());
        if(!next.equals(bindings)) { bindings=next; dirty=true; }
        if(dirty || !qualified()) forceBindings();
    }
    void requireBirth(UUID column,String name,boolean merge) throws IOException {
        var binding=bindings.get(name);
        if(binding==null || !column.equals(binding.column()) || merge!=(binding.operator()!=null)) throw new IOException("Native CF birth differs from trusted binding");
    }
    void requireSchemaBindings(DistributedSchema schema,Map<UUID,String> names) throws IOException { requireSchemaBindings(schema,names,null); }
    void requireSchemaBindings(DistributedSchema schema,Map<UUID,String> names,Set<String> needed) throws IOException {
        if(!schema.columnIds().equals(names.keySet())) throw new IOException("Native schema physical coverage changed");
        for(var column:schema.columns()) {
            if(needed!=null && !needed.contains(names.get(column.columnId()))) continue;
            var expected=column.operator()==null ? null : descriptor(column.operator(),new DeterministicContract(column.operator().deterministicContract(),column.operator().deterministicContractVersion(),true));
            var actual=bindings.get(names.get(column.columnId()));
            if(actual==null || !actual.column().equals(column.columnId()) || !Objects.equals(actual.operator(),expected) || !Objects.equals(actual.bucket(),bucketShape(column))) {
                var retainedAlias=bindings.entrySet().stream().filter(entry->entry.getValue().column().equals(column.columnId())).map(Map.Entry::getKey).findFirst().orElse(null);
                throw new IOException("Native schema differs from trusted operator tuple: column="+column.columnId()+", alias="+names.get(column.columnId())+", retainedAlias="+retainedAlias+", bindingPresent="+(actual!=null)+", columnMatches="+(actual!=null && actual.column().equals(column.columnId()))+", operatorMatches="+(actual!=null && Objects.equals(actual.operator(),expected))+", bucketMatches="+(actual!=null && Objects.equals(actual.bucket(),bucketShape(column))));
            }
        }
    }
    /** Historical META aliases are donor evidence; qualify semantics without granting CF ownership. */
    void requireMetadataSchemaBindings(DistributedSchema schema)throws IOException {
        for(var column:schema.columns()){
            var expected=column.operator()==null?null:require(column.operator());
            for(var binding:bindings.values())if(binding.column().equals(column.columnId()) && (!Objects.equals(binding.operator(),expected) || !Objects.equals(binding.bucket(),bucketShape(column))))throw new IOException("Metadata schema differs from qualified local operator/bucket tuple: "+column.columnId());
        }
    }
    private static byte[] encodedBindings(Map<String,Binding> bindings,byte[] bootstrap) throws IOException {
        if(bindings.size()>MAX_BINDINGS) throw new IllegalStateException("Qualified CF binding count exceeds budget");
        var bytes=new ByteArrayOutputStream(); var out=new DataOutputStream(bytes);
        out.writeInt(0x4f504249); out.writeBoolean(bootstrap!=null); if(bootstrap!=null) out.write(bootstrap); out.writeInt(bindings.size());
        for(var name:bindings.keySet().stream().sorted().toList()) {
            var binding=bindings.get(name); out.writeUTF(name); out.writeLong(binding.column().getMostSignificantBits()); out.writeLong(binding.column().getLeastSignificantBits());
            out.writeBoolean(binding.operator()!=null); if(binding.operator()!=null) { byte[] descriptor=encodeDescriptor(binding.operator()); out.writeInt(descriptor.length); out.write(descriptor); }
            out.writeBoolean(binding.bucket()!=null); if(binding.bucket()!=null) {
                out.writeInt(DistributedBucketMergeOperator.VALUE_FORMAT_ID); out.writeInt(BucketMergeOperand.FORMAT_ID);
                out.writeInt(binding.bucket().keys().size()); for(int length:binding.bucket().keys()) out.writeInt(length);
                out.writeInt(binding.bucket().tails().size()); for(var hash:binding.bucket().tails()) out.writeUTF(hash.name()); out.writeBoolean(binding.bucket().hasValue());
            }
        }
        out.flush(); if(bytes.size()>MAX_INDEX_BYTES) throw new IllegalStateException("Qualified CF binding bytes exceed budget"); return bytes.toByteArray();
    }
    private void forceBindings() throws IOException {
        var bytes=encodedBindings(bindings,bootstrap); dirty=true;
        synchronized(FILES) {
            ensureDirectory();
            try(var lock=FileChannel.open(directory.resolve("lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE);var ignored=lock.lock()) {
                byte[] proposedBootstrap=bootstrap; Map<String,Binding> previous; byte[] savedBootstrap;
                try { bootstrap=null; previous=readBindings(); savedBootstrap=bootstrap; } finally { bootstrap=proposedBootstrap; }
                if(savedBootstrap!=null && !Arrays.equals(savedBootstrap,bootstrap)) throw new IOException("Concurrent bootstrap identity changed");
                for(var entry:previous.entrySet()) if(!entry.getValue().equals(bindings.get(entry.getKey()))) throw new IOException("Concurrent immutable CF binding changed");
                replace(index,bytes);
            }
        }
        dirty=false;
    }
    private Map<String,Binding> readBindings() throws IOException {
        if(!Files.exists(index,LinkOption.NOFOLLOW_LINKS)) return Map.of();
        var in=new DataInputStream(new ByteArrayInputStream(read(index,MAX_INDEX_BYTES)));
        if(in.readInt()!=0x4f504249) throw new IOException("Invalid operator binding tag");
        if(in.readBoolean()) bootstrap=in.readNBytes(32);
        int count=in.readInt(); if(count<0 || count>MAX_BINDINGS) throw new IOException("Qualified CF count exceeds budget");
        var result=new HashMap<String,Binding>(); var columns=new HashSet<UUID>();
        for(int i=0;i<count;i++) {
            String name=in.readUTF(); UUID column=new UUID(in.readLong(),in.readLong()); DistributedOperatorDescriptor operator=null;
            if(in.readBoolean()) { int length=in.readInt(); if(length<1 || length>MAX_DESCRIPTOR_BYTES || length>in.available()) throw new IOException("Invalid descriptor length"); operator=decodeDescriptor(in.readNBytes(length)); }
            BucketShape bucket=null;
            if(in.readBoolean()) {
                if(operator==null || in.readInt()!=DistributedBucketMergeOperator.VALUE_FORMAT_ID || in.readInt()!=BucketMergeOperand.FORMAT_ID) throw new IOException("Bucket callback codec identity changed");
                int keyCount=in.readInt(); if(keyCount<1 || keyCount>it.cavallium.rockserver.core.common.distributed.DistributedSchemaCodec.MAX_KEY_COMPONENTS) throw new IOException("Invalid bucket callback key count");
                var keys=new ArrayList<Integer>(keyCount); for(int k=0;k<keyCount;k++) keys.add(in.readInt());
                int tailCount=in.readInt(); if(tailCount<1 || tailCount>keyCount) throw new IOException("Invalid bucket callback tail count");
                var tails=new ArrayList<ColumnHashType>(tailCount); for(int k=0;k<tailCount;k++) tails.add(ColumnHashType.valueOf(in.readUTF()));
                bucket=new BucketShape(keys,tails,in.readBoolean()); bucket.schema();
            }
            if(name.isEmpty() || name.length()>4096 || name.indexOf('\0')>=0 || !StandardCharsets.US_ASCII.newEncoder().canEncode(name) || DistributedStoragePool.isReservedColumn(name) || !columns.add(column) || result.put(name,new Binding(column,operator,bucket))!=null) throw new IOException("Invalid qualified CF binding");
        }
        if(in.available()!=0) throw new IOException("Trailing qualified binding bytes"); return Map.copyOf(result);
    }
    private static byte[] bootstrapIdentity(List<DistributedSchema> schemas,Map<UUID,String> names,Map<UUID,String> plain) throws IOException {
        var bytes=new ByteArrayOutputStream(); var out=new DataOutputStream(bytes); out.writeInt(schemas.size());for(var schema:schemas)out.write(schema.digest());
        for(var mapping:List.of(names,plain)) {
            out.writeInt(mapping.size()); for(var id:mapping.keySet().stream().sorted().toList()) { out.writeLong(id.getMostSignificantBits()); out.writeLong(id.getLeastSignificantBits()); out.writeUTF(mapping.get(id)); }
        }
        out.flush(); return sha(bytes.toByteArray());
    }
    private static byte[] encodeDescriptor(DistributedOperatorDescriptor descriptor) {
        try {
            var bytes=new ByteArrayOutputStream(); var out=new DataOutputStream(bytes); out.writeUTF(descriptor.name()); out.writeLong(descriptor.version()); out.writeUTF(descriptor.artifactSha256()); out.writeUTF(descriptor.className());
            out.writeInt(descriptor.valueCodecVersion()); out.writeInt(descriptor.operandCodecVersion()); out.writeUTF(descriptor.contract().identifier()); out.writeInt(descriptor.contract().version()); out.writeBoolean(descriptor.contract().admitted()); out.flush(); return bytes.toByteArray();
        } catch(IOException impossible) { throw new AssertionError(impossible); }
    }
    private static DistributedOperatorDescriptor decodeDescriptor(byte[] bytes) throws IOException {
        var in=new DataInputStream(new ByteArrayInputStream(bytes));
        var descriptor=new DistributedOperatorDescriptor(in.readUTF(),in.readLong(),in.readUTF(),in.readUTF(),in.readInt(),in.readInt(),new DeterministicContract(in.readUTF(),in.readInt(),in.readBoolean()));
        if(in.available()!=0 || descriptor.contract()==null || !descriptor.contract().admitted() || !Arrays.equals(bytes,encodeDescriptor(descriptor))) throw new IOException("Invalid trusted operator descriptor"); return descriptor;
    }
    private void ensureDirectory() throws IOException {
        if(Files.exists(directory,LinkOption.NOFOLLOW_LINKS) && !Files.isDirectory(directory,LinkOption.NOFOLLOW_LINKS)) throw new IOException("Artifact cache is not a directory");
        Files.createDirectories(directory); force(directory.getParent());
    }
    private static byte[] read(Path path,int maximum) throws IOException {
        if(!Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS) || Files.size(path)>maximum) throw new IOException("Artifact missing, changed or exceeds budget: "+path.getFileName());
        try(var channel=FileChannel.open(path,StandardOpenOption.READ,LinkOption.NOFOLLOW_LINKS)) {
            if(channel.size()>maximum) throw new IOException("Artifact exceeds budget"); var bytes=ByteBuffer.allocate((int)channel.size()); while(bytes.hasRemaining()) if(channel.read(bytes)<0) throw new EOFException("Artifact shortened"); if(channel.size()!=bytes.capacity()) throw new IOException("Artifact changed while reading"); return bytes.array();
        }
    }
    private void immutable(Path destination,byte[] bytes,int maximum) throws IOException {
        if(Files.exists(destination,LinkOption.NOFOLLOW_LINKS)) { if(!Arrays.equals(read(destination,maximum),bytes)) throw new IOException("Immutable staged artifact changed"); force(destination.getParent()); return; }
        Path temporary=writeTemporary(destination,bytes);
        try { Files.createLink(destination,temporary); force(destination.getParent()); }
        finally { Files.deleteIfExists(temporary); force(destination.getParent()); pendingUncertain=false; }
    }
    private void replace(Path destination,byte[] bytes) throws IOException {
        Path temporary=writeTemporary(destination,bytes);
        try { Files.move(temporary,destination,StandardCopyOption.ATOMIC_MOVE,StandardCopyOption.REPLACE_EXISTING); force(destination.getParent()); }
        finally { Files.deleteIfExists(temporary); force(destination.getParent()); pendingUncertain=false; }
    }
    private void preparePending(long futureBytes) throws IOException {
        if(!Thread.holdsLock(FILES)) throw new IllegalStateException("Operator pending writes require exclusive cache ownership");
        var inventory=inventoryDirectory(directory);
        if(futureBytes<0 || futureBytes>MAX_PENDING_BYTES) throw new IOException("Operator pending write exceeds cache ceiling");
        if(inventory.pendingFiles()!=0 || pendingUncertain) {
            pendingUncertain=true;
            try(var pending=Files.newDirectoryStream(directory,".operator-*.tmp")) { for(var file:pending) Files.delete(file); }
            force(directory);pendingUncertain=false;
        }
        if(inventoryDirectory(directory).pendingFiles()!=0) throw new IOException("Operator pending cleanup has not drained");
    }
    private Path writeTemporary(Path destination,byte[] bytes) throws IOException {
        preparePending(bytes.length);pendingUncertain=true;
        var temporary=Files.createTempFile(destination.getParent(),".operator-",".tmp");
        try(var channel=FileChannel.open(temporary,StandardOpenOption.WRITE,StandardOpenOption.TRUNCATE_EXISTING)) { var buffer=ByteBuffer.wrap(bytes); while(buffer.hasRemaining()) channel.write(buffer); channel.force(true); }
        catch(Throwable failure) {
            try { Files.deleteIfExists(temporary);force(destination.getParent());pendingUncertain=false; }
            catch(Throwable cleanup) { if(cleanup!=failure)failure.addSuppressed(cleanup); }
            throw failure;
        } return temporary;
    }
    private void force(Path directory) throws IOException { forcing.force(directory); }
    private static void diskForce(Path directory) throws IOException { try(var channel=FileChannel.open(directory,StandardOpenOption.READ)) { channel.force(true); } }
    private static byte[] sha(byte[] bytes) { try { return MessageDigest.getInstance("SHA-256").digest(bytes); } catch(NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); } }
}
