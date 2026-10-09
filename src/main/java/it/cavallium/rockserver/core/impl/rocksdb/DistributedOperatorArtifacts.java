package it.cavallium.rockserver.core.impl.rocksdb;

import it.cavallium.rockserver.core.common.distributed.DistributedSchema;
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
final class DistributedOperatorArtifacts {
    private static final Object FILES=new Object();
    private static final int MAX_ARTIFACTS=64,MAX_BINDINGS=4096,MAX_INDEX_BYTES=8*1024*1024,MAX_DESCRIPTOR_BYTES=64*1024;
    private static final long MAX_ARTIFACT_BYTES=128L*1024*1024;
    private final Path directory,index;
    private byte[] bootstrap;
    private Map<String,Binding> bindings;
    private boolean dirty;
    private record Binding(UUID column,DistributedOperatorDescriptor operator) {}
    DistributedOperatorArtifacts(Path poolPath) throws IOException {
        Path absolute=poolPath.toAbsolutePath().normalize();
        if(absolute.getFileName()==null) throw new IllegalArgumentException("Pool needs a named path");
        directory=absolute.resolveSibling(absolute.getFileName()+".operators"); index=directory.resolve("bindings"); bindings=readBindings();
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
                try(var abandoned=Files.newDirectoryStream(directory,".operator-*.tmp")) { for(var file:abandoned) Files.delete(file); }
                force(directory);
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
        return binding.operator()==null ? null : instantiate(binding.operator());
    }
    boolean contains(String name) { return bindings.containsKey(name); }
    private static void add(Map<String,Binding> next,UUID id,String name,DistributedOperatorDescriptor operator) {
        Objects.requireNonNull(id); Objects.requireNonNull(name);
        if(name.isEmpty() || name.length()>4096 || name.indexOf('\0')>=0 || !StandardCharsets.US_ASCII.newEncoder().canEncode(name) || DistributedStoragePool.isReservedColumn(name)) throw new IllegalArgumentException("Invalid physical CF name");
        var binding=new Binding(id,operator); var previous=next.putIfAbsent(name,binding);
        if(previous!=null && !previous.equals(binding) || next.entrySet().stream().anyMatch(entry -> !entry.getKey().equals(name) && entry.getValue().column().equals(id))) throw new IllegalArgumentException("Qualified physical CF birth changed");
    }
    private Map<String,Binding> proposed(DistributedSchema schema,Map<UUID,String> names,Map<UUID,String> plain) throws IOException {
        if(!schema.columnIds().equals(names.keySet()) || !Collections.disjoint(names.keySet(),plain.keySet())) throw new IllegalArgumentException("Bootstrap schema/CF coverage mismatch");
        var next=new HashMap<>(bindings);
        // Validate all aliases and reserved names before inspecting artifacts or changing memory.
        for(var column:schema.columns()) add(next,column.columnId(),names.get(column.columnId()),column.operator()==null ? null : descriptor(column.operator(),new DeterministicContract(column.operator().deterministicContract(),column.operator().deterministicContractVersion(),true)));
        for(var entry:plain.entrySet()) add(next,entry.getKey(),entry.getValue(),null);
        for(var column:schema.columns()) if(column.operator()!=null) require(column.operator());
        encodedBindings(next,bootstrap); return Map.copyOf(next);
    }
    void qualifyBootstrap(DistributedSchema schema,Map<UUID,String> names,Map<UUID,String> plain,boolean virgin) throws IOException {
        byte[] identity=bootstrapIdentity(schema,names,plain);
        if(!virgin) {
            if(bootstrap==null || !Arrays.equals(bootstrap,identity)) throw new IOException("Original qualified bootstrap identity changed");
            requireSchemaBindings(schema,names); for(var entry:plain.entrySet()) requireBirth(entry.getKey(),entry.getValue(),false);
            return;
        }
        if(bootstrap!=null && !Arrays.equals(bootstrap,identity)) throw new IOException("Original qualified bootstrap identity changed");
        var next=proposed(schema,names,plain); bootstrap=identity; bindings=next; dirty=true; forceBindings();
    }
    void validate(DistributedSchema schema,Map<UUID,String> names) throws IOException { proposed(schema,names,Map.of()); }
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
            if(actual==null || !actual.column().equals(column.columnId()) || !Objects.equals(actual.operator(),expected)) throw new IOException("Native schema differs from trusted operator tuple");
        }
    }
    private static byte[] encodedBindings(Map<String,Binding> bindings,byte[] bootstrap) throws IOException {
        if(bindings.size()>MAX_BINDINGS) throw new IllegalStateException("Qualified CF binding count exceeds budget");
        var bytes=new ByteArrayOutputStream(); var out=new DataOutputStream(bytes);
        out.writeInt(0x4f504249); out.writeBoolean(bootstrap!=null); if(bootstrap!=null) out.write(bootstrap); out.writeInt(bindings.size());
        for(var name:bindings.keySet().stream().sorted().toList()) {
            var binding=bindings.get(name); out.writeUTF(name); out.writeLong(binding.column().getMostSignificantBits()); out.writeLong(binding.column().getLeastSignificantBits());
            out.writeBoolean(binding.operator()!=null); if(binding.operator()!=null) { byte[] descriptor=encodeDescriptor(binding.operator()); out.writeInt(descriptor.length); out.write(descriptor); }
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
            if(name.isEmpty() || name.length()>4096 || name.indexOf('\0')>=0 || !StandardCharsets.US_ASCII.newEncoder().canEncode(name) || DistributedStoragePool.isReservedColumn(name) || !columns.add(column) || result.put(name,new Binding(column,operator))!=null) throw new IOException("Invalid qualified CF binding");
        }
        if(in.available()!=0) throw new IOException("Trailing qualified binding bytes"); return Map.copyOf(result);
    }
    private static byte[] bootstrapIdentity(DistributedSchema schema,Map<UUID,String> names,Map<UUID,String> plain) throws IOException {
        var bytes=new ByteArrayOutputStream(); var out=new DataOutputStream(bytes); out.write(schema.digest());
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
    private static void immutable(Path destination,byte[] bytes,int maximum) throws IOException {
        if(Files.exists(destination,LinkOption.NOFOLLOW_LINKS)) { if(!Arrays.equals(read(destination,maximum),bytes)) throw new IOException("Immutable staged artifact changed"); force(destination.getParent()); return; }
        Path temporary=writeTemporary(destination,bytes);
        try { Files.createLink(destination,temporary); force(destination.getParent()); }
        finally { Files.deleteIfExists(temporary); force(destination.getParent()); }
    }
    private static void replace(Path destination,byte[] bytes) throws IOException {
        Path temporary=writeTemporary(destination,bytes);
        try { Files.move(temporary,destination,StandardCopyOption.ATOMIC_MOVE,StandardCopyOption.REPLACE_EXISTING); force(destination.getParent()); }
        finally { Files.deleteIfExists(temporary); }
    }
    private static Path writeTemporary(Path destination,byte[] bytes) throws IOException {
        var temporary=Files.createTempFile(destination.getParent(),".operator-",".tmp");
        try(var channel=FileChannel.open(temporary,StandardOpenOption.WRITE,StandardOpenOption.TRUNCATE_EXISTING)) { var buffer=ByteBuffer.wrap(bytes); while(buffer.hasRemaining()) channel.write(buffer); channel.force(true); }
        catch(Throwable failure) { Files.deleteIfExists(temporary); throw failure; } return temporary;
    }
    private static void force(Path directory) throws IOException { try(var channel=FileChannel.open(directory,StandardOpenOption.READ)) { channel.force(true); } }
    private static byte[] sha(byte[] bytes) { try { return MessageDigest.getInstance("SHA-256").digest(bytes); } catch(NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); } }
}
