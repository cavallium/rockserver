package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.*;
import java.nio.*;
import java.nio.file.*;
import java.util.*;
import java.util.function.LongSupplier;

/** Immutable proof-qualified generations. Handle owners remain reachable until connection shutdown. */
final class RatisGenerationRegistry implements AutoCloseable {
    record Owned(DistributedSchema schema,RangeDirectory directory,RatisRocksDBConnection.Generation generation) {}
    private final Path root;
    private final RatisRoutedClient routed;
    private final RatisSchemaCatalogue schemas;
    private final RatisRocksDBConnection.GenerationFactory factory;
    private final LongSupplier ids;
    private final int maximum;
    private final Capacity capacity;
    private final Map<String,Owned> generations=new LinkedHashMap<>();
    RatisGenerationRegistry(Path root,RatisRoutedClient routed,RatisSchemaCatalogue schemas,RatisRocksDBConnection.GenerationFactory factory,LongSupplier ids,int maximum,DistributedSchema initial,Capacity capacity) throws IOException {
        this.root=root; this.routed=routed; this.schemas=schemas; this.factory=factory; this.ids=ids; this.maximum=maximum; this.capacity=capacity; Files.createDirectories(root);
        try {
        try (var files=Files.newDirectoryStream(root)) {
            for (var folder:files) {
                if (!Files.isDirectory(folder,LinkOption.NOFOLLOW_LINKS)) throw new IOException("Unexpected connection generation entry");
                var in=ByteBuffer.wrap(SessionJournalFiles.read(folder.resolve("binding"),8*1024*1024)); var schema=DistributedSchemaCodec.decode(blob(in,DistributedSchemaCodec.MAX_BYTES)); var directory=RangeDirectoryCodec.decode(blob(in,RangeDirectoryCodec.MAX_ENCODED_BYTES)); if (in.hasRemaining() || !folder.getFileName().toString().equals(key(schema))) throw new IOException("Invalid generation binding");
                schema.requireMatches(directory); routed.requireKnownSchema(schema); if (!Arrays.equals(routed.directory(schema).payloadDigest(),directory.payloadDigest())) throw new IOException("Original generation placement changed"); add(schema,directory,folder,false);
            }
        }
        routed.requireKnownSchema(initial); if (!generations.containsKey(key(initial))) add(initial,routed.directory(initial),root.resolve(key(initial)),true);
        } catch (Throwable failure) { try { close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); } if (failure instanceof IOException io) throw io; throw new IOException("Generation inventory open failed",failure); }
    }
    private static String key(DistributedSchema schema) { return HexFormat.of().formatHex(schema.digest()); }
    private void add(DistributedSchema schema,RangeDirectory directory,Path folder,boolean create) throws IOException {
        if (generations.size()>=maximum) throw new IOException("Connection immutable generation capacity exhausted");
        if (create) {
            capacity.admitBinding(schema);
            byte[] a=DistributedSchemaCodec.encode(schema),b=RangeDirectoryCodec.encode(directory); var expected=ByteBuffer.allocate(8+a.length+b.length).putInt(a.length).put(a).putInt(b.length).put(b).array();
            if (Files.exists(folder,LinkOption.NOFOLLOW_LINKS)) { if (!Files.isDirectory(folder,LinkOption.NOFOLLOW_LINKS) || !Arrays.equals(expected,SessionJournalFiles.read(folder.resolve("binding"),8*1024*1024))) throw new IOException("Original generation binding is missing/changed"); }
            else { Files.createDirectory(folder); SessionJournalFiles.write(folder.resolve("binding"),expected,8*1024*1024); SessionJournalFiles.force(root); }
        }
        capacity.forceBinding(folder);
        capacity.bound(schema);
        var owned=new Owned(schema,directory,Objects.requireNonNull(factory.create(schema,directory,folder,ids)));
        synchronized(this) { generations.put(key(schema),owned); }
    }
    Owned current() throws IOException {
        var directory=routed.directory(); var key=HexFormat.of().formatHex(directory.schemaDigest()); Owned current; synchronized(this) { current=generations.get(key); } if (current!=null) return current;
        var schema=schemas.read(directory.schemaDigest()); if (schema==null) throw new IOException("Activated immutable schema absent from catalogue"); routed.requireKnownSchema(schema); schema.requireMatches(directory); add(schema,directory,root.resolve(key),true); synchronized(this) { return generations.get(key); }
    }
    Owned transaction(long id) throws IOException { for (var generation:all()) if (generation.generation().adapter().ownsTransactionOrUpdate(id)) return generation; throw new IOException("Original transaction/update handle not owned by this connection"); }
    Owned iterator(long id) throws IOException { for (var generation:all()) if (generation.generation().adapter().ownsIterator(id)) return generation; throw new IOException("Original iterator handle not owned by this connection"); }
    synchronized Collection<Owned> all() { return List.copyOf(generations.values()); }
    @Override public void close() throws IOException { for (var generation:all()) generation.generation().close(); }
    private static byte[] blob(ByteBuffer in,int maximum) throws IOException { if (in.remaining()<4) throw new IOException("Truncated generation binding"); int size=in.getInt(); if (size<1 || size>maximum || size>in.remaining()) throw new IOException("Invalid generation binding bytes"); var value=new byte[size]; in.get(value); return value; }
    /** Forced slot ownership precedes the controller's first mutating checkpoint. */
    static final class Capacity {
        private record Slot(byte[] source,byte[] directory,byte[] target,boolean checkpointed) {}
        private final Path file,bindings,requests;
        @FunctionalInterface interface BindingForce { void force(Path path) throws IOException; }
        private final int maximum;
        private final BindingForce bindingForce;
        private final Map<UUID,Slot> slots=new LinkedHashMap<>();
        private boolean fenced;
        Capacity(Path file,Path bindings,Path requests,int maximum,boolean used) throws IOException { this(file,bindings,requests,maximum,used,SessionJournalFiles::force); }
        Capacity(Path file,Path bindings,Path requests,int maximum,boolean used,BindingForce bindingForce) throws IOException {
            this.file=file; this.bindings=bindings; this.requests=requests; this.maximum=maximum; this.bindingForce=Objects.requireNonNull(bindingForce);
            if (Files.exists(file,LinkOption.NOFOLLOW_LINKS)) {
                try (var in=new DataInputStream(new ByteArrayInputStream(SessionJournalFiles.read(file,4096)))) {
                    int count=in.readInt(); if(count<0 || count>maximum) throw new IOException("Invalid generation reservations");
                    for(int i=0;i<count;i++) { var id=new UUID(in.readLong(),in.readLong()); var slot=new Slot(read(in),read(in),read(in),in.readBoolean()); if(slot.source.length!=32 || slot.directory.length!=32 || slot.target.length!=0 && slot.target.length!=32) throw new IOException("Invalid reserved generation identity"); if(slots.putIfAbsent(id,slot)!=null) throw new IOException("Duplicate generation reservation"); }
                    if(in.read()!=-1) throw new IOException("Trailing generation reservation bytes");
                }
            } else { if(used) throw new IOException("Original generation capacity state is missing"); persist(); }
            for(var id:List.copyOf(slots.keySet())) {
                var path=requests.resolve(id+".request"); var slot=slots.get(id);
                if(!Files.exists(path,LinkOption.NOFOLLOW_LINKS)) { if(slot.checkpointed) throw new IOException("Original reserved schema request is missing"); slots.remove(id); }
                else reconcile(id,RatisSchemaClientJournal.read(path));
            }
            if(bindingCount()+slots.size()>maximum) throw new IOException("Recovered generations exceed connection capacity");
            persist();
        }
        synchronized void checkpoint(Path path,Map<String,byte[]> fields,RatisSchemaClient.Checkpoint checkpoint) throws IOException {
            requireUsable(); var id=identity(fields); String phase=new String(fields.get("phase"),java.nio.charset.StandardCharsets.UTF_8); boolean schema=!new String(fields.get("kind"),java.nio.charset.StandardCharsets.UTF_8).equals("UPLOAD") && ByteBuffer.wrap(fields.get("changed")).getLong()!=0 && !phase.equals("DONE") && !phase.equals("CONFLICT") && !phase.equals("ABORTED");
            if(schema) {
                byte[] source=DistributedSchemaCodec.decode(fields.get("sourceSchema")).digest(),directory=RangeDirectoryCodec.decode(fields.get("sourceDirectory")).payloadDigest(),target=target(fields); var prior=slots.get(id);
                if(prior==null && !hasTargetBinding(target)) {
                    if(bindingCount()+slots.size()>=maximum) throw new IOException("Connection immutable generation capacity exhausted before schema effects");
                    slots.put(id,new Slot(source.clone(),directory.clone(),target.clone(),false)); persist();
                } else if(prior!=null) {
                    if(!Arrays.equals(prior.source,source) || !Arrays.equals(prior.directory,directory) || prior.target.length!=0 && !Arrays.equals(prior.target,target)) throw new IOException("Reserved schema generation identity changed");
                    if(target.length!=0 && prior.target.length==0) { slots.put(id,new Slot(prior.source,prior.directory,target.clone(),prior.checkpointed)); persist(); }
                }
            }
            try { checkpoint.write(path,fields); }
            catch(Throwable failure) {
                var slot=slots.get(id);
                if(slot!=null && !slot.checkpointed && !Files.exists(path,LinkOption.NOFOLLOW_LINKS)) { slots.remove(id); try { persist(); } catch(Throwable cleanup) { failure.addSuppressed(cleanup); } }
                if(failure instanceof IOException io) throw io; if(failure instanceof RuntimeException runtime) throw runtime; throw new IOException(failure);
            }
            var slot=slots.get(id); if(slot!=null && !slot.checkpointed) slots.put(id,new Slot(slot.source,slot.directory,slot.target,true));
            reconcile(id,fields); persist();
        }
        private void reconcile(UUID id,Map<String,byte[]> fields) throws IOException {
            var slot=slots.get(id); if(slot==null) return; if(!identity(fields).equals(id)) throw new IOException("Reserved schema request token changed");
            if(!Arrays.equals(slot.source,DistributedSchemaCodec.decode(fields.get("sourceSchema")).digest()) || !Arrays.equals(slot.directory,RangeDirectoryCodec.decode(fields.get("sourceDirectory")).payloadDigest())) throw new IOException("Original reserved schema source changed");
            byte[] target=target(fields); if(slot.target.length!=0 && !Arrays.equals(slot.target,target)) throw new IOException("Original reserved schema target changed");
            String phase=new String(fields.get("phase"),java.nio.charset.StandardCharsets.UTF_8);
            if(hasTargetBinding(target) || phase.equals("CONFLICT") && conflictBeforeBegin(fields) || phase.equals("ABORTED") || phase.equals("DONE") && ByteBuffer.wrap(fields.get("aborted")).getLong()!=0) slots.remove(id);
            else slots.put(id,new Slot(slot.source,slot.directory,target.clone(),true));
        }
        synchronized void admitBinding(DistributedSchema schema) throws IOException {
            requireUsable(); if(hasTargetBinding(schema.digest())) return;
            boolean reserved=slots.values().stream().anyMatch(slot -> Arrays.equals(slot.target,schema.digest()));
            if(bindingCount()+slots.size()-(reserved ? 1 : 0)>=maximum) throw new IOException("Connection immutable generation capacity exhausted");
        }
        synchronized void bound(DistributedSchema schema) throws IOException {
            requireUsable(); byte[] target=schema.digest(); boolean changed=slots.entrySet().removeIf(entry -> Arrays.equals(entry.getValue().target,target)); if(changed) persist();
        }
        private static boolean conflictBeforeBegin(Map<String,byte[]> fields) {
            byte[] body=fields.get("definition"); if(body==null) return true;
            var definition=SchemaChange.Definition.decode(body); byte[] receipt=fields.get("result/"+definition.beginOperation().sequence());
            return receipt!=null && CommandResult.decode(receipt).status()!=CommandResult.Status.SUCCESS;
        }
        private static byte[] target(Map<String,byte[]> fields) { var target=fields.get("targetSchema"); return target==null ? new byte[0] : DistributedSchemaCodec.decode(target).digest(); }
        private boolean hasTargetBinding(byte[] target) throws IOException {
            if(target.length==0) return false; var path=bindings.resolve(HexFormat.of().formatHex(target)).resolve("binding");
            if(!Files.exists(path,LinkOption.NOFOLLOW_LINKS)) return false;
            try { var in=ByteBuffer.wrap(SessionJournalFiles.read(path,8*1024*1024)); var schema=DistributedSchemaCodec.decode(blob(in,DistributedSchemaCodec.MAX_BYTES)); var directory=RangeDirectoryCodec.decode(blob(in,RangeDirectoryCodec.MAX_ENCODED_BYTES)); schema.requireMatches(directory);
                if(in.hasRemaining() || !Arrays.equals(schema.digest(),target)) throw new IOException("Reserved target binding changed"); forceBinding(path.getParent()); return true;
            } catch(IllegalArgumentException invalid) { throw new IOException("Invalid reserved target binding",invalid); }
        }
        synchronized void forceBinding(Path folder) throws IOException {
            requireUsable();
            try { bindingForce.force(folder.resolve("binding")); bindingForce.force(folder); bindingForce.force(bindings); }
            catch(IOException failure) { fenced=true; throw failure; }
        }
        private int bindingCount() throws IOException { int count=0; if(Files.exists(bindings)) try(var folders=Files.newDirectoryStream(bindings)) { for(var folder:folders) { if(!Files.isDirectory(folder,LinkOption.NOFOLLOW_LINKS) || !Files.exists(folder.resolve("binding"),LinkOption.NOFOLLOW_LINKS)) throw new IOException("Invalid generation inventory"); count++; } } return count; }
        private static UUID identity(Map<String,byte[]> fields) throws IOException { byte[] value=fields.get("requestId"); if(value==null || value.length!=16) throw new IOException("Invalid generation request identity"); var in=ByteBuffer.wrap(value); return new UUID(in.getLong(),in.getLong()); }
        private void requireUsable() throws IOException { if(fenced) throw new IOException("Generation capacity persistence uncertain; reopen required"); }
        private void persist() throws IOException {
            requireUsable(); var bytes=new ByteArrayOutputStream(); try(var out=new DataOutputStream(bytes)) { out.writeInt(slots.size()); for(var entry:slots.entrySet()) { out.writeLong(entry.getKey().getMostSignificantBits()); out.writeLong(entry.getKey().getLeastSignificantBits()); write(out,entry.getValue().source); write(out,entry.getValue().directory); write(out,entry.getValue().target); out.writeBoolean(entry.getValue().checkpointed); } }
            try { SessionJournalFiles.write(file,bytes.toByteArray(),4096); } catch(IOException failure) { fenced=true; throw failure; }
        }
        private static void write(DataOutputStream out,byte[] bytes) throws IOException { out.writeInt(bytes.length); out.write(bytes); }
        private static byte[] read(DataInputStream in) throws IOException { int size=in.readInt(); if(size<0 || size>32 || size>in.available()) throw new IOException("Invalid generation reservation body"); return in.readNBytes(size); }
    }

}
