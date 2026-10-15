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
    private final RatisJournalAccess access;
    private final Path root;
    private final Map<UUID,RatisRoutedClient> routed;
    private final RatisSchemaCatalogue schemas;
    private final RatisRocksDBConnection.GenerationFactory factory;
    private final LongSupplier ids;
    private final int maximum;
    private final Capacity capacity;
    private final Map<String,Owned> generations=new LinkedHashMap<>();
    RatisGenerationRegistry(Path root,Map<UUID,RatisRoutedClient> routed,RatisSchemaCatalogue schemas,RatisRocksDBConnection.GenerationFactory factory,LongSupplier ids,int maximum,Capacity capacity,RatisJournalAccess access) throws IOException {
        this.access=Objects.requireNonNull(access);this.root=root; this.routed=Map.copyOf(routed); this.schemas=schemas; this.factory=factory; this.ids=ids; this.maximum=maximum; this.capacity=capacity; Files.createDirectories(root);
        try {
        if(routed.size()>maximum)throw new IOException("Declared domains exceed the shared generation budget");var currentSchemas=new ArrayList<DistributedSchema>();long schemaBytes=0;for(var data:routed.values()){var schema=schemas.read(data.directory().schemaDigest());if(schema==null)throw new IOException("Advertised domain schema content is missing");schema.requireMatches(data.directory());schemaBytes+=DistributedSchemaCodec.encodedSize(schema)+RangeDirectoryCodec.encode(data.directory()).length;if(schemaBytes>16L*1024*1024)throw new IOException("Database generation metadata exceeds shared byte budget");currentSchemas.add(schema);}requireUnique(currentSchemas);
        try (var files=Files.newDirectoryStream(root)) {
            for (var folder:files) {
                if (!Files.isDirectory(folder,LinkOption.NOFOLLOW_LINKS)) throw new IOException("Unexpected connection generation entry");
                var in=ByteBuffer.wrap(access.read(folder.resolve("binding"),8*1024*1024)); var schema=DistributedSchemaCodec.decode(blob(in,DistributedSchemaCodec.MAX_BYTES)); var directory=RangeDirectoryCodec.decode(blob(in,RangeDirectoryCodec.MAX_ENCODED_BYTES)); if (in.hasRemaining() || !folder.getFileName().toString().equals(key(schema,directory))) throw new IOException("Invalid generation binding");
                schema.requireMatches(directory);var data=data(schema.domainId());data.requireKnownSchema(schema);var qualified=data.directory(schema);
                // Only this exclusively owned, forced generation binding restores historical placement.
                // Its actors separately cross-check their immutable root bindings; no route is published or retargeted here.
                if(directory.generation()>qualified.generation() || directory.generation()==qualified.generation() && !Arrays.equals(directory.payloadDigest(),qualified.payloadDigest()))throw new IOException("Original generation placement is not qualified by current schema authority");
                add(schema,directory,folder,false);
            }
        }
        for(var domain:routed.keySet())current(domain);validateColumns();
        } catch (Throwable failure) { try { close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); } if (failure instanceof IOException io) throw io; throw new IOException("Generation inventory open failed",failure); }
    }
    static String key(DistributedSchema schema,RangeDirectory directory) { return key(schema.digest(),directory); }
    private static String key(byte[] schemaDigest,RangeDirectory directory) { return HexFormat.of().formatHex(schemaDigest)+"-"+HexFormat.of().formatHex(directory.payloadDigest()); }
    private void add(DistributedSchema schema,RangeDirectory directory,Path folder,boolean create) throws IOException {
        if (generations.size()>=maximum) throw new IOException("Connection immutable generation capacity exhausted");
        if (create) {
            capacity.admitBinding(schema,directory);
            byte[] a=DistributedSchemaCodec.encode(schema),b=RangeDirectoryCodec.encode(directory); var expected=ByteBuffer.allocate(8+a.length+b.length).putInt(a.length).put(a).putInt(b.length).put(b).array();
            if (Files.exists(folder,LinkOption.NOFOLLOW_LINKS)) { if (!Files.isDirectory(folder,LinkOption.NOFOLLOW_LINKS) || !Arrays.equals(expected,access.read(folder.resolve("binding"),8*1024*1024))) throw new IOException("Original generation binding is missing/changed"); }
            else { Files.createDirectory(folder); access.write(folder.resolve("binding"),expected,8*1024*1024); SessionJournalFiles.force(root); }
        }
        capacity.forceBinding(folder);
        capacity.bound(schema);
        var owned=new Owned(schema,directory,Objects.requireNonNull(factory.create(schema,directory,folder,ids)));
        synchronized(this) { generations.put(key(schema,directory),owned); }
    }
    private RatisRoutedClient data(UUID domain)throws IOException {var data=routed.get(domain);if(data==null)throw new IOException("Generation belongs to an undeclared database domain");return data;}
    record Scoped(Owned owner,TransactionScope scope) {}
    Scoped scope(it.cavallium.rockserver.core.common.TransactionOpenScope declaration) throws IOException {
        if (declaration==null) throw it.cavallium.rockserver.core.common.RocksDBException.of(it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.TX_SCOPE_REQUIRED,"Distributed transactions require an explicit bounded scope");
        var candidates=new ArrayList<DistributedSchema>(); var directories=new HashMap<UUID,RangeDirectory>();
        // Read bindings without constructing generation/session owners for a rejected declaration.
        for (var domain:routed.keySet()) {
            var directory=data(domain).directory();Owned cached;
            synchronized(this){cached=generations.get(key(directory.schemaDigest(),directory));}
            var schema=cached==null ? schemas.read(directory.schemaDigest()) : cached.schema();
            if (schema==null) throw new IOException("Activated immutable schema absent from catalogue");
            data(domain).requireKnownSchema(schema);schema.requireMatches(directory);candidates.add(schema);directories.put(domain,directory);
        }
        requireUnique(candidates); DistributedSchema selected=null;
        for (var span:declaration.spans()) {
            DistributedSchema candidate=null;
            for (var schema:candidates) if (schema.columns().stream().anyMatch(column -> column.apiId()==span.columnId())) {candidate=schema;break;}
            if (candidate==null) throw new IllegalArgumentException("Unknown transaction scope column");
            if (selected!=null && !selected.domainId().equals(candidate.domainId())) throw new IllegalArgumentException("Transaction scope cannot cross database domains");
            selected=candidate;
        }
        var directory=directories.get(selected.domainId());var physical=RatisRocksDBAdapter.scope(selected,directory,declaration);
        var key=key(selected,directory);Owned owner;synchronized(this){owner=generations.get(key);}
        if(owner==null){add(selected,directory,root.resolve(key),true);validateColumns();synchronized(this){owner=generations.get(key);}}
        return new Scoped(owner,physical);
    }
    Owned current(UUID domain) throws IOException {
        var data=data(domain);var directory=data.directory();var key=key(directory.schemaDigest(),directory);Owned current;synchronized(this){current=generations.get(key);}if(current!=null)return current;
        var schema=schemas.read(directory.schemaDigest());if(schema==null)throw new IOException("Activated immutable schema absent from catalogue");data.requireKnownSchema(schema);schema.requireMatches(directory);add(schema,directory,root.resolve(key),true);validateColumns();synchronized(this){return generations.get(key);}
    }
    List<Owned> current()throws IOException {var owners=new ArrayList<Owned>();for(var domain:routed.keySet())owners.add(current(domain));return List.copyOf(owners);}
    Owned column(long column)throws IOException {for(var owner:current())if(owner.schema.columns().stream().anyMatch(c->c.apiId()==column))return owner;throw new IOException("Unknown database column API identity");}
    Owned column(String name)throws IOException {Owned found=null;for(var owner:current())if(owner.schema.columns().stream().anyMatch(c->c.name().equals(name))){if(found!=null)throw new IOException("Ambiguous database column name");found=owner;}return found;}
    private static void requireUnique(Collection<DistributedSchema> schemas)throws IOException {
        var ids=new HashSet<Long>();var columns=new HashSet<UUID>();var names=new HashSet<String>();for(var schema:schemas)for(var column:schema.columns())if(!ids.add(column.apiId()) || !columns.add(column.columnId()) || !names.add(column.name()))throw new IOException("Database domains share a column UUID/API identity/name");
    }
    private void validateColumns()throws IOException {
        var ids=new HashSet<Long>();var columns=new HashSet<UUID>();var names=new HashSet<String>();
        for(var domain:routed.keySet()){var directory=data(domain).directory();var owner=generations.get(key(directory.schemaDigest(),directory));if(owner==null)continue;for(var column:owner.schema.columns())if(!ids.add(column.apiId()) || !columns.add(column.columnId()) || !names.add(column.name()))throw new IOException("Database domains share a column UUID/API identity/name");}
    }
    Owned transaction(long id) throws IOException { for (var generation:all()) if (generation.generation().adapter().ownsTransactionOrUpdate(id)) return generation; throw new IOException("Original transaction/update handle not owned by this connection"); }
    Owned iterator(long id) throws IOException { for (var generation:all()) if (generation.generation().adapter().ownsIterator(id)) return generation; throw new IOException("Original iterator handle not owned by this connection"); }
    synchronized Collection<Owned> all() { return List.copyOf(generations.values()); }
    @Override public void close() throws IOException { for (var generation:all()) generation.generation().close(); }
    private static byte[] blob(ByteBuffer in,int maximum) throws IOException { if (in.remaining()<4) throw new IOException("Truncated generation binding"); int size=in.getInt(); if (size<1 || size>maximum || size>in.remaining()) throw new IOException("Invalid generation binding bytes"); var value=new byte[size]; in.get(value); return value; }
    /** Forced slot ownership precedes the controller's first mutating checkpoint. */
    static final class Capacity {
        private record Slot(byte[] source,byte[] directory,byte[] target,boolean checkpointed) {}
        private final RatisJournalAccess access;
        private final Path file,bindings,requests;
        @FunctionalInterface interface BindingForce { void force(Path path) throws IOException; }
        private final int maximum;
        private final BindingForce bindingForce;
        private final Map<UUID,Slot> slots=new LinkedHashMap<>();
        private boolean fenced;
        Capacity(Path file,Path bindings,Path requests,int maximum,boolean used,RatisJournalAccess access) throws IOException { this(file,bindings,requests,maximum,used,SessionJournalFiles::force,access); }
        Capacity(Path file,Path bindings,Path requests,int maximum,boolean used,BindingForce bindingForce,RatisJournalAccess access) throws IOException {
            this.access=Objects.requireNonNull(access);access.register(file,4096);
            this.file=file; this.bindings=bindings; this.requests=requests; this.maximum=maximum; this.bindingForce=Objects.requireNonNull(bindingForce);
            if (Files.exists(file,LinkOption.NOFOLLOW_LINKS)) {
                try (var in=new DataInputStream(new ByteArrayInputStream(access.read(file,4096)))) {
                    int count=in.readInt(); if(count<0 || count>maximum) throw new IOException("Invalid generation reservations");
                    for(int i=0;i<count;i++) { var id=new UUID(in.readLong(),in.readLong()); var slot=new Slot(read(in),read(in),read(in),in.readBoolean()); if(slot.source.length!=32 || slot.directory.length!=32 || slot.target.length!=0 && slot.target.length!=32) throw new IOException("Invalid reserved generation identity"); if(slots.putIfAbsent(id,slot)!=null) throw new IOException("Duplicate generation reservation"); }
                    if(in.read()!=-1) throw new IOException("Trailing generation reservation bytes");
                }
            } else { if(used) throw new IOException("Original generation capacity state is missing"); persist(); }
            for(var id:List.copyOf(slots.keySet())) {
                var path=requests.resolve(id+".request"); var slot=slots.get(id);
                if(!Files.exists(path,LinkOption.NOFOLLOW_LINKS)) { if(slot.checkpointed) throw new IOException("Original reserved schema request is missing"); slots.remove(id); }
                else reconcile(id,RatisSchemaClientJournal.readOwned(path,access));
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
        synchronized void admitBinding(DistributedSchema schema,RangeDirectory directory) throws IOException {
            requireUsable(); if(Files.exists(bindings.resolve(key(schema,directory)).resolve("binding"),LinkOption.NOFOLLOW_LINKS)) return;
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
            if(target.length==0 || !Files.exists(bindings)) return false;
            try(var folders=Files.newDirectoryStream(bindings)) { for(var folder:folders) {
                var path=folder.resolve("binding");
                if(!Files.isDirectory(folder,LinkOption.NOFOLLOW_LINKS) || !Files.exists(path,LinkOption.NOFOLLOW_LINKS)) throw new IOException("Invalid generation inventory");
                try { var in=ByteBuffer.wrap(access.read(path,8*1024*1024)); var schema=DistributedSchemaCodec.decode(blob(in,DistributedSchemaCodec.MAX_BYTES)); var directory=RangeDirectoryCodec.decode(blob(in,RangeDirectoryCodec.MAX_ENCODED_BYTES)); schema.requireMatches(directory);
                    if(in.hasRemaining() || !folder.getFileName().toString().equals(key(schema,directory))) throw new IOException("Reserved target binding changed");
                    if(Arrays.equals(schema.digest(),target)) {forceBinding(folder);return true;}
                } catch(IllegalArgumentException invalid) { throw new IOException("Invalid reserved target binding",invalid); }
            } }
            return false;
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
            try { access.write(file,bytes.toByteArray(),4096); } catch(IOException failure) { fenced=true; throw failure; }
        }
        private static void write(DataOutputStream out,byte[] bytes) throws IOException { out.writeInt(bytes.length); out.write(bytes); }
        private static byte[] read(DataInputStream in) throws IOException { int size=in.readInt(); if(size<0 || size>32 || size>in.available()) throw new IOException("Invalid generation reservation body"); return in.readNBytes(size); }
    }

}
