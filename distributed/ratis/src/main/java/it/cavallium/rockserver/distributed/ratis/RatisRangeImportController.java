package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeSstImport;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/** Caller-driven durable SST import. Transports are borrowed; owned originals survive close/reopen. */
public final class RatisRangeImportController implements AutoCloseable {
    public static final int MAX_JOBS=8, MAX_SOURCE_FILES=RangeImportDefinition.MAX_GROUPS*NativeSstImportManifest.MAX_FILES, MAX_METADATA_BYTES=RatisSchemaClientJournal.MAX_BYTES;
    public static final long MAX_SOURCE_BYTES=64L*1024*1024*1024;
    public enum Phase { SOURCES, DEFINITION, BEGIN, FILES, INSTALL, ADMISSION, PUBLISH, ACTIVATE, ACTIVATION_ADMISSION, COMPLETE, DONE, CONFLICT }
    public record Progress(UUID importId,Phase phase,RatisRangeImportCatalogue.Head published,RatisRangeImportCatalogue.Head completed) {}
    public record Source(UUID groupId,UUID fileId) { public Source { Objects.requireNonNull(groupId); Objects.requireNonNull(fileId); } }
    public interface DataPlane {
        RatisReferencedFile transfer(RangeImportDefinition definition,RatisReferencedFile.Request request,Path source) throws IOException;
        NativeSstImportManifest.Receipt install(RangeImportDefinition definition,RatisRangeImportRecord.Install record) throws IOException;
        NativeRangeSstImport.Activation activate(RangeImportDefinition definition,RatisRangeImportRecord.Activate record) throws IOException;
        RangeImportAdmission.OwnerReceipt receipt(RangeImportDefinition definition,RangeDescriptor range,ReplicaDescriptor owner) throws IOException;
        RangeImportActivationAdmission.OwnerActivation activation(RangeImportDefinition definition,RatisRangeImportCatalogue.Head published,RangeDescriptor range,ReplicaDescriptor owner) throws IOException;
        RatisRangeImportApply.PublicationView publication(RangeImportDefinition definition) throws IOException;
    }
    private final Path root;
    private final UUID cluster,database;
    private final RatisNodePlan.Catalogue binding;
    private final RaftClient client;
    private final RatisRangeImportCatalogue catalogue;
    private final RatisSchemaAdministration schema;
    private final DataPlane data;
    private final FileChannel lockChannel;
    private final FileLock lock;
    private final Map<UUID,Handle> handles=new HashMap<>();
    private boolean closed,fenced,busy;
    public final class Handle implements AutoCloseable {
        private final UUID id; private final Map<String,byte[]> fields; private final RangeImportDefinition definition; private boolean released;
        private Handle(UUID id,Map<String,byte[]> fields) { this.id=id; this.fields=new java.util.concurrent.ConcurrentHashMap<>(fields); this.definition=RangeImportDefinition.decode(fields.get("definition")); }
        public UUID importId() { return id; }
        public Phase phase() { return Phase.valueOf(text(this,"phase")); }
        public RangeImportDefinition definition() { return definition; }
        @Override public void close() throws IOException { synchronized(RatisRangeImportController.this) { if(busy) throw new IOException("Import I/O still active"); released=true; handles.remove(id,this); } }
    }
    public RatisRangeImportController(Path root,UUID cluster,UUID database,RatisNodePlan.Catalogue binding,RaftClient client,DataPlane data) throws IOException {
        this.root=root.toAbsolutePath().normalize(); this.cluster=Objects.requireNonNull(cluster); this.database=Objects.requireNonNull(database);
        this.binding=Objects.requireNonNull(binding); this.client=Objects.requireNonNull(client); this.data=Objects.requireNonNull(data);
        if(!Files.isDirectory(this.root,LinkOption.NOFOLLOW_LINKS)) throw new IOException("Import journal root absent");
        catalogue=new RatisRangeImportCatalogue(client,cluster,database,binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn());
        schema=new RatisSchemaAdministration(client,cluster,database,binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn());
        lockChannel=FileChannel.open(this.root.resolve("import-controller.lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE);
        try { lock=lockChannel.tryLock(); if(lock==null) throw new IOException("Import journal root already owned"); }
        catch(IOException|RuntimeException failure) { lockChannel.close(); throw new IOException("Import journal lock unavailable",failure); }
    }
    public Handle prepare(RangeImportDefinition definition,Map<Source,Path> sources) throws IOException {
        begin(); try { return prepareOwned(definition,sources); } finally { end(); }
    }
    private Handle prepareOwned(RangeImportDefinition definition,Map<Source,Path> sources) throws IOException {
        requireOpen(); requireNamespace(definition); UUID id=definition.importId();
        if(handles.containsKey(id) || Files.exists(path(id))) throw new IOException("Import already retained; reopen original import");
        long retained=0; int jobs=0;
        try(var files=Files.newDirectoryStream(root,"*.import")) { for(var file:files) { jobs++; var fields=RatisSchemaClientJournal.read(file); retained=Math.addExact(retained,sourceBytes(RangeImportDefinition.decode(fields.get("definition")))); } }
        long bytes=sourceBytes(definition); if(jobs>=MAX_JOBS || bytes>MAX_SOURCE_BYTES-retained) throw new IOException("Import job/source capacity exhausted");
        var expected=new HashSet<Source>(); definition.manifests().forEach(m -> m.columns().forEach(c -> c.files().forEach(f -> expected.add(new Source(m.groupId(),f.fileId())))));
        long fileCount=definition.manifests().stream().flatMap(m -> m.columns().stream()).mapToLong(c -> c.files().size()).sum();
        if(fileCount>MAX_SOURCE_FILES) throw new IOException("Import source metadata capacity exhausted");
        if(!sources.keySet().equals(expected)) throw new IllegalArgumentException("Original source coverage differs");
        var fields=new HashMap<String,byte[]>();fields.put("definition",definition.encode());fields.put("binding",bindingBytes());
        var h=new Handle(id,fields);
        UUID issuer; do { issuer=UUID.randomUUID(); } while(issuer.equals(definition.operation().sessionId()));
        putText(h,"issuer",issuer.toString()); number(h,"sequence",0); number(h,"cursor",0); putText(h,"phase",Phase.SOURCES.name());
        var paths=new HashMap<String,byte[]>();
        for(var entry:sources.entrySet()) paths.put(sourceKey(entry.getKey()),entry.getValue().toAbsolutePath().normalize().toString().getBytes(StandardCharsets.UTF_8));
        h.fields.put("sourcePaths",encodeEntries(paths));
        save(h); handles.put(id,h); retain(h); return h;
    }
    public Handle reopen(UUID importId) throws IOException {
        begin(); try { return reopenOwned(importId); } finally { end(); }
    }
    private Handle reopenOwned(UUID importId) throws IOException {
        requireOpen(); if(handles.containsKey(importId)) return handles.get(importId);
        if(handles.size()>=MAX_JOBS) throw new IOException("Import handle capacity exhausted");
        final Handle h;
        try { h=new Handle(importId,RatisSchemaClientJournal.read(path(importId))); } catch(IllegalArgumentException malformed) { throw new IOException("Invalid retained import definition",malformed); }
        try {
            var d=h.definition(); requireNamespace(d); h.phase();
            if(!d.importId().equals(importId) || !Arrays.equals(bindingBytes(),h.fields.get("binding")) || UUID.fromString(text(h,"issuer")).equals(d.operation().sessionId()) || number(h,"sequence")<0 || number(h,"cursor")<0) throw new IllegalArgumentException("Import journal identity differs");
            if(h.fields.containsKey("pendingCommand")) validateCommand(h,DistributedCommandCodec.decode(h.fields.get("pendingCommand")));
            if(h.fields.containsKey("pendingAck")) validateCommand(h,DistributedCommandCodec.decode(h.fields.get("pendingAck")));
            if(h.fields.containsKey("published")) published(h).requireDefinition(d);
        } catch(RuntimeException invalid) { throw new IOException("Invalid retained import journal",invalid); }
        handles.put(importId,h); return h;
    }
    public Progress advance(Handle h,int maxVisits) throws IOException {
        begin(); try { return advanceOwned(h,maxVisits); } finally { end(); }
    }
    private Progress advanceOwned(Handle h,int maxVisits) throws IOException {
        requireOpen(); if(h==null || h.released || handles.get(h.id)!=h) throw new IllegalArgumentException("Foreign import handle");
        if(maxVisits<1 || maxVisits>4096) throw new IllegalArgumentException("Invalid import visit bound");
        for(int i=0;i<maxVisits;i++) { if(h.fields.containsKey("pendingAck")) { acknowledge(h); continue; } if(h.phase()==Phase.DONE || h.phase()==Phase.CONFLICT) break; step(h); }
        return new Progress(h.id,h.phase(),h.fields.containsKey("published") ? published(h) : null,h.fields.containsKey("completed") ? RatisRangeImportCatalogue.Head.decode(h.fields.get("completed")) : null);
    }
    private void step(Handle h) throws IOException {
        var d=h.definition(); int cursor=Math.toIntExact(number(h,"cursor"));
        switch(h.phase()) {
            case SOURCES -> retain(h);
            case DEFINITION -> {
                int chunks=RatisRangeImportCatalogue.definitionArtifact(d).chunks();
                var result=command(h,() -> cursor<chunks ? catalogue.prepareDefinitionChunk(next(h),d,cursor) : catalogue.prepareDefinitionCommit(next(h),d));
                finish(h,result,cursor<chunks ? Phase.DEFINITION : Phase.BEGIN,cursor<chunks ? cursor+1 : 0);
            }
            case BEGIN -> {
                var result=command(h,() -> { beforeRead(h,"begin-witness"); var prior=catalogue.readHead(d.sourceDirectory().domainId()); beforeRead(h,"schema-witness"); var guard=schema.readHead(d.sourceDirectory().domainId()); if(guard!=null) h.fields.put("schemaHead",guard.encode()); return catalogue.prepareBegin(next(h),d,prior,guard); });
                finish(h,result,Phase.FILES,0);
            }
            case FILES -> transfer(h);
            case INSTALL -> install(h);
            case ADMISSION -> admission(h);
            case PUBLISH -> {
                var result=command(h,() -> catalogue.preparePublication(d,RatisRangeImportCatalogue.Head.prepared(d),installed(h),schemaHead(h)));
                if(result.status()==CommandResult.Status.SUCCESS) h.fields.put("published",RatisRangeImportCatalogue.Head.prepared(d).publish(installed(h).digest()).encode());
                finish(h,result,Phase.ACTIVATE,0);
            }
            case ACTIVATE -> activate(h);
            case ACTIVATION_ADMISSION -> activationAdmission(h);
            case COMPLETE -> {
                var a=activated(h); var result=command(h,() -> catalogue.prepareCompletion(next(h),d,installed(h),published(h),a,schemaHead(h)));
                if(result.status()==CommandResult.Status.SUCCESS) h.fields.put("completed",published(h).complete(a.digest()).encode());
                finish(h,result,Phase.DONE,0);
            }
            default -> throw new IllegalStateException("Unexpected import phase");
        }
    }
    private void retain(Handle h) throws IOException {
        var paths=decodeEntries(h.fields.get("sourcePaths")); Path originals=originals(h); Files.createDirectories(originals); SessionJournalFiles.force(root);
        for(var manifest:h.definition().manifests()) {
            var sources=new HashMap<UUID,Path>(); for(var column:manifest.columns()) for(var file:column.files()) sources.put(file.fileId(),Path.of(new String(paths.get(sourceKey(new Source(manifest.groupId(),file.fileId()))),StandardCharsets.UTF_8)));
            NativeRangeSstImport.retain(originals,manifest,sources);
            Path target=manifestPath(h,manifest); byte[] encoded=manifest.encode();
            if(Files.exists(target,LinkOption.NOFOLLOW_LINKS) && !Files.isRegularFile(target,LinkOption.NOFOLLOW_LINKS)) throw new IOException("Invalid owned manifest path");
            if(!Files.exists(target) || Files.size(target)!=encoded.length || !Arrays.equals(Files.readAllBytes(target),encoded)) {
                Path pending=target.resolveSibling(target.getFileName()+".pending");
                try(var channel=FileChannel.open(pending,StandardOpenOption.CREATE,StandardOpenOption.TRUNCATE_EXISTING,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS)) {
                    var buffer=ByteBuffer.wrap(encoded); while(buffer.hasRemaining()) channel.write(buffer); channel.force(true);
                }
                Files.move(pending,target,StandardCopyOption.ATOMIC_MOVE,StandardCopyOption.REPLACE_EXISTING);
            }
            SessionJournalFiles.force(originals);
        }
        h.fields.remove("sourcePaths"); move(h,Phase.DEFINITION,0);
    }
    private record Transfer(UUID group,UUID file,long bytes,byte[] digest,Path source) {}
    private List<Transfer> transfers(Handle h) {
        var result=new ArrayList<Transfer>(); for(var m:h.definition().manifests()) { result.add(new Transfer(m.groupId(),null,m.encode().length,m.digest(),manifestPath(h,m))); for(var c:m.columns()) for(var f:c.files()) result.add(new Transfer(m.groupId(),f.fileId(),f.bytes(),f.sha256(),NativeRangeSstImport.original(originals(h),m,f))); } return result;
    }
    private static String referenceKey(Transfer t) { return "reference/"+t.group+"/"+(t.file==null ? "manifest" : t.file); }
    private void transfer(Handle h) throws IOException {
        var transfers=transfers(h); int cursor=Math.toIntExact(number(h,"cursor")); if(cursor==transfers.size()) { move(h,Phase.INSTALL,0); return; }
        var t=transfers.get(cursor); if(!h.fields.containsKey("transferRequest")) {
            String identity="fileClient/"+t.group,calls="fileCall/"+t.group;
            if(!h.fields.containsKey(identity)) { putText(h,identity,UUID.randomUUID().toString()); number(h,calls,0); }
            long call=number(h,calls); number(h,calls,Math.addExact(call,1));
            h.fields.put("transferRequest",new RatisReferencedFile.Request(t.group,UUID.fromString(text(h,identity)),call,t.bytes,t.digest).encode()); save(h);
        }
        var request=RatisReferencedFile.Request.decode(h.fields.get("transferRequest"));
        if(!request.groupId().equals(t.group) || request.length()!=t.bytes || !Arrays.equals(request.sha256(),t.digest)) throw new IOException("Frozen transfer differs");
        save(h); var reference=data.transfer(h.definition(),request,t.source); if(!reference.request().equals(request)) throw new IOException("File reply differs from original request");
        var references=references(h); references.put(referenceKey(t),reference.encode()); h.fields.put("references",encodeEntries(references)); h.fields.remove("transferRequest"); move(h,Phase.FILES,cursor+1);
    }
    private void install(Handle h) throws IOException {
        int cursor=Math.toIntExact(number(h,"cursor")); var d=h.definition(); if(cursor==d.manifests().size()) { move(h,Phase.ADMISSION,0); return; } var m=d.manifests().get(cursor);
        if(!h.fields.containsKey("installRecord")) {
            var references=references(h); var refs=new HashMap<UUID,RatisReferencedFile>(); for(var t:transfers(h)) if(t.group.equals(m.groupId()) && t.file!=null) refs.put(t.file,RatisReferencedFile.decode(references.get(referenceKey(t))));
            var record=new RatisRangeImportRecord.Install(d.importId(),m.groupId(),m.rangeEpoch(),d.digest(),RatisReferencedFile.decode(references.get("reference/"+m.groupId()+"/manifest")),refs); record.requireDefinition(d); h.fields.put("installRecord",record.encode()); save(h);
        }
        var record=(RatisRangeImportRecord.Install)RatisRangeImportRecord.decode(h.fields.get("installRecord")); save(h); var receipt=data.install(d,record); d.requireReceipt(receipt); if(!receipt.groupId().equals(m.groupId())) throw new IOException("Foreign installation receipt");
        h.fields.put("receipt/"+m.groupId(),receipt.encode()); h.fields.remove("installRecord"); move(h,Phase.INSTALL,cursor+1);
    }
    private void admission(Handle h) throws IOException {
        var d=h.definition(); if(!h.fields.containsKey("admission")) {
            beforeRead(h,"owner-installation"); var entries=new ArrayList<RangeImportAdmission.OwnerReceipt>(); for(var range:importedRanges(d)) for(var owner:range.replicas()) { beforeRead(h,"receipt/"+range.groupId()+"/"+owner.replicaId()); var proof=data.receipt(d,range,owner); if(proof==null || !Arrays.equals(proof.receipt().encode(),h.fields.get("receipt/"+range.groupId()))) throw new IOException("Owner lacks original installed receipt"); entries.add(proof); }
            h.fields.put("admission",new RangeImportAdmission(d,entries).encode()); save(h);
        }
        var admission=installed(h); int cursor=Math.toIntExact(number(h,"cursor")),chunks=RatisRangeImportCatalogue.admissionArtifact(admission).chunks();
        finish(h,command(h,() -> cursor<chunks ? catalogue.prepareAdmissionChunk(next(h),d,admission,cursor) : catalogue.prepareAdmissionCommit(next(h),d,admission)),cursor<chunks ? Phase.ADMISSION : Phase.PUBLISH,cursor<chunks ? cursor+1 : 0);
    }
    private void activate(Handle h) throws IOException {
        var d=h.definition(); int cursor=Math.toIntExact(number(h,"cursor")); if(cursor==d.manifests().size()) { move(h,Phase.ACTIVATION_ADMISSION,0); return; } var m=d.manifests().get(cursor);
        if(!h.fields.containsKey("activateRecord")) {
            beforeRead(h,"publication"); var view=data.publication(d); if(view==null || !Arrays.equals(view.publication().head().encode(),published(h).encode()) || !Arrays.equals(RangeDirectoryCodec.encode(view.directory()),RangeDirectoryCodec.encode(d.targetDirectory())) || !view.publication().catalogueGroup().equals(binding.groupId())) throw new IOException("Original completed publication unavailable");
            var record=new RatisRangeImportRecord.Activate(NativeSstImportManifest.Receipt.decode(h.fields.get("receipt/"+m.groupId())),view.publication()); record.requireDefinition(d); h.fields.put("activateRecord",record.encode()); save(h);
        }
        var record=(RatisRangeImportRecord.Activate)RatisRangeImportRecord.decode(h.fields.get("activateRecord")); save(h); var activation=data.activate(d,record);
        if(!Arrays.equals(activation.receipt().encode(),record.receipt().encode()) || !activation.catalogueGroup().equals(record.publication().catalogueGroup()) || !activation.cataloguePosition().equals(record.publication().completedCatalogueCut()) || !Arrays.equals(activation.publicationDigest(),record.publication().digest())) throw new IOException("Activation differs from frozen request");
        h.fields.put("activation/"+m.groupId(),activation.encode()); h.fields.remove("activateRecord"); move(h,Phase.ACTIVATE,cursor+1);
    }
    private void activationAdmission(Handle h) throws IOException {
        var d=h.definition(); if(!h.fields.containsKey("activationAdmission")) {
            beforeRead(h,"owner-activation"); var entries=new ArrayList<RangeImportActivationAdmission.OwnerActivation>(); for(var range:importedRanges(d)) for(var owner:range.replicas()) { beforeRead(h,"activation/"+range.groupId()+"/"+owner.replicaId()); var proof=data.activation(d,published(h),range,owner); if(proof==null || !Arrays.equals(proof.activation().encode(),h.fields.get("activation/"+range.groupId()))) throw new IOException("Owner lacks original activation"); entries.add(proof); }
            h.fields.put("activationAdmission",new RangeImportActivationAdmission(d,installed(h),binding.groupId(),RangeImportAdmission.sha256(published(h).encode()),entries).encode()); save(h);
        }
        var a=activated(h); int cursor=Math.toIntExact(number(h,"cursor")),chunks=RatisRangeImportCatalogue.activationArtifact(a).chunks();
        finish(h,command(h,() -> cursor<chunks ? catalogue.prepareActivationChunk(next(h),d,installed(h),published(h),a,cursor) : catalogue.prepareActivationCommit(next(h),d,installed(h),published(h),a)),cursor<chunks ? Phase.ACTIVATION_ADMISSION : Phase.COMPLETE,cursor<chunks ? cursor+1 : 0);
    }
    @FunctionalInterface private interface Prepare { DistributedCommand get() throws IOException; }
    private CommandResult command(Handle h,Prepare prepare) throws IOException {
        if(!h.fields.containsKey("pendingCommand")) { h.fields.put("pendingCommand",DistributedCommandCodec.encode(prepare.get())); save(h); }
        if(!h.fields.containsKey("pendingResult")) {
            var command=DistributedCommandCodec.decode(h.fields.get("pendingCommand"));
            if(RatisRangeImportCatalogue.requiresOwnerAdmission(command)) {
                save(h); var reply=RatisRoutedClient.rpc(() -> client.io().sendReadOnly(Message.valueOf(ByteString.copyFrom(RatisImportPublicationRpc.prepareRequest(command)))));
                if(reply==null || !reply.isSuccess() || reply.getMessage()==null || reply.getMessage().getContent().size()>RatisImportPublicationRpc.MAX_BYTES) throw new IOException("Import admission preparation unavailable");
                try { RatisImportPublicationRpc.decodePreparedReply(reply.getMessage().getContent().toByteArray(),ScopedTransactionRpc.IMPORT_PUBLICATION_PREPARE,command); }
                catch(IllegalArgumentException invalid) { throw new IOException("Import admission preparation changed original command",invalid); }
            }
            save(h); h.fields.put("pendingResult",catalogue.execute(command).encode()); save(h);
        }
        return CommandResult.decode(h.fields.get("pendingResult"));
    }
    private void finish(Handle h,CommandResult result,Phase phase,int cursor) throws IOException {
        var command=DistributedCommandCodec.decode(h.fields.get("pendingCommand"));
        if(command.operationId().sessionId().equals(UUID.fromString(text(h,"issuer")))) {
            var ack=new DistributedCommand(binding.groupId(),binding.epoch(),binding.schemaDigest(),next(h),List.of(),List.of(),command.operationId().sequence()); h.fields.put("pendingAck",DistributedCommandCodec.encode(ack));
        }
        h.fields.remove("pendingCommand"); h.fields.remove("pendingResult"); move(h,result.status()==CommandResult.Status.SUCCESS ? phase : Phase.CONFLICT,cursor);
    }
    private void acknowledge(Handle h) throws IOException {
        save(h); var reply=RatisRoutedClient.rpc(() -> client.io().send(Message.valueOf(ByteString.copyFrom(h.fields.get("pendingAck")))));
        if(reply==null || !reply.isSuccess() || reply.getMessage()==null || reply.getMessage().getContent().size()>CommandResult.MAX_BYTES) throw new IOException("Import acknowledgement unavailable");
        var result=CommandResult.decode(reply.getMessage().getContent().toByteArray()); if(result.status()!=CommandResult.Status.SUCCESS || result.valueCount()!=0) throw new IOException("Import acknowledgement rejected"); h.fields.remove("pendingAck"); save(h);
    }
    private OperationId next(Handle h) { long n=number(h,"sequence"); number(h,"sequence",Math.addExact(n,1)); return new OperationId(UUID.fromString(text(h,"issuer")),0,n); }
    private void validateCommand(Handle h,DistributedCommand command) {
        if(!command.groupId().equals(binding.groupId()) || command.rangeEpoch()!=binding.epoch() || !Arrays.equals(command.schemaDigest(),binding.schemaDigest())) throw new IllegalArgumentException("Foreign import catalogue command");
        var op=command.operationId(); if(!op.equals(h.definition().operation()) && (!op.sessionId().equals(UUID.fromString(text(h,"issuer"))) || op.epoch()!=0 || op.sequence()>=number(h,"sequence"))) throw new IllegalArgumentException("Foreign import operation");
    }
    private void beforeRead(Handle h,String action) throws IOException { putText(h,"read",action); save(h); }
    private void move(Handle h,Phase phase,int cursor) throws IOException { putText(h,"phase",phase.name()); number(h,"cursor",cursor); save(h); }
    private RangeImportAdmission installed(Handle h) { return RangeImportAdmission.decode(h.fields.get("admission"),h.definition()); }
    private RatisRangeImportCatalogue.Head published(Handle h) { return RatisRangeImportCatalogue.Head.decode(h.fields.get("published")); }
    private RangeImportActivationAdmission activated(Handle h) { return RangeImportActivationAdmission.decode(h.fields.get("activationAdmission"),h.definition(),installed(h),binding.groupId(),RangeImportAdmission.sha256(published(h).encode())); }
    private RatisSchemaAdministration.Head schemaHead(Handle h) { return h.fields.containsKey("schemaHead") ? RatisSchemaAdministration.Head.decode(h.fields.get("schemaHead")) : null; }
    private static List<RangeDescriptor> importedRanges(RangeImportDefinition d) { var groups=new HashSet<UUID>(); d.manifests().forEach(m -> groups.add(m.groupId())); return d.sourceDirectory().ranges().stream().filter(r -> groups.contains(r.groupId())).toList(); }
    private static long sourceBytes(RangeImportDefinition d) { long total=0; for(var m:d.manifests()) { total=Math.addExact(total,m.encode().length); for(var c:m.columns()) for(var f:c.files()) total=Math.addExact(total,f.bytes()); } return total; }
    private void requireNamespace(RangeImportDefinition d) { if(!d.sourceDirectory().clusterId().equals(cluster) || !d.sourceDirectory().databaseId().equals(database)) throw new IllegalArgumentException("Foreign import namespace"); }
    private byte[] bindingBytes() { var b=ByteBuffer.allocate(88); ScopedTransactionRpc.uuid(b,binding.groupId()); b.putLong(binding.epoch()).put(binding.schemaDigest()); ScopedTransactionRpc.uuid(b,binding.directoryColumn()); ScopedTransactionRpc.uuid(b,binding.sessionsColumn()); return b.array(); }
    private static String sourceKey(Source source) { return source.groupId()+"/"+source.fileId(); }
    private Path path(UUID id) { return root.resolve(id+".import"); }
    private Path originals(Handle h) { return root.resolve(h.id+".sources"); }
    private Path manifestPath(Handle h,NativeSstImportManifest m) { return originals(h).resolve(m.groupId()+".manifest"); }
    private static String text(Handle h,String key) { return new String(h.fields.get(key),StandardCharsets.UTF_8); }
    private static void putText(Handle h,String key,String value) { h.fields.put(key,value.getBytes(StandardCharsets.UTF_8)); }
    private static long number(Handle h,String key) { return ByteBuffer.wrap(h.fields.get(key)).getLong(); }
    private static void number(Handle h,String key,long value) { h.fields.put(key,ByteBuffer.allocate(8).putLong(value).array()); }
    private void save(Handle h) throws IOException { try { RatisSchemaClientJournal.write(path(h.id),h.fields); } catch(IOException failure) { fenced=true; throw failure; } }
    private static byte[] encodeEntries(Map<String,byte[]> entries) throws IOException {
        var bytes=new ByteArrayOutputStream(); var out=new DataOutputStream(bytes); out.writeInt(entries.size());
        for(var key:entries.keySet().stream().sorted().toList()) { out.writeUTF(key); byte[] value=entries.get(key); out.writeInt(value.length); out.write(value); if(bytes.size()>MAX_METADATA_BYTES-32) throw new IOException("Import metadata entries exceed bound"); }
        out.flush(); return bytes.toByteArray();
    }
    private static Map<String,byte[]> decodeEntries(byte[] bytes) throws IOException {
        if(bytes==null) return new HashMap<>(); var in=new DataInputStream(new ByteArrayInputStream(bytes)); int count=in.readInt();
        if(count<0 || count>MAX_SOURCE_FILES+RangeImportDefinition.MAX_GROUPS) throw new IOException("Invalid import metadata count"); var entries=new HashMap<String,byte[]>();
        for(int i=0;i<count;i++) { String key=in.readUTF(); int n=in.readInt(); if(key.length()>128 || n<0 || n>in.available() || entries.containsKey(key)) throw new IOException("Invalid import metadata entry"); entries.put(key,in.readNBytes(n)); }
        if(in.available()!=0) throw new IOException("Trailing import metadata entries"); return entries;
    }
    private Map<String,byte[]> references(Handle h) throws IOException { return decodeEntries(h.fields.get("references")); }
    private synchronized void begin() throws IOException { requireOpen(); if(busy) throw new IOException("Import I/O still active"); busy=true; }
    private synchronized void end() { busy=false; notifyAll(); }
    private void requireOpen() throws IOException { if(closed || fenced || !lock.isValid()) throw new IOException("Import controller closed/fenced; reopen originals"); }
    @Override public synchronized void close() throws IOException { if(closed) return; if(busy) throw new IOException("Import I/O still active"); closed=true; handles.values().forEach(h -> h.released=true); handles.clear(); try { lock.release(); } finally { lockChannel.close(); } }
}
