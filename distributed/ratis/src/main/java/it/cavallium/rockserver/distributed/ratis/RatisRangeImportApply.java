package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import org.rocksdb.RocksDBException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

/** Borrowed group resources; committed methods run on the state's actual completed apply lane. */
public final class RatisRangeImportApply {
    public record PublicationView(RatisRangeImportRecord.Publication publication,RangeDirectory directory) {
        public PublicationView { Objects.requireNonNull(publication); Objects.requireNonNull(directory); }
    }
    /** Runtime-owned authority reader: both values must come from the same completed catalogue cut. */
    @FunctionalInterface public interface PublicationReader { PublicationView read(RangeImportDefinition definition) throws IOException; }
    public enum Phase { FENCED,INSTALLED,PUBLISHED }
    public record Status(UUID groupId,long rangeEpoch,byte[] definitionDigest,byte[] manifestDigest,
                         ApplyPosition applied,Phase phase,NativeSstImportManifest.Receipt receipt,NativeRangeSstImport.Activation activation) {
        public Status {
            Objects.requireNonNull(groupId); Objects.requireNonNull(phase);
            if(rangeEpoch<0 || definitionDigest.length!=32 || manifestDigest.length!=32 || phase==Phase.FENCED && (receipt!=null || activation!=null) || phase==Phase.INSTALLED && (receipt==null || activation!=null) || phase==Phase.PUBLISHED && (receipt==null || activation==null))throw new IllegalArgumentException("Invalid import status proof/phase");
            if(receipt!=null) { if(!groupId.equals(receipt.groupId()) || rangeEpoch!=receipt.rangeEpoch() || !Arrays.equals(manifestDigest,receipt.manifestDigest()))throw new IllegalArgumentException("Import status receipt binding differs"); receipt.requireCut(applied); }
            if(activation!=null) { if(!Arrays.equals(receipt.encode(),activation.receipt().encode()))throw new IllegalArgumentException("Import status activation differs from receipt"); activation.requireCut(applied); }
            definitionDigest=definitionDigest.clone(); manifestDigest=manifestDigest.clone();
        }
        @Override public byte[] definitionDigest() { return definitionDigest.clone(); }
        @Override public byte[] manifestDigest() { return manifestDigest.clone(); }
    }
    private final DistributedStoragePool pool;
    private final ReplicatedRangeStore store;
    private final RatisReferencedFiles files;
    private final RangeImportDefinition definition;
    private final NativeSstImportManifest manifest;
    private final NativeRangeSstImport.Observer importObserver;
    public RatisRangeImportApply(DistributedStoragePool pool,ReplicatedRangeStore store,RatisReferencedFiles files,RangeImportDefinition definition) {
        this(pool,store,files,definition,stage->{});
    }
    RatisRangeImportApply(DistributedStoragePool pool,ReplicatedRangeStore store,RatisReferencedFiles files,RangeImportDefinition definition,NativeRangeSstImport.Observer importObserver) {
        this.pool=Objects.requireNonNull(pool); this.store=Objects.requireNonNull(store); this.files=Objects.requireNonNull(files); this.definition=Objects.requireNonNull(definition);
        this.importObserver=Objects.requireNonNull(importObserver);
        var owner=store.ownership().owner(); manifest=definition.manifest(owner.groupId()); files.requireGroup(owner.groupId());
        if(owner.epoch()!=manifest.rangeEpoch() || !store.ownership().interval().domainId().equals(manifest.domainId()) || !Arrays.equals(store.ownership().interval().lowerInclusive(),manifest.lowerInclusive()) || !Arrays.equals(store.ownership().interval().upperExclusive(),manifest.upperExclusive()))throw new IllegalArgumentException("Import apply owner differs from frozen definition");
    }
    public UUID groupId() { return manifest.groupId(); }
    public RangeImportDefinition definition() { return definition; }
    public void requireGroup(UUID group) { if(!groupId().equals(group))throw new IllegalArgumentException("Foreign import apply group"); }
    /** Original reply uses immutable binding fields and the store's own metadata synchronization; it never borrows the trusted RPC monitor. */
    public byte[] recoveredResult(RatisRangeImportRecord record) throws RocksDBException {
        record.requireDefinition(definition);
        if(record instanceof RatisRangeImportRecord.Install install) {
            var receipt=store.importReceipt(); if(receipt==null)return null;
            receipt.requireManifest(manifest); install.requireCut(receipt.position()); receipt.requireCut(store.applied()); return receipt.encode();
        }
        var activate=(RatisRangeImportRecord.Activate)record; var saved=store.importActivation(); if(saved==null)return null;
        requireInstalled(activate); requireSavedActivation(activate,saved); saved.requireCut(store.applied()); return saved.encode();
    }

    /** Pre-append validation copies/qualifies originals but changes no native apply position or user interval. */
    public synchronized void validateInstallProposal(RatisRangeImportRecord.Install record) throws IOException,RocksDBException {
        record.requireDefinition(definition);
        var saved=store.importReceipt();
        if(saved!=null) { saved.requireManifest(manifest); record.requireCut(saved.position()); return; }
        try(var pinned=pin(record)) { pool.validateSstImport(store,manifest,pinned.paths); }
    }
    public synchronized NativeSstImportManifest.Receipt applyInstall(ApplyPosition position,RatisRangeImportRecord.Install record) throws IOException,RocksDBException {
        record.requireDefinition(definition); record.requireCut(position);
        var saved=store.importReceipt();
        if(saved!=null) { saved.requireManifest(manifest); record.requireCut(saved.position()); return pool.importSst(store,manifest,Map.of(),position); }
        try(var pinned=pin(record)) { return pool.importSst(store,manifest,pinned.paths,position,importObserver); }
    }
    /** Only runtime-owned same-cut catalogue authority can qualify a fresh ACTIVATE proposal. */
    public synchronized void validateActivationProposal(RatisRangeImportRecord.Activate record,PublicationReader trustedReader) throws IOException,RocksDBException {
        requireInstalled(record); var saved=store.importActivation();
        if(saved!=null) { requireSavedActivation(record,saved); return; }
        if(trustedReader==null)throw new IllegalStateException("Trusted catalogue publication reader is not configured");
        var view=Objects.requireNonNull(trustedReader.read(definition),"Catalogue publication result"); view.publication().requireDefinition(definition);
        if(!Arrays.equals(view.publication().encode(),record.publication().encode()) || !Arrays.equals(view.directory().payloadDigest(),definition.targetDirectory().payloadDigest()))throw new IllegalArgumentException("Activation differs from exact same-cut published head/ACTIVE target directory");
    }
    /** Pure committed replay: no catalogue RPC, and every native effect completes before returning. */
    public synchronized NativeRangeSstImport.Activation applyActivate(ApplyPosition position,RatisRangeImportRecord.Activate record) throws RocksDBException {
        requireInstalled(record); var saved=store.importActivation();
        if(saved!=null) { requireSavedActivation(record,saved); saved.requireCut(position); store.applyNoop(position); return saved; }
        var publication=record.publication();
        var authority=new NativeRangeSstImport.Activation(record.receipt(),publication.catalogueGroup(),publication.completedCatalogueCut(),publication.digest(),position);
        pool.activateImport(store,authority); return store.importActivation();
    }
    public synchronized Status status() throws RocksDBException {
        for(int attempt=0;attempt<3;attempt++) {
            var before=store.applied(); var activation=store.importActivation(); var receipt=store.importReceipt(); boolean pending=store.importPending(); var after=store.applied();
            if(!Objects.equals(before,after))continue;
            if(receipt!=null) { receipt.requireManifest(manifest); receipt.requireCut(after); }
            if(activation!=null) { activation.receipt().requireManifest(manifest); activation.requireCut(after); }
            if(!pending && activation==null)throw new IllegalStateException("Native import status lacks publication authority");
            return new Status(groupId(),manifest.rangeEpoch(),definition.digest(),manifest.digest(),after,activation!=null ? Phase.PUBLISHED : receipt!=null ? Phase.INSTALLED : Phase.FENCED,receipt,activation);
        }
        throw new IllegalStateException("Native import completed cut changed during status read");
    }
    private void requireInstalled(RatisRangeImportRecord.Activate record) {
        record.requireDefinition(definition); var installed=store.importReceipt();
        if(installed==null || !Arrays.equals(installed.encode(),record.receipt().encode()))throw new IllegalArgumentException("Activation requires exact original native installed receipt");
        installed.requireCut(store.applied());
    }
    private static void requireSavedActivation(RatisRangeImportRecord.Activate record,NativeRangeSstImport.Activation saved) {
        var publication=record.publication();
        if(!Arrays.equals(saved.receipt().encode(),record.receipt().encode()) || !saved.catalogueGroup().equals(publication.catalogueGroup()) || !saved.cataloguePosition().equals(publication.completedCatalogueCut()) || !Arrays.equals(saved.publicationDigest(),publication.digest()))throw new IllegalArgumentException("Activation retry changes original publication authority");
    }
    private Pins pin(RatisRangeImportRecord.Install record) throws IOException {
        var pinned=new Pins();
        try {
            var entries=record.fileReferences().entrySet().stream().sorted(Map.Entry.comparingByKey(Comparator.comparing(UUID::toString))).toList();
            var refs=new ArrayList<RatisReferencedFile>();refs.add(record.manifestReference());entries.forEach(entry->refs.add(entry.getValue()));
            pinned.readers.addAll(files.pinAll(refs));var metadata=pinned.readers.getFirst();
            var restored=NativeSstImportManifest.decode(metadata.read().toByteArray()); if(!Arrays.equals(restored.encode(),manifest.encode()))throw new IllegalArgumentException("Referenced manifest differs from frozen original import");
            for(int i=0;i<entries.size();i++)pinned.paths.put(entries.get(i).getKey(),pinned.readers.get(i+1).immutablePath());
            return pinned;
        } catch(IOException|RuntimeException failure) { try { pinned.close(); } catch(IOException cleanup) { failure.addSuppressed(cleanup); } throw failure; }
    }
    private static final class Pins implements AutoCloseable {
        final List<RatisReferencedFiles.Reader> readers=new ArrayList<>(); final Map<UUID,Path> paths=new HashMap<>();
        @Override public void close() throws IOException { IOException failure=null; for(int i=readers.size()-1;i>=0;i--)try { readers.get(i).close(); } catch(IOException error) { if(failure==null)failure=error; else failure.addSuppressed(error); } if(failure!=null)throw failure; }
    }
}
