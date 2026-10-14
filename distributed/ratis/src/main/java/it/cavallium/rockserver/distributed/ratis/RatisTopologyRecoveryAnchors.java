package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;

/** Independently retained frozen GROUP images, charged to the source artifact reservation ledger. */
final class RatisTopologyRecoveryAnchors {
    static final int MAX_METADATA_BYTES=128*1024;
    private static final long DIRECTORY_METADATA_BYTES=3L*MAX_METADATA_BYTES-2*40; // Reservation marker and its pending replacement share this ceiling.
    record Saved(NativeTopologyFreeze freeze,NativeTopologyRecoveryFence fence,ReplicatedRangeStore.Configuration configuration,
                 NativeRangeImage.Manifest image,Path file) {}
    private final RatisTopologySourceSnapshotArtifacts artifacts;
    private final NativeRangeImage.Limits limits;
    RatisTopologyRecoveryAnchors(RatisTopologySourceSnapshotArtifacts artifacts,NativeRangeImage.Limits limits){this.artifacts=Objects.requireNonNull(artifacts);this.limits=Objects.requireNonNull(limits);}
    private static UUID id(NativeTopologyFreeze freeze,TopologyChange.Owner owner){return UUID.nameUUIDFromBytes(ByteBuffer.allocate(48).putLong(freeze.header().intentId().getMostSignificantBits()).putLong(freeze.header().intentId().getLeastSignificantBits()).putLong(freeze.groupId().getMostSignificantBits()).putLong(freeze.groupId().getLeastSignificantBits()).putLong(owner.replicaId().getMostSignificantBits()).putLong(owner.replicaId().getLeastSignificantBits()).array());}
    synchronized Saved capture(TopologyChange.Definition definition,ReplicatedRangeStore store,NativeTopologyFreeze freeze,ReplicaDescriptor receiver,ApplyPosition catalogueCut)throws Exception {
        var owner=TopologyChange.Owner.of(receiver);var directory=artifacts.reserveTopologyRecovery(id(freeze,owner));
        Path original=directory.resolve("capture"),image=directory.resolve("image"),receipt=directory.resolve("anchor");
        var fields=recoverFields(original,value->requireCapture(value,freeze,owner,catalogueCut));
        final Map<String,byte[]> captured=fields;
        var retained=recoverFields(receipt,value->requireReceipt(image,value,captured,freeze,owner,catalogueCut));
        if(retained!=null)return read(directory,freeze,owner,catalogueCut);
        if(fields==null && Files.exists(image,LinkOption.NOFOLLOW_LINKS))throw new IOException("Topology recovery image lost its original capture authority");
        if(fields!=null && !Files.exists(image,LinkOption.NOFOLLOW_LINKS) && !cut(fields).equals(store.applied()))throw new IOException("Original topology anchor capture lost its exact native cut");
        if(!Files.exists(image,LinkOption.NOFOLLOW_LINKS)) {
            requireWriteSlot(directory);
            try(var pin=store.captureForRecovery()) {
                freeze.requireCut(pin.position());if(!pin.groupId().equals(freeze.groupId()))throw new IOException("Anchor pin crossed original source");
                var cfg=pin.configuration();if(cfg==null)throw new IOException("Anchor lacks original native configuration");
                freeze.header().requireDefinition(definition);var range=definition.parents().stream().filter(p->p.groupId().equals(freeze.groupId())).findFirst().orElseThrow();
                if(!range.replicas().contains(receiver))throw new IOException("Anchor receiver is not original source owner");RatisRangeStateMachine.requireSchemaMembership(range,cfg.data());
                if(fields==null) {
                    fields=new HashMap<>();fields.put("freeze",freeze.encode());fields.put("fence",new NativeTopologyRecoveryFence(freeze.header(),freeze.groupId(),owner,catalogueCut,new byte[32]).encode());fields.put("cut",position(pin.position()));fields.put("configuration",configuration(cfg));
                    writeFields(original,fields);
                } else if(!cut(fields).equals(pin.position()) || !Arrays.equals(fields.get("configuration"),configuration(cfg)))throw new IOException("Topology anchor capture changed original cut/configuration");
                NativeRangeImage.write(image,pin,pin.interval(),limits);
            }
        }
        var manifest=validateImage(image,fields);fields=new HashMap<>(fields);fields.put("hash",manifest.sha256());fields.put("bytes",ByteBuffer.allocate(8).putLong(manifest.bytes()).array());writeFields(receipt,fields);
        return read(directory,freeze,owner,catalogueCut);
    }
    Saved read(NativeTopologyFreeze freeze,ReplicaDescriptor receiver)throws IOException {
        var directory=artifacts.topologyRecoveryDirectory(id(freeze,TopologyChange.Owner.of(receiver)));var capture=directory.resolve("capture");
        var original=NativeTopologyRecoveryFence.decode(readFields(Files.exists(capture,LinkOption.NOFOLLOW_LINKS)?capture:directory.resolve("capture.pending")).get("fence"));
        return read(directory,freeze,TopologyChange.Owner.of(receiver),original.catalogueCut());
    }
    private Saved read(Path directory,NativeTopologyFreeze freeze,TopologyChange.Owner owner,ApplyPosition catalogueCut)throws IOException {
        var captured=recoverFields(directory.resolve("capture"),value->requireCapture(value,freeze,owner,catalogueCut));
        var fields=recoverFields(directory.resolve("anchor"),value->requireReceipt(directory.resolve("image"),value,captured,freeze,owner,catalogueCut));
        if(fields==null)throw new IOException("Topology recovery anchor has no completed receipt");
        var image=validateImage(directory.resolve("image"),fields);
        var original=NativeTopologyRecoveryFence.decode(fields.get("fence"));var fence=new NativeTopologyRecoveryFence(freeze.header(),freeze.groupId(),owner,original.catalogueCut(),image.sha256());
        SessionJournalFiles.force(directory.resolve("image"));SessionJournalFiles.force(directory.resolve("anchor"));SessionJournalFiles.force(directory);
        return new Saved(freeze,fence,ReplicatedRangeStore.Configuration.decode(fields.get("configuration")),image,directory.resolve("image"));
    }
    @FunctionalInterface private interface FieldsValidator {void validate(Map<String,byte[]> fields)throws IOException;}
    /** Reconcile complete exact sidecars before choosing a cut; uncertain bytes are retained, never overwritten. */
    private static Map<String,byte[]> recoverFields(Path path,FieldsValidator validator)throws IOException {
        var pending=path.resolveSibling(path.getFileName()+".pending");
        Map<String,byte[]> current=Files.exists(path,LinkOption.NOFOLLOW_LINKS)?readFields(path):null;
        if(current!=null)validator.validate(current);
        if(Files.exists(pending,LinkOption.NOFOLLOW_LINKS)) {
            var next=readFields(pending);validator.validate(next);
            if(current!=null && !sameFields(current,next))throw new IOException("Topology recovery sidecar conflicts with original "+path.getFileName());
            SessionJournalFiles.force(pending);
            if(current==null)Files.move(pending,path,StandardCopyOption.ATOMIC_MOVE);
            else {SessionJournalFiles.force(path);Files.delete(pending);}
            SessionJournalFiles.force(path);SessionJournalFiles.force(path.getParent());current=next;
        }
        return current;
    }
    private static boolean sameFields(Map<String,byte[]> first,Map<String,byte[]> second){return first.keySet().equals(second.keySet()) && first.entrySet().stream().allMatch(row->Arrays.equals(row.getValue(),second.get(row.getKey())));}
    private static void requireCapture(Map<String,byte[]> fields,NativeTopologyFreeze freeze,TopologyChange.Owner owner,ApplyPosition catalogueCut)throws IOException {
        if(!fields.keySet().equals(Set.of("freeze","fence","cut","configuration")))throw new IOException("Invalid original topology capture fields");
        requireOriginal(fields,freeze,owner,catalogueCut);
        try {var cut=cut(fields);freeze.requireCut(cut);var cfg=ReplicatedRangeStore.Configuration.decode(fields.get("configuration"));if(cfg.index()>=0 && !RatisRelocationOwnerRpc.covers(cut,new ApplyPosition(cfg.term(),cfg.index())))throw new IOException("Capture configuration exceeds original cut");}
        catch(IllegalArgumentException|IllegalStateException invalid){throw new IOException("Invalid original topology cut/configuration",invalid);}
    }
    private void requireReceipt(Path image,Map<String,byte[]> fields,Map<String,byte[]> captured,NativeTopologyFreeze freeze,TopologyChange.Owner owner,ApplyPosition catalogueCut)throws IOException {
        if(captured==null || !fields.keySet().equals(Set.of("freeze","fence","cut","configuration","hash","bytes")))throw new IOException("Completed anchor lacks exact original capture");
        requireOriginal(fields,freeze,owner,catalogueCut);for(var row:captured.entrySet())if(!Arrays.equals(row.getValue(),fields.get(row.getKey())))throw new IOException("Completed anchor changes original capture");
        var manifest=validateImage(image,fields);var bytes=fields.get("bytes");
        if(!Arrays.equals(manifest.sha256(),fields.get("hash")) || bytes==null || bytes.length!=8 || manifest.bytes()!=ByteBuffer.wrap(bytes).getLong())throw new IOException("Retained topology image identity changed");
    }
    private static void requireWriteSlot(Path directory)throws IOException {
        try(var entries=Files.newDirectoryStream(directory,".native-range-building-*.tmp")){if(entries.iterator().hasNext())throw new IOException("Unsettled topology image scratch prevents another writer");}
    }
    private NativeRangeImage.Manifest validateImage(Path image,Map<String,byte[]> fields)throws IOException {
        var freeze=NativeTopologyFreeze.decode(fields.get("freeze"));var configuration=fields.get("configuration");byte[][] actual={null,null};
        var manifest=NativeRangeImage.visit(image,limits,(section,column,key,value)->{if(section==NativeRangeImage.Section.STATE && key.length==17){if(key[16]==1)actual[0]=value;if(key[16]==0x60)actual[1]=value;}});
        freeze.requireCut(manifest.position());
        if(!manifest.owner().groupId().equals(freeze.groupId()) || !manifest.position().equals(cut(fields)) || !Arrays.equals(actual[0],configuration) || !Arrays.equals(actual[1],freeze.encode()))throw new IOException("Topology recovery image lost original GROUP cut/configuration/FREEZE");
        return manifest;
    }
    private static void requireOriginal(Map<String,byte[]> fields,NativeTopologyFreeze freeze,TopologyChange.Owner owner,ApplyPosition catalogueCut)throws IOException {
        try {var fence=NativeTopologyRecoveryFence.decode(fields.get("fence"));if(!Arrays.equals(fields.get("freeze"),freeze.encode()) || !fence.owner().equals(owner) || !fence.groupId().equals(freeze.groupId()) || !Arrays.equals(fence.header().encode(),freeze.header().encode()) || !RatisRelocationOwnerRpc.covers(catalogueCut,fence.catalogueCut()))throw new IOException("Topology anchor changed original capture authority");}
        catch(IllegalArgumentException invalid){throw new IOException("Invalid original topology capture",invalid);}
    }
    private static byte[] position(ApplyPosition cut){return ByteBuffer.allocate(16).putLong(cut.term()).putLong(cut.index()).array();}
    private static ApplyPosition cut(Map<String,byte[]> fields)throws IOException {var bytes=fields.get("cut");if(bytes==null || bytes.length!=16)throw new IOException("Invalid topology capture cut");var in=ByteBuffer.wrap(bytes);return new ApplyPosition(in.getLong(),in.getLong());}
    private static byte[] configuration(ReplicatedRangeStore.Configuration cfg){return ByteBuffer.allocate(16+cfg.data().length).putLong(cfg.term()).putLong(cfg.index()).put(cfg.data()).array();}
    private static Map<String,byte[]> readFields(Path path)throws IOException {if(!Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS) || Files.size(path)>MAX_METADATA_BYTES)throw new IOException("Invalid topology anchor metadata");return RatisSchemaClientJournal.read(path);}
    private static void writeFields(Path path,Map<String,byte[]> fields)throws IOException {long size=1024;for(var bytes:fields.values())size+=bytes.length;if(size>MAX_METADATA_BYTES)throw new IOException("Topology anchor metadata budget exhausted");RatisSchemaClientJournal.write(path,fields);SessionJournalFiles.force(path);SessionJournalFiles.force(path.getParent());}
    static void validateDirectory(Path directory,NativeRangeImage.Limits limits)throws IOException {
        int files=0,images=0;long metadata=0;
        try(var entries=Files.newDirectoryStream(directory)){for(var file:entries){
            if(++files>6 || !Files.isRegularFile(file,LinkOption.NOFOLLOW_LINKS))throw new IOException("Invalid recovery artifact directory");
            String name=file.getFileName().toString();boolean image=name.equals("image") || name.matches("[.]native-range-building-.*[.]tmp");long bytes=Files.size(file);
            if(image && ++images>2 || !image && !Set.of("capture","capture.pending","anchor","anchor.pending").contains(name) || bytes>(image?limits.maxImageBytes():MAX_METADATA_BYTES))throw new IOException("Recovery artifact quota/identity exceeded");
            if(!image && (metadata=Math.addExact(metadata,bytes))>DIRECTORY_METADATA_BYTES)throw new IOException("Recovery aggregate metadata reservation exceeded");
        }}
    }
}
