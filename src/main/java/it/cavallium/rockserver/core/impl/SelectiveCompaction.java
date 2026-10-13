package it.cavallium.rockserver.core.impl;

import it.cavallium.rockserver.core.common.SstMaintenance;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import java.nio.file.Path;
import java.util.*;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.CompactionOptions;
import org.rocksdb.CompactionJobInfo;
import org.rocksdb.RocksDB;

/** Called only while the DB and column operation leases are held. */
public final class SelectiveCompaction {
    private SelectiveCompaction() {}

    public static SstMaintenance.Metadata metadata(RocksDB db, ColumnFamilyHandle column,
            List<String> configuredPaths, String session, long columnId, int level)
            throws org.rocksdb.RocksDBException {
        return boundedMetadata(db,column,configuredPaths,session,columnId,level,SstMaintenance.MAX_INPUT_FILES,SstMaintenance.MAX_METADATA_RESPONSE_BYTES);
    }

    /** Called with DB/CF leases retained; native admission precedes inventory copies. */
    public static SstMaintenance.Metadata boundedMetadata(RocksDB db,ColumnFamilyHandle column,
            List<String> configuredPaths,String session,long columnId,int level,int maxFiles,long maxBytes)
            throws org.rocksdb.RocksDBException {
        if(maxFiles<1 || maxFiles>SstMaintenance.MAX_INPUT_FILES || maxBytes<1 || maxBytes>SstMaintenance.MAX_METADATA_RESPONSE_BYTES)throw SstMaintenance.invalid("Invalid physical metadata limits");
        if(level < -1)throw SstMaintenance.invalid("Invalid level");
        // Reserve response-only identity/path strings before requesting native
        // inventory. The remainder also covers JNI arrays, defensive key/name
        // copies, UTF-16 hex (four bytes per raw byte), and retained native fields.
        long bytes=128L+2L*session.length();
        for(var path:configuredPaths){bytes=Math.addExact(bytes,2L*path.length());if(bytes>=maxBytes)throw SstMaintenance.invalid("Physical metadata exceeds response limit");}
        if(bytes>=maxBytes)throw SstMaintenance.invalid("Physical metadata exceeds response limit");
        var paths=configuredPaths;
        if(paths.isEmpty()) {
            paths=List.of(Path.of(db.getName()).toAbsolutePath().normalize().toString());
            bytes=Math.addExact(bytes,2L*paths.getFirst().length());
        }
        long nativeBytes=(maxBytes-bytes)/8;
        if(nativeBytes<1)throw SstMaintenance.invalid("Physical metadata exceeds response limit");
        var meta=db.getBoundedColumnFamilyInventory(column,level,maxFiles,nativeBytes);
        if(level>=meta.numberLevels)throw SstMaintenance.invalid("Invalid level");
        byte[] columnName=meta.name();
        bytes=Math.addExact(bytes,2L*columnName.length);
        if(bytes>maxBytes)throw SstMaintenance.invalid("Physical metadata exceeds response limit");
        var files=new ArrayList<SstMaintenance.File>(meta.files.size());
        var hex=HexFormat.of();
        for(var file:meta.files) {
            if(file.level<0 || file.level>=meta.numberLevels || level!=-1 && file.level!=level)throw SstMaintenance.invalid("Native SST level differs from selected inventory");
            byte[] smallest=file.smallestKey(),largest=file.largestKey();
            bytes=Math.addExact(bytes,96L+2L*file.name.length()+4L*(smallest.length+(long)largest.length));
            if(bytes>maxBytes)throw SstMaintenance.invalid("Physical metadata exceeds response limit");
            int pathId=paths.indexOf(Path.of(file.directory).toAbsolutePath().normalize().toString());
            if(pathId<0 || file.pathId!=pathId)throw SstMaintenance.invalid("SST path is absent from configured CF paths");
            files.add(new SstMaintenance.File(basename(file.name),file.level,pathId,file.size,
                    hex.formatHex(smallest),hex.formatHex(largest),file.beingCompacted));
        }
        files.sort(Comparator.comparingInt(SstMaintenance.File::level)
                .thenComparing(SstMaintenance.File::smallestKeyHex).thenComparing(SstMaintenance.File::name));
        return new SstMaintenance.Metadata(session,columnId,
                new String(columnName,java.nio.charset.StandardCharsets.UTF_8),meta.numberLevels,
                meta.baseLevel,paths,files);
    }

    @org.jetbrains.annotations.VisibleForTesting
    public static List<SstMaintenance.File> validate(SstMaintenance.Metadata meta, SstMaintenance.Request request) {
        if (!meta.session().equals(request.session())) throw conflict("DB session changed; refresh metadata");
        if (meta.columnId() != request.columnId() || request.level() >= meta.numLevels()
                || request.outputPathId() >= meta.paths().size()) throw SstMaintenance.invalid("Invalid column, level or path");
        var wanted = new HashSet<>(request.files());
        var level = meta.files().stream().filter(f -> f.level() == request.level()).toList();
        var selected = level.stream().filter(f -> wanted.contains(f.name())).toList();
        if (selected.size() != wanted.size()) throw conflict("Selected SSTs disappeared or changed level; refresh metadata");
        int first = level.indexOf(selected.getFirst());
        int last = level.indexOf(selected.getLast());
        if (last-first+1 != selected.size()) throw SstMaintenance.invalid("SST selection must be contiguous in user-key order");
        if (first > 0 && level.get(first-1).largestKeyHex().compareTo(selected.getFirst().smallestKeyHex()) >= 0
                || last+1 < level.size() && selected.getLast().largestKeyHex().compareTo(level.get(last+1).smallestKeyHex()) >= 0) {
            throw SstMaintenance.invalid("Selection shares a user-key boundary with an unselected SST");
        }
        long bytes = 0;
        for (var f : selected) {
            if (f.beingCompacted()) throw conflict("An input SST is already being compacted");
            if (f.sizeBytes() > request.maxInputBytes()-bytes) throw SstMaintenance.invalid("Observed input exceeds maxInputBytes");
            bytes += f.sizeBytes();
        }
        return selected;
    }

    public static SstMaintenance.Result compact(RocksDB db, ColumnFamilyHandle column,
            SstMaintenance.Metadata meta, SstMaintenance.Request request) throws org.rocksdb.RocksDBException {
        var selected = validate(meta, request);
        long inputBytes = selected.stream().mapToLong(SstMaintenance.File::sizeBytes).sum();
        var names = selected.stream().map(SstMaintenance.File::name).toList();
        if (!request.execute()) return new SstMaintenance.Result(false, names, List.of(), inputBytes, 0, 0);
        long start = System.nanoTime();
        try (var options = new CompactionOptions().setOutputFileSizeLimit(request.outputFileSizeLimit())
                    .setMaxSubcompactions(request.maxSubcompactions());
             var info = new CompactionJobInfo()) {
            // Inherit the current CF compression policy. No CF option or global pause is changed.
            var output = db.compactFiles(options, column, names, request.level(), request.outputPathId(), info);
            try (var stats = info.stats()) {
                return new SstMaintenance.Result(true, info.inputFiles().stream().map(SelectiveCompaction::basename).toList(),
                        output.stream().map(SelectiveCompaction::basename).toList(), inputBytes,
                        stats.totalOutputBytes(), System.nanoTime()-start);
            }
        }
    }

    private static String basename(String name) { return Path.of(name).getFileName().toString(); }
    private static RocksDBException conflict(String message) {
        return RocksDBException.of(RocksDBErrorType.COMPACTION_CONFLICT, message);
    }
}
