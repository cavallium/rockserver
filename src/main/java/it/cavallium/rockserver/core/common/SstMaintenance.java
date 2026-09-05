package it.cavallium.rockserver.core.common;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/** Live physical metadata and same-level manual compaction contracts. */
public final class SstMaintenance {
    private SstMaintenance() {}
    public static final int MAX_INPUT_FILES = 256;
    public static final int MAX_METADATA_RESPONSE_BYTES = 64 * 1024 * 1024;

    /** Keys are raw RocksDB user keys encoded as lowercase hex, in bytewise order. */
    public record File(String name, int level, int pathId, long sizeBytes,
                       String smallestKeyHex, String largestKeyHex, boolean beingCompacted) {}

    /** A metadata observation, not a pinned file set. Session changes on every DB reopen. */
    public record Metadata(String session, long columnId, String columnName, int numLevels,
                           int baseLevel, List<String> paths, List<File> files) {
        public Metadata { paths = List.copyOf(paths); files = List.copyOf(files); }
    }

    /**
     * Same-level L1+ compaction. execute=false only validates, without flushing or pinning.
     * maxInputBytes bounds the observed selection, NOT native input expansion or output I/O.
     * A timeout/disconnect does not prove an in-flight compaction has stopped.
     */
    public record Request(long columnId, String session, List<String> files, int level,
                          int outputPathId, long outputFileSizeLimit, long maxInputBytes,
                          int maxSubcompactions, boolean execute) {
        public Request {
            Objects.requireNonNull(session, "session");
            files = List.copyOf(files);
            if (session.isBlank() || session.length() > 128 || files.isEmpty()
                    || files.size() > MAX_INPUT_FILES || new HashSet<>(files).size() != files.size()
                    || level < 1 || outputPathId < 0 || outputFileSizeLimit <= 0
                    || maxInputBytes <= 0 || maxSubcompactions < 1 || maxSubcompactions > 16) {
                throw invalid("Invalid selective compaction parameters");
            }
            for (String file : files) {
                if (!file.matches("[0-9]{1,20}\\.sst")) throw invalid("Expected SST basename, not a path");
            }
        }
    }

    /** Actual native input names may exceed the preflight selection after expansion. */
    public record Result(boolean executed, List<String> inputFiles, List<String> outputFiles,
                         long validatedInputBytes, long outputBytes, long elapsedNanos) {
        public Result { inputFiles = List.copyOf(inputFiles); outputFiles = List.copyOf(outputFiles); }
    }

    public static RocksDBException invalid(String message) {
        return RocksDBException.of(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, message);
    }
}
