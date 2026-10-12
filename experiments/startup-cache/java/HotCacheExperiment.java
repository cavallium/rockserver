import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.file.Files;
import java.nio.file.Path;

/** Standalone Java 25 driver for the native fixture; never loads RocksJava. */
public final class HotCacheExperiment {
    private static final long OUTPUT_CAPACITY = 16 * 1024;

    public static void main(String[] args) throws Throwable {
        if (args.length != 5) {
            throw new IllegalArgumentException(
                    "Usage: HotCacheExperiment <library.so> <db> <snapshot> <capture|read|recapture|replace> <max-bytes>");
        }
        Path library = Path.of(args[0]).toAbsolutePath().normalize();
        Path db = Path.of(args[1]).toAbsolutePath().normalize();
        Path snapshot = Path.of(args[2]).toAbsolutePath().normalize();
        if (!Files.isRegularFile(library)) throw new IllegalArgumentException("Missing library: " + library);
        Path realDb = Files.exists(db) ? db.toRealPath() : db;
        Path realSnapshot = Files.exists(snapshot) ? snapshot.toRealPath()
                : snapshot.getParent().toRealPath().resolve(snapshot.getFileName());
        if (snapshot.startsWith(db) || realSnapshot.startsWith(realDb)) {
            throw new IllegalArgumentException("Snapshot must be outside the fixture DB directory");
        }
        int mode = switch (args[3]) {
            case "capture" -> 0;
            case "read" -> 1;
            case "recapture" -> 2;
            case "replace" -> 3;
            default -> throw new IllegalArgumentException("Unknown mode: " + args[3]);
        };
        long maxBytes = Long.parseLong(args[4]);
        if (maxBytes < 84 || maxBytes > 64L * 1024 * 1024) {
            throw new IllegalArgumentException("max-bytes must be between 84 and 67108864");
        }
        Linker linker = Linker.nativeLinker();
        var sizeT = linker.canonicalLayouts().get("size_t");
        if (sizeT == null || sizeT.byteSize() != Long.BYTES) {
            throw new UnsupportedOperationException("This experimental ABI requires a 64-bit size_t");
        }
        // All calls are synchronous. Native code owns DB/cache lifetimes and must
        // return only after releasing borrowed strings/output memory.
        try (Arena arena = Arena.ofConfined()) {
            SymbolLookup lookup = SymbolLookup.libraryLookup(library, arena);
            MethodHandle version = linker.downcallHandle(symbol(lookup, "rshc_abi_version"),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT));
            int abi = (int) version.invokeExact();
            if (abi != 1) throw new IllegalStateException("Unsupported native ABI: " + abi);
            MethodHandle run = linker.downcallHandle(symbol(lookup, "rshc_run"),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS, ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG,
                            ValueLayout.ADDRESS, sizeT));
            MemorySegment dbName = arena.allocateFrom(db.toString());
            MemorySegment snapshotName = arena.allocateFrom(snapshot.toString());
            MemorySegment output = arena.allocate(OUTPUT_CAPACITY);
            output.fill((byte) 0);
            int result = (int) run.invokeExact(dbName, snapshotName, mode, maxBytes, output, OUTPUT_CAPACITY);
            boolean terminated = false;
            for (long i = 0; i < output.byteSize(); i++) {
                if (output.get(ValueLayout.JAVA_BYTE, i) == 0) {
                    terminated = true;
                    break;
                }
            }
            if (!terminated) throw new IllegalStateException("Native output exceeded bounded buffer");
            String report = output.getString(0);
            if (result != 0) throw new IllegalStateException("Native experiment failed (" + result + "): " + report);
            if (!report.startsWith("{") || !report.endsWith("}")) {
                throw new IllegalStateException("Expected native JSON report: " + report);
            }
            System.out.println(report);
        }
    }

    private static MemorySegment symbol(SymbolLookup lookup, String name) {
        return lookup.find(name).orElseThrow(() -> new UnsatisfiedLinkError("Missing experiment symbol: " + name));
    }
}
