package it.cavallium.rockserver.distributed.ratis;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisSchemaClientJournalTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void pendingInputAndResultRecoverAsExactIndependentForcedSnapshots() throws Exception {
        Path file=root.resolve("request"); var fields=new HashMap<String,byte[]>(); fields.put("phase",new byte[]{1}); fields.put("pendingCommand",new byte[]{2,3,4});
        RatisSchemaClientJournal.write(file,fields); byte[] original=Files.readAllBytes(file);
        var recovered=RatisSchemaClientJournal.read(file); assertArrayEquals(fields.get("pendingCommand"),recovered.get("pendingCommand"));
        fields.put("pendingResult",new byte[]{5}); assertArrayEquals(original,Files.readAllBytes(file),"In-memory result is not a durable acknowledgement");
        RatisSchemaClientJournal.write(file,fields); assertArrayEquals(new byte[]{5},RatisSchemaClientJournal.read(file).get("pendingResult"));
    }
    @Test void checksumAndFieldBoundsRejectDamagedRecovery() throws Exception {
        Path file=root.resolve("request"); RatisSchemaClientJournal.write(file,Map.of("phase",new byte[]{1})); byte[] bytes=Files.readAllBytes(file); bytes[8]^=1; Files.write(file,bytes);
        assertThrows(IOException.class,() -> RatisSchemaClientJournal.read(file));
        assertThrows(IOException.class,() -> RatisSchemaClientJournal.write(file,Map.of("x".repeat(129),new byte[]{1})));
        var fields=new HashMap<String,byte[]>(); for(int i=0;i<=RatisSchemaClientJournal.MAX_FIELDS;i++) fields.put("key"+i,new byte[0]);
        assertThrows(IOException.class,() -> RatisSchemaClientJournal.write(file,fields));
    }
    @Test void oversizedInputLeavesTheForcedJournalAndTemporaryPathUntouched() throws Exception {
        Path file=root.resolve("request"); RatisSchemaClientJournal.write(file,Map.of("phase",new byte[]{1}));
        byte[] original=Files.readAllBytes(file);
        assertThrows(IOException.class,() -> RatisSchemaClientJournal.write(file,Map.of("x",new byte[RatisSchemaClientJournal.MAX_BYTES-46])));
        assertArrayEquals(original,Files.readAllBytes(file)); assertFalse(Files.exists(root.resolve("request.pending")));
        assertThrows(IOException.class,() -> RatisSchemaClientJournal.write(file,Map.of()));
    }
    @Test void modifiedUtfKeysRetainTheirExactEncoding() throws Exception {
        Path file=root.resolve("request"); String key="nul\0é𝄞";
        RatisSchemaClientJournal.write(file,Map.of(key,new byte[]{1,2}));
        assertArrayEquals(new byte[]{1,2},RatisSchemaClientJournal.read(file).get(key));
    }
}
