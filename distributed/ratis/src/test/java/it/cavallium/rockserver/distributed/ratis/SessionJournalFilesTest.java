package it.cavallium.rockserver.distributed.ratis;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class SessionJournalFilesTest {
    @TempDir Path root;
    @Test void exactLargePayloadAndPartialUnpublishedSidecarAreRemovedBeforeDirectoryForce() throws Exception {
        var path=root.resolve("large.bin"); var sidecar=root.resolve("large.bin.pending"); byte[] body=new byte[2*1024*1024+17]; new Random(73).nextBytes(body); int maximum=body.length+32;
        SessionJournalFiles.write(path,body,maximum); Files.write(sidecar,new byte[]{1,2}); var events=new ArrayList<Path>();
        SessionJournalFiles.remove(path,body,maximum,new SessionJournalFiles.Removal() {
            public void delete(Path file) throws IOException { events.add(file); Files.delete(file); }
            public void force(Path directory) throws IOException { assertFalse(Files.exists(path)); assertFalse(Files.exists(sidecar)); events.add(directory); SessionJournalFiles.force(directory); }
        });
        assertEquals(List.of(sidecar,path,root),events);
    }
    @Test void alteredChunkOrChecksumPreventsAnyRemovalIncludingTheSidecar() throws Exception {
        for (int offset:new int[]{65536,131073}) {
            var path=root.resolve("changed-"+offset+".bin"); var sidecar=path.resolveSibling(path.getFileName()+".pending"); byte[] body=new byte[131073]; new Random(74).nextBytes(body);
            SessionJournalFiles.write(path,body,body.length+32); Files.write(sidecar,new byte[]{1}); byte[] changed=Files.readAllBytes(path); changed[offset]^=1; Files.write(path,changed);
            assertThrows(IOException.class,() -> SessionJournalFiles.remove(path,body,body.length+32,file -> fail("Validation must precede every unlink")));
            assertArrayEquals(changed,Files.readAllBytes(path)); assertTrue(Files.exists(sidecar));
        }
    }
    @Test void oversizedOrNonFileSidecarPreservesTheQualifiedPublishedJournal() throws Exception {
        var path=root.resolve("bounds.bin"); var sidecar=root.resolve("bounds.bin.pending"); byte[] body=new byte[16]; SessionJournalFiles.write(path,body,48);
        Files.write(sidecar,new byte[49]); assertThrows(IOException.class,() -> SessionJournalFiles.remove(path,body,48)); assertArrayEquals(body,SessionJournalFiles.read(path,48));
        Files.delete(sidecar); Files.createDirectory(sidecar); assertThrows(IOException.class,() -> SessionJournalFiles.remove(path,body,48)); assertTrue(Files.isDirectory(sidecar)); assertTrue(Files.exists(path));
    }
}
