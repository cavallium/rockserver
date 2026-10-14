package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisBatchRequestJournalTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    RatisRocksDBBatchesTest fixture(){var f=new RatisRocksDBBatchesTest();f.root=root;return f;}
    @Test void lifetimeEmptyInputsKeepCanonicalWindowAcrossColdExpiredAndRecentRetry()throws Exception {
        try(var f=fixture().new Fixture()){
            var token=RatisRocksDBBatchesTest.prepare(f.batches);var empty=new it.cavallium.rockserver.core.common.KVBatch.KVBatchRef(List.of(),List.of());byte[] digest=RemoteMutationInputs.batch(71,false,empty);
            for(int ordinal=0;ordinal<4200;ordinal++)f.batches.append(token,ordinal,empty,digest,it.cavallium.rockserver.core.common.RequestContext.batch()).get(10,java.util.concurrent.TimeUnit.SECONDS);
            assertEquals(4200,f.batches.progress().copiedBatches());assertEquals(0,f.batches.owner().nextRequestSequence());assertTrue(f.writes.isEmpty());f.batches.close();f.session.close();
            try(var session=RatisDurableSessionClient.reopen(f.window,f.routed);var batches=new RatisRocksDBBatches(f.f.schema,f.f.directory,f.routed,session,f.f.scheduler,f.journal,RatisRocksDBBatches.SessionOwnership.BORROWED)){
                var expired=assertThrows(java.util.concurrent.ExecutionException.class,()->batches.append(token,0,empty,digest,it.cavallium.rockserver.core.common.RequestContext.batch()).get(10,java.util.concurrent.TimeUnit.SECONDS));assertInstanceOf(RatisBulkInputExpiredException.class,expired.getCause());assertEquals(RemoteMutationStatus.State.PENDING,batches.status(token).state());
                assertEquals(4200,batches.append(token,4199,empty,digest,it.cavallium.rockserver.core.common.RequestContext.batch()).get(10,java.util.concurrent.TimeUnit.SECONDS).copiedBatches());
                assertEquals(4201,batches.append(token,4200,empty,digest,it.cavallium.rockserver.core.common.RequestContext.batch()).get(10,java.util.concurrent.TimeUnit.SECONDS).copiedBatches());batches.finishInput(token,it.cavallium.rockserver.core.common.RequestContext.batch()).get(10,java.util.concurrent.TimeUnit.SECONDS);batches.acknowledgeResult(token,batches.status(token).resultDigest());assertEquals(1,batches.owner().nextRequestSequence());
            }
        }
    }
    @Test void coldDecoderRejectsNoncanonicalRetirementFloor()throws Exception {
        var schema=new RatisNodeServiceTest().schema();try(var env=new RatisNodeRuntimeTest.Env(root,false,schema.digest())){
            var directory=env.routed.directory();var token=new RemoteMutationToken(directory.clusterId(),directory.databaseId(),directory.domainId(),UUID.randomUUID(),UUID.randomUUID(),1,0,RemoteMutationToken.Kind.BULK,directory.schemaDigest(),directory.payloadDigest(),RemoteMutationInputs.bulkHeader(71,false));var file=root.resolve("bad-floor");
            try(var journal=new RatisBatchRequestJournal(file,new byte[32])){journal.begin(token,71,false,directory);journal.remoteInput(0,new byte[32],List.of());}
            byte[] body=SessionJournalFiles.read(file,RatisBatchRequestJournal.MAX_BYTES);java.nio.ByteBuffer.wrap(body).putLong(57+RangeDirectoryCodec.encode(directory).length,0);SessionJournalFiles.write(file,body,RatisBatchRequestJournal.MAX_BYTES);assertThrows(java.io.IOException.class,()->new RatisBatchRequestJournal(file,new byte[32]));
        }
    }
    @Test void forcedWriteFailureFencesInMemoryFloorAndColdReopenUsesOriginalAfterimage()throws Exception {
        var schema=new RatisNodeServiceTest().schema();var directory=new RatisNodeRuntimeTest.Env(root,false,schema.digest());
        try(directory){var source=directory.routed.directory();var token=new RemoteMutationToken(source.clusterId(),source.databaseId(),source.domainId(),UUID.randomUUID(),UUID.randomUUID(),1,0,RemoteMutationToken.Kind.BULK,source.schemaDigest(),source.payloadDigest(),RemoteMutationInputs.bulkHeader(71,false));var path=root.resolve("input-journal");byte[] binding=new byte[32],digest=new byte[32];
            try(var journal=new RatisBatchRequestJournal(path,binding)){journal.begin(token,71,false,source);for(int n=0;n<256;n++)journal.remoteInput(n,digest,List.of());assertEquals(-1,journal.request().retiredInputThrough);Files.createDirectory(path.resolveSibling(path.getFileName()+".pending"));assertThrows(java.io.IOException.class,()->journal.remoteInput(256,digest,List.of()));assertThrows(java.io.IOException.class,journal::request);}
            assertFalse(Files.exists(path.resolveSibling(path.getFileName()+".pending")));try(var journal=new RatisBatchRequestJournal(path,binding)){assertEquals(256,journal.request().copiedBatches);assertEquals(-1,journal.request().retiredInputThrough);assertEquals(256,journal.request().inputDigests.size());journal.remoteInput(256,digest,List.of());assertEquals(0,journal.request().retiredInputThrough);assertEquals(256,journal.request().inputDigests.size());assertThrows(RatisBulkInputExpiredException.class,()->journal.retainedInput(0,digest));}
        }
    }
}
