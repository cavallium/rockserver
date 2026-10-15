package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.common.export.*;
import it.cavallium.rockserver.core.server.ExportGrpcService;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisAtomicClientTest.*;

/** Native logical migration helpers shared by aggregate export acceptance. */
class RatisLogicalExportMigrationTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static <T>T await(CompletableFuture<T> value)throws Exception{return value.get(20,TimeUnit.SECONDS);}
    enum Applied { COMMITTED, SKIPPED, CONFLICT }
    @FunctionalInterface interface BeforeCommit {void run()throws Exception;}
    static Buf fence(long epoch,UUID owner){return Buf.wrap(ByteBuffer.allocate(24).putLong(epoch).putLong(owner.getMostSignificantBits()).putLong(owner.getLeastSignificantBits()).array());}
    static Applied migrate(RocksDBAsyncAPI api,ExportJobs.Chunk chunk,List<KV> rows,Buf expectedFence,Keys checkpoint,BeforeCommit competing)throws Exception {
        long transaction=await(api.openTransactionAsync(RatisRocksDBAdapterTest.scope(71,72,73,74), Duration.ofSeconds(30)));boolean closed=false;
        try {
            var guard=await(api.getAsync(transaction,71,key(1),RequestType.<Buf>forUpdate()));assertEquals(transaction,guard.updateId());assertArrayEquals(expectedFence.toByteArray(),guard.previous().toByteArray(),"Migration epoch/owner must match the trusted lease");
            byte[] exact=ExportWire.id(chunk.id()).toByteArray();var saved=await(api.getAsync(transaction,73,checkpoint,RequestType.<Buf>current()));
            if(saved!=null){assertArrayEquals(exact,saved.toByteArray());assertTrue(await(api.closeTransactionAsync(transaction,false)));closed=true;return Applied.SKIPPED;}
            if(!rows.isEmpty())await(api.putMultiAsync(transaction,74,rows.stream().map(KV::keys).toList(),rows.stream().map(KV::value).toList(),RequestType.<Buf>none()));
            await(api.putAsync(transaction,73,checkpoint,Buf.wrap(exact),RequestType.<Buf>none()));competing.run();
            if(await(api.closeTransactionAsync(transaction,true))){closed=true;return Applied.COMMITTED;}
            assertTrue(await(api.closeTransactionAsync(transaction,false)));closed=true;return Applied.CONFLICT;
        }finally{if(!closed)await(api.closeTransactionAsync(transaction,false));}
    }
    static void put(RocksDBAsyncAPI api,long column,Keys key,Buf value)throws Exception {
        var scope=new TransactionOpenScope(List.of(TransactionOpenScope.Span.point(column,key)));
        long transaction=await(api.openTransactionAsync(scope,Duration.ofSeconds(30)));boolean closed=false;
        try {
            await(api.putAsync(transaction,column,key,value,RequestType.none()));
            assertTrue(await(api.closeTransactionAsync(transaction,true)));closed=true;
        }finally{if(!closed)await(api.closeTransactionAsync(transaction,false));}
    }
    static List<KV> decode(Path file,ExportJobs.Chunk chunk,DistributedSchema immutableSchema)throws Exception {
        assertEquals(74,chunk.columnId());assertArrayEquals(chunk.id().contentHash(),MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file)));var binding=immutableSchema.column(chunk.columnId());var shape=binding.schema();var rows=new ArrayList<KV>();long physicalRows=0;
        try(var options=new Options();var reader=new SstFileReader(options);var reads=new ReadOptions().setFillCache(false)){
            reader.open(file.toString());reader.verifyChecksum();var properties=reader.getTableProperties();assertEquals("leveldb.BytewiseComparator",properties.getComparatorName());assertEquals(0,properties.getNumDeletions());assertEquals(0,properties.getNumRangeDeletions());assertEquals(0,properties.getNumMergeOperands());assertEquals(chunk.rows(),properties.getNumEntries());
            try(var iterator=reader.newIterator(reads)){
                for(iterator.seekToFirst();iterator.isValid();iterator.next()){
                    var sizing=ByteBuffer.allocate(0);assertTrue(iterator.key(sizing)+(long)iterator.value(sizing)<=DistributedBucket.MAX_BYTES);byte[] physical=iterator.key(),value=iterator.value();assertTrue(chunk.lowerInclusive()==null || Arrays.compareUnsigned(physical,chunk.lowerInclusive())>=0);assertTrue(chunk.upperExclusive()==null || Arrays.compareUnsigned(physical,chunk.upperExclusive())<0);physicalRows++;
                    if(shape.variableLengthKeysCount()==0)rows.add(new KV(immutableSchema.decodeFixedKey(binding.columnId(),physical),Buf.wrap(value)));
                    else for(var member:immutableSchema.decodeBucket(binding.columnId(),physical,value).members()){
                        var parts=new Buf[shape.keysCount()];int offset=0;for(int n=0;n<shape.fixedLengthKeysCount();n++){int width=shape.key(n);parts[n]=Buf.wrap(Arrays.copyOfRange(physical,offset,offset+width));offset+=width;}for(int n=0;n<member.tails().size();n++)parts[shape.fixedLengthKeysCount()+n]=Buf.wrap(member.tails().get(n));rows.add(new KV(new Keys(parts),Buf.wrap(member.value())));
                    }
                    assertTrue(rows.size()<=RangeBudget.DEFAULT_MAX_ITEMS,"Never publish a partial collision bucket");
                }
                iterator.status();
            }
        }
        assertEquals(chunk.rows(),physicalRows);return List.copyOf(rows);
    }
    Path download(ExportGrpcService service,ExportJobs.Chunk chunk)throws Exception {
        Path file=root.resolve("logical-"+chunk.id().groupId()+"-"+chunk.id().ordinal()+".sst");long offset=0;
        try(var output=FileChannel.open(file,StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE)){
            while(true){var request=new ExportJobs.ReadRequest(chunk.id(),offset,ExportJobs.MIN_PAGE_BYTES);var page=ExportWire.file(service.readExport(RatisPublicExportWorkflowTest.read(chunk.id(),offset)).block(RatisPublicExportWorkflowTest.WAIT),request);var bytes=ByteBuffer.wrap(page.bytes());while(bytes.hasRemaining())output.write(bytes);offset+=page.bytes().length;if(!page.hasMore())break;assertTrue(page.bytes().length>0);}output.force(true);
        }
        assertEquals(chunk.fileBytes(),offset);return file;
    }
}
