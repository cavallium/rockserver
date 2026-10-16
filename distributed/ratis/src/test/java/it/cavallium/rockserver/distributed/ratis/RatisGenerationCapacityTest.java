package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.distributed.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Forced manifest/credit recovery: aggregate actors never fulfill singleton schema-publication credit. */
class RatisGenerationCapacityTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final UUID cluster=new UUID(1,1),database=new UUID(1,2);
    DistributedSchema schema(int domain,long epoch,int column){return new DistributedSchema(cluster,database,new UUID(2,domain),epoch,0,List.of(new DistributedSchema.Column(column,new UUID(3,column),"column-"+column,ColumnSchema.of(IntArrayList.of(1),new ObjectArrayList<>(),true),null)));}
    RangeDirectory directory(DistributedSchema schema,long generation){var replica=new ReplicaDescriptor(new UUID(4,1),new UUID(4,2),new UUID(4,3),"capacity","localhost:12345",ReplicaDescriptor.Role.VOTER);var group=new UUID(5,schema.domainId().getLeastSignificantBits());return new RangeDirectory(cluster,database,schema.domainId(),schema.schemaEpoch(),schema.digest(),generation,schema.columnIds(),0,List.of(new RangeDescriptor(group,group,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(replica))));}
    RatisRoutedClient client(RangeDirectory directory){return new RatisRoutedClient(directory,new RatisRoutedClient.Transport(){public byte[] read(RangeDescriptor range,UUID column,byte[] key)throws IOException{throw new IOException("Capacity fixture must not read DATA");}public CommandResult command(RangeDescriptor range,DistributedCommand command)throws IOException{throw new IOException("Capacity fixture must not write DATA");}});}
    Path force(Path bindings,RatisTransactionBindings vector)throws IOException{Path folder=Files.createDirectory(bindings.resolve(RatisGenerationRegistry.key(vector)));SessionJournalFiles.write(folder.resolve("binding"),RatisGenerationRegistry.encode(vector),16*1024*1024);SessionJournalFiles.force(folder.resolve("binding"));SessionJournalFiles.force(folder);SessionJournalFiles.force(bindings);return folder;}
    int slots(Path ledger)throws IOException{return ByteBuffer.wrap(SessionJournalFiles.read(ledger,4096)).getInt();}
    @Test void pendingSingletonCreditCannotBeBorrowedOrFulfilledByAnAggregateMember()throws Exception{
        var oldA=schema(1,1,71);var oldB=schema(2,1,72);var newA=schema(1,2,71);var first=directory(oldA,1);var second=directory(oldB,1);var target=directory(newA,2);
        try(var a=client(first);var b=client(second);var next=client(target)){
            Path bindings=Files.createDirectory(root.resolve("generations")),requests=Files.createDirectory(root.resolve("requests")),ledger=root.resolve("capacity");
            force(bindings,RatisTransactionBindings.of(oldA,first,a));force(bindings,RatisTransactionBindings.of(oldB,second,b));
            var capacity=new RatisGenerationRegistry.Capacity(ledger,bindings,requests,3,false,RatisJournalAccess.standalone());UUID id=new UUID(6,1);var fields=new HashMap<String,byte[]>();
            fields.put("requestId",ByteBuffer.allocate(16).putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()).array());fields.put("kind","CREATE".getBytes(StandardCharsets.UTF_8));fields.put("changed",ByteBuffer.allocate(8).putLong(1).array());fields.put("aborted",new byte[8]);fields.put("phase","CLEANUP".getBytes(StandardCharsets.UTF_8));fields.put("sourceSchema",DistributedSchemaCodec.encode(oldA));fields.put("sourceDirectory",RangeDirectoryCodec.encode(first));fields.put("targetSchema",DistributedSchemaCodec.encode(newA));
            capacity.checkpoint(requests.resolve(id+".request"),fields,RatisSchemaClientJournal::write);assertEquals(1,slots(ledger));
            var aggregate=new RatisTransactionBindings(List.of(new RatisTransactionBindings.Domain(newA,target,next),new RatisTransactionBindings.Domain(oldB,second,b)));
            assertThrows(IOException.class,()->capacity.admitBinding(aggregate));assertEquals(1,slots(ledger));assertFalse(Files.exists(bindings.resolve(RatisGenerationRegistry.key(aggregate))));
            // A forced aggregate manifest cannot consume the reservation during cold reconciliation either.
            Path aggregateFolder=force(bindings,aggregate);assertThrows(IOException.class,()->new RatisGenerationRegistry.Capacity(ledger,bindings,requests,3,true,RatisJournalAccess.standalone()));assertEquals(1,slots(ledger));
            Files.delete(aggregateFolder.resolve("binding"));Files.delete(aggregateFolder);SessionJournalFiles.force(bindings);
            var singleton=RatisTransactionBindings.of(newA,target,next);assertDoesNotThrow(()->capacity.admitBinding(singleton));force(bindings,singleton);capacity.bound(newA);assertEquals(0,slots(ledger));
            assertDoesNotThrow(()->new RatisGenerationRegistry.Capacity(ledger,bindings,requests,3,true,RatisJournalAccess.standalone()));assertEquals(0,slots(ledger));
        }
    }
}
