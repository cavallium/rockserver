package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.io.IOException;
import java.nio.file.Path;

/** Explicit fixture owner credits; cold reopen reuses the same backend envelope. */
final class RatisPointFixtureBudget implements AutoCloseable {
    final RatisRequestBudget shared=new RatisRequestBudget(new RemoteMutationOwnerLimits(1,32,8,8,16,128L*1024*1024,512L<<20,512,16L*1024*1024,8L*1024*1024,1));
    RatisRocksDBDirect open(DistributedSchema schema,RatisRoutedClient routed,RatisDurableSessionClient session,RWScheduler scheduler,Path input,Path window)throws IOException {
        var credit=shared.owner(RemoteMutationToken.Kind.POINT,RatisSessionClient.directoryCharge(routed.directory(schema))+4*DistributedSchemaCodec.encodedSize(schema),RatisRequestBudget.files(input,input.resolveSibling(input.getFileName()+".released"),window));
        try{return new RatisRocksDBDirect(schema,routed.directory(schema),routed,session,scheduler,input,shared,credit);}catch(Throwable failure){credit.close();throw failure;}
    }
    @Override public void close()throws IOException{shared.close();}
}
