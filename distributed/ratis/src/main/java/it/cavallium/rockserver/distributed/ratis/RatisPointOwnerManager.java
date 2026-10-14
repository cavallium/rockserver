package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;

/** Fixed available point slots and exact retained owner lookup; immutable roots never change identity on retry. */
final class RatisPointOwnerManager implements AutoCloseable {
    private final List<RatisRocksDBDirect> actors=new ArrayList<>();private final List<RemoteMutationOwner> identities=new ArrayList<>();private final List<RatisDurableSessionClient> sessions=new ArrayList<>();private int closed;
    RatisPointOwnerManager(DistributedSchema schema,RangeDirectory directory,RatisRoutedClient routed,RWScheduler scheduler,Path root,RatisRequestBudget budget,RatisRocksDBConnection.SessionFactory factory,RatisSessionEpochs epochs,RatisCatalogue catalogue,RatisRoutedClient.SourceAckDirectoryReader acknowledgementDirectories,RatisTransactionStatuses homes)throws IOException {
        Files.createDirectories(root);var binding=RemoteMutationToken.digest(ByteBuffer.allocate(64).put(schema.digest()).put(directory.payloadDigest()).array());Path manifest=root.resolve("owners");byte[] existing=Files.exists(manifest,LinkOption.NOFOLLOW_LINKS)?SessionJournalFiles.read(manifest,4096):null;
        if(existing==null && Files.exists(root.resolve("slot-0"),LinkOption.NOFOLLOW_LINKS))throw new IOException("Used point owner manifest missing");
        var grantIds=new HashSet<OperationId>();try {
            for(int slot=0;slot<budget.limits().pointSlots();slot++) {
                Path folder=Files.createDirectories(root.resolve("slot-"+slot)),sessionRoot=folder.resolve("session"),journal=folder.resolve("input");
                long opening=RatisRequestBudget.files(journal,journal.resolveSibling("input.released"),sessionRoot.resolve("window.bin"));var credit=budget.owner(RatisSessionClient.directoryCharge(directory)+4L*DistributedSchemaCodec.encodedSize(schema),opening);RatisDurableSessionClient session=null;boolean transferred=false;
                try{session=factory.open(Files.createDirectories(sessionRoot),schema,directory);var grant=session.grant();if(!grantIds.add(new OperationId(grant.sessionId(),grant.epoch(),0)))throw new IOException("Point slots share a grant/sequence owner");session.installReroutes(folder.resolve("reroutes"),schema,epochs,routed,homes,scheduler,catalogue,acknowledgementDirectories);var actor=new RatisRocksDBDirect(schema,routed,session,scheduler,journal,budget,credit);actors.add(actor);identities.add(actor.owner());sessions.add(session);transferred=true;}
                finally{if(!transferred){if(session!=null)session.close();credit.close();}}
            }
            var out=ByteBuffer.allocate(36+28*actors.size()).put(binding).putInt(actors.size());for(int slot=0;slot<actors.size();slot++){var owner=actors.get(slot).owner();out.putInt(slot).putLong(owner.ownerId().getMostSignificantBits()).putLong(owner.ownerId().getLeastSignificantBits()).putLong(owner.ownerEpoch());}if(existing!=null && !Arrays.equals(existing,out.array()))throw new IOException("Retained point slot identities/configuration changed");SessionJournalFiles.write(manifest,out.array(),4096);
        }catch(Throwable failure){try{close();}catch(Exception cleanup){failure.addSuppressed(cleanup);}if(failure instanceof IOException io)throw io;throw new IOException("Point owner manager open failed",failure);}
    }
    List<RemoteMutationOwner> available(){var result=new ArrayList<RemoteMutationOwner>();for(var actor:actors)try{if(actor.activeRequests()==0 && actor.recoveryState()==RatisRocksDBDirect.RecoveryState.NONE)result.add(actor.owner());}catch(IOException unavailable){/* A fenced authority remains retained for exact-token resolution; healthy slots remain discoverable. */}return List.copyOf(result);}
    RatisRocksDBDirect select(RemoteMutationOwner expected)throws IOException{for(int index=0;index<identities.size();index++)if(identities.get(index).sameAuthority(expected))return actors.get(index);throw new IOException("Original point owner is not retained in this generation");}
    RatisRocksDBDirect select(RemoteMutationToken token)throws IOException{return select(new RemoteMutationOwner(token.clusterId(),token.databaseId(),token.domainId(),token.ownerId(),token.ownerEpoch(),token.requestSequence(),token.kind(),token.schemaDigest(),token.directoryDigest()));}
    List<RatisRocksDBDirect> all(){return List.copyOf(actors);}
    RatisRocksDBDirect reader(){return actors.getFirst();}
    @Override public synchronized void close()throws IOException{while(closed<actors.size()){actors.get(closed).close();sessions.get(closed).close();closed++;}}
}
