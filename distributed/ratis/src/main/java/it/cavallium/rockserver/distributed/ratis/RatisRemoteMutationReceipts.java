package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;

/** One authoritative forced retirement floor with bounded recent metadata. */
final class RatisRemoteMutationReceipts {
    private static final int MAXIMUM=256,MAX_BYTES=2*1024*1024,AUTHORITY_BYTES=32;
    private final RatisJournalAccess access;private final Path path; private final RemoteMutationOwner authority; private boolean fenced;
    private long retiredThrough=-1;private RemoteMutationToken outstanding;private Runnable afterForce;
    private final NavigableMap<Long,RemoteMutationStatus> receipts=new TreeMap<>();
    RatisRemoteMutationReceipts(Path path,RemoteMutationOwner authority,boolean used,RatisJournalAccess access) throws IOException {
        this.access=Objects.requireNonNull(access);access.register(path,MAX_BYTES);
        this.path=path;this.authority=authority;
        if(Files.exists(path)) {
            try {var in=ByteBuffer.wrap(access.read(path,MAX_BYTES));byte[] binding=new byte[AUTHORITY_BYTES];in.get(binding);if(!Arrays.equals(binding,binding()))throw new IOException("Retirement floor owner changed");retiredThrough=in.getLong();int present=in.get()&255;if(present>1)throw new IOException("Invalid outstanding slot flag");if(present==1){outstanding=RemoteMutationToken.read(in);requireAuthority(outstanding);if(outstanding.requestSequence()!=Math.incrementExact(retiredThrough))throw new IOException("Outstanding slot skipped retirement floor");}int count=in.getInt();if(retiredThrough< -1 || count<0 || count>MAXIMUM)throw new IOException("Invalid retirement floor/count");
                for(int n=0;n<count;n++){var token=RemoteMutationToken.read(in);requireAuthority(token);long applied=in.getLong(),copied=in.getLong();int outcomeId=in.get()&255;if(outcomeId>=CommandResult.Status.values().length)throw new IOException("Invalid retained outcome");var outcome=CommandResult.Status.values()[outcomeId];byte[] digest=new byte[32];in.get(digest);if(token.requestSequence()>retiredThrough || applied<0 || copied<0 || receipts.put(token.requestSequence(),new RemoteMutationStatus(token,RemoteMutationStatus.State.ACKNOWLEDGED,applied,copied,0,outcome,digest))!=null)throw new IOException("Invalid released sequence metadata");}
                if(in.hasRemaining() || retiredThrough>=0 && (receipts.isEmpty() || receipts.lastKey()!=retiredThrough))throw new IOException("Incomplete retirement metadata");SessionJournalFiles.force(path);SessionJournalFiles.force(path.getParent());
            }catch(RuntimeException malformed){throw new IOException("Invalid retirement floor",malformed);}
        } else {if(used)throw new IOException("Used mutation owner lost its retirement floor");persist();}
    }
    /** Checksum-inclusive next bind/ACK image; one request can add at most one retained receipt. */
    synchronized long futureBytes(RemoteMutationToken token)throws IOException {
        requireOpen();requireAuthority(token);long bytes=token.encodedSize();int count=receipts.size();
        return 77+Math.max(bytes+count*(bytes+49),Math.min(MAXIMUM,count+1)*(bytes+49));
    }
    synchronized long retainedBytesAfterNext(RemoteMutationToken token)throws IOException {
        return 4096+4*futureBytes(token)+1024L*Math.min(MAXIMUM,receipts.size()+1);
    }
    private byte[] binding(){return RemoteMutationToken.digest(new RemoteMutationToken(authority.clusterId(),authority.databaseId(),authority.ownerId(),authority.ownerId(),authority.ownerEpoch(),0,authority.kind(),authority.domains(),new byte[32]).encode());}
    private void requireOpen() throws IOException {if(fenced)throw new IOException("Retirement force uncertain; reopen original owner");}
    private void requireAuthority(RemoteMutationToken token) throws IOException {var descriptor=new RemoteMutationOwner(token.clusterId(),token.databaseId(),token.ownerId(),token.ownerEpoch(),0,token.kind(),token.domains());if(!authority.sameAuthority(descriptor))throw new IOException("Mutation owner authority changed");}
    synchronized long nextSequence() throws IOException {requireOpen();return Math.incrementExact(retiredThrough);}
    synchronized boolean established() throws IOException {requireOpen();return retiredThrough>=0;}
    synchronized void validateJournal(RemoteMutationToken token) throws IOException {requireOpen();if(token==null){if(outstanding!=null)throw new IOException("Outstanding request lost original full input");return;}requireAuthority(token);if(token.requestSequence()<=retiredThrough){if(outstanding!=null || token.requestSequence()!=retiredThrough)throw new IOException("Input journal differs from authoritative slot/floor");return;}bind(token);}
    synchronized void bind(RemoteMutationToken token) throws IOException {requireNew(token);if(outstanding!=null)return;outstanding=token;persist();}
    synchronized boolean retired(RemoteMutationToken token) throws IOException {requireOpen();requireAuthority(token);return token.requestSequence()<=retiredThrough;}
    synchronized RemoteMutationStatus find(RemoteMutationToken token) throws IOException {
        requireOpen();requireAuthority(token);if(token.requestSequence()>retiredThrough)return null;
        var result=receipts.get(token.requestSequence());if(result==null)return new RemoteMutationStatus(token,RemoteMutationStatus.State.EXPIRED,-1,-1,0,null,null);
        if(!result.token().equals(token))throw new IOException("Retired sequence owner, UUID or input changed");return result;
    }
    synchronized void requireNew(RemoteMutationToken token) throws IOException {requireOpen();requireAuthority(token);if(token.requestSequence()<=retiredThrough)throw new IOException("Request sequence expired or acknowledged; never allocate a fresh sequence for its UUID");if(token.requestSequence()!=nextSequence())throw new IOException("Request sequence hole or stale owner discovery");if(outstanding!=null && !outstanding.equals(token))throw new IOException("Outstanding sequence UUID or input changed");}
    synchronized void acknowledge(RemoteMutationStatus status) throws IOException {
        requireOpen();requireAuthority(status.token());var previous=find(status.token());if(previous!=null){if(!previous.progressKnown() || !Arrays.equals(previous.resultDigest(),status.resultDigest()) || previous.outcome()!=status.outcome())throw new IOException("Expired or changed result ACK");return;}
        requireNew(status.token());if(outstanding==null || !outstanding.equals(status.token()))throw new IOException("ACK lacks original issued slot");if(!status.progressKnown() || status.resultDigest()==null || status.resultDigest().length!=32)throw new IOException("ACK lacks exact terminal metadata");
        retiredThrough=status.token().requestSequence();outstanding=null;receipts.put(retiredThrough,status);while(receipts.size()>MAXIMUM)receipts.pollFirstEntry();persist();
    }
    synchronized void afterForce(Runnable hook){afterForce=hook;}
    private void persist() throws IOException {var out=ByteBuffer.allocate(AUTHORITY_BYTES+13+(outstanding==null?0:outstanding.encodedSize())+receipts.values().stream().mapToInt(value->value.token().encodedSize()+49).sum()).put(binding()).putLong(retiredThrough).put((byte)(outstanding==null?0:1));if(outstanding!=null)out.put(outstanding.encode());out.putInt(receipts.size());for(var value:receipts.values())out.put(value.token().encode()).putLong(value.appliedMutations()).putLong(value.copiedBatches()).put((byte)value.outcome().ordinal()).put(value.resultDigest());try{access.write(path,out.array(),MAX_BYTES);if(afterForce!=null)afterForce.run();}catch(IOException | RuntimeException failure){fenced=true;throw failure;}}
}
