package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.*;
import java.util.*;

/** Exhaustive fresh HOME reference qualification. This alone grants no source shutdown or GC authority. */
public final class RatisSourceReferenceClosure {
    private static final long RETAINED_BYTES=RatisNodePlan.MAX_HOME_GROUPS*(192L+65536)+NativeSourceClosing.MAX_BYTES+65536;
    @FunctionalInterface public interface Lease extends AutoCloseable {@Override void close();}
    @FunctionalInterface public interface Admission {Lease reserve(long bytes)throws IOException;}
    @FunctionalInterface public interface Reader {RatisHomeSourceReferencesRpc.Reply read(RatisHomeSourceReferencesRpc.Request request,Admission admission)throws IOException;}
    public static final class OutstandingReference extends IOException {
        private final UUID home,identity;private final boolean snapshot;
        OutstandingReference(UUID home,UUID identity,boolean snapshot){super("Unconsumed "+(snapshot?"snapshot":"transaction")+" reference "+identity+" remains on HOME "+home);this.home=home;this.identity=identity;this.snapshot=snapshot;}
        public UUID home(){return home;}public UUID identity(){return identity;}public boolean snapshot(){return snapshot;}
    }
    public record Witness(UUID home,long epoch,RatisNodePlan.Role role,byte[] closingDigest,ApplyPosition completedCut,ReplicatedRangeStore.Configuration configuration,byte[] scanDigest){
        public Witness{Objects.requireNonNull(home);Objects.requireNonNull(completedCut);Objects.requireNonNull(configuration);if(closingDigest.length!=32 || scanDigest.length!=32 || epoch<0 || role!=RatisNodePlan.Role.TRANSACTION_HOME && role!=RatisNodePlan.Role.SNAPSHOT_HOME)throw new IllegalArgumentException("Invalid HOME closure witness");closingDigest=closingDigest.clone();scanDigest=scanDigest.clone();}
        @Override public byte[] closingDigest(){return closingDigest.clone();}@Override public byte[] scanDigest(){return scanDigest.clone();}
    }
    /** Owned immutable evidence: retain until the actual caller finishes or journals its compact commitment. */
    public static final class Qualification implements AutoCloseable {
        private NativeSourceClosing source;private List<Witness> witnesses;private byte[] digest;private Lease memory;
        private Qualification(NativeSourceClosing source,List<Witness> witnesses,Lease memory){this.source=source;this.witnesses=List.copyOf(witnesses);this.memory=memory;var hash=hash();hash.update(source.encode());for(var witness:witnesses){hash.update(ByteBuffer.allocate(57).putLong(witness.home().getMostSignificantBits()).putLong(witness.home().getLeastSignificantBits()).putLong(witness.epoch()).put((byte)witness.role().ordinal()).putLong(witness.completedCut().term()).putLong(witness.completedCut().index()).putLong(witness.configuration().term()).putLong(witness.configuration().index()).array());hash.update(witness.closingDigest());byte[] cfg=witness.configuration().data();hash.update(ByteBuffer.allocate(4).putInt(cfg.length).array());hash.update(cfg);hash.update(witness.scanDigest());}digest=hash.digest();}
        private void requireOpen(){if(memory==null)throw new IllegalStateException("HOME reference qualification is released");}
        public synchronized NativeSourceClosing source(){requireOpen();return source;}public synchronized List<Witness> witnesses(){requireOpen();return witnesses;}public synchronized byte[] digest(){requireOpen();return digest.clone();}
        @Override public synchronized void close(){if(memory==null)return;memory.close();memory=null;source=null;witnesses=null;digest=null;}
    }
    private record Expected(RatisNodePlan.Role role,RangeDirectory directory,RangeDescriptor range){}
    public static Qualification scan(NativeSourceClosing source,Collection<RatisNodePlan.Home> configured,Collection<NativeHomeSourceClosing> receipts,Reader reader,Admission admission)throws IOException{
        Objects.requireNonNull(source);Objects.requireNonNull(configured);Objects.requireNonNull(receipts);Objects.requireNonNull(reader);Objects.requireNonNull(admission);
        Lease memory=Objects.requireNonNull(admission.reserve(RETAINED_BYTES));boolean returned=false;
        try{
            var expected=new TreeMap<UUID,Expected>(RangeDirectoryCodec.UUID_ORDER);var header=source.request().header();
            for(var home:configured){var directory=home.directory();if(home.role()!=RatisNodePlan.Role.TRANSACTION_HOME && home.role()!=RatisNodePlan.Role.SNAPSHOT_HOME || !directory.clusterId().equals(header.clusterId()) || !directory.databaseId().equals(header.databaseId()))throw new IOException("HOME closure namespace/role changed");for(var range:directory.ranges()){if(range.topologyState()!=RangeDescriptor.TopologyState.ACTIVE || expected.size()>=RatisNodePlan.MAX_HOME_GROUPS || expected.put(range.groupId(),new Expected(home.role(),directory,range))!=null)throw new IOException("HOME closure scope is duplicate/noncurrent/over bound");}}
            if(expected.isEmpty() || receipts.size()!=expected.size())throw new IOException("Source closure requires every configured transaction and snapshot HOME");
            var saved=new HashMap<UUID,NativeHomeSourceClosing>();for(var receipt:receipts){var r=receipt.request();var home=expected.get(r.home());if(home==null || r.homeEpoch()!=home.range().rangeEpoch() || !Arrays.equals(r.schema(),home.directory().schemaDigest()) || !r.source().equals(source) || saved.put(r.home(),receipt)!=null)throw new IOException("HOME closing receipt changed exact source/configured namespace");RatisRangeStateMachine.requireSchemaMembership(home.range(),r.configuration());}
            var witnesses=new ArrayList<Witness>(expected.size());
            for(var entry:expected.entrySet()){
                var receipt=saved.get(entry.getKey());byte[] cursor=null;var scan=hash();ApplyPosition cut=receipt.position();ReplicatedRangeStore.Configuration configuration=null;boolean exhausted=false;
                for(int pageNumber=0;pageNumber<=ReplicatedRangeStore.MAX_HOME_STATUS_ROWS;pageNumber++){
                    var request=new RatisHomeSourceReferencesRpc.Request(receipt,cursor,16);var reply=reader.read(request,admission);if(!Arrays.equals(reply.request().digest(),request.digest()))throw new IOException("HOME scan reader changed exact cursor/fence");var page=reply.page();if(!RatisRelocationOwnerRpc.covers(page.position(),cut))throw new IOException("HOME reference cut regressed");RatisRangeStateMachine.requireSchemaMembership(entry.getValue().range(),reply.configuration().data());
                    if(configuration!=null && (reply.configuration().index()<configuration.index() || reply.configuration().term()<configuration.term() || reply.configuration().index()==configuration.index() && !Arrays.equals(reply.configuration().data(),configuration.data())))throw new IOException("HOME reference configuration regressed/changed at same index");
                    if(page.transaction()!=null)throw new OutstandingReference(entry.getKey(),page.transaction().transactionId(),false);if(page.snapshot()!=null)throw new OutstandingReference(entry.getKey(),page.snapshot().fence().fenceId(),true);
                    scan.update(RatisHomeSourceReferencesRpc.replyBody(reply));cut=page.position();configuration=reply.configuration();if(!page.more()){exhausted=true;break;}cursor=page.next();
                }
                if(!exhausted)throw new IOException("HOME reference scan exceeded actual retained-row bound");
                witnesses.add(new Witness(entry.getKey(),receipt.request().homeEpoch(),entry.getValue().role(),OperatorUpload.sha256(receipt.encode()),cut,configuration,scan.digest()));
            }
            var result=new Qualification(source,witnesses,memory);returned=true;return result;
        }finally{if(!returned)memory.close();}
    }
    private static MessageDigest hash(){try{return MessageDigest.getInstance("SHA-256");}catch(NoSuchAlgorithmException impossible){throw new AssertionError(impossible);}}
    private RatisSourceReferenceClosure(){}
}
