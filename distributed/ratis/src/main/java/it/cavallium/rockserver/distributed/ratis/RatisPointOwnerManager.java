package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

/** Active slot selectors and bounded immutable owner roots. Pending renewal never retargets or allocates a replacement identity. */
final class RatisPointOwnerManager implements AutoCloseable {
    private static final int MAX_MANIFEST=16384,ENTRY_BYTES=101,INTENT_HEADER=64;
    private final DistributedSchema schema;private final RangeDirectory initialDirectory;private final RatisRoutedClient routed;private final RWScheduler scheduler;
    private final Path root;private final byte[] binding;private final RatisRequestBudget budget;private final RatisRocksDBConnection.SessionFactory factory;
    private final RatisSessionEpochs epochs;private final RatisCatalogue catalogue;private final RatisRoutedClient.SourceAckDirectoryReader acknowledgements;private final RatisTransactionStatuses homes;
    private final List<Entry> entries=new ArrayList<>();private final int[] selected;private boolean busy,fenced,closed;private Runnable selectorHook;
    private static final class Entry {
        final UUID root,previous;final int slot;final long reservation;final byte[] planDigest;UUID owner;long epoch=-1;
        RatisSessionBindingPlan plan;RatisRoutedClient.OwnerDirectoryLease qualification;RatisRequestBudget.Owner credit;RatisRocksDBDirect actor;RatisDurableSessionClient session;boolean released;
        Entry(UUID root,UUID previous,int slot,long reservation,byte[] planDigest){this.root=root;this.previous=previous;this.slot=slot;this.reservation=reservation;this.planDigest=planDigest.clone();}
    }
    RatisPointOwnerManager(DistributedSchema schema,RangeDirectory directory,RatisRoutedClient routed,RWScheduler scheduler,Path root,RatisRequestBudget budget,RatisRocksDBConnection.SessionFactory factory,RatisSessionEpochs epochs,RatisCatalogue catalogue,RatisRoutedClient.SourceAckDirectoryReader acknowledgements,RatisTransactionStatuses homes)throws IOException {
        this.schema=schema;initialDirectory=directory;this.routed=routed;this.scheduler=scheduler;this.root=root;this.budget=budget;this.factory=factory;this.epochs=epochs;this.catalogue=catalogue;this.acknowledgements=acknowledgements;this.homes=homes;
        binding=RemoteMutationToken.digest(ByteBuffer.allocate(96).put(schema.digest()).put(directory.payloadDigest()).put(homes.identityDigest()).array());selected=new int[budget.limits().pointSlots()];Arrays.fill(selected,-1);
        boolean used=Files.exists(root,LinkOption.NOFOLLOW_LINKS);Files.createDirectories(root);
        try {
            Path manifest=root.resolve("owners");if(Files.exists(manifest,LinkOption.NOFOLLOW_LINKS))decode(budget.read(manifest,MAX_MANIFEST));else{if(used)throw new IOException("Used point owner manifest missing");persist();}
            recoverUnpublishedIntents();
            for(var entry:List.copyOf(entries))open(entry);
            for(int slot=0;slot<selected.length;slot++)if(selected[slot]<0)start(slot,initialDirectory);
            for(var entry:List.copyOf(entries))if(selected[entry.slot]!=entries.indexOf(entry) && !pending(entry))entry.actor.historical();
        }catch(Throwable failure){try{close();}catch(Exception cleanup){failure.addSuppressed(cleanup);}if(failure instanceof IOException io)throw io;throw new IOException("Point owner manager open failed",failure);}
    }
    private Path folder(Entry entry){return root.resolve(entry.root.toString());}
    private Entry predecessor(Entry entry)throws IOException{if(entry.previous==null)return null;return entries.stream().filter(old->old.root.equals(entry.previous)).findFirst().orElseThrow(()->new IOException("Original renewal predecessor missing"));}
    private synchronized boolean pending(Entry entry){return selected[entry.slot]<0 || !entries.get(selected[entry.slot]).root.equals(entry.root) && (entry.previous==null?selected[entry.slot]<0:entries.get(selected[entry.slot]).root.equals(entry.previous));}
    private void reserve(Entry entry)throws IOException {
        if(entry.credit!=null)return;Path directory=folder(entry),input=directory.resolve("input");long files=RatisRequestBudget.files(input,directory.resolve("input.released"),directory.resolve("session/window.bin"));
        entry.credit=budget.owner(RemoteMutationToken.Kind.POINT,4*DistributedSchemaCodec.encodedSize(schema)+entry.reservation,files);
    }
    private void start(int slot,RangeDirectory data)throws IOException {
        Entry old; synchronized(this){old=selected[slot]<0?null:entries.get(selected[slot]);}
        if(old!=null)old.actor.beginRenewal();synchronized(this){if(entries.size()>=budget.limits().maximumPointOwners())throw new IOException("Retained point owner history capacity exhausted");}Entry entry=null;boolean intent=false,created=false;
        try {
            schema.requireMatches(data);var input=factory.bindings(data);long reservation=input.reservation();var credit=budget.owner(RemoteMutationToken.Kind.POINT,4*DistributedSchemaCodec.encodedSize(schema)+reservation,0);
            try{var plan=input.freeze();entry=new Entry(UUID.randomUUID(),old==null?null:old.root,slot,reservation,plan.digest());entry.plan=plan;entry.credit=credit;}catch(Throwable failure){credit.close();throw failure;}
            preflight(entry);Files.createDirectory(folder(entry));created=true;writeIntent(entry);intent=true;synchronized(this){entries.add(entry);}persist();open(entry);
        }catch(IOException|RuntimeException failure){if(!intent){if(created){synchronized(this){entries.add(entry);fenced=true;}}else{if(entry!=null && entry.credit!=null)entry.credit.close();}}throw failure;}
    }
    private void open(Entry entry)throws IOException {
        if(entry.actor!=null){if(pending(entry))publish(entry);return;}reserve(entry);
        if(entry.plan==null){long size=Files.size(folder(entry).resolve("intent"));if(size<INTENT_HEADER+32 || entry.reservation<8*(size-INTENT_HEADER-32))throw new IOException("Retained intent geometry exceeds reserved credit");byte[] body=budget.read(folder(entry).resolve("intent"),INTENT_HEADER+RatisSessionBindingPlan.MAX_BYTES+32);var decoded=decodeIntent(entry.root,body);if(decoded.slot!=entry.slot || !Objects.equals(decoded.previous,entry.previous) || decoded.reservation!=entry.reservation || !Arrays.equals(decoded.planDigest,entry.planDigest))throw new IOException("Immutable point intent differs from selector manifest");entry.plan=decoded.plan;}
        if(entry.qualification==null)entry.qualification=routed.retainOwnerDirectory(entry.plan);
        var old=predecessor(entry);if(pending(entry) && old!=null)old.actor.beginRenewal();
        if(pending(entry) && routed.directory(schema).generation()<entry.plan.directory().generation())routed.refresh(entry.plan.directory());
        preflight(entry);var session=factory.open(Files.createDirectories(folder(entry).resolve("session")),schema,entry.plan);boolean transferred=false;
        try {
            var grant=session.grant();synchronized(this){for(var other:entries)if(other!=entry && other.owner!=null && other.owner.equals(grant.sessionId()) && other.epoch==grant.epoch())throw new IOException("Immutable point roots share a native grant");}
            if(entry.owner!=null && (!entry.owner.equals(grant.sessionId()) || entry.epoch!=grant.epoch()))throw new IOException("Native owner identity changed on reopen");
            session.installReroutes(folder(entry).resolve("reroutes"),schema,epochs,routed,homes,scheduler,catalogue,acknowledgements,budget);
            var actor=new RatisRocksDBDirect(schema,entry.plan.directory(),routed,session,scheduler,folder(entry).resolve("input"),budget,entry.credit);if(pending(entry))actor.beginRenewal();synchronized(this){entry.actor=actor;entry.session=session;entry.owner=grant.sessionId();entry.epoch=grant.epoch();}transferred=true;
            if(pending(entry))publish(entry);budget.fulfilled(paths(entry));
        }finally{if(!transferred)session.close();}
    }
    private List<RatisJournalAccess.Reservation> paths(Entry entry)throws IOException {
        Path directory=folder(entry);var paths=new ArrayList<>(RatisBackendSessions.reservations(directory.resolve("session"),entry.plan.targets(),epochs));
        paths.add(new RatisJournalAccess.Reservation(root.resolve("owners"),MAX_MANIFEST,2L*(40+4*selected.length+ENTRY_BYTES*Math.max(entries.size(),1)+32)));
        paths.add(new RatisJournalAccess.Reservation(directory.resolve("intent"),INTENT_HEADER+RatisSessionBindingPlan.MAX_BYTES+32,2L*(INTENT_HEADER+entry.plan.encode().length+32)));
        paths.add(new RatisJournalAccess.Reservation(directory.resolve("reroutes/coordinator.lock"),32,0));paths.add(new RatisJournalAccess.Reservation(directory.resolve("input"),RatisDirectRequestJournal.MAX_BYTES,0));paths.add(new RatisJournalAccess.Reservation(directory.resolve("input.lock"),32,0));paths.add(new RatisJournalAccess.Reservation(directory.resolve("input.released"),128*1024,2*(150+32)));return List.copyOf(paths);
    }
    private void preflight(Entry entry)throws IOException {budget.preflight(paths(entry));}
    private void publish(Entry entry)throws IOException {
        var old=predecessor(entry);int before;synchronized(this){before=selected[entry.slot];selected[entry.slot]=entries.indexOf(entry);}
        try{persist();if(selectorHook!=null)selectorHook.run();}catch(IOException|RuntimeException failure){synchronized(this){selected[entry.slot]=before;fenced=true;}throw failure;}
        if(old!=null)old.actor.historical();entry.actor.activateSelected();
    }
    CompletableFuture<List<RemoteMutationOwner>> discover(RequestContext context) {
        final long deadline;try{deadline=scheduler.resolveMonotonicDeadline(context);}catch(RuntimeException invalid){return CompletableFuture.failedFuture(invalid);}synchronized(this){if(closed || fenced || busy)return CompletableFuture.failedFuture(new IOException("Point owner discovery closed, uncertain or already active"));busy=true;}
        var actual=budget.recover(()->{
            check(deadline);var failures=new ArrayList<Throwable>();
            for(var entry:List.copyOf(entries))if(pending(entry))try{open(entry);}catch(IOException|RuntimeException failure){retainSlotFailure(deadline,failure,failures);}
            var authority=epochs.read();if(authority==null)throw new IOException("Native owner authority unavailable");
            for(int slot=0;slot<selected.length;slot++){
                Entry entry;boolean retainedIntent;synchronized(this){entry=entries.get(selected[slot]);int selectedSlot=slot;retainedIntent=entries.stream().anyMatch(candidate->candidate.slot==selectedSlot && pending(candidate));}
                if(retainedIntent)continue;
                try{if(entry.actor.renewalRequired(authority)){check(deadline);entry.actor.beginRenewal();var current=catalogue.read(schema.domainId());if(current==null)throw new IOException("Authoritative renewal placement unavailable");start(slot,current);}}catch(IOException|RuntimeException failure){retainSlotFailure(deadline,failure,failures);}
            }
            check(deadline);var owners=available();if(owners.isEmpty() && !failures.isEmpty()){var failure=new IOException("No usable point owner; original failed slot work remains retained",failures.getFirst());for(int index=1;index<failures.size();index++)failure.addSuppressed(failures.get(index));throw failure;}return owners;
        });
        actual.whenComplete((value,failure)->{synchronized(this){busy=false;}});return actual.copy();
    }
    private void retainSlotFailure(long deadline,Throwable failure,List<Throwable> failures)throws IOException {
        check(deadline);synchronized(this){if(fenced || closed)throw new IOException("Point manager selector ownership is uncertain",failure);}failures.add(failure);
    }
    private void check(long deadline)throws IOException{if(deadline!=Long.MAX_VALUE && scheduler.isMonotonicDeadlineExpired(deadline))throw new IOException("Point owner discovery deadline expired; original renewal intent retained");}
    synchronized List<RemoteMutationOwner> available(){var result=new ArrayList<RemoteMutationOwner>();if(fenced || closed)return List.of();for(int index:selected)if(index>=0){var actor=entries.get(index).actor;try{if(actor!=null && actor.acceptsPreparation() && actor.activeRequests()==0 && actor.recoveryState()==RatisRocksDBDirect.RecoveryState.NONE)result.add(actor.owner());}catch(IOException unavailable){/* Exact historical authority remains retained. */}}return List.copyOf(result);}
    synchronized boolean contains(RemoteMutationToken token){return entries.stream().anyMatch(entry->entry.owner!=null && entry.owner.equals(token.ownerId()) && entry.epoch==token.ownerEpoch() && entry.plan!=null && token.kind()==RemoteMutationToken.Kind.POINT && schema.clusterId().equals(token.clusterId()) && schema.databaseId().equals(token.databaseId()) && schema.domainId().equals(token.domainId()) && Arrays.equals(schema.digest(),token.schemaDigest()) && Arrays.equals(entry.plan.directory().payloadDigest(),token.directoryDigest()));}
    synchronized RatisRocksDBDirect select(RemoteMutationOwner owner)throws IOException{for(var entry:entries)if(entry.actor!=null && entry.owner.equals(owner.ownerId()) && entry.epoch==owner.ownerEpoch() && owner.sameAuthority(new RemoteMutationOwner(schema.clusterId(),schema.databaseId(),schema.domainId(),entry.owner,entry.epoch,0,RemoteMutationToken.Kind.POINT,schema.digest(),entry.plan.directory().payloadDigest())))return entry.actor;throw new IOException("Original point owner unavailable");}
    RatisRocksDBDirect select(RemoteMutationToken token)throws IOException{return select(new RemoteMutationOwner(token.clusterId(),token.databaseId(),token.domainId(),token.ownerId(),token.ownerEpoch(),token.requestSequence(),token.kind(),token.schemaDigest(),token.directoryDigest()));}
    synchronized List<RatisRocksDBDirect> all(){return entries.stream().map(entry->entry.actor).filter(Objects::nonNull).toList();}
    synchronized RatisRocksDBDirect reader(){return entries.get(selected[0]).actor;}
    void afterSelectorForce(Runnable hook){selectorHook=hook;}
    private void writeIntent(Entry entry)throws IOException {byte[] plan=entry.plan.encode();var out=ByteBuffer.allocate(INTENT_HEADER+plan.length).putInt(entry.slot);uuid(out,entry.previous);out.putLong(entry.reservation).put(entry.planDigest).putInt(plan.length).put(plan);budget.write(folder(entry).resolve("intent"),out.array(),INTENT_HEADER+RatisSessionBindingPlan.MAX_BYTES+32);}
    private Entry decodeIntent(UUID rootId,byte[] body)throws IOException {
        try{var in=ByteBuffer.wrap(body);int slot=in.getInt();UUID previous=uuid(in);long reservation=in.getLong();byte[] digest=new byte[32];in.get(digest);int length=in.getInt();if(slot<0 || slot>=selected.length || length!=in.remaining() || reservation<8L*length || reservation>budget.limits().maximumBytes())throw new IOException("Invalid bounded point intent");byte[] bytes=new byte[length];in.get(bytes);var entry=new Entry(rootId,previous,slot,reservation,digest);entry.plan=factory.restore(bytes);schema.requireMatches(entry.plan.directory());if(!Arrays.equals(entry.plan.digest(),digest) || entry.plan.metadataBytes()!=reservation)throw new IOException("Point intent binding/geometry changed");return entry;}catch(RuntimeException malformed){throw new IOException("Malformed point renewal intent",malformed);}
    }
    private void recoverUnpublishedIntents()throws IOException {
        int count=0;try(var files=Files.newDirectoryStream(root)){for(var path:files){if(path.getFileName().toString().equals("owners") || path.getFileName().toString().equals("owners.pending"))continue;if(++count>budget.limits().maximumPointOwners())throw new IOException("Point owner root count exceeded");UUID id;try{id=UUID.fromString(path.getFileName().toString());}catch(IllegalArgumentException foreign){throw new IOException("Foreign point owner root",foreign);}if(entries.stream().anyMatch(entry->entry.root.equals(id)))continue;if(!Files.isDirectory(path,LinkOption.NOFOLLOW_LINKS))throw new IOException("Foreign point owner root object");Path intent=path.resolve("intent");if(!Files.exists(intent,LinkOption.NOFOLLOW_LINKS)){try(var contents=Files.newDirectoryStream(path)){if(contents.iterator().hasNext())throw new IOException("Unpublished point root has no forced immutable intent");}Files.delete(path);SessionJournalFiles.force(root);continue;}long size=Files.size(intent);if(size<INTENT_HEADER || size>INTENT_HEADER+RatisSessionBindingPlan.MAX_BYTES+32)throw new IOException("Unpublished intent exceeds bound");var header=ByteBuffer.allocate(INTENT_HEADER);try(var file=Files.newByteChannel(intent,StandardOpenOption.READ,LinkOption.NOFOLLOW_LINKS)){while(header.hasRemaining())if(file.read(header)<0)throw new IOException("Truncated unpublished intent header");}header.flip();int slot=header.getInt();UUID previous=uuid(header);long reservation=header.getLong();byte[] digest=new byte[32];header.get(digest);int length=header.getInt();if(slot<0 || slot>=selected.length || length!=size-INTENT_HEADER-32 || reservation<8L*length || reservation>budget.limits().maximumBytes())throw new IOException("Unpublished intent geometry is invalid");var entry=new Entry(id,previous,slot,reservation,digest);if(!pending(entry))throw new IOException("Unpublished renewal no longer names current predecessor");reserve(entry);entries.add(entry);var decoded=decodeIntent(id,budget.read(intent,INTENT_HEADER+RatisSessionBindingPlan.MAX_BYTES+32));entry.plan=decoded.plan;persist();}}
    }
    private synchronized void persist()throws IOException {
        var out=ByteBuffer.allocate(40+4*selected.length+ENTRY_BYTES*entries.size()).put(binding).putInt(selected.length).putInt(entries.size());for(int value:selected)out.putInt(value);for(var entry:entries){uuid(out,entry.root);uuid(out,entry.previous);out.putInt(entry.slot).putLong(entry.reservation).put(entry.planDigest).put((byte)(entry.owner==null?0:1));uuid(out,entry.owner);out.putLong(entry.epoch);}try{budget.write(root.resolve("owners"),out.array(),MAX_MANIFEST);}catch(IOException|RuntimeException failure){fenced=true;throw failure;}
    }
    private void decode(byte[] body)throws IOException {
        try{var in=ByteBuffer.wrap(body);byte[] actual=new byte[32];in.get(actual);if(!Arrays.equals(binding,actual) || in.getInt()!=selected.length)throw new IOException("Point manager binding/configuration changed");int count=in.getInt();if(count<0 || count>budget.limits().maximumPointOwners())throw new IOException("Point history exceeds bound");for(int n=0;n<selected.length;n++){selected[n]=in.getInt();if(selected[n]<-1 || selected[n]>=count)throw new IOException("Invalid point slot selector");}var ids=new HashSet<UUID>();for(int n=0;n<count;n++){UUID id=uuid(in),previous=uuid(in);int slot=in.getInt();long reservation=in.getLong();byte[] digest=new byte[32];in.get(digest);int present=Byte.toUnsignedInt(in.get());UUID owner=uuid(in);long epoch=in.getLong();if(id==null || !ids.add(id) || previous!=null && !ids.contains(previous) || slot<0 || slot>=selected.length || reservation<1 || reservation>budget.limits().maximumBytes() || present>1 || (present==0)!=(owner==null) || (owner==null?epoch!=-1:epoch<0))throw new IOException("Invalid retained point owner record");var entry=new Entry(id,previous,slot,reservation,digest);entry.owner=owner;entry.epoch=epoch;entries.add(entry);}if(in.hasRemaining())throw new IOException("Trailing point selector bytes");for(int slot=0;slot<selected.length;slot++)if(selected[slot]>=0 && (entries.get(selected[slot]).slot!=slot || entries.get(selected[slot]).owner==null))throw new IOException("Selected point owner is incomplete");}catch(RuntimeException malformed){throw new IOException("Malformed point manager manifest",malformed);}
    }
    private static void uuid(ByteBuffer out,UUID id){out.putLong(id==null?0:id.getMostSignificantBits()).putLong(id==null?0:id.getLeastSignificantBits());}
    private static UUID uuid(ByteBuffer in){long high=in.getLong(),low=in.getLong();return high==0 && low==0?null:new UUID(high,low);}
    @Override public synchronized void close()throws IOException{if(closed)return;if(busy)throw new IOException("Actual point renewal remains owned");for(var entry:entries)if(!entry.released){if(entry.actor!=null)entry.actor.close();if(entry.session!=null)entry.session.close();if(entry.qualification!=null)entry.qualification.close();if(entry.credit!=null)entry.credit.close();entry.released=true;}closed=true;}
}
