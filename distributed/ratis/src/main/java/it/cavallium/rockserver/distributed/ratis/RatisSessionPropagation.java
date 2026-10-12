package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.*;
import java.util.*;

/** Durable, budgeted propagation over a frozen complete directory. No executor or borrowed client is owned. */
public final class RatisSessionPropagation implements AutoCloseable {
    private static final int MAGIC=0x52535052,MAX_TRANSITIONS=128,MAX_RETIRED=2*MAX_TRANSITIONS,MAX_ARCHIVE_BYTES=64*1024*1024,MAX_BYTES=64*1024;
    public record Participant(UUID group,long rangeEpoch) {}
    private record Round(long number,SessionAuthority target,long generation,byte[] directory,List<Participant> ranges,BitSet done,int cursor,long schemaEpoch,byte[] schemaDigest,RangeDirectory frozen,byte[] frozenBytes) {
        boolean complete() { return target!=null && done.cardinality()==ranges.size(); }
    }
    public record Progress(long round,SessionAuthority target,long directoryGeneration,String directoryDigest,List<Participant> participants,
                           int completed,int total,boolean complete,Map<UUID,String> failures) {
        public Progress { participants=List.copyOf(participants); failures=Map.copyOf(failures); }
    }
    private record Transition(TopologyChange.Definition definition,Map<UUID,NativeTopologyActivation> activations) {}
    private final Map<UUID,Transition> transitions=new LinkedHashMap<>();
    private final Map<Participant,RatisSessionMaintenance.ClockRoute> retired=new HashMap<>();
    private final Map<String,RangeDirectory> directorySnapshots=new LinkedHashMap<>();
    private final Path root, manifest,reconciliationManifest,topologyManifest;
    private RangeDirectory qualifiedDirectory;
    private final RatisSessionEpochs epochs;
    private final RatisRoutedClient routed;
    private final int maxRanges;
    private final byte[] identity;
    private final FileChannel lockChannel;
    private final FileLock lock;
    private Round round;
    private boolean busy,closed,fenced;
    private byte[] reconciliationDirectory;
    private BitSet reconciled=new BitSet();
    private int reconciliationCursor;

    /** Uses one canonical root for its manifest and dedicated clock journals; parent storage must already be durable. */
    public RatisSessionPropagation(Path root,RatisSessionEpochs epochs,RatisRoutedClient routed,int maxRanges) throws IOException {
        this.root=root.toAbsolutePath(); this.epochs=Objects.requireNonNull(epochs); this.routed=Objects.requireNonNull(routed);
        if (maxRanges<1 || maxRanges>RangeDirectoryCodec.MAX_RANGES || !Files.isDirectory(this.root)) throw new IllegalArgumentException("Invalid propagation root/range budget");
        this.maxRanges=maxRanges; identity=routed.maintenanceIdentityDigest(); manifest=this.root.resolve("propagation.bin");
        reconciliationManifest=this.root.resolve("schema-reconciliation.bin");topologyManifest=this.root.resolve("topology-routes.bin");qualifiedDirectory=routed.bootstrapDirectory();
        lockChannel=FileChannel.open(this.root.resolve("propagation.lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE);
        FileLock acquired=null;
        try {
            try { acquired=lockChannel.tryLock(); }
            catch (java.nio.channels.OverlappingFileLockException owned) { throw new IOException("Propagation root already owned",owned); }
            if (acquired==null) throw new IOException("Propagation root already owned"); lock=acquired;
            restoreTopology();if(!Files.exists(topologyManifest))persistTopology();
            if (Files.exists(manifest)) {
                round=decode(SessionJournalFiles.read(manifest,MAX_BYTES));
                SessionJournalFiles.force(manifest); SessionJournalFiles.force(this.root);
            } else {
                round=new Round(0,null,-1,null,List.of(),new BitSet(),0,-1,null,null,null); persist(round);
            }
            restoreReconciliation();
            restoreQualifiedRoute();
        } catch (IOException | RuntimeException | Error failure) {
            try { if (acquired!=null) acquired.release(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
            try { lockChannel.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }
    /** Caller has verified a committed native receipt, either locally or through exact owner evidence. */
    synchronized void qualifyTopology(TopologyChange.Definition definition,NativeTopologyActivation activation)throws IOException{
        if(closed || fenced)throw new IOException("Clock topology owner is closed/fenced");validateTransition(definition,activation);var previous=transitions.get(definition.intentId());
        if(previous!=null && !Arrays.equals(previous.definition().encode(),definition.encode()))throw new IOException("Topology clock intent changed definition");
        if(previous==null && transitions.size()>=MAX_TRANSITIONS)throw new IOException("Clock topology archive capacity exhausted");
        if(previous==null)for(var known:transitions.values())if(known.definition().targetDirectory().generation()<=definition.sourceDirectory().generation())for(var witness:known.activations().values())if(!coversCut(activation.catalogueCut(),witness.catalogueCut()))throw new IOException("Clock topology catalogue cut regressed");
        var witnesses=new HashMap<UUID,NativeTopologyActivation>(previous==null ? Map.of() : previous.activations());var old=witnesses.putIfAbsent(activation.birth().groupId(),activation);if(old!=null){if(!old.position().equals(activation.position()) || !old.catalogueGroup().equals(activation.catalogueGroup()) || !old.catalogueCut().equals(activation.catalogueCut()) || !Arrays.equals(old.commonFreezeDigest(),activation.commonFreezeDigest()) || !Arrays.equals(old.publicationCommitment(),activation.publicationCommitment()) || old.owner().equals(activation.owner()) && !Arrays.equals(old.encode(),activation.encode()))throw new IOException("Topology clock activation changed original committed cut");return;}
        try{transitions.put(definition.intentId(),new Transition(definition,Map.copyOf(witnesses)));rebuildRetired();persistTopology();}catch(IOException|RuntimeException failure){fenced=true;throw failure instanceof IOException io ? io : new IOException("Clock topology archive mutation failed",failure);}
    }
    private void validateTransition(TopologyChange.Definition definition,NativeTopologyActivation activation)throws IOException{
        var bootstrap=routed.bootstrapDirectory();var source=definition.sourceDirectory();activation.header().requireDefinition(definition);
        if(!source.clusterId().equals(bootstrap.clusterId()) || !source.databaseId().equals(bootstrap.databaseId()) || !source.domainId().equals(bootstrap.domainId()) || source.routingPrefixBytes()!=bootstrap.routingPrefixBytes() || definition.kind()==TopologyChange.Kind.RELOCATE || definition.targets().stream().noneMatch(range->range.groupId().equals(activation.birth().groupId())))throw new IOException("Clock topology witness namespace/target changed");
    }
    private static boolean coversCut(ApplyPosition cut,ApplyPosition old){return cut.index()>=old.index() && cut.term()>=old.term() && (cut.index()!=old.index() || cut.term()==old.term());}
    private synchronized void rebuildRetired()throws IOException{retired.clear();for(var transition:transitions.values())for(var parent:transition.definition().parents()){var key=new Participant(parent.groupId(),parent.rangeEpoch());var route=new RatisSessionMaintenance.ClockRoute(transition.definition().sourceDirectory(),parent,true);var old=retired.putIfAbsent(key,route);if(old!=null && !old.descriptor().equals(parent))throw new IOException("Retired clock lineage changed");}if(retired.size()>MAX_RETIRED)throw new IOException("Retired clock route capacity exhausted");}
    synchronized RatisSessionMaintenance.ClockRoute historicalRoute(Participant participant){return retired.get(participant);}
    private synchronized List<Participant> targets(RangeDirectory directory){var result=new HashMap<Participant,Boolean>();for(var range:directory.ranges()){boolean born=false,active=false;for(var transition:transitions.values())if(transition.definition().targets().stream().anyMatch(target->target.groupId().equals(range.groupId()))){born=true;active|=transition.activations().containsKey(range.groupId());}if(!born || active)result.put(new Participant(range.groupId(),range.rangeEpoch()),true);}retired.keySet().forEach(key->result.put(key,true));return result.keySet().stream().sorted(Comparator.comparing(Participant::group).thenComparingLong(Participant::rangeEpoch)).toList();}
    private synchronized RatisSessionMaintenance.ClockRoute clockRoute(Round before,Participant participant,boolean replay)throws IOException{
        var old=before.frozen().ranges().stream().filter(range->range.groupId().equals(participant.group()) && range.rangeEpoch()==participant.rangeEpoch()).findFirst().orElse(null);var route=retired.get(participant);
        if(old!=null)return new RatisSessionMaintenance.ClockRoute(before.frozen(),old,replay || route!=null);
        if(route==null)throw new IOException("Clock participant lacks qualified route");return route;
    }
    private static List<Participant> members(RangeDirectory directory){return directory.ranges().stream().map(range->new Participant(range.groupId(),range.rangeEpoch())).sorted(Comparator.comparing(Participant::group)).toList();}
    private synchronized void qualifyDirectory(RangeDirectory desired,RangeDirectory original)throws IOException{
        if(desired.ranges().size()>maxRanges)throw new IOException("Current clock directory exceeds budget");var known=original;
        if(!Arrays.equals(known.payloadDigest(),desired.payloadDigest()) && !members(known).equals(members(desired))){for(var transition:transitions.values().stream().sorted(Comparator.comparingLong(t->t.definition().sourceDirectory().generation())).toList()){var source=transition.definition().sourceDirectory();if(Arrays.equals(known.payloadDigest(),source.payloadDigest()) || members(known).equals(members(source)) && source.schemaEpoch()>known.schemaEpoch()){known=transition.definition().targetDirectory();if(Arrays.equals(known.payloadDigest(),desired.payloadDigest()))break;}}}
        if(!Arrays.equals(known.payloadDigest(),desired.payloadDigest()) && (!members(known).equals(members(desired)) || desired.schemaEpoch()<known.schemaEpoch() || !Arrays.equals(desired.schemaDigest(),known.schemaDigest()) && !routed.registeredSchema(desired.schemaEpoch(),desired.schemaDigest())))throw new IOException("Changed clock topology lacks a committed native activation witness");
        if(!qualifiedDirectory.clusterId().equals(desired.clusterId()) || !qualifiedDirectory.databaseId().equals(desired.databaseId()) || !qualifiedDirectory.domainId().equals(desired.domainId()) || desired.generation()<known.generation())throw new IOException("Clock directory namespace/generation regressed");if(!Arrays.equals(qualifiedDirectory.payloadDigest(),desired.payloadDigest())){qualifiedDirectory=desired;persistTopology();}
    }
    private synchronized void persistTopology()throws IOException{
        try{var bytes=new ByteArrayOutputStream();var out=new DataOutputStream(bytes);out.writeInt(0x52535450);out.write(identity);writeImage(out,RangeDirectoryCodec.encode(qualifiedDirectory));directorySnapshots.put(HexFormat.of().formatHex(qualifiedDirectory.payloadDigest()),qualifiedDirectory);var keep=new HashSet<String>();keep.add(HexFormat.of().formatHex(qualifiedDirectory.payloadDigest()));if(round!=null && round.frozen()!=null){var key=HexFormat.of().formatHex(round.directory());keep.add(key);directorySnapshots.put(key,round.frozen());}directorySnapshots.keySet().retainAll(keep);out.writeInt(directorySnapshots.size());for(var entry:directorySnapshots.entrySet().stream().sorted(Map.Entry.comparingByKey()).toList())writeImage(out,RangeDirectoryCodec.encode(entry.getValue()));out.writeInt(transitions.size());for(var transition:transitions.values().stream().sorted(Comparator.comparing(t->t.definition().intentId())).toList()){writeImage(out,transition.definition().encode());out.writeInt(transition.activations().size());for(var entry:transition.activations().entrySet().stream().sorted(Map.Entry.comparingByKey()).toList())writeImage(out,entry.getValue().encode());if(bytes.size()>MAX_ARCHIVE_BYTES)throw new IOException("Clock topology archive byte capacity exhausted");}out.flush();SessionJournalFiles.write(topologyManifest,bytes.toByteArray(),MAX_ARCHIVE_BYTES);}catch(IOException failure){fenced=true;throw failure;}
    }
    private void restoreTopology()throws IOException{
        if(!Files.exists(topologyManifest))return;var in=new DataInputStream(new ByteArrayInputStream(SessionJournalFiles.read(topologyManifest,MAX_ARCHIVE_BYTES)));if(in.readInt()!=0x52535450 || !Arrays.equals(in.readNBytes(32),identity))throw new IOException("Wrong topology clock archive identity");qualifiedDirectory=RangeDirectoryCodec.decode(readImage(in,RangeDirectoryCodec.MAX_ENCODED_BYTES));int snapshots=in.readInt();if(snapshots<1 || snapshots>2)throw new IOException("Invalid clock directory snapshot count");for(int n=0;n<snapshots;n++){var directory=RangeDirectoryCodec.decode(readImage(in,RangeDirectoryCodec.MAX_ENCODED_BYTES));var bootstrap=routed.bootstrapDirectory();if(!directory.clusterId().equals(bootstrap.clusterId()) || !directory.databaseId().equals(bootstrap.databaseId()) || !directory.domainId().equals(bootstrap.domainId()) || directory.routingPrefixBytes()!=bootstrap.routingPrefixBytes())throw new IOException("Clock directory snapshot namespace changed");if(directorySnapshots.put(HexFormat.of().formatHex(directory.payloadDigest()),directory)!=null)throw new IOException("Duplicate clock directory snapshot");}if(!directorySnapshots.containsKey(HexFormat.of().formatHex(qualifiedDirectory.payloadDigest())))throw new IOException("Qualified clock directory snapshot missing");int count=in.readInt();if(count<0 || count>MAX_TRANSITIONS)throw new IOException("Clock topology archive count exceeded");for(int i=0;i<count;i++){var definition=TopologyChange.Definition.decode(readImage(in,TopologyChange.MAX_DEFINITION_BYTES));int witnesses=in.readInt();if(witnesses<1 || witnesses>definition.targets().size())throw new IOException("Invalid topology clock activation count");var activations=new HashMap<UUID,NativeTopologyActivation>();for(int n=0;n<witnesses;n++){var activation=NativeTopologyActivation.decode(readImage(in,NativeTopologyActivation.MAX_BYTES));validateTransition(definition,activation);if(activations.put(activation.birth().groupId(),activation)!=null)throw new IOException("Duplicate topology clock target witness");}if(transitions.put(definition.intentId(),new Transition(definition,Map.copyOf(activations)))!=null)throw new IOException("Duplicate topology clock intent");}if(in.available()!=0)throw new IOException("Trailing topology clock archive");rebuildRetired();
    }
    /** Recover only topology routes whose original native activation witnesses are durably archived. */
    private void restoreQualifiedRoute()throws IOException {
        var current=routed.directory();
        // Schema activation has its own proof pipeline; this archive cannot authorize a schema change.
        if(current.schemaEpoch()!=qualifiedDirectory.schemaEpoch() || !Arrays.equals(current.schemaDigest(),qualifiedDirectory.schemaDigest()))return;
        if(transitions.isEmpty())return;
        var ordered=transitions.values().stream().sorted(Comparator.comparingLong(t->t.definition().sourceDirectory().generation())).toList();
        var ancestor=routed.bootstrapDirectory();
        boolean archivedCurrent=Arrays.equals(ancestor.payloadDigest(),qualifiedDirectory.payloadDigest());
        for(var transition:ordered)if(Arrays.equals(ancestor.payloadDigest(),transition.definition().sourceDirectory().payloadDigest())) {
            ancestor=transition.definition().targetDirectory();
            archivedCurrent|=Arrays.equals(ancestor.payloadDigest(),qualifiedDirectory.payloadDigest());
        }
        if(!archivedCurrent)throw new IOException("Archived current clock route lacks an exact native activation chain");
        var restored=current;
        for(var transition:ordered) {
            var source=transition.definition().sourceDirectory();var target=transition.definition().targetDirectory();
            if(!Arrays.equals(restored.payloadDigest(),source.payloadDigest()))continue;
            if(source.schemaEpoch()!=current.schemaEpoch() || target.schemaEpoch()!=current.schemaEpoch()
                    || !Arrays.equals(source.schemaDigest(),current.schemaDigest()) || !Arrays.equals(target.schemaDigest(),current.schemaDigest())
                    || target.generation()<=source.generation())throw new IOException("Archived topology clock route changed schema/generation");
            restored=target;
        }
        // A receipt can be archived before the original pending round has drained. Restore its
        // published target route while retaining that round's exact frozen journals for replay.
        if(Arrays.equals(current.payloadDigest(),restored.payloadDigest()))return;
        try{routed.refresh(restored);}catch(IllegalArgumentException invalid){throw new IOException("Invalid restored topology clock route",invalid);}
    }
    private synchronized RangeDirectory resolveDirectory(byte[] digest)throws IOException{var known=directorySnapshots.get(HexFormat.of().formatHex(digest));if(known!=null)return known;for(var transition:transitions.values())for(var directory:List.of(transition.definition().sourceDirectory(),transition.definition().targetDirectory()))if(Arrays.equals(directory.payloadDigest(),digest))return directory;throw new IOException("Frozen clock directory has no durable qualified snapshot");}
    private static void writeImage(DataOutputStream out,byte[] bytes)throws IOException{out.writeInt(bytes.length);out.write(bytes);}
    private static byte[] readImage(DataInputStream in,int maximum)throws IOException{int length=in.readInt();if(length<1 || length>maximum || length>in.available())throw new IOException("Invalid clock archive image length");return in.readNBytes(length);}
    public synchronized Progress progress() { return progress(round,Map.of()); }
    public synchronized boolean schemaRefreshPending() {
        return round.target()!=null && (!Arrays.equals(round.directory(),routed.directory().payloadDigest()) || !round.ranges().equals(targets(routed.directory())));
    }

    /** Resolve frozen clock intents before replacing a round with its qualified newer schema. */
    public Progress reconcileSchemaRefresh(int budget) throws IOException {
        if (budget<1 || budget>32) throw new IllegalArgumentException("Invalid reconciliation budget");
        var before=enter();
        try {
            var directory=routed.directory();
            if(before.target()==null || Arrays.equals(before.directory(),directory.payloadDigest()) && before.ranges().equals(targets(directory)))return progress(before,Map.of());
            qualifyDirectory(directory,before.frozen());var participants=targets(directory);
            var digest=directory.payloadDigest();
            if (!Arrays.equals(reconciliationDirectory,digest)) { reconciliationDirectory=digest; reconciled=new BitSet(); reconciliationCursor=0; persistReconciliation(before); }
            var failures=new LinkedHashMap<UUID,String>(); int visited=0,work=0;
            while (visited++<before.ranges().size() && work<budget) {
                int index=reconciliationCursor; reconciliationCursor=(index+1)%before.ranges().size();
                if (reconciled.get(index)) continue; work++; persistReconciliation(before);
                var participant=before.ranges().get(index);
                Path journal=root.resolve(participant.group()+"-"+participant.rangeEpoch()+".clock");
                try (var owner=new RatisSessionMaintenance(journal,epochs,routed,participant.group(),clockRoute(before,participant,true))) {
                    if (owner.rangeEpoch()!=participant.rangeEpoch()) throw new IOException("Clock journal incarnation differs");
                    if (owner.pendingCommand()!=null) owner.execute();
                    reconciled.set(index); persistReconciliation(before);
                } catch (IOException failure) { if (fenced) throw failure; var message=String.valueOf(failure.getMessage()); failures.put(participant.group(),message.substring(0,Math.min(message.length(),512))); }
            }
            if (!Arrays.equals(digest,routed.directory().payloadDigest())) throw new IOException("Propagation schema changed during reconciliation");
            if (reconciled.cardinality()!=before.ranges().size()) return progress(before,failures);
            var ledger=epochs.read(); if (ledger==null) throw new IOException("Session catalogue is not initialized");
            var target=SessionAuthority.from(ledger);
            try { target.requireAtLeast(before.target()); } catch (IllegalArgumentException regressed) { throw new IOException("Propagation authority regressed",regressed); }
            var next=new Round(Math.addExact(before.number(),1),target,directory.generation(),digest,participants,new BitSet(),0,directory.schemaEpoch(),directory.schemaDigest(),directory,RangeDirectoryCodec.encode(directory));
            synchronized (this) { persist(next); round=next; }
            reconciliationDirectory=null; reconciled.clear(); return progress(next,Map.of());
        } finally { leave(); }
    }
    /** Whether the completed frozen scope also covers the current cached directory and requested cut. */
    public synchronized boolean covers(SessionAuthority target) {
        return round.complete() && Arrays.equals(round.directory(),routed.directory().payloadDigest()) && round.ranges().equals(targets(routed.directory())) && covers(round.target(),target);
    }
    Path scopeIdentity() { return root.normalize(); }
    /** Freeze one fresh catalogue cut and all current active ranges. */
    public Progress begin() throws IOException { return beginCut(null); }
    /** Verify a fresh catalogue image covers the shared cut, then freeze exactly that cut. */
    public Progress begin(SessionAuthority minimum) throws IOException { return beginCut(Objects.requireNonNull(minimum)); }
    private Progress beginCut(SessionAuthority minimum) throws IOException {
        final Round before=enter();
        try {
            if (before.target()!=null && !before.complete()) throw new IllegalStateException("Propagation round is unfinished");
            var directory=routed.directory();qualifyDirectory(directory,before.frozen()==null ? qualifiedDirectory : before.frozen());
            if(before.target()!=null && !Arrays.equals(before.directory(),directory.payloadDigest()))throw new IOException("Changed clock scope must drain original journals before replacement");
            if (directory.ranges().size()>maxRanges || directory.ranges().stream().anyMatch(range -> range.topologyState()!=RangeDescriptor.TopologyState.ACTIVE)) {
                throw new IOException("Complete active directory exceeds propagation scope/budget");
            }
            var ledger=epochs.read(); if (ledger==null) throw new IOException("Session catalogue is not initialized");
            var current=SessionAuthority.from(ledger);
            final SessionAuthority target;
            try {
                if (minimum!=null) current.requireAtLeast(minimum);
                target=minimum==null ? current : minimum;
                if (before.target()!=null) target.requireAtLeast(before.target());
            } catch (IllegalArgumentException regressed) { throw new IOException("Propagation authority cut regressed",regressed); }
            if (!target.clusterId().equals(directory.clusterId()) || !target.databaseId().equals(directory.databaseId())) throw new IOException("Wrong propagation catalogue identity");
            if (!Arrays.equals(directory.payloadDigest(),routed.directory().payloadDigest())) throw new IOException("Propagation directory changed during preparation");
            var ranges=targets(directory);
            var next=new Round(Math.addExact(before.number(),1),target,directory.generation(),directory.payloadDigest(),ranges,new BitSet(),0,directory.schemaEpoch(),directory.schemaDigest(),directory,RangeDirectoryCodec.encode(directory));
            synchronized (this) { persist(next); round=next; }
            return progress(next,Map.of());
        } finally { leave(); }
    }
    /** At most budget unfinished participants/control submissions; catalogue reads and transport discovery are separate work. */
    public Progress step(int budget) throws IOException {
        if (budget<1 || budget>32) throw new IllegalArgumentException("Invalid propagation work budget");
        final Round before=enter();
        try {
            if (before.target()==null) throw new IllegalStateException("No propagation round");
            if (!Arrays.equals(before.directory(),routed.directory().payloadDigest())) throw new IOException("Propagation directory changed; frozen round requires topology reconciliation");
            if (before.complete()) return progress(before,Map.of());
            var done=(BitSet)before.done().clone(); var failures=new LinkedHashMap<UUID,String>();
            int cursor=before.cursor(), visited=0, work=0;
            while (visited++<before.ranges().size() && work<budget) {
                int index=cursor; cursor=(cursor+1)%before.ranges().size(); if (done.get(index)) continue;
                work++; var target=before.ranges().get(index);
                Path journal=root.resolve(target.group()+"-"+target.rangeEpoch()+".clock");
                try (var owner=new RatisSessionMaintenance(journal,epochs,routed,target.group(),clockRoute(before,target,false))) {
                    if (owner.rangeEpoch()!=target.rangeEpoch()) throw new IOException("Clock journal belongs to another range incarnation");
                    if (!covers(owner.propagated(),before.target())) {
                        if (owner.pendingCommand()==null) owner.prepareAdvance();
                        owner.execute();
                    }
                    if (covers(owner.propagated(),before.target())) done.set(index);
                } catch (IOException failure) {
                    String message=String.valueOf(failure.getMessage()); failures.put(target.group(),message.substring(0,Math.min(message.length(),512)));
                }
            }
            if (!Arrays.equals(before.directory(),routed.directory().payloadDigest())) throw new IOException("Propagation directory changed during work");
            var next=new Round(before.number(),before.target(),before.generation(),before.directory(),before.ranges(),done,cursor,before.schemaEpoch(),before.schemaDigest(),before.frozen(),before.frozenBytes());
            synchronized (this) { persist(next); round=next; }
            return progress(next,failures);
        } finally { leave(); }
    }
    private static boolean covers(SessionAuthority actual,SessionAuthority target) {
        if (actual==null) return false;
        try { actual.requireAtLeast(target); return true; } catch (IllegalArgumentException old) { return false; }
    }
    private static Progress progress(Round round,Map<UUID,String> failures) {
        return new Progress(round.number(),round.target(),round.generation(),round.directory()==null ? "" : HexFormat.of().formatHex(round.directory()),round.ranges(),
                round.done().cardinality(),round.ranges().size(),round.complete(),failures);
    }
    private synchronized Round enter() throws IOException {
        if (closed || fenced || !lock.isValid()) throw new IOException("Propagation owner is closed or fenced; reopen its manifest");
        if (busy) throw new IllegalStateException("Propagation work is already active"); busy=true; return round;
    }
    private synchronized void leave() { busy=false; }
    @Override public synchronized void close() throws IOException {
        if (closed && !lockChannel.isOpen()) return;
        if (busy) throw new IOException("Propagation work is still active"); closed=true;
        try { if (lock.isValid()) lock.release(); } finally { lockChannel.close(); }
    }
    private void persist(Round next) throws IOException {
        try {
            var bytes=new ByteArrayOutputStream(); var output=new DataOutputStream(bytes);
            output.writeInt(MAGIC); output.write(identity); output.writeLong(next.number()); output.writeBoolean(next.target()!=null);
            if (next.target()!=null) {
                output.write(new RangeSessionControl(next.target(),null,0).encode()); output.writeLong(next.generation()); output.write(next.directory());
                output.writeLong(next.schemaEpoch()); output.write(next.schemaDigest());
                output.writeInt(next.ranges().size()); output.writeInt(next.cursor());
                for (int i=0;i<next.ranges().size();i++) {
                    var range=next.ranges().get(i); output.writeLong(range.group().getMostSignificantBits()); output.writeLong(range.group().getLeastSignificantBits());
                    output.writeLong(range.rangeEpoch()); output.writeBoolean(next.done().get(i));
                }
            }
            output.flush(); SessionJournalFiles.write(manifest,bytes.toByteArray(),MAX_BYTES);
        } catch (IOException failure) { fenced=true; throw failure; }
    }
    private void persistReconciliation(Round before) throws IOException {
        try {
            var bytes=new ByteArrayOutputStream(); var out=new DataOutputStream(bytes);
            out.writeInt(0x52534352); out.write(identity); out.writeLong(before.number()); out.write(reconciliationDirectory);
            out.writeInt(before.ranges().size()); out.writeInt(reconciliationCursor);
            for (int i=0;i<before.ranges().size();i++) out.writeBoolean(reconciled.get(i));
            out.flush(); SessionJournalFiles.write(reconciliationManifest,bytes.toByteArray(),MAX_BYTES);
        } catch (IOException failure) { fenced=true; throw failure; }
    }
    private void restoreReconciliation() throws IOException {
        if (!Files.exists(reconciliationManifest)) return;
        var in=new DataInputStream(new ByteArrayInputStream(SessionJournalFiles.read(reconciliationManifest,MAX_BYTES)));
        if (in.readInt()!=0x52534352 || !Arrays.equals(in.readNBytes(32),identity)) throw new IOException("Wrong schema reconciliation identity");
        long number=in.readLong(); byte[] directory=in.readNBytes(32); int count=in.readInt(),cursor=in.readInt();
        if (number<1 || directory.length!=32 || count<1 || count>maxRanges+MAX_RETIRED || cursor<0 || cursor>=count || in.available()!=count)
            throw new IOException("Invalid schema reconciliation checkpoint");
        var done=new BitSet();
        for (int i=0;i<count;i++) { int flag=in.readUnsignedByte(); if (flag>1) throw new IOException("Invalid schema reconciliation flag"); if (flag==1) done.set(i); }
        if (number>round.number()) throw new IOException("Reconciliation checkpoint exceeds propagation round");
        if (number==round.number()) {
            if (count!=round.ranges().size() || round.target()==null) throw new IOException("Reconciliation participants differ from round");
            reconciliationDirectory=directory; reconciled=done; reconciliationCursor=cursor;
        }
    }
    private Round decode(byte[] bytes) throws IOException {
        try {
            var input=new DataInputStream(new ByteArrayInputStream(bytes));
            if (input.readInt()!=MAGIC || !Arrays.equals(input.readNBytes(32),identity)) throw new IOException("Wrong propagation identity");
            long number=input.readLong(); if (number<0) throw new IOException("Invalid propagation round");
            Round result;
            if (!input.readBoolean()) {
                if (number!=0) throw new IOException("Missing propagation target");
                result=new Round(0,null,-1,null,List.of(),new BitSet(),0,-1,null,null,null);
            } else {
                var control=RangeSessionControl.decode(input.readNBytes(RangeSessionControl.ADVANCE_BYTES));
                var directory=routed.directory(); var target=control.authority();
                if (control.binding() || !target.clusterId().equals(directory.clusterId()) || !target.databaseId().equals(directory.databaseId())) throw new IOException("Wrong propagation target");
                long generation=input.readLong(); byte[] digest=input.readNBytes(32); long schemaEpoch=input.readLong(); byte[] schemaDigest=input.readNBytes(32);var frozen=resolveDirectory(digest);byte[] image=RangeDirectoryCodec.encode(frozen);if(generation!=frozen.generation() || schemaEpoch!=frozen.schemaEpoch() || !Arrays.equals(schemaDigest,frozen.schemaDigest()))throw new IOException("Frozen propagation directory binding changed");int count=input.readInt(), cursor=input.readInt();
                if (number==0 || generation<0 || digest.length!=32 || schemaEpoch<0 || schemaDigest.length!=32 || count<1 || count>maxRanges+MAX_RETIRED || cursor<0 || cursor>=count || input.available()!=25*count) throw new IOException("Invalid propagation participants");
                var ranges=new ArrayList<Participant>(); var done=new BitSet(); UUID previous=null;
                for (int i=0;i<count;i++) {
                    var group=new UUID(input.readLong(),input.readLong()); long epoch=input.readLong();
                    if (epoch<0 || previous!=null && previous.compareTo(group)>=0) throw new IOException("Noncanonical propagation participant");
                    previous=group; ranges.add(new Participant(group,epoch)); if (input.readBoolean()) done.set(i);
                }
                for(var participant:ranges)if(frozen.ranges().stream().noneMatch(range->range.groupId().equals(participant.group()) && range.rangeEpoch()==participant.rangeEpoch()) && !retired.containsKey(participant))throw new IOException("Propagation participant has no qualified frozen route");
                result=new Round(number,target,generation,digest,List.copyOf(ranges),done,cursor,schemaEpoch,schemaDigest,frozen,image);
            }
            if (input.available()!=0) throw new IOException("Trailing propagation manifest data"); return result;
        } catch (IllegalArgumentException invalid) { throw new IOException("Invalid propagation manifest",invalid); }
    }
}
