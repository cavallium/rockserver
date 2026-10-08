package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.*;
import java.util.*;

/** Durable, budgeted propagation over a frozen complete directory. No executor or borrowed client is owned. */
public final class RatisSessionPropagation implements AutoCloseable {
    private static final int MAGIC=0x52535052, MAX_BYTES=64*1024;
    public record Participant(UUID group,long rangeEpoch) {}
    private record Round(long number,SessionAuthority target,long generation,byte[] directory,List<Participant> ranges,BitSet done,int cursor) {
        boolean complete() { return target!=null && done.cardinality()==ranges.size(); }
    }
    public record Progress(long round,SessionAuthority target,long directoryGeneration,String directoryDigest,List<Participant> participants,
                           int completed,int total,boolean complete,Map<UUID,String> failures) {
        public Progress { participants=List.copyOf(participants); failures=Map.copyOf(failures); }
    }
    private final Path root, manifest;
    private final RatisSessionEpochs epochs;
    private final RatisRoutedClient routed;
    private final int maxRanges;
    private final byte[] identity;
    private final FileChannel lockChannel;
    private final FileLock lock;
    private Round round;
    private boolean busy,closed,fenced;

    /** Uses one canonical root for its manifest and dedicated clock journals; parent storage must already be durable. */
    public RatisSessionPropagation(Path root,RatisSessionEpochs epochs,RatisRoutedClient routed,int maxRanges) throws IOException {
        this.root=root.toAbsolutePath(); this.epochs=Objects.requireNonNull(epochs); this.routed=Objects.requireNonNull(routed);
        if (maxRanges<1 || maxRanges>RangeDirectoryCodec.MAX_RANGES || !Files.isDirectory(this.root)) throw new IllegalArgumentException("Invalid propagation root/range budget");
        this.maxRanges=maxRanges; identity=routed.sessionIdentityDigest(); manifest=this.root.resolve("propagation.bin");
        lockChannel=FileChannel.open(this.root.resolve("propagation.lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE);
        FileLock acquired=null;
        try {
            try { acquired=lockChannel.tryLock(); }
            catch (java.nio.channels.OverlappingFileLockException owned) { throw new IOException("Propagation root already owned",owned); }
            if (acquired==null) throw new IOException("Propagation root already owned"); lock=acquired;
            if (Files.exists(manifest)) {
                round=decode(SessionJournalFiles.read(manifest,MAX_BYTES));
                SessionJournalFiles.force(manifest); SessionJournalFiles.force(this.root);
            } else {
                round=new Round(0,null,-1,null,List.of(),new BitSet(),0); persist(round);
            }
        } catch (IOException | RuntimeException | Error failure) {
            try { if (acquired!=null) acquired.release(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
            try { lockChannel.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }
    public synchronized Progress progress() { return progress(round,Map.of()); }
    /** Freeze one catalogue cut and all current active ranges; an unfinished round is never silently replaced. */
    public Progress begin() throws IOException {
        final Round before=enter();
        try {
            if (before.target()!=null && !before.complete()) throw new IllegalStateException("Propagation round is unfinished");
            var directory=routed.directory();
            if (directory.ranges().size()>maxRanges || directory.ranges().stream().anyMatch(range -> range.topologyState()!=RangeDescriptor.TopologyState.ACTIVE)) {
                throw new IOException("Complete active directory exceeds propagation scope/budget");
            }
            var ledger=epochs.read(); if (ledger==null) throw new IOException("Session catalogue is not initialized");
            var target=SessionAuthority.from(ledger);
            if (!target.clusterId().equals(directory.clusterId()) || !target.databaseId().equals(directory.databaseId())) throw new IOException("Wrong propagation catalogue identity");
            if (before.target()!=null) target.requireAtLeast(before.target());
            if (!Arrays.equals(directory.payloadDigest(),routed.directory().payloadDigest())) throw new IOException("Propagation directory changed during preparation");
            var ranges=directory.ranges().stream().map(range -> new Participant(range.groupId(),range.rangeEpoch())).sorted(Comparator.comparing(Participant::group)).toList();
            var next=new Round(Math.addExact(before.number(),1),target,directory.generation(),directory.payloadDigest(),ranges,new BitSet(),0);
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
                try (var owner=new RatisSessionMaintenance(journal,epochs,routed,target.group())) {
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
            var next=new Round(before.number(),before.target(),before.generation(),before.directory(),before.ranges(),done,cursor);
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
            output.writeInt(MAGIC); output.writeInt(1); output.write(identity); output.writeLong(next.number()); output.writeBoolean(next.target()!=null);
            if (next.target()!=null) {
                output.write(new RangeSessionControl(next.target(),null,0).encode()); output.writeLong(next.generation()); output.write(next.directory());
                output.writeInt(next.ranges().size()); output.writeInt(next.cursor());
                for (int i=0;i<next.ranges().size();i++) {
                    var range=next.ranges().get(i); output.writeLong(range.group().getMostSignificantBits()); output.writeLong(range.group().getLeastSignificantBits());
                    output.writeLong(range.rangeEpoch()); output.writeBoolean(next.done().get(i));
                }
            }
            output.flush(); SessionJournalFiles.write(manifest,bytes.toByteArray(),MAX_BYTES);
        } catch (IOException failure) { fenced=true; throw failure; }
    }
    private Round decode(byte[] bytes) throws IOException {
        try {
            var input=new DataInputStream(new ByteArrayInputStream(bytes));
            if (input.readInt()!=MAGIC || input.readInt()!=1 || !Arrays.equals(input.readNBytes(32),identity)) throw new IOException("Wrong propagation identity/version");
            long number=input.readLong(); if (number<0) throw new IOException("Invalid propagation round");
            Round result;
            if (!input.readBoolean()) {
                if (number!=0) throw new IOException("Missing propagation target");
                result=new Round(0,null,-1,null,List.of(),new BitSet(),0);
            } else {
                var control=RangeSessionControl.decode(input.readNBytes(RangeSessionControl.ADVANCE_BYTES));
                var directory=routed.directory(); var target=control.authority();
                if (control.binding() || !target.clusterId().equals(directory.clusterId()) || !target.databaseId().equals(directory.databaseId())) throw new IOException("Wrong propagation target");
                long generation=input.readLong(); byte[] digest=input.readNBytes(32); int count=input.readInt(), cursor=input.readInt();
                if (number==0 || generation<0 || digest.length!=32 || count<1 || count>maxRanges || cursor<0 || cursor>=count || input.available()!=25*count) throw new IOException("Invalid propagation participants");
                var ranges=new ArrayList<Participant>(); var done=new BitSet(); UUID previous=null;
                for (int i=0;i<count;i++) {
                    var group=new UUID(input.readLong(),input.readLong()); long epoch=input.readLong();
                    if (epoch<0 || previous!=null && previous.compareTo(group)>=0) throw new IOException("Noncanonical propagation participant");
                    previous=group; ranges.add(new Participant(group,epoch)); if (input.readBoolean()) done.set(i);
                }
                if (Arrays.equals(digest,directory.payloadDigest())) {
                    var actual=directory.ranges().stream().map(range -> new Participant(range.groupId(),range.rangeEpoch())).sorted(Comparator.comparing(Participant::group)).toList();
                    if (generation!=directory.generation() || !ranges.equals(actual)
                            || directory.ranges().stream().anyMatch(range -> range.topologyState()!=RangeDescriptor.TopologyState.ACTIVE)) throw new IOException("Propagation scope differs from directory");
                }
                result=new Round(number,target,generation,digest,List.copyOf(ranges),done,cursor);
            }
            if (input.available()!=0) throw new IOException("Trailing propagation manifest data"); return result;
        } catch (IllegalArgumentException invalid) { throw new IOException("Invalid propagation manifest",invalid); }
    }
}
