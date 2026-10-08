package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;

/** Caller-driven durable coordinator. Clients are borrowed; no scheduling or decision policy is implied. */
public final class RatisDurableTransaction implements AutoCloseable {
    public static final int MAX_BYTES=64*1024*1024;
    private static final long MAX_AGGREGATE=16L*1024*1024;
    private static final int MAGIC=0x54584a4c;
    public record Intent(RangeDescriptor route,DistributedCommand acceptedBusiness,OperationId prepareControl,OperationId finalizeControl) {
        public Intent {
            Objects.requireNonNull(route); Objects.requireNonNull(acceptedBusiness); Objects.requireNonNull(prepareControl); Objects.requireNonNull(finalizeControl);
            if (acceptedBusiness.kind()!=DistributedCommand.Kind.COMMIT_ACCEPTED || prepareControl.equals(finalizeControl)
                    || prepareControl.equals(acceptedBusiness.operationId()) || finalizeControl.equals(acceptedBusiness.operationId())) throw new IllegalArgumentException("Invalid participant intent identities");
        }
    }
    public record ParticipantProgress(Intent intent,RatisRoutedClient.PreparedParticipantOperation preparation,ParticipantVote vote,
            RatisRoutedClient.PreparedFinalizationOperation finalization,ApplyPosition finalizationReceipt,ParticipantOutcome outcome) {}
    public record Snapshot(TransactionStatus initial,TransactionStatus observedHome,RatisRoutedClient.PreparedCommand pendingHome,
            CommandResult homeResult,boolean homeSuperseded,List<ParticipantProgress> participants) {
        public ParticipantProgress participant(UUID group) { return participants.stream().filter(p -> p.intent().route().groupId().equals(group)).findFirst().orElseThrow(); }
    }
    private static final class Entry {
        final Intent intent;
        TransactionStatus registration;
        RatisRoutedClient.PreparedParticipantOperation preparation;
        ParticipantVote vote;
        RatisRoutedClient.PreparedFinalizationOperation finalization;
        ApplyPosition receipt;
        ParticipantOutcome outcome;
        Entry(Intent intent) { this.intent=intent; }
        ParticipantProgress progress() { return new ParticipantProgress(intent,preparation,vote,finalization,receipt,outcome); }
    }
    private final Path path;
    private final RatisTransactionStatuses homes;
    private final RatisRoutedClient data;
    private final FileChannel channel;
    private final FileLock lock;
    private final byte[] identity,homeIdentity;
    private final LinkedHashMap<UUID,Entry> entries=new LinkedHashMap<>();
    private TransactionStatus initial,observed;
    private RatisRoutedClient.PreparedCommand pending;
    private CommandResult homeResult;
    private OperationId lastHome;
    private boolean active,closed,fenced,homeSuperseded;

    public static RatisDurableTransaction create(Path path,RatisTransactionStatuses homes,RatisRoutedClient data,
            TransactionStatus initial,RatisRoutedClient.PreparedCommand registration,List<Intent> intents) throws IOException {
        var journal=new RatisDurableTransaction(path,homes,data,true);
        try {
            journal.initial=Objects.requireNonNull(initial); initial.requireInitial();
            journal.validateRegistration(registration);
            if (intents.size()!=initial.participants().size()) throw new IllegalArgumentException("Missing participant intents");
            for (var intent:intents) {
                var group=intent.route().groupId();
                if (journal.entries.putIfAbsent(group,new Entry(intent))!=null) throw new IllegalArgumentException("Duplicate participant intent");
                data.restoreParticipant(journal.preparation(intent,initial),intent.route());
            }
            // Stable iteration order is part of the one canonical journal format.
            var sorted=new ArrayList<>(journal.entries.values()); sorted.sort(Comparator.comparing(e -> e.intent.route().groupId())); journal.entries.clear();
            for (var entry:sorted) journal.entries.put(entry.intent.route().groupId(),entry);
            journal.pending=registration; journal.lastHome=registration.command().operationId();
            journal.capacity(); journal.persist(); return journal;
        } catch (IOException | RuntimeException | Error failure) { try { journal.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); } throw failure; }
    }
    public static RatisDurableTransaction reopen(Path path,RatisTransactionStatuses homes,RatisRoutedClient data) throws IOException {
        return new RatisDurableTransaction(path,homes,data,false);
    }
    private RatisDurableTransaction(Path file,RatisTransactionStatuses homes,RatisRoutedClient data,boolean create) throws IOException {
        path=file.toAbsolutePath(); this.homes=Objects.requireNonNull(homes); this.data=Objects.requireNonNull(data);
        identity=data.sessionIdentityDigest(); homeIdentity=homes.identityDigest();
        if (!Files.isDirectory(path.getParent())) throw new IOException("Transaction journal parent is absent");
        channel=FileChannel.open(path.resolveSibling(path.getFileName()+".lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE);
        FileLock acquired=null;
        try {
            try { acquired=channel.tryLock(); } catch (OverlappingFileLockException owned) { throw new IOException("Transaction journal already owned",owned); }
            if (acquired==null) throw new IOException("Transaction journal already owned"); lock=acquired;
            if (create==Files.exists(path)) throw new IOException(create ? "Transaction journal already exists" : "Transaction journal absent");
            if (!create) { byte[] body=SessionJournalFiles.read(path,MAX_BYTES); decode(body); capacity(); if (!Arrays.equals(body,encode())) throw new IOException("Noncanonical transaction journal"); SessionJournalFiles.force(path); SessionJournalFiles.force(path.getParent()); }
        } catch (IOException | RuntimeException | Error failure) {
            try { if (acquired!=null) acquired.release(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
            try { channel.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }
    synchronized void requireClients(RatisTransactionStatuses expectedHomes,RatisRoutedClient expectedData) throws IOException {
        requireOpen();
        if (!Arrays.equals(homeIdentity,Objects.requireNonNull(expectedHomes).identityDigest()) || !Arrays.equals(identity,Objects.requireNonNull(expectedData).sessionIdentityDigest())) throw new IllegalArgumentException("Recovery clients differ from journal binding");
    }
    /** A newer immutable home image retires an obsolete expected-image CAS without inventing its exact RPC result. */
    public synchronized boolean resolveHomeFromObserved() throws IOException {
        idle(); if (homeSuperseded) return true;
        if (homeResult!=null || !homeSupersedesPending()) return false;
        homeSuperseded=true; persist(); return true;
    }
    private boolean homeSupersedesPending() {
        if (pending==null || observed==null) return false;
        var before=expected(pending);
        return before==null ? initial.sameDefinition(observed) : before.sameDefinition(observed) && observed.generation()>before.generation();
    }
    public synchronized Snapshot snapshot() throws IOException {
        requireOpen(); return new Snapshot(initial,observed,pending,homeResult,homeSuperseded,entries.values().stream().map(Entry::progress).toList());
    }
    public synchronized void freezePreparation(RatisRoutedClient.PreparedParticipantOperation operation) throws IOException {
        idle(); var entry=entry(operation.descriptor().groupId());
        if (observed==null) throw new IllegalStateException("Home registration has not been verified");
        var registration=operation.preparation().registration();
        requirePreparation(entry,operation.preparation());
        if (!entry.intent.route().equals(operation.descriptor())) throw new IllegalArgumentException("Preparation route differs from frozen intent");
        var expected=preparation(entry.intent,registration);
        if (entry.preparation!=null) { if (!Arrays.equals(entry.preparation.bytes(),operation.bytes())) throw new IllegalArgumentException("Preparation request changed"); return; }
        entry.registration=registration; entry.preparation=data.restoreParticipant(expected,entry.intent.route()); persist();
    }
    public ParticipantVote executePreparation(UUID group) throws IOException {
        Entry entry;
        synchronized (this) { idle(); entry=entry(group); if (entry.preparation==null) throw new IllegalStateException("Preparation is not frozen"); if (entry.vote!=null) return entry.vote; active=true; }
        try {
            var vote=data.executeParticipant(entry.preparation);
            if (vote.prepared()) new PreparedParticipant(entry.preparation.preparation(),vote.position());
            synchronized (this) { mergeVote(entry,vote); persist(); return entry.vote; }
        } finally { synchronized (this) { active=false; } }
    }
    public synchronized void freezeFinalization(RatisRoutedClient.PreparedFinalizationOperation operation) throws IOException {
        idle(); var entry=entry(operation.descriptor().groupId()); var finalization=operation.finalization();
        if (observed==null || observed.phase()==TransactionStatus.Phase.PREPARING || !observed.terminalDecision().equals(finalization.decision())
                || !finalization.operation().equals(entry.intent.finalizeControl()) || !operation.descriptor().equals(entry.intent.route())) throw new IllegalArgumentException("Finalization differs from verified decision/intent");
        finalization.requirePreparation(preparation(entry.intent,initial));
        if (!finalization.homeGroup().equals(homeRoute().groupId()) || finalization.homeEpoch()!=homeRoute().rangeEpoch()) throw new IllegalArgumentException("Finalization changed home binding");
        if (entry.finalization!=null) { if (!Arrays.equals(entry.finalization.bytes(),operation.bytes())) throw new IllegalArgumentException("Finalization request changed"); return; }
        entry.finalization=data.restoreFinalization(finalization,entry.intent.route()); persist();
    }
    public ApplyPosition executeFinalization(UUID group) throws IOException {
        Entry entry;
        synchronized (this) { idle(); entry=entry(group); if (entry.finalization==null) throw new IllegalStateException("Finalization is not frozen"); if (entry.outcome!=null) return entry.outcome.position(); active=true; }
        try {
            ApplyPosition position=entry.receipt;
            if (position==null) {
                position=data.executeFinalization(entry.finalization);
                synchronized (this) { entry.receipt=position; persist(); }
            }
            // A forced append receipt remains unresolved until matching native terminal proof is forced too.
            var outcome=data.finalizedParticipant(group,initial.transactionId());
            if (outcome==null) throw new IOException("Finalization receipt has no native terminal proof; retry lookup");
            synchronized (this) { mergeOutcome(entry,outcome); persist(); return position; }
        } finally { synchronized (this) { active=false; } }
    }
    public CommandResult executeHome() throws IOException {
        RatisRoutedClient.PreparedCommand request;
        synchronized (this) { idle(); if (pending==null) throw new IllegalStateException("No frozen home request"); if (homeResult!=null) return homeResult; if (homeSuperseded) throw new IllegalStateException("Observed home supersedes this request; exact RPC result is unknown"); request=pending; active=true; }
        try {
            var result=homes.execute(request);
            synchronized (this) {
                if (result.valueCount()!=0) throw new IOException("Home CAS returned unexpected values");
                if (result.status()==CommandResult.Status.SUCCESS) {
                    var successful=next(request);
                    if (observed!=null && observed.generation()>successful.generation()) requireMonotone(successful,observed);
                    else observe(successful);
                }
                homeResult=result; persist(); return result;
            }
        } finally { synchronized (this) { active=false; } }
    }
    public synchronized void queueHome(RatisRoutedClient.PreparedCommand request) throws IOException {
        idle(); homes.restore(request.command(),request.descriptor()); requireHomeRoute(request.descriptor());
        if (pending!=null && homeResult==null && !homeSuperseded) {
            if (sameCommand(pending,request)) return;
            throw new IllegalStateException("Uncertain home request must retain its exact identity/bytes/route");
        }
        var next=next(request); var expected=expected(request);
        if (observed==null) {
            if (expected!=null || !next.equals(initial) || homeResult==null || homeResult.status()==CommandResult.Status.SUCCESS) throw new IllegalArgumentException("Registration retry requires a definite no-effects home result");
        } else {
            if (expected==null || !expected.equals(observed)) throw new IllegalArgumentException("Home CAS does not use current observed image");
            next.requireSuccessorOf(observed); validateEvidence(next);
        }
        advance(request.command().operationId());
        pending=request; homeResult=null; homeSuperseded=false; persist();
    }
    public TransactionStatus refreshHome() throws IOException {
        synchronized (this) { idle(); active=true; }
        try {
            var read=homes.readRegistration(initial.transactionId());
            try { requireHomeRoute(read.descriptor()); } catch (IllegalArgumentException invalid) { throw new IOException("Home read changed frozen range incarnation",invalid); }
            var status=read.status();
            synchronized (this) { if (status==null) { if (observed!=null) throw new IOException("Home registration disappeared"); return null; } observe(status); persist(); return observed; }
        } finally { synchronized (this) { active=false; } }
    }
    public ParticipantProgress resolveParticipant(UUID group) throws IOException {
        Entry entry;
        synchronized (this) { idle(); entry=entry(group); active=true; }
        try {
            var outcome=data.finalizedParticipant(group,initial.transactionId());
            var prepared=data.preparedParticipant(group,initial.transactionId());
            synchronized (this) {
                var oldVote=entry.vote; var oldOutcome=entry.outcome; var oldRegistration=entry.registration;
                try {
                    if (prepared!=null) {
                        try { requirePreparation(entry,prepared.preparation()); }
                        catch (IllegalArgumentException invalid) { throw new IOException("Native prepared plan differs from frozen intent",invalid); }
                        entry.registration=prepared.preparation().registration();
                        mergeVote(entry,ParticipantVote.prepared(prepared.position()));
                    }
                    if (outcome!=null) mergeOutcome(entry,outcome);
                } catch (IOException invalid) { entry.vote=oldVote; entry.outcome=oldOutcome; entry.registration=oldRegistration; throw invalid; }
                persist(); return entry.progress();
            }
        } finally { synchronized (this) { active=false; } }
    }
    private void mergeVote(Entry entry,ParticipantVote vote) throws IOException {
        if (entry.vote!=null && !entry.vote.equals(vote)) throw new IOException("Participant vote changed"); entry.vote=vote;
    }
    private void mergeOutcome(Entry entry,ParticipantOutcome outcome) throws IOException {
        try {
            var finalization=outcome.finalization(); finalization.requirePreparation(preparation(entry.intent,initial));
            if (entry.receipt!=null && (entry.finalization==null || !entry.finalization.finalization().equals(finalization) || !entry.receipt.equals(outcome.position()))) throw new IllegalArgumentException("Native outcome contradicts saved finalization receipt");
            if (observed==null || observed.phase()==TransactionStatus.Phase.PREPARING || !observed.terminalDecision().equals(finalization.decision())
                    || !finalization.homeGroup().equals(homeRoute().groupId()) || finalization.homeEpoch()!=homeRoute().rangeEpoch()) throw new IllegalArgumentException("Outcome has no verified bound home decision");
            if (finalization.decision().phase()==TransactionStatus.Phase.COMMIT && !Arrays.equals(outcome.result(),entry.intent.acceptedBusiness().acceptance().result())) throw new IllegalArgumentException("Native result differs from accepted result");
            if (entry.outcome!=null && !entry.outcome.equals(outcome)) {
                var old=entry.outcome;
                boolean delayedNegative=old.finalization().decision().phase()==TransactionStatus.Phase.ABORT && old.vote()==null
                        && outcome.vote()!=null && !outcome.vote().prepared() && old.finalization().equals(outcome.finalization())
                        && old.position().equals(outcome.position()) && Arrays.equals(old.result(),outcome.result());
                if (!delayedNegative) throw new IllegalArgumentException("Native terminal outcome changed");
            }
            if (outcome.vote()!=null) mergeVote(entry,outcome.vote()); entry.outcome=outcome;
        } catch (IllegalArgumentException invalid) { throw new IOException("Invalid native participant proof",invalid); }
    }
    private void validateEvidence(TransactionStatus next) {
        if (!initial.sameDefinition(next)) throw new IllegalArgumentException("Home definition changed");
        boolean choosingCommit=observed.phase()==TransactionStatus.Phase.PREPARING && next.phase()==TransactionStatus.Phase.COMMIT;
        for (var participant:next.participants()) {
            var entry=entry(participant.groupId());
            var previous=observed.participants().stream().filter(p -> p.groupId().equals(participant.groupId())).findFirst().orElseThrow();
            if (participant.prepared()!=null && (choosingCommit || previous.prepared()==null)
                    && (entry.vote==null || !entry.vote.prepared() || !participant.prepared().equals(entry.vote.position()))) throw new IllegalArgumentException("Home vote lacks local native evidence");
            if (participant.finalized() && !previous.finalized() && entry.outcome==null) throw new IllegalArgumentException("Home ACK lacks native terminal outcome");
        }
        if (next.consumed() && entries.values().stream().anyMatch(e -> e.outcome==null)) throw new IllegalArgumentException("Consumption lacks every native outcome");
    }
    private void observe(TransactionStatus status) throws IOException {
        requireMonotone(initial,status);
        if (observed!=null) requireMonotone(observed,status);
        observed=status;
    }
    private static void requireMonotone(TransactionStatus before,TransactionStatus after) throws IOException {
        if (!before.sameDefinition(after) || after.generation()<before.generation()
                || before.phase()!=TransactionStatus.Phase.PREPARING && after.phase()!=before.phase()
                || before.consumed() && !after.consumed()) throw new IOException("Home state regressed");
        long steps=(before.phase()==after.phase() ? 0 : 1)+(before.consumed()==after.consumed() ? 0 : 1);
        for (var old:before.participants()) {
            var now=after.participants().stream().filter(p -> p.groupId().equals(old.groupId())).findFirst().orElseThrow();
            if (old.prepared()!=null && !old.prepared().equals(now.prepared()) || old.finalized() && !now.finalized()) throw new IOException("Home participant progress regressed");
            if (old.prepared()==null && now.prepared()!=null) {
                if (before.phase()!=TransactionStatus.Phase.PREPARING) throw new IOException("Home added vote after terminal decision");
                steps++;
            }
            if (!old.finalized() && now.finalized()) steps++;
        }
        if (after.generation()-before.generation()!=steps) throw new IOException("Home generation does not match monotone transitions");
    }
    private void requirePreparation(Entry entry,PreparedParticipant.Preparation preparation) {
        try { requireMonotone(initial,preparation.registration()); }
        catch (IOException invalid) { throw new IllegalArgumentException("Preparation has impossible registration progress",invalid); }
        if (!initial.sameDefinition(preparation.registration()) || !preparation(entry.intent,preparation.registration()).equals(preparation)
                || entry.registration!=null && !entry.registration.equals(preparation.registration())) throw new IllegalArgumentException("Preparation changed frozen definition/control/plan/registration");
    }
    private void requireHomeRoute(RangeDescriptor route) {
        if (!homeRoute.rangeId().equals(route.rangeId()) || !homeRoute.groupId().equals(route.groupId()) || homeRoute.rangeEpoch()!=route.rangeEpoch()
                || !Arrays.equals(homeRoute.lowerInclusive(),route.lowerInclusive()) || !Arrays.equals(homeRoute.upperExclusive(),route.upperExclusive())
                || route.topologyState()!=RangeDescriptor.TopologyState.ACTIVE) throw new IllegalArgumentException("Home range incarnation/bounds changed");
    }
    private void validateRegistration(RatisRoutedClient.PreparedCommand registration) {
        homes.restore(registration.command(),registration.descriptor());
        if (expected(registration)!=null || !initial.equals(next(registration))) throw new IllegalArgumentException("Invalid frozen home registration");
        homeRoute=registration.descriptor();
    }
    private RangeDescriptor homeRoute;
    private RangeDescriptor homeRoute() { return homeRoute; }
    private PreparedParticipant.Preparation preparation(Intent intent,TransactionStatus registration) {
        return new PreparedParticipant.Preparation(homeRoute().groupId(),homeRoute().rangeEpoch(),intent.prepareControl(),registration,intent.acceptedBusiness());
    }
    private static TransactionStatus next(RatisRoutedClient.PreparedCommand request) { return TransactionStatus.decode(request.command().mutations().getFirst().value()); }
    private static TransactionStatus expected(RatisRoutedClient.PreparedCommand request) { byte[] bytes=request.command().predicates().getFirst().expectedValue(); return bytes==null ? null : TransactionStatus.decode(bytes); }
    private static boolean sameCommand(RatisRoutedClient.PreparedCommand a,RatisRoutedClient.PreparedCommand b) { return a.descriptor().equals(b.descriptor()) && Arrays.equals(DistributedCommandCodec.encode(a.command()),DistributedCommandCodec.encode(b.command())); }
    private void advance(OperationId operation) {
        if (operation.epoch()<lastHome.epoch() || operation.epoch()==lastHome.epoch() && (!operation.sessionId().equals(lastHome.sessionId()) || operation.sequence()<=lastHome.sequence())) throw new IllegalArgumentException("Home recovery operation identity did not advance");
        if (operation.equals(initial.operation()) || entries.values().stream().anyMatch(e -> e.intent.prepareControl().equals(operation) || e.intent.finalizeControl().equals(operation))) throw new IllegalArgumentException("Home identity overlaps participant/business identity");
        lastHome=operation;
    }
    private Entry entry(UUID group) { var entry=entries.get(group); if (entry==null) throw new IllegalArgumentException("Unregistered participant group"); return entry; }
    private void requireOpen() throws IOException { if (closed || fenced || !lock.isValid()) throw new IOException("Transaction journal closed/fenced; reopen it"); }
    private void idle() throws IOException { requireOpen(); if (active) throw new IOException("Transaction journal RPC already active"); }
    private void persist() throws IOException {
        try { SessionJournalFiles.write(path,encode(),MAX_BYTES); }
        catch (IOException | RuntimeException failure) { fenced=true; if (failure instanceof IOException io) throw io; throw new IOException("Transaction journal persistence failed; reopen it",failure); }
    }
    @Override public synchronized void close() throws IOException {
        if (active) throw new IOException("Transaction RPC active; file ownership retained");
        if (closed && !channel.isOpen()) return; closed=true;
        try { if (lock.isValid()) lock.release(); } finally { channel.close(); }
    }
    /** Reserve all later proof/request metadata before registration can have effects. Results live only in accepted intents. */
    private void capacity() throws IOException {
        if (entries.isEmpty() || entries.size()>TransactionStatus.MAX_PARTICIPANTS || entries.size()!=initial.participants().size()) throw new IOException("Invalid participant count");
        long plans=0,routes=SessionWindowJournal.routeSize(homeRoute);
        var controls=new HashSet<OperationId>(); controls.add(initial.operation());
        for (var entry:entries.values()) {
            var intent=entry.intent;
            plans+=DistributedCommandCodec.encodedSize(intent.acceptedBusiness()); routes+=SessionWindowJournal.routeSize(intent.route());
            if (!controls.add(intent.prepareControl()) || !controls.add(intent.finalizeControl())) throw new IOException("Overlapping control operation identities");
            data.restoreParticipant(preparation(intent,initial),intent.route());
        }
        long pendingRouteReserve=60L+(homeRoute.lowerInclusive()==null ? 0 : homeRoute.lowerInclusive().length)
                +(homeRoute.upperExclusive()==null ? 0 : homeRoute.upperExclusive().length)+RangeDirectoryCodec.MAX_REPLICAS*(57L+128+256);
        routes+=pendingRouteReserve;
        if (plans>MAX_AGGREGATE || routes>MAX_AGGREGATE) throw new IOException("Transaction aggregate intent/route bound exceeded");
        // One initial and observed home, one bounded home CAS, and two finalization decisions per participant.
        long reserve=plans+routes+4L*TransactionStatus.MAX_BYTES+65536L
                +entries.size()*(2L*ParticipantFinalization.MAX_BYTES+TransactionStatus.MAX_BYTES+512);
        if (reserve+32>MAX_BYTES) throw new IOException("Transaction future persistence capacity unavailable");
        if (lastHome.equals(initial.operation()) || controls.contains(lastHome)) throw new IOException("Home operation overlaps participant/business identity");
    }
    private byte[] encode() throws IOException {
        long size=4+64+4L+initial.encode().length+SessionWindowJournal.routeSize(homeRoute)+32+4+(observed==null ? 0 : observed.encode().length)+1;
        if (pending!=null) size+=4L+DistributedCommandCodec.encodedSize(pending.command())+SessionWindowJournal.routeSize(pending.descriptor());
        size+=4+(homeResult==null ? 0 : homeResult.encode().length)+1+4;
        for (var entry:entries.values()) {
            size+=SessionWindowJournal.routeSize(entry.intent.route())+4L+DistributedCommandCodec.encodedSize(entry.intent.acceptedBusiness())+64+4+(entry.registration==null ? 0 : entry.registration.encode().length)+1+voteSize(entry.vote)+4+(entry.finalization==null ? 0 : entry.finalization.finalization().encode().length)+positionSize(entry.receipt)+1;
            if (entry.outcome!=null) size+=4L+entry.outcome.finalization().encode().length+16+voteSize(entry.outcome.vote());
        }
        if (size+32>MAX_BYTES) throw new IOException("Transaction journal bound exceeded");
        var output=ByteBuffer.allocate((int)size).putInt(MAGIC).put(identity).put(homeIdentity);
        blob(output,initial.encode()); SessionWindowJournal.route(output,homeRoute); operation(output,lastHome); blob(output,observed==null ? null : observed.encode());
        output.put((byte)(pending==null ? 0 : 1)); if (pending!=null) { blob(output,DistributedCommandCodec.encode(pending.command())); SessionWindowJournal.route(output,pending.descriptor()); }
        blob(output,homeResult==null ? null : homeResult.encode()); output.put((byte)(homeSuperseded ? 1 : 0)); output.putInt(entries.size());
        for (var entry:entries.values()) {
            var intent=entry.intent;
            SessionWindowJournal.route(output,intent.route()); blob(output,DistributedCommandCodec.encode(intent.acceptedBusiness())); operation(output,intent.prepareControl()); operation(output,intent.finalizeControl());
            blob(output,entry.registration==null ? null : entry.registration.encode()); output.put((byte)(entry.preparation==null ? 0 : 1)); vote(output,entry.vote); blob(output,entry.finalization==null ? null : entry.finalization.finalization().encode()); position(output,entry.receipt);
            output.put((byte)(entry.outcome==null ? 0 : 1));
            if (entry.outcome!=null) { blob(output,entry.outcome.finalization().encode()); output.putLong(entry.outcome.position().term()).putLong(entry.outcome.position().index()); vote(output,entry.outcome.vote()); }
        }
        if (output.hasRemaining()) throw new IOException("Transaction journal size mismatch"); return output.array();
    }
    private void decode(byte[] body) throws IOException {
        try {
            var input=ByteBuffer.wrap(body);
            if (input.getInt()!=MAGIC) throw new IOException("Invalid transaction journal format");
            byte[] bound=new byte[32],homeBound=new byte[32]; input.get(bound).get(homeBound);
            if (!Arrays.equals(bound,identity) || !Arrays.equals(homeBound,homeIdentity)) throw new IOException("Wrong transaction journal client identity");
            initial=TransactionStatus.decode(blob(input,TransactionStatus.MAX_BYTES,false)); initial.requireInitial(); homeRoute=SessionWindowJournal.route(input); lastHome=operation(input);
            byte[] status=blob(input,TransactionStatus.MAX_BYTES,true); observed=status==null ? null : TransactionStatus.decode(status);
            if (observed!=null) requireMonotone(initial,observed);
            if (flag(input)) {
                var command=DistributedCommandCodec.decode(blob(input,DistributedCommandCodec.MAX_ENCODED_BYTES,false)); var route=SessionWindowJournal.route(input);
                requireHomeRoute(route); pending=homes.restore(command,route);
            }
            byte[] result=blob(input,8,true); homeResult=result==null ? null : CommandResult.decode(result); homeSuperseded=flag(input);
            int count=input.getInt(); if (count<1 || count>TransactionStatus.MAX_PARTICIPANTS) throw new IOException("Invalid journal participant count");
            for (int i=0;i<count;i++) {
                var route=SessionWindowJournal.route(input); var business=DistributedCommandCodec.decode(blob(input,DistributedCommandCodec.MAX_ENCODED_BYTES,false));
                var intent=new Intent(route,business,operation(input),operation(input)); var entry=new Entry(intent);
                if (entries.putIfAbsent(route.groupId(),entry)!=null) throw new IOException("Duplicate journal participant");
                byte[] registration=blob(input,TransactionStatus.MAX_BYTES,true); entry.registration=registration==null ? null : TransactionStatus.decode(registration);
                if (entry.registration!=null) requirePreparation(entry,preparation(intent,entry.registration));
                if (flag(input)) {
                    if (entry.registration==null) throw new IOException("Frozen preparation has no registration");
                    entry.preparation=data.restoreParticipant(preparation(intent,entry.registration),route);
                }
                entry.vote=vote(input); byte[] finalization=blob(input,ParticipantFinalization.MAX_BYTES,true);
                if (finalization!=null) entry.finalization=data.restoreFinalization(ParticipantFinalization.decode(finalization),route);
                entry.receipt=position(input);
                if (flag(input)) {
                    var decision=ParticipantFinalization.decode(blob(input,ParticipantFinalization.MAX_BYTES,false)); var position=new ApplyPosition(input.getLong(),input.getLong()); var vote=vote(input);
                    var outcome=new ParticipantOutcome(decision,position,vote,decision.decision().phase()==TransactionStatus.Phase.COMMIT ? intent.acceptedBusiness().acceptance().result() : null);
                    mergeOutcome(entry,outcome);
                }
                if (entry.vote!=null && entry.vote.prepared()) new PreparedParticipant(preparation(intent,initial),entry.vote.position());
                if (entry.receipt!=null && entry.finalization==null) throw new IOException("Final receipt has no frozen request");
                if (entry.finalization!=null) {
                    var value=entry.finalization.finalization(); value.requirePreparation(preparation(intent,initial));
                    if (!value.operation().equals(intent.finalizeControl()) || observed==null || observed.phase()==TransactionStatus.Phase.PREPARING || !value.decision().equals(observed.terminalDecision())) throw new IOException("Frozen finalization differs from home/intent");
                }
            }
            if (input.hasRemaining()) throw new IOException("Trailing transaction journal bytes");
            if (pending==null || !pending.command().operationId().equals(lastHome) || !initial.sameDefinition(next(pending))) throw new IOException("Invalid journal home request");
            if (expected(pending)==null) { if (!next(pending).equals(initial)) throw new IOException("Invalid journal registration"); }
            else next(pending).requireSuccessorOf(expected(pending));
            if (homeSuperseded && (homeResult!=null || !homeSupersedesPending())) throw new IOException("Home request retirement has no newer observed proof");
            if (homeResult!=null && homeResult.status()==CommandResult.Status.SUCCESS) {
                var successful=next(pending);
                if (observed==null) throw new IOException("Successful home request lacks monotone observed state");
                requireMonotone(successful,observed);
            }
        } catch (RuntimeException invalid) { throw new IOException("Invalid transaction journal",invalid); }
    }
    private static long voteSize(ParticipantVote vote) { return vote!=null && vote.prepared() ? 17 : 1; }
    private static long positionSize(ApplyPosition position) { return position==null ? 1 : 17; }
    private static void vote(ByteBuffer output,ParticipantVote vote) { output.put((byte)(vote==null ? 0 : vote.prepared() ? 1 : 2)); if (vote!=null && vote.prepared()) output.putLong(vote.position().term()).putLong(vote.position().index()); }
    private static ParticipantVote vote(ByteBuffer input) throws IOException {
        int kind=input.get() & 255; if (kind>2) throw new IOException("Invalid journal vote");
        return kind==0 ? null : kind==2 ? ParticipantVote.negative() : ParticipantVote.prepared(new ApplyPosition(input.getLong(),input.getLong()));
    }
    private static void position(ByteBuffer output,ApplyPosition position) { output.put((byte)(position==null ? 0 : 1)); if (position!=null) output.putLong(position.term()).putLong(position.index()); }
    private static ApplyPosition position(ByteBuffer input) throws IOException { return flag(input) ? new ApplyPosition(input.getLong(),input.getLong()) : null; }
    private static boolean flag(ByteBuffer input) throws IOException { int flag=input.get() & 255; if (flag>1) throw new IOException("Invalid journal flag"); return flag==1; }
    private static void operation(ByteBuffer output,OperationId operation) { output.putLong(operation.sessionId().getMostSignificantBits()).putLong(operation.sessionId().getLeastSignificantBits()).putLong(operation.epoch()).putLong(operation.sequence()); }
    private static OperationId operation(ByteBuffer input) { return new OperationId(new UUID(input.getLong(),input.getLong()),input.getLong(),input.getLong()); }
    private static void blob(ByteBuffer output,byte[] bytes) { output.putInt(bytes==null ? -1 : bytes.length); if (bytes!=null) output.put(bytes); }
    private static byte[] blob(ByteBuffer input,int maximum,boolean nullable) throws IOException {
        int length=input.getInt(); if (length== -1 && nullable) return null;
        if (length<0 || length>maximum || length>input.remaining()) throw new IOException("Invalid journal blob size");
        byte[] bytes=new byte[length]; input.get(bytes); return bytes;
    }
}
