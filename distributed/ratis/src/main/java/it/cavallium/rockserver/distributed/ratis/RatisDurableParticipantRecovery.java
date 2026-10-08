package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;

/** Durable recovery of one retained positive native participant. Never registers homes, reports votes, or consumes results. */
public final class RatisDurableParticipantRecovery implements AutoCloseable {
    public static final int MAX_BYTES=32*1024*1024;
    private static final long MAX_ROUTES=16L*1024*1024;
    private static final int MAGIC=0x50524a4c;
    public record Snapshot(PreparedParticipant prepared,RangeDescriptor participantRoute,RangeDescriptor homeRoute,TransactionStatus observedHome,
            RatisRoutedClient.PreparedCommand pendingHome,CommandResult homeResult,boolean homeSuperseded,OperationId lastHome,OperationId finalizeControl,
            RatisRoutedClient.PreparedFinalizationOperation finalization,ApplyPosition finalizationReceipt,ParticipantOutcome outcome) {}
    private final Path path;
    private final RatisTransactionStatuses homes;
    private final RatisRoutedClient data;
    private final byte[] identity,homeIdentity;
    private final FileChannel channel;
    private final FileLock lock;
    private PreparedParticipant prepared;
    private RangeDescriptor participantRoute,homeRoute;
    private TransactionStatus definition,observed;
    private RatisRoutedClient.PreparedCommand pending;
    private CommandResult homeResult;
    private boolean homeSuperseded,active,closed,fenced;
    private OperationId lastHome,finalizeControl;
    private RatisRoutedClient.PreparedFinalizationOperation finalization;
    private ApplyPosition receipt;
    private ParticipantOutcome outcome;

    public static RatisDurableParticipantRecovery create(Path path,RatisTransactionStatuses homes,RatisRoutedClient data,PreparedParticipant nativePrepared,
            RangeDescriptor participantRoute,OperationId initialHomeControl,OperationId finalizeControl) throws IOException {
        var journal=new RatisDurableParticipantRecovery(path,homes,data,true);
        try {
            journal.prepared=Objects.requireNonNull(nativePrepared); journal.participantRoute=Objects.requireNonNull(participantRoute);
            journal.lastHome=Objects.requireNonNull(initialHomeControl); journal.finalizeControl=Objects.requireNonNull(finalizeControl); journal.definition=definition(nativePrepared.preparation().registration());
            data.restoreParticipant(nativePrepared.preparation(),participantRoute);
            journal.requireControls();
            var actual=data.preparedParticipant(nativePrepared.preparation().business().groupId(),nativePrepared.preparation().transactionId());
            if (!nativePrepared.equals(actual)) throw new IOException("Recovery creation lacks matching retained native positive preparation");
            var read=homes.readRegistration(nativePrepared.preparation().transactionId());
            if (read.status()==null) throw new IOException("Native prepared participant has no existing transaction home");
            var preparation=nativePrepared.preparation();
            if (!read.descriptor().groupId().equals(preparation.homeGroup()) || read.descriptor().rangeEpoch()!=preparation.homeEpoch()) throw new IOException("Native preparation/home route binding changed");
            journal.homeRoute=read.descriptor(); journal.observe(read.status()); journal.capacity(); journal.persist(); return journal;
        } catch (IOException | RuntimeException | Error failure) { try { journal.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); } throw failure; }
    }
    public static RatisDurableParticipantRecovery reopen(Path path,RatisTransactionStatuses homes,RatisRoutedClient data) throws IOException { return new RatisDurableParticipantRecovery(path,homes,data,false); }
    private RatisDurableParticipantRecovery(Path file,RatisTransactionStatuses homes,RatisRoutedClient data,boolean create) throws IOException {
        path=file.toAbsolutePath(); this.homes=Objects.requireNonNull(homes); this.data=Objects.requireNonNull(data); identity=data.sessionIdentityDigest(); homeIdentity=homes.identityDigest();
        if (!Files.isDirectory(path.getParent())) throw new IOException("Participant recovery journal parent is absent");
        channel=FileChannel.open(path.resolveSibling(path.getFileName()+".lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE); FileLock acquired=null;
        try {
            try { acquired=channel.tryLock(); } catch (OverlappingFileLockException owned) { throw new IOException("Participant recovery journal already owned",owned); }
            if (acquired==null) throw new IOException("Participant recovery journal already owned"); lock=acquired;
            if (create==Files.exists(path)) throw new IOException(create ? "Participant recovery journal already exists" : "Participant recovery journal absent");
            if (!create) { byte[] body=SessionJournalFiles.read(path,MAX_BYTES); decode(body); capacity(); if (!Arrays.equals(body,encode())) throw new IOException("Noncanonical participant recovery journal"); SessionJournalFiles.force(path); SessionJournalFiles.force(path.getParent()); }
        } catch (IOException | RuntimeException | Error failure) {
            try { if (acquired!=null) acquired.release(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
            try { channel.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); } throw failure;
        }
    }
    synchronized void requireClients(RatisTransactionStatuses expectedHomes,RatisRoutedClient expectedData) throws IOException {
        requireOpen(); if (!Arrays.equals(homeIdentity,Objects.requireNonNull(expectedHomes).identityDigest()) || !Arrays.equals(identity,Objects.requireNonNull(expectedData).sessionIdentityDigest())) throw new IllegalArgumentException("Participant recovery clients differ from journal binding");
    }
    public synchronized Snapshot snapshot() throws IOException { requireOpen(); return new Snapshot(prepared,participantRoute,homeRoute,observed,pending,homeResult,homeSuperseded,lastHome,finalizeControl,finalization,receipt,outcome); }
    public TransactionStatus refreshHome() throws IOException {
        synchronized (this) { idle(); active=true; }
        try {
            var read=homes.readRegistration(transaction());
            try { requireHomeRoute(read.descriptor()); } catch (IllegalArgumentException changed) { throw new IOException("Participant transaction home incarnation/bounds changed",changed); }
            if (read.status()==null) throw new IOException("Existing participant transaction home disappeared");
            synchronized (this) { if (!read.status().equals(observed)) { observe(read.status()); persist(); } return observed; }
        } finally { end(); }
    }
    public synchronized boolean resolveHomeFromObserved() throws IOException {
        idle(); if (homeSuperseded) return true; if (homeResult!=null || !supersedes()) return false; homeSuperseded=true; persist(); return true;
    }
    private boolean supersedes() { return pending!=null && observed!=null && observed.generation()>expected(pending).generation(); }
    public synchronized void queueHome(RatisRoutedClient.PreparedCommand request) throws IOException {
        idle(); homes.restore(request.command(),request.descriptor()); requireHomeRoute(request.descriptor());
        if (pending!=null && homeResult==null && !homeSuperseded) { if (sameCommand(pending,request)) return; throw new IllegalStateException("Unknown home CAS must retain its exact request/route/identity"); }
        var before=expected(request); var next=next(request);
        if (before==null || !before.equals(observed)) throw new IllegalArgumentException("Participant recovery requires the existing observed home image");
        requireTransition(before,next);
        var operation=request.command().operationId(); requireHomeControl(operation);
        if (operation.epoch()<lastHome.epoch() || operation.epoch()==lastHome.epoch() && (!operation.sessionId().equals(lastHome.sessionId()) || operation.sequence()<=lastHome.sequence())) throw new IllegalArgumentException("Participant recovery home identity did not advance");
        lastHome=operation; pending=request; homeResult=null; homeSuperseded=false; persist();
    }
    public CommandResult executeHome() throws IOException {
        RatisRoutedClient.PreparedCommand request;
        synchronized (this) { idle(); if (pending==null) throw new IllegalStateException("No frozen participant home CAS"); if (homeResult!=null) return homeResult; if (homeSuperseded) throw new IllegalStateException("Home CAS semantically superseded; exact RPC result unknown"); request=pending; active=true; }
        try {
            var result=homes.execute(request);
            synchronized (this) {
                if (result.valueCount()!=0) throw new IOException("Participant home CAS returned unexpected values");
                if (result.status()==CommandResult.Status.SUCCESS) { var successful=next(request); if (observed.generation()>successful.generation()) monotone(successful,observed); else observe(successful); }
                homeResult=result; persist(); return result;
            }
        } finally { end(); }
    }
    public synchronized void freezeFinalization(RatisRoutedClient.PreparedFinalizationOperation operation) throws IOException {
        idle(); Objects.requireNonNull(operation);
        if (!participantRoute.equals(operation.descriptor()) || !finalizeControl.equals(operation.finalization().operation())) throw new IllegalArgumentException("Participant finalization route/control changed");
        requireDecision(operation.finalization());
        if (finalization!=null) { if (!Arrays.equals(finalization.bytes(),operation.bytes())) throw new IllegalArgumentException("Frozen participant finalization changed"); return; }
        finalization=data.restoreFinalization(operation.finalization(),participantRoute); persist();
    }
    public ApplyPosition executeFinalization() throws IOException {
        synchronized (this) { idle(); if (finalization==null) throw new IllegalStateException("Participant FINALIZE is not frozen"); active=true; }
        try {
            if (receipt==null && outcome==null) {
                var position=data.executeFinalization(finalization);
                if (!after(position,prepared.position())) throw new IOException("FINALIZE receipt does not follow retained native preparation");
                synchronized (this) { receipt=position; persist(); }
            }
            var nativeOutcome=data.finalizedParticipant(group(),transaction());
            if (nativeOutcome==null) throw new IOException("Participant FINALIZE has no native outcome; retry lookup only");
            synchronized (this) { mergeOutcome(nativeOutcome); persist(); return outcome.position(); }
        } finally { end(); }
    }
    /** Fresh retained native plan and terminal proof; another recovery coordinator's winning control may supply semantic proof. */
    public ParticipantOutcome resolveParticipant() throws IOException {
        synchronized (this) { idle(); active=true; }
        try {
            var nativePrepared=data.preparedParticipant(group(),transaction());
            if (!prepared.equals(nativePrepared)) throw new IOException("Retained positive native preparation disappeared or changed");
            var nativeOutcome=data.finalizedParticipant(group(),transaction());
            synchronized (this) {
                if (nativeOutcome==null) { if (outcome!=null || receipt!=null) throw new IOException("Previously proven/finalized participant outcome is missing"); return null; }
                if (!nativeOutcome.equals(outcome)) { mergeOutcome(nativeOutcome); persist(); } return outcome;
            }
        } finally { end(); }
    }
    private void requireTransition(TransactionStatus before,TransactionStatus next) {
        next.requireSuccessorOf(before);
        if (before.phase()==TransactionStatus.Phase.PREPARING) {
            if (!before.decide(TransactionStatus.Phase.ABORT).equals(next)) throw new IllegalArgumentException("Single-participant recovery may only ABORT an undecided home");
        } else {
            if (outcome==null || !before.finalized(group()).equals(next)) throw new IllegalArgumentException("Single-participant recovery may only ACK its proven local outcome");
            requireDecision(outcome.finalization());
        }
    }
    private void requireDecision(ParticipantFinalization value) {
        if (observed==null || observed.phase()==TransactionStatus.Phase.PREPARING || !observed.terminalDecision().equals(value.decision())) throw new IllegalArgumentException("Participant finalization lacks verified terminal home");
        value.requirePrepared(prepared);
        if (!value.homeGroup().equals(homeRoute.groupId()) || value.homeEpoch()!=homeRoute.rangeEpoch()) throw new IllegalArgumentException("Participant finalization home binding changed");
    }
    private void mergeOutcome(ParticipantOutcome value) throws IOException {
        try {
            requireDecision(value.finalization());
            if (value.vote()==null || !value.vote().prepared() || !prepared.position().equals(value.vote().position()) || !after(value.position(),prepared.position())) throw new IllegalArgumentException("Terminal outcome contradicts retained native positive vote");
            byte[] expected=value.finalization().decision().phase()==TransactionStatus.Phase.COMMIT ? prepared.preparation().business().acceptance().result() : null;
            if (!Arrays.equals(expected,value.result()) || receipt!=null && (finalization==null || !finalization.finalization().equals(value.finalization()) || !receipt.equals(value.position()))) throw new IllegalArgumentException("Terminal business result/finalization receipt changed");
            if (outcome!=null && !outcome.equals(value)) throw new IllegalArgumentException("Native terminal participant outcome changed");
            outcome=value;
        } catch (IllegalArgumentException invalid) { throw new IOException("Invalid native participant terminal proof",invalid); }
    }
    private void observe(TransactionStatus status) throws IOException {
        monotone(definition,status); monotone(prepared.preparation().registration(),status); if (observed!=null) monotone(observed,status);
        var local=status.participants().stream().filter(p -> p.groupId().equals(group())).findFirst().orElseThrow();
        if (local.prepared()!=null && !local.prepared().equals(prepared.position())) throw new IOException("Home local vote contradicts retained native preparation");
        if (outcome!=null && !status.terminalDecision().equals(outcome.finalization().decision())) throw new IOException("Home changed terminal participant decision");
        observed=status;
    }
    private static void monotone(TransactionStatus before,TransactionStatus after) throws IOException {
        if (!before.sameDefinition(after) || after.generation()<before.generation() || before.phase()!=TransactionStatus.Phase.PREPARING && after.phase()!=before.phase()
                || before.consumed() && !after.consumed()) throw new IOException("Participant home definition/decision/progress regressed");
        long steps=(before.phase()==after.phase() ? 0 : 1)+(before.consumed()==after.consumed() ? 0 : 1);
        for (var old:before.participants()) {
            var next=after.participants().stream().filter(p -> p.groupId().equals(old.groupId())).findFirst().orElseThrow();
            if (old.prepared()!=null && !old.prepared().equals(next.prepared()) || old.finalized() && !next.finalized()) throw new IOException("Participant home proof changed");
            if (old.prepared()==null && next.prepared()!=null) { if (before.phase()!=TransactionStatus.Phase.PREPARING) throw new IOException("Home added vote after terminal decision"); steps++; }
            if (!old.finalized() && next.finalized()) steps++;
        }
        if (after.generation()-before.generation()!=steps) throw new IOException("Home generation contradicts monotone progress");
    }
    private void requireControls() {
        var preparation=prepared.preparation();
        if (preparation.ownsOperation(finalizeControl) || preparation.ownsOperation(lastHome) || finalizeControl.equals(lastHome)) throw new IllegalArgumentException("Participant recovery controls overlap retained business/prepare identities");
    }
    private void requireHomeControl(OperationId operation) { if (prepared.preparation().ownsOperation(operation) || finalizeControl.equals(operation)) throw new IllegalArgumentException("Home recovery identity overlaps participant control/business"); }
    private void requireHomeRoute(RangeDescriptor route) {
        if (!homeRoute.rangeId().equals(route.rangeId()) || !homeRoute.groupId().equals(route.groupId()) || homeRoute.rangeEpoch()!=route.rangeEpoch()
                || !Arrays.equals(homeRoute.lowerInclusive(),route.lowerInclusive()) || !Arrays.equals(homeRoute.upperExclusive(),route.upperExclusive()) || route.topologyState()!=RangeDescriptor.TopologyState.ACTIVE) throw new IllegalArgumentException("Original participant home incarnation/bounds changed");
    }
    private UUID transaction() { return prepared.preparation().transactionId(); }
    private UUID group() { return prepared.preparation().business().groupId(); }
    private static TransactionStatus expected(RatisRoutedClient.PreparedCommand command) { byte[] bytes=command.command().predicates().getFirst().expectedValue(); return bytes==null ? null : TransactionStatus.decode(bytes); }
    private static TransactionStatus next(RatisRoutedClient.PreparedCommand command) { return TransactionStatus.decode(command.command().mutations().getFirst().value()); }
    private static boolean sameCommand(RatisRoutedClient.PreparedCommand a,RatisRoutedClient.PreparedCommand b) { return a.descriptor().equals(b.descriptor()) && Arrays.equals(DistributedCommandCodec.encode(a.command()),DistributedCommandCodec.encode(b.command())); }
    private static boolean after(ApplyPosition next,ApplyPosition old) { return next!=null && next.term()>=old.term() && next.index()>old.index(); }
    private static TransactionStatus definition(TransactionStatus status) {
        var participants=status.participants().stream().map(p -> new TransactionStatus.Participant(p.rangeId(),p.groupId(),p.rangeEpoch(),p.schemaDigest(),p.planDigest(),null,false)).toList();
        return TransactionStatus.begin(status.clusterId(),status.databaseId(),status.domainId(),status.transactionId(),status.operation(),status.schemaEpoch(),status.schemaDigest(),status.planDigest(),participants);
    }
    private void requireOpen() throws IOException { if (closed || fenced || !lock.isValid()) throw new IOException("Participant recovery journal closed/fenced; reopen it"); }
    private void idle() throws IOException { requireOpen(); if (active) throw new IOException("Participant recovery RPC already active"); }
    private synchronized void end() { active=false; }
    private void persist() throws IOException {
        try { SessionJournalFiles.write(path,encode(),MAX_BYTES); }
        catch (IOException | RuntimeException failure) { fenced=true; if (failure instanceof IOException io) throw io; throw new IOException("Participant recovery persistence failed; reopen it",failure); }
    }
    @Override public synchronized void close() throws IOException {
        if (active) throw new IOException("Participant recovery RPC active; journal ownership retained");
        if (closed && !channel.isOpen()) return; closed=true; try { if (lock.isValid()) lock.release(); } finally { channel.close(); }
    }
    private void capacity() throws IOException {
        requireControls(); data.restoreParticipant(prepared.preparation(),participantRoute);
        if (!homeRoute.groupId().equals(prepared.preparation().homeGroup()) || homeRoute.rangeEpoch()!=prepared.preparation().homeEpoch()) throw new IOException("Participant recovery home binding changed");
        monotone(definition,prepared.preparation().registration());
        long routes=SessionWindowJournal.routeSize(participantRoute)+SessionWindowJournal.routeSize(homeRoute)+futureHomeRoute();
        if (routes>MAX_ROUTES || prepared.encode().length+routes+65536L+32>MAX_BYTES) throw new IOException("Participant recovery future route/metadata capacity unavailable");
        if (pending!=null) { requireHomeRoute(pending.descriptor()); homes.restore(pending.command(),pending.descriptor()); }
    }
    private long futureHomeRoute() { return 60L+(homeRoute.lowerInclusive()==null ? 0 : homeRoute.lowerInclusive().length)+(homeRoute.upperExclusive()==null ? 0 : homeRoute.upperExclusive().length)+RangeDirectoryCodec.MAX_REPLICAS*(57L+128+256); }
    /** Preparation/registration/business bytes occur once; later home images and native terminal proofs are compact. */
    private byte[] encode() throws IOException {
        var bytes=new ByteArrayOutputStream(); var out=new DataOutputStream(bytes); out.writeInt(MAGIC); out.write(identity); out.write(homeIdentity); blob(out,prepared.encode());
        route(out,participantRoute); route(out,homeRoute); operation(out,lastHome); operation(out,finalizeControl); status(out,observed);
        out.writeBoolean(pending!=null);
        if (pending!=null) {
            route(out,pending.descriptor()); var command=pending.command(); uuid(out,command.mutations().getFirst().columnId()); out.write(command.schemaDigest()); operation(out,command.operationId()); out.writeLong(command.acknowledgedThrough());
            status(out,expected(pending)); status(out,next(pending));
        }
        out.writeByte(homeResult==null ? 0 : homeResult.status().ordinal()+1); out.writeBoolean(homeSuperseded); out.writeBoolean(finalization!=null); position(out,receipt);
        out.writeBoolean(outcome!=null); if (outcome!=null) { operation(out,outcome.finalization().operation()); positionValue(out,outcome.position()); }
        out.flush(); if (bytes.size()>MAX_BYTES-32) throw new IOException("Participant recovery journal exceeds bound"); return bytes.toByteArray();
    }
    private void decode(byte[] body) throws IOException {
        try {
            var in=ByteBuffer.wrap(body); if (in.getInt()!=MAGIC) throw new IOException("Invalid participant recovery journal format");
            byte[] bound=new byte[32],homeBound=new byte[32]; in.get(bound).get(homeBound);
            if (!Arrays.equals(identity,bound) || !Arrays.equals(homeIdentity,homeBound)) throw new IOException("Participant recovery client identity changed");
            prepared=PreparedParticipant.decode(blob(in,16+PreparedParticipant.Preparation.MAX_BYTES)); definition=definition(prepared.preparation().registration());
            participantRoute=SessionWindowJournal.route(in); homeRoute=SessionWindowJournal.route(in); lastHome=operation(in); finalizeControl=operation(in);
            var home=status(in); if (home==null) throw new IOException("Participant recovery journal lacks existing home"); observe(home);
            if (flag(in)) {
                var route=SessionWindowJournal.route(in); requireHomeRoute(route); UUID column=uuid(in); byte[] schema=new byte[32]; in.get(schema); var operation=operation(in); long ack=in.getLong();
                var before=status(in); var next=status(in);
                if (before==null || next==null) throw new IOException("Participant recovery cannot register a missing home");
                byte[] key=ByteBuffer.allocate(16).putLong(transaction().getMostSignificantBits()).putLong(transaction().getLeastSignificantBits()).array();
                pending=homes.restore(new DistributedCommand(route.groupId(),route.rangeEpoch(),schema,operation,List.of(new Mutation(Mutation.Kind.PUT,column,key,next.encode())),List.of(new KeyPredicate(column,key,before.encode())),ack),route);
                if (!lastHome.equals(operation)) throw new IOException("Participant home high-water identity changed"); monotone(before,observed);
            }
            int result=in.get() & 255; if (result>CommandResult.Status.values().length) throw new IOException("Invalid participant home result kind");
            homeResult=result==0 ? null : new CommandResult(CommandResult.Status.values()[result-1],List.of()); homeSuperseded=flag(in);
            if (flag(in)) finalization=data.restoreFinalization(ownFinalization(),participantRoute);
            receipt=position(in); if (receipt!=null && (finalization==null || !after(receipt,prepared.position()))) throw new IOException("Participant finalization receipt lacks original request/vote ordering");
            if (flag(in)) {
                var control=operation(in); var position=positionValue(in); var value=new ParticipantFinalization(prepared.preparation().homeGroup(),prepared.preparation().homeEpoch(),group(),control,observed);
                mergeOutcome(new ParticipantOutcome(value,position,ParticipantVote.prepared(prepared.position()),observed.phase()==TransactionStatus.Phase.COMMIT ? prepared.preparation().business().acceptance().result() : null));
            }
            if (in.hasRemaining()) throw new IOException("Trailing participant recovery journal bytes");
            if (pending==null) { if (homeResult!=null || homeSuperseded) throw new IOException("Home response/retirement has no pending request"); }
            else {
                requireTransition(expected(pending),next(pending));
                if (homeSuperseded && (homeResult!=null || !supersedes())) throw new IOException("Home semantic retirement has no newer proof");
                if (homeResult!=null && homeResult.status()==CommandResult.Status.SUCCESS) monotone(next(pending),observed);
            }
        } catch (RuntimeException invalid) { throw new IOException("Invalid participant recovery journal",invalid); }
    }
    private ParticipantFinalization ownFinalization() {
        var value=new ParticipantFinalization(prepared.preparation().homeGroup(),prepared.preparation().homeEpoch(),group(),finalizeControl,observed); requireDecision(value); return value;
    }
    private void status(DataOutputStream out,TransactionStatus status) throws IOException {
        out.writeBoolean(status!=null); if (status==null) return;
        if (!definition.sameDefinition(status)) throw new IOException("Participant home definition changed"); out.writeByte(status.phase().ordinal()); out.writeBoolean(status.consumed());
        for (var participant:status.participants()) { position(out,participant.prepared()); out.writeBoolean(participant.finalized()); }
    }
    private TransactionStatus status(ByteBuffer in) throws IOException {
        if (!flag(in)) return null; int phase=in.get() & 255; if (phase>=TransactionStatus.Phase.values().length) throw new IOException("Invalid compact participant home phase"); boolean consumed=flag(in);
        var votes=new ArrayList<ApplyPosition>(); var finalized=new ArrayList<Boolean>();
        for (int i=0;i<definition.participants().size();i++) { votes.add(position(in)); finalized.add(flag(in)); }
        var status=definition;
        for (int i=0;i<votes.size();i++) if (votes.get(i)!=null) { var participant=definition.participants().get(i); status=status.prepared(participant.groupId(),participant.planDigest(),votes.get(i)); }
        if (phase!=TransactionStatus.Phase.PREPARING.ordinal()) status=status.decide(TransactionStatus.Phase.values()[phase]);
        for (int i=0;i<finalized.size();i++) if (finalized.get(i)) status=status.finalized(definition.participants().get(i).groupId());
        return consumed ? status.consume() : status;
    }
    private static void route(DataOutputStream out,RangeDescriptor route) throws IOException { var b=ByteBuffer.allocate(Math.toIntExact(SessionWindowJournal.routeSize(route))); SessionWindowJournal.route(b,route); out.write(b.array()); }
    private static void operation(DataOutputStream out,OperationId operation) throws IOException { uuid(out,operation.sessionId()); out.writeLong(operation.epoch()); out.writeLong(operation.sequence()); }
    private static OperationId operation(ByteBuffer in) { return new OperationId(uuid(in),in.getLong(),in.getLong()); }
    private static void uuid(DataOutputStream out,UUID uuid) throws IOException { out.writeLong(uuid.getMostSignificantBits()); out.writeLong(uuid.getLeastSignificantBits()); }
    private static UUID uuid(ByteBuffer in) { return new UUID(in.getLong(),in.getLong()); }
    private static void positionValue(DataOutputStream out,ApplyPosition position) throws IOException { out.writeLong(position.term()); out.writeLong(position.index()); }
    private static ApplyPosition positionValue(ByteBuffer in) { return new ApplyPosition(in.getLong(),in.getLong()); }
    private static void position(DataOutputStream out,ApplyPosition position) throws IOException { out.writeBoolean(position!=null); if (position!=null) positionValue(out,position); }
    private static ApplyPosition position(ByteBuffer in) throws IOException { return flag(in) ? positionValue(in) : null; }
    private static boolean flag(ByteBuffer in) throws IOException { int flag=in.get() & 255; if (flag>1) throw new IOException("Invalid participant recovery journal flag"); return flag==1; }
    private static void blob(DataOutputStream out,byte[] bytes) throws IOException { out.writeInt(bytes.length); out.write(bytes); }
    private static byte[] blob(ByteBuffer in,int maximum) throws IOException { int size=in.getInt(); if (size<0 || size>maximum || size>in.remaining()) throw new IOException("Invalid participant recovery journal blob"); byte[] bytes=new byte[size]; in.get(bytes); return bytes; }
}
