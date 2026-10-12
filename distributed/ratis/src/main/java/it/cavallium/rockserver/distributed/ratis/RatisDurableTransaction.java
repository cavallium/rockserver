package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.security.*;
import java.util.*;

/** Caller-driven durable coordinator. Clients are borrowed; no scheduling or decision policy is implied. */
public final class RatisDurableTransaction implements AutoCloseable {
    public static final int MAX_BYTES=64*1024*1024;
    private static final long MAX_AGGREGATE=16L*1024*1024;
    public static final int MAX_RESERVATION_EVIDENCE_BYTES=20+DistributedCommandCodec.MAX_ENCODED_BYTES+2*RangeDirectoryCodec.MAX_ENCODED_BYTES+RatisTopologyOperationOutcomeRpc.MAX_REPLY_BYTES;
    private byte[] reservationEvidence;
    private static final int MAGIC=0x54584a4c;
    public record Intent(RangeDescriptor route,DistributedCommand acceptedBusiness,OperationId prepareControl,OperationId finalizeControl,List<Integer> resultPositions,UUID captureId) {
        public Intent(RangeDescriptor route,DistributedCommand acceptedBusiness,OperationId prepareControl,OperationId finalizeControl) {
            this(route,acceptedBusiness,prepareControl,finalizeControl,java.util.stream.IntStream.range(0,localReturnCount(acceptedBusiness)).boxed().toList(),null);
        }
        public Intent(RangeDescriptor route,DistributedCommand acceptedBusiness,OperationId prepareControl,OperationId finalizeControl,List<Integer> resultPositions) {
            this(route,acceptedBusiness,prepareControl,finalizeControl,resultPositions,null);
        }
        public Intent {
            Objects.requireNonNull(route); Objects.requireNonNull(acceptedBusiness); Objects.requireNonNull(prepareControl); Objects.requireNonNull(finalizeControl);
            if (acceptedBusiness.kind()!=DistributedCommand.Kind.COMMIT_ACCEPTED || prepareControl.equals(finalizeControl)
                    || prepareControl.equals(acceptedBusiness.operationId()) || finalizeControl.equals(acceptedBusiness.operationId())) throw new IllegalArgumentException("Invalid participant intent identities");
            // Accepted command construction/decoding validates its immutable canonical origin audit.
            if (captureId!=null) Objects.requireNonNull(acceptedBusiness.acceptance(),"Remote capture requires accepted origin audit");
            resultPositions=List.copyOf(resultPositions);
            if (resultPositions.size()!=localReturnCount(acceptedBusiness)) throw new IllegalArgumentException("Participant result slot count differs from accepted return modes");
            int previous=-1; for (int position:resultPositions) { if (position<0 || position>255 || position<=previous) throw new IllegalArgumentException("Participant result slots must be strictly increasing within 0..255"); previous=position; }
        }
    }
    private static int localReturnCount(DistributedCommand command) {
        Objects.requireNonNull(command);
        if (command.kind()!=DistributedCommand.Kind.COMMIT_ACCEPTED) throw new IllegalArgumentException("Participant intent requires accepted business plan");
        var result=CommandResult.decode(command.acceptance().result());
        if (result.status()!=CommandResult.Status.SUCCESS || result.valueCount()!=command.mutations().stream().filter(m -> m.returnMode()!=Mutation.ReturnMode.NONE).count()) throw new IllegalArgumentException("Accepted result differs from participant return modes");
        return result.valueCount();
    }
    public static byte[] planDigest(List<Intent> intents) {
        Objects.requireNonNull(intents); if (intents.isEmpty() || intents.size()>TransactionStatus.MAX_PARTICIPANTS) throw new IllegalArgumentException("Invalid transaction intent count");
        var ordered=intents.stream().sorted((a,b) -> compare(a.route().groupId(),b.route().groupId())).toList();
        int size=4; for (var intent:ordered) size+=77+4*intent.resultPositions().size()+(intent.captureId()==null ? 0 : 16);
        var bytes=ByteBuffer.allocate(size).putInt(ordered.size());
        for (var intent:ordered) {
            uuid(bytes,intent.route().groupId()); uuid(bytes,intent.route().rangeId()); bytes.putLong(intent.route().rangeEpoch()).put(intent.acceptedBusiness().payloadDigest()).putInt(intent.resultPositions().size());
            for (int position:intent.resultPositions()) bytes.putInt(position);
            bytes.put((byte)(intent.captureId()==null ? 0 : 1)); if (intent.captureId()!=null) uuid(bytes,intent.captureId());
        }
        try { return MessageDigest.getInstance("SHA-256").digest(bytes.array()); } catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
    private static int compare(UUID a,UUID b) { int first=Long.compareUnsigned(a.getMostSignificantBits(),b.getMostSignificantBits()); return first!=0 ? first : Long.compareUnsigned(a.getLeastSignificantBits(),b.getLeastSignificantBits()); }
    private static void uuid(ByteBuffer bytes,UUID value) { bytes.putLong(value.getMostSignificantBits()).putLong(value.getLeastSignificantBits()); }
    public record ParticipantProgress(Intent intent,RatisRoutedClient.PreparedParticipantOperation preparation,ParticipantVote vote,
            RatisRoutedClient.PreparedFinalizationOperation finalization,ApplyPosition finalizationReceipt,ParticipantOutcome outcome,ParticipantFenceProof retirementProof) {}
    public record Snapshot(TransactionStatus initial,TransactionStatus observedHome,RatisRoutedClient.PreparedCommand pendingHome,
            CommandResult homeResult,boolean homeSuperseded,OperationId lastHome,
            RatisRoutedClient.PreparedHomeRecoveryOperation pendingRecovery,HomeRecoveryReceipt recoveryReceipt,List<ParticipantProgress> participants,HomeFenceProof homeRetirementProof) {
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
        ParticipantFenceProof retirementProof;
        Entry(Intent intent) { this.intent=intent; }
        ParticipantProgress progress() { return new ParticipantProgress(intent,preparation,vote,finalization,receipt,outcome,retirementProof); }
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
    private UUID recoveryNamespace=UUID.randomUUID();
    private long recoverySequence;
    private RatisRoutedClient.PreparedHomeRecoveryOperation pendingRecovery;
    private HomeRecoveryReceipt recoveryReceipt;
    private HomeFenceProof homeRetirementProof;
    private boolean active,closed,fenced,homeSuperseded,reclaimed;

    public static RatisDurableTransaction create(Path path,RatisTransactionStatuses homes,RatisRoutedClient data,
            TransactionStatus initial,RatisRoutedClient.PreparedCommand registration,List<Intent> intents) throws IOException {
        return create(path,homes,data,initial,registration,intents,null);
    }
    public static RatisDurableTransaction createReserved(Path path,RatisTransactionStatuses homes,RatisRoutedClient data,TransactionStatus initial,RatisRoutedClient.PreparedCommand registration,List<Intent> intents,RatisOperationRerouteAdmissionRpc.Evidence evidence)throws IOException {
        if(initial.origin()==null)throw new IllegalArgumentException("Reserved journal needs original HOME origin");
        return create(path,homes,data,initial,registration,intents,evidence);
    }
    private static RatisDurableTransaction create(Path path,RatisTransactionStatuses homes,RatisRoutedClient data,TransactionStatus initial,RatisRoutedClient.PreparedCommand registration,List<Intent> intents,RatisOperationRerouteAdmissionRpc.Evidence evidence)throws IOException {
        var journal=new RatisDurableTransaction(path,homes,data,true);
        try {
            journal.initial=Objects.requireNonNull(initial); initial.requireInitial(); if(initial.reserved())throw new IllegalArgumentException("Participant journal requires promoted HOME initial status");
            journal.reservationEvidence=validateReservationEvidence(initial,evidence);
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
        return before==null || before.reserved() ? initial.sameDefinition(observed) : before.sameDefinition(observed) && observed.generation()>before.generation();
    }
    public synchronized Snapshot snapshot() throws IOException {
        requireOpen(); return new Snapshot(initial,observed,pending,homeResult,homeSuperseded,lastHome,pendingRecovery,recoveryReceipt,entries.values().stream().map(Entry::progress).toList(),homeRetirementProof);
    }
    /** Returns the globally ordered business outcome only after every native finalization and home ACK is durable. */
    public synchronized CommandResult result() throws IOException {
        requireOpen();
        if(noEffectReservationState(observed) && observed.terminal())return observed.terminalBusinessResult();
        if (homeRetirementProof!=null && (observed==null || !observed.fullyFinalized()
                || entries.values().stream().anyMatch(entry -> entry.outcome==null && (observed.phase()!=TransactionStatus.Phase.ABORT || entry.retirementProof==null)))) throw new RatisTransactionOutcomeExpiredException(initial.transactionId());
        if (observed==null || !observed.fullyFinalized() || entries.values().stream().anyMatch(entry -> entry.outcome==null && entry.retirementProof==null)) throw new IllegalStateException("Transaction result requires all participant proofs and home ACKs");
        if (observed.phase()==TransactionStatus.Phase.COMMIT && entries.values().stream().anyMatch(entry -> entry.outcome==null)) throw new RatisTransactionOutcomeExpiredException(initial.transactionId());
        for (var entry:entries.values()) if (entry.outcome!=null && !entry.outcome.finalization().decision().equals(observed.terminalDecision())) throw new IOException("Saved participant outcome differs from terminal home");
        if (observed.phase()==TransactionStatus.Phase.ABORT) return new CommandResult(CommandResult.Status.CONFLICT,List.of());
        int count=resultCount(); var values=new ArrayList<byte[]>(Collections.nCopies(count,null));
        for (var entry:entries.values()) {
            var result=CommandResult.decode(entry.outcome.result());
            for (int i=0;i<result.valueCount();i++) values.set(entry.intent.resultPositions().get(i),result.value(i));
        }
        return new CommandResult(CommandResult.Status.SUCCESS,values);
    }
    public synchronized void freezePreparation(RatisRoutedClient.PreparedParticipantOperation operation) throws IOException {
        idle(); var entry=entry(operation.descriptor().groupId());
        if (observed==null) throw new IllegalStateException("Home registration has not been verified");
        var registration=operation.preparation().registration();
        requirePreparation(entry,operation.preparation());
        if (!entry.intent.route().equals(operation.descriptor())) throw new IllegalArgumentException("Preparation route differs from frozen intent");
        var expected=preparation(entry.intent,registration);
        if (entry.preparation!=null) { if (!Arrays.equals(entry.preparation.bytes(),operation.bytes())) throw new IllegalArgumentException("Preparation request changed"); return; }
        if (operation.negative() && (entry.vote!=null && entry.vote.prepared() || entry.outcome!=null && entry.outcome.vote()!=null && entry.outcome.vote().prepared())) throw new IllegalArgumentException("Negative PREPARE contradicts positive native proof");
        entry.registration=registration; entry.preparation=data.restoreParticipant(expected,entry.intent.route(),operation.negative()); persist();
    }
    public ParticipantVote executePreparation(UUID group) throws IOException {
        Entry entry;
        synchronized (this) { idle(); entry=entry(group); if (entry.preparation==null) throw new IllegalStateException("Preparation is not frozen"); if (entry.retirementProof!=null) return ParticipantVote.retired(entry.retirementProof); if (entry.vote!=null) return entry.vote; active=true; }
        try {
            var vote=data.executeParticipant(entry.preparation);
            if (vote.prepared()) new PreparedParticipant(entry.preparation.preparation(),vote.position());
            synchronized (this) { mergeVote(entry,vote); persist(); return vote; }
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
        synchronized (this) { idle(); entry=entry(group); if (entry.finalization==null) throw new IllegalStateException("Finalization is not frozen"); if (entry.outcome!=null) return entry.outcome.position(); if (entry.retirementProof!=null) throw new RatisParticipantRetiredException(entry.retirementProof); active=true; }
        try {
            ApplyPosition position=entry.receipt;
            if (position==null) {
                try { position=data.executeFinalization(entry.finalization); }
                catch (RatisParticipantRetiredException retired) { synchronized (this) { mergeRetirement(entry,retired.proof()); persist(); } throw retired; }
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
            var retained=pendingRecovery==null ? null : homes.executeRecovery(pendingRecovery);
            var before=expected(request);
            var result=retained!=null ? retained.result() : before!=null && before.reserved() && next(request).phase()==TransactionStatus.Phase.PREPARING ? homes.execute(request,promotionEvidence()) : homes.execute(request);
            if (retained!=null && !retained.recovery().equals(pendingRecovery.recovery())) throw new IOException("Protected home receipt changed frozen request");
            synchronized (this) {
                if (result.valueCount()!=0) throw new IOException("Home CAS returned unexpected values");
                if (result.status()==CommandResult.Status.SUCCESS) {
                    var successful=next(request);
                    if (observed!=null && observed.generation()>successful.generation()) requireMonotone(successful,observed);
                    else observe(successful);
                }
                if(result.status()==CommandResult.Status.SESSION_EXPIRED && pendingRecovery==null && expected(request)!=null && expected(request).reserved()) {
                    if(!noEffectReservationState(expected(request)))throw new IOException("Expired promotion has participant effects");
                    observed=expected(request);
                }
                homeResult=result; recoveryReceipt=retained; persist(); return result;
            }
        } catch (RatisHomeRetiredException retired) {
            synchronized (this) { mergeHomeRetirement(retired.proof()); persist(); } throw retired;
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
            if (!Objects.equals(expected,registrationExpected()) || !next.equals(initial) || homeResult==null || homeResult.status()==CommandResult.Status.SUCCESS) throw new IllegalArgumentException("Registration retry requires a definite no-effects home result");
        } else {
            if (expected==null || !expected.equals(observed)) throw new IllegalArgumentException("Home CAS does not use current observed image");
            next.requireSuccessorOf(observed); validateEvidence(next);
            if (protectedTransition(observed,next)) throw new IllegalArgumentException("ABORT/ACK requires protected recovery journal request");
        }
        advance(request.command().operationId());
        pending=request; pendingRecovery=null; recoveryReceipt=null; homeResult=null; homeSuperseded=false; persist();
    }
    /** Freezes a journal-owned protected ABORT or one proven participant ACK before any RPC. */
    public synchronized void queueRecovery(TransactionStatus next) throws IOException {
        idle(); Objects.requireNonNull(next);
        if (pending!=null && homeResult==null && !homeSuperseded) {
            if (pendingRecovery!=null && pendingRecovery.recovery().next().equals(next)) return;
            throw new IllegalStateException("Uncertain home request must retain its exact request");
        }
        if (observed==null || !protectedTransition(observed,next)) throw new IllegalArgumentException("Protected recovery requires existing home ABORT or one local ACK");
        next.requireSuccessorOf(observed); validateEvidence(next);
        ParticipantOutcome proof=null; ParticipantFenceProof fence=null;
        if (observed.phase()!=TransactionStatus.Phase.PREPARING) {
            for (var participant:next.participants()) if (participant.finalized() && !observed.participants().stream().filter(p -> p.groupId().equals(participant.groupId())).findFirst().orElseThrow().finalized()) { var entry=entry(participant.groupId()); proof=entry.outcome; fence=entry.retirementProof; }
        }
        if (recoverySequence==Long.MAX_VALUE) throw new IOException("Protected recovery identity namespace exhausted");
        var operation=new OperationId(recoveryNamespace,0,recoverySequence);
        var request=proof==null && fence!=null ? homes.prepareFencedRecovery(operation,observed,next,fence) : homes.prepareRecovery(operation,observed,next,proof);
        requireHomeRoute(request.descriptor());
        if (proof!=null) request.recovery().requireOutcome(proof);
        pendingRecovery=request; pending=request.command(); recoverySequence++; recoveryReceipt=null; homeResult=null; homeSuperseded=false; persist();
    }
    private static boolean protectedTransition(TransactionStatus before,TransactionStatus next) {
        if (before.phase()==TransactionStatus.Phase.PREPARING || before.reserved()) return next.phase()==TransactionStatus.Phase.ABORT;
        return next.participants().stream().anyMatch(p -> p.finalized() && !before.participants().stream().filter(old -> old.groupId().equals(p.groupId())).findFirst().orElseThrow().finalized());
    }
    public TransactionStatus refreshHome() throws IOException {
        synchronized (this) { idle(); active=true; }
        try {
            var read=homes.readRegistration(initial.transactionId());
            try { requireHomeRoute(read.descriptor()); } catch (IllegalArgumentException invalid) { throw new IOException("Home read changed frozen range incarnation",invalid); }
            var status=read.status();
            if(status!=null && status.reserved()) {
                if(!status.equals(registrationExpected()))throw new IOException("HOME reservation differs from promoted journal origin");
                synchronized(this){if(observed!=null && !observed.reserved())throw new IOException("Promoted HOME regressed to reservation");}
                return status;
            }
            if (status==null || !initial.sameDefinition(status) && !noEffectReservationState(status)) {
                var retirement=homes.fence(homes.identity(initial,homeRoute));
                if (retirement!=null) { synchronized (this) { mergeHomeRetirement(retirement); persist(); } throw new RatisHomeRetiredException(retirement); }
            }
            synchronized (this) { if (status==null) { if (observed!=null) throw new IOException("Home registration disappeared"); return null; } if (status.equals(observed)) return observed; observe(status); persist(); return observed; }
        } finally { synchronized (this) { active=false; } }
    }
    public ParticipantProgress resolveParticipant(UUID group) throws IOException {
        Entry entry;
        synchronized (this) { idle(); entry=entry(group); active=true; }
        try {
            var outcome=data.finalizedParticipant(group,initial.transactionId());
            var prepared=data.preparedParticipant(group,initial.transactionId());
            var retirement=outcome==null && prepared==null ? data.participantFence(participantIdentity(entry)) : null;
            synchronized (this) {
                var oldVote=entry.vote; var oldOutcome=entry.outcome; var oldRegistration=entry.registration; var oldRetirement=entry.retirementProof;
                try {
                    if (prepared!=null) {
                        try { requirePreparation(entry,prepared.preparation()); }
                        catch (IllegalArgumentException invalid) { throw new IOException("Native prepared plan differs from frozen intent",invalid); }
                        entry.registration=prepared.preparation().registration();
                        mergeVote(entry,ParticipantVote.prepared(prepared.position()));
                    }
                    if (outcome!=null) mergeOutcome(entry,outcome);
                    if (retirement!=null) mergeRetirement(entry,retirement);
                } catch (IOException invalid) { entry.vote=oldVote; entry.outcome=oldOutcome; entry.registration=oldRegistration; entry.retirementProof=oldRetirement; throw invalid; }
                if (!Objects.equals(oldVote,entry.vote) || !Objects.equals(oldOutcome,entry.outcome) || !Objects.equals(oldRegistration,entry.registration) || !Objects.equals(oldRetirement,entry.retirementProof)) persist();
                return entry.progress();
            }
        } finally { synchronized (this) { active=false; } }
    }
    private ParticipantIdentity participantIdentity(Entry entry) { return new ParticipantIdentity(homeRoute.groupId(),homeRoute.rangeEpoch(),entry.intent.route().groupId(),initial); }
    private void mergeRetirement(Entry entry,ParticipantFenceProof proof) throws IOException {
        if (observed!=null && observed.phase()==TransactionStatus.Phase.COMMIT && !observed.gcEligible()) throw new IOException("COMMIT retirement requires fully acknowledged consumed HOME");
        if (!participantIdentity(entry).equals(proof.identity()) || entry.retirementProof!=null && !proof.covers(entry.retirementProof)) throw new IOException("Participant retirement proof changed or regressed");
        entry.retirementProof=proof;
    }
    private void mergeVote(Entry entry,ParticipantVote vote) throws IOException {
        if (vote.retired()) { mergeRetirement(entry,vote.retirementProof()); return; }
        if (vote.prepared() && entry.preparation!=null && entry.preparation.negative()) throw new IOException("Positive native proof contradicts frozen negative PREPARE");
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
        if(noEffectReservationState(next))return;
        if (!initial.sameDefinition(next)) throw new IllegalArgumentException("Home definition changed");
        boolean choosingCommit=observed.phase()==TransactionStatus.Phase.PREPARING && next.phase()==TransactionStatus.Phase.COMMIT;
        for (var participant:next.participants()) {
            var entry=entry(participant.groupId());
            var previous=observed.participants().stream().filter(p -> p.groupId().equals(participant.groupId())).findFirst().orElseThrow();
            if (participant.prepared()!=null && (choosingCommit || previous.prepared()==null)
                    && (entry.vote==null || !entry.vote.prepared() || !participant.prepared().equals(entry.vote.position()))) throw new IllegalArgumentException("Home vote lacks local native evidence");
            if (participant.finalized() && !previous.finalized() && entry.outcome==null && (entry.retirementProof==null || observed.phase()!=TransactionStatus.Phase.ABORT)) throw new IllegalArgumentException("Home ACK lacks native terminal outcome or ABORT fence");
        }
        if (next.consumed() && entries.values().stream().anyMatch(e -> e.outcome==null && (next.phase()!=TransactionStatus.Phase.ABORT || e.retirementProof==null))) throw new IllegalArgumentException("Consumption lacks every terminal outcome or ABORT fence");
    }
    /** An expired promotion may retire its original reservation without any participant effects. */
    synchronized boolean noEffectReservationState(TransactionStatus status) {
        var reserved=registrationExpected();
        return reserved!=null && status!=null && reserved.sameDefinition(status) && (status.reserved() || status.phase()==TransactionStatus.Phase.ABORT && status.participants().isEmpty())
                && entries.values().stream().allMatch(e->e.registration==null && e.preparation==null && e.vote==null && e.finalization==null && e.receipt==null && e.outcome==null && e.retirementProof==null);
    }
    private void observe(TransactionStatus status) throws IOException {
        if(!noEffectReservationState(status))requireMonotone(initial,status);
        if (observed!=null) {
            if(observed.reserved()) {try{status.requireSuccessorOf(observed);}catch(IllegalArgumentException invalid){throw new IOException("Reserved HOME recovery changed original origin",invalid);}}
            else requireMonotone(observed,status);
        }
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
        if (!Objects.equals(expected(registration),registrationExpected()) || !initial.equals(next(registration))) throw new IllegalArgumentException("Invalid frozen home registration");
        homeRoute=registration.descriptor();
    }
    static byte[] validateReservationEvidence(TransactionStatus status,RatisOperationRerouteAdmissionRpc.Evidence evidence) {
        var origin=status.origin();if(origin==null){if(evidence!=null)throw new IllegalArgumentException("Ordinary journal cannot have reserved evidence");return null;}
        if(evidence==null || !evidence.intents().isEmpty())throw new IllegalArgumentException("Reserved journal requires immutable reservation-only evidence");
        var original=evidence.original();var source=evidence.sourceDirectory();var target=evidence.targetDirectory();var absence=evidence.absence();
        var route=source.ranges().stream().filter(r->r.groupId().equals(original.groupId())).findFirst().orElseThrow();
        var lineage=new DistributedCdcCheckpoint.Lineage(route.rangeId(),route.groupId(),route.rangeEpoch());
        var request=new RatisTopologyOperationOutcomeRpc.Request(origin.clusterId(),origin.databaseId(),origin.domainId(),origin.source(),origin.operation(),origin.originalReceiptDigest());
        if(original.kind()!=DistributedCommand.Kind.DATA || !source.clusterId().equals(origin.clusterId()) || !source.databaseId().equals(origin.databaseId()) || !source.domainId().equals(origin.domainId())
                || source.schemaEpoch()!=status.schemaEpoch() || target.schemaEpoch()!=status.schemaEpoch() || !Arrays.equals(source.schemaDigest(),status.schemaDigest()) || !Arrays.equals(target.schemaDigest(),status.schemaDigest()) || !Arrays.equals(original.schemaDigest(),source.schemaDigest()) || source.routingPrefixBytes()!=target.routingPrefixBytes() || !source.columns().equals(target.columns())
                || !lineage.equals(origin.source()) || !original.operationId().equals(origin.operation()) || original.rangeEpoch()!=route.rangeEpoch() || !Arrays.equals(original.receiptDigest(),origin.originalReceiptDigest())
                || !target.clusterId().equals(origin.clusterId()) || !target.databaseId().equals(origin.databaseId()) || !target.domainId().equals(origin.domainId()) || !Arrays.equals(target.payloadDigest(),origin.targetDirectoryDigest())
                || !Arrays.equals(RatisOperationRerouteAdmissionRpc.normalizedDigest(original,target),origin.planDigest()) || RatisOperationRerouteAdmissionRpc.targetParticipantCount(original,target)!=origin.targetParticipantCount()
                || !request.equals(absence.request()) || absence.outcome()!=RatisTopologyOperationOutcomeRpc.Outcome.ABSENT || !absence.cut().equals(origin.absenceCut()) || !Arrays.equals(OperatorUpload.sha256(absence.freeze().encode()),origin.absenceCommitment()))throw new IllegalArgumentException("Reserved journal evidence changed original binding");
        byte[] encoded=evidence.encode();if(encoded.length>MAX_RESERVATION_EVIDENCE_BYTES)throw new IllegalArgumentException("Reservation evidence exceeds bound");return encoded;
    }
    public synchronized RatisOperationRerouteAdmissionRpc.Evidence promotionEvidence()throws IOException {
        requireOpen();if(reservationEvidence==null)throw new IOException("Original reservation evidence missing");
        var saved=RatisOperationRerouteAdmissionRpc.Evidence.decode(reservationEvidence);
        return new RatisOperationRerouteAdmissionRpc.Evidence(saved.original(),saved.sourceDirectory(),saved.targetDirectory(),saved.absence(),entries.values().stream().map(e->e.intent).toList());
    }
    synchronized TransactionStatus registrationExpected() {
        return initial.origin()==null ? null : TransactionStatus.reserve(initial.origin(),initial.schemaEpoch(),initial.schemaDigest());
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
        if (operation.sessionId().equals(recoveryNamespace)) throw new IllegalArgumentException("Ordinary home identity overlaps protected recovery namespace");
        if (operation.epoch()<lastHome.epoch() || operation.epoch()==lastHome.epoch() && (!operation.sessionId().equals(lastHome.sessionId()) || operation.sequence()<=lastHome.sequence())) throw new IllegalArgumentException("Home recovery operation identity did not advance");
        if (operation.equals(initial.operation()) || entries.values().stream().anyMatch(e -> e.intent.prepareControl().equals(operation) || e.intent.finalizeControl().equals(operation))) throw new IllegalArgumentException("Home identity overlaps participant/business identity");
        lastHome=operation;
    }
    private Entry entry(UUID group) { var entry=entries.get(group); if (entry==null) throw new IllegalArgumentException("Unregistered participant group"); return entry; }
    private void mergeHomeRetirement(HomeFenceProof proof) throws IOException {
        if (!homes.identity(initial,homeRoute).equals(proof.identity()) || homeRetirementProof!=null && !proof.covers(homeRetirementProof)) throw new IOException("HOME retirement proof identity changed or regressed");
        homeRetirementProof=proof;
    }
    /** Explicitly discarding a consumed saved outcome additionally requires its already-forced HOME fence. */
    public synchronized boolean reclaimable() throws IOException { requireOpen(); return !active && observed!=null && observed.gcEligible() && homeRetirementProof!=null; }
    public void reclaimRetired() throws IOException { reclaimRetired(SessionJournalFiles.FILESYSTEM); }
    void reclaimRetired(SessionJournalFiles.Removal io) throws IOException {
        final byte[] body;
        synchronized (this) {
            requireOpen(); if (!reclaimable()) throw new IOException("Reclamation requires an idle consumed transaction and saved HOME retirement proof");
            body=encode(); active=true;
        }
        try {
            SessionJournalFiles.remove(path,body,MAX_BYTES,io);
            synchronized (this) { reclaimed=true; }
        } catch (IOException | RuntimeException failure) {
            synchronized (this) { fenced=true; }
            if (failure instanceof IOException ioFailure) throw ioFailure; throw new IOException("Transaction reclamation failed; ownership retained",failure);
        } finally { synchronized (this) { active=false; } }
    }
    private void requireOpen() throws IOException { if (reclaimed) throw new IOException("Transaction journal reclaimed"); if (closed || fenced || !lock.isValid()) throw new IOException("Transaction journal closed/fenced; reopen it"); }
    private void idle() throws IOException { requireOpen(); if (homeRetirementProof!=null) throw new RatisHomeRetiredException(homeRetirementProof); if (active) throw new IOException("Transaction journal RPC already active"); }
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
        if (!Arrays.equals(initial.planDigest(),planDigest(entries.values().stream().map(entry -> entry.intent).toList()))) throw new IOException("Frozen transaction plan digest differs from accepted plans/global result order");
        resultCount();
        if (recoverySequence<0 || recoveryNamespace.equals(lastHome.sessionId()) || recoveryNamespace.equals(initial.operation().sessionId())
                || entries.values().stream().anyMatch(e -> recoveryNamespace.equals(e.intent.acceptedBusiness().operationId().sessionId()) || recoveryNamespace.equals(e.intent.prepareControl().sessionId()) || recoveryNamespace.equals(e.intent.finalizeControl().sessionId()))) throw new IOException("Invalid protected recovery namespace");
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
        long reserve=(reservationEvidence==null ? 0 : reservationEvidence.length)+plans+routes+4L*TransactionStatus.MAX_BYTES+HomeFenceProof.MAX_BYTES+65536L
                +entries.size()*(2L*ParticipantFinalization.MAX_BYTES+TransactionStatus.MAX_BYTES+2L*ParticipantFenceProof.MAX_BYTES+512+17);
        if (reserve+32>MAX_BYTES) throw new IOException("Transaction future persistence capacity unavailable");
        if (lastHome.equals(initial.operation()) || controls.contains(lastHome)) throw new IOException("Home operation overlaps participant/business identity");
    }
    private int resultCount() throws IOException {
        return validateResultPlan(entries.values().stream().map(entry -> entry.intent).toList());
    }
    static int validateResultPlan(List<Intent> intents) throws IOException {
        boolean[] assigned=new boolean[256]; int count=0; long bytes=8;
        for (var intent:intents) {
            var result=CommandResult.decode(intent.acceptedBusiness().acceptance().result());
            bytes+=result.encode().length-8L;
            if (bytes>CommandResult.MAX_BYTES) throw new IOException("Global transaction result exceeds byte budget before registration");
            for (int position:intent.resultPositions()) {
                if (assigned[position]) throw new IOException("Duplicate global transaction result slot"); assigned[position]=true; count++;
            }
        }
        if (count>256) throw new IOException("Global transaction result count exceeds bound");
        for (int i=0;i<count;i++) if (!assigned[i]) throw new IOException("Global transaction result slots must exactly cover 0..N-1");
        return count;
    }
    private byte[] encode() throws IOException {
        long size=4+64+4L+(reservationEvidence==null ? 0 : reservationEvidence.length)+4L+initial.encode().length+SessionWindowJournal.routeSize(homeRoute)+32+24+4+(observed==null ? 0 : observed.encode().length)+1;
        if (pending!=null) size+=4L+(pendingRecovery==null ? DistributedCommandCodec.encodedSize(pending.command()) : pendingRecovery.bytes().length)+SessionWindowJournal.routeSize(pending.descriptor());
        size+=positionSize(recoveryReceipt==null ? null : recoveryReceipt.position());
        size+=4+(homeResult==null ? 0 : homeResult.encode().length)+1+4+(homeRetirementProof==null ? 0 : homeRetirementProof.encode().length)+4;
        for (var entry:entries.values()) {
            size+=SessionWindowJournal.routeSize(entry.intent.route())+4L+DistributedCommandCodec.encodedSize(entry.intent.acceptedBusiness())+64+4L+4L*entry.intent.resultPositions().size()+(entry.intent.captureId()==null ? 1 : 17)+4+(entry.registration==null ? 0 : entry.registration.encode().length)+1+voteSize(entry.vote)+4+(entry.finalization==null ? 0 : entry.finalization.finalization().encode().length)+positionSize(entry.receipt)+4+(entry.retirementProof==null ? 0 : entry.retirementProof.encode().length)+1;
            if (entry.outcome!=null) size+=4L+entry.outcome.finalization().encode().length+16+voteSize(entry.outcome.vote());
        }
        if (size+32>MAX_BYTES) throw new IOException("Transaction journal bound exceeded");
        var output=ByteBuffer.allocate((int)size).putInt(MAGIC).put(identity).put(homeIdentity);
        blob(output,reservationEvidence);blob(output,initial.encode()); SessionWindowJournal.route(output,homeRoute); operation(output,lastHome); uuid(output,recoveryNamespace); output.putLong(recoverySequence); blob(output,observed==null ? null : observed.encode());
        output.put((byte)(pending==null ? 0 : pendingRecovery==null ? 1 : 2)); if (pending!=null) { blob(output,pendingRecovery==null ? DistributedCommandCodec.encode(pending.command()) : pendingRecovery.bytes()); SessionWindowJournal.route(output,pending.descriptor()); }
        position(output,recoveryReceipt==null ? null : recoveryReceipt.position());
        blob(output,homeResult==null ? null : homeResult.encode()); output.put((byte)(homeSuperseded ? 1 : 0)); blob(output,homeRetirementProof==null ? null : homeRetirementProof.encode()); output.putInt(entries.size());
        for (var entry:entries.values()) {
            var intent=entry.intent;
            SessionWindowJournal.route(output,intent.route()); blob(output,DistributedCommandCodec.encode(intent.acceptedBusiness())); operation(output,intent.prepareControl()); operation(output,intent.finalizeControl()); output.putInt(intent.resultPositions().size()); for (int position:intent.resultPositions()) output.putInt(position);
            output.put((byte)(intent.captureId()==null ? 0 : 1)); if (intent.captureId()!=null) uuid(output,intent.captureId());
            blob(output,entry.registration==null ? null : entry.registration.encode()); output.put((byte)(entry.preparation==null ? 0 : entry.preparation.negative() ? 2 : 1)); vote(output,entry.vote); blob(output,entry.finalization==null ? null : entry.finalization.finalization().encode()); position(output,entry.receipt); blob(output,entry.retirementProof==null ? null : entry.retirementProof.encode());
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
            reservationEvidence=blob(input,MAX_RESERVATION_EVIDENCE_BYTES,true);
            initial=TransactionStatus.decode(blob(input,TransactionStatus.MAX_BYTES,false)); initial.requireInitial(); if(initial.reserved())throw new IllegalArgumentException("Participant journal requires promoted HOME initial status");
            if(!Arrays.equals(reservationEvidence,validateReservationEvidence(initial,reservationEvidence==null ? null : RatisOperationRerouteAdmissionRpc.Evidence.decode(reservationEvidence))))throw new IOException("Noncanonical reservation evidence");
            homeRoute=SessionWindowJournal.route(input); lastHome=operation(input); recoveryNamespace=new UUID(input.getLong(),input.getLong()); recoverySequence=input.getLong();
            byte[] status=blob(input,TransactionStatus.MAX_BYTES,true); observed=status==null ? null : TransactionStatus.decode(status);
            if (observed!=null && !noEffectReservationState(observed)) requireMonotone(initial,observed);
            int homeKind=input.get() & 255; if (homeKind>2) throw new IOException("Invalid pending home request kind");
            if (homeKind!=0) {
                byte[] command=blob(input,homeKind==2 ? HomeRecovery.MAX_BYTES : DistributedCommandCodec.MAX_ENCODED_BYTES,false); var route=SessionWindowJournal.route(input);
                requireHomeRoute(route);
                if (homeKind==2) { pendingRecovery=homes.restoreRecovery(HomeRecovery.decode(command),route); pending=pendingRecovery.command(); }
                else pending=homes.restore(DistributedCommandCodec.decode(command),route);
            }
            var recoveryPosition=position(input);
            byte[] result=blob(input,8,true); homeResult=result==null ? null : CommandResult.decode(result); homeSuperseded=flag(input);
            var homeRetirement=blob(input,HomeFenceProof.MAX_BYTES,true); if (homeRetirement!=null) mergeHomeRetirement(HomeFenceProof.decode(homeRetirement));
            if (recoveryPosition!=null) {
                if (pendingRecovery==null || homeResult==null) throw new IOException("Protected receipt lacks frozen request/result");
                recoveryReceipt=new HomeRecoveryReceipt(pendingRecovery.recovery(),recoveryPosition,homeResult);
            } else if (pendingRecovery!=null && homeResult!=null) throw new IOException("Protected result lacks retained receipt");
            int count=input.getInt(); if (count<1 || count>TransactionStatus.MAX_PARTICIPANTS) throw new IOException("Invalid journal participant count");
            for (int i=0;i<count;i++) {
                var route=SessionWindowJournal.route(input); var business=DistributedCommandCodec.decode(blob(input,DistributedCommandCodec.MAX_ENCODED_BYTES,false));
                var prepareControl=operation(input); var finalizeControl=operation(input); int slots=input.getInt();
                if (slots<0 || slots>256 || 4L*slots>input.remaining()) throw new IOException("Invalid participant result slot count");
                var positions=new ArrayList<Integer>(slots); for (int slot=0;slot<slots;slot++) positions.add(input.getInt());
                UUID captureId=flag(input) ? new UUID(input.getLong(),input.getLong()) : null;
                var intent=new Intent(route,business,prepareControl,finalizeControl,positions,captureId); var entry=new Entry(intent);
                if (entries.putIfAbsent(route.groupId(),entry)!=null) throw new IOException("Duplicate journal participant");
                byte[] registration=blob(input,TransactionStatus.MAX_BYTES,true); entry.registration=registration==null ? null : TransactionStatus.decode(registration);
                if (entry.registration!=null) requirePreparation(entry,preparation(intent,entry.registration));
                int preparationKind=input.get() & 255; if (preparationKind>2) throw new IOException("Invalid frozen PREPARE kind");
                if (preparationKind!=0) {
                    if (entry.registration==null) throw new IOException("Frozen preparation has no registration");
                    entry.preparation=data.restoreParticipant(preparation(intent,entry.registration),route,preparationKind==2);
                }
                var savedVote=vote(input); if (savedVote!=null) mergeVote(entry,savedVote); byte[] finalization=blob(input,ParticipantFinalization.MAX_BYTES,true);
                if (finalization!=null) entry.finalization=data.restoreFinalization(ParticipantFinalization.decode(finalization),route);
                entry.receipt=position(input);
                var retirement=blob(input,ParticipantFenceProof.MAX_BYTES,true); if (retirement!=null) mergeRetirement(entry,ParticipantFenceProof.decode(retirement));
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
            if (observed!=null && !initial.sameDefinition(observed) && !noEffectReservationState(observed))throw new IOException("Reserved recovery journal has participant effects");
            if (input.hasRemaining()) throw new IOException("Trailing transaction journal bytes");
            if (pending==null || pendingRecovery==null && !pending.command().operationId().equals(lastHome) || !initial.sameDefinition(next(pending)) && !noEffectReservationState(next(pending))) throw new IOException("Invalid journal home request");
            if (expected(pending)==null) { if (!next(pending).equals(initial)) throw new IOException("Invalid journal registration"); }
            else next(pending).requireSuccessorOf(expected(pending));
            if (pendingRecovery!=null) {
                var op=pendingRecovery.recovery().operation();
                if (!op.sessionId().equals(recoveryNamespace) || op.epoch()!=0 || recoverySequence<=0 || op.sequence()!=recoverySequence-1) throw new IOException("Protected recovery high-water identity changed");
                if (pendingRecovery.recovery().action()==HomeRecovery.Action.ACK) {
                    var recovery=pendingRecovery.recovery(); var entry=entry(recovery.proof().participant());
                    if (recovery.proof().fenced()) recovery.requireFence(entry.retirementProof);
                    else recovery.requireOutcome(entry.outcome);
                }
            } else if (expected(pending)!=null && protectedTransition(expected(pending),next(pending))) throw new IOException("Unprotected recovery journal transition");
            if (homeSuperseded && (homeResult!=null || !homeSupersedesPending())) throw new IOException("Home request retirement has no newer observed proof");
            if (homeResult!=null && homeResult.status()==CommandResult.Status.SUCCESS) {
                var successful=next(pending);
                if (observed==null) throw new IOException("Successful home request lacks monotone observed state");
                requireMonotone(successful,observed);
            }
        } catch (RuntimeException invalid) { throw new IOException("Invalid transaction journal",invalid); }
    }
    private static long voteSize(ParticipantVote vote) { return vote==null ? 1 : vote.prepared() ? 17 : 1; }
    private static long positionSize(ApplyPosition position) { return position==null ? 1 : 17; }
    private static void vote(ByteBuffer output,ParticipantVote vote) {
        if (vote!=null && vote.retired()) throw new IllegalArgumentException("Retirement is stored separately from the native vote");
        output.put((byte)(vote==null ? 0 : vote.prepared() ? 1 : 2));
        if (vote!=null && vote.prepared()) output.putLong(vote.position().term()).putLong(vote.position().index());
    }
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
