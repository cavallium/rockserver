package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;
import java.util.function.LongSupplier;
import org.apache.ratis.client.RaftClient;

/** Logical bootstrap over already-authorized placements. Readiness observations are rechecked after reopen, never restored as health. */
public final class RatisDatabaseBootstrap implements AutoCloseable {
    public enum Phase { PREFLIGHT, SCHEMA, GENESIS, CLOCK, PROPAGATE, VERIFY, READY, CONFLICT, EXPIRED }
    public record Progress(Phase phase,String directoryDigest,SessionAuthority target,int checked,int total,Map<UUID,String> failures) {
        public Progress { failures=Map.copyOf(failures); }
    }
    private final RatisNodePlan plan; private final RatisRoutedClient routed; private final LongSupplier time;
    private final byte[] scope; private final RatisCatalogue catalogue;
    private final DistributedSchema schema; private final RatisSchemaCatalogue schemas;
    private RatisSchemaPublication publication;
    private final FileChannel lockChannel; private final FileLock lock;
    private RatisCatalogueGenesis genesis; private RatisCatalogueClock clock; private RatisSessionPropagation propagation;
    private record State(Phase phase,SessionAuthority target,BitSet checked,int cursor) {}
    private State state=new State(Phase.PREFLIGHT,null,new BitSet(),0);
    private boolean busy,closing,closed;

    /** Holds no placement-creation authority itself. Clients remain borrowed; root exists on durable storage. */
    public RatisDatabaseBootstrap(Path root,RatisNodePlan plan,RaftClient catalogueClient,RatisRoutedClient routed,
            long initialTime,LongSupplier time,int maxRanges) throws IOException {
        this(root,plan,catalogueClient,routed,null,initialTime,time,maxRanges);
    }
    /** Publishes and freshly verifies the exact content-bound schema before creating the directory/ledger pair. */
    public RatisDatabaseBootstrap(Path root,RatisNodePlan plan,RaftClient catalogueClient,RatisRoutedClient routed,
            DistributedSchema schema,long initialTime,LongSupplier time,int maxRanges) throws IOException {
        this.plan=Objects.requireNonNull(plan); this.routed=Objects.requireNonNull(routed); this.time=Objects.requireNonNull(time); scope=plan.directory().payloadDigest();
        this.schema=schema; if (schema!=null) schema.requireMatches(plan.directory());
        if (initialTime<0 || plan.directory().generation()!=0 || plan.directory().ranges().size()>maxRanges || maxRanges<1 || maxRanges>RangeDirectoryCodec.MAX_RANGES) {
            throw new IllegalArgumentException("Invalid initial bootstrap scope/budget");
        }
        requireCachedScope(); Path directory=root.toAbsolutePath(); if (!Files.isDirectory(directory)) throw new IOException("Bootstrap root is absent");
        var c=plan.catalogue(); catalogue=new RatisCatalogue(catalogueClient,plan.directory().clusterId(),plan.directory().databaseId(),c.groupId(),c.epoch(),c.schemaDigest(),c.directoryColumn());
        schemas=schema==null ? null : new RatisSchemaCatalogue(catalogueClient,plan.directory().clusterId(),plan.directory().databaseId(),c.groupId(),c.epoch(),c.schemaDigest(),c.directoryColumn());
        var epochs=new RatisSessionEpochs(catalogueClient,plan.directory().clusterId(),plan.directory().databaseId(),c.groupId(),c.epoch(),c.schemaDigest(),c.sessionsColumn());
        lockChannel=FileChannel.open(directory.resolve("bootstrap.lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE);
        FileLock acquired=null;
        try {
            try { acquired=lockChannel.tryLock(); } catch (OverlappingFileLockException owned) { throw new IOException("Bootstrap root already owned",owned); }
            if (acquired==null) throw new IOException("Bootstrap root already owned"); lock=acquired;
            if (schema!=null) publication=new RatisSchemaPublication(directory.resolve("schema.bin"),catalogueClient,plan,schema);
            genesis=new RatisCatalogueGenesis(directory.resolve("genesis.bin"),catalogueClient,plan,initialTime);
            clock=new RatisCatalogueClock(directory.resolve("clock.bin"),epochs);
            Path clocks=Files.createDirectories(directory.resolve("ranges")); SessionJournalFiles.force(clocks); SessionJournalFiles.force(directory);
            propagation=new RatisSessionPropagation(clocks,epochs,routed,maxRanges);
        } catch (IOException | RuntimeException | Error failure) {
            try { if (propagation!=null) propagation.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
            try { if (clock!=null) clock.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
            try { if (genesis!=null) genesis.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
            try { if (publication!=null) publication.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
            try { if (acquired!=null) acquired.release(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
            try { lockChannel.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }
    /** Historical progress for this frozen scope, not a permanent availability assertion. */
    public synchronized Progress progress() { return progress(state,Map.of()); }
    /** Bounded participant checks or one mutation stage. Transport discovery/catalogue reads are additional work. */
    public Progress step(int budget) throws IOException {
        if (budget<1 || budget>32) throw new IllegalArgumentException("Invalid bootstrap work budget");
        final State before;
        synchronized (this) {
            if (closed || closing || !lock.isValid()) throw new IOException("Bootstrap owner is closed");
            if (busy) throw new IllegalStateException("Bootstrap work is active");
            if (state.phase()==Phase.READY) state=new State(Phase.VERIFY,state.target(),new BitSet(),0);
            busy=true; before=state;
        }
        try {
            requireCachedScope(); var failures=new LinkedHashMap<UUID,String>();
            Phase phase=before.phase(); SessionAuthority target=before.target(); BitSet checked=(BitSet)before.checked().clone(); int cursor=before.cursor();
            if (phase==Phase.CONFLICT || phase==Phase.EXPIRED) return progress(before,failures);
            if (phase!=Phase.PREFLIGHT && phase!=Phase.SCHEMA && phase!=Phase.GENESIS) requirePublishedScope();
            switch (phase) {
                case PREFLIGHT, VERIFY -> {
                    var ranges=plan.directory().ranges(); int visits=0, work=0;
                    while (visits++<ranges.size() && work<budget) {
                        int index=cursor; cursor=(cursor+1)%ranges.size(); if (checked.get(index)) continue;
                        work++; var descriptor=ranges.get(index);
                        try {
                            var state=routed.stateView(descriptor.groupId()); state.requireMatches(plan.directory(),descriptor);
                            if (state.applied()==null || state.applied().term()==0) throw new IllegalStateException("Current-term native apply is absent");
                            if (phase==Phase.VERIFY) state.requireServing(target);
                            checked.set(index);
                        } catch (IOException | IllegalStateException | IllegalArgumentException failure) {
                            String message=String.valueOf(failure.getMessage()); failures.put(descriptor.groupId(),message.substring(0,Math.min(512,message.length())));
                        }
                    }
                    if (checked.cardinality()==ranges.size()) {
                        if (phase==Phase.PREFLIGHT) phase=schema==null ? Phase.GENESIS : Phase.SCHEMA;
                        else { requirePublishedScope(); phase=Phase.READY; }
                    }
                }
                case SCHEMA -> {
                    var outcome=publication.execute();
                    if (outcome.status()==CommandResult.Status.SUCCESS || outcome.status()==CommandResult.Status.CONFLICT) {
                        requirePublishedSchema(); phase=Phase.GENESIS;
                    } else phase=Phase.EXPIRED;
                }
                case GENESIS -> {
                    requirePublishedSchema();
                    var outcome=genesis.execute();
                    phase=outcome.status()==CommandResult.Status.SUCCESS ? Phase.CLOCK : outcome.status()==CommandResult.Status.CONFLICT ? Phase.CONFLICT : Phase.EXPIRED;
                }
                case CLOCK -> {
                    var tick=clock.advance(time.getAsLong());
                    if (tick.outcome()==RatisCatalogueClock.Outcome.OBSERVED || tick.outcome()==RatisCatalogueClock.Outcome.APPLIED) { target=tick.authority(); phase=Phase.PROPAGATE; }
                }
                case PROPAGATE -> {
                    var priorRound=propagation.progress();
                    if (priorRound.target()==null || priorRound.complete() && !propagation.covers(target)) propagation.begin();
                    failures.putAll(propagation.step(budget).failures());
                    if (propagation.covers(target)) { phase=Phase.VERIFY; checked.clear(); cursor=0; }
                }
                default -> throw new IllegalStateException("Unexpected bootstrap phase");
            }
            requireCachedScope(); var next=new State(phase,target,checked,cursor);
            synchronized (this) { state=next; }
            return progress(next,failures);
        } finally { synchronized (this) { busy=false; } }
    }
    private void requireCachedScope() throws IOException {
        if (!Arrays.equals(scope,routed.directory().payloadDigest())) throw new IOException("Bootstrap routed directory changed; frozen intent cannot be rebased");
    }
    private void requirePublishedScope() throws IOException {
        var current=RatisRoutedClient.rpc(() -> catalogue.read(plan.directory().domainId()));
        if (current==null || !Arrays.equals(scope,current.payloadDigest())) throw new IOException("Published directory differs from frozen bootstrap scope");
        requirePublishedSchema();
    }
    private void requirePublishedSchema() throws IOException {
        if (schema==null) return;
        var current=RatisRoutedClient.rpc(() -> schemas.read(schema.digest()));
        if (current==null || !Arrays.equals(DistributedSchemaCodec.encode(schema),DistributedSchemaCodec.encode(current))) {
            throw new IOException("Published schema differs from frozen bootstrap content");
        }
    }
    private Progress progress(State state,Map<UUID,String> failures) {
        return new Progress(state.phase(),HexFormat.of().formatHex(scope),state.target(),state.checked().cardinality(),plan.directory().ranges().size(),failures);
    }
    @Override public synchronized void close() throws IOException {
        if (closed) return; if (busy) throw new IOException("Bootstrap work is active"); closing=true;
        propagation.close(); clock.close(); genesis.close(); if (publication!=null) publication.close();
        try { if (lock.isValid()) lock.release(); } finally { lockChannel.close(); }
        closed=true;
    }
}
