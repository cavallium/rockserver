package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;
import java.util.function.LongSupplier;
import org.apache.ratis.client.RaftClient;

/** Frozen database bootstrap. Every DATA domain shares one ledger and must pass fresh readiness. */
public final class RatisDatabaseBootstrap implements AutoCloseable {
    public static final int MAX_DOMAINS=DatabaseDomainRoster.MAX_DOMAINS,MAX_SCOPE_BYTES=16*1024*1024;
    public enum Phase { PREFLIGHT, SCHEMA, GENESIS, CLOCK, PROPAGATE, VERIFY, READY, CONFLICT, EXPIRED }
    public record Domain(RatisNodePlan plan,RatisRoutedClient routed,DistributedSchema schema,byte[] allocationWitness) {
        public Domain {
            Objects.requireNonNull(plan);Objects.requireNonNull(routed);
            if(schema==null && allocationWitness!=null)throw new IllegalArgumentException("Allocation witness requires schema content");
            RatisColumnAllocation.counter(allocationWitness);if(schema!=null)schema.requireMatches(plan.directory());
            allocationWitness=allocationWitness==null ? null : allocationWitness.clone();
        }
        @Override public byte[] allocationWitness(){return allocationWitness==null ? null : allocationWitness.clone();}
    }
    public record Progress(Phase phase,String directoryDigest,SessionAuthority target,int checked,int total,Map<UUID,String> failures) {
        public Progress { failures=Map.copyOf(failures); }
    }
    private static final class DomainWork implements AutoCloseable {
        final Domain input;final byte[] scope;final RatisCatalogue catalogue;final RatisSchemaCatalogue schemas;
        RatisSchemaPublication publication;RatisCatalogueGenesis genesis;RatisSessionPropagation propagation;
        DomainWork(Domain input,RaftClient client){this.input=input;scope=input.plan().directory().payloadDigest();var p=input.plan();var c=p.catalogue();
            catalogue=new RatisCatalogue(client,p.directory().clusterId(),p.directory().databaseId(),c.groupId(),c.epoch(),c.schemaDigest(),c.directoryColumn());
            schemas=input.schema()==null ? null : new RatisSchemaCatalogue(client,p.directory().clusterId(),p.directory().databaseId(),c.groupId(),c.epoch(),c.schemaDigest(),c.directoryColumn());}
        @Override public void close()throws IOException {if(propagation!=null)propagation.close();if(genesis!=null)genesis.close();if(publication!=null)publication.close();}
    }
    private record RangeWork(DomainWork domain,RangeDescriptor range) {}
    private record State(Phase phase,SessionAuthority target,BitSet checked,int cursor,int domain) {}
    private final List<DomainWork> domains;private final List<RangeWork> ranges;private final LongSupplier time;private final byte[] scope;
    private final DatabaseDomainRoster beforeRoster,afterRoster;private final RatisDatabaseDomainRoster rosters;
    private final FileChannel lockChannel;private final FileLock lock;private RatisCatalogueClock clock;
    private State state=new State(Phase.PREFLIGHT,null,new BitSet(),0,0);private boolean busy,closing,closed;

    /** Existing ledger is an immutable publication witness. Null creates it once at initialTime. All clients remain borrowed. */
    public RatisDatabaseBootstrap(Path root,List<Domain> inputs,RaftClient client,SessionEpochLedger existingLedger,DatabaseDomainRoster expectedRoster,
                                  long initialTime,LongSupplier time,int maxRanges)throws IOException {
        Objects.requireNonNull(client);this.time=Objects.requireNonNull(time);inputs=List.copyOf(inputs);
        if(inputs.isEmpty() || inputs.size()>MAX_DOMAINS || initialTime<0 || maxRanges<1 || maxRanges>RangeDirectoryCodec.MAX_RANGES)throw new IllegalArgumentException("Invalid database bootstrap scope/budget");
        var first=inputs.getFirst().plan();var directory=first.directory();var c=first.catalogue();
        if(!client.getGroupId().getUuid().equals(c.groupId()) || existingLedger!=null && (!existingLedger.clusterId().equals(directory.clusterId()) || !existingLedger.databaseId().equals(directory.databaseId())))throw new IllegalArgumentException("Wrong shared catalogue/ledger identity");
        var domainIds=new HashSet<UUID>();var groups=new HashSet<UUID>();var rangeIds=new HashSet<UUID>();var columns=new HashSet<UUID>();var names=new HashSet<String>();var apiIds=new HashSet<Long>();
        var metadataGroups=new HashSet<UUID>();metadataGroups.add(c.groupId());var metadataColumns=new HashSet<UUID>(List.of(c.directoryColumn(),c.sessionsColumn()));
        for(var home:first.homes()){metadataColumns.add(home.column());for(var range:home.directory().ranges())metadataGroups.add(range.groupId());}
        long bytes=0;byte[] nextAllocation=null;boolean hasSchema=false;var work=new ArrayList<DomainWork>();var allRanges=new ArrayList<RangeWork>();
        for(var input:inputs){var p=input.plan();var d=p.directory();var pc=p.catalogue();
            if(d.generation()!=0 || !d.clusterId().equals(directory.clusterId()) || !d.databaseId().equals(directory.databaseId())
                    || !p.nodeId().equals(first.nodeId()) || !p.nodeIncarnation().equals(first.nodeIncarnation())
                    || !pc.groupId().equals(c.groupId()) || pc.epoch()!=c.epoch() || !Arrays.equals(pc.schemaDigest(),c.schemaDigest())
                    || !pc.directoryColumn().equals(c.directoryColumn()) || !pc.sessionsColumn().equals(c.sessionsColumn()) || !pc.replicas().equals(c.replicas())
                    || !sameHomes(first,p) || !domainIds.add(d.domainId()))throw new IllegalArgumentException("Domains changed shared bootstrap identity");
            for(var column:d.columns())if(metadataColumns.contains(column) || !columns.add(column) || !names.add(p.columns().get(column)))throw new IllegalArgumentException("Duplicate DATA column/CF identity");
            if(columns.size()>RangeDirectoryCodec.MAX_COLUMNS)throw new IllegalArgumentException("Aggregate column budget exceeded");
            bytes+=RangeDirectoryCodec.encode(d).length;
            if(input.schema()!=null){if(hasSchema && !Arrays.equals(nextAllocation,input.allocationWitness()))throw new IllegalArgumentException("Domain allocation witnesses do not form one frozen publication sequence");bytes+=DistributedSchemaCodec.encode(input.schema()).length;long maximum=RatisColumnAllocation.counter(input.allocationWitness());for(var column:input.schema().columns()){if(!apiIds.add(column.apiId()))throw new IllegalArgumentException("Duplicate DATA API identity");maximum=Math.max(maximum,column.apiId());}nextAllocation=RatisColumnAllocation.counterValue(maximum);hasSchema=true;}
            if(bytes>MAX_SCOPE_BYTES)throw new IllegalArgumentException("Aggregate bootstrap metadata budget exceeded");
            var domain=new DomainWork(input,client);work.add(domain);
            if(domain.schemas!=null)domain.schemas.prepare(new OperationId(new UUID(0,0),0,0),input.schema(),input.allocationWitness(),Map.of(),Map.of(),Map.of());
            for(var range:d.ranges()){if(!rangeIds.add(range.rangeId()) || !groups.add(range.groupId()) || metadataGroups.contains(range.groupId()))throw new IllegalArgumentException("Duplicate DATA group identity");allRanges.add(new RangeWork(domain,range));}
        }
        if(allRanges.size()>maxRanges)throw new IllegalArgumentException("Aggregate range budget exceeded");
        beforeRoster=expectedRoster; afterRoster=expectedRoster==null ? DatabaseDomainRoster.initial(directory.clusterId(),directory.databaseId(),domainIds) : expectedRoster.add(domainIds);
        RatisDatabaseDomainRoster.requireNamespace(afterRoster,directory.clusterId(),directory.databaseId()); rosters=new RatisDatabaseDomainRoster(client,directory.clusterId(),directory.databaseId(),c);
        domains=List.copyOf(work);ranges=List.copyOf(allRanges);requireCachedScope();scope=binding(inputs,existingLedger,initialTime,beforeRoster,afterRoster);
        Path path=root.toAbsolutePath();if(!Files.isDirectory(path))throw new IOException("Bootstrap root is absent");
        var epochs=new RatisSessionEpochs(client,directory.clusterId(),directory.databaseId(),c.groupId(),c.epoch(),c.schemaDigest(),c.sessionsColumn());
        lockChannel=FileChannel.open(path.resolve("bootstrap.lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE);FileLock acquired=null;
        try {
            try{acquired=lockChannel.tryLock();}catch(OverlappingFileLockException owned){throw new IOException("Bootstrap root already owned",owned);}if(acquired==null)throw new IOException("Bootstrap root already owned");lock=acquired;
            var identity=path.resolve("bootstrap.identity");if(Files.exists(identity)){if(!Arrays.equals(scope,SessionJournalFiles.read(identity,64)))throw new IOException("Frozen bootstrap domains changed");}else SessionJournalFiles.write(identity,scope,64);
            var parent=Files.createDirectories(path.resolve("domains"));SessionJournalFiles.force(parent);SessionJournalFiles.force(path);
            var ledger=existingLedger==null ? SessionEpochLedger.initial(directory.clusterId(),directory.databaseId(),initialTime) : existingLedger;
            for(int i=0;i<domains.size();i++){var domain=domains.get(i);var input=domain.input;var journals=Files.createDirectories(parent.resolve(input.plan().directory().domainId().toString()));SessionJournalFiles.force(journals);SessionJournalFiles.force(parent);
                if(input.schema()!=null)domain.publication=new RatisSchemaPublication(journals.resolve("schema.bin"),client,input.plan(),input.schema(),input.allocationWitness(),Map.of(),Map.of(),Map.of());
                domain.genesis=new RatisCatalogueGenesis(journals.resolve("genesis.bin"),client,input.plan(),initialTime,i==0 && existingLedger==null ? null : ledger,i==0 ? beforeRoster : afterRoster,afterRoster);
                var clocks=Files.createDirectories(journals.resolve("ranges"));SessionJournalFiles.force(clocks);SessionJournalFiles.force(journals);domain.propagation=new RatisSessionPropagation(clocks,epochs,input.routed(),maxRanges);
            }
            clock=new RatisCatalogueClock(path.resolve("clock.bin"),epochs);
        }catch(IOException|RuntimeException|Error failure){for(var domain:domains)try{domain.close();}catch(IOException cleanup){failure.addSuppressed(cleanup);}try{if(clock!=null)clock.close();}catch(IOException cleanup){failure.addSuppressed(cleanup);}try{if(acquired!=null)acquired.release();}catch(IOException cleanup){failure.addSuppressed(cleanup);}try{lockChannel.close();}catch(IOException cleanup){failure.addSuppressed(cleanup);}throw failure;}
    }
    private static boolean sameHomes(RatisNodePlan first,RatisNodePlan next){
        if(first.homes().size()!=next.homes().size())return false;
        for(var home:first.homes()){var other=next.home(home.role());if(other==null || !home.column().equals(other.column()) || !home.physicalName().equals(other.physicalName()) || !Arrays.equals(home.directory().payloadDigest(),other.directory().payloadDigest()))return false;}return true;
    }
    private static byte[] binding(List<Domain> domains,SessionEpochLedger ledger,long initialTime,DatabaseDomainRoster beforeRoster,DatabaseDomainRoster afterRoster)throws IOException {
        var bytes=new ByteArrayOutputStream();var out=new DataOutputStream(bytes);out.writeLong(ledger==null ? initialTime : 0);out.writeInt(domains.size());
        for(var domain:domains){out.write(domain.plan().identityDigest());out.write(domain.plan().directory().payloadDigest());out.write(domain.schema()==null ? new byte[32] : domain.schema().digest());var witness=domain.allocationWitness();out.writeInt(witness==null ? -1 : witness.length);if(witness!=null)out.write(witness);}
        if(ledger!=null)out.write(ledger.encode());out.writeInt(beforeRoster==null ? -1 : beforeRoster.encode().length);if(beforeRoster!=null)out.write(beforeRoster.encode());out.write(afterRoster.encode());return OperatorUpload.sha256(bytes.toByteArray());
    }
    public synchronized Progress progress(){return progress(state,Map.of());}
    /** Bounded participant work or one immutable publication; catalogue observations are additional work. */
    public Progress step(int budget)throws IOException {
        if(budget<1 || budget>32)throw new IllegalArgumentException("Invalid bootstrap work budget");final State before;
        synchronized(this){if(closed || closing || !lock.isValid())throw new IOException("Bootstrap owner is closed");if(busy)throw new IllegalStateException("Bootstrap work is active");if(state.phase()==Phase.READY)state=new State(Phase.VERIFY,state.target(),new BitSet(),0,0);busy=true;before=state;}
        try {requireCachedScope();var failures=new LinkedHashMap<UUID,String>();var phase=before.phase();var target=before.target();var checked=(BitSet)before.checked().clone();int cursor=before.cursor(),index=before.domain();
            if(phase==Phase.CONFLICT || phase==Phase.EXPIRED)return progress(before,failures);
            if(phase!=Phase.PREFLIGHT && phase!=Phase.SCHEMA && phase!=Phase.GENESIS)requirePublishedScope();
            switch(phase){
                case PREFLIGHT,VERIFY -> {int visits=0,work=0;while(visits++<ranges.size() && work<budget){int next=cursor;cursor=(cursor+1)%ranges.size();if(checked.get(next))continue;work++;var entry=ranges.get(next);try{var view=entry.domain.input.routed().stateView(entry.range.groupId());view.requireMatches(entry.domain.input.plan().directory(),entry.range);if(view.applied()==null || view.applied().term()==0)throw new IllegalStateException("Current-term native apply is absent");if(phase==Phase.VERIFY)view.requireServing(target);checked.set(next);}catch(IOException|IllegalStateException|IllegalArgumentException failure){var message=String.valueOf(failure.getMessage());failures.put(entry.range.groupId(),message.substring(0,Math.min(512,message.length())));}}
                    if(checked.cardinality()==ranges.size()){if(phase==Phase.PREFLIGHT){phase=domains.stream().anyMatch(d->d.publication!=null) ? Phase.SCHEMA : Phase.GENESIS;index=0;}else{requirePublishedScope();phase=Phase.READY;}}}
                case SCHEMA -> {while(index<domains.size() && domains.get(index).publication==null)index++;var domain=domains.get(index);var outcome=domain.publication.execute();if(outcome.status()==CommandResult.Status.SUCCESS || outcome.status()==CommandResult.Status.CONFLICT){requirePublishedSchema(domain);index++;while(index<domains.size() && domains.get(index).publication==null)index++;if(index==domains.size()){phase=Phase.GENESIS;index=0;}}else phase=Phase.EXPIRED;}
                case GENESIS -> {var domain=domains.get(index);requirePublishedSchema(domain);var outcome=domain.genesis.execute();if(outcome.status()==CommandResult.Status.SUCCESS){if(++index==domains.size()){phase=Phase.CLOCK;index=0;}}else phase=outcome.status()==CommandResult.Status.CONFLICT ? Phase.CONFLICT : Phase.EXPIRED;}
                case CLOCK -> {var tick=clock.advance(time.getAsLong());if(tick.outcome()==RatisCatalogueClock.Outcome.OBSERVED || tick.outcome()==RatisCatalogueClock.Outcome.APPLIED){target=tick.authority();phase=Phase.PROPAGATE;index=0;}}
                case PROPAGATE -> {var propagation=domains.get(index).propagation;var prior=propagation.progress();if(prior.target()==null || prior.complete() && !propagation.covers(target))propagation.begin();failures.putAll(propagation.step(budget).failures());index=(index+1)%domains.size();boolean complete=true;for(var domain:domains)if(!domain.propagation.covers(target)){complete=false;break;}if(complete){phase=Phase.VERIFY;checked.clear();cursor=0;index=0;}}
                default -> throw new IllegalStateException("Unexpected bootstrap phase");
            }
            requireCachedScope();var next=new State(phase,target,checked,cursor,index);synchronized(this){state=next;}return progress(next,failures);
        }finally{synchronized(this){busy=false;}}
    }
    private void requireCachedScope()throws IOException {for(var domain:domains)if(!Arrays.equals(domain.scope,domain.input.routed().directory().payloadDigest()))throw new IOException("Bootstrap routed directory changed; frozen intent cannot be rebased");}
    private void requirePublishedScope()throws IOException {var view=rosters.readComplete();if(!view.roster().equals(afterRoster))throw new IOException("Published database roster differs from frozen bootstrap scope");for(var domain:domains){var current=RatisRoutedClient.rpc(()->domain.catalogue.read(domain.input.plan().directory().domainId()));if(current==null || !Arrays.equals(domain.scope,current.payloadDigest()))throw new IOException("Published directory differs from frozen bootstrap scope");requirePublishedSchema(domain);}}
    private void requirePublishedSchema(DomainWork domain)throws IOException {if(domain.schemas==null)return;var schema=domain.input.schema();var current=RatisRoutedClient.rpc(()->domain.schemas.read(schema.digest()));if(current==null || !Arrays.equals(DistributedSchemaCodec.encode(schema),DistributedSchemaCodec.encode(current)))throw new IOException("Published schema differs from frozen bootstrap content");}
    private Progress progress(State state,Map<UUID,String> failures){return new Progress(state.phase(),HexFormat.of().formatHex(scope),state.target(),state.checked().cardinality(),ranges.size(),failures);}
    @Override public synchronized void close()throws IOException {if(closed)return;if(busy)throw new IOException("Bootstrap work is active");closing=true;for(var domain:domains)domain.close();if(clock!=null)clock.close();try{if(lock.isValid())lock.release();}finally{lockChannel.close();}closed=true;}
}
