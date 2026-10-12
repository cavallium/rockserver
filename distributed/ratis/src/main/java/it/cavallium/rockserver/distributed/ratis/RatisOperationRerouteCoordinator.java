package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;

/** Session-owned reroute work on an already admitted mutation worker. Transports and scheduler are borrowed. */
final class RatisOperationRerouteCoordinator implements AutoCloseable {
    private static final int MAGIC=0x52524353,MAX_STATE_BYTES=155+HomeFenceProof.MAX_BYTES+NativeSourceOutcomeCredit.Request.MAX_BYTES+RatisSourceOutcomeRecord.MAX_BYTES+CommandResult.MAX_BYTES;
    private static final int MAX_ACTIVE=1;private static final long CHARGE=6L*RatisSourceOutcomeRpc.MAX_REQUEST_BYTES+4L*RatisOperationRerouteJournal.MAX_BYTES+3L*MAX_STATE_BYTES+4L*RatisSessionClient.MAX_SOURCE_DIRECTORY_BYTES+2L*RatisTransactionPlan.MAX_BYTES,MAX_BYTES=CHARGE;
    private final Path root;private final DistributedSchema schema;private final RatisSessionEpochs epochs;private final RatisRoutedClient routed;private final RatisTransactionStatuses homes;private final RatisAtomicClient atomic;private final RatisRoutedClient.SourceAckDirectoryReader ackDirectory;
    private final SessionEpochLedger.Grant business;private final RatisSessionClient.Limits limits;private final byte[] binding;
    private int active,rerouting;private long bytes;private boolean closed;
    RatisOperationRerouteCoordinator(Path root,DistributedSchema schema,RatisSessionEpochs epochs,RatisRoutedClient routed,RatisTransactionStatuses homes,RWScheduler scheduler,SessionEpochLedger.Grant business,RatisSessionClient.Limits limits,RatisRoutedClient.SourceAckDirectoryReader ackDirectory)throws IOException {
        this.root=root.toAbsolutePath().normalize();this.schema=Objects.requireNonNull(schema);this.epochs=Objects.requireNonNull(epochs);this.routed=Objects.requireNonNull(routed);this.homes=Objects.requireNonNull(homes);this.business=Objects.requireNonNull(business);this.limits=Objects.requireNonNull(limits);this.ackDirectory=Objects.requireNonNull(ackDirectory);
        binding=OperatorUpload.sha256(ByteBuffer.allocate(136).put(schema.digest()).put(routed.sessionIdentityDigest()).put(homes.identityDigest()).putLong(business.epoch()).putLong(business.sessionId().getMostSignificantBits()).putLong(business.sessionId().getLeastSignificantBits()).putLong(business.issuedAtMillis()).putLong(business.expiresAtMillis()).array());
        Files.createDirectories(this.root);SessionJournalFiles.force(this.root);SessionJournalFiles.force(this.root.getParent());atomic=new RatisAtomicClient(schema,homes,routed,scheduler,RatisAtomicClient.Limits.defaults());
    }
    private synchronized void enter()throws IOException {if(closed)throw new IOException("Reroute coordinator closed");active++;}
    private synchronized void enterReroute()throws IOException {if(closed || rerouting>=MAX_ACTIVE || CHARGE>MAX_BYTES-bytes)throw new IOException("Reroute coordinator at bounded ownership capacity");rerouting++;bytes+=CHARGE;}
    private synchronized void leave(boolean reroute){active--;if(reroute){rerouting--;bytes-=CHARGE;}}
    private Path operationPath(OperationId operation)throws IOException {
        if(!operation.sessionId().equals(business.sessionId()) || operation.epoch()!=business.epoch())throw new IOException("Reroute changed original session grant");
        return root.resolve(Long.toString(operation.sequence()));
    }
    CommandResult execute(RatisRoutedClient.PreparedCommand original,RequestContext context,RatisAtomicClient.InlineCheck check)throws IOException {
        enter();boolean reroute=false;try {
            check.check();var directory=operationPath(original.command().operationId());var input=directory.resolve("input.bin");
            if(!Files.exists(input,LinkOption.NOFOLLOW_LINKS)) {
                try{return routed.execute(original);}catch(RatisRoutedClient.OriginalOperationAbsent absent) {
                    enterReroute();reroute=true;
                    if(RatisOperationRerouteAdmissionRpc.acknowledgementOnly(original.command()))throw new IOException("Absent original ACK has no reroute authority",absent);
                    Files.createDirectories(directory);SessionJournalFiles.force(directory);SessionJournalFiles.force(root);
                    try(var selected=RatisOperationRerouteJournal.select(input,binding,absent.originalDirectory(),original.command(),routed.directory(schema),absent.evidence(),Duration.ofSeconds(30))){}
                }
            }
            if(!reroute){enterReroute();reroute=true;}
            try(var job=new Job(directory);var selected=RatisOperationRerouteJournal.reopen(input,binding)) {
                if(!Arrays.equals(DistributedCommandCodec.encode(original.command()),DistributedCommandCodec.encode(selected.originalCommand())))throw new IOException("Reroute changed original input");
                return resolve(job,selected,context,check);
            }
        }finally{leave(reroute);}
    }
    private void requireSelected(Job job,RatisOperationRerouteJournal input)throws IOException {
        var original=input.originalCommand();var operation=original.operationId();
        if(!operationPath(operation).equals(job.directory))throw new IOException("Selected input crossed original operation directory");
        if(job.request!=null && (input.origin()==null || !job.request.origin().equals(input.origin()) || !Arrays.equals(DistributedCommandCodec.encode(job.request.original()),DistributedCommandCodec.encode(original)) || !Arrays.equals(job.request.targetDirectory().payloadDigest(),input.targetDirectory().payloadDigest()) || !Arrays.equals(job.request.freeze().encode(),input.absence().freeze().encode())))throw new IOException("Reroute state changed immutable selected input");
        if((job.credited || job.published) && job.request==null || job.published && job.result==null || job.done && (!job.published || !job.sourceAcknowledged) || job.sourceAcknowledged && job.ack==null)throw new IOException("Inconsistent selected reroute lifecycle");
        if(job.ack!=null && (!job.ack.sessionId().equals(operation.sessionId()) || job.ack.epoch()!=operation.epoch() || job.ack.sequence()<=operation.sequence()))throw new IOException("Source ACK changed original grant or sequence");
        if(job.publication!=null && (job.request==null || !job.request.equals(job.publication.request()) || job.published && !Arrays.equals(job.result.encode(),job.publication.result())))throw new IOException("Published state changed original request or business result");
    }
    private CommandResult resolve(Job job,RatisOperationRerouteJournal input,RequestContext context,RatisAtomicClient.InlineCheck check)throws IOException {
        requireSelected(job,input);
        if(job.published)return job.result;
        check.check();Path issuing=job.directory.resolve("issuer.bin");
        try(var issuer=Files.exists(issuing) ? RatisGrantIssuer.reopen(issuing,epochs) : RatisGrantIssuer.create(issuing,epochs,Math.addExact(epochs.read().logicalTimeMillis(),SessionEpochLedger.MAX_LIFETIME_MILLIS))) {
            for(int n=0;issuer.phase()!=RatisGrantIssuer.Phase.ISSUED && n<32;n++){check.check();var phase=issuer.step();if(phase==RatisGrantIssuer.Phase.EXPIRED || phase==RatisGrantIssuer.Phase.EXHAUSTED)throw new IOException("Reroute control grant unavailable; original allocation retained");}
            if(issuer.issued()==null)throw new IOException("Reroute issuance quantum incomplete");var controls=issuer.issued().grant();
            if(input.origin()==null) {
                var command=input.originalCommand();var route=input.sourceDirectory().ranges().stream().filter(r->r.groupId().equals(command.groupId())).findFirst().orElseThrow();
                input.bindOrigin(new OperationRerouteOrigin(schema.clusterId(),schema.databaseId(),schema.domainId(),new DistributedCdcCheckpoint.Lineage(route.rangeId(),route.groupId(),route.rangeEpoch()),command.operationId(),command.receiptDigest(),OperatorUpload.sha256(input.absence().freeze().encode()),input.absence().cut(),input.targetDirectory().payloadDigest(),RatisOperationRerouteAdmissionRpc.normalizedDigest(command,input.targetDirectory()),RatisOperationRerouteAdmissionRpc.targetParticipantCount(command,input.targetDirectory()),new OperationId(controls.sessionId(),controls.epoch(),0)));
            }
            if(!input.origin().firstControl().equals(new OperationId(controls.sessionId(),controls.epoch(),0)))throw new IOException("Reroute issued allocation changed");
            var reserved=TransactionStatus.reserve(input.origin(),schema.schemaEpoch(),schema.digest());var identity=new RatisAtomicClient.Identity(reserved.transactionId(),reserved.operation(),input.origin().firstControl());
            var evidence=new RatisOperationRerouteAdmissionRpc.Evidence(input.originalCommand(),input.sourceDirectory(),input.targetDirectory(),input.absence(),List.of());
            if(!Files.exists(input.atomicPath())) {
                bind(job,input,controls,reserved,check);
                var current=homes.readRegistration(reserved.transactionId());
                if(current.status()==null) {check.check();var result=homes.execute(homes.prepareReservation(identity.firstControl(),input.origin(),schema.schemaEpoch(),schema.digest()),evidence);if(result.status()!=CommandResult.Status.SUCCESS)throw new IOException("Original HOME reservation did not apply");current=homes.readRegistration(reserved.transactionId());}
                if(current.status()==null || !reserved.origin().equals(current.status().origin()))throw new IOException("Original HOME reservation changed");
                if(current.status().phase()==TransactionStatus.Phase.PREPARING)throw new IOException("Promoted HOME has no original accepted journal; recapture is forbidden");
                if(job.request==null){job.request=new NativeSourceOutcomeCredit.Request(input.originalCommand(),input.origin(),input.targetDirectory(),input.absence().freeze(),homes.identity(reserved,current.descriptor()));job.persist();}
                if(current.status().reserved() && !job.credited) {
                    var record=new RatisSourceOutcomeRecord(RatisSourceOutcomeRecord.Kind.CREDIT,job.request,null);check.check();var credit=routed.appendSourceOutcome(input.sourceDirectory(),record,List.of());job.credited=credit.outcome()==NativeSourceOutcomePublication.Outcome.RETAINED;job.persist();
                }
                if(input.originalCommand().kind()==DistributedCommand.Kind.COMMIT_ACCEPTED || !job.credited)abort(job,reserved,check);
            }
            List<RatisDurableTransaction.Intent> intents=List.of();CommandResult actual;
            var home=homes.read(reserved.transactionId());
            if(home!=null && home.terminal() && home.participants().isEmpty())actual=home.terminalBusinessResult();
            else {
                check.check();try(var operation=Files.exists(input.atomicPath()) ? atomic.reopenInline(input.atomicPath(),context,check) : atomic.executeReservedPlanInline(input.atomicPath(),identity,reserved,RatisTransactionPlan.fromOriginal(input.targetDirectory(),input.originalCommand()),evidence,input.captureTtl(),context,check)) {
                    actual=operation.resumeInline(context,check);
                    if(operation.rejectedBeforeRegistration())abort(job,reserved,check);else intents=operation.snapshot().participants().stream().map(p->p.intent()).toList();
                }
            }
            if(job.request==null)throw new IOException("Source credit input missing from accepted journal lifecycle");
            if(job.publication==null) {
                check.check();var prepared=routed.prepareSourceOutcome(input.sourceDirectory(),new RatisSourceOutcomeRpc.Proposal(RatisSourceOutcomeRecord.Kind.PUBLISH,job.request,intents));
                if(prepared.fenced()){job.result=actual;job.published=true;job.persist();return actual;}
                job.publication=prepared.record();if(!Arrays.equals(actual.encode(),job.publication.result()))throw new IOException("Source publication disagrees with actual HOME/participant outcome");job.persist();
            }
            check.check();routed.appendSourceOutcome(input.sourceDirectory(),job.publication,intents);job.result=CommandResult.decode(job.publication.result());job.published=true;job.persist();return job.result;
        }
    }
    private void bind(Job job,RatisOperationRerouteJournal input,SessionEpochLedger.Grant controls,TransactionStatus reserved,RatisAtomicClient.InlineCheck check)throws IOException {
        Path paths=Files.createDirectories(job.directory.resolve("bindings"));SessionJournalFiles.force(paths);SessionJournalFiles.force(job.directory);
        var target=input.targetDirectory();var groups=input.originalCommand().acceptance()!=null ? input.originalCommand().acceptance().scope().participants(target,TransactionStatus.MAX_PARTICIPANTS) : RatisTransactionPlan.fromOriginal(target,input.originalCommand()).partitions().stream().map(p->p.route()).toList();
        for(var route:groups)for(boolean control:List.of(true,false)) {
            check.check();var grant=control ? controls : business;int window=control ? RatisAtomicClient.CONTROL_SLOTS : limits.outstanding();
            try(var maintenance=new RatisSessionMaintenance(paths.resolve(route.groupId()+"-"+control+".bin"),epochs,routed,route.groupId(),new RatisSessionMaintenance.ClockRoute(target,route,false))) {
                var pending=maintenance.pendingCommand();if(pending==null)pending=maintenance.prepareBinding(grant,window);
                if(!pending.sessionControl().grant().equals(grant) || pending.sessionControl().outstandingWindow()!=window || maintenance.execute().status()!=CommandResult.Status.SUCCESS)throw new IOException("Reroute target binding did not preserve original grant/window");
            }
        }
        check.check();homes.ensureBinding(paths.resolve("home-control.bin"),epochs,reserved.transactionId(),controls,256);homes.ensureBinding(paths.resolve("home-business.bin"),epochs,reserved.transactionId(),business,limits.outstanding());
    }
    private void abort(Job job,TransactionStatus reserved,RatisAtomicClient.InlineCheck check)throws IOException {
        var actual=homes.read(reserved.transactionId());if(actual!=null && actual.terminal())return;if(!reserved.equals(actual))throw new IOException("Cannot choose no-effect abort after promotion");
        if(job.abort==null){job.abort=new OperationId(UUID.randomUUID(),0,0);job.persist();}
        check.check();var request=homes.prepareRecovery(job.abort,reserved,reserved.abortReservation(TransactionStatus.NoEffectOutcome.CONFLICT),null);if(homes.executeRecovery(request).result().status()!=CommandResult.Status.SUCCESS)throw new IOException("Reserved no-effect abort did not apply");
    }
    /** Caller has already forced application consumption in the original window. */
    void recoverConsumed(RatisDurableSessionClient window,RequestContext context,RatisAtomicClient.InlineCheck check)throws IOException {
        enter();boolean reroute=false;try {
            long acknowledged=window.acknowledgedThrough();if(acknowledged<0)return;
            List<Path> directories;try(var files=Files.list(root)){directories=files.filter(p->Files.isDirectory(p,LinkOption.NOFOLLOW_LINKS)).limit(4097).toList();}
            if(directories.size()>4096)throw new IOException("Reroute cleanup directory bound exhausted");
            for(var directory:directories.stream().sorted(Comparator.comparingLong(p->Long.parseLong(p.getFileName().toString()))).toList()) {
                long sequence=Long.parseLong(directory.getFileName().toString());if(sequence>acknowledged || !Files.exists(directory.resolve("input.bin")))continue;
                if(!reroute){enterReroute();reroute=true;}
                try(var job=new Job(directory);var input=RatisOperationRerouteJournal.reopen(directory.resolve("input.bin"),binding)) {
                    requireSelected(job,input);if(job.done || !job.published)continue;check.check();
                    if(!job.sourceAcknowledged) {
                        var pending=window.pending().stream().filter(p->job.ack!=null && p.command().operationId().equals(job.ack)).findFirst().orElse(null);
                        if(pending==null && job.ack!=null && window.acknowledgedThrough()>=job.ack.sequence())job.sourceAcknowledged=true;
                        else {
                            if(pending==null) {
                                pending=window.pending().stream().filter(p->p.command().groupId().equals(input.originalCommand().groupId()) && p.command().rangeEpoch()==input.originalCommand().rangeEpoch() && RatisOperationRerouteAdmissionRpc.acknowledgementOnly(p.command()) && p.command().acknowledgedThrough()>=sequence).findFirst().orElse(null);
                                if(pending==null){var qualified=routed.qualifyAcknowledgementSource(input.original(routed),ackDirectory);check.check();pending=window.prepareAcknowledgementAt(qualified.directory(),qualified.parent());}
                                job.ack=pending.command().operationId();job.persist();
                            }
                            check.check();var result=window.execute(pending,routed::execute);
                            if(result.status()!=CommandResult.Status.SUCCESS && result.status()!=CommandResult.Status.SESSION_EXPIRED)throw new IOException("Original source ACK did not advance or verify its fence");
                            window.acknowledge(pending);job.sourceAcknowledged=true;
                        }
                        job.persist();
                    }
                    boolean atomicPresent=Files.exists(input.atomicPath()),rejected=atomicPresent && RatisAtomicRejection.isRejection(input.atomicPath());
                    if(atomicPresent) {
                        try(var operation=atomic.reopenForConsumptionInline(input.atomicPath(),context,check)){operation.consumeInline(context,check);}
                    }
                    if((!atomicPresent || rejected) && input.origin()!=null) {
                        if(!rejected && job.credited && input.originalCommand().kind()==DistributedCommand.Kind.DATA)throw new IOException("Accepted reroute lost its atomic journal; reserved identity cannot prove promoted retirement");
                        var status=homes.read(input.origin().transactionId());
                        if(status==null){var proof=homes.fence(job.request.home());if(proof==null)throw new IOException("Missing no-effect HOME has no original retirement proof");job.homeFence=proof;job.persist();}
                        if(status!=null && !status.consumed()) {
                            if(!status.terminal() || !status.participants().isEmpty())throw new IOException("Accepted HOME lost its original atomic journal; cleanup cannot reconstruct captures");
                            if(job.consume==null){job.consume=new OperationId(UUID.randomUUID(),0,0);job.persist();}check.check();
                            var recovery=homes.prepareRecovery(job.consume,status,status.consume(),null);try { if(homes.executeRecovery(recovery).result().status()!=CommandResult.Status.SUCCESS)throw new IOException("No-effect HOME consumption did not apply"); } catch(RatisHomeRetiredException retired) { if(!retired.proof().identity().equals(homes.identity(status,recovery.descriptor())))throw retired;job.homeFence=retired.proof();job.persist(); }
                        }
                    }
                    job.done=true;job.persist();
                }
            }
        }catch(NumberFormatException invalid){throw new IOException("Unrecognized reroute cleanup directory",invalid);}finally{leave(reroute);}
    }
    @Override public synchronized void close()throws IOException {if(active!=0)throw new IOException("Reroute work is still active");if(closed)return;atomic.close();closed=true;}
    private final class Job implements AutoCloseable {
        final Path directory,state;final FileChannel channel;final FileLock lock;NativeSourceOutcomeCredit.Request request;RatisSourceOutcomeRecord publication;CommandResult result;OperationId abort,ack,consume;HomeFenceProof homeFence;boolean credited,published,sourceAcknowledged,done;
        Job(Path directory)throws IOException {
            this.directory=directory;state=directory.resolve("state.bin");channel=FileChannel.open(directory.resolve("owner.lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE);FileLock acquired=null;
            try{try{acquired=channel.tryLock();}catch(OverlappingFileLockException busy){throw new IOException("Original reroute work already owned",busy);}if(acquired==null)throw new IOException("Original reroute work already owned");lock=acquired;if(Files.exists(state))decode(SessionJournalFiles.read(state,MAX_STATE_BYTES));}
            catch(IOException|RuntimeException failure){if(acquired!=null)acquired.release();channel.close();throw failure;}
        }
        void persist()throws IOException {byte[] req=request==null ? new byte[0] : request.encode(),pub=publication==null ? new byte[0] : publication.encode(),outcome=result==null ? new byte[0] : result.encode();var out=ByteBuffer.allocate(4+32+3*4+req.length+pub.length+outcome.length+2+1+(abort==null?0:32)+1+(ack==null?0:32)+1+(consume==null?0:32)+2+4+(homeFence==null?0:homeFence.encode().length)).putInt(MAGIC).put(binding);for(var bytes:List.of(req,pub,outcome))out.putInt(bytes.length).put(bytes);out.put((byte)(credited?1:0)).put((byte)(published?1:0)).put((byte)(abort==null?0:1));if(abort!=null)out.putLong(abort.sessionId().getMostSignificantBits()).putLong(abort.sessionId().getLeastSignificantBits()).putLong(abort.epoch()).putLong(abort.sequence());out.put((byte)(ack==null?0:1));if(ack!=null)out.putLong(ack.sessionId().getMostSignificantBits()).putLong(ack.sessionId().getLeastSignificantBits()).putLong(ack.epoch()).putLong(ack.sequence());out.put((byte)(consume==null?0:1));if(consume!=null)out.putLong(consume.sessionId().getMostSignificantBits()).putLong(consume.sessionId().getLeastSignificantBits()).putLong(consume.epoch()).putLong(consume.sequence());out.put((byte)(sourceAcknowledged?1:0)).put((byte)(done?1:0));byte[] fence=homeFence==null ? new byte[0] : homeFence.encode();out.putInt(fence.length).put(fence);SessionJournalFiles.write(state,out.array(),MAX_STATE_BYTES);}
        void decode(byte[] bytes)throws IOException {try{var in=ByteBuffer.wrap(bytes);byte[] bound=new byte[32];if(in.getInt()!=MAGIC)throw new IOException("Invalid reroute state");in.get(bound);if(!Arrays.equals(binding,bound))throw new IOException("Reroute state binding changed");byte[] req=blob(in,NativeSourceOutcomeCredit.Request.MAX_BYTES),pub=blob(in,RatisSourceOutcomeRecord.MAX_BYTES),outcome=blob(in,CommandResult.MAX_BYTES);request=req.length==0?null:NativeSourceOutcomeCredit.Request.decode(req);publication=pub.length==0?null:RatisSourceOutcomeRecord.decode(pub);result=outcome.length==0?null:CommandResult.decode(outcome);credited=flag(in);published=flag(in);if(flag(in))abort=new OperationId(new UUID(in.getLong(),in.getLong()),in.getLong(),in.getLong());if(flag(in))ack=new OperationId(new UUID(in.getLong(),in.getLong()),in.getLong(),in.getLong());if(flag(in))consume=new OperationId(new UUID(in.getLong(),in.getLong()),in.getLong(),in.getLong());if(consume!=null && (consume.epoch()!=0 || consume.sequence()!=0))throw new IOException("Invalid protected consumption identity");sourceAcknowledged=flag(in);done=flag(in);byte[] fence=blob(in,HomeFenceProof.MAX_BYTES);homeFence=fence.length==0?null:HomeFenceProof.decode(fence);if(homeFence!=null && (request==null || !homeFence.identity().equals(request.home())))throw new IOException("Cleanup retirement proof crossed original HOME");if(in.hasRemaining() || published && result==null || publication!=null && (request==null || !request.equals(publication.request())))throw new IOException("Inconsistent reroute state");}catch(RuntimeException malformed){throw new IOException("Invalid reroute state",malformed);}}
        @Override public void close()throws IOException {try{lock.release();}finally{channel.close();}}
    }
    private static byte[] blob(ByteBuffer in,int maximum){int size=in.getInt();if(size<0 || size>maximum || size>in.remaining())throw new IllegalArgumentException("Invalid reroute state blob");byte[] bytes=new byte[size];in.get(bytes);return bytes;}
    private static boolean flag(ByteBuffer in){int flag=in.get()&255;if(flag>1)throw new IllegalArgumentException("Invalid reroute state flag");return flag==1;}
}
