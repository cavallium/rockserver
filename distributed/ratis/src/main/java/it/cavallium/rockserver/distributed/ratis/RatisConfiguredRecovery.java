package it.cavallium.rockserver.distributed.ratis;

import java.io.*;
import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySourceSnapshotRpc.*;

/** One node-owned immutable recovery attempt. CONTROL only requests work; its borrowed executor owns IO. */
final class RatisConfiguredRecovery implements AutoCloseable {
    enum Phase { PREPARE, REGISTER, TRANSFER, PROBE, RELEASE, HEAD, COMPLETE, DONE, FENCE_RECEIVER, CLEANUP_OLD, LOCAL_DONE }
    enum CleanupDisposition { RELEASED, OBSOLETE }
    enum CleanupAction { RELEASE, OBSOLETE }
    record CurrentAlias(UUID transferId,byte[] digest) {
        CurrentAlias{Objects.requireNonNull(transferId);if(digest==null || digest.length!=32)throw new IllegalArgumentException("Invalid CURRENT alias digest");digest=digest.clone();}
        @Override public byte[] digest(){return digest.clone();}
        @Override public boolean equals(Object other){return other instanceof CurrentAlias alias && transferId.equals(alias.transferId) && Arrays.equals(digest,alias.digest);}
        @Override public int hashCode(){return Objects.hash(transferId,Arrays.hashCode(digest));}
    }
    sealed interface Preparation permits RemotePrepared,LocalCompletion {}
    record RemotePrepared(Prepared prepared) implements Preparation {RemotePrepared{Objects.requireNonNull(prepared);}}
    record LocalCompletion(LocalMetadataCompletion completion) implements Preparation {LocalCompletion{Objects.requireNonNull(completion);}}
    record CleanupWitness(UUID selectedMetadataTransferId,NativeRecoverySupersession.Scope scope,byte[] fenceDigest,List<CurrentAlias> references,List<Request> aliases,List<CleanupDisposition> dispositions){
        CleanupWitness {
            Objects.requireNonNull(selectedMetadataTransferId);Objects.requireNonNull(scope);fenceDigest=exactDigest(fenceDigest);references=List.copyOf(references);aliases=List.copyOf(aliases);dispositions=List.copyOf(dispositions);
            var selected=metadataRequest(scope,selectedMetadataTransferId);if(!Arrays.equals(scope.selectedMetadataRequestDigest(),requestDigest(selected)) || references.size()!=aliases.size() || aliases.isEmpty() || aliases.size()>NativeRecoverySupersession.MAX_CURRENT_REQUEST_DIGESTS || scope.currentRequestDigests().size()!=aliases.size())throw new IllegalArgumentException("Invalid completed cleanup scope/aliases");
            UUID previous=null;int count=0;for(int i=0;i<aliases.size();i++){var alias=aliases.get(i);var reference=references.get(i);if(previous!=null && RangeDirectoryCodec.UUID_ORDER.compare(previous,alias.transferId())>=0 || !alias.transferId().equals(reference.transferId()) || !Arrays.equals(requestDigest(alias),reference.digest()) || !scope.authorizesCurrentRequestDigest(reference.digest()))throw new IllegalArgumentException("Cleanup witness changed canonical alias");requireMetadataTransition(alias,selected);if(alias.transferId().equals(selectedMetadataTransferId))throw new IllegalArgumentException("META reused CURRENT identity");count=Math.addExact(count,(int)alias.range().replicas().stream().filter(owner->owner.role()==ReplicaDescriptor.Role.VOTER).count());previous=alias.transferId();}
            if(dispositions.size()!=count)throw new IllegalArgumentException("Cleanup witness lacks complete source dispositions");
        }
        @Override public byte[] fenceDigest(){return fenceDigest.clone();}
        Request originalSelected(){return metadataRequest(scope,selectedMetadataTransferId);}
        void require(Request selected){requireSameMetadata(originalSelected(),selected);}
        @Override public boolean equals(Object other){return other instanceof CleanupWitness w && selectedMetadataTransferId.equals(w.selectedMetadataTransferId) && scope.equals(w.scope) && Arrays.equals(fenceDigest,w.fenceDigest) && references.equals(w.references) && sameAliases(aliases,w.aliases) && dispositions.equals(w.dispositions);}
        @Override public int hashCode(){return Objects.hash(selectedMetadataTransferId,scope,Arrays.hashCode(fenceDigest),references,dispositions);}
    }
    enum LocalMetadataOrigin { LOCAL_SEED, METADATA_REGISTRATION }
    record LocalMetadataCompletion(byte[] requestDigest,ApplyPosition imageCut,byte[] imageHash,ApplyPosition completedCut,it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore.Configuration configuration,RatisSourceHeadRpc.Head head,LocalMetadataOrigin originKind,byte[] originDigest,byte[] freezeDigest){
        LocalMetadataCompletion {
            requestDigest=exactDigest(requestDigest);imageHash=exactDigest(imageHash);Objects.requireNonNull(originKind);originDigest=exactDigest(originDigest);freezeDigest=exactDigest(freezeDigest);Objects.requireNonNull(imageCut);Objects.requireNonNull(completedCut);Objects.requireNonNull(configuration);Objects.requireNonNull(head);
            if(imageCut.term()<1 || !NativeMetadataRecoveryRegistration.covers(completedCut,imageCut) || !NativeMetadataRecoveryRegistration.covers(completedCut,head.completedCut()) || configuration.term()>completedCut.term() || configuration.index()>completedCut.index() || configuration.index()==completedCut.index() && configuration.term()!=completedCut.term() || configuration.term()!=head.configuration().term() || configuration.index()!=head.configuration().index() || !Arrays.equals(configuration.data(),head.configuration().data()) || !head.request().minimumImageCut().equals(imageCut))throw new IllegalArgumentException("Local META completion changed native/image/head prefix");
        }
        @Override public byte[] requestDigest(){return requestDigest.clone();}@Override public byte[] imageHash(){return imageHash.clone();}@Override public byte[] originDigest(){return originDigest.clone();}@Override public byte[] freezeDigest(){return freezeDigest.clone();}
        void require(Request request){if(!request.metadataOnly() || !Arrays.equals(requestDigest, RatisConfiguredRecovery.requestDigest(request)))throw new IllegalArgumentException("Local completion changed selected META");requireSameMetadata(head.request().source(),request);if(!head.request().source().transferId().equals(request.transferId()))throw new IllegalArgumentException("Local completion HEAD changed request identity");}
        int encodedSize(){return Math.toIntExact(185L+configuration.data().length+RatisSourceHeadRpc.encodedReplyBytes(head));}
    }
    /** Exact existing LOCAL_DONE journal evidence, distinct from a native registration receipt. */
    record LocalMetadataProof(Request request,LocalMetadataCompletion completion,byte[] digest){
        LocalMetadataProof {completion.require(request);digest=exactDigest(digest);}
        @Override public byte[] digest(){return digest.clone();}
    }
    static LocalMetadataProof localMetadataProof(Map<String,byte[]> fields,byte[] expectedBinding,UUID group)throws IOException {
        try {
            if(!Arrays.equals(expectedBinding,fields.get("binding")) || !Arrays.equals("LOCAL_DONE".getBytes(StandardCharsets.US_ASCII),fields.get("phase")))throw new IllegalArgumentException("Journal is not a bound LOCAL_DONE publication");
            var request=request(fields.get("request"));if(!request.metadataOnly() || !request.sourceGroup().equals(group))throw new IllegalArgumentException("Local journal crossed selected META group");
            byte[] encoded=Objects.requireNonNull(fields.get("localCompletion"));var completion=decodeLocal(encoded,request);if(!Arrays.equals(encoded,encodeLocal(completion)))throw new IllegalArgumentException("Noncanonical local completion proof");
            return new LocalMetadataProof(request,completion,OperatorUpload.sha256(encoded));
        }catch(RuntimeException invalid){throw new IOException("Invalid forced local META journal proof",invalid);}
    }
    interface Host {
        /** Borrow an immutable graph request; no remote IO, mutation or unadmitted payload copies. */
        default Request resolveCurrentAlias(CurrentAlias reference)throws IOException{throw new IOException("Retained CURRENT graph alias unavailable");}
        default CompletableFuture<byte[]> forceSupersession(NativeRecoverySupersession.Scope scope){return CompletableFuture.failedFuture(new IOException("Receiver supersession owner unavailable"));}
        default CompletableFuture<CleanupDisposition> cleanup(Request alias,ReplicaDescriptor sourceOwner,CleanupAction action,NativeRecoverySupersession.Scope scope,byte[] fenceDigest){return CompletableFuture.failedFuture(new IOException("Original source cleanup owner unavailable"));}
        CompletableFuture<Preparation> prepare(Request request,CleanupWitness witness);
        CompletableFuture<byte[]> register(Prepared prepared);
        CompletableFuture<Prepared> transfer(Request request);
        CompletableFuture<RatisRecoveryImageRpc.Evidence> probe(RatisRecoveryImageRpc.Request request);
        CompletableFuture<RatisSourceHeadRpc.Head> head(RatisSourceHeadRpc.Request request);
        /** False means no completion force was attempted; a new source head may be selected. */
        CompletableFuture<Boolean> complete(Prepared prepared,RatisSourceHeadRpc.Head frozenHead);
        CompletableFuture<Void> release(Prepared prepared);
    }
    interface Metadata extends RatisProofQueryService.ResourceLease,RatisTopologySourceSnapshotClient.ReplyAdmission {
        void requireRawBytes(int bytes)throws IOException;
        @Override default void beforeReply(int bytes)throws IOException{requireRawBytes(bytes);}
    }
    @FunctionalInterface interface Scope {void require(Request request)throws IOException;}
    private record State(Request initial,Request request,Phase phase,Prepared prepared,byte[] registration,
                         RatisSourceHeadRpc.Head head,Abandoned abandoned,Supersession supersession,CleanupWitness witness,LocalMetadataCompletion localCompletion) {}
    private record Supersession(Request selected,NativeRecoverySupersession.Scope scope,List<CurrentAlias> references,List<Request> aliases,Phase previous,
                                byte[] fence,List<CleanupDisposition> dispositions,CleanupAction action) {
        Supersession {references=List.copyOf(references);aliases=List.copyOf(aliases);fence=fence==null?null:fence.clone();dispositions=List.copyOf(dispositions);Objects.requireNonNull(action);}
    }
    private record Cleanup(Request alias,ReplicaDescriptor owner) {}
    private static final int MAX_WAITERS=16;
    private final Path file;private final byte[] binding;private final Executor executor;private final Host host;
    private final RatisJournalAccess files;private final Metadata metadata;
    private final FileChannel channel;private final FileLock lock;
    private final CompletableFuture<Void> stopped=new CompletableFuture<>();
    private State state,dirty;private Phase phase;private CompletableFuture<Phase> actual;private int waiters;
    private boolean stopping,closed;

    /** Metadata and journal replacement capacity must be reserved before this constructor reads or copies state. */
    RatisConfiguredRecovery(Path directory,byte[] binding,Request initial,Host host,Executor executor,
                            RatisJournalAccess files,Metadata metadata,Scope scope)throws IOException {
        this.binding=Objects.requireNonNull(binding).clone();this.host=Objects.requireNonNull(host);this.executor=Objects.requireNonNull(executor);this.files=Objects.requireNonNull(files);this.metadata=Objects.requireNonNull(metadata);
        if(binding.length!=32 || initial!=null && initial.relocation()){metadata.close();throw new IllegalArgumentException("Invalid configured recovery binding/purpose");}
        var root=directory.toAbsolutePath().normalize();if(!Files.isDirectory(root,LinkOption.NOFOLLOW_LINKS)){metadata.close();throw new IOException("Recovery journal requires an owned existing directory");}
        file=root.resolve("recovery");Path lockFile=root.resolve("recovery.lock");try{channel=FileChannel.open(lockFile,StandardOpenOption.CREATE,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS);}catch(IOException failed){metadata.close();throw failed;}FileLock acquired=null;
        try {
            try{acquired=channel.tryLock();}catch(OverlappingFileLockException busy){throw new IOException("Recovery journal already owned",busy);}if(acquired==null)throw new IOException("Recovery journal already owned");lock=acquired;
            files.register(file,RatisSchemaClientJournal.MAX_BYTES);
            int count=0;try(var entries=Files.newDirectoryStream(root)){for(var entry:entries){if(++count>3 || !Set.of("recovery","recovery.pending","recovery.lock").contains(entry.getFileName().toString()) || !Files.isRegularFile(entry,LinkOption.NOFOLLOW_LINKS))throw new IOException("Foreign object in recovery journal");}}
            if(Files.exists(file,LinkOption.NOFOLLOW_LINKS)){long size=Files.size(file);if(size>RatisSchemaClientJournal.MAX_BYTES)throw new IOException("Recovery journal exceeds bound");metadata.requireRawBytes(Math.toIntExact(size));state=decode(RatisSchemaClientJournal.readOwned(file,files));if(initial!=null){try{requireRequest(initial,state.initial());}catch(IllegalArgumentException changed){if(!terminal(state.phase()) || !state.initial().metadataOnly() || !initial.metadataOnly() || (initial.transferId().equals(state.initial().transferId()) || initial.transferId().equals(state.request().transferId())))throw changed;requireRequest(state.initial().successor(initial.transferId()),initial);state=new State(initial,initial,Phase.PREPARE,null,null,null,null,null,state.witness(),null);}}}
            else if(initial==null){var unpublished=root.resolve("recovery.pending");if(!Files.isRegularFile(unpublished,LinkOption.NOFOLLOW_LINKS) || Files.size(unpublished)>RatisSchemaClientJournal.MAX_BYTES)throw new IOException("Recovery has no retained original request");metadata.requireRawBytes(Math.toIntExact(Files.size(unpublished)));state=decode(RatisSchemaClientJournal.readOwned(unpublished,files));if(state.phase()!=Phase.PREPARE || state.abandoned()!=null)throw new IOException("Missing canonical recovery journal after host dispatch");requireRequest(state.initial(),state.request());}
            else state=new State(initial,initial,Phase.PREPARE,null,null,null,null,null,null,null);
            scope.require(state.initial());
            // A pending replacement never authorized another host action. The forced main state,
            // or the undispatched initial request, remains the exact retry authority.
            var pending=root.resolve("recovery.pending");if(Files.exists(pending,LinkOption.NOFOLLOW_LINKS)){if(Files.size(pending)>RatisSchemaClientJournal.MAX_BYTES)throw new IOException("Recovery sidecar exceeds bound");Files.delete(pending);SessionJournalFiles.force(root);}
            persist(state);phase=state.phase();channel.force(true);SessionJournalFiles.force(root);
        }catch(IOException|RuntimeException|Error failed){try{if(acquired!=null)acquired.release();}catch(IOException cleanup){failed.addSuppressed(cleanup);}try{channel.close();}catch(IOException cleanup){failed.addSuppressed(cleanup);}metadata.close();throw failed;}
    }
    static boolean terminal(Phase phase){return phase==Phase.DONE || phase==Phase.LOCAL_DONE;}
    private static byte[] exactDigest(byte[] bytes){if(bytes==null || bytes.length!=32)throw new IllegalArgumentException("Invalid recovery digest");return bytes.clone();}
    private static Request metadataRequest(NativeRecoverySupersession.Scope scope,UUID transfer){return new Request(transfer,scope.header(),scope.catalogue(),scope.receiver(),Purpose.ANCESTOR_METADATA,scope.groupId(),scope.sourceDirectory());}
    private static void requireSameMetadata(Request original,Request next){if(!original.metadataOnly() || !next.metadataOnly() || !original.catalogue().equals(next.catalogue()) || !original.receiver().equals(next.receiver()) || !original.sourceGroup().equals(next.sourceGroup()) || !Arrays.equals(original.header().encode(),next.header().encode()) || !Arrays.equals(original.sourceDirectory().payloadDigest(),next.sourceDirectory().payloadDigest()))throw new IllegalArgumentException("META renewal changed immutable scope");}
    private static void requireMetadataTransition(Request previous,Request next) {
        var before=previous.range();var after=next.range();var left=previous.receiver();var right=next.receiver();
        if(previous.purpose()!=Purpose.CURRENT_LEAF || !next.metadataOnly() || !previous.catalogue().equals(next.catalogue()) || !previous.clusterId().equals(next.clusterId()) || !previous.databaseId().equals(next.databaseId()) || !previous.domainId().equals(next.domainId())
                || !before.groupId().equals(after.groupId()) || !before.rangeId().equals(after.rangeId()) || before.rangeEpoch()!=after.rangeEpoch() || !Arrays.equals(before.lowerInclusive(),after.lowerInclusive()) || !Arrays.equals(before.upperExclusive(),after.upperExclusive()) || next.sourceDirectory().generation()<previous.sourceDirectory().generation()
                || !left.replicaId().equals(right.replicaId()) || !left.nodeId().equals(right.nodeId()) || !left.nodeIncarnation().equals(right.nodeIncarnation()) || !left.peerId().equals(right.peerId()) || !left.address().equals(right.address()))throw new IllegalArgumentException("META continuation changed original CURRENT identity");
    }
    synchronized Phase phase(){return phase;}
    synchronized Request initial(){return state.initial();}
    /** No host dispatch; caller takes admission for these immutable bodies before our metadata lease closes. */
    RatisConfiguredRecoveries.RetainedRequests retainedRequests(RatisTopologySourceSnapshotClient.ReplyAdmission admission)throws IOException {
        Objects.requireNonNull(admission);
        synchronized(this){if(stopping || (actual!=null && dirty!=null))throw new IOException("Recovery journal cannot lend unsettled request bodies");}
        if(dirty!=null){persist(dirty);state=dirty;phase=dirty.phase();dirty=null;}
        var retained=new LinkedHashMap<UUID,Request>();
        retainRequest(retained,state.initial());retainRequest(retained,state.request());
        if(state.abandoned()!=null){retainRequest(retained,state.abandoned().original());retainRequest(retained,state.abandoned().successorRequest());}
        if(state.supersession()!=null){for(var alias:state.supersession().aliases())retainRequest(retained,alias);retainRequest(retained,state.supersession().selected());}
        if(state.witness()!=null){for(var alias:state.witness().aliases())retainRequest(retained,alias);retainRequest(retained,state.witness().originalSelected());}
        long bytes=256L+48L*retained.size();for(var request:retained.values())bytes=Math.addExact(bytes,encodedRequestBytes(request));
        if(state.witness()!=null)bytes=Math.addExact(bytes,witnessBytes(state.witness()));if(state.localCompletion()!=null)bytes=Math.addExact(bytes,state.localCompletion().encodedSize());
        if(bytes>Integer.MAX_VALUE)throw new IOException("Retained recovery request bodies exceed bound");
        admission.beforeReply((int)bytes);
        return new RatisConfiguredRecoveries.RetainedRequests(state.supersession()==null?state.request():state.supersession().selected(),List.copyOf(retained.values()),state.phase(),state.witness(),state.localCompletion());
    }
    private static void retainRequest(Map<UUID,Request> retained,Request request)throws IOException {
        var previous=retained.putIfAbsent(request.transferId(),request);
        if(previous!=null && previous!=request)try{requireRequest(previous,request);}catch(IllegalArgumentException conflict){throw new IOException("Retained recovery transfer identity changed body",conflict);}
    }
    void ensureSelected(Request selected)throws IOException {
        synchronized(this){if(stopping || actual!=null)throw new IOException("Recovery journal has active/stopped IO");}
        if(dirty!=null){persist(dirty);state=dirty;phase=dirty.phase();dirty=null;}
        try{requireRequest(state.initial(),selected);return;}catch(IllegalArgumentException different){}
        try{requireRequest(state.request(),selected);return;}catch(IllegalArgumentException different){}
        if(state.supersession()!=null){try{requireRequest(state.supersession().selected(),selected);return;}catch(IllegalArgumentException different){throw new IOException("Supersession keeps its frozen selected META",different);}}
        if(!terminal(state.phase()) || !state.initial().metadataOnly() || !selected.metadataOnly() || selected.transferId().equals(state.initial().transferId()) || selected.transferId().equals(state.request().transferId()))throw new IOException("Existing recovery owns another exact purpose/request");
        try{requireRequest(state.initial().successor(selected.transferId()),selected);}catch(IllegalArgumentException changed){throw new IOException("META renewal changed immutable scope",changed);}
        var candidate=new State(selected,selected,Phase.PREPARE,null,null,null,null,null,state.witness(),null);dirty=candidate;persist(candidate);state=candidate;phase=candidate.phase();dirty=null;
    }
    /** Runs on the manager's actual IO owner; forcing this recipe never dispatches a host action. */
    void reconcile(Request selected,NativeRecoverySupersession.Scope scope,List<Request> aliases)throws IOException {
        Objects.requireNonNull(selected);Objects.requireNonNull(scope);Objects.requireNonNull(aliases);
        synchronized(this){if(stopping || actual!=null)throw new IOException("Recovery journal has active/stopped IO");}
        if(dirty!=null){persist(dirty);state=dirty;phase=dirty.phase();dirty=null;}
        if(state.initial().metadataOnly()){ensureSelected(selected);return;}
        long bytes=1024L+encodedRequestBytes(selected)+scope.encodedSize()+4L+48L*aliases.size();if(aliases.size()>NativeRecoverySupersession.MAX_CURRENT_REQUEST_DIGESTS)throw new IOException("CURRENT aliases exceed bound");if(bytes>RatisSchemaClientJournal.MAX_BYTES)throw new IOException("Supersession recipe exceeds journal bound");metadata.requireRawBytes(Math.toIntExact(bytes));
        var ordered=new ArrayList<>(aliases);ordered.sort(Comparator.comparing(Request::transferId,RangeDirectoryCodec.UUID_ORDER));
        if(state.supersession()!=null){var saved=state.supersession();try{requireRequest(saved.selected(),selected);var observed=new NativeRecoverySupersession.Scope(scope.catalogue(),saved.scope().catalogueCut(),scope.header(),scope.sourceDirectory(),scope.groupId(),scope.receiver(),scope.selectedMetadataRequestDigest(),scope.currentRequestDigests());if(!observed.equals(saved.scope()) || !NativeMetadataRecoveryRegistration.covers(scope.catalogueCut(),saved.scope().catalogueCut()) || !sameAliases(saved.aliases(),ordered))throw new IllegalArgumentException("Supersession retry changed frozen recipe");return;}catch(IllegalArgumentException changed){throw new IOException("Supersession retry changed original authority",changed);}}
        var references=ordered.stream().map(alias->new CurrentAlias(alias.transferId(),requestDigest(alias))).toList();var recipe=new Supersession(selected,scope,references,ordered,state.phase(),null,List.of(),CleanupAction.RELEASE);var candidate=new State(state.initial(),state.request(),Phase.FENCE_RECEIVER,state.prepared(),state.registration(),state.head(),state.abandoned(),recipe,null,null);
        validate(candidate);dirty=candidate;persist(candidate);state=candidate;phase=candidate.phase();dirty=null;
    }
    private static boolean sameAliases(List<Request> first,List<Request> second){if(first.size()!=second.size())return false;for(int i=0;i<first.size();i++)try{requireRequest(first.get(i),second.get(i));}catch(IllegalArgumentException changed){return false;}return true;}
    private static Cleanup cleanupAt(Supersession recipe,int cursor) {
        for(var alias:recipe.aliases())for(var owner:alias.range().replicas().stream().filter(value->value.role()==ReplicaDescriptor.Role.VOTER).sorted(Comparator.comparing(ReplicaDescriptor::replicaId,RangeDirectoryCodec.UUID_ORDER)).toList())if(cursor--==0)return new Cleanup(alias,owner);return null;
    }
    /** Timer use allocates no observer while actual IO remains in flight. */
    void advance(){requestStep(false);}
    CompletableFuture<Phase> step(){return requestStep(true);}
    private CompletableFuture<Phase> requestStep(boolean observe) {
        final CompletableFuture<Phase> pending;boolean start=false;
        synchronized(this){
            if(stopping)return CompletableFuture.failedFuture(new IOException("Configured recovery stopped"));
            if(actual==null && terminal(phase))return CompletableFuture.completedFuture(phase);
            if(actual==null){actual=new CompletableFuture<>();waiters=0;start=true;}
            if(observe && waiters>=MAX_WAITERS)return CompletableFuture.failedFuture(new RejectedExecutionException("Recovery observation bound exhausted"));
            if(observe)waiters++;pending=actual;
        }
        if(start)post(()->start(pending),pending);
        return observe?pending.copy():pending;
    }
    private void post(Runnable job,CompletableFuture<Phase> pending){try{executor.execute(job);}catch(RuntimeException rejected){finish(pending,rejected);}}
    private void start(CompletableFuture<Phase> pending) {
        final State current,unsaved;
        boolean stop;synchronized(this){current=state;unsaved=dirty;stop=stopping && unsaved==null;}
        if(stop){finish(pending,new CancellationException("Recovery stopped before dispatch"));return;}
        if(unsaved!=null){save(unsaved,pending);return;}
        try {
            CompletableFuture<State> next=switch(current.phase()) {
                case PREPARE -> host.prepare(current.request(),current.witness()).handle((preparation,failure)->{
                    var problem=unwrap(failure);if(problem instanceof AbandonedException terminal){var receipt=terminal.receipt();requireRequest(current.request(),receipt.original());return new State(current.initial(),receipt.successorRequest(),Phase.PREPARE,null,null,null,receipt,null,current.witness(),null);}
                    if(problem!=null)throw new CompletionException(problem);if(preparation instanceof LocalCompletion local){var completion=local.completion();try{metadata.requireRawBytes(Math.addExact(completion.encodedSize(),encodedRequestBytes(current.request())));}catch(IOException denied){throw new CompletionException(denied);}completion.require(current.request());return new State(current.initial(),current.request(),Phase.LOCAL_DONE,null,null,null,current.abandoned(),null,current.witness(),completion);}var prepared=((RemotePrepared)preparation).prepared();requireRequest(current.request(),prepared.request());return new State(current.initial(),current.request(),Phase.REGISTER,prepared,null,null,current.abandoned(),null,current.witness(),null);
                });
                case REGISTER -> host.register(current.prepared()).thenApply(digest->{if(digest==null || digest.length!=32)throw new IllegalArgumentException("Missing exact native registration digest");return new State(current.initial(),current.request(),Phase.TRANSFER,current.prepared(),digest.clone(),null,current.abandoned(),null,current.witness(),null);});
                case TRANSFER -> host.transfer(current.request()).thenApply(prepared->{if(!Arrays.equals(reply(PREPARE,prepared),reply(PREPARE,current.prepared())))throw new IllegalArgumentException("Transfer changed original prepared image");return new State(current.initial(),current.request(),Phase.PROBE,current.prepared(),current.registration(),null,current.abandoned(),null,current.witness(),null);});
                case PROBE -> {var probe=new RatisRecoveryImageRpc.Request(current.request(),current.prepared().image().position(),current.prepared().image().sha256());yield host.probe(probe).thenApply(evidence->{evidence.require(probe);if(!evidence.imageInstalled())return current;if(!Arrays.equals(evidence.registrationDigest(),current.registration()))throw new IllegalArgumentException("Image receipt changed local registration");return new State(current.initial(),current.request(),Phase.RELEASE,current.prepared(),current.registration(),null,current.abandoned(),null,current.witness(),null);});}
                case HEAD -> {var query=new RatisSourceHeadRpc.Request(current.request(),current.prepared().image().position());yield host.head(query).thenApply(head->{RatisSourceHeadRpc.reply(RatisSourceHeadRpc.reply(head),query);return new State(current.initial(),current.request(),Phase.COMPLETE,current.prepared(),current.registration(),head,current.abandoned(),null,current.witness(),null);});}
                case COMPLETE -> host.complete(current.prepared(),current.head()).thenApply(done->new State(current.initial(),current.request(),done?Phase.DONE:Phase.HEAD,current.prepared(),current.registration(),done?current.head():null,current.abandoned(),null,current.witness(),null));
                case RELEASE -> host.release(current.prepared()).thenApply(ignored->new State(current.initial(),current.request(),Phase.HEAD,current.prepared(),current.registration(),null,current.abandoned(),null,current.witness(),null));
                case FENCE_RECEIVER -> host.forceSupersession(current.supersession().scope()).thenApply(digest->{if(digest==null || digest.length!=32)throw new IllegalArgumentException("Missing exact native supersession digest");var recipe=current.supersession();return new State(current.initial(),current.request(),Phase.CLEANUP_OLD,current.prepared(),current.registration(),current.head(),current.abandoned(),new Supersession(recipe.selected(),recipe.scope(),recipe.references(),recipe.aliases(),recipe.previous(),digest,recipe.dispositions(),recipe.action()),current.witness(),null);});
                case CLEANUP_OLD -> cleanupStep(current);
                case DONE,LOCAL_DONE -> CompletableFuture.completedFuture(current);
            };
            next.whenComplete((candidate,failure)->post(()->{if(failure!=null)finish(pending,unwrap(failure));else if(candidate==current)finish(pending,null);else save(candidate,pending);},pending));
        }catch(Throwable failed){finish(pending,failed);}
    }
    private CompletableFuture<State> cleanupStep(State current) {
        var recipe=current.supersession();var item=cleanupAt(recipe,recipe.dispositions().size());if(item==null)throw new IllegalArgumentException("Cleanup cursor escaped frozen cohort");
        CompletableFuture<CleanupDisposition> action;try{action=Objects.requireNonNull(host.cleanup(item.alias(),item.owner(),recipe.action(),recipe.scope(),recipe.fence().clone()));}catch(Throwable failure){action=CompletableFuture.failedFuture(failure);}
        return action.handle((disposition,failure)->{
            if(failure!=null)return new State(current.initial(),current.request(),Phase.CLEANUP_OLD,current.prepared(),current.registration(),current.head(),current.abandoned(),new Supersession(recipe.selected(),recipe.scope(),recipe.references(),recipe.aliases(),recipe.previous(),recipe.fence(),recipe.dispositions(),recipe.action()==CleanupAction.RELEASE?CleanupAction.OBSOLETE:CleanupAction.RELEASE),current.witness(),null);
            if(disposition!=(recipe.action()==CleanupAction.RELEASE?CleanupDisposition.RELEASED:CleanupDisposition.OBSOLETE))throw new IllegalArgumentException("Cleanup acknowledgement changed requested disposition");
            var completed=new ArrayList<>(recipe.dispositions());completed.add(disposition);if(cleanupAt(recipe,completed.size())==null)return new State(recipe.selected(),recipe.selected(),Phase.PREPARE,null,null,null,null,null,new CleanupWitness(recipe.selected().transferId(),recipe.scope(),recipe.fence(),recipe.references(),recipe.aliases(),completed),null);
            return new State(current.initial(),current.request(),Phase.CLEANUP_OLD,current.prepared(),current.registration(),current.head(),current.abandoned(),new Supersession(recipe.selected(),recipe.scope(),recipe.references(),recipe.aliases(),recipe.previous(),recipe.fence(),completed,CleanupAction.RELEASE),current.witness(),null);
        });
    }
    private void save(State candidate,CompletableFuture<Phase> pending) {
        synchronized(this){dirty=candidate;}
        try{persist(candidate);synchronized(this){state=candidate;phase=candidate.phase();dirty=null;}finish(pending,null);}
        catch(Throwable failed){finish(pending,failed);}
    }
    private void finish(CompletableFuture<Phase> pending,Throwable failure) {
        Phase result;boolean stop;
        synchronized(this){if(actual!=pending)return;actual=null;waiters=0;result=phase;stop=stopping;}
        if(failure==null)pending.complete(result);else pending.completeExceptionally(failure);
        if(stop)finishStop();
    }
    CompletableFuture<Void> stop(){boolean close;synchronized(this){stopping=true;close=actual==null;}if(close)finishStop();return stopped.copy();}
    private void finishStop(){
        synchronized(this){if(closed || actual!=null)return;closed=true;state=null;dirty=null;}
        IOException failure=null;try{lock.release();}catch(IOException failed){failure=failed;}try{channel.close();}catch(IOException failed){if(failure==null)failure=failed;else failure.addSuppressed(failed);}metadata.close();
        if(failure==null)stopped.complete(null);else stopped.completeExceptionally(failure);
    }
    @Override public void close()throws IOException {var done=stop();if(!done.isDone())throw new IOException("Recovery close waits for actual host and journal IO");try{done.join();}catch(CompletionException failed){throw new IOException("Recovery close failed",failed.getCause());}}
    private static Throwable unwrap(Throwable problem){while((problem instanceof CompletionException || problem instanceof ExecutionException) && problem.getCause()!=null)problem=problem.getCause();return problem;}
    private void persist(State value)throws IOException {
        validate(value);long size=40+field("binding",32)+field("initial",encodedRequestBytes(value.initial()))+field("request",encodedRequestBytes(value.request()))+field("phase",value.phase().name().length());
        if(value.prepared()!=null)size+=field("prepared",value.prepared().encodedSize());if(value.registration()!=null)size+=field("registration",32);if(value.head()!=null)size+=field("head",RatisSourceHeadRpc.encodedReplyBytes(value.head()));
        if(value.abandoned()!=null){var receipt=value.abandoned();size+=field("abandoned",73L+encodedRequestBytes(receipt.original())+57L+receipt.sourceOwner().peerId().length()+receipt.sourceOwner().address().length());}
        if(value.supersession()!=null){var recipe=value.supersession();size+=field("selected",encodedRequestBytes(recipe.selected()))+field("supersessionScope",recipe.scope().encodedSize())+field("aliases",aliasBytes(recipe.references()))+field("previousPhase",recipe.previous().name().length())+field("cleanup",recipe.dispositions().size())+field("cleanupAction",recipe.action().name().length());if(recipe.fence()!=null)size+=field("fence",32);}
        if(value.witness()!=null){var witness=value.witness();size+=field("cleanupSelected",16)+field("cleanupScope",witness.scope().encodedSize())+field("cleanupAliases",aliasBytes(witness.references()))+field("cleanupFence",32)+field("cleanupDispositions",witness.dispositions().size());}
        if(value.localCompletion()!=null)size+=field("localCompletion",value.localCompletion().encodedSize());
        if(size>RatisSchemaClientJournal.MAX_BYTES)throw new IOException("Recovery journal metadata bound exhausted");metadata.requireRawBytes(Math.toIntExact(size));var fields=encode(value);
        long actual=40;for(var entry:fields.entrySet())actual+=field(entry.getKey(),entry.getValue().length);if(actual!=size)throw new IOException("Recovery journal sizing differs from encoded fields");
        long original=Files.exists(file,LinkOption.NOFOLLOW_LINKS)?Files.size(file):0;var reservation=new RatisJournalAccess.Reservation(file,RatisSchemaClientJournal.MAX_BYTES,Math.addExact(original,size));
        files.reserve(reservation);RatisSchemaClientJournal.writeOwned(file,fields,files);files.fulfilled(reservation);
    }
    private static long field(String name,long bytes){return 6L+name.length()+bytes;}
    private Map<String,byte[]> encode(State value){
        var fields=new HashMap<String,byte[]>();fields.put("binding",binding);fields.put("initial",request(PREPARE,value.initial()));fields.put("request",request(PREPARE,value.request()));fields.put("phase",value.phase().name().getBytes(StandardCharsets.US_ASCII));
        if(value.prepared()!=null)fields.put("prepared",reply(PREPARE,value.prepared()));if(value.registration()!=null)fields.put("registration",value.registration());if(value.head()!=null)fields.put("head",RatisSourceHeadRpc.reply(value.head()));if(value.abandoned()!=null)fields.put("abandoned",reply(PREPARE,value.abandoned()));if(value.supersession()!=null){var recipe=value.supersession();fields.put("selected",request(PREPARE,recipe.selected()));fields.put("supersessionScope",recipe.scope().encode());fields.put("aliases",encodeAliases(recipe.references()));fields.put("previousPhase",recipe.previous().name().getBytes(StandardCharsets.US_ASCII));byte[] cleanup=new byte[recipe.dispositions().size()];for(int i=0;i<cleanup.length;i++)cleanup[i]=(byte)recipe.dispositions().get(i).ordinal();fields.put("cleanup",cleanup);fields.put("cleanupAction",recipe.action().name().getBytes(StandardCharsets.US_ASCII));if(recipe.fence()!=null)fields.put("fence",recipe.fence());}
        if(value.witness()!=null){var w=value.witness();fields.put("cleanupSelected",uuidBytes(w.selectedMetadataTransferId()));fields.put("cleanupScope",w.scope().encode());fields.put("cleanupAliases",encodeAliases(w.references()));fields.put("cleanupFence",w.fenceDigest());fields.put("cleanupDispositions",encodeDispositions(w.dispositions()));}
        if(value.localCompletion()!=null)fields.put("localCompletion",encodeLocal(value.localCompletion()));return fields;
    }
    private State decode(Map<String,byte[]> fields)throws IOException {
        try{if(!Arrays.equals(binding,fields.get("binding")) || !Set.of("binding","initial","request","phase","prepared","registration","head","abandoned","selected","supersessionScope","aliases","previousPhase","cleanup","cleanupAction","fence","cleanupSelected","cleanupScope","cleanupAliases","cleanupFence","cleanupDispositions","localCompletion").containsAll(fields.keySet()))throw new IllegalArgumentException("Recovery journal binding/fields differ");
            var initial=request(fields.get("initial"));var request=request(fields.get("request"));var phase=Phase.valueOf(new String(fields.get("phase"),StandardCharsets.US_ASCII));var prepared=fields.containsKey("prepared")?reply(fields.get("prepared"),PREPARE,request):null;
            var head=fields.containsKey("head")?RatisSourceHeadRpc.reply(fields.get("head"),new RatisSourceHeadRpc.Request(request,Objects.requireNonNull(prepared).image().position())):null;
            Abandoned abandoned=null;if(fields.containsKey("abandoned")){var packet=ScopedTransactionRpc.parse(fields.get("abandoned"),MAX_REPLY_BYTES);var bytes=java.nio.ByteBuffer.wrap(packet.body());if(bytes.get()!=2)throw new IllegalArgumentException("Expected abandonment receipt");int length=bytes.getInt();if(length<1 || length>MAX_REQUEST_BYTES || length>bytes.remaining())throw new IllegalArgumentException("Invalid abandonment scope length");byte[] body=new byte[length];bytes.get(body);var previous=request(ScopedTransactionRpc.frame(PREPARE,body));try{reply(fields.get("abandoned"),PREPARE,previous);throw new IllegalArgumentException("Expected terminal abandonment");}catch(AbandonedException terminal){abandoned=terminal.receipt();}}
            Supersession recipe=null;if(fields.containsKey("selected")){var dispositions=new ArrayList<CleanupDisposition>();for(byte code:Objects.requireNonNull(fields.get("cleanup"))){if(code<0 || code>=CleanupDisposition.values().length)throw new IllegalArgumentException("Unknown cleanup disposition");dispositions.add(CleanupDisposition.values()[code]);}var references=decodeAliases(fields.get("aliases"));var aliases=new ArrayList<Request>();for(var reference:references){var alias=host.resolveCurrentAlias(reference);if(alias==null || !alias.transferId().equals(reference.transferId()) || !Arrays.equals(requestDigest(alias),reference.digest()))throw new IllegalArgumentException("Retained graph alias changed immutable body");aliases.add(alias);}recipe=new Supersession(request(fields.get("selected")),NativeRecoverySupersession.Scope.decode(fields.get("supersessionScope")),references,aliases,Phase.valueOf(new String(fields.get("previousPhase"),StandardCharsets.US_ASCII)),fields.get("fence"),dispositions,CleanupAction.valueOf(new String(fields.get("cleanupAction"),StandardCharsets.US_ASCII)));}
            else if(fields.keySet().stream().anyMatch(Set.of("supersessionScope","aliases","previousPhase","cleanup","cleanupAction","fence")::contains))throw new IllegalArgumentException("Partial supersession journal");
            CleanupWitness witness=null;if(fields.containsKey("cleanupSelected")){var id=java.nio.ByteBuffer.wrap(fields.get("cleanupSelected"));if(id.remaining()!=16)throw new IllegalArgumentException("Invalid cleanup selected identity");var references=decodeAliases(fields.get("cleanupAliases"));witness=new CleanupWitness(new UUID(id.getLong(),id.getLong()),NativeRecoverySupersession.Scope.decode(fields.get("cleanupScope")),fields.get("cleanupFence"),references,resolveAliases(references),decodeDispositions(fields.get("cleanupDispositions")));}
            else if(fields.keySet().stream().anyMatch(Set.of("cleanupScope","cleanupAliases","cleanupFence","cleanupDispositions")::contains))throw new IllegalArgumentException("Partial cleanup witness");
            var local=fields.containsKey("localCompletion")?decodeLocal(fields.get("localCompletion"),request):null;
            var state=new State(initial,request,phase,prepared,fields.get("registration"),head,abandoned,recipe,witness,local);validate(state);return state;
        }catch(RuntimeException failed){throw new IOException("Invalid configured recovery journal",failed);}
    }
    private List<Request> resolveAliases(List<CurrentAlias> references)throws IOException {var aliases=new ArrayList<Request>(references.size());for(var reference:references){var alias=host.resolveCurrentAlias(reference);if(alias==null || !alias.transferId().equals(reference.transferId()) || !Arrays.equals(requestDigest(alias),reference.digest()))throw new IOException("Retained graph alias changed immutable body");aliases.add(alias);}return List.copyOf(aliases);}
    private static long witnessBytes(CleanupWitness witness){return 16L+witness.scope().encodedSize()+aliasBytes(witness.references())+32+witness.dispositions().size();}
    private static byte[] uuidBytes(UUID id){return java.nio.ByteBuffer.allocate(16).putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()).array();}
    private static byte[] encodeDispositions(List<CleanupDisposition> values){var bytes=new byte[values.size()];for(int i=0;i<bytes.length;i++)bytes[i]=(byte)values.get(i).ordinal();return bytes;}
    private static List<CleanupDisposition> decodeDispositions(byte[] bytes){Objects.requireNonNull(bytes);var values=new ArrayList<CleanupDisposition>(bytes.length);for(byte code:bytes){if(code<0 || code>=CleanupDisposition.values().length)throw new IllegalArgumentException("Invalid cleanup disposition");values.add(CleanupDisposition.values()[code]);}return List.copyOf(values);}
    private static byte[] encodeLocal(LocalMetadataCompletion completion){byte[] cfg=completion.configuration().data(),head=RatisSourceHeadRpc.reply(completion.head());var out=java.nio.ByteBuffer.allocate(completion.encodedSize());out.put(completion.requestDigest()).putLong(completion.imageCut().term()).putLong(completion.imageCut().index()).put(completion.imageHash()).putLong(completion.completedCut().term()).putLong(completion.completedCut().index()).putLong(completion.configuration().term()).putLong(completion.configuration().index()).putInt(cfg.length).put(cfg).putInt(head.length).put(head).put((byte)completion.originKind().ordinal()).put(completion.originDigest()).put(completion.freezeDigest());return out.array();}
    private static LocalMetadataCompletion decodeLocal(byte[] bytes,Request request){if(bytes.length<185 || bytes.length>185+65536+RatisSourceHeadRpc.MAX_REPLY_BYTES)throw new IllegalArgumentException("Local completion exceeds bound");var in=java.nio.ByteBuffer.wrap(bytes);byte[] digest=new byte[32],hash=new byte[32],origin=new byte[32],freeze=new byte[32];in.get(digest);var image=new ApplyPosition(in.getLong(),in.getLong());in.get(hash);var completed=new ApplyPosition(in.getLong(),in.getLong());long term=in.getLong(),index=in.getLong();byte[] cfg=ScopedTransactionRpc.blob(in,65536,false),head=ScopedTransactionRpc.blob(in,RatisSourceHeadRpc.MAX_REPLY_BYTES,false);if(in.remaining()!=65)throw new IllegalArgumentException("Invalid local completion framing");int kind=in.get()&255;if(kind>=LocalMetadataOrigin.values().length)throw new IllegalArgumentException("Unknown local META provenance");in.get(origin).get(freeze);return new LocalMetadataCompletion(digest,image,hash,completed,new it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore.Configuration(term,index,cfg),RatisSourceHeadRpc.reply(head,new RatisSourceHeadRpc.Request(request,image)),LocalMetadataOrigin.values()[kind],origin,freeze);}
    private static int aliasBytes(List<CurrentAlias> aliases){return Math.toIntExact(4L+48L*aliases.size());}
    private static byte[] encodeAliases(List<CurrentAlias> aliases){var out=java.nio.ByteBuffer.allocate(aliasBytes(aliases)).putInt(aliases.size());for(var alias:aliases)out.putLong(alias.transferId().getMostSignificantBits()).putLong(alias.transferId().getLeastSignificantBits()).put(alias.digest());return out.array();}
    private static List<CurrentAlias> decodeAliases(byte[] bytes){if(bytes==null || bytes.length<4 || bytes.length>4+48*NativeRecoverySupersession.MAX_CURRENT_REQUEST_DIGESTS)throw new IllegalArgumentException("Invalid aliases framing");var in=java.nio.ByteBuffer.wrap(bytes);int count=in.getInt();if(count<1 || count>NativeRecoverySupersession.MAX_CURRENT_REQUEST_DIGESTS || count*48L!=in.remaining())throw new IllegalArgumentException("Invalid alias count");var aliases=new ArrayList<CurrentAlias>();for(int i=0;i<count;i++){var transfer=new UUID(in.getLong(),in.getLong());byte[] digest=new byte[32];in.get(digest);aliases.add(new CurrentAlias(transfer,digest));}return List.copyOf(aliases);}
    static byte[] requestDigest(Request request){return OperatorUpload.sha256(RatisTopologySourceSnapshotRpc.request(PREPARE,request));}
    private static void validateSupersession(State state) {
        boolean superseding=state.phase()==Phase.FENCE_RECEIVER || state.phase()==Phase.CLEANUP_OLD;var recipe=state.supersession();if(superseding!=(recipe!=null))throw new IllegalArgumentException("Supersession phase lost recipe");if(recipe==null)return;
        if(recipe.previous()==Phase.FENCE_RECEIVER || recipe.previous()==Phase.CLEANUP_OLD || !recipe.selected().metadataOnly() || recipe.aliases().isEmpty() || recipe.aliases().size()>NativeRecoverySupersession.MAX_CURRENT_REQUEST_DIGESTS || state.initial().purpose()!=Purpose.CURRENT_LEAF || recipe.references().size()!=recipe.aliases().size())throw new IllegalArgumentException("Invalid supersession purpose/history");
        requireMetadataTransition(state.initial(),recipe.selected());var scope=recipe.scope();if(!scope.catalogue().equals(recipe.selected().catalogue()) || !scope.groupId().equals(recipe.selected().sourceGroup()) || !scope.receiver().equals(recipe.selected().receiver()) || !Arrays.equals(scope.header().encode(),recipe.selected().header().encode()) || !Arrays.equals(scope.sourceDirectory().payloadDigest(),recipe.selected().sourceDirectory().payloadDigest()) || !Arrays.equals(scope.selectedMetadataRequestDigest(),requestDigest(recipe.selected())) || scope.currentRequestDigests().size()!=recipe.aliases().size())throw new IllegalArgumentException("Supersession changed selected META authority");
        UUID previous=null;int offset=0;var known=new HashMap<UUID,Request>();for(var alias:recipe.aliases()){var reference=recipe.references().get(offset++);if(!alias.transferId().equals(reference.transferId()))throw new IllegalArgumentException("Alias reference identity changed");if(alias.purpose()!=Purpose.CURRENT_LEAF || previous!=null && RangeDirectoryCodec.UUID_ORDER.compare(previous,alias.transferId())>=0)throw new IllegalArgumentException("Duplicate/noncanonical CURRENT alias");requireMetadataTransition(alias,recipe.selected());if(!scope.authorizesCurrentRequestDigest(reference.digest()))throw new IllegalArgumentException("CURRENT alias is outside receiver fence");known.put(alias.transferId(),alias);previous=alias.transferId();}
        if(known.containsKey(recipe.selected().transferId()))throw new IllegalArgumentException("Selected META reused an old CURRENT transfer identity");
        for(var retained:state.abandoned()==null?List.of(state.initial(),state.request()):List.of(state.initial(),state.request(),state.abandoned().original())){var alias=known.get(retained.transferId());if(alias==null)throw new IllegalArgumentException("Supersession omitted retained CURRENT alias");requireRequest(retained,alias);}
        if(state.phase()==Phase.FENCE_RECEIVER?(recipe.fence()!=null || !recipe.dispositions().isEmpty() || recipe.action()!=CleanupAction.RELEASE):(recipe.fence()==null || recipe.fence().length!=32 || cleanupAt(recipe,recipe.dispositions().size())==null))throw new IllegalArgumentException("Supersession cleanup lost exact fence/cursor");
    }
    private static void validate(State state)throws IOException {
        try{if(state.initial().relocation())throw new IllegalArgumentException("Wrong recovery purpose");requireRequest(state.initial().successor(state.request().transferId()),state.request());
            if(state.request().transferId().equals(state.initial().transferId())!=(state.abandoned()==null))throw new IllegalArgumentException("Recovery successor lacks abandonment");if(state.abandoned()!=null)requireRequest(state.abandoned().successorRequest(),state.request());
            if(state.witness()!=null){state.witness().require(state.request());if(state.supersession()!=null)throw new IllegalArgumentException("Completed cleanup witness overlaps active supersession");}
            if(state.phase()==Phase.LOCAL_DONE){if(state.localCompletion()==null || state.prepared()!=null || state.registration()!=null || state.head()!=null || state.supersession()!=null)throw new IllegalArgumentException("Local terminal manufactured remote evidence");state.localCompletion().require(state.request());if(state.witness()!=null && !NativeMetadataRecoveryRegistration.covers(state.localCompletion().head().catalogueCut(),state.witness().scope().catalogueCut()))throw new IllegalArgumentException("Local completion HEAD regressed cleanup authority");return;}if(state.localCompletion()!=null)throw new IllegalArgumentException("Local receipt outside local terminal");
            validateSupersession(state);var effective=state.supersession()==null?state.phase():state.supersession().previous();
            boolean prepared=effective.ordinal()>=Phase.REGISTER.ordinal(),registered=effective.ordinal()>=Phase.TRANSFER.ordinal(),head=effective.ordinal()>=Phase.COMPLETE.ordinal();
            if(prepared!=(state.prepared()!=null) || registered!=(state.registration()!=null) || head!=(state.head()!=null) || registered && state.registration().length!=32)throw new IllegalArgumentException("Recovery phase lost required forced evidence");
            if(prepared)requireRequest(state.request(),state.prepared().request());if(head)RatisSourceHeadRpc.reply(RatisSourceHeadRpc.reply(state.head()),new RatisSourceHeadRpc.Request(state.request(),state.prepared().image().position()));
        }catch(RuntimeException failed){throw new IOException("Invalid recovery phase/evidence",failed);}
    }
}
