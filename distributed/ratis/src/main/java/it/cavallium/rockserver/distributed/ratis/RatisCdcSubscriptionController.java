package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/** Caller-driven CDC delivery and explicit consumer ACK. Borrowed transports outlive this owner. */
public final class RatisCdcSubscriptionController implements AutoCloseable {
    public static final int MAX_JOBS=8,MAX_METADATA_BYTES=RatisSchemaClientJournal.MAX_BYTES;
    public static final long MAX_PENDING_REPLY_BYTES=(long)MAX_JOBS*RatisCdcPollRpc.MAX_REPLY_BYTES;
    public enum Phase { CREATE, ACTIVE, CLOSING, CLOSED, CONFLICT }
    public record Poll(DistributedCdcCheckpoint.Lineage lineage,RatisCdcPollRpc.Outcome outcome,RatisCdcSubscriptionBatch batch,int requiredBytes,
                       DistributedCdcCheckpoint.Cursor floor,ApplyPosition cut) {}
    public static final class Conflict extends IOException {
        private Conflict(String action) { super("Frozen CDC "+action+" command conflicted; reopen does not rebase it"); }
    }
    private enum Action { CREATE, DELIVER, ACK, HANDOFF, CLOSE }
    private final RatisJournalAccess journalAccess;
    private final Path root;
    private final UUID cluster,database;
    private final RatisNodePlan.Catalogue binding;
    private final RaftClient client;
    private final RatisRoutedClient routed;
    private final RatisCdcSubscriptionCatalogue catalogue;
    private final RatisTopologyCatalogue topologies;
    private final FileChannel lockChannel;
    private final FileLock lock;
    private final Map<UUID,Handle> handles=new HashMap<>();
    private boolean busy,closed,fenced;
    public final class Handle implements AutoCloseable {
        private final UUID id;
        private final Map<String,byte[]> fields;
        private final DistributedCdcSubscription.Definition definition;
        private final RangeDirectory source;
        private boolean released;
        private Handle(UUID id,Map<String,byte[]> fields) {
            this.id=id; this.fields=new ConcurrentHashMap<>(fields);
            definition=DistributedCdcSubscription.Definition.decode(fields.get("definition")); source=RangeDirectoryCodec.decode(fields.get("source"));
        }
        public UUID subscriptionId() { return id; }
        public DistributedCdcSubscription.Definition definition() { return definition; }
        public DistributedCdcSubscription state() { return DistributedCdcSubscription.decode(fields.get("state")); }
        public Phase phase() { return Phase.valueOf(text(this,"phase")); }
        @Override public void close() throws IOException { synchronized(RatisCdcSubscriptionController.this) { if(busy) throw new IOException("CDC I/O still active"); released=true; handles.remove(id,this); } }
    }
    public RatisCdcSubscriptionController(Path root,UUID cluster,UUID database,RatisNodePlan.Catalogue binding,RaftClient client,RatisRoutedClient routed) throws IOException {
        this(root,cluster,database,binding,client,routed,RatisJournalAccess.standalone());
    }
    static RatisCdcSubscriptionController journalOwned(Path root,UUID cluster,UUID database,RatisNodePlan.Catalogue binding,RaftClient client,RatisRoutedClient routed,RatisJournalAccess access)throws IOException {return new RatisCdcSubscriptionController(root,cluster,database,binding,client,routed,access);}
    private RatisCdcSubscriptionController(Path root,UUID cluster,UUID database,RatisNodePlan.Catalogue binding,RaftClient client,RatisRoutedClient routed,RatisJournalAccess access) throws IOException {
        journalAccess=Objects.requireNonNull(access);access.register(root.resolve("cdc-controller.lock"),32);
        this.root=root.toAbsolutePath().normalize(); this.cluster=Objects.requireNonNull(cluster); this.database=Objects.requireNonNull(database);
        this.binding=Objects.requireNonNull(binding); this.client=Objects.requireNonNull(client); this.routed=Objects.requireNonNull(routed);
        var route=routed.directory(); if(!route.clusterId().equals(cluster) || !route.databaseId().equals(database)) throw new IllegalArgumentException("Foreign CDC routed database");
        if(!Files.isDirectory(this.root,LinkOption.NOFOLLOW_LINKS)) throw new IOException("CDC journal root absent");
        catalogue=new RatisCdcSubscriptionCatalogue(client,cluster,database,binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn());
        topologies=new RatisTopologyCatalogue(client,cluster,database,binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn());
        lockChannel=FileChannel.open(this.root.resolve("cdc-controller.lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS);
        try { lock=lockChannel.tryLock(); if(lock==null) throw new IOException("CDC journal root already owned"); }
        catch(IOException|RuntimeException failure) { lockChannel.close(); throw new IOException("CDC journal root unavailable",failure); }
    }
    public Handle create(DistributedCdcSubscription.Definition definition,RangeDirectory sourceDirectory) throws IOException {
        begin(); try {
            requireDefinition(definition,sourceDirectory); UUID id=definition.subscriptionId();
            if(handles.containsKey(id) || Files.exists(path(id),LinkOption.NOFOLLOW_LINKS)) throw new IOException("CDC subscription already retained; reopen original");
            int jobs=0; try(var files=Files.newDirectoryStream(root,"*.cdc")) { for(var ignored:files) if(++jobs>=MAX_JOBS) throw new IOException("CDC retained job/reply capacity exhausted"); }
            var fields=new HashMap<String,byte[]>(); fields.put("definition",definition.encode()); fields.put("source",RangeDirectoryCodec.encode(sourceDirectory)); fields.put("binding",bindingBytes());
            fields.put("state",DistributedCdcSubscription.initial(definition).encode()); fields.put("phase",Phase.CREATE.name().getBytes(StandardCharsets.UTF_8));
            fields.put("issuer",UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8)); fields.put("sequence",longBytes(0)); fields.put("lineage",longBytes(0));
            var h=new Handle(id,fields); save(h); handles.put(id,h); resume(h); return h;
        } finally { end(); }
    }
    public Handle reopen(UUID subscriptionId) throws IOException {
        begin(); try {
            if(handles.containsKey(subscriptionId)) return handles.get(subscriptionId);
            if(handles.size()>=MAX_JOBS) throw new IOException("CDC handle capacity exhausted");
            final Handle h;
            try { h=new Handle(subscriptionId,RatisSchemaClientJournal.readOwned(path(subscriptionId),journalAccess)); validateRecovered(h); }
            catch(RuntimeException malformed) { throw new IOException("Invalid retained CDC journal",malformed); }
            handles.put(subscriptionId,h); return h;
        } finally { end(); }
    }
    public Poll poll(Handle h,int maxRecords,int maxBytes) throws IOException {
        begin(); try {
            requireHandle(h); resume(h); requireActive(h);
            if(h.fields.containsKey("batchReady")) return batch(h);
            if(h.fields.containsKey("pollReply")) clearPoll(h); // A definite GAP/TOO_LARGE may be retried with a new budget.
            if(!h.fields.containsKey("pollRequest")) {
                var state=h.state(); var lineages=new ArrayList<>(state.checkpoint().cursors().keySet());
                var lineage=lineages.get(Math.toIntExact(number(h,"lineage")%lineages.size()));
                var request=new RatisCdcPollRpc.Request(new RatisCdcPollRpc.Binding(cluster,database,h.definition.domainId(),lineage),state.checkpoint().cursors().get(lineage),maxRecords,maxBytes);
                h.fields.put("pollDirectory",RangeDirectoryCodec.encode(pollDirectory(h,lineage))); h.fields.put("batchState",state.encode()); h.fields.put("pollRequest",RatisCdcPollRpc.request(request)); text(h,"batchId",UUID.randomUUID().toString()); save(h);
            }
            var request=request(h); save(h);
            var result=routed.pollCdcDirectory(RangeDirectoryCodec.decode(h.fields.get("pollDirectory")),request.binding().lineage(),request.from(),request.maxRecords(),request.maxBytes());
            byte[] reply=RatisCdcPollRpc.reply(result); RatisCdcPollRpc.decodeReply(reply,request); h.fields.put("pollReply",reply); save(h);
            if(result.outcome()!=RatisCdcPollRpc.Outcome.DATA) return new Poll(result.request().binding().lineage(),result.outcome(),null,result.requiredBytes(),result.floor(),result.cut());
            deliver(h); return batch(h);
        } finally { end(); }
    }
    public DistributedCdcSubscription acknowledge(Handle h,UUID batchId) throws IOException {
        begin(); try {
            requireHandle(h); Objects.requireNonNull(batchId);
            if(h.fields.containsKey("lastAck") && batchId.toString().equals(text(h,"lastAck"))) {
                if(h.fields.containsKey("closeRequested")) resumeClose(h);
                else { acknowledgeReceipt(h); if(h.fields.containsKey("pendingCommand")) resolve(h); }
                return h.state();
            }
            resume(h);
            if(h.fields.containsKey("lastAck") && batchId.toString().equals(text(h,"lastAck"))) return h.state();
            requireActive(h);
            if(!h.fields.containsKey("batchReady") || !batchId.toString().equals(text(h,"batchId"))) throw new IllegalArgumentException("CDC ACK is not the retained batch");
            var result=result(h); var next=h.state().checkpoint().advance(result.request().binding().lineage(),result.next());
            if(next.equals(h.state().checkpoint())) { finishBatch(h); save(h); handoff(h); return h.state(); }
            freeze(h,Action.ACK,catalogue.prepareAcknowledgement(next(h),h.state(),next)); resolve(h); handoff(h); return h.state();
        } finally { end(); }
    }
    public DistributedCdcSubscription closeSubscription(Handle h) throws IOException {
        begin(); try {
            requireHandle(h);
            if(h.phase()==Phase.CLOSED) { acknowledgeReceipt(h); return h.state(); }
            if(!h.fields.containsKey("closeRequested")) { text(h,"closeRequested",h.phase().name()); text(h,"phase",Phase.CLOSING.name()); save(h); }
            resumeClose(h); return h.state();
        } finally { end(); }
    }
    private void resume(Handle h) throws IOException {
        if(h.fields.containsKey("closeRequested")) { resumeClose(h); return; }
        acknowledgeReceipt(h);
        if(h.fields.containsKey("pendingCommand")) resolve(h);
        if(h.phase()==Phase.CONFLICT) throw new Conflict(text(h,"conflictAction"));
        if(h.phase()==Phase.CREATE) createCatalogue(h);
        if(h.phase()==Phase.ACTIVE && h.fields.containsKey("pollReply") && result(h).outcome()==RatisCdcPollRpc.Outcome.DATA && !h.fields.containsKey("batchReady")) deliver(h);
        if(h.phase()==Phase.ACTIVE && !h.fields.containsKey("pollRequest")) handoff(h);
    }
    private void createCatalogue(Handle h) throws IOException {
        text(h,"read","capacity"); save(h); var capacity=catalogue.readCapacity();
        final DistributedCommand command;
        try { command=catalogue.prepareCreate(next(h),h.definition,h.source,capacity); }
        catch(IllegalArgumentException exhausted) { text(h,"phase",Phase.CONFLICT.name()); text(h,"conflictAction",Action.CREATE.name()); save(h); throw new Conflict(Action.CREATE.name()); }
        freeze(h,Action.CREATE,command); resolve(h);
    }
    /** Explicit closure supersedes source qualification, but never rewrites an uncertain original. */
    private void resumeClose(Handle h) throws IOException {
        // A queued PREPARE may acquire a new hold after a prior cancellation echo.
        if(h.fields.containsKey("abandonedDelivery")) catalogue.cancelDelivery(DistributedCommandCodec.decode(h.fields.get("abandonedDelivery")));
        acknowledgeReceipt(h);
        if(h.fields.containsKey("pendingCommand")) {
            var action=Action.valueOf(text(h,"action"));
            if((action==Action.DELIVER || action==Action.HANDOFF) && !h.fields.containsKey("pendingResult")) {
                h.fields.put("abandonedDelivery",h.fields.get("pendingCommand"));
                h.fields.remove("pendingCommand"); h.fields.remove("action"); save(h);
                catalogue.cancelDelivery(DistributedCommandCodec.decode(h.fields.get("abandonedDelivery")));
            } else { resolve(h); if(h.phase()==Phase.CLOSED) return; }
        }
        if(Phase.CREATE.name().equals(text(h,"closeRequested"))) createCatalogue(h);
        text(h,"phase",Phase.CLOSING.name()); save(h);
        var observed=catalogue.read(h.id); var previous=h.state();
        if(observed==null || !observed.definition().equals(h.definition) || observed.generation()<previous.generation()) throw new IOException("CDC closure lacks matching catalogue state");
        if(!observed.currentStarts().equals(previous.currentStarts())) {
            try { observed.requireSuccessorOf(previous); } catch(IllegalArgumentException invalid) { throw new IOException("CDC closure observed an unqualified frontier replacement",invalid); }
        } else for(var entry:previous.checkpoint().cursors().entrySet()) {
            if(!observed.checkpoint().cursors().get(entry.getKey()).covers(entry.getValue())
                    || !observed.delivered().cursors().get(entry.getKey()).covers(previous.delivered().cursors().get(entry.getKey()))) throw new IOException("CDC closure observed regressing catalogue state");
        }
        h.fields.put("state",observed.encode()); save(h);
        if(observed.phase()==DistributedCdcSubscription.Phase.CLOSED) {
            finishClose(h); queueReceipt(h,number(h,"sequence")-1); save(h); acknowledgeReceipt(h); return;
        }
        freeze(h,Action.CLOSE,catalogue.prepareClose(next(h),observed)); resolve(h);
    }
    private static void finishClose(Handle h) {
        text(h,"phase",Phase.CLOSED.name()); clearPoll(h); h.fields.remove("closeRequested"); h.fields.remove("abandonedDelivery"); h.fields.remove("handoffRetry"); h.fields.remove("handoffRetryState");
    }
    private void queueReceipt(Handle h,long consumed) {
        h.fields.put("receiptSequence",longBytes(consumed));
        var ack=new DistributedCommand(binding.groupId(),binding.epoch(),binding.schemaDigest(),next(h),List.of(),List.of(),consumed);
        h.fields.put("pendingReceiptAck",DistributedCommandCodec.encode(ack));
    }
    private void deliver(Handle h) throws IOException {
        var result=result(h); var state=h.state(); var delivered=state.delivered().advance(result.request().binding().lineage(),result.next());
        if(delivered.equals(state.delivered())) { h.fields.put("batchReady",new byte[]{1}); save(h); return; }
        freeze(h,Action.DELIVER,catalogue.prepareDelivery(next(h),state,delivered)); resolve(h);
    }
    private void freeze(Handle h,Action action,DistributedCommand command) throws IOException {
        if(h.fields.containsKey("pendingCommand")) throw new IOException("Original CDC command remains unresolved");
        text(h,"action",action.name()); h.fields.put("pendingCommand",DistributedCommandCodec.encode(command)); save(h);
    }
    private void resolve(Handle h) throws IOException {
        var command=DistributedCommandCodec.decode(h.fields.get("pendingCommand")); var action=Action.valueOf(text(h,"action"));
        if(!h.fields.containsKey("pendingResult")) {
            if(RatisCdcSubscriptionCatalogue.requiresOwnerAdmission(command)) { save(h); catalogue.prepareDeliveryOwner(command); }
            save(h); var result=catalogue.execute(command); if(result.valueCount()!=0) throw new IOException("CDC command returned unexpected values"); h.fields.put("pendingResult",result.encode()); save(h);
        }
        var result=CommandResult.decode(h.fields.get("pendingResult"));
        boolean retryHandoff=action==Action.HANDOFF && result.status()==CommandResult.Status.CONFLICT;
        // A definite birth outcome must never trigger another CREATE solely to close the job.
        if(action==Action.CREATE && h.fields.containsKey("closeRequested")) text(h,"closeRequested",Phase.ACTIVE.name());
        if(result.status()==CommandResult.Status.SUCCESS) {
            h.fields.put("state",command.mutations().getFirst().value()); text(h,"phase",action==Action.CLOSE ? Phase.CLOSED.name() : Phase.ACTIVE.name());
            if(action==Action.DELIVER) h.fields.put("batchReady",new byte[]{1});
            if(action==Action.ACK) finishBatch(h);
            if(action==Action.HANDOFF) {
                var handoffState=DistributedCdcSubscription.decode(command.mutations().getFirst().value()); var header=handoffState.latestHandoff().header();
                for(var parent:header.parents()) h.fields.remove("lineageTopology/"+parent.groupId());
                for(var child:header.targets()) h.fields.put("lineageTopology/"+child.groupId(),header.definitionDigest());
                h.fields.remove("barrier/"+header.intentId()); h.fields.remove("handoffRetry"); h.fields.remove("handoffRetryState");
            }
            if(action==Action.CLOSE) finishClose(h);
        } else if(retryHandoff) {
            // Native CAS definitively saved a no-effect outcome. Only now may a new operation witness later optional proofs.
            h.fields.put("handoffRetry",DistributedCdcSubscription.decode(command.mutations().getFirst().value()).latestHandoff().encode());
            h.fields.put("handoffRetryState",command.predicates().getFirst().expectedValue()); h.fields.put("conflictResult",h.fields.get("pendingResult"));
            text(h,"phase",h.fields.containsKey("closeRequested") ? Phase.CLOSING.name() : Phase.ACTIVE.name());
        } else { text(h,"phase",h.fields.containsKey("closeRequested") ? Phase.CLOSING.name() : Phase.CONFLICT.name()); text(h,"conflictAction",action.name()); h.fields.put("conflictResult",h.fields.get("pendingResult")); }
        queueReceipt(h,command.operationId().sequence());
        h.fields.remove("pendingCommand"); h.fields.remove("pendingResult"); h.fields.remove("action"); save(h); acknowledgeReceipt(h);
        if(result.status()!=CommandResult.Status.SUCCESS && !retryHandoff) throw new Conflict(action.name());
    }
    private void acknowledgeReceipt(Handle h) throws IOException {
        if(!h.fields.containsKey("pendingReceiptAck")) return; save(h);
        var reply=RatisRoutedClient.rpc(() -> client.io().send(Message.valueOf(ByteString.copyFrom(h.fields.get("pendingReceiptAck")))));
        if(reply==null || !reply.isSuccess() || reply.getMessage()==null || reply.getMessage().getContent().size()>CommandResult.MAX_BYTES) throw new IOException("CDC receipt acknowledgement unavailable");
        final CommandResult result; try { result=CommandResult.decode(reply.getMessage().getContent().toByteArray()); } catch(IllegalArgumentException invalid) { throw new IOException("Invalid CDC receipt acknowledgement",invalid); }
        if(result.status()!=CommandResult.Status.SUCCESS || result.valueCount()!=0) throw new IOException("CDC receipt acknowledgement rejected");
        h.fields.remove("pendingReceiptAck"); h.fields.remove("receiptSequence"); save(h);
    }
    private Poll batch(Handle h) {
        var result=result(h); var batch=new RatisCdcSubscriptionBatch(UUID.fromString(text(h,"batchId")),DistributedCdcSubscription.decode(h.fields.get("batchState")),result);
        return new Poll(result.request().binding().lineage(),result.outcome(),batch,0,result.floor(),result.cut());
    }
    private void finishBatch(Handle h) {
        for(var record:result(h).records()) if(record.kind()==DistributedCdcRecord.Kind.TERMINAL_BARRIER) {
            var header=record.barrier().header(); String key="barrier/"+header.intentId(); byte[] retained=h.fields.get(key);
            if(retained!=null && !Arrays.equals(retained,header.encode())) throw new IllegalArgumentException("CDC terminal intent changed");
            h.fields.put(key,header.encode());
        }
        text(h,"lastAck",text(h,"batchId")); number(h,"lineage",Math.incrementExact(number(h,"lineage"))); clearPoll(h); }
    private static void clearPoll(Handle h) { for(var field:List.of("pollRequest","pollReply","batchState","batchId","batchReady","pollDirectory")) h.fields.remove(field); }
    private RangeDirectory pollDirectory(Handle h,DistributedCdcCheckpoint.Lineage lineage) throws IOException {
        byte[] digest=h.fields.get("lineageTopology/"+lineage.groupId()); if(digest==null) return h.source;
        text(h,"read","lineageTopology"); save(h); var topology=topologies.readDefinition(digest);
        if(topology==null || !topology.header().domainId().equals(h.definition.domainId())) throw new IOException("CDC successor immutable topology absent/foreign");
        return topology.targetDirectory();
    }
    private void handoff(Handle h) throws IOException {
        if(h.fields.containsKey("pollRequest") || h.fields.containsKey("pendingCommand") || h.fields.containsKey("closeRequested")) return;
        for(var key:h.fields.keySet().stream().filter(k->k.startsWith("barrier/")).sorted().toList()) {
            var header=TopologyChange.Header.decode(h.fields.get(key));
            if(h.fields.containsKey("handoffRetry")) {
                var original=DistributedCdcBarrierHandoff.decode(h.fields.get("handoffRetry")); if(!header.intentId().equals(original.header().intentId())) continue;
                text(h,"read","handoffRetryPredecessor"); save(h); var observed=catalogue.read(h.id);
                if(observed==null || !Arrays.equals(observed.encode(),h.fields.get("handoffRetryState")) || !Arrays.equals(h.fields.get("state"),h.fields.get("handoffRetryState"))) { text(h,"phase",Phase.CONFLICT.name()); text(h,"conflictAction",Action.HANDOFF.name()); save(h); throw new Conflict(Action.HANDOFF.name()); }
            }
            text(h,"read","handoffAuthority"); save(h);
            var topology=topologies.readDefinition(header.definitionDigest()); var committed=topologies.readIntent(header.intentId());
            if(topology==null || committed==null) throw new IOException("CDC terminal topology authority absent"); header.requireDefinition(topology);
            if(committed.phase()!=TopologyChange.Phase.ACTIVE && committed.phase()!=TopologyChange.Phase.RETIRED) continue;
            var handoff=DistributedCdcBarrierHandoff.from(topology,committed);
            if(h.fields.containsKey("handoffRetry") && !handoff.equals(DistributedCdcBarrierHandoff.decode(h.fields.get("handoffRetry")))) { text(h,"phase",Phase.CONFLICT.name()); text(h,"conflictAction",Action.HANDOFF.name()); save(h); throw new Conflict(Action.HANDOFF.name()); }
            boolean ready=true;
            for(var entry:handoff.parentTerminal().cursors().entrySet()) { var ack=h.state().checkpoint().cursors().get(entry.getKey()); if(ack==null || !ack.covers(entry.getValue())) { ready=false; break; } }
            if(!ready) continue;
            final DistributedCommand command;
            try { command=catalogue.prepareLineageTransition(next(h),h.state(),handoff,topology,committed); } catch(IllegalArgumentException blocked) { throw new IOException("CDC acknowledged terminal handoff exceeds bounded frontier or authority",blocked); }
            freeze(h,Action.HANDOFF,command); resolve(h); return;
        }
    }
    private RatisCdcPollRpc.Request request(Handle h) { return RatisCdcPollRpc.decodeRequest(h.fields.get("pollRequest")); }
    private RatisCdcPollRpc.Result result(Handle h) { return RatisCdcPollRpc.decodeReply(h.fields.get("pollReply"),request(h)); }
    private void validateRecovered(Handle h) {
        requireDefinition(h.definition,h.source); h.phase(); UUID issuer=UUID.fromString(text(h,"issuer"));
        if(!h.id.equals(h.definition.subscriptionId()) || !Arrays.equals(h.fields.get("binding"),bindingBytes()) || !h.state().definition().equals(h.definition) || number(h,"sequence")<0 || number(h,"lineage")<0) throw new IllegalArgumentException("CDC journal namespace/state identity differs");
        if(h.fields.containsKey("handoffRetry")) {
            var original=DistributedCdcBarrierHandoff.decode(h.fields.get("handoffRetry")); var predecessor=DistributedCdcSubscription.decode(h.fields.get("handoffRetryState"));
            if(!predecessor.equals(h.state()) || !original.header().clusterId().equals(cluster) || !original.header().databaseId().equals(database) || !original.header().domainId().equals(h.definition.domainId())) throw new IllegalArgumentException("CDC known no-effect handoff retry changed predecessor/namespace"); predecessor.lineageTransition(original);
        } else if(h.fields.containsKey("handoffRetryState")) throw new IllegalArgumentException("CDC handoff retry lost original cuts");
        for(var row:h.fields.entrySet()) {
            if(row.getKey().startsWith("lineageTopology/")) { var group=UUID.fromString(row.getKey().substring(16)); if(row.getValue().length!=32 || h.state().currentStarts().cursors().keySet().stream().noneMatch(l->l.groupId().equals(group))) throw new IllegalArgumentException("CDC current topology pointer changed"); }
            if(row.getKey().startsWith("barrier/")) { var header=TopologyChange.Header.decode(row.getValue()); if(!row.getKey().equals("barrier/"+header.intentId()) || !header.clusterId().equals(cluster) || !header.databaseId().equals(database) || !header.domainId().equals(h.definition.domainId())) throw new IllegalArgumentException("CDC retained terminal namespace changed"); }
        }
        if(h.fields.containsKey("closeRequested")) Phase.valueOf(text(h,"closeRequested"));
        else if(h.phase()==Phase.CLOSING || h.fields.containsKey("abandonedDelivery")) throw new IllegalArgumentException("CDC closing state lacks durable intent");
        if(h.fields.containsKey("abandonedDelivery")) {
            var original=DistributedCommandCodec.decode(h.fields.get("abandonedDelivery")); validateCommand(h,original,issuer);
            RatisCdcSubscriptionCatalogue.validator(cluster,database,binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn()).accept(original);
            if(!RatisCdcSubscriptionCatalogue.requiresOwnerAdmission(original) || !DistributedCdcSubscription.decode(original.mutations().getFirst().value()).definition().equals(h.definition)) throw new IllegalArgumentException("CDC abandoned delivery changed identity");
        }
        if(h.fields.containsKey("pendingCommand")) {
            var command=DistributedCommandCodec.decode(h.fields.get("pendingCommand")); validateCommand(h,command,issuer); var action=Action.valueOf(text(h,"action"));
            var next=DistributedCdcSubscription.decode(command.mutations().getFirst().value());
            var actual=command.mutations().size()==2 ? Action.CREATE : next.phase()==DistributedCdcSubscription.Phase.CLOSED ? Action.CLOSE : !next.currentStarts().equals(h.state().currentStarts()) ? Action.HANDOFF : RatisCdcSubscriptionCatalogue.requiresOwnerAdmission(command) ? Action.DELIVER : Action.ACK;
            if(action!=actual || (action==Action.CREATE ? h.phase()!=Phase.CREATE && !(h.phase()==Phase.CLOSING && Phase.CREATE.name().equals(text(h,"closeRequested"))) : !Arrays.equals(command.predicates().getFirst().expectedValue(),h.fields.get("state")))) throw new IllegalArgumentException("CDC frozen action/predecessor changed");
            RatisCdcSubscriptionCatalogue.validator(cluster,database,binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn()).accept(command);
            if(!DistributedCdcSubscription.decode(command.mutations().getFirst().value()).definition().equals(h.definition)) throw new IllegalArgumentException("CDC pending definition changed");
        }
        if(h.fields.containsKey("pendingResult")) { if(!h.fields.containsKey("pendingCommand")) throw new IllegalArgumentException("CDC result lacks original command"); if(CommandResult.decode(h.fields.get("pendingResult")).valueCount()!=0) throw new IllegalArgumentException("CDC pending result returned values"); }
        if(h.fields.containsKey("pendingReceiptAck")) {
            var ack=DistributedCommandCodec.decode(h.fields.get("pendingReceiptAck")); validateCommand(h,ack,issuer);
            if(ack.kind()!=DistributedCommand.Kind.DATA || !ack.mutations().isEmpty() || !ack.predicates().isEmpty() || ack.acknowledgedThrough()!=number(h,"receiptSequence")) throw new IllegalArgumentException("CDC receipt ACK differs from resolved operation");
        }
        if(h.fields.containsKey("pollRequest")) {
            var request=request(h); var before=DistributedCdcSubscription.decode(h.fields.get("batchState")); UUID.fromString(text(h,"batchId"));
            if(!before.definition().equals(h.definition) || !request.binding().clusterId().equals(cluster) || !request.binding().databaseId().equals(database) || !request.binding().domainId().equals(h.definition.domainId()) || !request.from().equals(before.checkpoint().cursors().get(request.binding().lineage()))) throw new IllegalArgumentException("CDC pending poll binding changed");
            var directory=RangeDirectoryCodec.decode(h.fields.get("pollDirectory"));
            if(!directory.clusterId().equals(cluster) || !directory.databaseId().equals(database) || !directory.domainId().equals(h.definition.domainId()) || directory.ranges().stream().noneMatch(r->r.groupId().equals(request.binding().lineage().groupId()) && r.rangeId().equals(request.binding().lineage().rangeId()) && r.rangeEpoch()==request.binding().lineage().rangeEpoch())) throw new IllegalArgumentException("CDC frozen poll geometry changed");
            if(h.fields.containsKey("pollReply")) result(h);
        } else if(h.fields.containsKey("pollReply") || h.fields.containsKey("batchReady")) throw new IllegalArgumentException("CDC batch lacks original poll");
        if(h.fields.containsKey("batchReady")) {
            var result=result(h); if(result.outcome()!=RatisCdcPollRpc.Outcome.DATA || !h.state().delivered().cursors().get(result.request().binding().lineage()).covers(result.next())) throw new IllegalArgumentException("CDC batch lacks persisted DATA delivery");
        }
    }
    private void validateCommand(Handle h,DistributedCommand command,UUID issuer) {
        var op=command.operationId(); if(!op.sessionId().equals(issuer) || op.epoch()!=0 || op.sequence()>=number(h,"sequence") || !command.groupId().equals(binding.groupId()) || command.rangeEpoch()!=binding.epoch() || !Arrays.equals(command.schemaDigest(),binding.schemaDigest())) throw new IllegalArgumentException("CDC journal operation/binding changed");
    }
    private void requireDefinition(DistributedCdcSubscription.Definition definition,RangeDirectory source) {
        if(!definition.clusterId().equals(cluster) || !definition.databaseId().equals(database) || !definition.equals(DistributedCdcSubscription.Definition.create(definition.subscriptionId(),definition.incarnationId(),source,definition.columns(),definition.start()))) throw new IllegalArgumentException("CDC source/namespace differs");
    }
    private OperationId next(Handle h) { long sequence=number(h,"sequence"); number(h,"sequence",Math.incrementExact(sequence)); return new OperationId(UUID.fromString(text(h,"issuer")),0,sequence); }
    private byte[] bindingBytes() { var bytes=ByteBuffer.allocate(88); ScopedTransactionRpc.uuid(bytes,binding.groupId()); bytes.putLong(binding.epoch()).put(binding.schemaDigest()); ScopedTransactionRpc.uuid(bytes,binding.directoryColumn()); ScopedTransactionRpc.uuid(bytes,binding.sessionsColumn()); return bytes.array(); }
    private static byte[] longBytes(long number) { return ByteBuffer.allocate(8).putLong(number).array(); }
    private static long number(Handle h,String key) { return ByteBuffer.wrap(h.fields.get(key)).getLong(); }
    private static void number(Handle h,String key,long value) { h.fields.put(key,longBytes(value)); }
    private static String text(Handle h,String key) { return new String(h.fields.get(key),StandardCharsets.UTF_8); }
    private static void text(Handle h,String key,String value) { h.fields.put(key,value.getBytes(StandardCharsets.UTF_8)); }
    private Path path(UUID id) { return root.resolve(id+".cdc"); }
    private void save(Handle h) throws IOException { try { RatisSchemaClientJournal.writeOwned(path(h.id),h.fields,journalAccess); } catch(IOException failure) { fenced=true; throw failure; } }
    private synchronized void begin() throws IOException { if(closed || fenced || !lock.isValid()) throw new IOException("CDC controller closed/fenced; reopen original journal"); if(busy) throw new IOException("CDC I/O still active"); busy=true; }
    private synchronized void end() { busy=false; notifyAll(); }
    private void requireHandle(Handle h) { if(h==null || h.released || handles.get(h.id)!=h) throw new IllegalArgumentException("Foreign CDC handle"); }
    private void requireActive(Handle h) { if(h.phase()!=Phase.ACTIVE) throw new IllegalStateException("CDC subscription is not active"); }
    @Override public synchronized void close() throws IOException { if(closed) return; if(busy) throw new IOException("CDC I/O still active"); closed=true; handles.values().forEach(h -> h.released=true); handles.clear(); try { lock.release(); } finally { lockChannel.close(); } }
}
