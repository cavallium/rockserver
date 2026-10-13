package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Native catalogue CAS and controlled source/transport; no server, socket or quorum. */
class RatisCdcSubscriptionControllerTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final class Fixture implements AutoCloseable {
        final RatisCdcSubscriptionCatalogueTest support=new RatisCdcSubscriptionCatalogueTest();
        final RatisRangeImportCatalogueTest fixtureOwner=new RatisRangeImportCatalogueTest();
        final RatisRangeImportCatalogueTest.Fixture nativeCatalogue;
        final RangeDirectory directory;
        final UUID selected,unselected=RatisRangeImportCatalogueTest.id(900);
        final RatisNodePlan.Catalogue binding;
        final RaftClient client;
        final RatisRoutedClient routed;
        final RatisCdcSubscriptionCatalogue catalogue;
        final List<byte[]> polls=new ArrayList<>(),prepares=new ArrayList<>(),receiptAcks=new ArrayList<>();
        final Map<String,List<byte[]>> writes=new HashMap<>();
        final Set<String> lose=new HashSet<>();
        RatisCdcPollRpc.Outcome outcome=RatisCdcPollRpc.Outcome.DATA;
        boolean losePoll,competeDelivery,competeCreate,transportClosed,unselectedOnly,expiredQualification,delayedGate;
        int qualifications;
        @FunctionalInterface interface Observer { void run() throws IOException; }
        Observer pollObserver=() -> {},closeObserver=() -> {},cancelObserver=() -> {};
        Fixture(boolean twoRanges) throws Exception {
            fixtureOwner.root=Files.createDirectory(root.resolve("catalogue")); nativeCatalogue=fixtureOwner.new Fixture();
            var original=support.directory(nativeCatalogue); selected=original.columns().iterator().next(); var ranges=original.ranges();
            if(twoRanges) {
                var old=ranges.getFirst(); ranges=List.of(new RangeDescriptor(old.rangeId(),old.groupId(),old.rangeEpoch(),old.configurationVersion(),null,new byte[]{(byte)128},RangeDescriptor.TopologyState.ACTIVE,old.replicas()),
                        new RangeDescriptor(RatisRangeImportCatalogueTest.id(700),RatisRangeImportCatalogueTest.id(701),1,1,new byte[]{(byte)128},null,RangeDescriptor.TopologyState.ACTIVE,old.replicas()));
            }
            directory=new RangeDirectory(original.clusterId(),original.databaseId(),original.domainId(),original.schemaEpoch(),original.schemaDigest(),original.generation(),Set.of(selected,unselected),0,ranges);
            nativeCatalogue.seed(directory); binding=new RatisNodePlan.Catalogue(nativeCatalogue.outer.catalogue,1,new byte[32],nativeCatalogue.outer.directoryColumn,nativeCatalogue.outer.sessions,List.of(RatisRangeImportCatalogueTest.owner(1,ReplicaDescriptor.Role.VOTER)));
            routed=new RatisRoutedClient(directory,new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor descriptor,UUID column,byte[] key) { throw new AssertionError(); }
                public CommandResult command(RangeDescriptor descriptor,DistributedCommand command) { throw new AssertionError(); }
                public byte[] transaction(RangeDescriptor descriptor,byte[] bytes,boolean fresh) throws IOException {
                    assertTrue(fresh); var request=RatisCdcPollRpc.decodeRequest(bytes);
                    boolean qualification=request.maxBytes()==RatisCdcPollRpc.REPLY_BASE_BYTES+1 && request.maxRecords()==1;
                    if(!qualification) { polls.add(bytes.clone()); pollObserver.run(); }
                    var cut=new ApplyPosition(1,5); var beginning=DistributedCdcCheckpoint.Cursor.beginning(); RatisCdcPollRpc.Result result;
                    if(qualification) qualifications++;
                    if(qualification && expiredQualification || !qualification && outcome==RatisCdcPollRpc.Outcome.GAP) result=new RatisCdcPollRpc.Result(request,RatisCdcPollRpc.Outcome.GAP,cut,DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,4)),request.from(),false,0,List.of());
                    else if(!qualification && outcome==RatisCdcPollRpc.Outcome.TOO_LARGE) result=new RatisCdcPollRpc.Result(request,outcome,cut,beginning,request.from(),true,request.maxBytes()+100,List.of());
                    else {
                        var record=record(request.binding().lineage()); var records=request.from().covers(record.end()) ? List.<DistributedCdcRecord>of() : List.of(record);
                        result=new RatisCdcPollRpc.Result(request,RatisCdcPollRpc.Outcome.DATA,cut,beginning,records.isEmpty() ? request.from() : records.getLast().end(),true,0,records);
                    }
                    byte[] reply=RatisCdcPollRpc.reply(result); if(losePoll && !qualification) { losePoll=false; throw new IOException("Lost original poll reply"); } return reply;
                }
                public void close() { transportClosed=true; }
            });
            var authority=new RatisCdcDeliveryAuthority(Map.of(routed.directory().domainId(), routed), domain -> directory); byte[][] prepared={null};
            var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args) -> {
                var message=(Message)args[0]; byte[] bytes=message.getContent().toByteArray();
                if(method.getName().equals("sendReadOnly")) {
                    var packet=ScopedTransactionRpc.parse(bytes,RatisCdcDeliveryRpc.MAX_BYTES);
                    if(packet.kind()==RatisCdcDeliveryRpc.PREPARE) {
                        var command=RatisCdcDeliveryRpc.decode(bytes); nativeCatalogue.pure.accept(command); authority.validate(command); prepares.add(DistributedCommandCodec.encode(command)); prepared[0]=DistributedCommandCodec.encode(command);
                        if(lose.remove("PREPARE")) throw new IOException("Lost exact delivery prepare reply"); return reply(bytes);
                    }
                    if(packet.kind()==RatisCdcDeliveryRpc.CANCEL) {
                        RatisCdcDeliveryRpc.decode(bytes); prepared[0]=null; delayedGate=false; cancelObserver.run();
                        if(lose.remove("CANCEL")) throw new IOException("Lost delivery cancellation reply"); return reply(bytes);
                    }
                    return nativeCatalogue.client.io().sendReadOnly(message);
                }
                if(method.getName().equals("send")) {
                    var command=DistributedCommandCodec.decode(bytes); String action;
                    if(command.mutations().isEmpty()) { receiptAcks.add(bytes.clone()); action="RECEIPT"; }
                    else {
                        action=action(command); writes.computeIfAbsent(action,k -> new ArrayList<>()).add(bytes.clone());
                        if(action.equals("CREATE") && competeCreate) {
                            competeCreate=false; var competing=catalogue().prepareCreate(nativeCatalogue.operation(1),definition(99),directory,null);
                            assertEquals(CommandResult.Status.SUCCESS,catalogue().execute(competing).status());
                        }
                        if(action.equals("CLOSE")) closeObserver.run();
                        if(action.equals("DELIVER")) {
                            assertArrayEquals(bytes,prepared[0],"Delivery append requires its exact prepared command"); prepared[0]=null;
                            if(competeDelivery) {
                                competeDelivery=false; var old=DistributedCdcSubscription.decode(command.predicates().getFirst().expectedValue()); var lineage=old.delivered().cursors().keySet().iterator().next();
                                var competing=catalogue().prepareDelivery(nativeCatalogue.operation(1),old,old.delivered().advance(lineage,DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,4)))); catalogue().execute(competing);
                            }
                        }
                    }
                    if(delayedGate) throw new IOException("Late delivery preparation still owns the catalogue gate");
                    var reply=nativeCatalogue.client.io().send(message); if(lose.remove(action)) throw new IOException("Lost original "+action+" reply"); return reply;
                }
                throw new AssertionError(method);
            });
            client=(RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args) -> switch(method.getName()) { case "getGroupId" -> nativeCatalogue.client.getGroupId(); case "io" -> io; default -> throw new AssertionError(method); });
            catalogue=new RatisCdcSubscriptionCatalogue(client,directory.clusterId(),directory.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn());
        }
        RatisCdcSubscriptionCatalogue catalogue() { return new RatisCdcSubscriptionCatalogue(nativeCatalogue.client,directory.clusterId(),directory.databaseId(),binding.groupId(),1,new byte[32],binding.directoryColumn()); }
        String action(DistributedCommand command) {
            if(command.mutations().size()==2) return "CREATE";
            var next=DistributedCdcSubscription.decode(command.mutations().getFirst().value());
            if(next.phase()==DistributedCdcSubscription.Phase.CLOSED) return "CLOSE";
            return RatisCdcSubscriptionCatalogue.requiresOwnerAdmission(command) ? "DELIVER" : "ACK";
        }
        DistributedCdcRecord record(DistributedCdcCheckpoint.Lineage lineage) {
            var command=new DistributedCommand(lineage.groupId(),lineage.rangeEpoch(),directory.schemaDigest(),new OperationId(RatisRangeImportCatalogueTest.id(990),0,0),
                    List.of(new Mutation(Mutation.Kind.PUT,unselectedOnly ? unselected : selected,new byte[]{1},new byte[]{7}),new Mutation(Mutation.Kind.MERGE,unselected,new byte[]{2},new byte[]{8}),new Mutation(Mutation.Kind.DELETE,unselectedOnly ? unselected : selected,new byte[]{3},null)));
            return new DistributedCdcRecord(lineage,new ApplyPosition(1,3),command,null);
        }
        DistributedCdcSubscription.Definition definition(int id) {
            var start=new HashMap<DistributedCdcCheckpoint.Lineage,DistributedCdcCheckpoint.Cursor>();
            for(var range:directory.ranges()) start.put(new DistributedCdcCheckpoint.Lineage(range.rangeId(),range.groupId(),range.rangeEpoch()),DistributedCdcCheckpoint.Cursor.beginning());
            return DistributedCdcSubscription.Definition.create(new UUID(70,id),new UUID(71,id),directory,List.of(selected),new DistributedCdcCheckpoint(start));
        }
        RatisCdcSubscriptionController controller(Path journal) throws IOException { return new RatisCdcSubscriptionController(journal,directory.clusterId(),directory.databaseId(),binding,client,routed); }
        RaftClientReply reply(byte[] bytes) { return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf("cdc-fixture")).setGroupId(client==null ? nativeCatalogue.client.getGroupId() : client.getGroupId()).setCallId(1).setSuccess(true).setMessage(Message.valueOf(ByteString.copyFrom(bytes))).build(); }
        public void close() throws Exception { routed.close(); nativeCatalogue.close(); }
    }
    @Test void lostCreatePollDeliveryAckAndReceiptRepliesReopenOriginalBatchWithoutImplicitAck() throws Exception {
        try(var f=new Fixture(false)) {
            Path journal=Files.createDirectory(root.resolve("journal")); var definition=f.definition(1);
            try(var c=f.controller(journal)) { f.lose.add("CREATE"); assertThrows(IOException.class,()->c.create(definition,f.directory)); }
            assertEquals(1,f.catalogue.readCapacity().count());
            try(var c=f.controller(journal)) { var h=c.reopen(definition.subscriptionId()); f.losePoll=true; assertThrows(IOException.class,()->c.poll(h,16,4096)); }
            try(var c=f.controller(journal)) { var h=c.reopen(definition.subscriptionId()); f.lose.add("DELIVER"); assertThrows(IOException.class,()->c.poll(h,32,8192)); }
            UUID batchId;
            try(var c=f.controller(journal)) {
                var h=c.reopen(definition.subscriptionId()); var first=c.poll(h,32,8192); batchId=first.batch().id();
                assertArrayEquals(f.writes.get("CREATE").getFirst(),f.writes.get("CREATE").getLast()); assertArrayEquals(f.writes.get("DELIVER").getFirst(),f.writes.get("DELIVER").getLast()); assertArrayEquals(f.polls.getFirst(),f.polls.getLast());
                assertEquals(definition.start(),h.state().checkpoint()); assertNotEquals(h.state().checkpoint(),h.state().delivered());
                var batch=first.batch(); assertEquals(definition,batch.definition()); assertEquals(1,batch.envelopes().size()); assertEquals(2,batch.envelopes().getFirst().events().size()); assertTrue(batch.envelopes().getFirst().events().stream().allMatch(e -> e.mutation().columnId().equals(f.selected)));
                int calls=f.polls.size(); assertEquals(batchId,c.poll(h,1,512).batch().id()); assertEquals(calls,f.polls.size());
                f.lose.add("ACK"); assertThrows(IOException.class,()->c.acknowledge(h,batchId));
            }
            try(var c=f.controller(journal)) {
                var h=c.reopen(definition.subscriptionId()); var state=c.acknowledge(h,batchId); assertEquals(state.checkpoint(),state.delivered()); assertEquals(state,c.acknowledge(h,batchId));
                assertArrayEquals(f.writes.get("ACK").getFirst(),f.writes.get("ACK").getLast()); var empty=c.poll(h,16,4096).batch(); assertTrue(empty.envelopes().isEmpty()); assertEquals(state.checkpoint(),h.state().checkpoint());
                f.lose.add("RECEIPT"); assertThrows(IOException.class,()->c.closeSubscription(h));
            }
            try(var c=f.controller(journal)) {
                var h=c.reopen(definition.subscriptionId()); assertEquals(DistributedCdcSubscription.Phase.CLOSED,c.closeSubscription(h).phase()); assertThrows(IllegalStateException.class,()->c.poll(h,16,4096));
                assertArrayEquals(f.receiptAcks.get(f.receiptAcks.size()-2),f.receiptAcks.getLast()); assertEquals(1,f.catalogue.readCapacity().count());
            }
            assertFalse(f.transportClosed,"Controller close leaves borrowed routed transport alive");
        }
    }
    @Test void gapAndRequiredBudgetDoNotDeliverOrAcknowledgeAndCloseDoesNotAcknowledgeBatch() throws Exception {
        try(var f=new Fixture(false)) {
            Path journal=Files.createDirectory(root.resolve("journal")); try(var c=f.controller(journal)) {
                var h=c.create(f.definition(1),f.directory); var initial=h.state();
                f.outcome=RatisCdcPollRpc.Outcome.GAP; var gap=c.poll(h,16,4096); assertEquals(RatisCdcPollRpc.Outcome.GAP,gap.outcome()); assertNull(gap.batch()); assertEquals(initial,h.state());
                f.outcome=RatisCdcPollRpc.Outcome.TOO_LARGE; var large=c.poll(h,16,4096); assertEquals(4196,large.requiredBytes()); assertNull(large.batch()); assertEquals(initial,h.state()); assertFalse(f.writes.containsKey("DELIVER"));
                f.outcome=RatisCdcPollRpc.Outcome.DATA; var batch=c.poll(h,16,4096).batch(); var delivered=h.state(); assertEquals(initial.checkpoint(),delivered.checkpoint());
                assertThrows(IllegalArgumentException.class,()->c.acknowledge(h,UUID.randomUUID())); assertEquals(batch.id(),c.poll(h,16,4096).batch().id());
                var closed=c.closeSubscription(h); assertEquals(delivered.checkpoint(),closed.checkpoint()); assertEquals(delivered.delivered(),closed.delivered()); assertFalse(f.writes.containsKey("ACK"));
            }
        }
    }
    @Test void explicitAckMovesRoundRobinToNextLineage() throws Exception {
        try(var f=new Fixture(true)) {
            Path journal=Files.createDirectory(root.resolve("journal")); try(var c=f.controller(journal)) {
                var h=c.create(f.definition(1),f.directory); var first=c.poll(h,16,4096).batch(); c.acknowledge(h,first.id()); var second=c.poll(h,16,4096).batch();
                assertNotEquals(first.lineage(),second.lineage()); assertEquals(DistributedCdcCheckpoint.Cursor.beginning(),second.from());
            }
        }
    }
    @Test void definiteDeliveryConflictSurvivesReopenWithoutIssuingAnotherCommand() throws Exception {
        try(var f=new Fixture(false)) {
            Path journal=Files.createDirectory(root.resolve("journal")); UUID id=f.definition(1).subscriptionId();
            try(var c=f.controller(journal)) { var h=c.create(f.definition(1),f.directory); f.competeDelivery=true; assertThrows(RatisCdcSubscriptionController.Conflict.class,()->c.poll(h,16,4096)); assertEquals(RatisCdcSubscriptionController.Phase.CONFLICT,h.phase()); }
            int writes=f.writes.get("DELIVER").size(); try(var c=f.controller(journal)) { var h=c.reopen(id); assertThrows(RatisCdcSubscriptionController.Conflict.class,()->c.poll(h,16,4096)); }
            assertEquals(writes,f.writes.get("DELIVER").size()); assertEquals(DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,4)),f.catalogue.read(id).delivered().cursors().values().iterator().next());
        }
    }
    @Test void jobCapacityAndBlockedPollRetainExclusiveOwnershipUntilActualReturn() throws Exception {
        try(var f=new Fixture(false)) {
            Path journal=Files.createDirectory(root.resolve("journal")); try(var c=f.controller(journal)) {
                var handles=new ArrayList<RatisCdcSubscriptionController.Handle>(); for(int i=0;i<RatisCdcSubscriptionController.MAX_JOBS;i++) handles.add(c.create(f.definition(i),f.directory));
                assertThrows(IOException.class,()->c.create(f.definition(99),f.directory)); assertEquals(RatisCdcSubscriptionController.MAX_JOBS,f.catalogue.readCapacity().count());
                var entered=new CountDownLatch(1); var release=new CountDownLatch(1); var executor=Executors.newSingleThreadExecutor();
                f.pollObserver=() -> { entered.countDown(); try { if(!release.await(10,TimeUnit.SECONDS)) throw new IOException("Controlled poll timed out"); } catch(InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException(interrupted); } };
                try {
                    var future=executor.submit(()->c.poll(handles.getFirst(),16,4096)); assertTrue(entered.await(5,TimeUnit.SECONDS));
                    assertTimeoutPreemptively(java.time.Duration.ofSeconds(2),()->{ assertThrows(IOException.class,c::close); assertThrows(IOException.class,handles.getFirst()::close); assertThrows(IOException.class,()->c.closeSubscription(handles.getFirst())); });
                    assertThrows(IOException.class,()->f.controller(journal)); release.countDown(); assertNotNull(future.get(5,TimeUnit.SECONDS).batch());
                } finally { release.countDown(); executor.shutdownNow(); assertTrue(executor.awaitTermination(5,TimeUnit.SECONDS)); }
            }
        }
    }
    @Test void filteredEmptyDataStillRequiresExplicitAckToAdvanceDurableCursor() throws Exception {
        try(var f=new Fixture(false)) {
            f.unselectedOnly=true; Path journal=Files.createDirectory(root.resolve("journal"));
            try(var c=f.controller(journal)) {
                var h=c.create(f.definition(1),f.directory); var start=h.state().checkpoint(); var batch=c.poll(h,16,4096).batch();
                assertTrue(batch.envelopes().isEmpty()); assertNotEquals(batch.from(),batch.next()); assertEquals(start,h.state().checkpoint());
                assertEquals(batch.id(),c.poll(h,16,4096).batch().id()); var acknowledged=c.acknowledge(h,batch.id());
                assertEquals(batch.next(),acknowledged.checkpoint().cursors().get(batch.lineage())); assertEquals(acknowledged.checkpoint(),acknowledged.delivered());
            }
        }
    }

    @Test void explicitCloseFencesExpiredPendingDeliveryWithoutConsumerAck() throws Exception {
        try(var f=new Fixture(false)) {
            Path journal=Files.createDirectory(root.resolve("journal")); var definition=f.definition(1);
            try(var c=f.controller(journal)) {
                var h=c.create(definition,f.directory); f.pollObserver=()->f.expiredQualification=true;
                assertThrows(IOException.class,()->c.poll(h,16,4096)); int qualifications=f.qualifications;
                byte[] original=RatisSchemaClientJournal.read(journal.resolve(definition.subscriptionId()+".cdc")).get("pendingCommand");
                var closed=c.closeSubscription(h); assertEquals(DistributedCdcSubscription.Phase.CLOSED,closed.phase());
                assertEquals(definition.start(),closed.checkpoint()); assertEquals(definition.start(),closed.delivered());
                assertEquals(qualifications,f.qualifications,"Explicit closure never requalifies expired source data");
                assertEquals(CommandResult.Status.SESSION_EXPIRED,f.catalogue().execute(DistributedCommandCodec.decode(original)).status());
                assertEquals(closed,f.catalogue.read(definition.subscriptionId())); assertFalse(f.writes.containsKey("ACK"));
            }
        }
    }

    @Test void closingIntentAndOriginalCommandsSurviveLostCancellationCloseAndReceiptReplies() throws Exception {
        try(var f=new Fixture(false)) {
            Path journal=Files.createDirectory(root.resolve("journal")); var definition=f.definition(1); byte[] original;
            try(var c=f.controller(journal)) {
                var h=c.create(definition,f.directory); f.pollObserver=()->f.expiredQualification=true;
                assertThrows(IOException.class,()->c.poll(h,16,4096));
                original=RatisSchemaClientJournal.read(journal.resolve(definition.subscriptionId()+".cdc")).get("pendingCommand");
                f.lose.add("CANCEL"); assertThrows(IOException.class,()->c.closeSubscription(h)); assertEquals(RatisCdcSubscriptionController.Phase.CLOSING,h.phase());
            }
            try(var c=f.controller(journal)) {
                var h=c.reopen(definition.subscriptionId()); f.lose.add("CLOSE"); assertThrows(IOException.class,()->c.closeSubscription(h));
                assertEquals(RatisCdcSubscriptionController.Phase.CLOSING,h.phase());
                assertEquals(DistributedCdcSubscription.Phase.CLOSED,f.catalogue.read(definition.subscriptionId()).phase());
            }
            try(var c=f.controller(journal)) {
                var h=c.reopen(definition.subscriptionId()); f.lose.add("RECEIPT"); assertThrows(IOException.class,()->c.closeSubscription(h));
                assertEquals(RatisCdcSubscriptionController.Phase.CLOSED,h.phase()); assertArrayEquals(f.writes.get("CLOSE").getFirst(),f.writes.get("CLOSE").getLast());
            }
            try(var c=f.controller(journal)) {
                var h=c.reopen(definition.subscriptionId()); var closed=c.closeSubscription(h);
                assertEquals(definition.start(),closed.checkpoint()); assertEquals(definition.start(),closed.delivered());
                assertEquals(CommandResult.Status.SESSION_EXPIRED,f.catalogue().execute(DistributedCommandCodec.decode(original)).status());
                assertThrows(IllegalStateException.class,()->c.poll(h,16,4096)); assertFalse(f.writes.containsKey("ACK")); assertEquals(1,f.qualifications);
            }
        }
    }
    @Test void closePreservesDeliveryHiddenByLostReplyWithoutRequalifyingOrAcknowledgingIt() throws Exception {
        try(var f=new Fixture(false)) {
            Path journal=Files.createDirectory(root.resolve("journal")); var definition=f.definition(1);
            try(var c=f.controller(journal)) {
                var h=c.create(definition,f.directory); f.lose.add("DELIVER"); assertThrows(IOException.class,()->c.poll(h,16,4096));
                var delivered=f.catalogue.read(definition.subscriptionId()); assertNotEquals(delivered.checkpoint(),delivered.delivered());
                f.expiredQualification=true; var closed=c.closeSubscription(h);
                assertEquals(delivered.checkpoint(),closed.checkpoint()); assertEquals(delivered.delivered(),closed.delivered());
                assertEquals(DistributedCdcSubscription.Phase.CLOSED,closed.phase()); assertEquals(1,f.qualifications); assertFalse(f.writes.containsKey("ACK"));
            }
        }
    }
    @Test void deliveryWinningCloseCasUsesNewCloseIdentityOnlyAfterDefiniteConflictAndReopen() throws Exception {
        try(var f=new Fixture(false)) {
            Path journal=Files.createDirectory(root.resolve("journal")); var definition=f.definition(1); byte[] failedClose;
            try(var c=f.controller(journal)) {
                var h=c.create(definition,f.directory); f.pollObserver=()->f.expiredQualification=true;
                assertThrows(IOException.class,()->c.poll(h,16,4096));
                var original=DistributedCommandCodec.decode(RatisSchemaClientJournal.read(journal.resolve(definition.subscriptionId()+".cdc")).get("pendingCommand"));
                f.closeObserver=()->{ f.closeObserver=()->{}; assertEquals(CommandResult.Status.SUCCESS,f.catalogue().execute(original).status()); };
                assertThrows(RatisCdcSubscriptionController.Conflict.class,()->c.closeSubscription(h));
                assertEquals(RatisCdcSubscriptionController.Phase.CLOSING,h.phase()); failedClose=f.writes.get("CLOSE").getLast();
            }
            try(var c=f.controller(journal)) {
                var h=c.reopen(definition.subscriptionId()); var closed=c.closeSubscription(h); var successful=DistributedCommandCodec.decode(f.writes.get("CLOSE").getLast());
                assertNotEquals(DistributedCommandCodec.decode(failedClose).operationId(),successful.operationId());
                assertEquals(DistributedCdcSubscription.Phase.CLOSED,closed.phase()); assertEquals(definition.start(),closed.checkpoint()); assertNotEquals(closed.checkpoint(),closed.delivered());
                assertEquals(CommandResult.Status.SESSION_EXPIRED,f.catalogue().execute(DistributedCommandCodec.decode(failedClose)).status());
                assertEquals(closed,f.catalogue.read(definition.subscriptionId())); assertEquals(1,f.qualifications); assertFalse(f.writes.containsKey("ACK"));
            }
        }
    }
    @Test void explicitCloseResolvesPendingCreateBeforePersistingClosure() throws Exception {
        try(var f=new Fixture(false)) {
            Path journal=Files.createDirectory(root.resolve("journal")); var definition=f.definition(1);
            try(var c=f.controller(journal)) { f.lose.add("CREATE"); assertThrows(IOException.class,()->c.create(definition,f.directory)); }
            try(var c=f.controller(journal)) {
                var h=c.reopen(definition.subscriptionId()); var closed=c.closeSubscription(h);
                assertEquals(DistributedCdcSubscription.Phase.CLOSED,closed.phase()); assertEquals(definition.start(),closed.checkpoint());
                assertArrayEquals(f.writes.get("CREATE").getFirst(),f.writes.get("CREATE").getLast()); assertFalse(f.writes.containsKey("DELIVER"));
            }
        }
    }

    @Test void closingConflictedBirthNeverIssuesAnotherCreateAfterLostReplyOrReopen() throws Exception {
        try(var f=new Fixture(false)) {
            Path journal=Files.createDirectory(root.resolve("journal")); var definition=f.definition(1);
            try(var c=f.controller(journal)) {
                f.competeCreate=true; f.lose.add("CREATE"); assertThrows(IOException.class,()->c.create(definition,f.directory));
                var h=c.reopen(definition.subscriptionId()); assertThrows(RatisCdcSubscriptionController.Conflict.class,()->c.closeSubscription(h));
            }
            int creates=f.writes.get("CREATE").size();
            try(var c=f.controller(journal)) {
                var h=c.reopen(definition.subscriptionId()); assertThrows(IOException.class,()->c.closeSubscription(h));
                assertThrows(IOException.class,()->c.closeSubscription(h)); assertEquals(creates,f.writes.get("CREATE").size());
                assertNull(f.catalogue.read(definition.subscriptionId())); assertEquals(1,f.catalogue.readCapacity().count());
            }
        }
    }

    @Test void everyFrozenCloseRetryCancelsLateUnsubmittedDeliveryPreparationBeforeAppend() throws Exception {
        try(var f=new Fixture(false)) {
            Path journal=Files.createDirectory(root.resolve("journal")); var definition=f.definition(1); byte[] originalClose;
            try(var c=f.controller(journal)) {
                var h=c.create(definition,f.directory); f.lose.add("PREPARE"); assertThrows(IOException.class,()->c.poll(h,16,4096));
                f.cancelObserver=()->{ f.cancelObserver=()->{}; f.delayedGate=true; };
                assertThrows(IOException.class,()->c.closeSubscription(h)); assertEquals(RatisCdcSubscriptionController.Phase.CLOSING,h.phase());
                originalClose=f.writes.get("CLOSE").getLast();
            }
            try(var c=f.controller(journal)) {
                var h=c.reopen(definition.subscriptionId()); var closed=c.closeSubscription(h);
                assertEquals(DistributedCdcSubscription.Phase.CLOSED,closed.phase()); assertEquals(definition.start(),closed.checkpoint());
                assertArrayEquals(originalClose,f.writes.get("CLOSE").getLast()); assertFalse(f.delayedGate); assertEquals(1,f.qualifications);
            }
        }
    }

}
