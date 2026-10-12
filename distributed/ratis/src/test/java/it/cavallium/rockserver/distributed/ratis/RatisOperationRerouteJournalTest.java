package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisOperationRerouteJournalTest {
    @TempDir Path root;
    final byte[] binding=new byte[32];final Duration ttl=Duration.ofSeconds(30);
    record Input(RangeDirectory source,DistributedCommand command,RangeDirectory target,OperationRerouteOrigin origin,RatisTopologyOperationOutcomeRpc.Reply absence) {}
    Input input(boolean accepted) {
        UUID cluster=new UUID(1,1),database=new UUID(1,2),domain=new UUID(1,3),column=new UUID(1,4),group=new UUID(1,5);
        var owner=new ReplicaDescriptor(new UUID(2,1),new UUID(2,2),new UUID(2,3),"owner","127.0.0.1:1",ReplicaDescriptor.Role.VOTER);
        var parent=new RangeDescriptor(group,group,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(owner));
        var source=new RangeDirectory(cluster,database,domain,1,new byte[32],1,Set.of(column),1,List.of(parent));
        var left=new RangeDescriptor(new UUID(3,1),new UUID(3,2),1,0,null,new byte[]{64},RangeDescriptor.TopologyState.ACTIVE,List.of(owner));var right=new RangeDescriptor(new UUID(3,3),new UUID(3,4),1,0,new byte[]{64},null,RangeDescriptor.TopologyState.ACTIVE,List.of(owner));
        var target=new RangeDirectory(cluster,database,domain,1,new byte[32],2,Set.of(column),1,List.of(left,right));var definition=new TopologyChange.Definition(new UUID(3,5),source,target);
        var operation=new OperationId(new UUID(4,1),1,7);var command=new DistributedCommand(group,1,new byte[32],operation,List.of(new Mutation(Mutation.Kind.PUT,column,new byte[]{1},new byte[]{9})));
        if(accepted)command=DistributedCommand.acceptedTransaction(command,new TransactionAcceptance(new ApplyPosition(1,1),0,new UUID(4,2),new TransactionScope(List.of(TransactionScope.Span.point(column,new byte[]{1}))),new CommandResult(CommandResult.Status.SUCCESS,List.of()).encode()));
        var freeze=new NativeTopologyFreeze(definition.header(),group,new ApplyPosition(1,2),NativeTopologyFreeze.boundsDigest(null,null),new byte[32]);
        var lineage=new DistributedCdcCheckpoint.Lineage(group,group,1);var request=new RatisTopologyOperationOutcomeRpc.Request(cluster,database,domain,lineage,operation,command.receiptDigest());
        var absence=new RatisTopologyOperationOutcomeRpc.Reply(request,RatisTopologyOperationOutcomeRpc.Outcome.ABSENT,new ApplyPosition(1,2),freeze,1,0,new byte[]{1},null);
        var origin=new OperationRerouteOrigin(cluster,database,domain,lineage,operation,command.receiptDigest(),OperatorUpload.sha256(freeze.encode()),absence.cut(),target.payloadDigest(),RatisOperationRerouteAdmissionRpc.normalizedDigest(command,target),RatisOperationRerouteAdmissionRpc.targetParticipantCount(command,target),new OperationId(new UUID(4,3),2,0));return new Input(source,command,target,origin,absence);
    }
    @Test void emptyAcceptedReadCertificationPreservesItsOriginalAudit()throws Exception {
        var base=input(true);var command=DistributedCommand.readCertification(base.command().groupId(),base.command().rangeEpoch(),base.command().schemaDigest(),base.command().operationId(),base.command().acceptance());
        var old=base.origin();var request=new RatisTopologyOperationOutcomeRpc.Request(old.clusterId(),old.databaseId(),old.domainId(),old.source(),old.operation(),command.receiptDigest());var saved=base.absence();
        var absence=new RatisTopologyOperationOutcomeRpc.Reply(request,saved.outcome(),saved.cut(),saved.freeze(),saved.configurationTerm(),saved.configurationIndex(),saved.configuration(),null);
        var origin=new OperationRerouteOrigin(old.clusterId(),old.databaseId(),old.domainId(),old.source(),old.operation(),command.receiptDigest(),old.absenceCommitment(),old.absenceCut(),old.targetDirectoryDigest(),RatisOperationRerouteAdmissionRpc.normalizedDigest(command,base.target()),RatisOperationRerouteAdmissionRpc.targetParticipantCount(command,base.target()),old.firstControl());
        Path path=root.resolve("read-certification.bin");try(var ignored=create(path,new Input(base.source(),command,base.target(),origin,absence))){}
        try(var journal=RatisOperationRerouteJournal.reopen(path,binding)){assertTrue(journal.originalCommand().readCertification());assertTrue(journal.originalCommand().mutations().isEmpty());assertArrayEquals(DistributedCommandCodec.encode(command),DistributedCommandCodec.encode(journal.originalCommand()));assertArrayEquals(ScopedTransactionRpc.scopeBytes(command.acceptance().scope()),ScopedTransactionRpc.scopeBytes(journal.originalCommand().acceptance().scope()));}
    }
    static RangeDirectory preparedSource(RatisRoutedClient.PreparedCommand prepared)throws Exception {var field=RatisRoutedClient.PreparedCommand.class.getDeclaredField("source");field.setAccessible(true);return (RangeDirectory)field.get(prepared);}
    RatisOperationRerouteJournal create(Path path,Input input)throws IOException{return RatisOperationRerouteJournal.create(path,binding,input.source(),input.command(),input.target(),input.origin(),input.absence(),ttl);}
    @Test void forcedAcceptedInputReopensExactlyAndHasExclusiveStableOwnership()throws Exception {
        var input=input(true);Path path=root.resolve("input.bin");byte[] bytes;
        try(var journal=create(path,input)) {
            bytes=Files.readAllBytes(path);assertEquals(path.resolveSibling("input.bin.atomic"),journal.atomicPath());assertThrows(IOException.class,()->RatisOperationRerouteJournal.reopen(path,binding));
            assertArrayEquals(DistributedCommandCodec.encode(input.command()),DistributedCommandCodec.encode(journal.originalCommand()));
        }
        try(var journal=RatisOperationRerouteJournal.reopen(path,binding)) {
            journal.requireInput(input.source(),input.command(),input.target(),input.origin(),input.absence(),ttl);assertEquals(ttl,journal.captureTtl());assertArrayEquals(input.origin().encode(),journal.origin().encode());assertArrayEquals(RatisTopologyOperationOutcomeRpc.reply(input.absence()),RatisTopologyOperationOutcomeRpc.reply(journal.absence()));assertArrayEquals(bytes,Files.readAllBytes(path));
            try(var routed=new RatisRoutedClient(input.source(),new RatisRoutedClient.Transport(){public byte[] read(RangeDescriptor range,UUID column,byte[] key){throw new AssertionError();}public CommandResult command(RangeDescriptor range,DistributedCommand command){throw new AssertionError();}})){routed.refresh(input.target());var restored=journal.original(routed);assertArrayEquals(input.source().payloadDigest(),preparedSource(restored).payloadDigest());assertArrayEquals(DistributedCommandCodec.encode(input.command()),DistributedCommandCodec.encode(restored.command()));}
        }
    }
    @Test void changedBindingAndOriginalReceiptAreRejectedWithoutChangingSavedInput()throws Exception {
        var input=input(false);Path path=root.resolve("binding.bin");try(var ignored=create(path,input)){}byte[] saved=Files.readAllBytes(path),wrong=binding.clone();wrong[0]=1;
        assertThrows(IOException.class,()->RatisOperationRerouteJournal.reopen(path,wrong));
        var changed=new DistributedCommand(input.command().groupId(),1,new byte[32],input.command().operationId(),List.of(new Mutation(Mutation.Kind.PUT,input.source().columns().iterator().next(),new byte[]{1},new byte[]{8})));
        assertThrows(IllegalArgumentException.class,()->create(root.resolve("changed.bin"),new Input(input.source(),changed,input.target(),input.origin(),input.absence())));
        try(var journal=RatisOperationRerouteJournal.reopen(path,binding)){assertThrows(IOException.class,()->journal.requireInput(input.source(),input.command(),input.target(),input.origin(),input.absence(),Duration.ofSeconds(20)));}assertArrayEquals(saved,Files.readAllBytes(path));
    }
    @Test void unknownPostPublicationFailureStillRecoversOriginalInput()throws Exception {
        var input=input(true);Path path=root.resolve("unknown.bin");
        assertThrows(IOException.class,()->RatisOperationRerouteJournal.create(path,binding,input.source(),input.command(),input.target(),input.origin(),input.absence(),ttl,(file,body,maximum)->{SessionJournalFiles.write(file,body,maximum);throw new IOException("Lost force confirmation");}));
        try(var journal=RatisOperationRerouteJournal.reopen(path,binding)){journal.requireInput(input.source(),input.command(),input.target(),input.origin(),input.absence(),ttl);assertNotNull(journal.originalCommand().acceptance());}
    }
    @Test void originalHistoricalDirectoryRemainsDistinctFromLaterFreezeAuthority()throws Exception {
        var input=input(false);var source=input.source();var historical=new RangeDirectory(source.clusterId(),source.databaseId(),source.domainId(),source.schemaEpoch(),source.schemaDigest(),0,source.columns(),source.routingPrefixBytes(),source.ranges());
        var saved=new Input(historical,input.command(),input.target(),input.origin(),input.absence());Path path=root.resolve("historical.bin");try(var ignored=create(path,saved)){}
        try(var journal=RatisOperationRerouteJournal.reopen(path,binding)){assertEquals(0,journal.sourceDirectory().generation());assertEquals(1,journal.absence().freeze().header().sourceGeneration());journal.requireInput(saved.source(),saved.command(),saved.target(),saved.origin(),saved.absence(),ttl);}
    }
    @Test void durableMainOwnsOnlyAnExactlyMatchingValidatedSidecar()throws Exception {
        var input=input(false);Path path=root.resolve("sidecar.bin"),pending=path.resolveSibling("sidecar.bin.pending");try(var ignored=create(path,input)){}var exact=Files.readAllBytes(path);Files.write(pending,exact);
        try(var recovered=RatisOperationRerouteJournal.reopen(path,binding)){assertFalse(Files.exists(pending));recovered.requireInput(input.source(),input.command(),input.target(),input.origin(),input.absence(),ttl);}
        Files.writeString(pending,"invalid");assertThrows(IOException.class,()->RatisOperationRerouteJournal.reopen(path,binding));assertArrayEquals(exact,Files.readAllBytes(path));assertEquals("invalid",Files.readString(pending));
    }
    @Test void corruptOversizedAndUnpublishedInputsNeverBecomeRecoverableAuthority()throws Exception {
        var input=input(false);Path path=root.resolve("corrupt.bin");try(var ignored=create(path,input)){}byte[] bytes=Files.readAllBytes(path);bytes[50]^=1;Files.write(path,bytes);assertThrows(IOException.class,()->RatisOperationRerouteJournal.reopen(path,binding));
        Path huge=root.resolve("huge.bin");try(var file=java.nio.channels.FileChannel.open(huge,StandardOpenOption.CREATE,StandardOpenOption.WRITE)){file.position(RatisOperationRerouteJournal.MAX_BYTES);file.write(java.nio.ByteBuffer.wrap(new byte[]{0}));}assertThrows(IOException.class,()->RatisOperationRerouteJournal.reopen(huge,binding));
        Path pending=root.resolve("pending.bin");Files.writeString(pending.resolveSibling("pending.bin.pending"),"invalid");assertThrows(IOException.class,()->create(pending,input));assertThrows(IOException.class,()->RatisOperationRerouteJournal.reopen(pending,binding));
    }
}
