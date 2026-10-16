package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class NativeOperationReceiptTest {
    final UUID group=new UUID(711,1),column=new UUID(711,2);final OperationId operation=new OperationId(new UUID(711,3),1,2);
    final byte[] success=new CommandResult(CommandResult.Status.SUCCESS,List.of()).encode();
    DistributedCommand put(long acknowledged){return new DistributedCommand(group,1,new byte[32],operation,List.of(new Mutation(Mutation.Kind.PUT,column,new byte[]{1},new byte[]{2})),List.of(),acknowledged);}
    TransactionAcceptance audit(){return new TransactionAcceptance(new ApplyPosition(1,1),0,new UUID(711,4),new TransactionScope(List.of(TransactionScope.Span.point(column,new byte[]{1}))),success);}
    @Test void originalSemanticClassificationNeverUsesResultShapeOrActualEffects(){
        var accepted=DistributedCommand.acceptedTransaction(put(-1),audit());
        var predicate=new DistributedCommand(group,1,new byte[32],operation,List.of(),List.of(new KeyPredicate(column,new byte[]{1},null)));
        for(var command:List.of(put(-1),put(1),predicate,accepted,DistributedCommand.readCertification(group,1,new byte[32],operation,audit()),DistributedCommand.rejectAcceptedTransaction(accepted))){
            assertEquals(NativeOperationReceipt.Kind.BUSINESS,NativeOperationReceipt.classify(command));
            for(var status:CommandResult.Status.values()){
                var result=new CommandResult(status,List.of()).encode();var receipt=NativeOperationReceipt.encode(command,result);assertEquals(NativeOperationReceipt.Kind.BUSINESS,NativeOperationReceipt.validate(receipt));assertArrayEquals(result,NativeOperationReceipt.result(receipt,command.receiptDigest()));assertEquals(41,receipt.length);
            }
        }
        var authority=new RangeSessionControl(new SessionAuthority(new UUID(711,5),new UUID(711,6),1,10,1),null,0);
        for(var command:List.of(new DistributedCommand(group,1,new byte[32],operation,List.of(),List.of(),1),DistributedCommand.advanceSessionEpochFloor(group,1,new byte[32],new OperationId(operation.sessionId(),2,operation.sequence()),2),DistributedCommand.sessionControl(group,1,new byte[32],operation,authority,1))){
            assertEquals(NativeOperationReceipt.Kind.CONTROL,NativeOperationReceipt.classify(command));for(var status:CommandResult.Status.values()){var result=new CommandResult(status,List.of()).encode();var receipt=NativeOperationReceipt.encode(command,result);assertEquals(NativeOperationReceipt.Kind.CONTROL,NativeOperationReceipt.validate(receipt));assertArrayEquals(result,NativeOperationReceipt.result(receipt,command.receiptDigest()));}
            assertThrows(IllegalArgumentException.class,()->NativeOperationReceipt.encode(command,new CommandResult(CommandResult.Status.SUCCESS,List.of(new byte[]{4})).encode()));
        }
    }
    @Test void oneCurrentReceiptRejectsUnknownKindsMalformedControlAndDigestReuse(){
        var command=put(-1);var receipt=NativeOperationReceipt.encode(command,success);assertEquals(89,NativeOperationReceipt.KEY_BYTES+receipt.length);
        var unknown=receipt.clone();unknown[0]=2;assertThrows(IllegalArgumentException.class,()->NativeOperationReceipt.validate(unknown));assertThrows(IllegalArgumentException.class,()->NativeOperationReceipt.validate(new byte[32]));assertThrows(IllegalArgumentException.class,()->NativeOperationReceipt.validate(new byte[NativeOperationReceipt.MIN_BYTES-1]));assertThrows(IllegalArgumentException.class,()->NativeOperationReceipt.validate(new byte[NativeOperationReceipt.MAX_BYTES+1]));
        assertThrows(IllegalArgumentException.class,()->NativeOperationReceipt.result(receipt,new byte[32]));var control=receipt.clone();control[0]=1;control[33]=127;assertThrows(IllegalArgumentException.class,()->NativeOperationReceipt.validate(control));
    }
}
