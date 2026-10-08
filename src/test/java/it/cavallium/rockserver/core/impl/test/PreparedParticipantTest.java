package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class PreparedParticipantTest {
    final UUID group=new UUID(0,1),column=new UUID(0,2),home=new UUID(0,3),transaction=new UUID(0,4);
    final OperationId businessId=new OperationId(new UUID(0,5),1,0),prepareId=new OperationId(new UUID(0,6),1,0);
    DistributedCommand base() { return new DistributedCommand(group,1,new byte[32],businessId,List.of(new Mutation(Mutation.Kind.PUT,column,new byte[]{1},new byte[]{2},Mutation.ReturnMode.PREVIOUS))); }
    TransactionStatus registration(byte[] digest) { return TransactionStatus.begin(new UUID(0,7),new UUID(0,8),new UUID(0,9),transaction,businessId,1,new byte[32],new byte[32],List.of(new TransactionStatus.Participant(group,group,1,new byte[32],digest,null,false))); }
    DistributedCommand accepted() {
        var scope=new TransactionScope(List.of(TransactionScope.Span.range(column,new byte[]{0},new byte[]{3})));
        return DistributedCommand.acceptedTransaction(base(),new TransactionAcceptance(new ApplyPosition(1,0),2,new UUID(0,10),scope,new CommandResult(CommandResult.Status.SUCCESS,Collections.singletonList(null)).encode()));
    }
    PreparedParticipant.Preparation preparation() { return new PreparedParticipant.Preparation(home,1,prepareId,registration(base().payloadDigest()),accepted()); }
    @Test void preparationAndDurableReceiptHaveCanonicalOwnedValueSemantics() {
        var preparation=preparation(); byte[] bytes=preparation.encode();
        assertEquals(68+preparation.registration().encode().length+DistributedCommandCodec.encodedSize(preparation.business()),bytes.length);
        var decoded=PreparedParticipant.Preparation.decode(bytes); assertEquals(preparation,decoded); assertEquals(preparation.hashCode(),decoded.hashCode()); Arrays.fill(bytes,(byte)1); assertEquals(preparation,decoded);
        var record=new PreparedParticipant(preparation,new ApplyPosition(1,1)); assertEquals(record,PreparedParticipant.decode(record.encode()));
        assertTrue(preparation.ownsOperation(businessId)); assertTrue(preparation.ownsOperation(prepareId));
        assertThrows(IllegalArgumentException.class,() -> new PreparedParticipant.Preparation(home,1,businessId,preparation.registration(),preparation.business()));
    }
    @Test void frozenRegistrationAndSeparateOperationIdentitiesCannotBeSubstituted() {
        assertThrows(IllegalArgumentException.class,() -> new PreparedParticipant.Preparation(home,1,businessId,registration(base().payloadDigest()),accepted()));
        byte[] changed=new byte[32]; changed[0]=1;
        assertThrows(IllegalArgumentException.class,() -> new PreparedParticipant.Preparation(home,1,prepareId,registration(changed),accepted()));
        assertThrows(IllegalArgumentException.class,() -> new PreparedParticipant.Preparation(home,1,prepareId,registration(base().payloadDigest()).decide(TransactionStatus.Phase.ABORT),accepted()));
        assertThrows(IllegalArgumentException.class,() -> new PreparedParticipant.Preparation(home,1,prepareId,registration(base().payloadDigest()),base()));
        assertThrows(IllegalArgumentException.class,() -> new PreparedParticipant(preparation(),new ApplyPosition(1,0)));
        assertThrows(IllegalArgumentException.class,() -> new PreparedParticipant(preparation(),new ApplyPosition(2,1)));
    }
    @Test void decoderPreflightsLengthsAndRejectsTruncationAndUnsupportedEncoding() {
        byte[] bytes=preparation().encode();
        for (int n=0;n<bytes.length;n++) { byte[] shortBody=Arrays.copyOf(bytes,n); assertThrows(IllegalArgumentException.class,() -> PreparedParticipant.Preparation.decode(shortBody)); }
        for (int offset:new int[]{0,60,64+preparation().registration().encode().length}) {
            byte[] invalid=bytes.clone(); ByteBuffer.wrap(invalid).putInt(offset,-1); assertThrows(IllegalArgumentException.class,() -> PreparedParticipant.Preparation.decode(invalid));
        }
        assertThrows(IllegalArgumentException.class,() -> PreparedParticipant.Preparation.decode(Arrays.copyOf(bytes,bytes.length+1)));
    }
}
