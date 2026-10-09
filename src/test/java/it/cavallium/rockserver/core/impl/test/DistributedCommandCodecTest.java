package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class DistributedCommandCodecTest {
    private static final UUID GROUP=new UUID(1,2), COLUMN=new UUID(3,4);
    private static final OperationId OPERATION=new OperationId(new UUID(5,6),7,8);
    private static Mutation put() { return new Mutation(Mutation.Kind.PUT,COLUMN,new byte[] {10},new byte[] {11}); }
    private static DistributedCommand data(List<Mutation> mutations) { return new DistributedCommand(GROUP,9,new byte[32],OPERATION,mutations); }
    private static TransactionAcceptance audit() { return new TransactionAcceptance(new ApplyPosition(1,0),0,new UUID(7,8),new TransactionScope(List.of(TransactionScope.Span.range(COLUMN,null,null))),new CommandResult(CommandResult.Status.SUCCESS,List.of()).encode()); }

    @Test void oneCurrentFormatRoundTripsEverySemanticKind() throws Exception {
        var base=data(List.of(put())); var accepted=DistributedCommand.acceptedTransaction(base,audit());
        var control=new RangeSessionControl(new SessionAuthority(new UUID(9,1),new UUID(9,2),1,10,1),null,0);
        var commands=List.of(base,DistributedCommand.advanceSessionEpochFloor(GROUP,9,new byte[32],OPERATION,2),
                DistributedCommand.sessionControl(GROUP,9,new byte[32],OPERATION,control,1),accepted,DistributedCommand.rejectAcceptedTransaction(accepted));
        assertEquals(List.of(DistributedCommand.Kind.values()),commands.stream().map(DistributedCommand::kind).toList());
        for (var command:commands) {
            byte[] bytes=DistributedCommandCodec.encode(command); var copy=DistributedCommandCodec.decode(bytes);
            assertEquals(1,ByteBuffer.wrap(bytes).getInt()); assertEquals(command.kind(),copy.kind()); assertEquals(DistributedCommandCodec.encodedSize(command),bytes.length);
            assertArrayEquals(bytes,DistributedCommandCodec.encode(copy)); assertArrayEquals(MessageDigest.getInstance("SHA-256").digest(bytes),copy.payloadDigest());
            assertEquals(command.operationId(),copy.operationId()); assertEquals(command.acknowledgedThrough(),copy.acknowledgedThrough());
        }
        var ack=new DistributedCommand(GROUP,9,new byte[32],OPERATION,List.of(),List.of(),7);
        assertEquals(DistributedCommand.Kind.DATA,ack.kind()); assertEquals(109,DistributedCommandCodec.encode(ack).length);
        assertArrayEquals(DistributedCommandCodec.encode(ack),DistributedCommandCodec.encode(DistributedCommandCodec.decode(DistributedCommandCodec.encode(ack))));
    }

    @Test void orderedRawOperandsReturnsPredicatesAndOwnedArraysAreCanonical() {
        var ordered=List.of(put(),new Mutation(Mutation.Kind.MERGE,COLUMN,new byte[] {10},new byte[] {1,2},Mutation.ReturnMode.CURRENT),
                new Mutation(Mutation.Kind.MERGE,COLUMN,new byte[] {10},new byte[] {2,1},Mutation.ReturnMode.PREVIOUS),new Mutation(Mutation.Kind.DELETE,COLUMN,new byte[0],null));
        var command=new DistributedCommand(GROUP,9,new byte[32],OPERATION,ordered,List.of(new KeyPredicate(COLUMN,new byte[] {10},null),new KeyPredicate(COLUMN,new byte[] {11},new byte[0])),1);
        var copy=DistributedCommandCodec.decode(DistributedCommandCodec.encode(command)); assertEquals(ordered,copy.mutations()); assertEquals(command.predicates(),copy.predicates());
        var reversed=new ArrayList<>(ordered); Collections.reverse(reversed); assertFalse(Arrays.equals(command.payloadDigest(),data(reversed).payloadDigest()));
        byte[] key={1}, value={2}, schema=new byte[32]; var mutations=new ArrayList<>(List.of(new Mutation(Mutation.Kind.PUT,COLUMN,key,value)));
        var owned=new DistributedCommand(GROUP,9,schema,OPERATION,mutations); byte[] wire=DistributedCommandCodec.encode(owned);
        key[0]=9; value[0]=9; schema[0]=9; mutations.clear(); owned.schemaDigest()[0]=9; owned.mutations().getFirst().key()[0]=9;
        assertArrayEquals(wire,DistributedCommandCodec.encode(owned)); assertThrows(UnsupportedOperationException.class,() -> owned.mutations().clear());
        var range=data(List.of(new Mutation(Mutation.Kind.DELETE_RANGE,COLUMN,new byte[] {127},new byte[] {(byte)128})));
        assertEquals(range.mutations(),DistributedCommandCodec.decode(DistributedCommandCodec.encode(range)).mutations());
    }

    @Test void decoderRejectsUnknownFormatKindsShapesTruncationAndImpossibleLengths() {
        byte[] valid=DistributedCommandCodec.encode(data(List.of(put())));
        for (int length=0;length<valid.length;length++) { byte[] truncated=Arrays.copyOf(valid,length); assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(truncated)); }
        assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(Arrays.copyOf(valid,valid.length+1)));
        for (int version:new int[] {0,2,3,4,5,6,7,8,Integer.MAX_VALUE}) { byte[] bad=valid.clone(); ByteBuffer.wrap(bad).putInt(version); assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(bad)); }
        for (int tag:new int[] {0,2,3,5,127}) { byte[] bad=valid.clone(); bad[4]=(byte)tag; assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(bad)); }
        for (int offset:new int[] {101,105,127,132}) { byte[] bad=valid.clone(); ByteBuffer.wrap(bad).putInt(offset,Integer.MAX_VALUE); assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(bad)); }
        for (int offset:new int[] {21,77,85,93}) { byte[] bad=valid.clone(); ByteBuffer.wrap(bad).putLong(offset,-2); assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(bad)); }
        for (int offset:new int[] {109,110}) { byte[] bad=valid.clone(); bad[offset]=127; assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(bad)); }
        assertThrows(IllegalArgumentException.class,() -> new DistributedCommand(GROUP,9,new byte[32],OPERATION,List.of(),List.of()));
        var readOnly=new DistributedCommand(GROUP,9,new byte[32],OPERATION,List.of(),List.of(new KeyPredicate(COLUMN,new byte[0],null)),0);
        for (var command:List.of(readOnly,DistributedCommand.acceptedTransaction(readOnly,audit()))) assertArrayEquals(DistributedCommandCodec.encode(command),DistributedCommandCodec.encode(DistributedCommandCodec.decode(DistributedCommandCodec.encode(command))));
        for (long ack:new long[] {-2,8,Long.MAX_VALUE}) assertThrows(IllegalArgumentException.class,() -> new DistributedCommand(GROUP,9,new byte[32],OPERATION,List.of(put()),List.of(),ack));
    }

    @Test void itemAndEnvelopeBoundsAreExactAndControlBodiesCannotCarryData() {
        int size=DistributedCommandCodec.MAX_ENCODED_BYTES-DistributedCommandCodec.HEADER_BYTES-26;
        var maximum=data(List.of(new Mutation(Mutation.Kind.PUT,COLUMN,new byte[0],new byte[size]))); assertEquals(DistributedCommandCodec.MAX_ENCODED_BYTES,DistributedCommandCodec.encode(maximum).length);
        assertThrows(IllegalArgumentException.class,() -> data(List.of(new Mutation(Mutation.Kind.PUT,COLUMN,new byte[0],new byte[size+1]))));
        int expectedBytes=DistributedCommandCodec.MAX_ENCODED_BYTES-DistributedCommandCodec.HEADER_BYTES-28-25;
        var conditionMaximum=new DistributedCommand(GROUP,9,new byte[32],OPERATION,List.of(put()),List.of(new KeyPredicate(COLUMN,new byte[0],new byte[expectedBytes])));
        assertEquals(DistributedCommandCodec.MAX_ENCODED_BYTES,DistributedCommandCodec.encode(conditionMaximum).length);
        assertArrayEquals(DistributedCommandCodec.encode(conditionMaximum),DistributedCommandCodec.encode(DistributedCommandCodec.decode(DistributedCommandCodec.encode(conditionMaximum))));
        var maximumItems=data(Collections.nCopies(DistributedCommandCodec.MAX_ITEMS,put()));
        byte[] maximumItemsBytes=DistributedCommandCodec.encode(maximumItems);
        assertArrayEquals(maximumItemsBytes,DistributedCommandCodec.encode(DistributedCommandCodec.decode(maximumItemsBytes)));
        assertThrows(IllegalArgumentException.class,() -> data(Collections.nCopies(DistributedCommandCodec.MAX_ITEMS+1,put())));
        var floor=DistributedCommandCodec.encode(DistributedCommand.advanceSessionEpochFloor(GROUP,9,new byte[32],OPERATION,2));
        for (int offset:new int[] {101,105}) { byte[] bad=floor.clone(); ByteBuffer.wrap(bad).putInt(offset,1); assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(bad)); }
        byte[] badFloor=floor.clone(); ByteBuffer.wrap(badFloor).putLong(109,-1); assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(badFloor));
        var control=new RangeSessionControl(new SessionAuthority(new UUID(9,1),new UUID(9,2),1,10,1),null,0);
        byte[] bytes=DistributedCommandCodec.encode(DistributedCommand.sessionControl(GROUP,9,new byte[32],OPERATION,control,-1));
        ByteBuffer.wrap(bytes).putInt(109,Integer.MAX_VALUE); assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(bytes));
    }
}
