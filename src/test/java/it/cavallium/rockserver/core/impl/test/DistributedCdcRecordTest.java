package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** Current typed record codec; native atomic production and public projection are separate checks. */
class DistributedCdcRecordTest {
    static UUID id(long n) { return new UUID(0,n); }
    NativeTopologyFreeze freeze() {
        var helper=new NativeTopologyFreezeTest(); var definition=helper.split(helper.outer.schema(),100); var parent=definition.parents().getFirst();
        byte[] admission=new byte[32]; admission[0]=7;
        return new NativeTopologyFreeze(definition.header(),parent.groupId(),new ApplyPosition(2,9),NativeTopologyFreeze.boundsDigest(parent.lowerInclusive(),parent.upperExclusive()),admission);
    }
    DistributedCdcRecord mutations() {
        var lineage=new DistributedCdcCheckpoint.Lineage(id(10),id(11),3); byte[] schema=new byte[32]; schema[0]=4;
        var command=new DistributedCommand(lineage.groupId(),lineage.rangeEpoch(),schema,new OperationId(id(12),1,0),List.of(
                new Mutation(Mutation.Kind.PUT,id(13),new byte[]{1},new byte[]{2}),new Mutation(Mutation.Kind.MERGE,id(13),new byte[]{3},new byte[]{4})));
        return new DistributedCdcRecord(lineage,new ApplyPosition(2,5),command,null);
    }
    @Test void terminalBoundaryHasOnlyOriginalParentFreezeIdentityAndNoMutationOperation() {
        var freeze=freeze(); var record=DistributedCdcRecord.terminalBarrier(freeze); var parent=freeze.header().participant(TopologyChange.Stage.FREEZE,freeze.groupId());
        assertEquals(DistributedCdcRecord.Kind.TERMINAL_BARRIER,record.kind()); assertNull(record.command()); assertNull(record.finalization()); assertEquals(0,record.mutationCount()); assertTrue(record.events().isEmpty());
        assertEquals(new DistributedCdcCheckpoint.Lineage(parent.rangeId(),parent.groupId(),parent.rangeEpoch()),record.lineage()); assertEquals(freeze.position(),record.position()); assertEquals(DistributedCdcCheckpoint.Cursor.after(freeze.position()),record.end());
        assertArrayEquals(freeze.encode(),record.barrier().encode()); assertArrayEquals(freeze.header().schemaDigest(),record.schemaDigest());
        byte[] schema=record.schemaDigest(); schema[0]^=1; assertArrayEquals(freeze.header().schemaDigest(),record.schemaDigest());
        byte[] encoded=record.encode(); assertEquals(DistributedCdcRecord.HEADER_BYTES+freeze.encode().length,encoded.length); assertEquals(encoded.length,record.encodedSize());
        var decoded=DistributedCdcRecord.decode(encoded); assertArrayEquals(encoded,decoded.encode()); assertArrayEquals(freeze.encode(),decoded.barrier().encode()); assertEquals(record.lineage(),decoded.lineage()); assertNull(decoded.command());
        assertEquals(DistributedCdcRecord.HEADER_BYTES,DistributedCdcRecord.MIN_BYTES);
        assertEquals(DistributedCdcRecord.HEADER_BYTES+Math.max(DistributedCommandCodec.MAX_ENCODED_BYTES+ParticipantFinalization.MAX_BYTES,NativeTopologyFreeze.MAX_BYTES),DistributedCdcRecord.MAX_BYTES);
    }
    @Test void mutationEnvelopeKeepsItsActualOperationsOperandsAndEventOrdinals() {
        var record=mutations(); assertEquals(DistributedCdcRecord.Kind.MUTATIONS,record.kind()); assertNull(record.barrier()); assertEquals(2,record.mutationCount()); assertEquals(2,record.events().size());
        assertEquals(record.command().operationId(),DistributedCdcRecord.decode(record.encode()).command().operationId());
        assertEquals(0,record.events().getFirst().cursor().mutationOrdinal()); assertEquals(1,record.events().getLast().cursor().mutationOrdinal()); assertArrayEquals(new byte[]{4},record.events().getLast().mutation().value());
        assertEquals(record.encode().length,record.encodedSize()); assertArrayEquals(record.encode(),DistributedCdcRecord.decode(record.encode()).encode());
        assertThrows(IllegalArgumentException.class,()->new DistributedCdcRecord(record.lineage(),new ApplyPosition(0,0),record.command(),null));
        assertThrows(IllegalArgumentException.class,()->new DistributedCdcRecord(new DistributedCdcCheckpoint.Lineage(id(10),id(99),3),record.position(),record.command(),null));
    }
    @Test void mixedKindsForgedOuterLineageAndCutOrMalformedFramesFailClosed() {
        byte[] barrier=DistributedCdcRecord.terminalBarrier(freeze()).encode(),mutation=mutations().encode();
        for(int offset:new int[]{1,17,33,41,49}) {
            byte[] changed=barrier.clone(); changed[offset]^=1; assertThrows(IllegalArgumentException.class,()->DistributedCdcRecord.decode(changed));
        }
        byte[] changedInnerCut=barrier.clone(); int headerLength=ByteBuffer.wrap(barrier).getInt(DistributedCdcRecord.HEADER_BYTES+4);
        ByteBuffer.wrap(changedInnerCut).putLong(DistributedCdcRecord.HEADER_BYTES+8+headerLength+16,3);
        assertThrows(IllegalArgumentException.class,()->DistributedCdcRecord.decode(changedInnerCut));
        byte[] wrongKind=barrier.clone(); wrongKind[0]=(byte)DistributedCdcRecord.Kind.MUTATIONS.ordinal(); assertThrows(IllegalArgumentException.class,()->DistributedCdcRecord.decode(wrongKind));
        byte[] crossed=mutation.clone(); crossed[0]=(byte)DistributedCdcRecord.Kind.TERMINAL_BARRIER.ordinal(); assertThrows(IllegalArgumentException.class,()->DistributedCdcRecord.decode(crossed));
        byte[] unknown=barrier.clone(); unknown[0]=(byte)255; assertThrows(IllegalArgumentException.class,()->DistributedCdcRecord.decode(unknown));
        byte[] finalization=Arrays.copyOf(barrier,barrier.length+1); ByteBuffer.wrap(finalization).putInt(61,1); assertThrows(IllegalArgumentException.class,()->DistributedCdcRecord.decode(finalization));
        for(int length:new int[]{0,DistributedCdcRecord.MIN_BYTES-1,DistributedCdcRecord.MIN_BYTES,barrier.length-1}) assertThrows(IllegalArgumentException.class,()->DistributedCdcRecord.decode(Arrays.copyOf(barrier,length)));
        assertThrows(IllegalArgumentException.class,()->DistributedCdcRecord.decode(Arrays.copyOf(barrier,barrier.length+1)));
        assertThrows(IllegalArgumentException.class,()->DistributedCdcRecord.decode(Arrays.copyOfRange(mutation,1,mutation.length)),"The former untyped layout is not a current record");
    }
    @Test void originalFreezePayloadRetainsNamespaceAndDigestQualification() {
        var original=freeze(); byte[] bytes=DistributedCdcRecord.terminalBarrier(original).encode();
        var body=ByteBuffer.wrap(bytes,DistributedCdcRecord.HEADER_BYTES,bytes.length-DistributedCdcRecord.HEADER_BYTES).slice(); int headerBytes=body.getInt(4);
        // Header namespace and commitments remain owned by the typed FREEZE, rather than a fabricated DATA operation.
        for(int offset:new int[]{20,36,52,93,125,157,189}) {
            byte[] changed=bytes.clone(); changed[DistributedCdcRecord.HEADER_BYTES+8+offset]^=1;
            var decoded=DistributedCdcRecord.decode(changed);
            assertThrows(IllegalArgumentException.class,()->decoded.barrier().requireRequest(original.header(),original.admissionDigest()));
        }
        assertTrue(headerBytes<=TopologyChange.MAX_HEADER_BYTES); assertArrayEquals(original.admissionDigest(),DistributedCdcRecord.decode(bytes).barrier().admissionDigest());
    }
}
