package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class OperatorUploadTest {
    @Test void exactDescriptorAndChunkCoverageRoundTripWithoutPathsOrAdmissionFlags() {
        byte[] bytes=new byte[OperatorUpload.CHUNK_BYTES+7]; new Random(3).nextBytes(bytes);
        var operator=new DistributedSchema.Operator("sum",7,"example.Sum",OperatorUpload.sha256(bytes),2,3,"integer-addition",4);
        var upload=OperatorUpload.create(new UUID(0,1),new UUID(0,2),new UUID(0,3),operator,bytes);
        assertEquals(operator,OperatorUpload.decode(upload.encode()).operator()); assertArrayEquals(upload.encode(),OperatorUpload.decode(upload.encode()).encode());
        var first=Arrays.copyOfRange(bytes,0,OperatorUpload.CHUNK_BYTES); var second=Arrays.copyOfRange(bytes,OperatorUpload.CHUNK_BYTES,bytes.length);
        upload.requireArtifact(List.of(first,second));
        assertThrows(IllegalArgumentException.class,() -> upload.requireArtifact(List.of(first)));
        assertThrows(IllegalArgumentException.class,() -> upload.requireArtifact(List.of(second,first)));
        var changed=first.clone(); changed[0]^=1; assertThrows(IllegalArgumentException.class,() -> upload.requireChunk(0,changed));
        upload.chunks().getFirst().sha256()[0]^=1; upload.requireChunk(0,first);
        assertThrows(UnsupportedOperationException.class,() -> upload.chunks().clear());
        assertThrows(IllegalArgumentException.class,() -> OperatorUpload.decode(Arrays.copyOf(upload.encode(),upload.encode().length+1)));
        for(int n=0;n<upload.encode().length;n++) { byte[] truncated=Arrays.copyOf(upload.encode(),n); assertThrows(IllegalArgumentException.class,() -> OperatorUpload.decode(truncated)); }
    }
}
