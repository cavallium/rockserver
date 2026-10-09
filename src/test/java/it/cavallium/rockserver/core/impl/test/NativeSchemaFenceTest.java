package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import java.util.Arrays;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class NativeSchemaFenceTest {
    @Test void compactOriginalHeaderProofHasStrictCanonicalBoundsAndDefensiveDigests() {
        var helper=new ReplicatedSchemaFenceTest(); var header=helper.header(helper.outer.schema(),1);
        var fence=new NativeSchemaFence(header,helper.outer.group,new ApplyPosition(1,7));
        assertEquals(277,fence.encode().length); assertEquals(fence,NativeSchemaFence.decode(fence.encode()));
        fence.requireHeader(header); fence.requireCut(new ApplyPosition(2,8));
        assertThrows(IllegalArgumentException.class,() -> fence.requireHeader(helper.header(helper.outer.schema(),2)));
        assertThrows(IllegalArgumentException.class,() -> fence.requireCut(new ApplyPosition(2,7)));
        assertThrows(IllegalArgumentException.class,() -> fence.requireCut(new ApplyPosition(1,6)));
        assertThrows(IllegalArgumentException.class,() -> new NativeSchemaFence(header,new UUID(0,999),new ApplyPosition(1,7)));
        assertThrows(IllegalArgumentException.class,() -> new NativeSchemaFence(header,helper.outer.group,new ApplyPosition(0,7)));
        for (int size=0;size<NativeSchemaFence.BYTES;size++) {
            byte[] truncated=Arrays.copyOf(fence.encode(),size); assertThrows(IllegalArgumentException.class,() -> NativeSchemaFence.decode(truncated));
        }
        assertThrows(IllegalArgumentException.class,() -> NativeSchemaFence.decode(Arrays.copyOf(fence.encode(),NativeSchemaFence.BYTES+1)));
        // Retaining the expected digest must not authenticate independently corrupted stored fields.
        for (int offset:new int[]{4,20,36,52,68,76,84,116,180,196,212,220}) {
            byte[] altered=fence.encode(); altered[offset+7]^=1;
            var corruptIdentity=NativeSchemaFence.decode(altered);
            assertThrows(IllegalArgumentException.class,() -> corruptIdentity.requireHeader(header));
        }
        byte[] original=fence.headerDigest(); fence.headerDigest()[0]^=1; assertArrayEquals(original,fence.headerDigest());
        assertNull(fence.admissionDigest()); fence.requireAdmissionDigest(null);
        byte[] admission=new byte[32]; admission[0]=7;
        var qualified=new NativeSchemaFence(header,helper.outer.group,new ApplyPosition(1,7),admission);
        assertEquals(qualified,NativeSchemaFence.decode(qualified.encode())); qualified.requireHeader(header); qualified.requireAdmissionDigest(admission);
        admission[0]=8; assertEquals(7,qualified.admissionDigest()[0]); qualified.admissionDigest()[0]=9; assertEquals(7,qualified.admissionDigest()[0]);
        assertThrows(IllegalArgumentException.class,() -> qualified.requireAdmissionDigest(admission)); assertThrows(IllegalArgumentException.class,() -> qualified.requireAdmissionDigest(null));
        assertThrows(IllegalArgumentException.class,() -> fence.requireAdmissionDigest(new byte[32]));
        assertThrows(IllegalArgumentException.class,() -> new NativeSchemaFence(header,helper.outer.group,new ApplyPosition(1,7),new byte[31]));
        var flag=fence.encode(); flag[244]=2; assertThrows(IllegalArgumentException.class,() -> NativeSchemaFence.decode(flag));
        var nonzeroNull=fence.encode(); nonzeroNull[245]=1; assertThrows(IllegalArgumentException.class,() -> NativeSchemaFence.decode(nonzeroNull));
        var installed=new NativeSchemaInstallProof(qualified,header.targetSchemaDigest(),new ApplyPosition(1,8));
        assertEquals(qualified,NativeSchemaInstallProof.decode(installed.encode()).fence()); installed.requireHeader(header);
        var corrupt=fence.encode(); corrupt[0]^=1; assertThrows(IllegalArgumentException.class,() -> NativeSchemaFence.decode(corrupt));
    }
}
