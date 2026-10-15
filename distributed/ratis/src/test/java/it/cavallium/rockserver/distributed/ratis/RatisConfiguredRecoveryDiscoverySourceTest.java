package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.ApplyPosition;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Actual borrowed-client adapter; admission must precede local reply copying and decoding. */
class RatisConfiguredRecoveryDiscoverySourceTest {
    private static UUID id(long n){return new UUID(986,n);}
    private static final class Fixture {
        final RaftClient client=mock(RaftClient.class);final BlockingApi io=mock(BlockingApi.class);
        final RatisConfiguredRecoveryDiscovery.Source source;
        final RatisCatalogueViewRpc.Request view=new RatisCatalogueViewRpc.Request(id(1),id(2),id(3),id(4),id(5));
        final RatisRecoveryLineageRpc.Request birth=new RatisRecoveryLineageRpc.Request(id(1),id(2),id(3),id(4),id(5),id(6),id(7),1,new ApplyPosition(1,1));
        Fixture(){when(client.getGroupId()).thenReturn(RaftGroupId.valueOf(id(3)));when(client.io()).thenReturn(io);source=new RatisCatalogue(client,id(1),id(2),id(3),1,new byte[32],id(4)).recoveryDiscoverySource();}
        void reply(byte[] bytes)throws Exception{var result=new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf("catalogue")).setGroupId(RaftGroupId.valueOf(id(3))).setCallId(1).setSuccess(true).setMessage(Message.valueOf(ByteString.copyFrom(bytes))).build();when(io.sendReadOnly(any(Message.class))).thenReturn(result);}
    }
    @Test void admitsExactWireLengthsAndPreservesScopeForBothReplies()throws Exception {
        var f=new Fixture();var lengths=new ArrayList<Integer>();
        byte[] view=RatisCatalogueViewRpc.reply(new RatisCatalogueViewRpc.View(f.view,new ApplyPosition(1,3),null));f.reply(view);assertEquals(new ApplyPosition(1,3),f.source.view(f.view,lengths::add).cut());
        byte[] birth=RatisRecoveryLineageRpc.reply(new RatisRecoveryLineageRpc.Reply(f.birth,new ApplyPosition(1,4),null));f.reply(birth);assertNull(f.source.birth(f.birth,lengths::add).birth());
        assertEquals(List.of(view.length,birth.length),lengths);verify(f.io,times(2)).sendReadOnly(any(Message.class));
    }
    @Test void deniedAdmissionPrecedesMalformedReplyDecodingAndCannotEscapeBinding()throws Exception {
        var f=new Fixture();f.reply(new byte[]{0});var denied=new IOException("Denied before copying malformed reply");var calls=new AtomicInteger();
        RatisConfiguredRecoveryDiscovery.ReplyAdmission admission=bytes->{assertEquals(1,bytes);calls.incrementAndGet();throw denied;};
        assertSame(denied,assertThrows(IOException.class,()->f.source.view(f.view,admission)));assertSame(denied,assertThrows(IOException.class,()->f.source.birth(f.birth,admission)));assertEquals(2,calls.get());
        clearInvocations(f.io);var wrong=new RatisCatalogueViewRpc.Request(id(10),id(2),id(3),id(4),id(5));assertThrows(IOException.class,()->f.source.view(wrong,admission));verifyNoInteractions(f.io);assertEquals(2,calls.get());
    }
    @Test void oversizedReplyIsRefusedBeforeAdmission()throws Exception {
        var f=new Fixture();f.reply(new byte[RatisCatalogueViewRpc.MAX_REPLY_BYTES+1]);var calls=new AtomicInteger();assertThrows(IOException.class,()->f.source.view(f.view,bytes->calls.incrementAndGet()));assertEquals(0,calls.get());
    }
}
