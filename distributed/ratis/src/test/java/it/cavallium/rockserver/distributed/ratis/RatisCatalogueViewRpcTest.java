package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Current bounded atomic CAT issuance observation, including the actual native query handler. */
class RatisCatalogueViewRpcTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    private static UUID id(long n){return new UUID(12040,n);}
    private static final RatisCatalogueViewRpc.Request REQUEST=new RatisCatalogueViewRpc.Request(id(1),id(2),id(3),id(4),id(5));
    private static final ApplyPosition CUT=new ApplyPosition(1,9);
    private static RangeDirectory directory() {
        var replica=new ReplicaDescriptor(id(6),id(7),id(8),"owner","127.0.0.1:9001",ReplicaDescriptor.Role.VOTER);
        return new RangeDirectory(REQUEST.cluster(),REQUEST.database(),REQUEST.domain(),1,new byte[32],0,Set.of(id(9)),0,List.of(new RangeDescriptor(id(10),id(11),1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(replica))));
    }
    private static DatabaseDomainRoster roster(){return DatabaseDomainRoster.initial(REQUEST.cluster(),REQUEST.database(),List.of(REQUEST.domain(),id(12)));}
    @Test void currentNullableDirectoryAndRosterRoundTripWithExactWireLength() {
        assertEquals(REQUEST,RatisCatalogueViewRpc.request(RatisCatalogueViewRpc.request(REQUEST)));
        for(boolean directory:List.of(false,true))for(boolean roster:List.of(false,true)) {
            var view=new RatisCatalogueViewRpc.View(REQUEST,CUT,directory?directory():null,roster?roster():null);byte[] wire=RatisCatalogueViewRpc.reply(view);var decoded=RatisCatalogueViewRpc.reply(wire,REQUEST);
            assertEquals(ScopedTransactionRpc.HEADER_BYTES+104+(directory?RangeDirectoryCodec.encode(view.directory()).length:0)+(roster?view.roster().encode().length:0),wire.length);
            assertEquals(CUT,decoded.cut());assertEquals(view.roster(),decoded.roster());assertArrayEquals(wire,RatisCatalogueViewRpc.reply(decoded));if(directory)assertArrayEquals(view.directory().payloadDigest(),decoded.directory().payloadDigest());else assertNull(decoded.directory());
        }
        var unissued=new RatisCatalogueViewRpc.View(REQUEST,CUT,null,DatabaseDomainRoster.initial(REQUEST.cluster(),REQUEST.database(),List.of(id(12))));assertFalse(RatisCatalogueViewRpc.reply(RatisCatalogueViewRpc.reply(unissued),REQUEST).roster().domainIds().contains(REQUEST.domain()));
    }
    @Test void foreignNamespaceScopeAndUnissuedDirectoryAreRejected() {
        var foreign=DatabaseDomainRoster.initial(id(13),REQUEST.database(),List.of(REQUEST.domain()));assertThrows(IllegalArgumentException.class,()->new RatisCatalogueViewRpc.View(REQUEST,CUT,null,foreign));
        var unissued=DatabaseDomainRoster.initial(REQUEST.cluster(),REQUEST.database(),List.of(id(12)));assertThrows(IllegalArgumentException.class,()->new RatisCatalogueViewRpc.View(REQUEST,CUT,directory(),unissued));
        byte[] wire=RatisCatalogueViewRpc.reply(new RatisCatalogueViewRpc.View(REQUEST,CUT,directory(),roster()));var wrong=new RatisCatalogueViewRpc.Request(REQUEST.cluster(),REQUEST.database(),id(14),REQUEST.column(),REQUEST.domain());assertThrows(IllegalArgumentException.class,()->RatisCatalogueViewRpc.reply(wire,wrong));
        byte[] body=ScopedTransactionRpc.parse(wire,RatisCatalogueViewRpc.MAX_REPLY_BYTES).body();int rosterStart=104+RangeDirectoryCodec.encode(directory()).length;ByteBuffer.wrap(body).putLong(rosterStart,15);assertThrows(IllegalArgumentException.class,()->RatisCatalogueViewRpc.reply(ScopedTransactionRpc.frame(RatisCatalogueViewRpc.KIND,body),REQUEST));
    }
    @Test void malformedLengthsNoncanonicalRosterAndTrailingBytesFailClosed() {
        byte[] exact=ScopedTransactionRpc.parse(RatisCatalogueViewRpc.reply(new RatisCatalogueViewRpc.View(REQUEST,CUT,directory(),roster())),RatisCatalogueViewRpc.MAX_REPLY_BYTES).body();int rosterLength=100+RangeDirectoryCodec.encode(directory()).length;
        for(int length:List.of(-1,RatisCatalogue.MAX_DIRECTORY_BYTES+1,exact.length)){byte[] body=exact.clone();ByteBuffer.wrap(body).putInt(96,length);assertThrows(IllegalArgumentException.class,()->RatisCatalogueViewRpc.reply(ScopedTransactionRpc.frame(RatisCatalogueViewRpc.KIND,body),REQUEST));}
        for(int length:List.of(-1,DatabaseDomainRoster.MAX_BYTES+1,0)){byte[] body=exact.clone();ByteBuffer.wrap(body).putInt(rosterLength,length);assertThrows(IllegalArgumentException.class,()->RatisCatalogueViewRpc.reply(ScopedTransactionRpc.frame(RatisCatalogueViewRpc.KIND,body),REQUEST));}
        byte[] trailing=Arrays.copyOf(exact,exact.length+1);assertThrows(IllegalArgumentException.class,()->RatisCatalogueViewRpc.reply(ScopedTransactionRpc.frame(RatisCatalogueViewRpc.KIND,trailing),REQUEST));
        byte[] duplicate=exact.clone();int domains=rosterLength+4+DatabaseDomainRoster.HEADER_BYTES;System.arraycopy(duplicate,domains,duplicate,domains+16,16);assertThrows(IllegalArgumentException.class,()->RatisCatalogueViewRpc.reply(ScopedTransactionRpc.frame(RatisCatalogueViewRpc.KIND,duplicate),REQUEST));
        assertThrows(IllegalArgumentException.class,()->RatisCatalogueViewRpc.reply(new byte[RatisCatalogueViewRpc.MAX_REPLY_BYTES+1],REQUEST));
        assertThrows(IllegalArgumentException.class,()->RatisCatalogueViewRpc.reply(ScopedTransactionRpc.frame(RatisCatalogueViewRpc.KIND,Arrays.copyOf(exact,100)),REQUEST));
    }
    @Test void actualCatalogueHandlerReturnsDirectoryAndIssuanceAtOneCompletedNativeCut()throws Exception {
        try(var source=new RatisTwoPoolWorkflowFixture(root,false)) {
            var binding=source.plan.catalogue();var request=new RatisCatalogueViewRpc.Request(source.first.clusterId(),source.first.databaseId(),binding.groupId(),binding.directoryColumn(),source.first.domainId());
            byte[] wire=source.catalogueMachine.queryLinearizable(Message.valueOf(ByteString.copyFrom(RatisCatalogueViewRpc.request(request)))).get(10,TimeUnit.SECONDS).getContent().toByteArray();var view=RatisCatalogueViewRpc.reply(wire,request);
            var key=ByteBuffer.allocate(16).putLong(request.domain().getMostSignificantBits()).putLong(request.domain().getLeastSignificantBits()).array();var store=RatisTopologySeedRuntimeTest.field(source.catalogueMachine,"store",ReplicatedRangeStore.class);
            var actual=store.readSchemaAuthorityPoints(List.of(new ReplicatedRangeStore.PhysicalPoint(binding.directoryColumn(),key),new ReplicatedRangeStore.PhysicalPoint(binding.directoryColumn(),RatisDatabaseDomainRoster.key(request.database()))),new int[]{RatisCatalogue.MAX_DIRECTORY_BYTES,DatabaseDomainRoster.MAX_BYTES});
            assertEquals(actual.position(),view.cut());assertArrayEquals(actual.values().getFirst(),RangeDirectoryCodec.encode(view.directory()));assertArrayEquals(actual.values().get(1),view.roster().encode());assertTrue(view.roster().domainIds().containsAll(List.of(source.first.domainId(),source.second.domainId())));
        }
    }
}
