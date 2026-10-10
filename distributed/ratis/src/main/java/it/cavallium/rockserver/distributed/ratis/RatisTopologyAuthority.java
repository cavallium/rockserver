package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/** One fresh completed catalogue cut authorizing the exact parent freeze intent. */
public final class RatisTopologyAuthority {
    public static final int KIND=84,REQUEST_BYTES=ScopedTransactionRpc.HEADER_BYTES+96;
    public static final int MAX_REPLY_BYTES=ScopedTransactionRpc.HEADER_BYTES+40+TopologyChange.MAX_DEFINITION_BYTES+TopologyChange.MAX_STATUS_BYTES;
    private RatisTopologyAuthority() {}
    public record Request(UUID cluster,UUID database,UUID domain,UUID intent,byte[] digest) {
        public Request { Objects.requireNonNull(cluster); Objects.requireNonNull(database); Objects.requireNonNull(domain); Objects.requireNonNull(intent); if(digest==null || digest.length!=32) throw new IllegalArgumentException("Invalid topology authority digest"); digest=digest.clone(); }
        @Override public byte[] digest() { return digest.clone(); }
        public static Request of(TopologyChange.Header header) { return new Request(header.clusterId(),header.databaseId(),header.domainId(),header.intentId(),header.definitionDigest()); }
    }
    public record View(UUID catalogue,ApplyPosition cut,TopologyChange.Definition definition,TopologyChange.State state) {
        public View {
            Objects.requireNonNull(catalogue); Objects.requireNonNull(cut); Objects.requireNonNull(definition); Objects.requireNonNull(state);
            if(cut.term()<1 || cut.index()<1 || state.phase()!=TopologyChange.Phase.FENCING || definition.kind()==TopologyChange.Kind.RELOCATE) throw new IllegalArgumentException("Topology authority requires a committed split/merge FENCING intent");
            state.requireDefinition(definition);
        }
        public void require(Request request) {
            var header=state.header();
            if(!request.cluster().equals(header.clusterId()) || !request.database().equals(header.databaseId()) || !request.domain().equals(header.domainId()) || !request.intent().equals(header.intentId()) || !Arrays.equals(request.digest(),header.definitionDigest())) throw new IllegalArgumentException("Topology authority identity differs");
        }
        /** Stable persisted admission identity; the fresh read cut remains a separate qualification check. */
        public byte[] admissionDigest() {
            byte[] initial=TopologyChange.State.begin(definition.header()).beginFencing().encode(); var bytes=ByteBuffer.allocate(16+initial.length); uuid(bytes,catalogue); bytes.put(initial); return OperatorUpload.sha256(bytes.array());
        }
    }
    public static final class Client {
        private final RaftClient client;
        private final UUID cluster,database,catalogue;
        public Client(RaftClient borrowed,UUID cluster,UUID database,UUID catalogue) {
            client=Objects.requireNonNull(borrowed); this.cluster=Objects.requireNonNull(cluster); this.database=Objects.requireNonNull(database); this.catalogue=Objects.requireNonNull(catalogue);
            if(!client.getGroupId().getUuid().equals(catalogue)) throw new IllegalArgumentException("Topology authority client group differs");
        }
        public View read(TopologyChange.Header header) throws IOException {
            var expected=Request.of(header); if(!cluster.equals(expected.cluster()) || !database.equals(expected.database())) throw new IllegalArgumentException("Topology authority client namespace differs");
            var reply=RatisRoutedClient.rpc(()->client.io().sendReadOnly(Message.valueOf(ByteString.copyFrom(request(expected)))));
            if(reply==null || !reply.isSuccess() || reply.getMessage()==null || reply.getRaftGroupId()==null || !reply.getRaftGroupId().getUuid().equals(catalogue) || reply.getMessage().getContent().size()>MAX_REPLY_BYTES) throw new IOException("Topology authority read failed",reply==null ? null : reply.getException());
            try { var view=decodeReply(reply.getMessage().getContent().toByteArray(),expected,catalogue); header.requireDefinition(view.definition()); return view; }
            catch(IllegalArgumentException invalid) { throw new IOException("Invalid topology authority reply",invalid); }
        }
    }
    public static byte[] request(Request request) {
        var out=ByteBuffer.allocate(96); uuid(out,request.cluster()); uuid(out,request.database()); uuid(out,request.domain()); uuid(out,request.intent()); out.put(request.digest()); return ScopedTransactionRpc.frame(KIND,out.array());
    }
    public static Request decodeRequest(byte[] encoded) {
        var packet=ScopedTransactionRpc.parse(encoded,REQUEST_BYTES); if(packet.kind()!=KIND || packet.body().length!=96) throw new IllegalArgumentException("Invalid topology authority request");
        var in=ByteBuffer.wrap(packet.body()); var cluster=uuid(in); var database=uuid(in); var domain=uuid(in); var intent=uuid(in); byte[] digest=new byte[32]; in.get(digest); return new Request(cluster,database,domain,intent,digest);
    }
    /** Fixed server-owned keys and CF; caller must establish the fresh linearizable barrier. */
    public static View readNative(ReplicatedRangeStore store,UUID column,UUID cluster,UUID database,Request request) throws IOException {
        if(!cluster.equals(request.cluster()) || !database.equals(request.database())) throw new IOException("Topology authority namespace differs");
        try {
            var domain=ByteBuffer.allocate(16); uuid(domain,request.domain());
            var points=store.readSchemaAuthorityPoints(List.of(new ReplicatedRangeStore.PhysicalPoint(column,RatisTopologyCatalogue.headKey(request.domain())),new ReplicatedRangeStore.PhysicalPoint(column,RatisTopologyCatalogue.stateKey(request.intent())),new ReplicatedRangeStore.PhysicalPoint(column,RatisTopologyCatalogue.definitionKey(request.digest())),new ReplicatedRangeStore.PhysicalPoint(column,domain.array())),new int[]{TopologyChange.MAX_STATUS_BYTES,TopologyChange.MAX_STATUS_BYTES,TopologyChange.MAX_DEFINITION_BYTES,RatisCatalogue.MAX_DIRECTORY_BYTES});
            var values=points.values(); if(points.position()==null || values.stream().anyMatch(Objects::isNull) || !Arrays.equals(values.get(0),values.get(1))) throw new IOException("Topology authority head/state missing or inconsistent");
            var definition=TopologyChange.Definition.decode(values.get(2)); var state=TopologyChange.State.decode(values.get(0));
            if(!Arrays.equals(values.get(3),RangeDirectoryCodec.encode(definition.sourceDirectory()))) throw new IOException("Topology authority source directory changed");
            var view=new View(store.ownership().owner().groupId(),points.position(),definition,state); view.require(request); return view;
        } catch(org.rocksdb.RocksDBException|IllegalArgumentException|IllegalStateException invalid) { throw new IOException("Invalid native topology authority",invalid); }
    }
    public static byte[] reply(View view) {
        byte[] definition=view.definition().encode(),state=view.state().encode(); var out=ByteBuffer.allocate(40+definition.length+state.length); uuid(out,view.catalogue()); out.putLong(view.cut().term()).putLong(view.cut().index()); ScopedTransactionRpc.putBlob(out,definition); ScopedTransactionRpc.putBlob(out,state); return ScopedTransactionRpc.frame(KIND,out.array());
    }
    public static View decodeReply(byte[] encoded,Request request,UUID catalogue) {
        var packet=ScopedTransactionRpc.parse(encoded,MAX_REPLY_BYTES); if(packet.kind()!=KIND) throw new IllegalArgumentException("Wrong topology authority reply kind"); var in=ByteBuffer.wrap(packet.body());
        if(in.remaining()<40) throw new IllegalArgumentException("Truncated topology authority reply"); var group=uuid(in); var cut=new ApplyPosition(in.getLong(),in.getLong());
        var definition=TopologyChange.Definition.decode(ScopedTransactionRpc.blob(in,TopologyChange.MAX_DEFINITION_BYTES,false)); var state=TopologyChange.State.decode(ScopedTransactionRpc.blob(in,TopologyChange.MAX_STATUS_BYTES,false));
        if(in.hasRemaining() || !group.equals(catalogue)) throw new IllegalArgumentException("Topology authority catalogue/tail differs"); var view=new View(group,cut,definition,state); view.require(request); return view;
    }
    private static void uuid(ByteBuffer out,UUID id) { ScopedTransactionRpc.uuid(out,id); }
    private static UUID uuid(ByteBuffer in) { return ScopedTransactionRpc.uuid(in); }
}
