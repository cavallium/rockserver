package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Actual native catalogue writes through controlled borrowed transports; no Raft servers or artifact execution. */
class RatisOperatorCatalogueTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final RatisSessionEpochsTest outer=new RatisSessionEpochsTest();
    final AtomicLong sequence=new AtomicLong();
    OperationId operation() { return outer.operation(sequence.getAndIncrement()); }
    OperatorUpload upload(UUID id,String name,long revision,byte[] bytes) {
        return OperatorUpload.create(id,outer.cluster,outer.database,new DistributedSchema.Operator(name,revision,"example.Operator",
                OperatorUpload.sha256(bytes),1,2,"integer-addition",1),bytes);
    }
    class Fixture implements AutoCloseable {
        final RatisSessionEpochsTest.Fixture nativeFixture;
        final RaftClient client;
        final RatisOperatorCatalogue catalogue;
        final Consumer<DistributedCommand> validator;
        final Set<String> admitted=new HashSet<>();
        boolean loseReply;
        Fixture() throws Exception { this(new RatisOperatorCatalogue.Limits(32,64L*1024*1024)); }
        Fixture(RatisOperatorCatalogue.Limits limits) throws Exception {
            nativeFixture=outer.new Fixture(root);
            if(nativeFixture.store.applied()!=null) nativeFixture.next.set(nativeFixture.store.applied().index()+1);
            RatisOperatorCatalogue.AdmissionPolicy policy=upload -> {
                if(!admitted.contains(HexFormat.of().formatHex(upload.digest()))) throw new IllegalArgumentException("Operator manifest is not locally admitted");
            };
            var operators=RatisOperatorCatalogue.validator(outer.cluster,outer.database,outer.catalogue,1,new byte[32],outer.directoryColumn,limits,policy,key -> {
                try(var pin=nativeFixture.store.capture()) { return pin.get(outer.directoryColumn,key); }
                catch(Exception failure) { throw new IllegalStateException(failure); }
            });
            validator=RatisCatalogue.validator(outer.cluster,outer.database,outer.catalogue,1,new byte[32],outer.directoryColumn,outer.sessions,operators);
            var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args) -> {
                byte[] reply;
                if(method.getName().equals("send")) {
                    var command=DistributedCommandCodec.decode(((Message)args[0]).getContent().toByteArray()); validator.accept(command);
                    reply=nativeFixture.store.apply(new ApplyPosition(1,nativeFixture.next.getAndIncrement()),command);
                    if(loseReply) reply=new byte[4];
                } else if(method.getName().equals("sendReadOnly")) {
                    var packet=ScopedTransactionRpc.parse(((Message)args[0]).getContent().toByteArray(),ScopedTransactionRpc.MAX_REQUEST_BYTES);
                    assertEquals(ScopedTransactionRpc.POINT,packet.kind()); var in=ByteBuffer.wrap(packet.body());
                    assertEquals(outer.directoryColumn,ScopedTransactionRpc.uuid(in)); byte[] key=ScopedTransactionRpc.blob(in,2*1024*1024,false);
                    assertFalse(in.hasRemaining()); byte[] value;
                    try(var pin=nativeFixture.store.capture()) { value=pin.get(outer.directoryColumn,key); }
                    reply=ScopedTransactionRpc.frame(ScopedTransactionRpc.POINT,value==null ? ByteBuffer.allocate(4).putInt(-1).array() : ByteBuffer.allocate(value.length+4).putInt(value.length).put(value).array());
                } else throw new AssertionError(method);
                return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf("fixture"))
                        .setGroupId(RaftGroupId.valueOf(outer.catalogue)).setCallId(1).setSuccess(true).setMessage(Message.valueOf(ByteString.copyFrom(reply))).build();
            });
            client=(RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args) -> switch(method.getName()) {
                case "getGroupId" -> RaftGroupId.valueOf(outer.catalogue); case "io" -> io; case "close" -> throw new AssertionError("Borrowed transport cannot close"); default -> throw new AssertionError(method);
            });
            catalogue=new RatisOperatorCatalogue(client,outer.cluster,outer.database,outer.catalogue,1,new byte[32],outer.directoryColumn,limits,policy);
        }
        void admit(OperatorUpload upload) { admitted.add(HexFormat.of().formatHex(upload.digest())); }
        void begin(OperatorUpload upload) throws Exception {
            admit(upload); assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(catalogue.prepareBegin(operation(),upload,catalogue.readNameHead(upload.operator().name()),catalogue.readBudget())).status());
        }
        void chunks(OperatorUpload upload,byte[] bytes) throws Exception {
            for(int i=0;i<upload.chunks().size();i++) {
                byte[] chunk=Arrays.copyOfRange(bytes,i*OperatorUpload.CHUNK_BYTES,Math.min(bytes.length,(i+1)*OperatorUpload.CHUNK_BYTES));
                var command=catalogue.prepareChunk(operation(),upload,i,chunk);
                assertTrue(DistributedCommandCodec.encode(command).length<=DistributedCommandCodec.MAX_ENCODED_BYTES);
                assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(command).status());
            }
        }
        public void close() throws Exception { nativeFixture.close(); }
    }
    @Test void eightMiBArtifactNeedsCompleteChunksAndSurvivesUnknownOutcomesAndReopen() throws Exception {
        byte[] bytes=new byte[OperatorUpload.MAX_ARTIFACT_BYTES]; new Random(19).nextBytes(bytes);
        var upload=upload(new UUID(90,1),"sum",0,bytes); byte[] published;
        try(var f=new Fixture()) {
            f.admit(upload); var begin=f.catalogue.prepareBegin(operation(),upload,null,null); byte[] frozen=DistributedCommandCodec.encode(begin);
            f.loseReply=true; assertThrows(IOException.class,() -> f.catalogue.execute(begin)); f.loseReply=false;
            assertEquals(CommandResult.Status.SUCCESS,f.catalogue.execute(begin).status()); assertArrayEquals(frozen,DistributedCommandCodec.encode(begin));
            assertNull(f.catalogue.readPublished("sum",0)); assertNull(f.catalogue.readBySha(upload.operator().artifactSha256()));
            assertThrows(IllegalStateException.class,() -> f.catalogue.preparePublication(operation(),upload,null));
            f.chunks(upload,bytes);
            var publication=f.catalogue.preparePublication(operation(),upload,null); published=DistributedCommandCodec.encode(publication);
            f.loseReply=true; assertThrows(IOException.class,() -> f.catalogue.execute(publication)); f.loseReply=false;
            assertEquals(CommandResult.Status.SUCCESS,f.catalogue.execute(publication).status()); assertArrayEquals(published,DistributedCommandCodec.encode(publication));
            assertArrayEquals(upload.encode(),f.catalogue.readBySha(upload.operator().artifactSha256()).encode());
            assertEquals(1,f.catalogue.readBudget().uploads()); assertEquals(bytes.length,f.catalogue.readBudget().bytes());
        }
        try(var reopened=new Fixture()) {
            assertArrayEquals(upload.encode(),reopened.catalogue.readPublished("sum",0).encode());
            var artifact=reopened.catalogue.readBySha(upload.operator().artifactSha256()); var chunks=new ArrayList<byte[]>();
            for(int i=0;i<artifact.chunks().size();i++) chunks.add(reopened.catalogue.readChunk(artifact,i));
            artifact.requireArtifact(chunks); assertEquals(0,reopened.nativeFixture.closes.get());
        }
    }
    @Test void allocationRaceIsPermanentAndPublicationDoesNotDependOnLatestNameHead() throws Exception {
        byte[] bytes={1,2,3}; var a=upload(new UUID(91,1),"sum",0,bytes); var b=upload(new UUID(91,2),"sum",0,bytes);
        try(var f=new Fixture()) {
            f.admit(a); f.admit(b); var first=f.catalogue.prepareBegin(operation(),a,null,null); var losing=f.catalogue.prepareBegin(operation(),b,null,null);
            assertEquals(CommandResult.Status.SUCCESS,f.catalogue.execute(first).status());
            assertEquals(CommandResult.Status.CONFLICT,f.catalogue.execute(losing).status()); assertEquals(CommandResult.Status.CONFLICT,f.catalogue.execute(losing).status());
            assertNull(f.catalogue.readUpload(b.uploadId())); assertEquals(a.uploadId(),f.catalogue.readNameHead("sum").uploadId());
            var next=upload(new UUID(91,3),"sum",1,bytes); f.begin(next); f.chunks(a,bytes); f.chunks(next,bytes);
            assertEquals(CommandResult.Status.SUCCESS,f.catalogue.execute(f.catalogue.preparePublication(operation(),a,null)).status());
            var witness=f.catalogue.readBySha(a.operator().artifactSha256());
            assertEquals(CommandResult.Status.SUCCESS,f.catalogue.execute(f.catalogue.preparePublication(operation(),next,witness)).status());
            assertArrayEquals(a.encode(),f.catalogue.readPublished("sum",0).encode()); assertArrayEquals(next.encode(),f.catalogue.readPublished("sum",1).encode());
            assertArrayEquals(a.encode(),f.catalogue.readBySha(a.operator().artifactSha256()).encode());
            assertThrows(IllegalArgumentException.class,() -> f.catalogue.prepareBegin(operation(),b,f.catalogue.readNameHead("sum"),f.catalogue.readBudget()));
        }
    }
    @Test void localAdmissionByteBudgetsAndChunkContentAreEnforced() throws Exception {
        byte[] bytes={4,5,6}; var upload=upload(new UUID(92,1),"sum",0,bytes);
        try(var f=new Fixture(new RatisOperatorCatalogue.Limits(1,3))) {
            assertThrows(IllegalArgumentException.class,() -> f.catalogue.prepareBegin(operation(),upload,null,null)); f.begin(upload);
            assertThrows(IllegalArgumentException.class,() -> f.catalogue.prepareChunk(operation(),upload,0,new byte[]{4,5,7}));
            var valid=f.catalogue.prepareChunk(operation(),upload,0,bytes);
            var corrupted=new DistributedCommand(valid.groupId(),valid.rangeEpoch(),valid.schemaDigest(),operation(),
                    List.of(new Mutation(Mutation.Kind.PUT,outer.directoryColumn,RatisOperatorCatalogue.chunkKey(upload.uploadId(),0),new byte[]{4,5,7}),valid.mutations().get(1)),valid.predicates());
            assertThrows(IllegalArgumentException.class,() -> f.validator.accept(corrupted)); assertNull(f.catalogue.readChunk(upload,0));
            var next=upload(new UUID(92,2),"sum",1,bytes); f.admit(next);
            assertThrows(IllegalArgumentException.class,() -> f.catalogue.prepareBegin(operation(),next,f.catalogue.readNameHead("sum"),f.catalogue.readBudget()));
        }
    }
    @Test void composedValidatorRequiresConfiguredOperatorsAndPreservesDirectoryNamespace() throws Exception {
        var upload=upload(new UUID(94,1),"sum",0,new byte[]{1});
        try(var f=new Fixture()) {
            f.admit(upload); var begin=f.catalogue.prepareBegin(operation(),upload,null,null);
            assertThrows(IllegalArgumentException.class,() -> f.nativeFixture.validate.accept(begin));
            assertDoesNotThrow(() -> f.validator.accept(begin));
            var original=outer.directory(outer.cluster);
            var directory=new RangeDirectory(original.clusterId(),original.databaseId(),new UUID(0x7000000000000000L,1),
                    original.schemaEpoch(),original.schemaDigest(),0,original.columns(),original.routingPrefixBytes(),original.ranges());
            var catalogue=new RatisCatalogue(f.client,outer.cluster,outer.database,outer.catalogue,1,new byte[32],outer.directoryColumn);
            var command=catalogue.prepare(operation(),null,directory);
            assertEquals(0x70,command.mutations().getFirst().key()[0]);
            assertFalse(RatisOperatorCatalogue.isCommand(command));
            assertDoesNotThrow(() -> f.nativeFixture.validate.accept(command));
            assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(command).status());
            assertArrayEquals(RangeDirectoryCodec.encode(directory),RangeDirectoryCodec.encode(catalogue.read(directory.domainId())));
        }
    }
    @Test void publicationRechecksCommittedChunkBytesAfterPreparation() throws Exception {
        byte[] bytes={10,11,12}; var upload=upload(new UUID(95,1),"sum",0,bytes);
        try(var f=new Fixture()) {
            f.begin(upload); f.chunks(upload,bytes);
            var publication=f.catalogue.preparePublication(operation(),upload,null);
            // Controlled corruption fixture bypasses catalogue validation while retaining the original completion marker.
            var corrupt=new DistributedCommand(outer.catalogue,1,new byte[32],operation(),
                    List.of(new Mutation(Mutation.Kind.PUT,outer.directoryColumn,RatisOperatorCatalogue.chunkKey(upload.uploadId(),0),new byte[]{10,11,13})),List.of());
            f.nativeFixture.store.apply(new ApplyPosition(1,f.nativeFixture.next.getAndIncrement()),corrupt);
            assertThrows(IllegalArgumentException.class,() -> f.validator.accept(publication));
            assertNull(f.catalogue.readPublished("sum",0));
            assertNull(f.catalogue.readBySha(upload.operator().artifactSha256()));
        }
    }
    @Test void finalShaIsCheckedAgainstNativeChunksEvenWhenManifestWasAdmitted() throws Exception {
        byte[] bytes={7,8,9}; var proper=upload(new UUID(93,1),"sum",0,bytes); byte[] wrongSha=new byte[32];
        var descriptor=new DistributedSchema.Operator("sum",0,"example.Operator",wrongSha,1,2,"integer-addition",1);
        var bad=new OperatorUpload(proper.uploadId(),outer.cluster,outer.database,descriptor,bytes.length,proper.chunks());
        try(var f=new Fixture()) {
            f.begin(bad); f.chunks(bad,bytes);
            assertThrows(IllegalArgumentException.class,() -> f.catalogue.preparePublication(operation(),bad,null));
            assertNull(f.catalogue.readPublished("sum",0)); assertNull(f.catalogue.readBySha(wrongSha));
        }
    }
}
