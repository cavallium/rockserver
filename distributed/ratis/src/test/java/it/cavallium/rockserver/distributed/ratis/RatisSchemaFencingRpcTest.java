package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RatisSchemaFencingRpcTest {
    final RatisSchemaAdministrationTest fixture=new RatisSchemaAdministrationTest();
    SchemaChange.Definition definition() {
        var source=fixture.initial(); return new SchemaChange.Definition(new UUID(20,1),fixture.operation(),fixture.directory(source,0),source,
                fixture.next(source,List.of(source.column(7),fixture.column(8,new UUID(20,2),"added"))));
    }
    RatisSchemaAdministration admin() {
        var client=(org.apache.ratis.client.RaftClient)java.lang.reflect.Proxy.newProxyInstance(org.apache.ratis.client.RaftClient.class.getClassLoader(),new Class<?>[]{org.apache.ratis.client.RaftClient.class},(proxy,method,args) -> {
            if (method.getName().equals("getGroupId")) return org.apache.ratis.protocol.RaftGroupId.valueOf(fixture.outer.catalogue);
            throw new AssertionError("Pure preparation accessed transport");
        }); return fixture.admin(client);
    }
    @Test void exactFencingEchoCanonicalityAndCommitmentPreservation() {
        var definition=definition(); var manifest=fixture.admissions(definition); var head=new RatisSchemaAdministration.Head(SchemaChange.Status.begin(definition.header()),8);
        var command=admin().prepareFencing(fixture.operation(),head,definition,manifest); byte[] bytes=DistributedCommandCodec.encode(command);
        var request=RatisSchemaFencingRpc.prepareRequest(command); assertArrayEquals(bytes,DistributedCommandCodec.encode(RatisSchemaFencingRpc.request(request)));
        assertArrayEquals(bytes,DistributedCommandCodec.encode(RatisSchemaFencingRpc.decodePreparedReply(RatisSchemaFencingRpc.preparedReply(ScopedTransactionRpc.SCHEMA_FENCING_PREPARE,command),ScopedTransactionRpc.SCHEMA_FENCING_PREPARE,command)));
        var changed=admin().prepareFencing(fixture.operation(),head,definition,manifest);
        assertThrows(IllegalArgumentException.class,() -> RatisSchemaFencingRpc.decodePreparedReply(RatisSchemaFencingRpc.prepareRequest(changed),ScopedTransactionRpc.SCHEMA_FENCING_PREPARE,command));
        assertThrows(IllegalArgumentException.class,() -> RatisSchemaFencingRpc.decodePreparedReply(RatisSchemaFencingRpc.cancelRequest(command),ScopedTransactionRpc.SCHEMA_FENCING_PREPARE,command));
        var next=RatisSchemaAdministration.Head.decode(command.mutations().getFirst().value()); assertArrayEquals(manifest.digest(),next.status().admissionDigest());
        assertEquals(2,command.predicates().size()); assertArrayEquals(RangeDirectoryCodec.encode(definition.sourceDirectory()),command.predicates().get(1).expectedValue());
    }
    @Test void committedImmutableReaderRejectsMissingOrChangedContext() {
        var definition=definition(); var manifest=fixture.admissions(definition); var prepared=new RatisSchemaAdministration.Head(SchemaChange.Status.begin(definition.header()),8);
        var publication=admin().prepareAdmissions(fixture.operation(),prepared,definition,manifest);
        var fencing=admin().prepareFencing(fixture.operation(),prepared,definition,manifest);
        var values=new HashMap<String,byte[]>();
        values.put(HexFormat.of().formatHex(RatisSchemaAdministration.definitionKey(definition.digest())),definition.encode());
        values.put(HexFormat.of().formatHex(java.nio.ByteBuffer.allocate(16).putLong(definition.header().domainId().getMostSignificantBits()).putLong(definition.header().domainId().getLeastSignificantBits()).array()),RangeDirectoryCodec.encode(definition.sourceDirectory()));
        var configured=RatisSchemaAdministration.validator(fixture.outer.cluster,fixture.outer.database,fixture.outer.catalogue,1,new byte[32],fixture.outer.directoryColumn,key -> values.get(HexFormat.of().formatHex(key)));
        var unconfigured=RatisSchemaAdministration.validator(fixture.outer.cluster,fixture.outer.database,fixture.outer.catalogue,1,new byte[32],fixture.outer.directoryColumn);
        assertThrows(IllegalArgumentException.class,() -> unconfigured.accept(publication)); assertThrows(IllegalArgumentException.class,() -> unconfigured.accept(fencing));
        configured.accept(publication); assertThrows(IllegalArgumentException.class,() -> configured.accept(fencing));
        var begin=admin().prepareBegin(definition,null,null,null); assertThrows(IllegalArgumentException.class,() -> unconfigured.accept(begin)); assertThrows(IllegalArgumentException.class,() -> configured.accept(begin));
        values.put(HexFormat.of().formatHex(RatisSchemaCatalogue.key(definition.sourceSchema().digest())),DistributedSchemaCodec.encode(definition.sourceSchema()));
        values.put(HexFormat.of().formatHex(RatisSchemaCatalogue.key(definition.targetSchema().digest())),DistributedSchemaCodec.encode(definition.targetSchema())); configured.accept(begin);
        var wrongIssuer=new DistributedCommand(begin.groupId(),begin.rangeEpoch(),begin.schemaDigest(),fixture.operation(),begin.mutations(),begin.predicates()); assertThrows(IllegalArgumentException.class,() -> configured.accept(wrongIssuer));
        String key=HexFormat.of().formatHex(RatisSchemaAdministration.admissionKey(manifest.digest())); values.put(key,manifest.encode()); configured.accept(fencing);
        byte[] altered=manifest.encode(); altered[altered.length-1]^=1; values.put(key,altered); assertThrows(IllegalArgumentException.class,() -> configured.accept(fencing));
        values.put(key,manifest.encode()); values.put(HexFormat.of().formatHex(RatisSchemaAdministration.definitionKey(definition.digest())),new byte[]{1}); assertThrows(IllegalArgumentException.class,() -> configured.accept(publication));
    }
    @Test void expectedAuthorityAndRetainedOutcomeHaveOneCurrentCanonicalShape() {
        var definition=definition(); var manifest=fixture.admissions(definition);
        var request=new RatisSchemaAuthority.Request(fixture.outer.cluster,fixture.outer.database,definition.header().domainId(),definition.header(),manifest.digest(),false);
        assertEquals(request,RatisSchemaAuthority.request(RatisSchemaAuthority.request(request)));
        var status=SchemaChange.Status.begin(definition.header()).beginFencing(manifest.digest()).fenced(fixture.outer.dataGroup,new ApplyPosition(1,1)).installed(fixture.outer.dataGroup,new ApplyPosition(1,2)).publish().activated(fixture.outer.dataGroup,new ApplyPosition(1,3)).retire();
        var outcome=new RatisSchemaAuthority.Request(fixture.outer.cluster,fixture.outer.database,definition.header().domainId(),definition.header(),manifest.digest(),true);
        var view=new RatisSchemaAuthority.View(outcome,fixture.outer.catalogue,new ApplyPosition(1,10),definition.targetDirectory(),new RatisSchemaAdministration.Head(status,8),manifest.encode());
        byte[] reply=RatisSchemaAuthority.reply(view); var decoded=RatisSchemaAuthority.decodeReply(reply,outcome,fixture.outer.catalogue);
        assertArrayEquals(manifest.encode(),decoded.manifest(definition).encode()); assertThrows(IllegalArgumentException.class,() -> RatisSchemaAuthority.decodeReply(reply,request,fixture.outer.catalogue));
        byte[] bad=manifest.digest(); bad[0]^=1; assertThrows(IllegalArgumentException.class,() -> new RatisSchemaAuthority.View(new RatisSchemaAuthority.Request(fixture.outer.cluster,fixture.outer.database,definition.header().domainId(),definition.header(),bad,true),fixture.outer.catalogue,new ApplyPosition(1,10),definition.targetDirectory(),view.head(),manifest.encode()));
        var initial=new RatisSchemaAuthority.Request(fixture.outer.cluster,fixture.outer.database,definition.header().domainId());
        assertEquals(initial,RatisSchemaAuthority.request(RatisSchemaAuthority.request(initial))); assertEquals(ScopedTransactionRpc.HEADER_BYTES+57,RatisSchemaAuthority.request(initial).length);
    }
    @Test void maximumReplicaCoverageAndLongSourceBoundsFitPublicationAndFencingCommands() {
        var source=fixture.initial(); var target=fixture.next(source,List.of(source.column(7),fixture.column(8,new UUID(50,1),"added")));
        int low=1,high=RangeDirectoryCodec.MAX_ENCODED_BYTES/126;
        while (low<high) {
            int middle=(low+high+1)/2;
            try { if (RangeDirectoryCodec.encodedSize(largeDirectory(source,middle))<=RatisCatalogue.MAX_DIRECTORY_BYTES-16) low=middle; else high=middle-1; }
            catch (IllegalArgumentException oversized) { if (!"Directory exceeds byte limit".equals(oversized.getMessage())) throw oversized; high=middle-1; }
        }
        var directory=largeDirectory(source,low); var definition=new SchemaChange.Definition(new UUID(50,2),fixture.operation(),directory,source,target);
        var bindings=new ArrayList<SchemaAdmissionManifest.Binding>(); var proofs=new ArrayList<SchemaAdmissionManifest.ReplicaAdmission>();
        for (var range:directory.ranges()) {
            bindings.add(new SchemaAdmissionManifest.Binding(range.groupId(),"x".repeat(4096),false));
            var proof=new NativeSchemaCapacityReservation(definition.header(),range.groupId(),new byte[32],definition.encode().length);
            for (var replica:range.replicas()) proofs.add(new SchemaAdmissionManifest.ReplicaAdmission(range.groupId(),replica.replicaId(),proof));
        }
        assertThrows(IllegalArgumentException.class,() -> new SchemaAdmissionManifest(definition,bindings,proofs.subList(0,proofs.size()-1)));
        var manifest=new SchemaAdmissionManifest(definition,bindings,proofs); assertEquals(1024,manifest.replicas().size()); assertTrue(manifest.encode().length>450*1024);
        var head=new RatisSchemaAdministration.Head(SchemaChange.Status.begin(definition.header()),8);
        var publication=admin().prepareAdmissions(fixture.operation(),head,definition,manifest); var fencing=admin().prepareFencing(fixture.operation(),head,definition,manifest);
        assertTrue(DistributedCommandCodec.encode(publication).length<=DistributedCommandCodec.MAX_ENCODED_BYTES); assertTrue(DistributedCommandCodec.encode(fencing).length<=DistributedCommandCodec.MAX_ENCODED_BYTES);
        assertEquals(2,fencing.predicates().size()); assertEquals(2,publication.predicates().size());
    }
    RangeDirectory largeDirectory(DistributedSchema schema,int width) {
        var replicas=new ArrayList<ReplicaDescriptor>();
        for (int i=0;i<16;i++) replicas.add(new ReplicaDescriptor(new UUID(60,i),new UUID(61,i),new UUID(62,i),"replica"+i,"127.0.0.1:"+(9000+i),i<5 ? ReplicaDescriptor.Role.VOTER : ReplicaDescriptor.Role.LISTENER));
        var ranges=new ArrayList<RangeDescriptor>(); byte[] lower=null;
        for (int i=0;i<64;i++) { byte[] upper=i==63 ? null : new byte[width]; if (upper!=null) upper[0]=(byte)(i+1); ranges.add(new RangeDescriptor(new UUID(63,i),new UUID(64,i),1,1,lower,upper,RangeDescriptor.TopologyState.ACTIVE,replicas)); lower=upper; }
        return new RangeDirectory(schema.clusterId(),schema.databaseId(),schema.domainId(),schema.schemaEpoch(),schema.digest(),0,schema.columnIds(),0,ranges);
    }

}
