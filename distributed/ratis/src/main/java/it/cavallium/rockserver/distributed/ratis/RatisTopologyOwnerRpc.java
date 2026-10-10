package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

/** Exact replica metadata evidence. The server must qualify its actual division before constructing it. */
public final class RatisTopologyOwnerRpc {
    public static final int KIND=88,MAX_CONFIGURATION_BYTES=64*1024;
    private static final int MAX_OWNER_BYTES=57+128+256,MAX_TARGET_BYTES=53+TopologyChange.MAX_DEFINITION_BYTES;
    public static final int MAX_REQUEST_BYTES=ScopedTransactionRpc.HEADER_BYTES+MAX_TARGET_BYTES;
    public static final int MAX_REPLY_BYTES=ScopedTransactionRpc.HEADER_BYTES+60+MAX_TARGET_BYTES+MAX_OWNER_BYTES+214+MAX_CONFIGURATION_BYTES+NativeTopologySeed.MAX_BYTES;
    private RatisTopologyOwnerRpc() {}
    public record Target(TopologyChange.Definition definition,TopologyChange.Stage stage,UUID catalogue,UUID group,UUID replica) {
        public Target {
            Objects.requireNonNull(definition);Objects.requireNonNull(stage);Objects.requireNonNull(catalogue);Objects.requireNonNull(group);Objects.requireNonNull(replica);
            if(definition.kind()==TopologyChange.Kind.RELOCATE)throw new IllegalArgumentException("Relocation owner execution is not configured");
            definition.header().participant(stage,group).owner(replica);
        }
        public RangeDescriptor range(){return (stage==TopologyChange.Stage.FREEZE ? definition.parents() : definition.targets()).stream().filter(r->r.groupId().equals(group)).findFirst().orElseThrow();}
        public ReplicaDescriptor owner(){return range().replicas().stream().filter(r->r.replicaId().equals(replica)).findFirst().orElseThrow();}
        public byte[] encode(){var body=definition.encode();var out=ByteBuffer.allocate(53+body.length);ScopedTransactionRpc.uuid(out,catalogue);ScopedTransactionRpc.uuid(out,group);ScopedTransactionRpc.uuid(out,replica);out.put((byte)stage.ordinal());ScopedTransactionRpc.putBlob(out,body);return out.array();}
        public byte[] digest(){return OperatorUpload.sha256(encode());}
    }
    public record Evidence(Target target,ReplicaDescriptor owner,TopologyChange.Proof proof,ApplyPosition completedCut,
            long configurationTerm,long configurationIndex,byte[] configuration,long raftTerm,byte[] artifact) {
        public Evidence {
            Objects.requireNonNull(target);Objects.requireNonNull(owner);Objects.requireNonNull(proof);Objects.requireNonNull(completedCut);
            if(!owner.equals(target.owner()) || completedCut.term()<1 || completedCut.term()!=raftTerm
                    || configuration==null || configuration.length<1 || configuration.length>MAX_CONFIGURATION_BYTES
                    || artifact==null || artifact.length<1 || artifact.length>artifactMaximum(target.stage()))throw new IllegalArgumentException("Invalid topology replica identity/cut/budget");
            if(configurationTerm==0 && configurationIndex==-1) { /* Forced immutable bootstrap membership. */ }
            else if(configurationTerm<1 || configurationIndex<0 || !covers(completedCut,new ApplyPosition(configurationTerm,configurationIndex)))throw new IllegalArgumentException("Topology native configuration exceeds completed prefix");
            try{RatisRangeStateMachine.requireSchemaMembership(target.range(),configuration);}catch(IOException invalid){throw new IllegalArgumentException("Topology native membership differs",invalid);}
            TopologyChange.Proof actual;
            switch(target.stage()) {
                case FREEZE -> {
                    var freeze=NativeTopologyFreeze.decode(artifact);var descriptor=target.range();var directory=target.definition().sourceDirectory();
                    var view=new RatisTopologyAuthority.View(target.catalogue(),new ApplyPosition(1,1),target.definition(),TopologyChange.State.begin(target.definition().header()).beginFencing());
                    freeze.requireRequest(target.definition().header(),view.admissionDigest());
                    freeze.requireParent(target.group(),descriptor.rangeEpoch(),directory.domainId(),directory.schemaDigest(),descriptor.lowerInclusive(),descriptor.upperExclusive());freeze.requireCut(completedCut);actual=freeze.proof(owner.replicaId());
                }
                case SEED -> {
                    var seed=NativeTopologySeed.decode(artifact);seed.header().requireDefinition(target.definition());
                    var authority=new RatisTopologyAuthority.View(target.catalogue(),new ApplyPosition(1,1),target.definition(),TopologyChange.State.begin(target.definition().header()).beginFencing());
                    byte[] admission=authority.admissionDigest();for(var freeze:seed.parentFreezes().values())freeze.requireRequest(target.definition().header(),admission);
                    if(!seed.localReplica().equals(owner))throw new IllegalArgumentException("Seed belongs to another actual replica");actual=seed.proof(target.group());
                }
                case ACTIVATE -> {
                    var activation=NativeTopologyActivation.decode(artifact);activation.header().requireDefinition(target.definition());
                    if(!activation.owner().equals(TopologyChange.Owner.of(owner)) || !activation.birth().groupId().equals(target.group()) || !activation.catalogueGroup().equals(target.catalogue()))throw new IllegalArgumentException("Activation belongs to another actual replica/publication");
                    activation.requireCut(completedCut);actual=activation.proof();
                }
                default -> throw new AssertionError(target.stage());
            }
            if(!actual.equals(proof) || !covers(completedCut,proof.position()))throw new IllegalArgumentException("Topology proof differs from actual native artifact");
            configuration=configuration.clone();artifact=artifact.clone();
        }
        @Override public byte[] configuration(){return configuration.clone();}
        @Override public byte[] artifact(){return artifact.clone();}
    }
    public static boolean covers(ApplyPosition completed,ApplyPosition prior){return completed.term()>=prior.term() && completed.index()>=prior.index() && (completed.index()!=prior.index() || completed.term()==prior.term() || prior.term()==0 && prior.index()==0);}
    private static int artifactMaximum(TopologyChange.Stage stage){return switch(stage){case FREEZE->NativeTopologyFreeze.MAX_BYTES;case SEED->NativeTopologySeed.MAX_BYTES;case ACTIVATE->NativeTopologyActivation.MAX_BYTES;};}
    public static byte[] request(Target target){return ScopedTransactionRpc.frame(KIND,target.encode());}
    public static Target decodeRequest(byte[] bytes){return target(body(bytes,MAX_REQUEST_BYTES));}
    private static ByteBuffer body(byte[] bytes,int maximum){if(ScopedTransactionRpc.kind(bytes,maximum)!=KIND)throw new IllegalArgumentException("Wrong topology owner frame kind");return ByteBuffer.wrap(bytes,ScopedTransactionRpc.HEADER_BYTES,bytes.length-ScopedTransactionRpc.HEADER_BYTES).slice();}
    private static Target target(ByteBuffer in){
        if(in.remaining()<53 || in.remaining()>MAX_TARGET_BYTES)throw new IllegalArgumentException("Invalid topology owner target size");
        UUID catalogue=ScopedTransactionRpc.uuid(in),group=ScopedTransactionRpc.uuid(in),replica=ScopedTransactionRpc.uuid(in);int stage=in.get()&255;
        if(stage>=TopologyChange.Stage.values().length)throw new IllegalArgumentException("Invalid topology owner stage");
        var definition=TopologyChange.Definition.decode(ScopedTransactionRpc.blob(in,TopologyChange.MAX_DEFINITION_BYTES,false));
        if(in.hasRemaining())throw new IllegalArgumentException("Trailing topology owner target bytes");return new Target(definition,TopologyChange.Stage.values()[stage],catalogue,group,replica);
    }
    private static byte[] owner(ReplicaDescriptor owner){var peer=owner.peerId().getBytes(StandardCharsets.US_ASCII);var address=owner.address().getBytes(StandardCharsets.US_ASCII);var out=ByteBuffer.allocate(57+peer.length+address.length);ScopedTransactionRpc.uuid(out,owner.nodeId());ScopedTransactionRpc.uuid(out,owner.nodeIncarnation());ScopedTransactionRpc.uuid(out,owner.replicaId());out.put((byte)owner.role().ordinal());ScopedTransactionRpc.putBlob(out,peer);ScopedTransactionRpc.putBlob(out,address);return out.array();}
    private static ReplicaDescriptor owner(byte[] bytes){var in=ByteBuffer.wrap(bytes);if(in.remaining()<57)throw new IllegalArgumentException("Truncated topology replica identity");UUID node=ScopedTransactionRpc.uuid(in),incarnation=ScopedTransactionRpc.uuid(in),replica=ScopedTransactionRpc.uuid(in);int role=in.get()&255;if(role>=ReplicaDescriptor.Role.values().length)throw new IllegalArgumentException("Invalid topology owner role");byte[] peer=ScopedTransactionRpc.blob(in,128,false),address=ScopedTransactionRpc.blob(in,256,false);if(in.hasRemaining())throw new IllegalArgumentException("Trailing topology replica identity");for(byte value:peer)if(value<0)throw new IllegalArgumentException("Non-ASCII peer");for(byte value:address)if(value<0)throw new IllegalArgumentException("Non-ASCII address");return new ReplicaDescriptor(node,incarnation,replica,new String(peer,StandardCharsets.US_ASCII),new String(address,StandardCharsets.US_ASCII),ReplicaDescriptor.Role.values()[role]);}
    public static byte[] reply(Evidence evidence){
        byte[] target=evidence.target().encode(),owner=owner(evidence.owner()),proof=evidence.proof().encode(),configuration=evidence.configuration(),artifact=evidence.artifact();
        var out=ByteBuffer.allocate(60+target.length+owner.length+proof.length+configuration.length+artifact.length);
        ScopedTransactionRpc.putBlob(out,target);ScopedTransactionRpc.putBlob(out,owner);ScopedTransactionRpc.putBlob(out,proof);
        out.putLong(evidence.completedCut().term()).putLong(evidence.completedCut().index()).putLong(evidence.configurationTerm()).putLong(evidence.configurationIndex());ScopedTransactionRpc.putBlob(out,configuration);out.putLong(evidence.raftTerm());ScopedTransactionRpc.putBlob(out,artifact);return ScopedTransactionRpc.frame(KIND,out.array());
    }
    public static Evidence decodeReply(byte[] bytes,Target expected){
        var in=body(bytes,MAX_REPLY_BYTES);var target=target(ByteBuffer.wrap(ScopedTransactionRpc.blob(in,MAX_TARGET_BYTES,false)));
        if(!Arrays.equals(target.encode(),expected.encode()))throw new IllegalArgumentException("Topology owner reply changed exact request");
        var owner=owner(ScopedTransactionRpc.blob(in,MAX_OWNER_BYTES,false));var proof=TopologyChange.Proof.decode(ScopedTransactionRpc.blob(in,214,false));
        if(in.remaining()<44)throw new IllegalArgumentException("Truncated topology owner native cut");
        var cut=new ApplyPosition(in.getLong(),in.getLong());long configurationTerm=in.getLong(),configurationIndex=in.getLong();byte[] configuration=ScopedTransactionRpc.blob(in,MAX_CONFIGURATION_BYTES,false);
        if(in.remaining()<12)throw new IllegalArgumentException("Truncated topology owner artifact");long term=in.getLong();byte[] artifact=ScopedTransactionRpc.blob(in,artifactMaximum(target.stage()),false);
        if(in.hasRemaining())throw new IllegalArgumentException("Trailing topology owner reply bytes");return new Evidence(target,owner,proof,cut,configurationTerm,configurationIndex,configuration,term,artifact);
    }
}
