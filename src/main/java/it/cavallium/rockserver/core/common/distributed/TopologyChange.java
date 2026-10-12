package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.*;

/** Structural topology intent and proof references. Native fresh admission must qualify their provenance. */
public final class TopologyChange {
    public static final int MAX_DEFINITION_BYTES=DistributedCommandCodec.MAX_ENCODED_BYTES-8192,MAX_HEADER_BYTES=8192,MAX_STATUS_BYTES=16384;
    private static final int DEFINITION_MAGIC=0x54434446,HEADER_MAGIC=0x54434844,STATE_MAGIC=0x54435354,PROOF_MAGIC=0x54435046;
    public enum Kind { SPLIT, MERGE, RELOCATE }
    public enum Phase { PREPARED, FENCING, FENCED, SEEDED, PUBLISHED, ACTIVE, RETIRED, ABORTED, JOINING, RECONFIGURED }
    public enum Stage { FREEZE, SEED, ACTIVATE }
    private TopologyChange() {}
    public record Owner(UUID replicaId,ReplicaDescriptor.Role role,byte[] identityDigest) {
        public Owner { Objects.requireNonNull(replicaId); Objects.requireNonNull(role); exact(identityDigest); identityDigest=identityDigest.clone(); }
        public static Owner of(ReplicaDescriptor replica) {
            byte[] peer=replica.peerId().getBytes(StandardCharsets.US_ASCII),address=replica.address().getBytes(StandardCharsets.US_ASCII);
            var bytes=ByteBuffer.allocate(57+peer.length+address.length); uuid(bytes,replica.nodeId()); uuid(bytes,replica.nodeIncarnation()); uuid(bytes,replica.replicaId()); blob(bytes,peer); blob(bytes,address); bytes.put((byte)replica.role().ordinal());
            return new Owner(replica.replicaId(),replica.role(),hash(bytes.array()));
        }
        @Override public byte[] identityDigest() { return identityDigest.clone(); }
        @Override public boolean equals(Object other) { return other instanceof Owner owner && replicaId.equals(owner.replicaId) && role==owner.role && Arrays.equals(identityDigest,owner.identityDigest); }
        @Override public int hashCode() { return 31*Objects.hash(replicaId,role)+Arrays.hashCode(identityDigest); }
    }
    public record Participant(UUID rangeId,UUID groupId,long rangeEpoch,long configurationVersion,List<Owner> owners) {
        public Participant {
            Objects.requireNonNull(rangeId); Objects.requireNonNull(groupId); Objects.requireNonNull(owners);
            if(rangeEpoch<0 || configurationVersion<0 || owners.isEmpty() || owners.size()>RangeDirectoryCodec.MAX_REPLICAS) throw new IllegalArgumentException("Invalid topology participant bounds");
            owners=owners.stream().sorted(Comparator.comparing(Owner::replicaId,RangeDirectoryCodec.UUID_ORDER)).toList(); var ids=new HashSet<UUID>(); int voters=0;
            for(var owner:owners) { if(!ids.add(owner.replicaId())) throw new IllegalArgumentException("Duplicate topology owner"); if(owner.role()==ReplicaDescriptor.Role.VOTER) voters++; }
            if(voters<1 || voters>5) throw new IllegalArgumentException("Topology group requires 1..5 voters");
        }
        static Participant of(RangeDescriptor range) { return new Participant(range.rangeId(),range.groupId(),range.rangeEpoch(),range.configurationVersion(),range.replicas().stream().map(Owner::of).toList()); }
        public Owner owner(UUID replica) { return owners.stream().filter(o->o.replicaId().equals(replica)).findFirst().orElseThrow(()->new IllegalArgumentException("Unknown topology owner")); }
        public int quorum() { return (int)owners.stream().filter(o->o.role()==ReplicaDescriptor.Role.VOTER).count()/2+1; }
    }
    public static final class Definition {
        private final UUID intentId;
        private final Kind kind;
        private final RangeDirectory source,target;
        private final List<RangeDescriptor> parents,targets;
        private final byte[] encoded,digest;
        public Definition(UUID intentId,RangeDirectory source,RangeDirectory target) { this(intentId,null,source,target); }
        public Definition(UUID intentId,Kind requested,RangeDirectory source,RangeDirectory target) {
            this.intentId=Objects.requireNonNull(intentId); this.source=Objects.requireNonNull(source); this.target=Objects.requireNonNull(target);
            if(!source.clusterId().equals(target.clusterId()) || !source.databaseId().equals(target.databaseId()) || !source.domainId().equals(target.domainId())
                    || source.schemaEpoch()!=target.schemaEpoch() || !Arrays.equals(source.schemaDigest(),target.schemaDigest()) || !source.columns().equals(target.columns()) || source.routingPrefixBytes()!=target.routingPrefixBytes()
                    || source.generation()==Long.MAX_VALUE || target.generation()!=source.generation()+1) throw new IllegalArgumentException("Topology changed namespace/schema/projection/generation");
            var before=new HashMap<UUID,RangeDescriptor>(); source.ranges().forEach(r->before.put(r.rangeId(),r)); var after=new HashMap<UUID,RangeDescriptor>(); target.ranges().forEach(r->after.put(r.rangeId(),r));
            parents=source.ranges().stream().filter(r->!r.equals(after.get(r.rangeId()))).toList(); targets=target.ranges().stream().filter(r->!r.equals(before.get(r.rangeId()))).toList();
            if(parents.size()==1 && targets.size()==2) kind=Kind.SPLIT; else if(parents.size()==2 && targets.size()==1) kind=Kind.MERGE;
            else if(parents.size()==1 && targets.size()==1) kind=Kind.RELOCATE; else throw new IllegalArgumentException("Topology must change one exact split/merge/relocation interval");
            if(requested!=null && requested!=kind) throw new IllegalArgumentException("Topology kind differs from directory delta");
            if(parents.stream().anyMatch(r->r.topologyState()!=RangeDescriptor.TopologyState.ACTIVE) || targets.stream().anyMatch(r->r.topologyState()!=RangeDescriptor.TopologyState.ACTIVE)) throw new IllegalArgumentException("Topology changed participant is not ACTIVE");
            switch(kind) {
                case SPLIT -> {
                    var parent=parents.getFirst(); var left=targets.getFirst(); var right=targets.getLast(); fresh(left); fresh(right);
                    if(!sameBound(parent.lowerInclusive(),left.lowerInclusive()) || !sameBound(parent.upperExclusive(),right.upperExclusive()) || left.upperExclusive()==null || !sameBound(left.upperExclusive(),right.lowerInclusive()) || left.upperExclusive().length==0) throw new IllegalArgumentException("Split changed parent interval/boundary");
                    membership(parent,left); membership(parent,right);
                }
                case MERGE -> {
                    var left=parents.getFirst(); var right=parents.getLast(); var merged=targets.getFirst(); fresh(merged);
                    if(left.upperExclusive()==null || !sameBound(left.upperExclusive(),right.lowerInclusive()) || !sameBound(left.lowerInclusive(),merged.lowerInclusive()) || !sameBound(right.upperExclusive(),merged.upperExclusive())) throw new IllegalArgumentException("Merge changed adjacent parent union");
                    membership(left,right); membership(left,merged);
                }
                case RELOCATE -> relocation(parents.getFirst(),targets.getFirst());
            }
            byte[] a=RangeDirectoryCodec.encode(source),b=RangeDirectoryCodec.encode(target); long size=29L+a.length+b.length; if(size>MAX_DEFINITION_BYTES) throw new IllegalArgumentException("Topology definition exceeds native command budget");
            var bytes=ByteBuffer.allocate((int)size).putInt(DEFINITION_MAGIC); uuid(bytes,intentId); bytes.put((byte)kind.ordinal()); blob(bytes,a); blob(bytes,b); encoded=bytes.array(); digest=hash(encoded);
        }
        private void fresh(RangeDescriptor range) { if(source.ranges().stream().anyMatch(r->r.rangeId().equals(range.rangeId()) || r.groupId().equals(range.groupId()))) throw new IllegalArgumentException("Replacement range/group identity was already in source"); }
        private static void membership(RangeDescriptor source,RangeDescriptor target) { if(source.configurationVersion()!=target.configurationVersion() || !source.replicas().equals(target.replicas())) throw new IllegalArgumentException("Normal split/merge changed configured membership"); }
        private static void relocation(RangeDescriptor source,RangeDescriptor target) {
            if(!source.rangeId().equals(target.rangeId()) || !source.groupId().equals(target.groupId()) || source.rangeEpoch()!=target.rangeEpoch() || !sameBound(source.lowerInclusive(),target.lowerInclusive()) || !sameBound(source.upperExclusive(),target.upperExclusive())
                    || source.configurationVersion()==Long.MAX_VALUE || target.configurationVersion()!=source.configurationVersion()+1) throw new IllegalArgumentException("Relocation changed lineage/interval/configuration step");
            var old=new HashMap<UUID,ReplicaDescriptor>(); source.replicas().forEach(r->old.put(r.replicaId(),r)); var next=new HashMap<UUID,ReplicaDescriptor>(); target.replicas().forEach(r->next.put(r.replicaId(),r)); int changes=0,removedVoters=0,promotedListeners=0;
            for(var replica:source.replicas()) {
                var replacement=next.get(replica.replicaId()); if(replacement==null) { changes++;if(replica.role()==ReplicaDescriptor.Role.VOTER)removedVoters++; }
                else if(!replica.equals(replacement)) {
                    if(replica.role()==replacement.role() || !replica.nodeId().equals(replacement.nodeId()) || !replica.nodeIncarnation().equals(replacement.nodeIncarnation()) || !replica.peerId().equals(replacement.peerId()) || !replica.address().equals(replacement.address())) throw new IllegalArgumentException("Relocation replaced an existing peer identity/endpoint"); changes++;if(replica.role()==ReplicaDescriptor.Role.LISTENER && replacement.role()==ReplicaDescriptor.Role.VOTER)promotedListeners++;
                }
            }
            for(var replica:target.replicas()) if(!old.containsKey(replica.replicaId())) { if(replica.role()!=ReplicaDescriptor.Role.LISTENER) throw new IllegalArgumentException("New relocation peer must be staged as LISTENER"); changes++; }
            if(changes!=1 && !(changes==2 && removedVoters==1 && promotedListeners==1)) throw new IllegalArgumentException("Relocation requires one membership step or one staged voter replacement");
        }
        public UUID intentId() { return intentId; }
        public Kind kind() { return kind; }
        public RangeDirectory sourceDirectory() { return source; }
        public RangeDirectory targetDirectory() { return target; }
        public List<RangeDescriptor> parents() { return parents; }
        public List<RangeDescriptor> targets() { return targets; }
        public byte[] encode() { return encoded.clone(); }
        public byte[] digest() { return digest.clone(); }
        public Header header() { return new Header(this); }
        public static Definition decode(byte[] bytes) {
            var in=input(bytes,MAX_DEFINITION_BYTES,29,DEFINITION_MAGIC); UUID id=uuid(in); var kind=tag(Kind.values(),in.get()); var source=RangeDirectoryCodec.decode(blob(in,RangeDirectoryCodec.MAX_ENCODED_BYTES)); var target=RangeDirectoryCodec.decode(blob(in,RangeDirectoryCodec.MAX_ENCODED_BYTES)); end(in);
            var definition=new Definition(id,kind,source,target); canonical(bytes,definition.encode()); return definition;
        }
    }
    public static final class Header {
        private final UUID intent,cluster,database,domain;
        private final Kind kind;
        private final long schemaEpoch,sourceGeneration,targetGeneration;
        private final byte[] schemaDigest,definitionDigest,sourceDigest,targetDigest;
        private final List<Participant> parents,targets;
        private Header(Definition d) { this(d.intentId(),d.source.clusterId(),d.source.databaseId(),d.source.domainId(),d.kind(),d.source.schemaEpoch(),d.source.generation(),d.target.generation(),d.source.schemaDigest(),d.digest(),d.source.payloadDigest(),d.target.payloadDigest(),d.parents.stream().map(Participant::of).toList(),d.targets.stream().map(Participant::of).toList()); }
        private Header(UUID intent,UUID cluster,UUID database,UUID domain,Kind kind,long schemaEpoch,long sourceGeneration,long targetGeneration,byte[] schemaDigest,byte[] definitionDigest,byte[] sourceDigest,byte[] targetDigest,List<Participant> parents,List<Participant> targets) {
            this.intent=Objects.requireNonNull(intent); this.cluster=Objects.requireNonNull(cluster); this.database=Objects.requireNonNull(database); this.domain=Objects.requireNonNull(domain); this.kind=Objects.requireNonNull(kind); this.schemaEpoch=schemaEpoch; this.sourceGeneration=sourceGeneration; this.targetGeneration=targetGeneration;
            for(var digest:List.of(schemaDigest,definitionDigest,sourceDigest,targetDigest)) exact(digest); this.schemaDigest=schemaDigest.clone(); this.definitionDigest=definitionDigest.clone(); this.sourceDigest=sourceDigest.clone(); this.targetDigest=targetDigest.clone();
            if(schemaEpoch<0 || sourceGeneration<0 || sourceGeneration==Long.MAX_VALUE || targetGeneration!=sourceGeneration+1 || parents.size()!=(kind==Kind.MERGE ? 2 : 1) || targets.size()!=(kind==Kind.SPLIT ? 2 : 1)) throw new IllegalArgumentException("Invalid topology header shape/generation");
            this.parents=ordered(parents); this.targets=ordered(targets);
            if(kind==Kind.RELOCATE) { var p=this.parents.getFirst(); var t=this.targets.getFirst(); if(!p.rangeId().equals(t.rangeId()) || !p.groupId().equals(t.groupId()) || p.rangeEpoch()!=t.rangeEpoch() || p.configurationVersion()==Long.MAX_VALUE || t.configurationVersion()!=p.configurationVersion()+1) throw new IllegalArgumentException("Relocation header changed lineage/config step"); }
            else for(var p:this.parents) for(var t:this.targets) if(p.rangeId().equals(t.rangeId()) || p.groupId().equals(t.groupId())) throw new IllegalArgumentException("Topology header reused parent identity");
        }
        private static List<Participant> ordered(List<Participant> list) { var groups=new HashSet<UUID>(); var ranges=new HashSet<UUID>(); for(var p:list) if(!groups.add(p.groupId()) || !ranges.add(p.rangeId())) throw new IllegalArgumentException("Duplicate topology participant"); return list.stream().sorted(Comparator.comparing(Participant::groupId,RangeDirectoryCodec.UUID_ORDER)).toList(); }
        public UUID intentId() { return intent; }
        public UUID clusterId() { return cluster; }
        public UUID databaseId() { return database; }
        public UUID domainId() { return domain; }
        public Kind kind() { return kind; }
        public long schemaEpoch() { return schemaEpoch; }
        public long sourceGeneration() { return sourceGeneration; }
        public long targetGeneration() { return targetGeneration; }
        public byte[] schemaDigest() { return schemaDigest.clone(); }
        public byte[] definitionDigest() { return definitionDigest.clone(); }
        public byte[] sourceDirectoryDigest() { return sourceDigest.clone(); }
        public byte[] targetDirectoryDigest() { return targetDigest.clone(); }
        public List<Participant> parents() { return parents; }
        public List<Participant> targets() { return targets; }
        public Participant participant(Stage stage,UUID group) { return (stage==Stage.FREEZE ? parents : targets).stream().filter(p->p.groupId().equals(group)).findFirst().orElseThrow(()->new IllegalArgumentException("Unknown topology stage participant")); }
        public void requireDefinition(Definition definition) { if(!Arrays.equals(encode(),definition.header().encode())) throw new IllegalArgumentException("Topology header differs from immutable definition"); }
        public byte[] encode() {
            int size=229; for(var p:parents) size+=52+49*p.owners().size(); for(var p:targets) size+=52+49*p.owners().size(); if(size>MAX_HEADER_BYTES) throw new IllegalArgumentException("Topology header exceeds bound");
            var out=ByteBuffer.allocate(size).putInt(HEADER_MAGIC); uuid(out,intent); uuid(out,cluster); uuid(out,database); uuid(out,domain); out.put((byte)kind.ordinal()).putLong(schemaEpoch).putLong(sourceGeneration).putLong(targetGeneration).put(schemaDigest).put(definitionDigest).put(sourceDigest).put(targetDigest); participants(out,parents); participants(out,targets); return out.array();
        }
        public static Header decode(byte[] bytes) {
            var in=input(bytes,MAX_HEADER_BYTES,229,HEADER_MAGIC); UUID intent=uuid(in),cluster=uuid(in),database=uuid(in),domain=uuid(in); var kind=tag(Kind.values(),in.get()); long epoch=in.getLong(),source=in.getLong(),target=in.getLong();
            var header=new Header(intent,cluster,database,domain,kind,epoch,source,target,readDigest(in),readDigest(in),readDigest(in),readDigest(in),participants(in),participants(in)); end(in); canonical(bytes,header.encode()); return header;
        }
    }
    /** Position is the stage entry/local birth in this participant's group, never a parent cut copied to a new group. */
    public record Proof(Stage stage,byte[] definitionDigest,UUID rangeId,UUID groupId,long rangeEpoch,long configurationVersion,Owner owner,ApplyPosition position,byte[] artifactDigest,byte[] predecessorDigest) {
        public Proof {
            Objects.requireNonNull(stage); Objects.requireNonNull(rangeId); Objects.requireNonNull(groupId); Objects.requireNonNull(owner); Objects.requireNonNull(position); exact(definitionDigest); exact(artifactDigest); exact(predecessorDigest);
            if(rangeEpoch<0 || configurationVersion<0 || (stage==Stage.SEED ? position.term()==0 && position.index()!=0 : position.term()==0 || position.index()==0)) throw new IllegalArgumentException("Invalid topology proof binding/cut");
            definitionDigest=definitionDigest.clone(); artifactDigest=artifactDigest.clone(); predecessorDigest=predecessorDigest.clone();
        }
        public static Proof of(Stage stage,Header header,UUID group,UUID replica,ApplyPosition position,byte[] artifact,byte[] predecessor) { var p=header.participant(stage,group); return new Proof(stage,header.definitionDigest(),p.rangeId(),p.groupId(),p.rangeEpoch(),p.configurationVersion(),p.owner(replica),position,artifact,predecessor); }
        @Override public byte[] definitionDigest() { return definitionDigest.clone(); }
        @Override public byte[] artifactDigest() { return artifactDigest.clone(); }
        @Override public byte[] predecessorDigest() { return predecessorDigest.clone(); }
        public byte[] encode() { var out=ByteBuffer.allocate(214).putInt(PROOF_MAGIC).put((byte)stage.ordinal()).put(definitionDigest); uuid(out,rangeId); uuid(out,groupId); out.putLong(rangeEpoch).putLong(configurationVersion); writeOwner(out,owner); out.putLong(position.term()).putLong(position.index()).put(artifactDigest).put(predecessorDigest); return out.array(); }
        public byte[] digest() { return hash(encode()); }
        public static Proof decode(byte[] bytes) { var in=input(bytes,214,214,PROOF_MAGIC); var proof=new Proof(tag(Stage.values(),in.get()),readDigest(in),uuid(in),uuid(in),in.getLong(),in.getLong(),readOwner(in),new ApplyPosition(in.getLong(),in.getLong()),readDigest(in),readDigest(in)); end(in); canonical(bytes,proof.encode()); return proof; }
        @Override public boolean equals(Object other) { return other instanceof Proof proof && Arrays.equals(encode(),proof.encode()); }
        @Override public int hashCode() { return Arrays.hashCode(encode()); }
    }
    /** Genuine target configuration commitment and native catch-up cuts, qualified freshly by the adapter. */
    public record RelocationProof(UUID groupId,Owner owner,ApplyPosition configurationPosition,ApplyPosition completedPosition,ApplyPosition requiredCatchupCut,byte[] configurationDigest) {
        public static final int BYTES=149;
        public RelocationProof { Objects.requireNonNull(groupId);Objects.requireNonNull(owner);Objects.requireNonNull(configurationPosition);Objects.requireNonNull(completedPosition);Objects.requireNonNull(requiredCatchupCut);exact(configurationDigest);configurationDigest=configurationDigest.clone();if(owner.role()!=ReplicaDescriptor.Role.VOTER || configurationPosition.term()<1 || configurationPosition.index()<1 || requiredCatchupCut.term()<1 || !covers(completedPosition,configurationPosition) || !covers(completedPosition,requiredCatchupCut))throw new IllegalArgumentException("Relocation proof lacks committed configuration/native catch-up"); }
        private static boolean covers(ApplyPosition cut,ApplyPosition expected){return cut.term()>=expected.term() && cut.index()>=expected.index() && (cut.index()!=expected.index() || cut.term()==expected.term());}
        @Override public byte[] configurationDigest(){return configurationDigest.clone();}
        public byte[] encode(){var out=ByteBuffer.allocate(BYTES).putInt(0x54524350);uuid(out,groupId);writeOwner(out,owner);for(var cut:List.of(configurationPosition,completedPosition,requiredCatchupCut))out.putLong(cut.term()).putLong(cut.index());return out.put(configurationDigest).array();}
        public static RelocationProof decode(byte[] bytes){var in=input(bytes,BYTES,BYTES,0x54524350);var group=uuid(in);var owner=readOwner(in);var config=new ApplyPosition(in.getLong(),in.getLong());var completed=new ApplyPosition(in.getLong(),in.getLong());var required=new ApplyPosition(in.getLong(),in.getLong());var result=new RelocationProof(group,owner,config,completed,required,readDigest(in));end(in);canonical(bytes,result.encode());return result;}
        @Override public boolean equals(Object other){return other instanceof RelocationProof proof && Arrays.equals(encode(),proof.encode());}
        @Override public int hashCode(){return Arrays.hashCode(encode());}
    }
    public record Key(UUID groupId,UUID replicaId) { public Key { Objects.requireNonNull(groupId); Objects.requireNonNull(replicaId); } }
    public static final class State {
        private final Header header;
        private final long generation;
        private final Phase phase;
        private final Map<Key,Proof> frozen,seeded,activated;
        private final RelocationProof relocation;
        private State(Header header,long generation,Phase phase,Map<Key,Proof> frozen,Map<Key,Proof> seeded,Map<Key,Proof> activated) {
            this(header,generation,phase,frozen,seeded,activated,null);
        }
        private State(Header header,long generation,Phase phase,Map<Key,Proof> frozen,Map<Key,Proof> seeded,Map<Key,Proof> activated,RelocationProof relocation) {
            this.relocation=relocation;
            this.header=Objects.requireNonNull(header); this.generation=generation; this.phase=Objects.requireNonNull(phase); this.frozen=Map.copyOf(frozen); this.seeded=Map.copyOf(seeded); this.activated=Map.copyOf(activated);
            for(var stage:Stage.values()) for(var entry:proofs(stage).entrySet()) { var proof=entry.getValue(); bind(proof,stage); if(!entry.getKey().equals(key(proof))) throw new IllegalArgumentException("Topology proof key differs"); }
            for(var stage:Stage.values()) for(var p:stage==Stage.FREEZE ? header.parents : header.targets) {
                ApplyPosition cut=null; for(var proof:proofs(stage).values()) if(proof.groupId().equals(p.groupId())) { if(cut!=null && !cut.equals(proof.position())) throw new IllegalArgumentException("Owners disagree on original group stage cut"); cut=proof.position(); }
            }
            boolean f=quorum(Stage.FREEZE),s=quorum(Stage.SEED),a=quorum(Stage.ACTIVATE);
            if(header.kind==Kind.RELOCATE){
                if(!this.frozen.isEmpty() || !this.seeded.isEmpty() || !this.activated.isEmpty())throw new IllegalArgumentException("Writable relocation cannot carry split execution proofs");
                long expected=switch(phase){case PREPARED->0;case ABORTED,JOINING->1;case RECONFIGURED->2;case ACTIVE->3;default->throw new IllegalArgumentException("Invalid writable relocation phase");};
                if(generation!=expected || (phase==Phase.RECONFIGURED || phase==Phase.ACTIVE)!=(relocation!=null))throw new IllegalArgumentException("Relocation generation/configuration proof differs");
                if(relocation!=null){var target=header.targets.getFirst();if(!target.groupId().equals(relocation.groupId()) || !target.owner(relocation.owner().replicaId()).equals(relocation.owner()))throw new IllegalArgumentException("Relocation proof changed target owner");}return;
            }
            if(relocation!=null || phase==Phase.JOINING || phase==Phase.RECONFIGURED)throw new IllegalArgumentException("Split/merge carries relocation authority");
            if(phase==Phase.PREPARED || phase==Phase.ABORTED) { if(!this.frozen.isEmpty() || !this.seeded.isEmpty() || !this.activated.isEmpty()) throw new IllegalArgumentException("Prepared/aborted topology has execution proofs"); }
            else if(phase==Phase.FENCING ? f || !this.seeded.isEmpty() || !this.activated.isEmpty() : !f) throw new IllegalArgumentException("Topology lacks parent freeze quorum");
            if((phase==Phase.FENCED && s) || (phase==Phase.SEEDED || phase==Phase.PUBLISHED || phase==Phase.ACTIVE || phase==Phase.RETIRED) && !s || (phase!=Phase.PUBLISHED && phase!=Phase.ACTIVE && phase!=Phase.RETIRED) && !this.activated.isEmpty() || phase==Phase.PUBLISHED && a || (phase==Phase.ACTIVE || phase==Phase.RETIRED) && !a) throw new IllegalArgumentException("Topology readiness differs from seed/activation quorums");
            for(var proof:this.frozen.values()) if(!Arrays.equals(proof.predecessorDigest(),header.sourceDigest)) throw new IllegalArgumentException("Freeze proof changed source commitment");
            for(var proof:this.seeded.values()) if(!Arrays.equals(proof.predecessorDigest(),freezeDigest())) throw new IllegalArgumentException("Seed proof changed parent cut commitment");
            for(var proof:this.activated.values()) { var seed=this.seeded.get(key(proof)); if(seed==null || !Arrays.equals(proof.predecessorDigest(),seed.digest()) || proof.position().index()<=seed.position().index() || proof.position().term()<seed.position().term()) throw new IllegalArgumentException("Owner activation lacks its exact local seed/birth"); }
            long base=switch(phase) { case PREPARED -> 0; case FENCING,FENCED,SEEDED -> 1; case PUBLISHED,ACTIVE -> 2; case RETIRED -> 3; case ABORTED -> 1; case JOINING,RECONFIGURED -> throw new IllegalArgumentException("Not a split phase"); };
            if(generation!=base+this.frozen.size()+this.seeded.size()+this.activated.size()) throw new IllegalArgumentException("Topology generation differs from single-step history");
        }
        public static State begin(Header header) { return new State(header,0,Phase.PREPARED,Map.of(),Map.of(),Map.of()); }
        public Header header() { return header; }
        public Phase phase() { return phase; }
        public long generation() { return generation; }
        public boolean terminal() { return phase==Phase.RETIRED || phase==Phase.ABORTED || header.kind==Kind.RELOCATE && phase==Phase.ACTIVE; }
        public RelocationProof relocation(){return relocation;}
        public State beginJoining(){if(header.kind!=Kind.RELOCATE || phase!=Phase.PREPARED && phase!=Phase.JOINING)throw new IllegalStateException("Joining requires prepared relocation");return phase==Phase.JOINING?this:new State(header,1,Phase.JOINING,frozen,seeded,activated,null);}
        public State reconfigured(RelocationProof proof){Objects.requireNonNull(proof);if(header.kind!=Kind.RELOCATE || phase!=Phase.JOINING && phase!=Phase.RECONFIGURED)throw new IllegalStateException("Configuration proof requires irreversible JOINING");if(phase==Phase.RECONFIGURED){if(!relocation.equals(proof))throw new IllegalArgumentException("Original relocation proof changed");return this;}return new State(header,2,Phase.RECONFIGURED,frozen,seeded,activated,proof);}
        public State publishRelocation(){if(header.kind!=Kind.RELOCATE || phase!=Phase.RECONFIGURED && phase!=Phase.ACTIVE)throw new IllegalStateException("Publication requires committed relocation readiness");return phase==Phase.ACTIVE?this:new State(header,3,Phase.ACTIVE,frozen,seeded,activated,relocation);}
        public Map<Key,Proof> frozen() { return frozen; }
        public Map<Key,Proof> seeded() { return seeded; }
        public Map<Key,Proof> activated() { return activated; }
        public void requireDefinition(Definition definition) { header.requireDefinition(definition); }
        private Map<Key,Proof> proofs(Stage stage) { return switch(stage) { case FREEZE -> frozen; case SEED -> seeded; case ACTIVATE -> activated; }; }
        private void bind(Proof proof,Stage stage) { var p=header.participant(stage,proof.groupId()); if(proof.stage()!=stage || !Arrays.equals(header.definitionDigest,proof.definitionDigest()) || !p.rangeId().equals(proof.rangeId()) || p.rangeEpoch()!=proof.rangeEpoch() || p.configurationVersion()!=proof.configurationVersion() || !p.owner(proof.owner().replicaId()).equals(proof.owner())) throw new IllegalArgumentException("Topology proof changed immutable stage/owner binding"); }
        private boolean quorum(Stage stage) { return quorum(header,stage,proofs(stage)); }
        private static boolean quorum(Header header,Stage stage,Map<Key,Proof> proofs) { for(var p:stage==Stage.FREEZE ? header.parents : header.targets) { long voters=p.owners().stream().filter(o->o.role()==ReplicaDescriptor.Role.VOTER && proofs.containsKey(new Key(p.groupId(),o.replicaId()))).count(); if(voters<p.quorum()) return false; } return true; }
        public byte[] freezeDigest() {
            if(!quorum(Stage.FREEZE)) throw new IllegalStateException("Parent cut commitment requires freeze quorums"); var out=ByteBuffer.allocate(32+64*header.parents.size()).put(header.definitionDigest);
            for(var p:header.parents) { var cut=frozen.values().stream().filter(proof->proof.groupId().equals(p.groupId())).findFirst().orElseThrow().position(); uuid(out,p.rangeId()); uuid(out,p.groupId()); out.putLong(p.rangeEpoch()).putLong(p.configurationVersion()).putLong(cut.term()).putLong(cut.index()); } return hash(out.array());
        }
        private void executable() { if(header.kind==Kind.RELOCATE) throw new UnsupportedOperationException("Relocation requires its own writable configuration/catch-up workflow"); }
        public State beginFencing() { executable(); if(phase==Phase.FENCING) return this; if(phase!=Phase.PREPARED) throw new IllegalStateException("Fencing requires PREPARED intent"); return new State(header,generation+1,Phase.FENCING,frozen,seeded,activated); }
        public State frozen(Proof proof) { return add(Stage.FREEZE,proof); }
        public State seeded(Proof proof) { return add(Stage.SEED,proof); }
        public State activated(Proof proof) { return add(Stage.ACTIVATE,proof); }
        private State add(Stage stage,Proof proof) {
            executable(); bind(proof,stage); var prior=proofs(stage).get(key(proof)); if(prior!=null) { if(!prior.equals(proof)) throw new IllegalArgumentException("Topology original proof changed"); return this; }
            if(terminal() || phase==Phase.PREPARED || stage==Stage.SEED && (phase==Phase.FENCING) || stage==Stage.ACTIVATE && phase!=Phase.PUBLISHED && phase!=Phase.ACTIVE) throw new IllegalStateException("Topology proof submitted before its stage");
            var next=new HashMap<>(proofs(stage)); next.put(key(proof),proof); Phase after=phase;
            if(stage==Stage.FREEZE && phase==Phase.FENCING && quorum(header,stage,next)) after=Phase.FENCED;
            if(stage==Stage.SEED && phase==Phase.FENCED && quorum(header,stage,next)) after=Phase.SEEDED;
            if(stage==Stage.ACTIVATE && phase==Phase.PUBLISHED && quorum(header,stage,next)) after=Phase.ACTIVE;
            return new State(header,generation+1,after,stage==Stage.FREEZE ? next : frozen,stage==Stage.SEED ? next : seeded,stage==Stage.ACTIVATE ? next : activated);
        }
        public State publish() { return advance(Phase.SEEDED,Phase.PUBLISHED); }
        public State retire() { return advance(Phase.ACTIVE,Phase.RETIRED); }
        private State advance(Phase before,Phase next) { executable(); if(phase==next) return this; if(phase!=before) throw new IllegalStateException("Topology cannot skip execution stage"); return new State(header,generation+1,next,frozen,seeded,activated); }
        public State abort() { if(phase==Phase.ABORTED) return this; if(phase!=Phase.PREPARED) throw new IllegalStateException("Topology cannot abandon begun fencing"); return new State(header,generation+1,Phase.ABORTED,frozen,seeded,activated); }
        public void requireSuccessorOf(State previous) {
            if(Arrays.equals(encode(),previous.encode())) return;
            if(!Arrays.equals(header.encode(),previous.header.encode()) || generation!=previous.generation+1) throw new IllegalArgumentException("Topology successor changed intent/generation"); State expected=null; int additions=0;
            try {
                if(header.kind==Kind.RELOCATE){expected=switch(phase){case JOINING->previous.beginJoining();case RECONFIGURED->previous.reconfigured(relocation);case ACTIVE->previous.publishRelocation();case ABORTED->previous.abort();default->null;};if(expected==null || !equals(expected))throw new IllegalArgumentException("Relocation successor is not one exact phase step");return;}
                for(var stage:Stage.values()) for(var entry:proofs(stage).entrySet()) if(!previous.proofs(stage).containsKey(entry.getKey())) { expected=previous.add(stage,entry.getValue()); additions++; }
                if(additions==0) expected=switch(phase) { case FENCING -> previous.beginFencing(); case PUBLISHED -> previous.publish(); case RETIRED -> previous.retire(); case ABORTED -> previous.abort(); default -> null; };
            } catch(IllegalStateException|UnsupportedOperationException invalid) { throw new IllegalArgumentException("Illegal topology successor phase",invalid); }
            if(additions>1 || expected==null || !Arrays.equals(encode(),expected.encode())) throw new IllegalArgumentException("Topology successor is not one proof/phase step");
        }
        public byte[] encode() {
            byte[] h=header.encode(); int size=33+h.length+82*(frozen.size()+seeded.size()+activated.size())+(relocation==null?0:RelocationProof.BYTES); if(size>MAX_STATUS_BYTES) throw new IllegalArgumentException("Topology state exceeds bound");
            var out=ByteBuffer.allocate(size).putInt(STATE_MAGIC); blob(out,h); out.putLong(generation).put((byte)phase.ordinal()); for(var stage:Stage.values()) compact(out,header,stage,proofs(stage));out.putInt(relocation==null?0:RelocationProof.BYTES);if(relocation!=null)out.put(relocation.encode()); return out.array();
        }
        @Override public boolean equals(Object other) { return other instanceof State state && Arrays.equals(encode(),state.encode()); }
        @Override public int hashCode() { return Arrays.hashCode(encode()); }
        public static State decode(byte[] bytes) { var in=input(bytes,MAX_STATUS_BYTES,29,STATE_MAGIC); var header=Header.decode(blob(in,MAX_HEADER_BYTES)); if(in.remaining()<21) throw new IllegalArgumentException("Truncated topology state"); long generation=in.getLong(); var phase=tag(Phase.values(),in.get()); var frozen=compact(in,header,Stage.FREEZE);var seeded=compact(in,header,Stage.SEED);var activated=compact(in,header,Stage.ACTIVATE);if(in.remaining()<4)throw new IllegalArgumentException("Missing relocation proof framing");int length=in.getInt();if(length!=0 && length!=RelocationProof.BYTES || length>in.remaining())throw new IllegalArgumentException("Invalid relocation proof length");RelocationProof relocation=null;if(length!=0){byte[] proof=new byte[length];in.get(proof);relocation=RelocationProof.decode(proof);}var state=new State(header,generation,phase,frozen,seeded,activated,relocation); end(in); canonical(bytes,state.encode()); return state; }
    }
    private static Key key(Proof proof) { return new Key(proof.groupId(),proof.owner().replicaId()); }
    private static void compact(ByteBuffer out,Header header,Stage stage,Map<Key,Proof> map) {
        var participants=stage==Stage.FREEZE ? header.parents : header.targets; out.putInt(map.size());
        for(var key:map.keySet().stream().sorted(Comparator.comparing(Key::groupId,RangeDirectoryCodec.UUID_ORDER).thenComparing(Key::replicaId,RangeDirectoryCodec.UUID_ORDER)).toList()) {
            var proof=map.get(key); var p=header.participant(stage,key.groupId()); out.put((byte)participants.indexOf(p)).put((byte)p.owners().indexOf(proof.owner())).putLong(proof.position().term()).putLong(proof.position().index()).put(proof.artifactDigest()).put(proof.predecessorDigest());
        }
    }
    private static Map<Key,Proof> compact(ByteBuffer in,Header header,Stage stage) {
        var participants=stage==Stage.FREEZE ? header.parents : header.targets; if(in.remaining()<4) throw new IllegalArgumentException("Truncated topology proofs"); int count=in.getInt(); if(count<0 || count>participants.stream().mapToInt(p->p.owners().size()).sum() || 82L*count>in.remaining()) throw new IllegalArgumentException("Invalid topology proof count"); var map=new HashMap<Key,Proof>();
        for(int i=0;i<count;i++) { int group=in.get()&255,owner=in.get()&255; if(group>=participants.size() || owner>=participants.get(group).owners().size()) throw new IllegalArgumentException("Invalid topology proof owner index"); var p=participants.get(group); var o=p.owners().get(owner); var proof=Proof.of(stage,header,p.groupId(),o.replicaId(),new ApplyPosition(in.getLong(),in.getLong()),readDigest(in),readDigest(in)); if(map.put(key(proof),proof)!=null) throw new IllegalArgumentException("Duplicate topology proof"); } return map;
    }
    private static void participants(ByteBuffer out,List<Participant> list) { out.putInt(list.size()); for(var p:list) { uuid(out,p.rangeId()); uuid(out,p.groupId()); out.putLong(p.rangeEpoch()).putLong(p.configurationVersion()).putInt(p.owners().size()); for(var owner:p.owners()) writeOwner(out,owner); } }
    private static List<Participant> participants(ByteBuffer in) { if(in.remaining()<4) throw new IllegalArgumentException("Truncated topology participants"); int count=in.getInt(); if(count<1 || count>2 || count*52L>in.remaining()) throw new IllegalArgumentException("Invalid topology participant count"); var list=new ArrayList<Participant>(); for(int i=0;i<count;i++) { UUID range=uuid(in),group=uuid(in); long epoch=in.getLong(),config=in.getLong(); int owners=in.getInt(); if(owners<1 || owners>RangeDirectoryCodec.MAX_REPLICAS || owners*49L>in.remaining()) throw new IllegalArgumentException("Invalid topology owner count"); var replicas=new ArrayList<Owner>(); for(int n=0;n<owners;n++) replicas.add(readOwner(in)); list.add(new Participant(range,group,epoch,config,replicas)); } return list; }
    private static void writeOwner(ByteBuffer out,Owner owner) { uuid(out,owner.replicaId()); out.put((byte)owner.role().ordinal()).put(owner.identityDigest()); }
    private static Owner readOwner(ByteBuffer in) { return new Owner(uuid(in),tag(ReplicaDescriptor.Role.values(),in.get()),readDigest(in)); }
    private static byte[] hash(byte[] bytes) { try { return MessageDigest.getInstance("SHA-256").digest(bytes); } catch(NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); } }
    private static void exact(byte[] digest) { if(digest==null || digest.length!=32) throw new IllegalArgumentException("Invalid topology digest"); }
    private static boolean sameBound(byte[] a,byte[] b) { return Arrays.equals(a,b); }
    private static ByteBuffer input(byte[] bytes,int maximum,int minimum,int magic) { if(bytes==null || bytes.length<minimum || bytes.length>maximum) throw new IllegalArgumentException("Invalid topology encoding bound"); var in=ByteBuffer.wrap(bytes); if(in.getInt()!=magic) throw new IllegalArgumentException("Invalid topology encoding tag"); return in; }
    private static <T> T tag(T[] values,byte value) { int index=value&255; if(index>=values.length) throw new IllegalArgumentException("Unknown topology tag"); return values[index]; }
    private static byte[] readDigest(ByteBuffer in) { if(in.remaining()<32) throw new IllegalArgumentException("Truncated topology digest"); byte[] bytes=new byte[32]; in.get(bytes); return bytes; }
    private static byte[] blob(ByteBuffer in,int maximum) { if(in.remaining()<4) throw new IllegalArgumentException("Truncated topology blob"); int n=in.getInt(); if(n<0 || n>maximum || n>in.remaining()) throw new IllegalArgumentException("Invalid topology blob size"); byte[] bytes=new byte[n]; in.get(bytes); return bytes; }
    private static void blob(ByteBuffer out,byte[] bytes) { out.putInt(bytes.length).put(bytes); }
    private static UUID uuid(ByteBuffer in) { if(in.remaining()<16) throw new IllegalArgumentException("Truncated topology UUID"); return new UUID(in.getLong(),in.getLong()); }
    private static void uuid(ByteBuffer out,UUID id) { out.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()); }
    private static void end(ByteBuffer in) { if(in.hasRemaining()) throw new IllegalArgumentException("Trailing topology bytes"); }
    private static void canonical(byte[] actual,byte[] expected) { if(!Arrays.equals(actual,expected)) throw new IllegalArgumentException("Noncanonical topology encoding"); }
}
