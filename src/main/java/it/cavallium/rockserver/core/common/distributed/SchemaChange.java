package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/** Durable administrative intent and proofs; this model does not execute native schema changes. */
public final class SchemaChange {
    public static final int MAX_PARTICIPANTS=64;
    public static final int MAX_DEFINITION_BYTES=2*1024*1024-8192;
    public static final int MAX_STATUS_BYTES=16*1024-1;
    private static final int DEFINITION_MAGIC=0x53434446, HEADER_MAGIC=0x53434844, STATUS_MAGIC=0x53435354;
    public enum Kind { CREATE_COLUMN, DELETE_COLUMN }
    public enum Phase { PREPARED, FENCING, FENCED, INSTALLED, PUBLISHED, RETIRED, ABORTED }
    public record Participant(UUID rangeId,UUID groupId,long rangeEpoch,long configurationVersion) {
        public Participant {
            Objects.requireNonNull(rangeId); Objects.requireNonNull(groupId);
            if (rangeEpoch<0 || configurationVersion<0) throw new IllegalArgumentException("Negative participant epoch");
        }
    }
    private SchemaChange() {}

    public static final class Definition {
        private final UUID changeId;
        private final OperationId beginOperation;
        private final RangeDirectory sourceDirectory,targetDirectory;
        private final DistributedSchema sourceSchema,targetSchema;
        private final Kind kind;
        public Definition(UUID changeId,OperationId beginOperation,RangeDirectory sourceDirectory,
                          DistributedSchema sourceSchema,DistributedSchema targetSchema) {
            this.changeId=Objects.requireNonNull(changeId); this.beginOperation=Objects.requireNonNull(beginOperation);
            this.sourceDirectory=Objects.requireNonNull(sourceDirectory); this.sourceSchema=Objects.requireNonNull(sourceSchema);
            this.targetSchema=Objects.requireNonNull(targetSchema); sourceSchema.requireMatches(sourceDirectory);
            if (!sourceSchema.clusterId().equals(targetSchema.clusterId()) || !sourceSchema.databaseId().equals(targetSchema.databaseId())
                    || !sourceSchema.domainId().equals(targetSchema.domainId()) || sourceSchema.routingPrefixBytes()!=targetSchema.routingPrefixBytes()
                    || sourceSchema.schemaEpoch()==Long.MAX_VALUE || targetSchema.schemaEpoch()!=sourceSchema.schemaEpoch()+1
                    || sourceDirectory.generation()==Long.MAX_VALUE || sourceDirectory.ranges().size()>MAX_PARTICIPANTS
                    || sourceDirectory.ranges().stream().anyMatch(r -> r.topologyState()!=RangeDescriptor.TopologyState.ACTIVE)) {
                throw new IllegalArgumentException("Schema change requires exact active source domain and next schema epoch");
            }
            Set<UUID> added=new HashSet<>(targetSchema.columnIds()), removed=new HashSet<>(sourceSchema.columnIds());
            added.removeAll(sourceSchema.columnIds()); removed.removeAll(targetSchema.columnIds());
            if (added.size()==1 && removed.isEmpty()) kind=Kind.CREATE_COLUMN;
            else if (removed.size()==1 && added.isEmpty()) kind=Kind.DELETE_COLUMN;
            else throw new IllegalArgumentException("Schema change must add or remove exactly one column");
            for (var column:sourceSchema.columns()) if (!removed.contains(column.columnId()) && !column.equals(targetSchema.column(column.columnId()))) {
                throw new IllegalArgumentException("Existing column content must remain identical");
            }
            if (kind==Kind.CREATE_COLUMN) {
                var column=targetSchema.column(added.iterator().next());
                long maximum=sourceSchema.columns().stream().mapToLong(DistributedSchema.Column::apiId).max().orElse(0);
                if (column.apiId()<=maximum || sourceSchema.columns().stream().anyMatch(c -> c.name().equals(column.name()))) {
                    throw new IllegalArgumentException("New column must use a fresh name and increasing API ID");
                }
            }
            targetDirectory=new RangeDirectory(sourceDirectory.clusterId(),sourceDirectory.databaseId(),sourceDirectory.domainId(),
                    targetSchema.schemaEpoch(),targetSchema.digest(),sourceDirectory.generation()+1,targetSchema.columnIds(),
                    sourceDirectory.routingPrefixBytes(),sourceDirectory.ranges());
            long size=52L+12+RangeDirectoryCodec.encodedSize(sourceDirectory)+DistributedSchemaCodec.encodedSize(sourceSchema)
                    +DistributedSchemaCodec.encodedSize(targetSchema);
            if (size>MAX_DEFINITION_BYTES) throw new IllegalArgumentException("Schema change definition exceeds command budget");
        }
        public UUID changeId() { return changeId; }
        public OperationId beginOperation() { return beginOperation; }
        public Kind kind() { return kind; }
        public RangeDirectory sourceDirectory() { return sourceDirectory; }
        public RangeDirectory targetDirectory() { return targetDirectory; }
        public DistributedSchema sourceSchema() { return sourceSchema; }
        public DistributedSchema targetSchema() { return targetSchema; }
        public byte[] encode() {
            byte[] directory=RangeDirectoryCodec.encode(sourceDirectory), source=DistributedSchemaCodec.encode(sourceSchema), target=DistributedSchemaCodec.encode(targetSchema);
            var out=ByteBuffer.allocate(64+directory.length+source.length+target.length).putInt(DEFINITION_MAGIC);
            uuid(out,changeId); uuid(out,beginOperation.sessionId()); out.putLong(beginOperation.epoch()).putLong(beginOperation.sequence());
            blob(out,directory); blob(out,source); blob(out,target); return out.array();
        }
        public byte[] digest() { return hash(encode()); }
        public Header header() { return new Header(this); }
        public static Definition decode(byte[] bytes) {
            var in=input(bytes,MAX_DEFINITION_BYTES,64,DEFINITION_MAGIC);
            UUID change=uuid(in); var operation=new OperationId(uuid(in),in.getLong(),in.getLong());
            var directory=RangeDirectoryCodec.decode(blob(in)); var source=DistributedSchemaCodec.decode(blob(in)); var target=DistributedSchemaCodec.decode(blob(in));
            end(in); var result=new Definition(change,operation,directory,source,target); canonical(bytes,result.encode()); return result;
        }
    }

    public static final class Header {
        private final UUID changeId,clusterId,databaseId,domainId;
        private final byte[] definitionDigest,sourceDirectoryDigest,targetDirectoryDigest,sourceSchemaDigest,targetSchemaDigest;
        private final long sourceGeneration,targetGeneration,sourceSchemaEpoch,targetSchemaEpoch;
        private final Kind kind;
        private final UUID changedColumnId;
        private final long changedApiId,sourceMaximumApiId,targetMaximumApiId;
        private final List<Participant> participants;
        private Header(Definition definition) {
            this(definition.changeId(),definition.sourceSchema.clusterId(),definition.sourceSchema.databaseId(),definition.sourceSchema.domainId(),
                    definition.digest(),definition.sourceDirectory.payloadDigest(),definition.targetDirectory.payloadDigest(),
                    definition.sourceSchema.digest(),definition.targetSchema.digest(),definition.sourceDirectory.generation(),definition.targetDirectory.generation(),
                    definition.sourceSchema.schemaEpoch(),definition.targetSchema.schemaEpoch(),definition.kind,
                    changedColumn(definition).columnId(),changedColumn(definition).apiId(),maximumApiId(definition.sourceSchema),maximumApiId(definition.targetSchema),
                    definition.sourceDirectory.ranges().stream().map(r -> new Participant(r.rangeId(),r.groupId(),r.rangeEpoch(),r.configurationVersion())).toList());
        }
        private static long maximumApiId(DistributedSchema schema) { return schema.columns().stream().mapToLong(DistributedSchema.Column::apiId).max().orElse(0); }
        private static DistributedSchema.Column changedColumn(Definition definition) {
            var changed=definition.kind==Kind.CREATE_COLUMN ? definition.targetSchema : definition.sourceSchema;
            var other=definition.kind==Kind.CREATE_COLUMN ? definition.sourceSchema : definition.targetSchema;
            return changed.columns().stream().filter(c -> !other.columnIds().contains(c.columnId())).findFirst().orElseThrow();
        }
        private Header(UUID changeId,UUID clusterId,UUID databaseId,UUID domainId,byte[] definitionDigest,byte[] sourceDirectoryDigest,
                       byte[] targetDirectoryDigest,byte[] sourceSchemaDigest,byte[] targetSchemaDigest,long sourceGeneration,long targetGeneration,
                       long sourceSchemaEpoch,long targetSchemaEpoch,Kind kind,UUID changedColumnId,long changedApiId,long sourceMaximumApiId,long targetMaximumApiId,List<Participant> participants) {
            this.changeId=changeId; this.clusterId=clusterId; this.databaseId=databaseId; this.domainId=domainId;
            this.definitionDigest=definitionDigest.clone(); this.sourceDirectoryDigest=sourceDirectoryDigest.clone(); this.targetDirectoryDigest=targetDirectoryDigest.clone();
            this.sourceSchemaDigest=sourceSchemaDigest.clone(); this.targetSchemaDigest=targetSchemaDigest.clone();
            this.sourceGeneration=sourceGeneration; this.targetGeneration=targetGeneration; this.sourceSchemaEpoch=sourceSchemaEpoch; this.targetSchemaEpoch=targetSchemaEpoch; this.kind=kind;
            this.changedColumnId=Objects.requireNonNull(changedColumnId); this.changedApiId=changedApiId;
            this.sourceMaximumApiId=sourceMaximumApiId; this.targetMaximumApiId=targetMaximumApiId;
            if (changedApiId<=0 || sourceMaximumApiId<0 || targetMaximumApiId<0
                    || kind==Kind.CREATE_COLUMN && (changedApiId<=sourceMaximumApiId || targetMaximumApiId!=changedApiId)
                    || kind==Kind.DELETE_COLUMN && (changedApiId>sourceMaximumApiId || targetMaximumApiId>sourceMaximumApiId
                            || changedApiId==sourceMaximumApiId && targetMaximumApiId>=sourceMaximumApiId
                            || changedApiId<sourceMaximumApiId && targetMaximumApiId!=sourceMaximumApiId)) throw new IllegalArgumentException("Invalid schema change API ID bounds");
            if (sourceGeneration<0 || sourceGeneration==Long.MAX_VALUE || targetGeneration!=sourceGeneration+1 || sourceSchemaEpoch<0
                    || sourceSchemaEpoch==Long.MAX_VALUE || targetSchemaEpoch!=sourceSchemaEpoch+1 || participants.isEmpty() || participants.size()>MAX_PARTICIPANTS) {
                throw new IllegalArgumentException("Invalid schema change header bounds");
            }
            this.participants=participants.stream().sorted(Comparator.comparing(Participant::groupId,RangeDirectoryCodec.UUID_ORDER)).toList();
            var groups=new HashSet<UUID>(); var ranges=new HashSet<UUID>();
            for (var participant:this.participants) if (!groups.add(participant.groupId()) || !ranges.add(participant.rangeId())) throw new IllegalArgumentException("Duplicate participant");
        }
        public UUID changeId() { return changeId; }
        public UUID clusterId() { return clusterId; }
        public UUID databaseId() { return databaseId; }
        public UUID domainId() { return domainId; }
        public Kind kind() { return kind; }
        public byte[] definitionDigest() { return definitionDigest.clone(); }
        public byte[] sourceDirectoryDigest() { return sourceDirectoryDigest.clone(); }
        public byte[] targetDirectoryDigest() { return targetDirectoryDigest.clone(); }
        public byte[] sourceSchemaDigest() { return sourceSchemaDigest.clone(); }
        public byte[] targetSchemaDigest() { return targetSchemaDigest.clone(); }
        public long sourceGeneration() { return sourceGeneration; }
        public long targetGeneration() { return targetGeneration; }
        public long sourceSchemaEpoch() { return sourceSchemaEpoch; }
        public long targetSchemaEpoch() { return targetSchemaEpoch; }
        public UUID changedColumnId() { return changedColumnId; }
        public long changedApiId() { return changedApiId; }
        public long sourceMaximumApiId() { return sourceMaximumApiId; }
        public long targetMaximumApiId() { return targetMaximumApiId; }
        public List<Participant> participants() { return participants; }
        public byte[] digest() { return hash(encode()); }
        public byte[] encode() {
            var out=ByteBuffer.allocate(305+48*participants.size()).putInt(HEADER_MAGIC);
            uuid(out,changeId); uuid(out,clusterId); uuid(out,databaseId); uuid(out,domainId);
            out.put(definitionDigest).put(sourceDirectoryDigest).put(targetDirectoryDigest).put(sourceSchemaDigest).put(targetSchemaDigest);
            out.putLong(sourceGeneration).putLong(targetGeneration).putLong(sourceSchemaEpoch).putLong(targetSchemaEpoch).put((byte)(kind.ordinal()+1)).putInt(participants.size());
            uuid(out,changedColumnId); out.putLong(changedApiId).putLong(sourceMaximumApiId).putLong(targetMaximumApiId);
            for (var p:participants) { uuid(out,p.rangeId()); uuid(out,p.groupId()); out.putLong(p.rangeEpoch()).putLong(p.configurationVersion()); }
            return out.array();
        }
        public static Header decode(byte[] bytes) {
            var in=input(bytes,MAX_STATUS_BYTES,305,HEADER_MAGIC); UUID change=uuid(in),cluster=uuid(in),database=uuid(in),domain=uuid(in);
            byte[] definition=SchemaChange.digest(in),sourceDirectory=SchemaChange.digest(in),targetDirectory=SchemaChange.digest(in),sourceSchema=SchemaChange.digest(in),targetSchema=SchemaChange.digest(in);
            long sourceGeneration=in.getLong(),targetGeneration=in.getLong(),sourceEpoch=in.getLong(),targetEpoch=in.getLong();
            Kind kind=wire(Kind.values(),in.get()); int count=in.getInt();
            UUID changedColumnId=uuid(in); long changedApiId=in.getLong(),sourceMaximumApiId=in.getLong(),targetMaximumApiId=in.getLong();
            if (count<1 || count>MAX_PARTICIPANTS || in.remaining()!=48*count) throw new IllegalArgumentException("Invalid participant count");
            var participants=new ArrayList<Participant>();
            for (int i=0;i<count;i++) participants.add(new Participant(uuid(in),uuid(in),in.getLong(),in.getLong()));
            var result=new Header(change,cluster,database,domain,definition,sourceDirectory,targetDirectory,sourceSchema,targetSchema,sourceGeneration,targetGeneration,sourceEpoch,targetEpoch,kind,changedColumnId,changedApiId,sourceMaximumApiId,targetMaximumApiId,participants);
            canonical(bytes,result.encode()); return result;
        }
    }

    public static final class Status {
        private final Header header;
        private final Phase phase;
        private final byte[] admissionDigest;
        private final Map<UUID,ApplyPosition> fenced,installed,activated;
        private Status(Header header,Phase phase,Map<UUID,ApplyPosition> fenced,Map<UUID,ApplyPosition> installed,Map<UUID,ApplyPosition> activated,byte[] admissionDigest) {
            this.header=Objects.requireNonNull(header); this.phase=Objects.requireNonNull(phase); this.fenced=Map.copyOf(fenced); this.installed=Map.copyOf(installed); this.activated=Map.copyOf(activated);
            boolean admitted=phase!=Phase.PREPARED && phase!=Phase.ABORTED;
            if(admitted!=(admissionDigest!=null) || admissionDigest!=null && admissionDigest.length!=32) throw new IllegalArgumentException("Schema phase requires exact admission commitment");
            this.admissionDigest=admissionDigest==null ? null : admissionDigest.clone();
            for (var group:this.fenced.keySet()) participant(group);
            for (var entry:this.installed.entrySet()) {
                participant(entry.getKey()); var fence=this.fenced.get(entry.getKey());
                if (fence==null || !follows(entry.getValue(),fence)) throw new IllegalArgumentException("Install does not follow fence");
            }
            for (var entry:this.activated.entrySet()) {
                participant(entry.getKey()); var install=this.installed.get(entry.getKey());
                if (install==null || !follows(entry.getValue(),install)) throw new IllegalArgumentException("Activation does not follow install");
            }
            int count=header.participants.size(); boolean allFenced=fenced.size()==count,allInstalled=installed.size()==count;
            if (!installed.isEmpty() && !allFenced || phase==Phase.PREPARED && (!fenced.isEmpty() || !installed.isEmpty())
                    || phase==Phase.FENCING && (allFenced || !installed.isEmpty())
                    || phase==Phase.FENCED && (!allFenced || allInstalled)
                    || (phase==Phase.INSTALLED || phase==Phase.PUBLISHED || phase==Phase.RETIRED) && !allInstalled
                    || phase==Phase.ABORTED && (!fenced.isEmpty() || !installed.isEmpty())
                    || !activated.isEmpty() && phase!=Phase.PUBLISHED && phase!=Phase.RETIRED
                    || phase==Phase.RETIRED && activated.size()!=count) throw new IllegalArgumentException("Proofs differ from schema change phase");
        }
        public static Status begin(Header header) { return new Status(header,Phase.PREPARED,Map.of(),Map.of(),Map.of(),null); }
        public Header header() { return header; }
        public Phase phase() { return phase; }
        public byte[] admissionDigest() { return admissionDigest==null ? null : admissionDigest.clone(); }
        public Map<UUID,ApplyPosition> fenced() { return fenced; }
        public Map<UUID,ApplyPosition> installed() { return installed; }
        public Map<UUID,ApplyPosition> activated() { return activated; }
        private void participant(UUID group) {
            if (header.participants.stream().noneMatch(p -> p.groupId().equals(group))) throw new IllegalArgumentException("Group is outside original schema change membership");
        }
        public Status fenced(UUID group,ApplyPosition position) {
            participant(group); Objects.requireNonNull(position); var prior=fenced.get(group);
            if (prior!=null) { if (!prior.equals(position)) throw new IllegalArgumentException("Conflicting fence proof"); return this; }
            if (phase!=Phase.FENCING) throw new IllegalStateException("Fence requires irrevocable fencing intent");
            var next=new HashMap<>(fenced); next.put(group,position);
            return new Status(header,next.size()==header.participants.size() ? Phase.FENCED : Phase.FENCING,next,installed,activated,admissionDigest);
        }
        public Status installed(UUID group,ApplyPosition position) {
            participant(group); Objects.requireNonNull(position); var prior=installed.get(group);
            if (prior!=null) { if (!prior.equals(position)) throw new IllegalArgumentException("Conflicting install proof"); return this; }
            if (phase!=Phase.FENCED) throw new IllegalStateException("Install requires every participant fenced");
            var next=new HashMap<>(installed); next.put(group,position);
            return new Status(header,next.size()==header.participants.size() ? Phase.INSTALLED : Phase.FENCED,fenced,next,activated,admissionDigest);
        }
        public Status activated(UUID group,ApplyPosition position) {
            participant(group); Objects.requireNonNull(position); var prior=activated.get(group);
            if (prior!=null) { if (!prior.equals(position)) throw new IllegalArgumentException("Conflicting activation proof"); return this; }
            if (phase!=Phase.PUBLISHED) throw new IllegalStateException("Activation requires published target directory");
            var next=new HashMap<>(activated); next.put(group,position);
            return new Status(header,Phase.PUBLISHED,fenced,installed,next,admissionDigest);
        }
        /** Catalogue CAS must choose this irrevocable path before any data-group fence is submitted. */
        public Status beginFencing(byte[] digest) {
            if(digest==null || digest.length!=32) throw new IllegalArgumentException("FENCING requires an admission digest");
            if(phase==Phase.FENCING) { if(!Arrays.equals(admissionDigest,digest)) throw new IllegalArgumentException("Original admission digest changed"); return this; }
            if(phase!=Phase.PREPARED) throw new IllegalStateException("FENCING requires prepared intent");
            return new Status(header,Phase.FENCING,fenced,installed,activated,digest);
        }
        public Status publish() { return advance(Phase.INSTALLED,Phase.PUBLISHED); }
        public Status retire() {
            if (activated.size()!=header.participants.size()) throw new IllegalStateException("Retirement requires every participant activated");
            return advance(Phase.PUBLISHED,Phase.RETIRED);
        }
        public Status abort() {
            if (phase==Phase.ABORTED) return this;
            if (phase!=Phase.PREPARED || !fenced.isEmpty()) throw new IllegalStateException("Abort requires prepared intent before fencing begins");
            return new Status(header,Phase.ABORTED,fenced,installed,activated,admissionDigest);
        }
        private Status advance(Phase from,Phase to) {
            if (phase==to) return this;
            if (phase!=from) throw new IllegalStateException("Invalid schema change phase transition");
            return new Status(header,to,fenced,installed,activated,admissionDigest);
        }
        /** Require one legal proof/phase advancement, or an identical retry. */
        public void requireSuccessorOf(Status previous) {
            try { requireLegalSuccessor(previous); }
            catch (IllegalStateException invalidPhase) { throw new IllegalArgumentException("Invalid schema change successor phase",invalidPhase); }
        }
        private void requireLegalSuccessor(Status previous) {
            if (!Arrays.equals(header.encode(),previous.header.encode())) throw new IllegalArgumentException("Schema change header differs");
            if(previous.admissionDigest!=null && !Arrays.equals(admissionDigest,previous.admissionDigest)) throw new IllegalArgumentException("Schema admission digest changed");
            if (Arrays.equals(encode(),previous.encode())) return;
            Status expected=null; int additions=0;
            for (var e:fenced.entrySet()) if (!previous.fenced.containsKey(e.getKey())) { expected=previous.fenced(e.getKey(),e.getValue()); additions++; }
            for (var e:installed.entrySet()) if (!previous.installed.containsKey(e.getKey())) { expected=previous.installed(e.getKey(),e.getValue()); additions++; }
            for (var e:activated.entrySet()) if (!previous.activated.containsKey(e.getKey())) { expected=previous.activated(e.getKey(),e.getValue()); additions++; }
            if (additions==0) expected=switch(phase) { case FENCING -> previous.beginFencing(admissionDigest); case PUBLISHED -> previous.publish(); case RETIRED -> previous.retire(); case ABORTED -> previous.abort(); default -> null; };
            if (additions>1 || expected==null || !Arrays.equals(encode(),expected.encode())) throw new IllegalArgumentException("Not a single monotone schema change successor");
        }
        public byte[] encode() {
            byte[] content=header.encode(); var out=ByteBuffer.allocate(22+(admissionDigest==null ? 0 : 32)+content.length+32*(fenced.size()+installed.size()+activated.size())).putInt(STATUS_MAGIC);
            blob(out,content); out.put((byte)(phase.ordinal()+1)).put((byte)(admissionDigest==null ? 0 : 1)); if(admissionDigest!=null) out.put(admissionDigest); proofs(out,fenced); proofs(out,installed); proofs(out,activated); return out.array();
        }
        public static Status decode(byte[] bytes) {
            var in=input(bytes,MAX_STATUS_BYTES,22,STATUS_MAGIC); var header=Header.decode(blob(in));
            if (in.remaining()<14) throw new IllegalArgumentException("Truncated schema change status");
            var phase=wire(Phase.values(),in.get()); int present=in.get()&255;
            if(present>1 || present==1 && in.remaining()<44) throw new IllegalArgumentException("Invalid admission digest presence/bounds");
            byte[] admission=present==1 ? digest(in) : null; var fenced=proofs(in); var installed=proofs(in); var activated=proofs(in); end(in);
            var result=new Status(header,phase,fenced,installed,activated,admission); canonical(bytes,result.encode()); return result;
        }
        private static void proofs(ByteBuffer out,Map<UUID,ApplyPosition> proofs) {
            out.putInt(proofs.size()); for (var group:proofs.keySet().stream().sorted(RangeDirectoryCodec.UUID_ORDER).toList()) {
                uuid(out,group); out.putLong(proofs.get(group).term()).putLong(proofs.get(group).index());
            }
        }
        private static Map<UUID,ApplyPosition> proofs(ByteBuffer in) {
            if (in.remaining()<4) throw new IllegalArgumentException("Truncated proof count"); int count=in.getInt();
            if (count<0 || count>MAX_PARTICIPANTS || 32L*count>in.remaining()) throw new IllegalArgumentException("Invalid proof count");
            var result=new HashMap<UUID,ApplyPosition>();
            for (int i=0;i<count;i++) if (result.put(uuid(in),new ApplyPosition(in.getLong(),in.getLong()))!=null) throw new IllegalArgumentException("Duplicate proof");
            return result;
        }
    }
    private static boolean follows(ApplyPosition next,ApplyPosition previous) {
        return next.term()>=previous.term() && next.index()>previous.index();
    }
    private static ByteBuffer input(byte[] bytes,int maximum,int minimum,int magic) {
        Objects.requireNonNull(bytes); if (bytes.length<minimum || bytes.length>maximum) throw new IllegalArgumentException("Invalid schema change byte size");
        var in=ByteBuffer.wrap(bytes); if (in.getInt()!=magic) throw new IllegalArgumentException("Unknown schema change encoding"); return in;
    }
    private static void canonical(byte[] bytes,byte[] encoded) { if (!Arrays.equals(bytes,encoded)) throw new IllegalArgumentException("Noncanonical schema change encoding"); }
    private static void end(ByteBuffer in) { if (in.hasRemaining()) throw new IllegalArgumentException("Trailing schema change bytes"); }
    private static <T> T wire(T[] values,byte wire) { int index=(wire & 255)-1; if (index<0 || index>=values.length) throw new IllegalArgumentException("Unknown schema change kind/phase"); return values[index]; }
    private static void uuid(ByteBuffer out,UUID value) { out.putLong(value.getMostSignificantBits()).putLong(value.getLeastSignificantBits()); }
    private static UUID uuid(ByteBuffer in) { return new UUID(in.getLong(),in.getLong()); }
    private static byte[] digest(ByteBuffer in) { byte[] bytes=new byte[32]; in.get(bytes); return bytes; }
    private static void blob(ByteBuffer out,byte[] bytes) { out.putInt(bytes.length).put(bytes); }
    private static byte[] blob(ByteBuffer in) {
        if (in.remaining()<4) throw new IllegalArgumentException("Truncated schema change blob"); int length=in.getInt();
        if (length<0 || length>in.remaining()) throw new IllegalArgumentException("Invalid schema change blob length");
        byte[] bytes=new byte[length]; in.get(bytes); return bytes;
    }
    private static byte[] hash(byte[] bytes) {
        try { return MessageDigest.getInstance("SHA-256").digest(bytes); } catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
}
