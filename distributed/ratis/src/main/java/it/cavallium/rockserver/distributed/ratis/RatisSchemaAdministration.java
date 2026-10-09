package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Consumer;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/** Replicated administrative intent and publication CAS. Native fencing and installation remain separate duties. */
public final class RatisSchemaAdministration {
    public static final int MAX_HEAD_BYTES=SchemaChange.MAX_STATUS_BYTES+12;
    private static final byte DEFINITION=0x43,HEAD=0x41,OUTCOME=0x4f,ADMISSION=0x4d;
    @FunctionalInterface public interface ImmutableReader { byte[] read(byte[] key); }
    private final RaftClient client;
    private final UUID cluster,database,group,column;
    private final long epoch;
    private final byte[] schema;
    private final Consumer<DistributedCommand> validator;

    /** Domain-local consumed issuance history; the catalogue allocation counter owns global allocation. */
    public record Head(SchemaChange.Status status,long allocatedThrough) {
        public Head {
            Objects.requireNonNull(status);
            if (allocatedThrough<Math.max(status.header().sourceMaximumApiId(),status.header().targetMaximumApiId()))
                throw new IllegalArgumentException("Schema column high-water mark regressed");
        }
        public boolean terminal() { return status.phase()==SchemaChange.Phase.RETIRED || status.phase()==SchemaChange.Phase.ABORTED; }
        public byte[] encode() { byte[] value=status.encode(); return ByteBuffer.allocate(12+value.length).putLong(allocatedThrough).putInt(value.length).put(value).array(); }
        public static Head decode(byte[] value) {
            if (value==null || value.length<12 || value.length>MAX_HEAD_BYTES) throw new IllegalArgumentException("Invalid schema head length");
            var in=ByteBuffer.wrap(value); long allocated=in.getLong(); int length=in.getInt();
            if (length!=in.remaining()) throw new IllegalArgumentException("Invalid schema head status length");
            byte[] status=new byte[length]; in.get(status); return new Head(SchemaChange.Status.decode(status),allocated);
        }
    }

    public RatisSchemaAdministration(RaftClient client,UUID cluster,UUID database,UUID group,long epoch,byte[] schema,UUID column) {
        this.client=Objects.requireNonNull(client); this.cluster=Objects.requireNonNull(cluster); this.database=Objects.requireNonNull(database);
        this.group=Objects.requireNonNull(group); this.column=Objects.requireNonNull(column);
        if (epoch<0 || schema.length!=32 || !client.getGroupId().getUuid().equals(group)) throw new IllegalArgumentException("Invalid schema administration binding");
        this.epoch=epoch; this.schema=schema.clone(); validator=structuralValidator(cluster,database,group,epoch,schema,column);
    }

    public SchemaChange.Definition readDefinition(byte[] digest) throws IOException {
        byte[] value=read(definitionKey(digest),SchemaChange.MAX_DEFINITION_BYTES);
        if (value==null) return null;
        try { var definition=SchemaChange.Definition.decode(value); requireDefinition(definition,cluster,database,definitionKey(digest)); return definition; }
        catch (IllegalArgumentException invalid) { throw new IOException("Invalid schema change definition",invalid); }
    }
    public Head readHead(UUID domain) throws IOException { return readHead(headKey(domain),domain,null); }
    public Head readOutcome(UUID domain,byte[] definitionDigest) throws IOException { return readHead(outcomeKey(definitionDigest),domain,definitionDigest); }
    private Head readHead(byte[] key,UUID domain,byte[] digest) throws IOException {
        byte[] value=read(key,MAX_HEAD_BYTES); if (value==null) return null;
        try {
            var head=Head.decode(value); requireHeader(head.status().header(),cluster,database,domain);
            if (digest!=null && (!head.terminal() || !Arrays.equals(digest,head.status().header().definitionDigest()))) throw new IllegalArgumentException("Wrong terminal schema outcome");
            return head;
        } catch (IllegalArgumentException invalid) { throw new IOException("Invalid schema change head",invalid); }
    }
    /** An identity marker is retained even after deletion, independent of outcome/receipt retention. */
    public byte[] readAllocationCounter() throws IOException { byte[] value=read(RatisColumnAllocation.counterKey(),8); RatisColumnAllocation.counter(value); return value; }
    public byte[] readColumnIdentity(UUID id) throws IOException { return read(RatisColumnIdentities.columnKey(id),72); }
    public byte[] readApiIdentity(long id) throws IOException { return read(RatisColumnIdentities.apiKey(id),72); }
    private byte[] read(byte[] key,int maximum) throws IOException {
        try { return ScopedTransactionRpc.pointValue(content(RatisRoutedClient.rpc(() -> client.io().sendReadOnly(message(ScopedTransactionRpc.pointRequest(column,key)))),maximum+20),maximum); }
        catch (IllegalArgumentException invalid) { throw new IOException("Invalid administrative point reply",invalid); }
    }

    /** Check the complete begin budget before publishing this immutable definition. */
    public DistributedCommand prepareDefinition(OperationId operation,SchemaChange.Definition definition,Head previous,byte[] previousColumn,byte[] previousApi) {
        prepareBegin(definition,previous,previousColumn,previousApi);
        byte[] key=definitionKey(definition.digest());
        return checked(operation,List.of(new Mutation(Mutation.Kind.PUT,column,key,definition.encode())),List.of(new KeyPredicate(column,key,null)));
    }
    public DistributedCommand prepareBegin(SchemaChange.Definition definition,Head previous,byte[] previousColumn,byte[] previousApi) {
        var header=definition.header();
        long allocated=Math.max(previous==null ? 0 : previous.allocatedThrough(),Math.max(header.sourceMaximumApiId(),header.targetMaximumApiId()));
        var next=new Head(SchemaChange.Status.begin(header),allocated); byte[] key=headKey(header.domainId());
        byte[] issued=header.kind()==SchemaChange.Kind.CREATE_COLUMN ? header.targetSchemaDigest() : header.sourceSchemaDigest();
        byte[] expected=RatisColumnIdentities.value(header.domainId(),header.changedColumnId(),header.changedApiId(),issued);
        return checked(definition.beginOperation(),List.of(new Mutation(Mutation.Kind.PUT,column,key,next.encode())),
                List.of(new KeyPredicate(column,key,previous==null ? null : previous.encode()),
                        new KeyPredicate(column,directoryKey(header.domainId()),RangeDirectoryCodec.encode(definition.sourceDirectory())),
                        new KeyPredicate(column,RatisColumnIdentities.columnKey(header.changedColumnId()),previousColumn==null ? expected : previousColumn),
                        new KeyPredicate(column,RatisColumnIdentities.apiKey(header.changedApiId()),previousApi==null ? expected : previousApi)));
    }
    public SchemaAdmissionManifest readAdmissions(SchemaChange.Definition definition,byte[] digest) throws IOException {
        byte[] value=read(admissionKey(digest),SchemaAdmissionManifest.MAX_BYTES); if (value==null) return null;
        try { var manifest=SchemaAdmissionManifest.decode(value,definition); if (!Arrays.equals(manifest.digest(),digest)) throw new IllegalArgumentException("Admission digest changed"); return manifest; }
        catch (IllegalArgumentException invalid) { throw new IOException("Invalid immutable admissions",invalid); }
    }
    public DistributedCommand prepareAdmissions(OperationId operation,Head prepared,SchemaChange.Definition definition,SchemaAdmissionManifest manifest) {
        manifest.requireDefinition(definition);
        if (prepared.status().phase()!=SchemaChange.Phase.PREPARED || !sameHeader(prepared.status().header(),definition.header())) throw new IllegalArgumentException("Admissions require original PREPARED head");
        byte[] key=admissionKey(manifest.digest());
        return checked(operation,List.of(new Mutation(Mutation.Kind.PUT,column,key,manifest.encode())),
                List.of(new KeyPredicate(column,headKey(definition.header().domainId()),prepared.encode()),new KeyPredicate(column,key,null)));
    }
    public DistributedCommand prepareFencing(OperationId operation,Head prepared,SchemaChange.Definition definition,SchemaAdmissionManifest manifest) {
        manifest.requireDefinition(definition);
        if (prepared.status().phase()!=SchemaChange.Phase.PREPARED || !sameHeader(prepared.status().header(),definition.header())) throw new IllegalArgumentException("Fencing requires original PREPARED head");
        var next=new Head(prepared.status().beginFencing(manifest.digest()),prepared.allocatedThrough());
        byte[] key=headKey(definition.header().domainId());
        return checked(operation,List.of(new Mutation(Mutation.Kind.PUT,column,key,next.encode())),
                List.of(new KeyPredicate(column,key,prepared.encode()),new KeyPredicate(column,directoryKey(definition.header().domainId()),RangeDirectoryCodec.encode(definition.sourceDirectory()))));
    }
    public CommandResult executeFencing(DistributedCommand command) throws IOException {
        validator.accept(command); if (!isFencing(command)) throw new IllegalArgumentException("Expected exact fencing transition");
        byte[] encoded=DistributedCommandCodec.encode(command);
        RatisSchemaFencingRpc.decodePreparedReply(content(RatisRoutedClient.rpc(() -> client.io().sendReadOnly(message(RatisSchemaFencingRpc.prepareRequest(command)))),RatisSchemaFencingRpc.MAX_BYTES),ScopedTransactionRpc.SCHEMA_FENCING_PREPARE,command);
        return executePrepared(command,encoded);
    }
    public void cancelFencing(DistributedCommand command) throws IOException {
        validator.accept(command); if (!isFencing(command)) throw new IllegalArgumentException("Expected exact fencing transition");
        RatisSchemaFencingRpc.decodePreparedReply(content(RatisRoutedClient.rpc(() -> client.io().sendReadOnly(message(RatisSchemaFencingRpc.cancelRequest(command)))),RatisSchemaFencingRpc.MAX_BYTES),ScopedTransactionRpc.SCHEMA_FENCING_CANCEL,command);
    }
    public DistributedCommand prepareAdvance(OperationId operation,Head previous,SchemaChange.Status next) {
        if (previous.status().phase()==SchemaChange.Phase.PREPARED && next.phase()==SchemaChange.Phase.FENCING) throw new IllegalArgumentException("Fencing requires immutable admission preparation");
        if (next.phase()==SchemaChange.Phase.PUBLISHED && previous.status().phase()!=SchemaChange.Phase.PUBLISHED) throw new IllegalArgumentException("Schema publication must atomically replace the directory");
        var head=new Head(next,previous.allocatedThrough()); byte[] key=headKey(next.header().domainId());
        if (head.terminal()) return checked(operation,List.of(new Mutation(Mutation.Kind.PUT,column,key,head.encode()),
                        new Mutation(Mutation.Kind.PUT,column,outcomeKey(next.header().definitionDigest()),head.encode())),
                List.of(new KeyPredicate(column,key,previous.encode()),new KeyPredicate(column,outcomeKey(next.header().definitionDigest()),null)));
        return checked(operation,List.of(new Mutation(Mutation.Kind.PUT,column,key,head.encode())),List.of(new KeyPredicate(column,key,previous.encode())));
    }
    public DistributedCommand preparePublication(OperationId operation,Head previous,SchemaChange.Definition definition) {
        if (!sameHeader(previous.status().header(),definition.header())) throw new IllegalArgumentException("Publication definition changed");
        var next=new Head(previous.status().publish(),previous.allocatedThrough()); UUID domain=definition.header().domainId();
        return checked(operation,List.of(new Mutation(Mutation.Kind.PUT,column,headKey(domain),next.encode()),
                        new Mutation(Mutation.Kind.PUT,column,directoryKey(domain),RangeDirectoryCodec.encode(definition.targetDirectory()))),
                List.of(new KeyPredicate(column,headKey(domain),previous.encode()),new KeyPredicate(column,directoryKey(domain),RangeDirectoryCodec.encode(definition.sourceDirectory()))));
    }
    private DistributedCommand checked(OperationId operation,List<Mutation> mutations,List<KeyPredicate> predicates) {
        var command=new DistributedCommand(group,epoch,schema,operation,mutations,predicates); validator.accept(command);
        DistributedCommandCodec.encode(command); return command;
    }
    public CommandResult execute(DistributedCommand command) throws IOException {
        validator.accept(command); if (isFencing(command)) throw new IllegalArgumentException("Fencing requires fresh catalogue preparation");
        return executePrepared(command,DistributedCommandCodec.encode(command));
    }
    private CommandResult executePrepared(DistributedCommand command,byte[] encoded) throws IOException {
        try {
            var result=CommandResult.decode(content(RatisRoutedClient.rpc(() -> client.io().send(message(encoded))),CommandResult.MAX_BYTES));
            if (result.valueCount()!=0 || result.status()==CommandResult.Status.RESULT_TOO_LARGE) throw new IOException("Unexpected schema administration result");
            return result;
        } catch (IllegalArgumentException invalid) { throw new IOException("Invalid schema administration reply",invalid); }
    }

    public static boolean isCommand(DistributedCommand command) {
        if (command.mutations().isEmpty()) return false;
        byte[] key=command.mutations().getFirst().key(); return key.length>0 && (key[0]==HEAD || key[0]==DEFINITION || key[0]==ADMISSION) && key.length!=16;
    }
    public static boolean isFencing(DistributedCommand command) {
        if (command.mutations().size()!=1 || command.predicates().isEmpty() || command.mutations().getFirst().key().length!=17 || command.mutations().getFirst().key()[0]!=HEAD) return false;
        var next=Head.decode(command.mutations().getFirst().value()); byte[] old=command.predicates().getFirst().expectedValue();
        return old!=null && Head.decode(old).status().phase()==SchemaChange.Phase.PREPARED && next.status().phase()==SchemaChange.Phase.FENCING;
    }
    public static Consumer<DistributedCommand> validator(UUID cluster,UUID database,UUID group,long epoch,byte[] schema,UUID column) {
        return validator(cluster,database,group,epoch,schema,column,null,false);
    }
    public static Consumer<DistributedCommand> validator(UUID cluster,UUID database,UUID group,long epoch,byte[] schema,UUID column,ImmutableReader reader) {
        return validator(cluster,database,group,epoch,schema,column,Objects.requireNonNull(reader),false);
    }
    static Consumer<DistributedCommand> structuralValidator(UUID cluster,UUID database,UUID group,long epoch,byte[] schema,UUID column) {
        return validator(cluster,database,group,epoch,schema,column,null,true);
    }
    private static Consumer<DistributedCommand> validator(UUID cluster,UUID database,UUID group,long epoch,byte[] schema,UUID column,ImmutableReader reader,boolean structural) {
        byte[] expected=Objects.requireNonNull(schema).clone();
        return command -> {
            if (command.hasBucketData() || command.acceptance()!=null || command.transactionRejection() || command.kind()!=DistributedCommand.Kind.DATA || !command.groupId().equals(group) || command.rangeEpoch()!=epoch
                    || !Arrays.equals(command.schemaDigest(),expected) || command.acknowledgedThrough()!=-1 || command.minimumSessionEpoch()!=-1
                    || command.mutations().isEmpty() || command.mutations().stream().anyMatch(m -> m.kind()!=Mutation.Kind.PUT || m.returnMode()!=Mutation.ReturnMode.NONE || !m.columnId().equals(column))
                    || command.predicates().stream().anyMatch(p -> !p.columnId().equals(column))) throw new IllegalArgumentException("Invalid schema administration envelope");
            var first=command.mutations().getFirst(); byte[] key=first.key();
            if (key.length==33 && key[0]==DEFINITION) {
                if (command.mutations().size()!=1 || command.predicates().size()!=1 || !Arrays.equals(key,command.predicates().getFirst().key()) || command.predicates().getFirst().expectedValue()!=null) throw new IllegalArgumentException("Definition requires immutable absent-content CAS");
                requireDefinition(SchemaChange.Definition.decode(first.value()),cluster,database,key); return;
            }
            if (key.length==33 && key[0]==ADMISSION) {
                if (command.mutations().size()!=1 || command.predicates().size()!=2 || !Arrays.equals(key,command.predicates().get(1).key()) || command.predicates().get(1).expectedValue()!=null) throw new IllegalArgumentException("Admissions require immutable absent-content CAS");
                var header=manifestHeader(first.value()); requireHeader(header,cluster,database,header.domainId());
                var prepared=Head.decode(command.predicates().getFirst().expectedValue());
                if (prepared.status().phase()!=SchemaChange.Phase.PREPARED || !sameHeader(prepared.status().header(),header) || !Arrays.equals(command.predicates().getFirst().key(),headKey(header.domainId()))
                        || !Arrays.equals(key,admissionKey(OperatorUpload.sha256(first.value())))) throw new IllegalArgumentException("Admission publication differs from original PREPARED intent");
                if (!structural) {
                    var definition=committedDefinition(reader,header,cluster,database);
                    SchemaAdmissionManifest.decode(first.value(),definition);
                    byte[] directory=reader.read(directoryKey(header.domainId()));
                    if (directory==null || !Arrays.equals(RangeDirectoryCodec.decode(directory).payloadDigest(),header.sourceDirectoryDigest())) throw new IllegalArgumentException("Admission source directory changed");
                }
                return;
            }
            var next=Head.decode(first.value()); var header=next.status().header(); UUID domain=header.domainId(); requireHeader(header,cluster,database,domain);
            if (!Arrays.equals(key,headKey(domain)) || command.predicates().isEmpty() || !Arrays.equals(command.predicates().getFirst().key(),key)) throw new IllegalArgumentException("Invalid domain administration head");
            byte[] previousBytes=command.predicates().getFirst().expectedValue(); Head previous=previousBytes==null ? null : Head.decode(previousBytes);
            if (next.status().phase()==SchemaChange.Phase.PREPARED && next.status().fenced().isEmpty() && next.status().installed().isEmpty()
                    && (previous==null || !sameHeader(previous.status().header(),header))) validateBegin(command,next,previous,cluster,database,reader,structural);
            else {
                if (previous==null || next.allocatedThrough()!=previous.allocatedThrough()) throw new IllegalArgumentException("Schema progress changed allocation ownership");
                next.status().requireSuccessorOf(previous.status());
                if (previous.status().phase()==SchemaChange.Phase.PREPARED && next.status().phase()==SchemaChange.Phase.FENCING) validateFencing(command,next,previous,cluster,database,reader,structural);
                else if (next.status().phase()==SchemaChange.Phase.PUBLISHED && previous.status().phase()!=SchemaChange.Phase.PUBLISHED) validatePublication(command,next,previous,column);
                else if (next.terminal()) {
                    if (command.mutations().size()!=2 || command.predicates().size()!=2) throw new IllegalArgumentException("Terminal schema progress requires retained outcome");
                    requirePair(command,1,outcomeKey(header.definitionDigest()),next.encode(),null);
                } else if (command.mutations().size()!=1 || command.predicates().size()!=1) throw new IllegalArgumentException("Schema proof progress requires one head CAS");
            }
        };
    }
    private static SchemaChange.Header manifestHeader(byte[] bytes) {
        if (bytes==null || bytes.length<8 || bytes.length>SchemaAdmissionManifest.MAX_BYTES) throw new IllegalArgumentException("Invalid admission manifest budget");
        var in=ByteBuffer.wrap(bytes); in.getInt(); int length=in.getInt();
        if (length<1 || length>16*1024 || length>in.remaining()) throw new IllegalArgumentException("Invalid admission header");
        byte[] encoded=new byte[length]; in.get(encoded); return SchemaChange.Header.decode(encoded);
    }
    private static SchemaChange.Definition committedDefinition(ImmutableReader reader,SchemaChange.Header header,UUID cluster,UUID database) {
        if (reader==null) throw new IllegalArgumentException("Committed schema metadata reader is not configured");
        byte[] value=reader.read(definitionKey(header.definitionDigest()));
        if (value==null || value.length>SchemaChange.MAX_DEFINITION_BYTES) throw new IllegalArgumentException("Original committed schema definition missing");
        var definition=SchemaChange.Definition.decode(value); requireDefinition(definition,cluster,database,definitionKey(header.definitionDigest()));
        if (!sameHeader(definition.header(),header)) throw new IllegalArgumentException("Committed schema definition changed"); return definition;
    }
    private static void validateFencing(DistributedCommand command,Head next,Head previous,UUID cluster,UUID database,ImmutableReader reader,boolean structural) {
        var header=next.status().header();
        if (command.mutations().size()!=1 || command.predicates().size()!=2 || !Arrays.equals(command.predicates().get(1).key(),directoryKey(header.domainId()))) throw new IllegalArgumentException("Fencing requires exact source directory CAS");
        var source=RangeDirectoryCodec.decode(command.predicates().get(1).expectedValue());
        if (!Arrays.equals(source.payloadDigest(),header.sourceDirectoryDigest())) throw new IllegalArgumentException("Fencing source directory changed");
        if (!structural) {
            var definition=committedDefinition(reader,header,cluster,database);
            byte[] bytes=reader.read(admissionKey(next.status().admissionDigest()));
            if (bytes==null || !Arrays.equals(OperatorUpload.sha256(bytes),next.status().admissionDigest())) throw new IllegalArgumentException("Original committed admissions missing");
            SchemaAdmissionManifest.decode(bytes,definition);
        }
    }
    private static void validateBegin(DistributedCommand command,Head next,Head previous,UUID cluster,UUID database,ImmutableReader reader,boolean structural) {
        if (previous!=null && !previous.terminal() || command.mutations().size()!=1 || command.predicates().size()!=4) throw new IllegalArgumentException("Schema administration already active or invalid begin shape");
        var header=next.status().header(); var source=RangeDirectoryCodec.decode(command.predicates().get(1).expectedValue());
        if (!Arrays.equals(command.predicates().get(1).key(),directoryKey(header.domainId())) || !Arrays.equals(source.payloadDigest(),header.sourceDirectoryDigest())
                || !source.clusterId().equals(header.clusterId()) || !source.databaseId().equals(header.databaseId()) || !source.domainId().equals(header.domainId())) throw new IllegalArgumentException("Schema begin changed original source directory");
        if (!structural) {
            var definition=committedDefinition(reader,header,cluster,database);
            if (!command.operationId().equals(definition.beginOperation()) || !Arrays.equals(RangeDirectoryCodec.encode(source),RangeDirectoryCodec.encode(definition.sourceDirectory()))) throw new IllegalArgumentException("Schema begin changed original immutable definition");
            byte[] publishedSource=reader.read(RatisSchemaCatalogue.key(header.sourceSchemaDigest())),publishedTarget=reader.read(RatisSchemaCatalogue.key(header.targetSchemaDigest()));
            if (!Arrays.equals(publishedSource,DistributedSchemaCodec.encode(definition.sourceSchema())) || !Arrays.equals(publishedTarget,DistributedSchemaCodec.encode(definition.targetSchema()))) throw new IllegalArgumentException("Schema begin lacks original committed source/target schemas");
        }
        long prior=Math.max(previous==null ? 0 : previous.allocatedThrough(),header.sourceMaximumApiId());
        if (header.kind()==SchemaChange.Kind.CREATE_COLUMN && header.changedApiId()<=prior || next.allocatedThrough()!=Math.max(prior,header.targetMaximumApiId())) throw new IllegalArgumentException("Schema begin reuses or changes allocated column ID");
        byte[] issued=null;
        for (int i=0;i<2;i++) {
            byte[] key=i==0 ? RatisColumnIdentities.columnKey(header.changedColumnId()) : RatisColumnIdentities.apiKey(header.changedApiId());
            var predicate=command.predicates().get(i+2); byte[] old=predicate.expectedValue();
            if (!Arrays.equals(predicate.key(),key)) throw new IllegalArgumentException("Wrong column issuance key");
            byte[] first=RatisColumnIdentities.requireOwned(old,header.domainId(),header.changedColumnId(),header.changedApiId());
            if (issued!=null && !Arrays.equals(issued,first) || header.kind()==SchemaChange.Kind.CREATE_COLUMN && !Arrays.equals(first,header.targetSchemaDigest())) throw new IllegalArgumentException("Schema column incarnation was issued for a different definition");
            issued=first;
        }
        if (previous!=null) requireDirectoryAfterTerminal(source,previous);
    }
    private static void validatePublication(DistributedCommand command,Head next,Head previous,UUID column) {
        if (command.mutations().size()!=2 || command.predicates().size()!=2 || previous.status().phase()!=SchemaChange.Phase.INSTALLED) throw new IllegalArgumentException("Schema publication lacks installed head and directory CAS");
        var header=next.status().header(); var source=RangeDirectoryCodec.decode(command.predicates().get(1).expectedValue());
        var target=RangeDirectoryCodec.decode(command.mutations().get(1).value()); byte[] key=directoryKey(header.domainId());
        if (RangeDirectoryCodec.encodedSize(source)>RatisCatalogue.MAX_DIRECTORY_BYTES || RangeDirectoryCodec.encodedSize(target)>RatisCatalogue.MAX_DIRECTORY_BYTES
                || !Arrays.equals(command.predicates().get(1).key(),key) || !Arrays.equals(command.mutations().get(1).key(),key)
                || !source.clusterId().equals(header.clusterId()) || !source.databaseId().equals(header.databaseId()) || !source.domainId().equals(header.domainId())
                || !target.clusterId().equals(source.clusterId()) || !target.databaseId().equals(source.databaseId()) || !target.domainId().equals(source.domainId())
                || source.generation()!=header.sourceGeneration() || target.generation()!=header.targetGeneration()
                || source.schemaEpoch()!=header.sourceSchemaEpoch() || target.schemaEpoch()!=header.targetSchemaEpoch()
                || !Arrays.equals(source.schemaDigest(),header.sourceSchemaDigest()) || !Arrays.equals(target.schemaDigest(),header.targetSchemaDigest())
                || !Arrays.equals(source.payloadDigest(),header.sourceDirectoryDigest()) || !Arrays.equals(target.payloadDigest(),header.targetDirectoryDigest())
                || source.routingPrefixBytes()!=target.routingPrefixBytes() || !source.ranges().equals(target.ranges())) throw new IllegalArgumentException("Schema publication changed frozen directory identities");
    }
    private static void requirePair(DistributedCommand command,int index,byte[] key,byte[] value,byte[] expected) {
        if (!Arrays.equals(command.mutations().get(index).key(),key) || !Arrays.equals(command.mutations().get(index).value(),value)
                || !Arrays.equals(command.predicates().get(index).key(),key) || !Arrays.equals(command.predicates().get(index).expectedValue(),expected)) throw new IllegalArgumentException("Invalid retained schema outcome CAS");
    }
    /** Ordinary topology publication is held behind the domain's active schema administration. */
    public static void requireTopologyGuard(KeyPredicate guard,RangeDirectory expected) {
        if (!Arrays.equals(guard.key(),headKey(expected.domainId()))) throw new IllegalArgumentException("Missing schema administration guard");
        byte[] value=guard.expectedValue(); if (value==null) return;
        var head=Head.decode(value); requireDirectoryAfterTerminal(expected,head);
    }
    private static void requireDirectoryAfterTerminal(RangeDirectory directory,Head previous) {
        var header=previous.status().header();
        if (!previous.terminal() || !directory.clusterId().equals(header.clusterId()) || !directory.databaseId().equals(header.databaseId()) || !directory.domainId().equals(header.domainId())
                || directory.schemaEpoch()!=(previous.status().phase()==SchemaChange.Phase.ABORTED ? header.sourceSchemaEpoch() : header.targetSchemaEpoch())
                || !Arrays.equals(directory.schemaDigest(),previous.status().phase()==SchemaChange.Phase.ABORTED ? header.sourceSchemaDigest() : header.targetSchemaDigest())) throw new IllegalArgumentException("Active or foreign schema administration prevents directory publication");
    }
    private static void requireDefinition(SchemaChange.Definition definition,UUID cluster,UUID database,byte[] key) {
        if (RangeDirectoryCodec.encodedSize(definition.sourceDirectory())>RatisCatalogue.MAX_DIRECTORY_BYTES || RangeDirectoryCodec.encodedSize(definition.targetDirectory())>RatisCatalogue.MAX_DIRECTORY_BYTES
                || !definition.sourceSchema().clusterId().equals(cluster) || !definition.sourceSchema().databaseId().equals(database) || !Arrays.equals(key,definitionKey(definition.digest()))) throw new IllegalArgumentException("Schema definition identity/content mismatch");
    }
    private static void requireHeader(SchemaChange.Header header,UUID cluster,UUID database,UUID domain) {
        if (!header.clusterId().equals(cluster) || !header.databaseId().equals(database) || !header.domainId().equals(domain)) throw new IllegalArgumentException("Schema administrative header identity mismatch");
    }
    private static boolean sameHeader(SchemaChange.Header a,SchemaChange.Header b) { return Arrays.equals(a.encode(),b.encode()); }
    public static byte[] headKey(UUID domain) { return taggedUuid(HEAD,domain); }
    public static byte[] definitionKey(byte[] digest) { return taggedDigest(DEFINITION,digest); }
    public static byte[] admissionKey(byte[] digest) { return taggedDigest(ADMISSION,digest); }
    public static byte[] outcomeKey(byte[] digest) { return taggedDigest(OUTCOME,digest); }
    private static byte[] taggedUuid(byte tag,UUID id) { return ByteBuffer.allocate(17).put(tag).putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()).array(); }
    private static byte[] directoryKey(UUID id) { return ByteBuffer.allocate(16).putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()).array(); }
    private static byte[] taggedDigest(byte tag,byte[] digest) { if (digest==null || digest.length!=32) throw new IllegalArgumentException("Invalid schema definition digest"); return ByteBuffer.allocate(33).put(tag).put(digest).array(); }
    private static Message message(byte[] bytes) { return Message.valueOf(ByteString.copyFrom(bytes)); }
    private static byte[] content(RaftClientReply reply,int maximum) throws IOException {
        if (reply==null || !reply.isSuccess() || reply.getMessage()==null || reply.getMessage().getContent().size()>maximum) throw new IOException("Invalid schema administration reply",reply==null ? null : reply.getException());
        return reply.getMessage().getContent().toByteArray();
    }
}
