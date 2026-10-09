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
    private static final byte DEFINITION=0x43,HEAD=0x41,OUTCOME=0x4f;
    private final RaftClient client;
    private final UUID cluster,database,group,column;
    private final long epoch;
    private final byte[] schema;
    private final Consumer<DistributedCommand> validator;

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
        this.epoch=epoch; this.schema=schema.clone(); validator=validator(cluster,database,group,epoch,schema,column);
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
                        new KeyPredicate(column,definitionKey(header.definitionDigest()),definition.encode()),
                        new KeyPredicate(column,RatisColumnIdentities.columnKey(header.changedColumnId()),previousColumn==null ? expected : previousColumn),
                        new KeyPredicate(column,RatisColumnIdentities.apiKey(header.changedApiId()),previousApi==null ? expected : previousApi),
                        new KeyPredicate(column,RatisSchemaCatalogue.key(header.targetSchemaDigest()),DistributedSchemaCodec.encode(definition.targetSchema())),
                        new KeyPredicate(column,RatisSchemaCatalogue.key(header.sourceSchemaDigest()),DistributedSchemaCodec.encode(definition.sourceSchema()))));
    }
    public DistributedCommand prepareAdvance(OperationId operation,Head previous,SchemaChange.Status next) {
        if (next.phase()==SchemaChange.Phase.PUBLISHED) throw new IllegalArgumentException("Schema publication must atomically replace the directory");
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
        validator.accept(command);
        try {
            var result=CommandResult.decode(content(RatisRoutedClient.rpc(() -> client.io().send(message(DistributedCommandCodec.encode(command)))),CommandResult.MAX_BYTES));
            if (result.valueCount()!=0 || result.status()==CommandResult.Status.RESULT_TOO_LARGE) throw new IOException("Unexpected schema administration result");
            return result;
        } catch (IllegalArgumentException invalid) { throw new IOException("Invalid schema administration reply",invalid); }
    }

    public static boolean isCommand(DistributedCommand command) {
        if (command.mutations().isEmpty()) return false;
        byte[] key=command.mutations().getFirst().key(); return key.length>0 && (key[0]==HEAD || key[0]==DEFINITION) && key.length!=16;
    }
    public static Consumer<DistributedCommand> validator(UUID cluster,UUID database,UUID group,long epoch,byte[] schema,UUID column) {
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
            var next=Head.decode(first.value()); var header=next.status().header(); UUID domain=header.domainId(); requireHeader(header,cluster,database,domain);
            if (!Arrays.equals(key,headKey(domain)) || command.predicates().isEmpty() || !Arrays.equals(command.predicates().getFirst().key(),key)) throw new IllegalArgumentException("Invalid domain administration head");
            byte[] previousBytes=command.predicates().getFirst().expectedValue(); Head previous=previousBytes==null ? null : Head.decode(previousBytes);
            if (next.status().phase()==SchemaChange.Phase.PREPARED && next.status().fenced().isEmpty() && next.status().installed().isEmpty()
                    && (previous==null || !sameHeader(previous.status().header(),header))) validateBegin(command,next,previous,cluster,database,column);
            else {
                if (previous==null || next.allocatedThrough()!=previous.allocatedThrough()) throw new IllegalArgumentException("Schema progress changed allocation ownership");
                next.status().requireSuccessorOf(previous.status());
                if (next.status().phase()==SchemaChange.Phase.PUBLISHED) validatePublication(command,next,previous,column);
                else if (next.terminal()) {
                    if (command.mutations().size()!=2 || command.predicates().size()!=2) throw new IllegalArgumentException("Terminal schema progress requires retained outcome");
                    requirePair(command,1,outcomeKey(header.definitionDigest()),next.encode(),null);
                } else if (command.mutations().size()!=1 || command.predicates().size()!=1) throw new IllegalArgumentException("Schema proof progress requires one head CAS");
            }
        };
    }
    private static void validateBegin(DistributedCommand command,Head next,Head previous,UUID cluster,UUID database,UUID column) {
        if (previous!=null && !previous.terminal() || command.mutations().size()!=1 || command.predicates().size()!=7) throw new IllegalArgumentException("Schema administration already active or invalid begin shape");
        var header=next.status().header(); var source=RangeDirectoryCodec.decode(command.predicates().get(1).expectedValue());
        var definition=SchemaChange.Definition.decode(command.predicates().get(2).expectedValue());
        requireDefinition(definition,cluster,database,definitionKey(header.definitionDigest()));
        if (!sameHeader(header,definition.header()) || !command.operationId().equals(definition.beginOperation())
                || !Arrays.equals(command.predicates().get(1).key(),directoryKey(header.domainId()))
                || !Arrays.equals(command.predicates().get(2).key(),definitionKey(header.definitionDigest()))
                || !Arrays.equals(RangeDirectoryCodec.encode(source),RangeDirectoryCodec.encode(definition.sourceDirectory()))
                || !Arrays.equals(command.predicates().get(5).key(),RatisSchemaCatalogue.key(header.targetSchemaDigest()))
                || !Arrays.equals(command.predicates().get(5).expectedValue(),DistributedSchemaCodec.encode(definition.targetSchema()))
                || !Arrays.equals(command.predicates().get(6).key(),RatisSchemaCatalogue.key(header.sourceSchemaDigest()))
                || !Arrays.equals(command.predicates().get(6).expectedValue(),DistributedSchemaCodec.encode(definition.sourceSchema()))) throw new IllegalArgumentException("Schema begin changed original definition/directory/schema");
        long prior=Math.max(previous==null ? 0 : previous.allocatedThrough(),header.sourceMaximumApiId());
        if (header.kind()==SchemaChange.Kind.CREATE_COLUMN && header.changedApiId()<=prior || next.allocatedThrough()!=Math.max(prior,header.targetMaximumApiId())) throw new IllegalArgumentException("Schema begin reuses or changes allocated column ID");
        byte[] issued=null;
        for (int i=0;i<2;i++) {
            byte[] key=i==0 ? RatisColumnIdentities.columnKey(header.changedColumnId()) : RatisColumnIdentities.apiKey(header.changedApiId());
            var predicate=command.predicates().get(i+3); byte[] old=predicate.expectedValue();
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
