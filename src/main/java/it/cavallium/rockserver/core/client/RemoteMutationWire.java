package it.cavallium.rockserver.core.client;

import com.google.protobuf.*;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.api.proto.*;
import it.cavallium.rockserver.core.common.cdc.DistributedCdcWire;
import it.cavallium.rockserver.core.common.distributed.*;
import java.util.*;

/** Current bounded mutation wire contract; malformed replies never become recovery authority. */
public final class RemoteMutationWire {
    public static final int MAX_FRAME_BYTES=CommandResult.MAX_BYTES+64*1024, MAX_ENTRIES=4096;
    private RemoteMutationWire() {}
    public static <T extends MessageLite> T bounded(T value) { if(value.getSerializedSize()>MAX_FRAME_BYTES)throw new IllegalArgumentException("Mutation frame exceeds byte bound");return value; }
    public static byte[] digest(ByteString value) { if(value.size()!=32)throw new IllegalArgumentException("Invalid mutation digest length");return value.toByteArray(); }
    public static ByteString token(RemoteMutationToken value) { return ByteString.copyFrom(value.encode()); }
    public static RemoteMutationToken token(ByteString value) { if(value.size()<RemoteMutationToken.MIN_BYTES || value.size()>RemoteMutationToken.MAX_BYTES)throw new IllegalArgumentException("Invalid mutation token length");return RemoteMutationToken.decode(value.toByteArray()); }
    public static RemoteMutationDomainBinding domain(DomainBinding value){return RemoteMutationDomainBinding.newBuilder().setDomainId(DistributedCdcWire.uuid(value.domainId())).setDirectoryGeneration(value.directoryGeneration()).setDirectoryDigest(ByteString.copyFrom(value.directoryDigest())).setSchemaEpoch(value.schemaEpoch()).setSchemaDigest(ByteString.copyFrom(value.schemaDigest())).build();}
    public static RemoteMutationAuthority owner(RemoteMutationOwner value) {
        return RemoteMutationAuthority.newBuilder().setClusterId(DistributedCdcWire.uuid(value.clusterId())).setDatabaseId(DistributedCdcWire.uuid(value.databaseId())).setOwnerId(DistributedCdcWire.uuid(value.ownerId())).setOwnerEpoch(value.ownerEpoch()).setNextRequestSequence(value.nextRequestSequence()).setKind(value.kind().name()).addAllDomains(value.domains().stream().map(RemoteMutationWire::domain).toList()).build();
    }
    public static RemoteMutationOwner owner(RemoteMutationAuthority value) {
        if(value.getDomainsCount()<1 || value.getDomainsCount()>DomainBinding.MAX_DOMAINS)throw new IllegalArgumentException("Invalid owner domain count");
        var domains=value.getDomainsList().stream().map(d->new DomainBinding(DistributedCdcWire.uuid(d.getDomainId()),d.getDirectoryGeneration(),digest(d.getDirectoryDigest()),d.getSchemaEpoch(),digest(d.getSchemaDigest()))).toList();
        if(!domains.equals(DomainBinding.canonical(domains)))throw new IllegalArgumentException("Noncanonical owner domains");
        return new RemoteMutationOwner(DistributedCdcWire.uuid(value.getClusterId()),DistributedCdcWire.uuid(value.getDatabaseId()),DistributedCdcWire.uuid(value.getOwnerId()),value.getOwnerEpoch(),value.getNextRequestSequence(),RemoteMutationToken.Kind.valueOf(value.getKind()),domains);
    }
    public static List<Long> columns(List<Long> columns){if(columns.isEmpty() || columns.size()>256)throw new IllegalArgumentException("Invalid atomic column count");return List.copyOf(columns);}
    public record AtomicInput(List<AtomicMutation> mutations,List<AtomicPredicate> predicates) {}
    private static long keySize(KeyTuple key){if(key.getKeysCount()>DistributedSchemaCodec.MAX_KEY_COMPONENTS)throw new IllegalArgumentException("Atomic key component count exceeds bound");long bytes=4;for(var part:key.getKeysList())bytes=Math.addExact(bytes,4L+part.size());return bytes;}
    public static AtomicInput atomic(List<RemoteMutationAtomicWrite> mutations,List<RemoteMutationAtomicPredicate> predicates,long ttl){
        if(ttl<1 || ttl>60000 || (long)mutations.size()+predicates.size()<1 || (long)mutations.size()+predicates.size()>256)throw new IllegalArgumentException("Invalid atomic count/lifetime");
        long bytes=17;for(var m:mutations){var kind=Mutation.Kind.valueOf(m.getKind());Mutation.ReturnMode.valueOf(m.getReturns());if(kind!=Mutation.Kind.PUT && kind!=Mutation.Kind.DELETE && kind!=Mutation.Kind.MERGE || kind==Mutation.Kind.DELETE && m.hasValue() || kind==Mutation.Kind.MERGE && !m.hasValue())throw new IllegalArgumentException("Invalid atomic mutation kind/value");if(!m.hasKey())throw new IllegalArgumentException("Missing atomic key");bytes=Math.addExact(bytes,14+keySize(m.getKey())+(m.hasValue()?m.getValue().size():0));if(bytes>2*1024*1024)throw new IllegalArgumentException("Atomic input exceeds byte bound");}for(var p:predicates){if(!p.hasKey())throw new IllegalArgumentException("Missing atomic predicate key");bytes=Math.addExact(bytes,12+keySize(p.getKey())+(p.hasExpected()?p.getExpected().size():0));if(bytes>2*1024*1024)throw new IllegalArgumentException("Atomic input exceeds byte bound");}
        var writes=mutations.stream().map(m->new AtomicMutation(m.getColumn(),key(m.getKey()),Mutation.Kind.valueOf(m.getKind()),m.hasValue()?Buf.wrap(m.getValue().toByteArray()):null,Mutation.ReturnMode.valueOf(m.getReturns()))).toList();var tests=predicates.stream().map(p->new AtomicPredicate(p.getColumn(),key(p.getKey()),p.hasExpected()?Buf.wrap(p.getExpected().toByteArray()):null)).toList();RemoteMutationInputs.atomicSize(writes,tests,ttl);return new AtomicInput(writes,tests);
    }
    public static List<RemoteMutationAtomicWrite> mutations(List<AtomicMutation> values){return values.stream().map(m->{var b=RemoteMutationAtomicWrite.newBuilder().setColumn(m.column()).setKey(key(m.key())).setKind(m.kind().name()).setReturns(m.returns().name());if(m.value()!=null)b.setValue(ByteString.copyFrom(m.value().toByteArray()));return b.build();}).toList();}
    public static List<RemoteMutationAtomicPredicate> predicates(List<AtomicPredicate> values){return values.stream().map(p->{var b=RemoteMutationAtomicPredicate.newBuilder().setColumn(p.column()).setKey(key(p.key()));if(p.expected()!=null)b.setExpected(ByteString.copyFrom(p.expected().toByteArray()));return b.build();}).toList();}
    public static KeyTuple key(Keys value) { var builder=KeyTuple.newBuilder();if(value.keys().length>DistributedSchemaCodec.MAX_KEY_COMPONENTS)throw new IllegalArgumentException("Too many key components");for(var part:value.keys())builder.addKeys(ByteString.copyFrom(part.toByteArray()));return bounded(builder.build()); }
    public static Keys key(KeyTuple value) { if(value.getKeysCount()>DistributedSchemaCodec.MAX_KEY_COMPONENTS)throw new IllegalArgumentException("Too many key components");var parts=new Buf[value.getKeysCount()];for(int i=0;i<parts.length;i++)parts[i]=Buf.wrap(value.getKeys(i).toByteArray());return new Keys(parts); }
    public static List<RemoteMutationEntry> entries(List<Keys> keys,List<Buf> values) {
        if(keys.size()>MAX_ENTRIES || (!values.isEmpty() && keys.size()!=values.size()))throw new IllegalArgumentException("Invalid mutation entry count");var rows=new ArrayList<RemoteMutationEntry>(keys.size());long bytes=0;
        for(int i=0;i<keys.size();i++){var row=RemoteMutationEntry.newBuilder().setKey(key(keys.get(i)));if(!values.isEmpty() && values.get(i)!=null)row.setValue(ByteString.copyFrom(values.get(i).toByteArray()));var built=row.build();bytes+=built.getSerializedSize()+5L;if(bytes>MAX_FRAME_BYTES)throw new IllegalArgumentException("Mutation input exceeds byte bound");rows.add(built);}return List.copyOf(rows);
    }
    public static List<Keys> keys(List<RemoteMutationEntry> entries) { if(entries.size()>MAX_ENTRIES)throw new IllegalArgumentException("Too many mutation entries");var result=new ArrayList<Keys>(entries.size());for(var row:entries){if(!row.hasKey())throw new IllegalArgumentException("Missing logical key");result.add(key(row.getKey()));}return List.copyOf(result); }
    public static List<Buf> values(List<RemoteMutationEntry> entries,boolean present) { var result=new ArrayList<Buf>(entries.size());for(var row:entries){if(!present && row.hasValue())throw new IllegalArgumentException("Unexpected mutation value");if(present)result.add(row.hasValue()?Buf.wrap(row.getValue().toByteArray()):null);}return Collections.unmodifiableList(result); }
    public static RemoteMutationObservation status(RemoteMutationStatus value) {
        validateStatus(value);var b=RemoteMutationObservation.newBuilder().setToken(token(value.token())).setState(value.state().name()).setAppliedMutations(value.appliedMutations()).setCopiedBatches(value.copiedBatches()).setRetainedResults(value.retainedResults());if(value.outcome()!=null)b.setOutcome(value.outcome().name());if(value.resultDigest()!=null)b.setResultDigest(ByteString.copyFrom(value.resultDigest()));return b.build();
    }
    public static RemoteMutationStatus status(RemoteMutationObservation value,RemoteMutationToken expected) {
        bounded(value);var actual=token(value.getToken());if(!actual.equals(expected))throw new IllegalArgumentException("Crossed mutation token");var result=new RemoteMutationStatus(actual,RemoteMutationStatus.State.valueOf(value.getState()),value.getAppliedMutations(),value.getCopiedBatches(),value.getRetainedResults(),value.hasOutcome()?CommandResult.Status.valueOf(value.getOutcome()):null,value.hasResultDigest()?digest(value.getResultDigest()):null);validateStatus(result);return result;
    }
    private static void validateStatus(RemoteMutationStatus value) {
        Objects.requireNonNull(value.token());Objects.requireNonNull(value.state());if(value.retainedResults()<0 || value.appliedMutations() < -1 || value.copiedBatches() < -1 || ((value.appliedMutations()<0)!=(value.copiedBatches()<0)))throw new IllegalArgumentException("Invalid mutation progress");if(value.resultDigest()!=null && value.resultDigest().length!=32)throw new IllegalArgumentException("Invalid result digest");
    }
    public static void budget(int offset,int items,int bytes) { if(offset<0 || items<1 || items>RemoteMutationResultPage.MAX_ITEMS || bytes<1 || bytes>CommandResult.MAX_BYTES)throw new IllegalArgumentException("Invalid mutation page budget"); }
    public static RemoteMutationPage page(RemoteMutationResultPage value,int offset,int items,int bytes,RemoteMutationToken expected) {
        var b=RemoteMutationPage.newBuilder().setStatus(status(value.status())).setOffset(value.offset()).setNextOffset(value.nextOffset()).setHasMore(value.hasMore());value.rows().forEach(row->b.addRows(ByteString.copyFrom(row)));var wire=bounded(b.build());page(wire,expected,offset,items,bytes);return wire;
    }
    public static RemoteMutationResultPage page(RemoteMutationPage value,RemoteMutationToken expected,int offset,int items,int bytes) {
        budget(offset,items,bytes);bounded(value);if(!value.hasStatus() || value.getOffset()!=offset || value.getRowsCount()>items)throw new IllegalArgumentException("Invalid mutation page metadata");var rows=new ArrayList<byte[]>(value.getRowsCount());long size=0;for(var row:value.getRowsList()){size+=row.size();if(size>bytes)throw new IllegalArgumentException("Mutation page exceeds requested bytes");var encoded=row.toByteArray();CommandResult.decode(encoded);rows.add(encoded);}var result=new RemoteMutationResultPage(status(value.getStatus(),expected),offset,value.getNextOffset(),rows);if(result.hasMore()!=value.getHasMore())throw new IllegalArgumentException("Invalid mutation page continuation");return result;
    }
    public static RemoteMutationToken prepared(RemoteMutationTokenResponse value,RemoteMutationOwner owner,UUID request,long sequence,byte[] input) {
        var result=token(value.getToken());var expected=new RemoteMutationToken(owner.clusterId(),owner.databaseId(),owner.ownerId(),request,owner.ownerEpoch(),sequence,owner.kind(),owner.domains(),input);if(!result.equals(expected))throw new IllegalArgumentException("Crossed prepared mutation identity/input");return result;
    }
}
