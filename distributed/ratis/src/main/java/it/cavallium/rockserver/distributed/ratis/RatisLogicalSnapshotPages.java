package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.util.*;

/** Logical pages over frozen owner captures; no current-directory reads or replacement snapshots. */
public final class RatisLogicalSnapshotPages {
    public static final int MAX_PARTITIONS=64;
    public record Owner(RangeDescriptor route,RatisRoutedClient.SnapshotTarget target,SnapshotFenceStatus.Capture token,TransactionScope scope) {
        public Owner {
            Objects.requireNonNull(route); Objects.requireNonNull(target); Objects.requireNonNull(token); Objects.requireNonNull(scope);
            var actual=target.descriptor(); var participant=target.fence().participant(route.groupId());
            if (route.topologyState()!=RangeDescriptor.TopologyState.ACTIVE || !route.rangeId().equals(actual.rangeId()) || !route.groupId().equals(actual.groupId()) || route.rangeEpoch()!=actual.rangeEpoch()
                    || !Arrays.equals(route.lowerInclusive(),actual.lowerInclusive()) || !Arrays.equals(route.upperExclusive(),actual.upperExclusive())
                    || !token.ownerIncarnation().equals(target.incarnation()) || !Arrays.equals(ScopedTransactionRpc.scopeBytes(scope),ScopedTransactionRpc.scopeBytes(participant.scope()))) throw new IllegalArgumentException("Snapshot page owner differs from its frozen capture");
        }
    }
    interface OwnerReader {
        byte[] get(Owner owner,UUID column,byte[] key) throws IOException;
        PhysicalRangePage page(Owner owner,UUID column,byte[] lower,byte[] upper,boolean reverse,byte[] resume,int items,int bytes) throws IOException;
    }
    private static OwnerReader immutable(RatisRoutedClient data) {
        Objects.requireNonNull(data); return new OwnerReader() {
            public byte[] get(Owner owner,UUID column,byte[] key) throws IOException { return data.snapshotGet(owner.target(),owner.token(),column,key); }
            public PhysicalRangePage page(Owner owner,UUID column,byte[] lower,byte[] upper,boolean reverse,byte[] resume,int items,int bytes) throws IOException { return data.snapshotPage(owner.target(),owner.token(),column,lower,upper,reverse,resume,items,bytes); }
        };
    }
    private record Partition(Owner owner,byte[] lower,byte[] upper) {}
    private RatisLogicalSnapshotPages() {}
    public static RangePage<KV> read(RatisRoutedClient data,List<Owner> owners,TransactionScope scope,DistributedSchema schema,
                                     long column,Keys lower,Keys upper,boolean reverse,Keys resume,RangeBudget budget) throws IOException {
        return read(immutable(data),owners,scope,schema,column,lower,upper,reverse,resume,budget);
    }
    static RangePage<KV> read(OwnerReader reader,List<Owner> owners,TransactionScope scope,DistributedSchema schema,long column,Keys lower,Keys upper,boolean reverse,Keys resume,RangeBudget budget) throws IOException {
        var binding=RatisLogicalClient.supported(schema,column); UUID id=binding.columnId();
        byte[] low=RatisLogicalRangePages.bound(schema,id,lower),high=RatisLogicalRangePages.upperBound(schema,id,upper);
        if (low!=null && high!=null && Arrays.compareUnsigned(low,high)>=0 || !scope.containsRange(id,low,high)) throw new IllegalArgumentException("Logical snapshot page outside its declared union scope");
        var frozen=owners(owners,schema); var partitions=new ArrayList<Partition>(); byte[] cursor=low;
        for (var owner:frozen) {
            byte[] from=maximum(low,owner.route().lowerInclusive()),to=minimum(high,owner.route().upperExclusive());
            if (from!=null && to!=null && Arrays.compareUnsigned(from,to)>=0) continue;
            if (!Arrays.equals(cursor,from) || !owner.scope().containsRange(id,from,to)) throw new IllegalArgumentException("Snapshot owners do not completely cover logical page bounds");
            partitions.add(new Partition(owner,from,to)); cursor=to;
        }
        if (partitions.isEmpty() || !Arrays.equals(cursor,high)) throw new IllegalArgumentException("Snapshot owners leave a logical page coverage gap");
        if (reverse) Collections.reverse(partitions);
        return RatisLogicalRangePages.read(new Reader(Objects.requireNonNull(reader),frozen,partitions,id,reverse),scope,schema,column,lower,upper,reverse,resume,budget);
    }
    public static Buf get(RatisRoutedClient data,List<Owner> owners,TransactionScope scope,DistributedSchema schema,long column,Keys keys) throws IOException {
        return get(immutable(data),owners,scope,schema,column,keys);
    }
    static Buf get(OwnerReader reader,List<Owner> owners,TransactionScope scope,DistributedSchema schema,long column,Keys keys) throws IOException {
        var binding=RatisLogicalClient.supported(schema,column); var encoded=schema.encodeLogicalKey(binding.columnId(),keys); byte[] key=encoded.physicalKey();
        if (!scope.contains(binding.columnId(),key)) throw new IllegalArgumentException("Logical snapshot point outside declared scope");
        var frozen=owners(owners,schema); var physical=new Reader(Objects.requireNonNull(reader),frozen,List.of(),binding.columnId(),false);
        byte[] value=physical.get(binding.columnId(),key);
        if (binding.schema().variableLengthKeysCount()!=0) {
            try { value=schema.decodeBucket(binding.columnId(),key,value).get(encoded.variableKeys()); }
            catch (IllegalArgumentException invalid) { throw new IOException("Stored bucket cannot be served completely",invalid); }
        }
        if (value!=null && !binding.schema().hasValue() && value.length!=0) throw new IOException("Nonempty value in key-only column");
        return value==null ? null : Buf.wrap(value);
    }
    private static List<Owner> owners(List<Owner> owners,DistributedSchema schema) {
        if (Objects.requireNonNull(owners).isEmpty() || owners.size()>MAX_PARTITIONS) throw new IllegalArgumentException("Invalid frozen snapshot owner count");
        var frozen=new ArrayList<>(List.copyOf(owners)); frozen.sort((a,b) -> compareLower(a.route().lowerInclusive(),b.route().lowerInclusive()));
        var fence=frozen.getFirst().target().fence(); var groups=new HashSet<UUID>(); byte[] previousUpper=null; boolean first=true;
        if (!schema.clusterId().equals(fence.clusterId()) || !schema.databaseId().equals(fence.databaseId()) || !schema.domainId().equals(fence.domainId())
                || schema.schemaEpoch()!=fence.schemaEpoch() || !Arrays.equals(schema.digest(),fence.schemaDigest())) throw new IllegalArgumentException("Logical schema differs from frozen snapshot");
        for (var owner:frozen) {
            if (!owner.target().fence().equals(fence) || !groups.add(owner.route().groupId()) || !first && (previousUpper==null || owner.route().lowerInclusive()==null || Arrays.compareUnsigned(previousUpper,owner.route().lowerInclusive())>0)) throw new IllegalArgumentException("Frozen snapshot routes overlap or change definition");
            previousUpper=owner.route().upperExclusive(); first=false;
        }
        return List.copyOf(frozen);
    }
    private static byte[] maximum(byte[] a,byte[] b) { return a==null ? b : b==null || Arrays.compareUnsigned(a,b)>=0 ? a : b; }
    private static byte[] minimum(byte[] a,byte[] b) { return a==null ? b : b==null || Arrays.compareUnsigned(a,b)<=0 ? a : b; }
    private static int compareLower(byte[] a,byte[] b) { return a==null ? b==null ? 0 : -1 : b==null ? 1 : Arrays.compareUnsigned(a,b); }
    private static boolean contains(byte[] lower,byte[] upper,byte[] key) { return (lower==null || Arrays.compareUnsigned(key,lower)>=0) && (upper==null || Arrays.compareUnsigned(key,upper)<0); }
    private static final class Reader implements RatisLogicalRangePages.PhysicalReader {
        final OwnerReader source; final List<Owner> owners; final List<Partition> partitions; final UUID column; final boolean reverse;
        int partition,scanned; long scannedBytes;
        Reader(OwnerReader source,List<Owner> owners,List<Partition> partitions,UUID column,boolean reverse) { this.source=source; this.owners=owners; this.partitions=partitions; this.column=column; this.reverse=reverse; }
        private void charge(int rows,long bytes) throws IOException {
            scanned+=rows; scannedBytes+=bytes;
            if (scanned>RatisLogicalRangePages.MAX_SCANNED_ROWS || scannedBytes>RatisLogicalRangePages.MAX_SCANNED_BYTES) throw new IOException("Logical snapshot scan quantum exhausted");
        }
        public byte[] get(UUID column,byte[] key) throws IOException {
            if (!this.column.equals(column)) throw new IllegalArgumentException("Snapshot page column changed");
            var owner=owners.stream().filter(candidate -> contains(candidate.route().lowerInclusive(),candidate.route().upperExclusive(),key) && candidate.scope().contains(column,key)).findFirst().orElseThrow(() -> new IllegalArgumentException("Snapshot point has no declared frozen owner"));
            byte[] value=source.get(owner,column,key); charge(1,key.length+(value==null ? 0L : value.length)); return value;
        }
        public PhysicalRangePage page(UUID column,byte[] lower,byte[] upper,boolean reverse,byte[] resume,int items,int bytes) throws IOException {
            if (!this.column.equals(column) || this.reverse!=reverse) throw new IllegalArgumentException("Snapshot page traversal changed");
            while (partition<partitions.size()) {
                var part=partitions.get(partition); byte[] cursor=resume;
                if (cursor!=null) {
                    if (!reverse && part.upper()!=null && Arrays.compareUnsigned(cursor,part.upper())>=0 || reverse && part.lower()!=null && Arrays.compareUnsigned(cursor,part.lower())<=0) { partition++; continue; }
                    if (!contains(part.lower(),part.upper(),cursor)) cursor=null;
                }
                int count=Math.min(items,RatisLogicalRangePages.MAX_SCANNED_ROWS-scanned);
                int remaining=(int)Math.min(bytes,RatisLogicalRangePages.MAX_SCANNED_BYTES-scannedBytes);
                if (count<1 || remaining<PhysicalRangePage.HEADER_BYTES) throw new IOException("Logical snapshot scan quantum exhausted");
                var owner=part.owner(); var page=source.page(owner,column,part.lower(),part.upper(),reverse,cursor,count,remaining);
                if (page.rows().size()>count || page.payloadBytes()>remaining) throw new IOException("Snapshot owner exceeded physical page budget");
                byte[] previous=cursor;
                for (var row:page.rows()) {
                    byte[] key=row.key(); int order=previous==null ? 0 : Arrays.compareUnsigned(key,previous);
                    if (!contains(part.lower(),part.upper(),key) || !owner.scope().contains(column,key) || previous!=null && (reverse ? order>=0 : order<=0)) throw new IOException("Snapshot owner page changed bounds or order"); previous=key;
                }
                charge(page.rows().size(),page.payloadBytes()); if (!page.hasMore()) partition++;
                if (!page.rows().isEmpty()) return new PhysicalRangePage(page.rows(),page.hasMore() || partition<partitions.size());
            }
            return new PhysicalRangePage(List.of(),false);
        }
    }
}
