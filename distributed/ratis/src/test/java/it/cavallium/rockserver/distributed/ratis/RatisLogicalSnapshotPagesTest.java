package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** Frozen owner-token transport fixtures isolate logical routing, collision decoding and scan budgets. */
class RatisLogicalSnapshotPagesTest {
    final UUID cluster=new UUID(0,1),database=new UUID(0,2),domain=new UUID(0,3),column=new UUID(0,4),left=new UUID(0,5),right=new UUID(0,6);
    Keys key(int value) { return new Keys(Buf.wrap(new byte[]{(byte)value})); }
    Keys member(int fixed,String tail) { return new Keys(Buf.wrap(new byte[]{(byte)fixed}),Buf.wrap(tail.getBytes(StandardCharsets.UTF_8))); }
    List<Keys> keys(RangePage<KV> page) { return page.items().stream().map(KV::keys).toList(); }
    record Call(UUID group,byte[] lower,byte[] upper,byte[] resume,int items,int bytes) {}
    final class Fixture implements AutoCloseable {
        final DistributedSchema schema;
        final RangeDirectory directory;
        final SnapshotFence fence;
        final RatisRoutedClient data;
        final TransactionScope scope;
        final List<RatisLogicalSnapshotPages.Owner> owners;
        final Map<UUID,TreeMap<byte[],byte[]>> rows=new HashMap<>();
        final List<Call> calls=new ArrayList<>();
        UUID unavailable; boolean badOrder; int fetchedRows,gets;
        Fixture(boolean bucket,int width,byte[] split) {
            this(ColumnSchema.of(IntArrayList.of(width),bucket ? ObjectArrayList.of(ColumnHashType.ALLSAME8) : ObjectArrayList.of(),true),split);
        }
        Fixture(ColumnSchema shape,byte[] split) {
            schema=new DistributedSchema(cluster,database,domain,1,0,List.of(new DistributedSchema.Column(71,column,"values",shape,null)));
            var routes=new ArrayList<RangeDescriptor>(); var participants=new ArrayList<SnapshotFence.Participant>();
            for (int n=0;n<2;n++) {
                UUID group=n==0 ? left : right; byte[] low=n==0 ? null : split,high=n==0 ? split : null;
                var replica=new ReplicaDescriptor(new UUID(0,20+n),new UUID(0,30+n),new UUID(0,40+n),"frozen-"+n,"127.0.0.1:"+(12345+n),ReplicaDescriptor.Role.VOTER);
                var route=new RangeDescriptor(group,group,1,0,low,high,RangeDescriptor.TopologyState.ACTIVE,List.of(replica)); routes.add(route);
                var partScope=new TransactionScope(List.of(TransactionScope.Span.range(column,low,high)));
                participants.add(new SnapshotFence.Participant(group,group,1,schema.digest(),new OperationId(new UUID(0,50+n),1,0),new OperationId(new UUID(0,50+n),1,1),partScope));
                rows.put(group,new TreeMap<>(Arrays::compareUnsigned));
            }
            directory=new RangeDirectory(cluster,database,domain,1,schema.digest(),0,Set.of(column),0,routes);
            fence=new SnapshotFence(cluster,database,domain,new UUID(0,60),new UUID(0,61),1,new OperationId(new UUID(0,62),1,0),0,1,schema.digest(),participants);
            scope=new TransactionScope(List.of(TransactionScope.Span.range(column,null,null)));
            data=new RatisRoutedClient(directory,new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID col,byte[] key) { throw new AssertionError("No live read fallback"); }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) { throw new AssertionError("No mutation fallback"); }
                public byte[] transaction(RangeDescriptor route,byte[] bytes,boolean fresh) { throw new AssertionError("No owner discovery, OPEN or recapture"); }
                public byte[] snapshot(RangeDescriptor route,ReplicaDescriptor replica,byte[] bytes) throws IOException {
                    assertEquals(directory.ranges().stream().filter(r -> r.groupId().equals(route.groupId())).findFirst().orElseThrow(),route); assertEquals(route.replicas().getFirst(),replica);
                    if (route.groupId().equals(unavailable)) throw new RatisSnapshotLostException("Original owner capture is unavailable");
                    var packet=ScopedTransactionRpc.parse(bytes,ScopedTransactionRpc.MAX_REQUEST_BYTES); var input=ByteBuffer.wrap(packet.body());
                    var binding=ScopedTransactionRpc.snapshotBinding(input); var token=ScopedTransactionRpc.snapshotToken(input); assertEquals(ScopedTransactionRpc.SnapshotBinding.of(fence),binding);
                    int index=route.groupId().equals(left) ? 0 : 1; assertEquals(new UUID(0,70+index),token.ownerIncarnation()); assertEquals(new UUID(0,80+index),token.snapshotId()); assertEquals(column,ScopedTransactionRpc.uuid(input));
                    final byte[] payload;
                    if (packet.kind()==ScopedTransactionRpc.SNAPSHOT_GET) {
                        gets++; byte[] key=ScopedTransactionRpc.blob(input,PhysicalRangePage.MAX_BYTES,false); assertFalse(input.hasRemaining());
                        byte[] value=rows.get(route.groupId()).get(key); var output=ByteBuffer.allocate(4+(value==null ? 0 : value.length)); ScopedTransactionRpc.putBlob(output,value); payload=output.array();
                    } else {
                        assertEquals(ScopedTransactionRpc.SNAPSHOT_PAGE,packet.kind()); byte[] low=ScopedTransactionRpc.blob(input,PhysicalRangePage.MAX_BYTES,true),high=ScopedTransactionRpc.blob(input,PhysicalRangePage.MAX_BYTES,true); boolean reverse=input.get()!=0;
                        byte[] resume=ScopedTransactionRpc.blob(input,PhysicalRangePage.MAX_BYTES,true); int items=input.getInt(),limit=input.getInt(); assertFalse(input.hasRemaining()); calls.add(new Call(route.groupId(),low,high,resume,items,limit));
                        assertTrue(fence.participant(route.groupId()).scope().containsRange(column,low,high));
                        var eligible=new ArrayList<PhysicalRangePage.Row>();
                        for (var entry:rows.get(route.groupId()).entrySet()) if ((low==null || Arrays.compareUnsigned(entry.getKey(),low)>=0) && (high==null || Arrays.compareUnsigned(entry.getKey(),high)<0)
                                && (resume==null || (reverse ? Arrays.compareUnsigned(entry.getKey(),resume)<0 : Arrays.compareUnsigned(entry.getKey(),resume)>0))) eligible.add(new PhysicalRangePage.Row(entry.getKey(),entry.getValue()));
                        if (reverse) Collections.reverse(eligible); var selected=new ArrayList<PhysicalRangePage.Row>();
                        for (var row:eligible) { if (selected.size()==items) break; var next=new ArrayList<>(selected); next.add(row); if (new PhysicalRangePage(next,false).payloadBytes()>limit) break; selected=next; }
                        boolean more=selected.size()<eligible.size(); if (selected.isEmpty() && more) throw new IOException("Physical row exceeds owner page budget");
                        if (badOrder) Collections.reverse(selected); fetchedRows+=selected.size(); payload=ScopedTransactionRpc.pageBytes(new PhysicalRangePage(selected,more));
                    }
                    return ScopedTransactionRpc.frame(packet.kind(),ScopedTransactionRpc.snapshotReply(binding,token,payload));
                }
            });
            var frozen=new ArrayList<RatisLogicalSnapshotPages.Owner>();
            for (int n=0;n<routes.size();n++) {
                var route=routes.get(n); var target=data.restoreSnapshotTarget(fence,route,route.replicas().getFirst(),new UUID(0,70+n));
                frozen.add(new RatisLogicalSnapshotPages.Owner(route,target,new SnapshotFenceStatus.Capture(new UUID(0,70+n),new UUID(0,80+n),new ApplyPosition(1,10+n)),fence.participant(route.groupId()).scope()));
            }
            Collections.reverse(frozen); owners=List.copyOf(frozen);
        }
        void put(byte[] key,byte[] value) { UUID group=directory.route(column,key).groupId(); rows.get(group).put(key.clone(),value.clone()); }
        void fixed(int key) { put(new byte[]{(byte)key},new byte[]{(byte)key}); }
        void bucket(int key,String... tails) {
            var bucket=DistributedBucket.empty(schema.column(71).schema()); for (var tail:tails) bucket=bucket.put(List.of(tail.getBytes(StandardCharsets.UTF_8)),new byte[]{(byte)key}); put(new byte[]{(byte)key,0},bucket.encode());
        }
        RangePage<KV> read(Keys lower,Keys upper,boolean reverse,Keys resume,RangeBudget budget) throws IOException { return RatisLogicalSnapshotPages.read(data,owners,scope,schema,71,lower,upper,reverse,resume,budget); }
        public void close() throws Exception { data.close(); }
    }

    @Test void fixedPagesClipEveryFrozenRouteAndResumeAcrossOwnersInEitherDirection() throws Exception {
        try (var f=new Fixture(false,1,new byte[]{(byte)128})) {
            for (int n:new int[]{1,3,129,140}) f.fixed(n);
            var first=f.read(null,null,false,null,new RangeBudget(2,48)); assertEquals(List.of(key(1),key(3)),keys(first)); assertTrue(first.hasMore());
            var rest=f.read(null,null,false,first.resumeAfter(),new RangeBudget(2,48)); assertEquals(List.of(key(129),key(140)),keys(rest)); assertFalse(rest.hasMore());
            var reverse=f.read(null,null,true,null,new RangeBudget(2,48)); assertEquals(List.of(key(140),key(129)),keys(reverse));
            assertEquals(List.of(key(3),key(1)),keys(f.read(null,null,true,reverse.resumeAfter(),new RangeBudget(2,48))));
            assertEquals(List.of(key(3),key(129)),keys(f.read(key(2),key(140),false,null,RangeBudget.DEFAULT)));
            assertEquals(List.of(key(129),key(140)),keys(f.read(null,null,false,key(128),RangeBudget.DEFAULT)));
            assertEquals(List.of(key(3),key(1)),keys(f.read(null,null,true,key(128),RangeBudget.DEFAULT)));
            assertTrue(f.calls.stream().anyMatch(call -> call.group().equals(left) && Arrays.equals(call.upper(),new byte[]{(byte)128})));
            assertTrue(f.calls.stream().anyMatch(call -> call.group().equals(right) && Arrays.equals(call.lower(),new byte[]{(byte)128})));
            assertThrows(RocksDBException.class,() -> f.read(null,null,false,null,new RangeBudget(1,1)));
        }
    }

    @Test void bucketsKeepMemberOrderLogicalBoundsAndExclusiveWithinBucketResume() throws Exception {
        try (var f=new Fixture(true,1,new byte[]{(byte)128})) {
            f.bucket(2,"beta","alpha"); f.bucket(200,"beta","alpha");
            var bounded=f.read(member(2,"beta"),member(200,"beta"),false,null,RangeBudget.DEFAULT); assertEquals(List.of(member(2,"beta"),member(200,"alpha")),keys(bounded));
            assertEquals(List.of(member(200,"alpha"),member(2,"beta")),keys(f.read(member(2,"beta"),member(200,"beta"),true,null,RangeBudget.DEFAULT)));
            assertEquals(List.of(member(2,"alpha")),keys(f.read(member(2,"alpha"),member(2,"beta"),false,null,RangeBudget.DEFAULT)));
            var first=f.read(null,null,false,null,new RangeBudget(1,1024)); assertEquals(List.of(member(2,"alpha")),keys(first));
            var second=f.read(null,null,false,first.resumeAfter(),new RangeBudget(1,1024)); assertEquals(List.of(member(2,"beta")),keys(second));
            assertEquals(List.of(member(200,"alpha"),member(200,"beta")),keys(f.read(null,null,false,second.resumeAfter(),RangeBudget.DEFAULT)));
            var reverse=f.read(null,null,true,null,new RangeBudget(1,1024)); assertEquals(List.of(member(200,"beta")),keys(reverse));
            assertEquals(List.of(member(200,"alpha"),member(2,"beta"),member(2,"alpha")),keys(f.read(null,null,true,reverse.resumeAfter(),RangeBudget.DEFAULT)));
            assertArrayEquals(new byte[]{2},RatisLogicalSnapshotPages.get(f.data,f.owners,f.scope,f.schema,71,member(2,"alpha")).toByteArray());
            assertNull(RatisLogicalSnapshotPages.get(f.data,f.owners,f.scope,f.schema,71,member(2,"missing")));
            int before=f.calls.size()+f.gets;
            assertThrows(IllegalArgumentException.class,() -> f.read(member(2,"beta"),member(2,"alpha"),false,null,RangeBudget.DEFAULT));
            assertThrows(IllegalArgumentException.class,() -> f.read(member(2,"beta"),member(200,"beta"),false,member(2,"alpha"),RangeBudget.DEFAULT));
            assertThrows(IllegalArgumentException.class,() -> f.read(member(2,"beta"),member(200,"beta"),false,member(200,"beta"),RangeBudget.DEFAULT));
            var partial=new TransactionScope(List.of(TransactionScope.Span.range(column,null,new byte[]{(byte)200,0})));
            assertThrows(IllegalArgumentException.class,() -> RatisLogicalSnapshotPages.read(f.data,f.owners,partial,f.schema,71,member(2,"alpha"),member(200,"beta"),false,null,RangeBudget.DEFAULT));
            assertEquals(before,f.calls.size()+f.gets);
        }
    }

    @Test void fixedPrefixBoundsStayInsideDeclaredBucketScopeAndMixWithFullMembers() throws Exception {
        try (var f=new Fixture(true,1,new byte[]{(byte)128})) {
            for (int n:new int[]{1,2,200,201}) f.bucket(n,"beta","alpha");
            var prefixScope=new TransactionScope(List.of(TransactionScope.Span.range(column,new byte[]{2},new byte[]{(byte)201})));
            RatisLogicalRangePages.validateBounds(prefixScope,f.schema,71,key(2),key(201));
            var all=RatisLogicalSnapshotPages.read(f.data,f.owners,prefixScope,f.schema,71,key(2),key(201),false,null,RangeBudget.DEFAULT);
            assertEquals(List.of(member(2,"alpha"),member(2,"beta"),member(200,"alpha"),member(200,"beta")),keys(all)); assertFalse(all.hasMore());
            assertTrue(f.calls.stream().anyMatch(call -> call.group().equals(left) && Arrays.equals(call.lower(),new byte[]{2})));
            assertTrue(f.calls.stream().anyMatch(call -> call.group().equals(right) && Arrays.equals(call.upper(),new byte[]{(byte)201})));
            var oneBucket=new TransactionScope(List.of(TransactionScope.Span.range(column,new byte[]{2},new byte[]{3})));
            assertEquals(List.of(member(2,"alpha"),member(2,"beta")),keys(RatisLogicalSnapshotPages.read(f.data,f.owners,oneBucket,f.schema,71,key(2),key(3),false,null,RangeBudget.DEFAULT)));
            assertEquals(List.of(member(2,"beta"),member(200,"alpha"),member(200,"beta")),keys(RatisLogicalSnapshotPages.read(f.data,f.owners,prefixScope,f.schema,71,member(2,"beta"),key(201),false,null,RangeBudget.DEFAULT)));
            assertEquals(List.of(member(200,"alpha"),member(2,"beta"),member(2,"alpha")),keys(RatisLogicalSnapshotPages.read(f.data,f.owners,prefixScope,f.schema,71,key(2),member(200,"beta"),true,null,RangeBudget.DEFAULT)));
            int before=f.calls.size()+f.gets;
            assertThrows(IllegalArgumentException.class,() -> RatisLogicalSnapshotPages.read(f.data,f.owners,prefixScope,f.schema,71,key(2),member(201,"beta"),false,null,RangeBudget.DEFAULT));
            assertThrows(IllegalArgumentException.class,() -> RatisLogicalSnapshotPages.read(f.data,f.owners,prefixScope,f.schema,71,key(2),key(201),false,member(1,"alpha"),RangeBudget.DEFAULT));
            assertThrows(IllegalArgumentException.class,() -> RatisLogicalSnapshotPages.read(f.data,f.owners,prefixScope,f.schema,71,key(2),key(201),false,member(201,"alpha"),RangeBudget.DEFAULT));
            assertThrows(IllegalArgumentException.class,() -> RatisLogicalSnapshotPages.read(f.data,f.owners,prefixScope,f.schema,71,key(2),key(201),false,key(2),RangeBudget.DEFAULT));
            assertThrows(IllegalArgumentException.class,() -> RatisLogicalRangePages.validateBounds(prefixScope,f.schema,71,new Keys(Buf.wrap(new byte[]{2,0})),key(201)));
            assertEquals(before,f.calls.size()+f.gets);
        }
    }

    @Test void fixedOnlyMultiFieldPrefixesValidateWidthsAndNeverAcceptPartialVariableTails() throws Exception {
        var fixed=ColumnSchema.of(IntArrayList.of(1,2),ObjectArrayList.of(),true);
        try (var f=new Fixture(fixed,new byte[]{(byte)128})) {
            f.put(new byte[]{2,0,1},new byte[]{1}); f.put(new byte[]{2,0,2},new byte[]{2}); f.put(new byte[]{3,0,1},new byte[]{3});
            var scope=new TransactionScope(List.of(TransactionScope.Span.range(column,new byte[]{2},new byte[]{3})));
            var first=new Keys(Buf.wrap(new byte[]{2}),Buf.wrap(new byte[]{0,1})); var second=new Keys(Buf.wrap(new byte[]{2}),Buf.wrap(new byte[]{0,2}));
            assertEquals(List.of(first,second),keys(RatisLogicalSnapshotPages.read(f.data,f.owners,scope,f.schema,71,key(2),key(3),false,null,RangeBudget.DEFAULT)));
            assertEquals(List.of(first),keys(RatisLogicalSnapshotPages.read(f.data,f.owners,scope,f.schema,71,key(2),second,false,null,RangeBudget.DEFAULT)));
            int before=f.calls.size(); assertThrows(IllegalArgumentException.class,() -> RatisLogicalRangePages.validateBounds(scope,f.schema,71,new Keys(Buf.wrap(new byte[]{2,0})),key(3)));
            assertThrows(IllegalArgumentException.class,() -> RatisLogicalSnapshotPages.read(f.data,f.owners,scope,f.schema,71,key(2),key(3),false,key(2),RangeBudget.DEFAULT)); assertEquals(before,f.calls.size());
        }
        var twoTails=ColumnSchema.of(IntArrayList.of(1),ObjectArrayList.of(ColumnHashType.ALLSAME8,ColumnHashType.ALLSAME8),true);
        try (var f=new Fixture(twoTails,new byte[]{(byte)128})) {
            assertThrows(IllegalArgumentException.class,() -> RatisLogicalRangePages.validateBounds(f.scope,f.schema,71,member(2,"partial-variable"),key(3)));
            assertThrows(IllegalArgumentException.class,() -> RatisLogicalSnapshotPages.read(f.data,f.owners,f.scope,f.schema,71,key(2),member(3,"partial-variable"),false,null,RangeBudget.DEFAULT));
            assertTrue(f.calls.isEmpty()); assertEquals(0,f.gets);
        }
    }

    @Test void frozenOwnerReadsIgnoreCurrentDirectoryAndOwnerLossNeverFallsBack() throws Exception {
        try (var f=new Fixture(false,1,new byte[]{(byte)128})) {
            f.fixed(1); f.fixed(129); var replica=f.directory.ranges().getFirst().replicas().getFirst(); UUID replacement=new UUID(0,99);
            var current=new RangeDescriptor(replacement,replacement,2,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(replica));
            f.data.refresh(new RangeDirectory(cluster,database,domain,1,f.schema.digest(),1,Set.of(column),0,List.of(current)));
            assertEquals(List.of(key(1),key(129)),keys(f.read(null,null,false,null,RangeBudget.DEFAULT)));
            assertArrayEquals(new byte[]{1},RatisLogicalSnapshotPages.get(f.data,f.owners,f.scope,f.schema,71,key(1)).toByteArray());
            f.unavailable=right; assertThrows(RatisSnapshotLostException.class,() -> f.read(null,null,false,null,RangeBudget.DEFAULT));
        }
    }

    @Test void coverageAndPhysicalOrderingAreValidatedBeforeReturningAnyPartialPage() throws Exception {
        try (var f=new Fixture(false,1,new byte[]{(byte)128})) {
            f.fixed(1); f.fixed(3); f.fixed(129); var leftOwner=f.owners.stream().filter(owner -> owner.route().groupId().equals(left)).findFirst().orElseThrow();
            assertThrows(IllegalArgumentException.class,() -> RatisLogicalSnapshotPages.read(f.data,List.of(leftOwner),f.scope,f.schema,71,null,null,false,null,RangeBudget.DEFAULT));
            assertThrows(IllegalArgumentException.class,() -> RatisLogicalSnapshotPages.read(f.data,List.of(leftOwner,leftOwner),f.scope,f.schema,71,null,null,false,null,RangeBudget.DEFAULT));
            assertThrows(IllegalArgumentException.class,() -> RatisLogicalSnapshotPages.read(f.data,Collections.nCopies(65,leftOwner),f.scope,f.schema,71,null,null,false,null,RangeBudget.DEFAULT));
            assertThrows(IllegalArgumentException.class,() -> f.read(key(1),key(129),false,key(129),RangeBudget.DEFAULT)); assertTrue(f.calls.isEmpty());
            f.badOrder=true; assertThrows(IOException.class,() -> f.read(null,null,false,null,RangeBudget.DEFAULT));
        }
        try (var f=new Fixture(false,1,new byte[]{(byte)128})) { f.fixed(129); var page=f.read(null,null,false,null,RangeBudget.DEFAULT); assertEquals(List.of(key(129)),keys(page)); assertFalse(page.hasMore()); assertEquals(2,f.calls.size()); }
    }

    @Test void rowScanQuantumIsSharedAcrossPartitionsIncludingEmptyBuckets() throws Exception {
        try (var f=new Fixture(true,2,new byte[]{8})) {
            for (int n=0;n<4097;n++) f.put(new byte[]{(byte)(n>>>8),(byte)n,0},new byte[0]);
            assertThrows(IOException.class,() -> f.read(null,null,false,null,RangeBudget.DEFAULT)); assertEquals(4096,f.fetchedRows); assertEquals(32,f.calls.size());
            assertTrue(f.calls.stream().anyMatch(call -> call.group().equals(left))); assertTrue(f.calls.stream().anyMatch(call -> call.group().equals(right)));
        }
    }

    @Test void byteScanBudgetIncludesBucketResumeGetAndEarlierOwnerPages() throws Exception {
        try (var f=new Fixture(true,1,new byte[]{(byte)128})) {
            byte[] large=new byte[1_900_000]; var bucket=DistributedBucket.empty(f.schema.column(71).schema()).put(List.of(new byte[]{1}),large).encode();
            for (int n:new int[]{0,1,2,3,4,128}) f.put(new byte[]{(byte)n,0},bucket);
            var resume=new Keys(Buf.wrap(new byte[]{0}),Buf.wrap(new byte[]{2})); var page=f.read(null,null,false,resume,RangeBudget.DEFAULT);
            assertEquals(4,page.items().size()); assertTrue(page.hasMore()); assertEquals(1,f.gets);
            var last=f.calls.getLast(); assertEquals(right,last.group()); assertTrue(last.bytes()<PhysicalRangePage.MAX_BYTES);
            assertTrue(last.bytes()<=RatisLogicalRangePages.MAX_SCANNED_BYTES-bucket.length-4L*(bucket.length+8+2));
        }
    }
}
