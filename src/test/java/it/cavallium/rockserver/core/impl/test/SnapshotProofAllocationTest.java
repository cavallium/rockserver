package it.cavallium.rockserver.core.impl.test;

import com.sun.management.ThreadMXBean;
import it.cavallium.rockserver.core.common.distributed.*;
import java.lang.management.ManagementFactory;
import java.security.MessageDigest;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class SnapshotProofAllocationTest {
    private static volatile int sink;

    private SnapshotFence fence() {
        var participants=new ArrayList<SnapshotFence.Participant>();
        for (int i=0;i<8;i++) {
            var group=new UUID(0,10+i); var key=new byte[8192]; Arrays.fill(key,(byte)i);
            participants.add(new SnapshotFence.Participant(new UUID(0,3), group, group, 1, new byte[32], new OperationId(group,1,1), new OperationId(group,1,2), new TransactionScope(List.of(TransactionScope.Span.point(new UUID(0,30),key)))));
        }
        return new SnapshotFence(new UUID(0,1), new UUID(0,2), new UUID(0,4), new UUID(0,5), 1, new OperationId(new UUID(0,4),1,0), List.of(new DomainBinding(new UUID(0,3), 2, new byte[32], 3, new byte[32])), participants);
    }

    private SnapshotFenceStatus terminal(SnapshotFence fence) {
        var status=SnapshotFenceStatus.begin(fence);
        for (var participant:fence.participants()) status=status.ready(participant.groupId(),new ApplyPosition(1,2));
        for (var participant:fence.participants()) status=status.capture(participant.groupId(),
                new SnapshotFenceStatus.Capture(participant.groupId(),new UUID(0,40),new ApplyPosition(1,3)));
        return status.complete().consume();
    }

    @Test void repeatedProofComparisonsAndDigestsDoNotAllocateDefinitionSizedBuffers() throws Exception {
        var a=fence(); var b=SnapshotFence.decode(a.encode());
        var first=terminal(a); var second=SnapshotFenceStatus.decode(first.encode());
        byte[] expected=MessageDigest.getInstance("SHA-256").digest(first.terminalDecision().encode());
        assertArrayEquals(expected,first.terminalDecision().digest());
        var threads=(ThreadMXBean)ManagementFactory.getThreadMXBean();
        assumeTrue(threads.isThreadAllocatedMemorySupported());
        if (!threads.isThreadAllocatedMemoryEnabled()) threads.setThreadAllocatedMemoryEnabled(true);
        for (int i=0;i<64;i++) compare(a,b,first,second);
        long threadId=Thread.currentThread().threadId(),before=threads.getThreadAllocatedBytes(threadId);
        for (int i=0;i<64;i++) compare(a,b,first,second);
        long allocated=threads.getThreadAllocatedBytes(threadId)-before;
        System.out.println("64 immutable snapshot proof checks allocated "+allocated+" bytes");
        assertTrue(allocated<128*1024,"Repeated immutable proof checks allocated "+allocated+" bytes");
        var digest=first.terminalDecision().digest(); digest[0]^=1;
        assertArrayEquals(expected,first.terminalDecision().digest());
    }

    private void compare(SnapshotFence a,SnapshotFence b,SnapshotFenceStatus first,SnapshotFenceStatus second) {
        if (!a.equals(b) || !first.equals(second) || !a.participants().getFirst().equals(b.participants().getFirst()))
            throw new AssertionError("Equivalent canonical proofs differ");
        sink=a.hashCode()^b.hashCode()^first.hashCode()^second.hashCode()
                ^a.participants().getFirst().hashCode()^b.participants().getFirst().hashCode()
                ^first.terminalDecision().digest()[0]^second.terminalDecision().digest()[0];
    }
    @Test void concurrentFirstObserversReceiveTheCanonicalDigestAndIndependentArrays() throws Exception {
        var status=terminal(fence());
        byte[] expected=MessageDigest.getInstance("SHA-256").digest(status.terminalDecision().encode());
        var start=new java.util.concurrent.CountDownLatch(1);
        try (var executor=java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var observers=new ArrayList<java.util.concurrent.Future<?>>();
            for (int i=0;i<16;i++) observers.add(executor.submit(() -> {
                start.await();
                for (int n=0;n<32;n++) {
                    byte[] digest=status.terminalDecision().digest(); assertArrayEquals(expected,digest); digest[0]^=1;
                }
                return null;
            }));
            start.countDown();
            for (var observer:observers) observer.get(5,java.util.concurrent.TimeUnit.SECONDS);
        }
        assertArrayEquals(expected,status.terminalDecision().digest());
    }
}
