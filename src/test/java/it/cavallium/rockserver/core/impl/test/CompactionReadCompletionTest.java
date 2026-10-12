package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.impl.EmbeddedDB;
import org.junit.jupiter.api.Test;
import org.rocksdb.Statistics;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CompactionReadCompletionTest {
    private long[] sample(Statistics statistics) throws Exception {
        var method = EmbeddedDB.class.getDeclaredMethod("sampleCompactionReadCompletion", Statistics.class);
        method.setAccessible(true);
        var result = method.invoke(null, statistics);
        var values = new long[3];
        String[] names = {"bytes", "micros", "count"};
        for (int i = 0; i < names.length; i++) {
            var accessor = result.getClass().getDeclaredMethod(names[i]); accessor.setAccessible(true);
            values[i] = (long) accessor.invoke(result);
        }
        return values;
    }
    @Test void atomicPopulationExcludesJobProgressAndSubmissionTiming() throws Exception {
        var statistics = mock(Statistics.class);
        when(statistics.getCompactionReadCompletion()).thenReturn(new long[]{64, 4L << 20, 250000});
        assertArrayEquals(new long[]{4L << 20, 250000, 64}, sample(statistics));
        verify(statistics).getCompactionReadCompletion();
        verifyNoMoreInteractions(statistics);
    }
    @Test void changingLegacyHistogramsCannotInvalidateAnAtomicCompletion() throws Exception {
        var statistics = mock(Statistics.class);
        when(statistics.getHistogramData(any())).thenThrow(new AssertionError("separate histogram sampling"));
        when(statistics.getCompactionReadCompletion()).thenReturn(new long[]{64, 4L << 20, 250000});
        assertArrayEquals(new long[]{4L << 20, 250000, 64}, sample(statistics));
        verify(statistics, never()).getHistogramData(any());
    }
    @Test void unavailableAndMalformedTuplesFailClosedWithoutRetry() throws Exception {
        for (var invalid : new long[][]{null, {}, {0, 0}, {0, 0, 0, 0},
                {-1, 0, 0}, {1, -1, 0}, {1, 0, -1}, {0, 1, 0}, {0, 0, 1}}) {
            var statistics = mock(Statistics.class);
            when(statistics.getCompactionReadCompletion()).thenReturn(invalid);
            assertArrayEquals(new long[]{-1, -1, -1}, sample(statistics));
            verify(statistics).getCompactionReadCompletion();
            verifyNoMoreInteractions(statistics);
        }
    }
    @Test void telemetryErrorsRemainUnavailable() throws Exception {
        var statistics = mock(Statistics.class);
        when(statistics.getCompactionReadCompletion()).thenThrow(new IllegalStateException("unavailable"));
        assertArrayEquals(new long[]{-1, -1, -1}, sample(statistics));
    }
    @Test void unavailableThenValidUsesTheNextAtomicPopulation() throws Exception {
        var statistics = mock(Statistics.class);
        when(statistics.getCompactionReadCompletion()).thenReturn(null, new long[]{64, 4L << 20, 250000});
        assertArrayEquals(new long[]{-1, -1, -1}, sample(statistics));
        assertArrayEquals(new long[]{4L << 20, 250000, 64}, sample(statistics));
    }
    @Test void quietAndResetPopulationsRemainCumulativeInputs() throws Exception {
        var statistics = mock(Statistics.class);
        when(statistics.getCompactionReadCompletion()).thenReturn(new long[]{64, 4L << 20, 250000});
        assertArrayEquals(sample(statistics), sample(statistics));
        when(statistics.getCompactionReadCompletion()).thenReturn(new long[]{0, 0, 0});
        assertArrayEquals(new long[]{0, 0, 0}, sample(statistics));
        verify(statistics, never()).reset();
    }
    @Test void zeroByteAndSubMicrosecondCompletionsRemainValid() throws Exception {
        var statistics = mock(Statistics.class);
        when(statistics.getCompactionReadCompletion()).thenReturn(new long[]{1, 0, 0});
        assertArrayEquals(new long[]{0, 0, 1}, sample(statistics));
    }
}
