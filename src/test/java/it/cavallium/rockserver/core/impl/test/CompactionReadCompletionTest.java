package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.impl.EmbeddedDB;
import org.junit.jupiter.api.Test;
import org.rocksdb.HistogramData;
import org.rocksdb.HistogramType;
import org.rocksdb.Statistics;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CompactionReadCompletionTest {
    private static final HistogramType BYTES = HistogramType.FILE_READ_COMPACTION_COMPLETED_BYTES;
    private static final HistogramType MICROS = HistogramType.FILE_READ_COMPACTION_COMPLETED_MICROS;

    private HistogramData histogram(long count, long sum) {
        return new HistogramData(0, 0, 0, 0, 0, 0, count, sum, 0);
    }
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
    @Test void completedPopulationExcludesJobProgressAndSubmissionTiming() throws Exception {
        var statistics = mock(Statistics.class);
        when(statistics.getHistogramData(BYTES)).thenReturn(histogram(64, 4L << 20));
        when(statistics.getHistogramData(MICROS)).thenReturn(histogram(64, 250000));
        assertArrayEquals(new long[]{4L << 20, 250000, 64}, sample(statistics));
        verify(statistics, never()).getTickerCount(any());
        verify(statistics, never()).getHistogramData(HistogramType.FILE_READ_COMPACTION_MICROS);
    }
    @Test void publicationBetweenSnapshotsRetriesTheWholePopulation() throws Exception {
        var statistics = mock(Statistics.class);
        when(statistics.getHistogramData(BYTES)).thenReturn(histogram(63, 63L << 16), histogram(64, 4L << 20), histogram(64, 4L << 20));
        when(statistics.getHistogramData(MICROS)).thenReturn(histogram(63, 246000), histogram(64, 250000), histogram(64, 250000));
        assertArrayEquals(new long[]{4L << 20, 250000, 64}, sample(statistics));
        verify(statistics, times(4)).getHistogramData(BYTES);
        verify(statistics, times(4)).getHistogramData(MICROS);
    }
    @Test void countBeforeSumPublicationCannotMixChangingTimeSums() throws Exception {
        var statistics = mock(Statistics.class);
        when(statistics.getHistogramData(BYTES)).thenReturn(histogram(64, 4L << 20));
        when(statistics.getHistogramData(MICROS)).thenReturn(histogram(64, 246000), histogram(64, 250000), histogram(64, 250000));
        assertArrayEquals(new long[]{4L << 20, 250000, 64}, sample(statistics));
        verify(statistics, times(4)).getHistogramData(MICROS);
    }
    @Test void resetBetweenSnapshotsDiscardsOldEpoch() throws Exception {
        var statistics = mock(Statistics.class);
        when(statistics.getHistogramData(BYTES)).thenReturn(histogram(64, 4L << 20), histogram(0, 0), histogram(0, 0));
        when(statistics.getHistogramData(MICROS)).thenReturn(histogram(0, 0));
        assertArrayEquals(new long[]{0, 0, 0}, sample(statistics));
        verify(statistics, times(4)).getHistogramData(BYTES);
    }
    @Test void persistentMismatchIsBoundedAndUnavailable() throws Exception {
        var statistics = mock(Statistics.class);
        when(statistics.getHistogramData(BYTES)).thenReturn(histogram(64, 4L << 20));
        when(statistics.getHistogramData(MICROS)).thenReturn(histogram(63, 246000));
        assertArrayEquals(new long[]{-1, -1, -1}, sample(statistics));
        verify(statistics, times(6)).getHistogramData(BYTES);
        verify(statistics, times(6)).getHistogramData(MICROS);
    }
    @Test void missingErrorsAndInvalidCountersAreUnavailable() throws Exception {
        for (var invalid : new HistogramData[]{null, histogram(-1, 0), histogram(1, -1), histogram(0, 1)}) {
            var statistics = mock(Statistics.class);
            when(statistics.getHistogramData(BYTES)).thenReturn(invalid);
            when(statistics.getHistogramData(MICROS)).thenReturn(histogram(0, 0));
            assertArrayEquals(new long[]{-1, -1, -1}, sample(statistics));
        }
        var statistics = mock(Statistics.class);
        when(statistics.getHistogramData(BYTES)).thenThrow(new IllegalStateException("unavailable"));
        assertArrayEquals(new long[]{-1, -1, -1}, sample(statistics));
    }
    @Test void quietAndResetPopulationsRemainCumulativeInputs() throws Exception {
        var statistics = mock(Statistics.class);
        when(statistics.getHistogramData(BYTES)).thenReturn(histogram(64, 4L << 20));
        when(statistics.getHistogramData(MICROS)).thenReturn(histogram(64, 250000));
        assertArrayEquals(sample(statistics), sample(statistics));
        when(statistics.getHistogramData(BYTES)).thenReturn(histogram(0, 0));
        when(statistics.getHistogramData(MICROS)).thenReturn(histogram(0, 0));
        assertArrayEquals(new long[]{0, 0, 0}, sample(statistics));
        verify(statistics, never()).reset();
    }
}
