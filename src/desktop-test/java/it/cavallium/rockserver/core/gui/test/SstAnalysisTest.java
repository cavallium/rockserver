package it.cavallium.rockserver.core.gui.test;

import static org.junit.jupiter.api.Assertions.*;
import it.cavallium.rockserver.core.gui.SstAnalysis;
import it.cavallium.rockserver.core.common.SstMaintenance;
import java.util.*;
import org.junit.jupiter.api.Test;

class SstAnalysisTest {
    private static SstMaintenance.File file(int level, long size) {
        return new SstMaintenance.File(size + ".sst", level, 0, size, "00", "ff", level == 0);
    }

    @Test void summarizesLongSizesAndMedianWithoutOverflow() {
        var s = SstAnalysis.summarize(List.of(file(0, 1L << 33), file(1, 1L << 34), file(1, 1L << 35), file(1, 1L << 36)));
        assertEquals(15L << 33, s.bytes());
        assertEquals(3L << 33, s.median());
        assertEquals(1L << 36, s.largest());
        assertEquals(1, s.compacting());
        assertEquals(8d / 3, s.largestToMedian());
        assertThrows(ArithmeticException.class, () -> SstAnalysis.summarize(List.of(file(0, Long.MAX_VALUE), file(1, 1))));
        assertThrows(IllegalArgumentException.class, () -> SstAnalysis.summarize(List.of(file(0, -1))));
    }

    @Test void preservesEmptyGroupsAndRejectsUnmappedFiles() {
        var levels = SstAnalysis.grouped(List.of(file(0, 100), file(2, 500)), 4, SstMaintenance.File::level);
        assertEquals(List.of(100L, 0L, 500L, 0L), levels.stream().map(SstAnalysis.Stats::bytes).toList());
        assertEquals(0, levels.get(1).files());
        assertTrue(Double.isNaN(levels.get(1).largestToMedian()));
        assertThrows(IllegalArgumentException.class, () -> SstAnalysis.grouped(List.of(file(4, 1)), 4, SstMaintenance.File::level));
    }

    @Test void treemapAreasMatchSizesWithoutOverlapsOrPaddingDistortion() {
        var random = new Random(184);
        var weights = new ArrayList<Long>();
        for (int i = 0; i < 300; i++) weights.add((long) random.nextInt(10000));
        weights.add(0L);
        double total = weights.stream().mapToDouble(Long::doubleValue).sum();
        var rectangles = SstAnalysis.treemap(weights, 1100, 400);
        for (int i = 0; i < rectangles.size(); i++) {
            var r = rectangles.get(i);
            assertEquals(1100 * 400 * weights.get(i) / total, r.width * r.height, 1e-7);
            assertTrue(r.x >= 0 && r.y >= 0 && r.getMaxX() <= 1100.000001 && r.getMaxY() <= 400.000001);
            for (int j = 0; j < i; j++) {
                var intersection = r.createIntersection(rectangles.get(j));
                assertTrue(intersection.isEmpty() || intersection.getWidth() * intersection.getHeight() < 1e-7);
            }
        }
    }

    @Test void extremeSkewAndManySubpixelFilesDoNotOverflowTheStack() {
        var weights = new ArrayList<Long>(Collections.nCopies(65_536, 1L));
        weights.set(0, Long.MAX_VALUE);
        var rectangles = SstAnalysis.treemap(weights, 800, 300);
        assertEquals(weights.size(), rectangles.size());
        assertTrue(rectangles.stream().allMatch(r -> Double.isFinite(r.width) && r.width >= 0 && r.height >= 0));
    }

    @Test void emptyAndZeroSizesDoNotInventStorage() {
        assertTrue(SstAnalysis.treemap(List.of(), 100, 100).isEmpty());
        assertTrue(SstAnalysis.treemap(List.of(0L, 0L), 100, 100).stream().allMatch(r -> r.isEmpty()));
        assertTrue(SstAnalysis.treemap(List.of(1L), 0, 100).getFirst().isEmpty());
        assertThrows(IllegalArgumentException.class, () -> SstAnalysis.treemap(List.of(-1L), 100, 100));
        assertEquals("1.0 TiB", SstAnalysis.size(1L << 40));
    }
}
