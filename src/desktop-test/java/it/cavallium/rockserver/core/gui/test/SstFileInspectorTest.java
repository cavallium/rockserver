package it.cavallium.rockserver.core.gui.test;

import static org.junit.jupiter.api.Assertions.*;
import it.cavallium.rockserver.core.common.SstMaintenance;
import it.cavallium.rockserver.core.gui.SstFileInspector;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class SstFileInspectorTest {
    @Test void detailsUseTheFullObservationForSharesRankAndRangeIntersections() {
        var file = new SstMaintenance.File("000001.sst", 0, 0, 100, "01", "7f", true);
        var metadata = new SstMaintenance.Metadata("session", 7, "events", 3, 1, List.of("/hot", "/cold"), List.of(
                file, new SstMaintenance.File("000002.sst", 0, 1, 50, "40", "ff", false),
                new SstMaintenance.File("000003.sst", 0, 0, 50, "80", "ff", false),
                new SstMaintenance.File("000004.sst", 1, 1, 200, "01", "ff", false)));
        var details = SstFileInspector.describe(metadata, file);
        var values = details.facts().stream().collect(Collectors.toMap(SstFileInspector.Fact::name, SstFileInspector.Fact::value));
        assertEquals("/hot/000001.sst", values.get("File path"));
        assertEquals("25.00%", values.get("Share of column"));
        assertEquals("50.00%", values.get("Share of level"));
        assertEquals("66.67%", values.get("Share of storage path"));
        assertEquals("2.00×", values.get("Size / level median"));
        assertTrue(values.get("Size rank in level").startsWith("1 of 3"));
        assertEquals("1 other SSTs", values.get("Same-level range intersections"));
        assertEquals("000002.sst", values.get("Intersecting files"));
        assertEquals("Compacting at observation", values.get("Compaction"));
        assertTrue(details.report().contains("Smallest raw key (hex): 01"));
    }

    @Test void zeroSizesDoNotFabricatePercentagesAndTiesShareRank() {
        var file = new SstMaintenance.File("000001.sst", 1, 0, 0, "01", "02", false);
        var m = new SstMaintenance.Metadata("session", 7, "events", 2, 1, List.of("/data/"), List.of(file));
        var values = SstFileInspector.describe(m, file).facts().stream().collect(Collectors.toMap(SstFileInspector.Fact::name, SstFileInspector.Fact::value));
        assertEquals("n/a (zero total)", values.get("Share of column"));
        assertEquals("n/a (zero median)", values.get("Size / level median"));
        assertEquals("/data/000001.sst", values.get("File path"));
        assertEquals("None", values.get("Intersecting files"));
    }
}
