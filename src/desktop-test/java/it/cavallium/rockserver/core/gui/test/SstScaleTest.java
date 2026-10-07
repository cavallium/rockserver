package it.cavallium.rockserver.core.gui.test;

import static org.junit.jupiter.api.Assertions.*;
import it.cavallium.rockserver.core.gui.*;
import it.cavallium.rockserver.core.common.SstMaintenance;
import java.awt.event.MouseEvent;
import java.awt.image.BufferedImage;
import java.util.*;
import java.util.concurrent.FutureTask;
import javax.swing.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class SstScaleTest {
    static SstMaintenance.Metadata fixture(int count, boolean singleGroup) {
        var files = new ArrayList<SstMaintenance.File>();
        for (int i = 0; i < count; i++) {
            files.add(new SstMaintenance.File(String.format(Locale.ROOT, "%06d.sst", i), singleGroup ? 6 : i % 7,
                    singleGroup ? 0 : i % 3, (i % 1024 + 1L) * 1048576,
                    String.format(Locale.ROOT, "%08x", i * 16), String.format(Locale.ROOT, "%08x", i * 16 + 15), i % 100 == 0));
        }
        return new SstMaintenance.Metadata("scale", 7, "large-column", 7, 4, List.of("/nvme/a", "/hdd/a", "/hdd/b"), files);
    }

    @Test void hierarchicalAggregationPreservesEveryFileAndByteEvenWithinOneLevelAndPath() {
        for (boolean singleGroup : List.of(false, true)) {
            var index = SstAnalysis.index(fixture(25000, singleGroup));
            assertEquals(SstAnalysis.summarize(index.metadata().files()), index.total());
            var seen = new HashSet<String>();
            walk(index.bySize(), seen, 0);
            assertEquals(25000, seen.size());
            var filtered = index.bySize().stream().filter(f -> f.name().contains("001")).toList();
            assertEquals(SstAnalysis.summarize(filtered), SstAnalysis.summarizeInSizeOrder(filtered));
        }
        for (int n : new int[]{0, 1, 64, 255, 256, 257}) {
            var index = SstAnalysis.index(fixture(n, true));
            var seen = new HashSet<String>(); walk(index.bySize(), seen, 0);
            assertEquals(n, seen.size());
        }
    }

    private static void walk(List<SstMaintenance.File> files, Set<String> seen, int depth) {
        assertTrue(depth < 10, "Every group must eventually reach individual files");
        var tiles = SstAnalysis.tiles(files);
        assertTrue(tiles.size() <= SstAnalysis.MAX_MAP_TILES);
        assertEquals(files.size(), tiles.stream().mapToInt(t -> t.files().size()).sum());
        assertEquals(SstAnalysis.summarize(files).bytes(), tiles.stream().mapToLong(SstAnalysis.Tile::bytes).sum());
        for (var tile : tiles) {
            if (tile.files().size() == 1) assertTrue(seen.add(tile.files().getFirst().name()), "No duplicate files");
            else {
                assertTrue(tile.files().size() < files.size());
                walk(tile.files(), seen, depth + 1);
            }
        }
    }

    @Test void largeInventoryRemainsCompleteWhileMapZoomsAndRestoresScope() throws Exception {
        var metadata = fixture(25000, false);
        var task = new FutureTask<Void>(() -> {
            ViewerTheme.install();
            var panel = new SstExplorerPanel(); panel.display(metadata);
            var table = (JTable) field(panel, "fileTable");
            assertEquals(25000, table.getRowCount());
            var map = (JPanel) field(panel, "map"); map.setSize(900, 400);
            var image = new BufferedImage(900, 400, BufferedImage.TYPE_INT_RGB);
            var graphics = image.createGraphics();
            try { map.paint(graphics); } finally { graphics.dispose(); }
            assertEquals(7, ((List<?>) field(map, "entries")).size());
            map.dispatchEvent(new MouseEvent(map, MouseEvent.MOUSE_CLICKED, 0, 0, 10, 10, 1, false));
            assertTrue(((List<?>) field(map, "entries")).size() <= 256);
            assertFalse(((Deque<?>) field(map, "history")).isEmpty());
            assertEquals(25000, table.getRowCount(), "Zoom is not inventory truncation");
            ((JButton) field(map, "back")).doClick();
            assertTrue(((Deque<?>) field(map, "history")).isEmpty());
            assertEquals(7, ((List<?>) field(map, "entries")).size());
            table.setRowSelectionInterval(24999, 24999);
            var inspector = (SstFileInspector) field(panel, "inspector");
            assertTrue(inspector.report().contains((String) table.getValueAt(24999, 0)));
            ((JTextField) field(panel, "fileFilter")).setText("000000.sst");
            assertEquals(1, table.getRowCount());
            assertEquals("000000.sst", table.getValueAt(0, 0));
            return null;
        });
        SwingUtilities.invokeAndWait(task); task.get();
    }

    private static Object field(Object target, String name) throws Exception {
        var field = target.getClass().getDeclaredField(name); field.setAccessible(true); return field.get(target);
    }
}
