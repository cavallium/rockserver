package it.cavallium.rockserver.core.gui.test;

import static org.junit.jupiter.api.Assertions.*;
import it.cavallium.rockserver.core.gui.*;
import it.cavallium.rockserver.core.common.SstMaintenance;
import java.awt.event.MouseEvent;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import javax.swing.*;
import org.junit.jupiter.api.Test;

class SstExplorerPanelTest {
    static SstMaintenance.Metadata fixture() {
        return new SstMaintenance.Metadata("session", 7, "events", 7, 4, List.of("/nvme/rockserver", "/hdd/rockserver"), List.of(
                new SstMaintenance.File("000101.sst", 0, 0, 64L << 20, "01", "ff", true),
                new SstMaintenance.File("000102.sst", 0, 0, 128L << 20, "02", "ff", false),
                new SstMaintenance.File("000110.sst", 4, 0, 256L << 20, "01", "3f", false),
                new SstMaintenance.File("000111.sst", 4, 0, 256L << 20, "40", "7f", false),
                new SstMaintenance.File("000120.sst", 5, 1, 1L << 30, "01", "3f", false),
                new SstMaintenance.File("000121.sst", 5, 1, 1L << 30, "40", "7f", false),
                new SstMaintenance.File("000122.sst", 5, 1, 4L << 30, "80", "ff", false),
                new SstMaintenance.File("000130.sst", 6, 1, 8L << 30, "01", "7f", false),
                new SstMaintenance.File("000131.sst", 6, 1, 2L << 30, "80", "ff", false)));
    }

    @Test void filteringSortingAndMapSelectionUseTheSameFiles() throws Exception {
        edt(() -> {
            ViewerTheme.install();
            var panel = new SstExplorerPanel();
            panel.display(fixture());
            JTable table = (JTable) field(panel, "fileTable");
            assertEquals(9, table.getRowCount());
            table.getRowSorter().toggleSortOrder(3);
            assertEquals(64L << 20, table.getValueAt(0, 3));
            table.setRowSelectionInterval(0, 0);
            SstFileInspector details = (SstFileInspector) field(panel, "inspector");
            assertTrue(details.report().contains("000101.sst"));
            assertTrue(details.report().contains("/nvme/rockserver"));
            assertTrue(details.report().contains("Compacting at observation"));
            var path = (JComboBox<?>) field(panel, "pathFilter");
            path.setSelectedIndex(1);
            assertEquals(4, table.getRowCount());
            assertTrue(details.report().contains("000101.sst"));
            var level = (JComboBox<?>) field(panel, "levelFilter");
            level.setSelectedIndex(5); // L4
            assertEquals(2, table.getRowCount());
            var map = (JPanel) field(panel, "map");
            map.setSize(600, 200);
            map.dispatchEvent(new MouseEvent(map, MouseEvent.MOUSE_CLICKED, 0, 0, 10, 10, 1, false));
            assertTrue(details.report().contains("000110.sst"));
            assertTrue(details.report().contains("Smallest raw key (hex): 01"));
            level.setSelectedIndex(2); // empty L1
            assertEquals(0, table.getRowCount());
            assertTrue(details.report().contains("Filtered files: 0"));
            panel.clear();
            assertEquals(0, table.getRowCount());
            assertTrue(details.report().isEmpty());
            return null;
        });
    }

    @Test void refreshPreservesSelectionAndFiltersButDropsDisappearedFiles() throws Exception {
        edt(() -> {
            ViewerTheme.install();
            var panel = new SstExplorerPanel();
            panel.display(fixture());
            var path = (JComboBox<?>) field(panel, "pathFilter");
            path.setSelectedIndex(2);
            var table = (JTable) field(panel, "fileTable");
            table.setRowSelectionInterval(0, 0);
            var inspector = (SstFileInspector) field(panel, "inspector");
            assertTrue(inspector.report().contains("000130.sst"));
            panel.display(fixture());
            assertEquals(2, path.getSelectedIndex());
            assertTrue(inspector.report().contains("000130.sst"));
            var meta = fixture();
            panel.display(new SstMaintenance.Metadata(meta.session(), meta.columnId(), meta.columnName(), meta.numLevels(),
                    meta.baseLevel(), meta.paths(), meta.files().stream().filter(f -> !f.name().equals("000130.sst")).toList()));
            assertFalse(inspector.report().contains("000130.sst"));
            assertEquals(-1, table.getSelectedRow());
            return null;
        });
    }

    @Test void chartClickDrillsIntoTreemapAndKeyboardSelectsFiles() throws Exception {
        edt(() -> {
            ViewerTheme.install();
            var panel = new SstExplorerPanel(); panel.display(fixture());
            var views = (JTabbedPane) field(panel, "views"); views.setSelectedIndex(1);
            var levels = (JPanel) field(panel, "levels"); levels.setSize(600, 446);
            levels.dispatchEvent(new MouseEvent(levels, MouseEvent.MOUSE_CLICKED, 0, 0, 10, 40, 1, false));
            assertEquals(0, views.getSelectedIndex());
            assertEquals(1, ((JComboBox<?>) field(panel, "levelFilter")).getSelectedIndex());
            var map = (JPanel) field(panel, "map");
            map.getActionMap().get("select-" + java.awt.event.KeyEvent.VK_RIGHT).actionPerformed(null);
            assertTrue(((SstFileInspector) field(panel, "inspector")).report().contains("000102.sst"));
            return null;
        });
    }

    @Test void invalidObservationDoesNotReplaceExistingData() throws Exception {
        edt(() -> {
            ViewerTheme.install();
            var panel = new SstExplorerPanel();
            panel.display(fixture());
            var bad = new SstMaintenance.Metadata("session", 7, "events", 1, 0, List.of("/data"), fixture().files());
            assertThrows(IllegalArgumentException.class, () -> panel.display(bad));
            assertEquals(fixture(), field(panel, "metadata"));
            return null;
        });
    }

    @Test void overviewKeepsMissingMetadataDistinctFromEmptyColumns() throws Exception {
        edt(() -> {
            ViewerTheme.install();
            var panel = new SstColumnOverviewPanel(name -> {});
            panel.display(List.of(new SstColumnOverviewPanel.Column("empty", SstAnalysis.summarize(List.of()), null),
                    new SstColumnOverviewPanel.Column("missing", null, null)));
            var table = (JTable) field(panel, "table");
            assertEquals(0L, table.getValueAt(0, 2));
            assertNull(table.getValueAt(1, 2));
            assertEquals("Unavailable", table.getValueAt(1, 6));
            return null;
        });
    }

    private static Object field(Object target, String name) throws Exception {
        var f = target.getClass().getDeclaredField(name);
        f.setAccessible(true);
        return f.get(target);
    }
    private static <T> T edt(Callable<T> operation) throws Exception {
        var task = new FutureTask<>(operation);
        SwingUtilities.invokeAndWait(task);
        return task.get();
    }
}
