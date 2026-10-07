package it.cavallium.rockserver.core.gui.test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;
import it.cavallium.rockserver.core.gui.*;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.buffer.Buf;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.List;
import java.util.concurrent.*;
import javax.imageio.ImageIO;
import javax.swing.*;
import org.junit.jupiter.api.*;

@Timeout(30)
class DataLayoutTest {
    private DbViewerUI frame;
    private RocksDBSyncAPI api;
    @BeforeEach void setup() throws Exception {
        Assumptions.assumeFalse(GraphicsEnvironment.isHeadless());
        var connection = mock(RocksDBConnection.class); api = mock(RocksDBSyncAPI.class);
        when(connection.getUrl()).thenReturn(URI.create("file:///demo/rockserver"));
        when(connection.getSyncApi(any())).thenReturn(api);
        var schema = ColumnSchema.of(IntList.of(8, 8), ObjectList.of(), true);
        when(api.getAllColumnDefinitions()).thenReturn(Map.of("messages", schema));
        when(api.getColumnId("messages")).thenReturn(7L);
        var rows = new ArrayList<KV>();
        for (int i = 1; i <= 250; i++) rows.add(new KV(new Keys(Buf.wrap(ByteBuffer.allocate(8).putLong(42).array()),
                Buf.wrap(ByteBuffer.allocate(8).putLong(i).array())), Buf.wrap(("{\"id\":" + i + ",\"text\":\"Deployment ready for review\",\"status\":\"delivered\",\"metadata\":{\"region\":\"eu\",\"synthetic\":true}}").getBytes(StandardCharsets.UTF_8))));
        doReturn(new RangePage<>(rows, rows.getLast().keys(), true)).when(api)
                .getRangePage(anyLong(), anyLong(), any(), any(), anyBoolean(), any(), any(), any());
        frame = edt(() -> { ViewerTheme.install(); var f = new DbViewerUI(connection); f.setVisible(true); return f; });
        idle();
        edt(() -> { ((JList<?>)field("tableList")).setSelectedIndex(0); return null; });
        idle();
        edt(() -> {
            JTable table = (JTable)field("dataTable"); table.setRowSelectionInterval(2, 2); table.setColumnSelectionInterval(2, 2);
            ((JComboBox<?>)field("decoder")).setSelectedItem(CellInterpreter.JSON);
            var inspector = (CellDetailViewerPanel)field("cellDetailViewer");
            var f = CellDetailViewerPanel.class.getDeclaredField("tabbedPane"); f.setAccessible(true);
            var tabs = (JTabbedPane)f.get(inspector); tabs.setSelectedIndex(tabs.indexOfTab("JSON"));
            return null;
        });
        clearInvocations(api);
    }
    @AfterEach void close() throws Exception { if (frame != null) edt(() -> { frame.dispose(); return null; }); }

    @Test void rightInspectorAndRecordsRemainUsefulAtBothWindowSizes() throws Exception {
        for (var size : List.of(new Dimension(1280, 850), new Dimension(1000, 720))) {
            edt(() -> { frame.setSize(size); frame.validate(); return null; });
            edt(() -> {
                JTable table = (JTable)field("dataTable");

                var inspector = (CellDetailViewerPanel)field("cellDetailViewer");
                var f = CellDetailViewerPanel.class.getDeclaredField("jsonView"); f.setAccessible(true);

                assertFalse(((JPanel)field("rangePanel")).isVisible());
                assertFalse(((JPanel)field("filterRow")).isVisible());
                String folder = System.getProperty("rockserver.data.screenshots");
                if (folder != null) {
                    var content = frame.getContentPane();
                    var image = new BufferedImage(content.getWidth(), content.getHeight(), BufferedImage.TYPE_INT_RGB);
                    var g = image.createGraphics(); try { content.paintAll(g); } finally { g.dispose(); }
                    ImageIO.write(image, "png", Path.of(folder, "rockserver-data-" + size.width + ".png").toFile());
                }
                assertTrue(table.getParent().getHeight() >= 280, "Grid viewport " + table.getParent().getHeight() + " at " + size);
                assertTrue(((JTextArea)f.get(inspector)).getParent().getHeight() >= 240, "Inspector viewport " + ((JTextArea)f.get(inspector)).getParent().getHeight() + " at " + size);
                return null;
            });
        }
        verifyNoInteractions(api);
    }

    @Test void dockingAndDisclosureAreLocalAndKeepInspectorPreferences() throws Exception {
        edt(() -> {
            ((JToggleButton)field("rangeToggle")).doClick(); assertTrue(((JPanel)field("rangePanel")).isVisible());
            ((JToggleButton)field("filterToggle")).doClick(); assertTrue(((JPanel)field("filterRow")).isVisible());
            ((JComboBox<?>)field("inspectorDock")).setSelectedItem("Below");
            assertEquals(JSplitPane.VERTICAL_SPLIT, ((JSplitPane)field("dataSplit")).getOrientation());
            ((JComboBox<?>)field("inspectorDock")).setSelectedItem("Hidden");
            assertNull(((JSplitPane)field("dataSplit")).getRightComponent());
            return null;
        });
        edt(() -> {
            JTable table = (JTable)field("dataTable");
            assertTrue(table.getParent().getWidth() > 850);
            ((JComboBox<?>)field("inspectorDock")).setSelectedItem("Right");
            var inspector = (CellDetailViewerPanel)field("cellDetailViewer");
            var f = CellDetailViewerPanel.class.getDeclaredField("tabbedPane"); f.setAccessible(true);
            var tabs = (JTabbedPane)f.get(inspector);
            assertEquals("JSON", tabs.getTitleAt(tabs.getSelectedIndex()));
            return null;
        });
        verifyNoInteractions(api);
    }

    private Object field(String name) throws Exception { var f = DbViewerUI.class.getDeclaredField(name); f.setAccessible(true); return f.get(frame); }
    private void idle() throws Exception { long end = System.nanoTime()+TimeUnit.SECONDS.toNanos(10); while(edt(() -> (boolean)field("busy"))) { if(System.nanoTime()>end)fail("Request stuck"); Thread.sleep(10); } }
    private static <T>T edt(Callable<T> action) throws Exception { var task=new FutureTask<>(action); SwingUtilities.invokeAndWait(task);return task.get(); }
}
