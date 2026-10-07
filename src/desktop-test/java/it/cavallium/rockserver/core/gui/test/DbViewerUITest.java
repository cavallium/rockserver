package it.cavallium.rockserver.core.gui.test;

import it.cavallium.rockserver.core.gui.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.*;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.awt.GraphicsEnvironment;
import java.awt.image.BufferedImage;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import javax.imageio.ImageIO;
import javax.swing.*;
import org.junit.jupiter.api.*;

@Timeout(20)
class DbViewerUITest {
    private RocksDBConnection connection;
    private RocksDBSyncAPI api;
    private DbViewerUI viewer;
    private final ColumnSchema schema = ColumnSchema.of(IntList.of(1), ObjectList.of(), true);
    private final Keys firstKey = new Keys(Buf.wrap(new byte[]{1}));
    private final Keys secondKey = new Keys(Buf.wrap(new byte[]{2}));

    @BeforeEach void setup() throws Exception {
        Assumptions.assumeFalse(GraphicsEnvironment.isHeadless(), "Run with xvfb-run for Swing coverage");
        edt(() -> { ViewerTheme.install(); return null; });
        connection = mock(RocksDBConnection.class);
        api = mock(RocksDBSyncAPI.class);
        when(connection.getUrl()).thenReturn(URI.create("http://localhost:5333"));
        when(connection.getSyncApi(any())).thenReturn(api);
        when(api.getAllColumnDefinitions()).thenReturn(Map.of("events", schema, "users", schema));
        when(api.getColumnId("events")).thenReturn(7L);
        doReturn(new RangePage<>(List.of(new KV(firstKey, Buf.wrap(new byte[]{65}))), firstKey, true),
                new RangePage<>(List.of(new KV(secondKey, Buf.wrap(new byte[]{66}))), secondKey, false))
                .when(api).getRangePage(anyLong(), anyLong(), nullable(Keys.class), nullable(Keys.class),
                        anyBoolean(), nullable(Keys.class), any(), any());
        viewer = edt(() -> {
            var frame = new DbViewerUI(connection);
            frame.setVisible(true);
            return frame;
        });
        awaitIdle();
    }

    @AfterEach void cleanup() throws Exception {
        if (viewer != null) edt(() -> { viewer.dispose(); return null; });
    }

    @Test void navigatesExclusivePagesAndClearsHiddenSelection() throws Exception {
        selectEvents();
        awaitIdle();
        assertEquals(1, edt(() -> table().getRowCount()).intValue());
        assertTrue(edt(() -> button("nextButton").isEnabled()));
        assertFalse(edt(() -> button("previousButton").isEnabled()));
        edt(() -> { button("nextButton").doClick(); return null; });
        awaitIdle();
        verify(api).getRangePage(eq(0L), eq(7L), isNull(), isNull(), eq(false), eq(firstKey), any(),
                eq(new RangeBudget(250, RangeBudget.DEFAULT_MAX_BYTES)));
        assertFalse(edt(() -> button("nextButton").isEnabled()));
        assertTrue(edt(() -> button("previousButton").isEnabled()));
        assertArrayEquals(new byte[]{2}, edt(() -> (byte[]) table().getValueAt(0, 0)));
        edt(() -> { button("previousButton").doClick(); return null; });
        awaitIdle();
        verify(api, times(2)).getRangePage(eq(0L), eq(7L), isNull(), isNull(), eq(false), isNull(), any(), any());
        edt(() -> { ((JTextField) field("tableFilterField")).setText("users"); return null; });
        assertEquals(0, edt(() -> table().getRowCount()).intValue());
        assertFalse(edt(() -> button("nextButton").isEnabled()));
    }

    @Test void metadataIsExplicitAndRenderedWithPhysicalEntrySemantics() throws Exception {
        selectEvents();
        awaitIdle();
        verify(api, never()).getTableProperties(anyLong());
        var properties = mock(ColumnTableProperties.class);
        when(properties.tableCount()).thenReturn(12L);
        when(properties.numEntries()).thenReturn(900L);
        when(properties.compressions()).thenReturn(Map.of("ZSTD", 12L));
        when(api.getTableProperties(7)).thenReturn(properties);
        edt(() -> { button("storageButton").doClick(); return null; });
        awaitIdle();
        String text = edt(() -> ((JTextArea) field("storageView")).getText());
        assertTrue(text.contains("900"));
        assertTrue(text.contains("not live row counts"));
        assertTrue(text.contains("ZSTD"));
        String screenshot = System.getProperty("rockserver.viewer.screenshot");
        if (screenshot != null) edt(() -> {
            var image = new BufferedImage(viewer.getContentPane().getWidth(), viewer.getContentPane().getHeight(), BufferedImage.TYPE_INT_RGB);
            var graphics = image.createGraphics();
            try { viewer.getContentPane().printAll(graphics); } finally { graphics.dispose(); }
            ImageIO.write(image, "png", Path.of(screenshot).toFile());
            return null;
        });
    }

    @Test void sstLayoutReadsMetadataWithoutScanningRowsAndRendersLinkedViews() throws Exception {
        selectEvents();
        awaitIdle();
        clearInvocations(api, connection);
        when(api.getSstMetadata(7, -1)).thenReturn(SstExplorerPanelTest.fixture());
        edt(() -> {
            ((JTabbedPane) field("tabs")).setSelectedIndex(2);
            button("sstButton").doClick();
            return null;
        });
        awaitIdle();
        verify(api).getSstMetadata(7, -1);
        verify(connection).getSyncApi(RequestContext.batch(java.time.Duration.ofSeconds(30)));
        verify(api, never()).getRangePage(anyLong(), anyLong(), any(), any(), anyBoolean(), any(), any(), any());
        verify(api, never()).getTableProperties(anyLong());
        String screenshot = System.getProperty("rockserver.sst.screenshot");
        if (screenshot != null) edt(() -> {
            var tableField = SstExplorerPanel.class.getDeclaredField("fileTable");
            tableField.setAccessible(true);
            ((JTable) tableField.get(field("sstExplorer"))).setRowSelectionInterval(0, 0);
            var image = new BufferedImage(viewer.getContentPane().getWidth(), viewer.getContentPane().getHeight(), BufferedImage.TYPE_INT_RGB);
            var graphics = image.createGraphics();
            try { viewer.getContentPane().printAll(graphics); } finally { graphics.dispose(); }
            ImageIO.write(image, "png", Path.of(screenshot).toFile());
            var viewsField = SstExplorerPanel.class.getDeclaredField("views");
            viewsField.setAccessible(true);
            var views = (JTabbedPane) viewsField.get(field("sstExplorer"));
            for (int i = 1; i <= 3; i++) {
                views.setSelectedIndex(i);
                viewer.validate();
                graphics = image.createGraphics();
                try { viewer.getContentPane().printAll(graphics); } finally { graphics.dispose(); }
                ImageIO.write(image, "png", Path.of(screenshot.replace(".png", "-" + i + ".png")).toFile());
            }
            return null;
        });
    }

    @Test void overviewIncludesEveryColumnAndRetainsPartialFailures() throws Exception {
        when(api.getSstMetadata(7, -1)).thenReturn(SstExplorerPanelTest.fixture());
        when(api.getColumnId("users")).thenReturn(8L);
        when(api.getSstMetadata(8, -1)).thenThrow(new IllegalStateException("Column was dropped"));
        edt(() -> { button("columnSizesButton").doClick(); return null; });
        awaitIdle();
        verify(api).getSstMetadata(7, -1);
        verify(api).getSstMetadata(8, -1);
        assertEquals(4, edt(() -> ((JTabbedPane) field("tabs")).getSelectedIndex()).intValue());
        verify(api, never()).getRangePage(anyLong(), anyLong(), any(), any(), anyBoolean(), any(), any(), any());
    }

    @Test void storageOnlySelectionDoesNotFetchLogicalRows() throws Exception {
        when(api.getSstMetadata(7, -1)).thenReturn(SstExplorerPanelTest.fixture());
        edt(() -> { ((JTabbedPane) field("tabs")).setSelectedIndex(2); return null; });
        selectEvents();
        awaitIdle();
        verify(api).getSstMetadata(7, -1);
        verify(connection).getSyncApi(RequestContext.batch(java.time.Duration.ofSeconds(30)));
        verify(api, never()).getRangePage(anyLong(), anyLong(), any(), any(), anyBoolean(), any(), any(), any());
    }

    @Test void stoppingComparisonFinishesCurrentRequestAndSkipsRemainingColumns() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        when(api.getSstMetadata(7, -1)).thenAnswer(invocation -> {
            entered.countDown();
            assertTrue(release.await(10, TimeUnit.SECONDS));
            return SstExplorerPanelTest.fixture();
        });
        try {
            edt(() -> { button("columnSizesButton").doClick(); return null; });
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            edt(() -> { button("stopComparisonButton").doClick(); return null; });
            verify(api, never()).getColumnId("users");
        } finally { release.countDown(); }
        awaitIdle();
        verify(api, never()).getColumnId("users");
        String summary = edt(() -> ((JLabel) ((JPanel) field("columnOverview")).getComponent(0)).getText());
        assertTrue(summary.contains("1 not inspected"));
        assertTrue(edt(() -> ((JLabel) field("statusLabel")).getText()).contains("partial results"));
    }

    @Test void localViewsDoNotIssueAdditionalDatabaseRequests() throws Exception {
        selectEvents(); awaitIdle(); clearInvocations(api);
        edt(() -> {
            ((JTabbedPane) field("tabs")).setSelectedIndex(1); // visual schema
            button("pageSizesButton").doClick();
            ((JTabbedPane) field("tabs")).setSelectedIndex(0);
            table().setRowSelectionInterval(0, 0); table().setColumnSelectionInterval(0, 0);
            ((JComboBox<?>) field("decoder")).setSelectedItem(CellInterpreter.TEXT_UTF8);
            return null;
        });
        verifyNoInteractions(api);
    }

    @Test void failedMetadataRequestKeepsUiUsableWithoutModalDialog() throws Exception {
        when(api.getSstMetadata(7, -1)).thenThrow(new IllegalStateException("Server busy"));
        edt(() -> { ((JTabbedPane) field("tabs")).setSelectedIndex(2); return null; });
        selectEvents(); awaitIdle();
        assertTrue(edt(() -> button("errorDetails").isVisible()));
        assertTrue(edt(() -> button("sstButton").isEnabled()));
        assertTrue(edt(() -> ((JLabel) field("statusLabel")).getText()).contains("Server busy"));
    }

    @Test void decoderChoiceSurvivesColumnNavigation() throws Exception {
        selectEvents(); awaitIdle();
        edt(() -> {
            table().setRowSelectionInterval(0,0); table().setColumnSelectionInterval(1,1);
            ((JComboBox<?>)field("decoder")).setSelectedItem(CellInterpreter.JSON);
            ((JList<?>)field("tableList")).setSelectedIndex(1);
            return null;
        });
        awaitIdle(); selectEvents(); awaitIdle();
        edt(() -> {
            table().setRowSelectionInterval(0,0);table().setColumnSelectionInterval(1,1);
            assertEquals(CellInterpreter.JSON,((JComboBox<?>)field("decoder")).getSelectedItem());
            return null;
        });
    }

    @Test void closingDrainsInFlightRequestBeforeClosingConnectionExactlyOnce() throws Exception {
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        doAnswer(invocation -> {
            started.countDown();
            assertTrue(release.await(10, TimeUnit.SECONDS));
            return RangePage.empty();
        }).when(api).getRangePage(anyLong(), anyLong(), nullable(Keys.class), nullable(Keys.class),
                anyBoolean(), nullable(Keys.class), any(), any());
        try {
            selectEvents();
            assertTrue(started.await(5, TimeUnit.SECONDS));
            edt(() -> { viewer.dispose(); viewer.dispose(); return null; });
            verify(connection, never()).close();
        } finally { release.countDown(); }
        verify(connection, timeout(5000).times(1)).close();
        edt(() -> { viewer.dispose(); return null; });
        verify(connection, times(1)).close();
    }

    private void selectEvents() throws Exception {
        edt(() -> { ((JList<?>) field("tableList")).setSelectedIndex(0); return null; });
    }

    private JTable table() throws Exception { return (JTable) field("dataTable"); }
    private JButton button(String name) throws Exception { return (JButton) field(name); }
    private Object field(String name) throws Exception {
        var field = DbViewerUI.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(viewer);
    }

    private void awaitIdle() throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (edt(() -> (boolean) field("busy"))) {
            if (System.nanoTime() > deadline) fail("Viewer did not become idle");
            Thread.sleep(10);
        }
    }

    private static <T> T edt(Callable<T> operation) throws Exception {
        var task = new FutureTask<>(operation);
        SwingUtilities.invokeAndWait(task);
        return task.get();
    }
}
