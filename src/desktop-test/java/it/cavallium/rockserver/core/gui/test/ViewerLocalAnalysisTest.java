package it.cavallium.rockserver.core.gui.test;

import static org.junit.jupiter.api.Assertions.*;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.gui.*;
import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.FutureTask;
import javax.swing.*;
import org.junit.jupiter.api.Test;

class ViewerLocalAnalysisTest {
    @Test void histogramCountsExactlyTheLoadedRecordsAndIncludesKeyBytes() {
        var rows = List.of(new KV(new Keys(), null), new KV(new Keys(Buf.wrap(new byte[10])), Buf.wrap(new byte[21])),
                new KV(new Keys(Buf.wrap(new byte[32])), null), new KV(new Keys(Buf.wrap(new byte[4])), Buf.wrap(new byte[2000000])));
        var sizes = PageInsightsPanel.analyze(rows);
        assertEquals(4, sizes.rows());
        assertEquals(46, sizes.keyBytes());
        assertEquals(2000021, sizes.valueBytes());
        assertEquals(2000004, sizes.largest());
        assertEquals(List.of(1L, 1L, 1L, 0L, 0L, 0L, 0L, 1L), sizes.buckets());
    }

    @Test void wrappedNotesCannotDisplaceSchemaOrPropertyChartsOnFirstLayout() throws Exception {
        var task = new FutureTask<Void>(() -> {
            ViewerTheme.install();
            var schema = new SchemaPanel();
            schema.display("events", ColumnSchema.of(it.unimi.dsi.fastutil.ints.IntList.of(8, 4),
                    it.unimi.dsi.fastutil.objects.ObjectList.of(ColumnHashType.XXHASH32), true));
            var properties = new ColumnPropertiesPanel(new JTextArea());
            for (JPanel panel : List.of(schema, properties)) {
                panel.setSize(900, 600); panel.doLayout();
                var layout = (java.awt.BorderLayout) panel.getLayout();
                assertTrue(layout.getLayoutComponent(java.awt.BorderLayout.CENTER).getHeight() > 200);
                assertTrue(layout.getLayoutComponent(java.awt.BorderLayout.SOUTH).getHeight() <= 140);
            }
            return null;
        });
        SwingUtilities.invokeAndWait(task); task.get();
    }

    @Test void largeCellsHaveBoundedPreviewsAnd128BitNumbersUseOnly128Bits() throws Exception {
        byte[] bytes = new byte[8 * 1024 * 1024];
        java.util.Arrays.fill(bytes, (byte) 65);
        assertTrue(CellInterpreter.TEXT_UTF8.interpret(bytes).length() < 1100);
        assertTrue(CellInterpreter.BSON.interpret(bytes).contains("preview"));
        assertEquals(new BigInteger(1, java.util.Arrays.copyOf(bytes, 16)).toString(), CellInterpreter.NUM_UNSIGNED_BE_128.interpret(bytes));
        var task = new FutureTask<Void>(() -> {
            ViewerTheme.install();
            var panel = new CellDetailViewerPanel(); panel.displayCellData(bytes);
            for (String field : List.of("stringView", "hexView")) {
                var f = CellDetailViewerPanel.class.getDeclaredField(field); f.setAccessible(true);
                String text = ((JTextArea) f.get(panel)).getText();
                assertTrue(text.length() < 25000, field + " must not materialize the full 8 MiB cell");
                assertTrue(text.contains("preview limited"));
            }
            var f = CellDetailViewerPanel.class.getDeclaredField("currentBytes"); f.setAccessible(true);
            assertSame(bytes, f.get(panel), "Full bytes remain available for explicit export without another DB read");
            return null;
        });
        SwingUtilities.invokeAndWait(task); task.get();
    }
}
