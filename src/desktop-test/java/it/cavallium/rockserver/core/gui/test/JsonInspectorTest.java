package it.cavallium.rockserver.core.gui.test;

import static org.junit.jupiter.api.Assertions.*;
import it.cavallium.rockserver.core.gui.*;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.FutureTask;
import javax.swing.*;
import org.junit.jupiter.api.Test;

class JsonInspectorTest {
    private static byte[] bytes(String s) { return s.getBytes(StandardCharsets.UTF_8); }

    @Test void formatsObjectsArraysAndPrimitivesWithoutRoundingNumbers() {
        String source="{\"large\":18446744073709551615,\"decimal\":0.123456789012345678901,\"items\":[true,null,\"雪\"]}";
        var preview=JsonPreview.format(bytes(source),true);
        assertTrue(preview.valid());
        assertTrue(preview.text().contains("\n"));
        assertTrue(preview.text().contains("18446744073709551615"));
        assertTrue(preview.text().contains("0.123456789012345678901"));
        for(String value:new String[]{"[1,2,3]","\"text\"","true","null","-0","1e123"})
            assertEquals(value,JsonPreview.format(bytes(value),false).text());
        assertEquals(source,CellInterpreter.JSON.interpret(bytes(source)));
    }

    @Test void rejectsMalformedTrailingOversizedAndOverlyDeepDocuments() {
        for(String invalid:new String[]{"", "{", "{unquoted:1}","[1,]", "{} {}", "true false"})
            assertFalse(JsonPreview.format(bytes(invalid),true).valid(),invalid);
        assertFalse(JsonPreview.format(new byte[JsonPreview.MAX_BYTES+1],true).valid());
        assertFalse(JsonPreview.format(bytes("[".repeat(65)+"0"+"]".repeat(65)),true).valid());
        assertFalse(JsonPreview.format(bytes("["+"0,".repeat(5000)+"0]"),true).valid());
    }

    @Test void specificRecordOverridesCoexistWithFieldDefaults() throws Exception {
        var task = new FutureTask<Void>(() -> {
            ViewerTheme.install(); var panel = new CellDetailViewerPanel();
            var schema = ColumnSchema.of(IntList.of(8), ObjectList.of(), true);
            var a = new CellDetailViewerPanel.CellContext("mixed", schema, 1, "record-a");
            var b = new CellDetailViewerPanel.CellContext("mixed", schema, 1, "record-b");
            var c = new CellDetailViewerPanel.CellContext("mixed", schema, 1, "record-c");
            var field = CellDetailViewerPanel.class.getDeclaredField("tabbedPane"); field.setAccessible(true);
            var tabs = (JTabbedPane) field.get(panel);
            panel.displayCellData(bytes("{}"), a); tabs.setSelectedIndex(tabs.indexOfTab("JSON"));
            panel.displayCellData(bytes("{}"), b); assertEquals("JSON", tabs.getTitleAt(tabs.getSelectedIndex()));
            tabs.setSelectedIndex(tabs.indexOfTab("BSON"));
            panel.displayCellData(bytes("{}"), a); assertEquals("JSON", tabs.getTitleAt(tabs.getSelectedIndex()));
            panel.displayCellData(bytes("{}"), b); assertEquals("BSON", tabs.getTitleAt(tabs.getSelectedIndex()));
            panel.displayCellData(bytes("{}"), c); assertEquals("BSON", tabs.getTitleAt(tabs.getSelectedIndex()));
            return null;
        });
        SwingUtilities.invokeAndWait(task); task.get();
    }

    @Test void tabsAreRememberedByDatabaseColumnSchemaAndKeyOrValueComponent() throws Exception {
        var task=new FutureTask<Void>(() -> {
            ViewerTheme.install();
            var panel=new CellDetailViewerPanel();
            var schema=ColumnSchema.of(IntList.of(8),ObjectList.of(),true);
            var key=new CellDetailViewerPanel.CellContext("messages",schema,0);
            var value=new CellDetailViewerPanel.CellContext("messages",schema,1);
            var other=new CellDetailViewerPanel.CellContext("users",schema,1);
            var f=CellDetailViewerPanel.class.getDeclaredField("tabbedPane");f.setAccessible(true);
            var tabs=(JTabbedPane)f.get(panel);
            panel.displayCellData(new byte[8],key);tabs.setSelectedIndex(tabs.indexOfTab("Numeric"));
            panel.displayCellData(bytes("{\"first\":1}"),value);tabs.setSelectedIndex(tabs.indexOfTab("JSON"));
            panel.displayCellData(bytes("{\"second\":2}"),value);
            assertEquals("JSON",tabs.getTitleAt(tabs.getSelectedIndex()));
            var json=CellDetailViewerPanel.class.getDeclaredField("jsonView");json.setAccessible(true);
            assertTrue(((JTextArea)json.get(panel)).getText().contains("second"));
            panel.displayCellData(new byte[8],key);
            assertEquals("Numeric",tabs.getTitleAt(tabs.getSelectedIndex()));
            panel.displayCellData(bytes("{}"),other);tabs.setSelectedIndex(tabs.indexOfTab("Hex"));
            panel.displayCellData(null); // selection clearing during paging must not overwrite preferences
            panel.displayCellData(bytes("{}"),value);
            assertEquals("JSON",tabs.getTitleAt(tabs.getSelectedIndex()));
            panel.displayCellData(bytes("{}"),other);
            assertEquals("Hex",tabs.getTitleAt(tabs.getSelectedIndex()));
            return null;
        });
        SwingUtilities.invokeAndWait(task);task.get();
    }
}
