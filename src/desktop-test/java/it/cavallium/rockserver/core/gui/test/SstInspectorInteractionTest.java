package it.cavallium.rockserver.core.gui.test;

import static org.junit.jupiter.api.Assertions.*;
import it.cavallium.rockserver.core.gui.*;
import java.awt.*;
import java.awt.datatransfer.DataFlavor;
import java.awt.image.BufferedImage;
import java.nio.file.Path;
import java.util.concurrent.FutureTask;
import javax.imageio.ImageIO;
import javax.swing.*;
import org.junit.jupiter.api.*;

class SstInspectorInteractionTest {
    @Test void expandedInspectorCopiesExactReportAndDisplaysFullMetadata() throws Exception {
        Assumptions.assumeFalse(GraphicsEnvironment.isHeadless());
        var task = new FutureTask<Void>(() -> {
            ViewerTheme.install();
            var inspector = new SstFileInspector();
            var data = SstExplorerPanelTest.fixture();
            var snapshot = SstFileInspector.describe(data, data.files().get(6));
            inspector.display(snapshot);
            inspector.expand();
            var dialog = java.util.Arrays.stream(Window.getWindows()).filter(w -> w instanceof JDialog && w.isVisible())
                    .map(w -> (JDialog) w).filter(w -> w.getTitle().equals(snapshot.title())).findFirst().orElseThrow();
            try {
                var expanded = (SstFileInspector) dialog.getContentPane();
                assertEquals(inspector.report(), expanded.report());
                var f = SstFileInspector.class.getDeclaredField("copyReport"); f.setAccessible(true);
                ((JButton) f.get(expanded)).doClick();
                assertEquals(snapshot.report(), Toolkit.getDefaultToolkit().getSystemClipboard().getData(DataFlavor.stringFlavor));
                String screenshot = System.getProperty("rockserver.inspector.expanded");
                if (screenshot != null) {
                    var image = new BufferedImage(expanded.getWidth(), expanded.getHeight(), BufferedImage.TYPE_INT_RGB);
                    var graphics = image.createGraphics();
                    try { expanded.printAll(graphics); } finally { graphics.dispose(); }
                    ImageIO.write(image, "png", Path.of(screenshot).toFile());
                }
                dialog.setMinimumSize(new Dimension(400, 200));
                dialog.setSize(650, 280);
                dialog.validate();
                var footerField = SstFileInspector.class.getDeclaredField("footer"); footerField.setAccessible(true);
                assertFalse(((JPanel) footerField.get(expanded)).isVisible());
                var factsField = SstFileInspector.class.getDeclaredField("facts"); factsField.setAccessible(true);
                assertTrue(((JTable) factsField.get(expanded)).getParent().getHeight() > 20, "Compact inspector keeps file properties visible");
                inspector.display(null);
                assertEquals(snapshot.report(), expanded.report(), "Expanded view retains its explicitly observed snapshot");
            } finally { dialog.dispose(); }
            return null;
        });
        SwingUtilities.invokeAndWait(task);
        task.get();
    }
}
