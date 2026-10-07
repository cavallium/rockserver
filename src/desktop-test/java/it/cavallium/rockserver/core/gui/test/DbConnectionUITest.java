package it.cavallium.rockserver.core.gui.test;

import static org.junit.jupiter.api.Assertions.*;

import it.cavallium.rockserver.core.gui.DbConnectionUI;
import it.cavallium.rockserver.core.gui.DbViewerUI;
import it.cavallium.rockserver.core.gui.ViewerTheme;
import java.awt.GraphicsEnvironment;
import java.awt.Window;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import javax.swing.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Assumptions;

@Timeout(30)
class DbConnectionUITest {
    @Test void testingAnEmbeddedConnectionDoesNotOpenAViewer() throws Exception {
        Assumptions.assumeFalse(GraphicsEnvironment.isHeadless(), "Run with xvfb-run for Swing coverage");
        System.setProperty("rockserver.core.print-config", "false");
        DbConnectionUI frame = edt(() -> {
            ViewerTheme.install();
            var result = new DbConnectionUI();
            result.setVisible(true);
            ((JComboBox<?>) field(result, "modeComboBox")).setSelectedIndex(1);
            ((JButton) field(result, "testButton")).doClick();
            return result;
        });
        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (!edt(() -> ((JButton) field(frame, "testButton")).isEnabled())) {
                if (System.nanoTime() > deadline) fail("Connection test did not finish");
                Thread.sleep(10);
            }
            assertTrue(edt(() -> ((JTextArea) field(frame, "statusArea")).getText()).startsWith("Connection verified."));
            assertTrue(edt(frame::isVisible));
            assertFalse(edt(() -> Arrays.stream(Window.getWindows()).anyMatch(w -> w instanceof DbViewerUI && w.isVisible())));
        } finally {
            edt(() -> { frame.dispose(); return null; });
        }
    }

    private static Object field(DbConnectionUI frame, String name) throws Exception {
        var field = DbConnectionUI.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(frame);
    }

    private static <T> T edt(Callable<T> operation) throws Exception {
        var task = new FutureTask<>(operation);
        SwingUtilities.invokeAndWait(task);
        return task.get();
    }
}
