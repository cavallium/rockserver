package it.cavallium.rockserver.core.gui;

import java.awt.*;
import java.awt.event.*;
import java.util.List;
import java.util.function.Consumer;
import javax.swing.*;

/** A virtualized chart of already-loaded values. Never performs database I/O. */
public final class LocalBarChart extends JPanel implements Scrollable {
    public record Bar(String label, long value, String detail) {
        public Bar { if (value < 0) throw new IllegalArgumentException("Negative bar value"); }
    }
    private List<Bar> bars = List.of();
    private long maximum;
    private final Consumer<Bar> select;
    private static final int ROW = 58;

    public LocalBarChart(Consumer<Bar> select) {
        this.select = select;
        setToolTipText("");
        addMouseListener(new MouseAdapter() {
            @Override public void mouseClicked(MouseEvent event) {
                int row = event.getY() / ROW;
                if (select != null && row >= 0 && row < bars.size()) select.accept(bars.get(row));
            }
        });
    }
    public void display(List<Bar> values) {
        bars = List.copyOf(values);
        maximum = bars.stream().mapToLong(Bar::value).max().orElse(0);
        setPreferredSize(new Dimension(550, Math.max(100, bars.size() * ROW)));
        revalidate(); repaint();
    }
    @Override public String getToolTipText(MouseEvent event) {
        int row = event.getY() / ROW;
        if (row < 0 || row >= bars.size()) return null;
        var b = bars.get(row);
        return b.label().replace("<", "‹") + " · " + b.detail().replace("<", "‹");
    }
    @Override protected void paintComponent(Graphics graphics) {
        super.paintComponent(graphics);
        var g = (Graphics2D) graphics.create();
        try {
            g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
            Rectangle clip = g.getClipBounds();
            int first = Math.max(0, clip.y / ROW), last = Math.min(bars.size(), (clip.y + clip.height) / ROW + 1);
            if (bars.isEmpty()) { g.setColor(ViewerTheme.MUTED); g.drawString("No data loaded", 12, 25); }
            for (int i = first; i < last; i++) {
                var b = bars.get(i); int y = i * ROW;
                g.setColor(ViewerTheme.TEXT); g.drawString(b.label(), 12, y + 19);
                g.setColor(ViewerTheme.MUTED);
                int detailWidth = g.getFontMetrics().stringWidth(b.detail());
                if (detailWidth + g.getFontMetrics().stringWidth(b.label()) + 36 < getWidth())
                    g.drawString(b.detail(), getWidth() - 12 - detailWidth, y + 19);
                int width = Math.max(0, getWidth() - 24);
                g.setColor(ViewerTheme.SURFACE); g.fillRoundRect(12, y + 29, width, 12, 6, 6);
                g.setColor(ViewerTheme.ACCENT);
                g.fillRoundRect(12, y + 29, maximum == 0 ? 0 : (int) (width * ((double) b.value() / maximum)), 12, 6, 6);
            }
        } finally { g.dispose(); }
    }
    @Override public Dimension getPreferredScrollableViewportSize() { return new Dimension(550, 280); }
    @Override public int getScrollableUnitIncrement(Rectangle r, int o, int d) { return ROW; }
    @Override public int getScrollableBlockIncrement(Rectangle r, int o, int d) { return Math.max(ROW, r.height - ROW); }
    @Override public boolean getScrollableTracksViewportWidth() { return true; }
    @Override public boolean getScrollableTracksViewportHeight() { return false; }
}
