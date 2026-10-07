package it.cavallium.rockserver.core.gui;

import it.cavallium.rockserver.core.common.KV;
import java.awt.*;
import java.util.ArrayList;
import java.util.List;
import javax.swing.*;

/** Size-only analysis of the bounded page already in client memory. */
public final class PageInsightsPanel extends JPanel {
    private final JLabel summary = new JLabel("Load a page to inspect its record sizes.");
    private final LocalBarChart chart = new LocalBarChart(null);
    private static final long[] LIMITS = {0, 31, 255, 1023, 16383, 65535, 1048575, Long.MAX_VALUE};
    private static final String[] LABELS = {"Empty", "1–31 B", "32–255 B", "256–1023 B", "1–<16 KiB", "16–<64 KiB", "64 KiB–<1 MiB", "1 MiB or larger"};
    public record Sizes(int rows, long keyBytes, long valueBytes, long largest, List<Long> buckets) {}

    public PageInsightsPanel() {
        super(new BorderLayout(0, 8));
        summary.setBorder(BorderFactory.createEmptyBorder(8, 8, 8, 8));
        summary.setForeground(ViewerTheme.MUTED);
        add(summary, BorderLayout.NORTH);
        add(new JScrollPane(chart), BorderLayout.CENTER);
        var note = new JLabel("Key + value bytes · loaded page only · before local table filters · no additional database reads");
        note.setForeground(ViewerTheme.MUTED);
        note.setBorder(BorderFactory.createEmptyBorder(6, 8, 6, 8));
        add(note, BorderLayout.SOUTH);
    }
    public static Sizes analyze(List<KV> rows) {
        long keys = 0, values = 0, largest = 0;
        long[] buckets = new long[LIMITS.length];
        for (var row : rows) {
            long keyBytes = 0;
            for (var key : row.keys().keys()) keyBytes = Math.addExact(keyBytes, key.size());
            long valueBytes = row.value() == null ? 0 : row.value().size();
            keys = Math.addExact(keys, keyBytes); values = Math.addExact(values, valueBytes);
            long total = Math.addExact(keyBytes, valueBytes); largest = Math.max(largest, total);
            for (int i = 0; i < LIMITS.length; i++) if (total <= LIMITS[i]) { buckets[i]++; break; }
        }
        return new Sizes(rows.size(), keys, values, largest, java.util.Arrays.stream(buckets).boxed().toList());
    }
    public void display(List<KV> rows) {
        var s = analyze(rows);
        summary.setText(s.rows() + " loaded rows · keys " + SstAnalysis.size(s.keyBytes()) + " · values " + SstAnalysis.size(s.valueBytes())
                + " · largest record " + SstAnalysis.size(s.largest()));
        summary.setToolTipText("Loaded page only, before local table filtering. Not a database-wide distribution; no extra reads.");
        var bars = new ArrayList<LocalBarChart.Bar>();
        for (int i = 0; i < LIMITS.length; i++) bars.add(new LocalBarChart.Bar(LABELS[i], s.buckets().get(i), s.buckets().get(i) + " records"));
        chart.display(bars);
    }
}
