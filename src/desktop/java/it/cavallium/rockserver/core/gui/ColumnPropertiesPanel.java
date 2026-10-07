package it.cavallium.rockserver.core.gui;

import it.cavallium.rockserver.core.common.ColumnTableProperties;
import java.awt.*;
import java.util.List;
import javax.swing.*;

/** Charts of one explicit table-properties observation, with no refresh timer. */
public final class ColumnPropertiesPanel extends JPanel {
    private final JLabel summary = new JLabel("Load column properties explicitly to inspect SST internals.");
    private final LocalBarChart storage = new LocalBarChart(null);
    private final LocalBarChart compression = new LocalBarChart(null);
    public ColumnPropertiesPanel(JTextArea report) {
        super(new BorderLayout(0, 10));
        summary.setForeground(ViewerTheme.MUTED); add(summary, BorderLayout.NORTH);
        var tabs = new JTabbedPane();
        tabs.addTab("Storage components", new JScrollPane(storage));
        tabs.addTab("Compression", new JScrollPane(compression));
        tabs.addTab("Text report", new JScrollPane(report));
        add(tabs, BorderLayout.CENTER);
        var note = new JTextArea("Explicit metadata observation only. Data/index/filter sizes are SST properties, not total disk or cache usage. Physical entries include old versions and deletions; they are not live rows.");
        note.setEditable(false); note.setLineWrap(true); note.setWrapStyleWord(true); note.setRows(3); note.setForeground(ViewerTheme.MUTED);
        var explanation = new JScrollPane(note);
        explanation.setPreferredSize(new Dimension(550, 64));
        add(explanation, BorderLayout.SOUTH);
    }
    public void clear() {
        summary.setText("Load column properties explicitly to inspect SST internals.");
        storage.display(List.of()); compression.display(List.of());
    }
    public void display(ColumnTableProperties p) {
        summary.setText(p.tableCount() + " SSTs · " + p.numEntries() + " physical entries · " + p.numDeletions() + " point deletions · " + p.numMergeOperands() + " merge operands");
        storage.display(List.of(new LocalBarChart.Bar("Data blocks", p.dataSize(), SstAnalysis.size(p.dataSize())),
                new LocalBarChart.Bar("Indexes", p.indexSize(), SstAnalysis.size(p.indexSize())),
                new LocalBarChart.Bar("Filters", p.filterSize(), SstAnalysis.size(p.filterSize()))));
        compression.display(p.compressions().entrySet().stream().sorted(java.util.Map.Entry.comparingByKey())
                .map(e -> new LocalBarChart.Bar(e.getKey(), e.getValue(), e.getValue() + " SST files")).toList());
    }
}
