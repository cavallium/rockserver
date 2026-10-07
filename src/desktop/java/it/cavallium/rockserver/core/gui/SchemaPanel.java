package it.cavallium.rockserver.core.gui;

import it.cavallium.rockserver.core.common.ColumnSchema;
import java.awt.*;
import java.util.Objects;
import javax.swing.*;
import javax.swing.table.DefaultTableModel;

/** Logical key structure, derived entirely from column definitions. */
public final class SchemaPanel extends JPanel {
    private final JPanel diagram = new JPanel(new FlowLayout(FlowLayout.LEFT, 12, 12));
    private final JLabel summary = new JLabel("Select a column to inspect its schema.");
    private final JTextArea merge = new JTextArea();
    private final DefaultTableModel model = new DefaultTableModel(new String[]{"Component", "Kind", "Physical representation"}, 0) {
        @Override public boolean isCellEditable(int r, int c) { return false; }
    };
    public SchemaPanel() {
        super(new BorderLayout(0, 12));
        var heading = new JPanel(new BorderLayout());
        summary.setForeground(ViewerTheme.MUTED);
        heading.add(summary, BorderLayout.NORTH);
        var schematic = new JScrollPane(diagram, JScrollPane.VERTICAL_SCROLLBAR_NEVER, JScrollPane.HORIZONTAL_SCROLLBAR_AS_NEEDED);
        schematic.setPreferredSize(new Dimension(550, 74));
        schematic.setBorder(BorderFactory.createEmptyBorder());
        heading.add(schematic, BorderLayout.CENTER);
        add(heading, BorderLayout.NORTH);
        var table = new JTable(model); table.setFillsViewportHeight(true); table.setRowHeight(34);
        add(new JScrollPane(table), BorderLayout.CENTER);
        merge.setEditable(false); merge.setLineWrap(true); merge.setWrapStyleWord(true); merge.setRows(6);
        merge.setBorder(ViewerTheme.section("VALUE AND MERGE BEHAVIOR"));
        var behavior = new JScrollPane(merge);
        behavior.setPreferredSize(new Dimension(550, 140));
        add(behavior, BorderLayout.SOUTH);
    }
    public void display(String name, ColumnSchema schema) {
        model.setRowCount(0); diagram.removeAll();
        if (schema == null) { summary.setText("Select a column to inspect its schema."); merge.setText(""); }
        else {
            summary.setText(schema.fixedLengthKeysCount() + " fixed keys · " + schema.variableLengthKeysCount() + " variable keys · "
                    + (schema.hasValue() ? "values stored" : "key-only column"));
            for (int i = 0; i < schema.keysCount(); i++) {
                boolean fixed = i < schema.fixedLengthKeysCount();
                String representation = fixed ? schema.key(i) + " bytes" : schema.variableTailKey(i) + " hash · " + schema.key(i) + " bytes";
                model.addRow(new Object[]{"Key " + i, fixed ? "Fixed length" : "Variable logical key", representation});
                // A compact schematic; the table remains complete for schemas with many components.
                if (i < 6) {
                    var card = new JLabel("Key " + i + " · " + (fixed ? schema.key(i) + " B" : "variable"));
                    card.setOpaque(true); card.setBackground(ViewerTheme.SURFACE);
                    card.setBorder(BorderFactory.createEmptyBorder(12, 14, 12, 14)); diagram.add(card);
                    if (i < Math.min(5, schema.keysCount() - 1)) diagram.add(new JLabel("→"));
                }
            }
            if (schema.keysCount() > 6) diagram.add(new JLabel("+ " + (schema.keysCount() - 6) + " components below"));
            merge.setText("Values: " + (schema.hasValue() ? "stored separately from key components" : "not stored")
                    + "\nMerge operator: " + Objects.toString(schema.mergeOperatorName(), "none")
                    + "\nVersion: " + Objects.toString(schema.mergeOperatorVersion(), "not specified")
                    + "\nImplementation: " + Objects.toString(schema.mergeOperatorClass(), "not specified")
                    + "\nVariable keys are supplied as logical bytes; their physical hash width is not their logical length."
                    + "\nThe schematic shows component order, not proportional byte sizes.");
        }
        diagram.revalidate(); diagram.repaint();
    }
}
