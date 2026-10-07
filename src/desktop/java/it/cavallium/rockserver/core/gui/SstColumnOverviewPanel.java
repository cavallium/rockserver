package it.cavallium.rockserver.core.gui;

import java.awt.*;
import java.awt.event.*;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import javax.swing.*;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.DefaultTableCellRenderer;

/** Compact database-wide comparison; retains summaries rather than every column's file inventory. */
public final class SstColumnOverviewPanel extends JPanel {
    public record Column(String name, SstAnalysis.Stats stats, String error) {
        public Column {
            Objects.requireNonNull(name);
            if (stats == null && error == null) error = "Metadata unavailable";
        }
    }
    private List<Column> columns = List.of();
    private final JLabel summary = new JLabel("Load column sizes to compare physical SST storage across the database.");
    private final JTextArea details = new JTextArea(4, 20);
    private final JButton inspectButton = new JButton("Inspect selected column");
    private final AbstractTableModel model = new AbstractTableModel() {
        private final String[] names = {"Column", "SST files", "SST size", "Relative bytes", "Largest SST", "Compacting", "Status"};
        public int getRowCount() { return columns.size(); }
        public int getColumnCount() { return names.length; }
        public String getColumnName(int c) { return names[c]; }
        public Class<?> getColumnClass(int c) { return switch (c) { case 1, 5 -> Integer.class; case 2, 3, 4 -> Long.class; default -> String.class; }; }
        public Object getValueAt(int row, int c) {
            Column value = columns.get(row);
            if (c == 0) return value.name();
            if (c == 6) return value.error() == null ? "Loaded" : "Unavailable";
            if (value.stats() == null) return null;
            var s = value.stats();
            return switch (c) { case 1 -> s.files(); case 2, 3 -> s.bytes(); case 4 -> s.largest(); case 5 -> s.compacting(); default -> null; };
        }
    };
    private final JTable table = new JTable(model);
    private long maximum;
    private final LocalBarChart ranking;
    private final JTextField search = new JTextField();

    public SstColumnOverviewPanel(Consumer<String> inspect) {
        super(new BorderLayout(0, 10));
        ranking = new LocalBarChart(bar -> inspect.accept(bar.label()));
        summary.setForeground(ViewerTheme.MUTED);
        add(summary, BorderLayout.NORTH);
        table.setAutoCreateRowSorter(true);
        table.setFillsViewportHeight(true);
        table.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        table.setDefaultRenderer(Object.class, new DefaultTableCellRenderer() {
            { putClientProperty("html.disable", Boolean.TRUE); }
        });
        table.getColumnModel().getColumn(3).setCellRenderer(new DefaultTableCellRenderer() {
            private long bytes;
            @Override public Component getTableCellRendererComponent(JTable t, Object value, boolean selected, boolean focused, int row, int column) {
                super.getTableCellRendererComponent(t, "", selected, focused, row, column);
                bytes = value instanceof Long size ? size : 0;
                return this;
            }
            @Override protected void paintComponent(Graphics g) {
                super.paintComponent(g);
                g.setColor(ViewerTheme.ACCENT);
                int width = maximum == 0 ? 0 : (int) ((getWidth() - 12) * ((double) bytes / maximum));
                g.fillRoundRect(6, 10, Math.max(0, width), Math.max(0, getHeight() - 20), 6, 6);
            }
        });
        for (int column : new int[]{2, 4}) table.getColumnModel().getColumn(column).setCellRenderer(new DefaultTableCellRenderer() {
            @Override public Component getTableCellRendererComponent(JTable table, Object value, boolean selected, boolean focused, int row, int column) {
                super.getTableCellRendererComponent(table, value instanceof Long bytes ? SstAnalysis.size(bytes) : "—", selected, focused, row, column);
                setHorizontalAlignment(SwingConstants.TRAILING);
                setToolTipText(value == null ? "Metadata unavailable" : value + " bytes");
                return this;
            }
        });
        inspectButton.setEnabled(false);
        table.getSelectionModel().addListSelectionListener(e -> {
            if (e.getValueIsAdjusting()) return;
            int row = table.getSelectedRow();
            inspectButton.setEnabled(row >= 0);
            if (row < 0) { details.setText("Select a column to inspect its loaded metadata."); return; }
            var c = columns.get(table.convertRowIndexToModel(row));
            details.setText(c.error() == null ? c.name() + " · " + SstAnalysis.size(c.stats().bytes()) + " in " + c.stats().files()
                    + " SST files\nLargest SST: " + SstAnalysis.size(c.stats().largest()) + " · median: " + SstAnalysis.size(c.stats().median())
                    + "\nOpen the column to inspect its levels, paths and files." : c.name() + "\nMetadata unavailable: " + c.error());
        });
        Runnable open = () -> {
            int row = table.getSelectedRow();
            if (row >= 0) inspect.accept(columns.get(table.convertRowIndexToModel(row)).name());
        };
        table.addMouseListener(new MouseAdapter() {
            @Override public void mouseClicked(MouseEvent event) { if (event.getClickCount() == 2) open.run(); }
        });
        inspectButton.addActionListener(e -> open.run());
        details.setEditable(false);
        details.setLineWrap(true);
        details.setWrapStyleWord(true);
        details.setBorder(BorderFactory.createEmptyBorder(8, 8, 8, 8));
        var bottom = new JPanel(new BorderLayout());
        bottom.add(inspectButton, BorderLayout.NORTH);
        bottom.add(new JScrollPane(details), BorderLayout.CENTER);
        var views = new JTabbedPane();
        views.addTab("Column inventory", new JScrollPane(table));
        views.addTab("Size ranking", new JScrollPane(ranking));
        var center = new JPanel(new BorderLayout(0, 8));
        search.putClientProperty("JTextField.placeholderText", "Find a column in the loaded comparison…");
        search.putClientProperty("JTextField.showClearButton", true);
        search.getDocument().addDocumentListener(new javax.swing.event.DocumentListener() {
            public void insertUpdate(javax.swing.event.DocumentEvent e) { filter(); }
            public void removeUpdate(javax.swing.event.DocumentEvent e) { filter(); }
            public void changedUpdate(javax.swing.event.DocumentEvent e) { filter(); }
        });
        center.add(search, BorderLayout.NORTH);
        center.add(views, BorderLayout.CENTER);
        add(center, BorderLayout.CENTER);
        add(bottom, BorderLayout.SOUTH);
    }

    private void filter() {
        String text = search.getText().strip().toLowerCase(java.util.Locale.ROOT);
        @SuppressWarnings("unchecked") var sorter = (javax.swing.table.TableRowSorter<javax.swing.table.TableModel>) table.getRowSorter();
        sorter.setRowFilter(new RowFilter<>() {
            @Override public boolean include(Entry<? extends javax.swing.table.TableModel, ? extends Integer> entry) {
                return entry.getStringValue(0).toLowerCase(java.util.Locale.ROOT).contains(text);
            }
        });
        ranking.display(columns.stream().filter(c -> c.stats() != null && c.name().toLowerCase(java.util.Locale.ROOT).contains(text))
                .sorted(java.util.Comparator.comparingLong((Column c) -> c.stats().bytes()).reversed())
                .map(c -> new LocalBarChart.Bar(c.name(), c.stats().bytes(), SstAnalysis.size(c.stats().bytes()) + " · " + c.stats().files() + " SSTs")).toList());
    }

    public void display(List<Column> values) {
        display(values, values.size());
    }

    public void display(List<Column> values, int expectedColumns) {
        columns = List.copyOf(values);
        maximum = columns.stream().filter(c -> c.stats() != null).mapToLong(c -> c.stats().bytes()).max().orElse(0);
        long bytes = 0;
        int failed = 0;
        for (var column : columns) {
            if (column.stats() == null) failed++; else bytes = Math.addExact(bytes, column.stats().bytes());
        }
        model.fireTableDataChanged();
        filter();
        summary.setText(SstAnalysis.size(bytes) + " observed · " + (columns.size() - failed) + " columns loaded · " + failed
                + " unavailable · " + Math.max(0, expectedColumns - columns.size()) + " not inspected · sequential observations");
        details.setText("Bars share a linear byte scale. Unavailable columns are excluded from totals, not counted as empty.\n"
                + "Sequential observations, not an atomic snapshot. SST files only: memtables, WAL and blobs are excluded. Double-click a column to explore it.");
    }
}
