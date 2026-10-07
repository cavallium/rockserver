package it.cavallium.rockserver.core.gui;

import it.cavallium.rockserver.core.common.SstMaintenance;
import java.awt.*;
import java.awt.datatransfer.StringSelection;
import java.awt.event.*;
import java.util.*;
import java.util.List;
import javax.swing.*;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.DefaultTableCellRenderer;

/** Copyable physical-file facts and contextual comparisons, derived from one metadata observation. */
public final class SstFileInspector extends JPanel {
    public record Fact(String name, String value) {}
    public record Snapshot(String title, String subtitle, List<Fact> facts, String note) {
        public Snapshot { facts = List.copyOf(facts); }
        public String report() {
            var text = new StringBuilder(title).append('\n').append(subtitle).append('\n');
            for (var fact : facts) text.append(fact.name()).append(": ").append(fact.value()).append('\n');
            return text.append('\n').append(note).toString();
        }
    }

    public static Snapshot describe(SstMaintenance.Metadata metadata, SstMaintenance.File file) {
        return describe(SstAnalysis.index(metadata), file);
    }

    public static Snapshot describe(SstAnalysis.Index index, SstMaintenance.File file) {
        var metadata = index.metadata();
        var inLevel = index.levelFiles().get(file.level());
        var column = index.total();
        var level = index.levels().get(file.level());
        var path = index.paths().get(file.pathId());
        long rank = 1 + inLevel.stream().filter(f -> f.sizeBytes() > file.sizeBytes()).count();
        var overlaps = inLevel.stream().filter(f -> !f.name().equals(file.name()))
                .filter(f -> file.smallestKeyHex().compareTo(f.largestKeyHex()) <= 0
                        && file.largestKeyHex().compareTo(f.smallestKeyHex()) >= 0).toList();
        String directory = metadata.paths().get(file.pathId());
        String separator = directory.endsWith("/") || directory.endsWith("\\") ? "" : directory.contains("\\") ? "\\" : "/";
        String overlappingNames = String.join(", ", overlaps.stream().limit(20).map(SstMaintenance.File::name).toList());
        if (overlaps.size() > 20) overlappingNames += " … (" + overlaps.size() + " total)";
        return new Snapshot(file.name(), SstAnalysis.size(file.sizeBytes()) + " · L" + file.level() + " · Path " + file.pathId(), List.of(
                new Fact("Column", metadata.columnName()),
                new Fact("File path", directory + separator + file.name()),
                new Fact("Exact size", String.format(Locale.ROOT, "%,d bytes", file.sizeBytes())),
                new Fact("Level", "L" + file.level() + (file.level() == metadata.baseLevel() ? " (base level)" : "")),
                new Fact("Storage path", "Path " + file.pathId() + " · " + directory),
                new Fact("Compaction", file.beingCompacted() ? "Compacting at observation" : "Not compacting at observation"),
                new Fact("Share of column", percent(file.sizeBytes(), column.bytes())),
                new Fact("Share of level", percent(file.sizeBytes(), level.bytes())),
                new Fact("Share of storage path", percent(file.sizeBytes(), path.bytes())),
                new Fact("Size rank in level", rank + " of " + level.files() + " (largest first; ties share rank)"),
                new Fact("Median SST in level", SstAnalysis.size(level.median())),
                new Fact("Size / level median", level.median() == 0 ? "n/a (zero median)" : String.format(Locale.ROOT, "%.2f×", file.sizeBytes() / level.median())),
                new Fact("Same-level range intersections", overlaps.size() + " other SSTs"),
                new Fact("Intersecting files", overlaps.isEmpty() ? "None" : overlappingNames),
                new Fact("Smallest raw key (hex)", file.smallestKeyHex()),
                new Fact("Largest raw key (hex)", file.largestKeyHex())
        ), "Observed metadata, not a pinned file. Raw key bounds are inclusive and compared bytewise; intersections do not prove duplicate logical records. Overlap is normal in L0. Entry counts, compression and tombstones are not exposed per file by this metadata API.");
    }

    private static String percent(long part, long total) {
        return total == 0 ? "n/a (zero total)" : String.format(Locale.ROOT, "%.2f%%", 100d * part / total);
    }

    private final JLabel title = new JLabel("Select an SST");
    private final JLabel subtitle = new JLabel("Click a tile or select a file row");
    private final JLabel feedback = new JLabel(" ");
    private final JPanel footer = new JPanel(new BorderLayout(0, 5));
    private final JTextArea note = new JTextArea();
    private final JButton copyReport = new JButton("Copy report");
    private final JButton copyValue = new JButton("Copy value");
    private final JButton expand = new JButton("Expand");
    private Snapshot snapshot;
    private final AbstractTableModel model = new AbstractTableModel() {
        public int getRowCount() { return snapshot == null ? 0 : snapshot.facts().size(); }
        public int getColumnCount() { return 2; }
        public String getColumnName(int c) { return c == 0 ? "Property" : "Value"; }
        public Object getValueAt(int row, int c) {
            var f = snapshot.facts().get(row);
            return c == 0 ? f.name() : f.value();
        }
    };
    private final JTable facts = new JTable(model);

    public SstFileInspector() {
        super(new BorderLayout(0, 8));
        setBorder(BorderFactory.createEmptyBorder(12, 12, 10, 4));
        title.setFont(title.getFont().deriveFont(Font.BOLD, 18f));
        title.putClientProperty("html.disable", Boolean.TRUE);
        subtitle.putClientProperty("html.disable", Boolean.TRUE);
        subtitle.setForeground(ViewerTheme.MUTED);
        var heading = new JPanel(new BorderLayout(0, 5));
        heading.add(title, BorderLayout.NORTH);
        heading.add(subtitle, BorderLayout.CENTER);
        var actions = new JPanel(new FlowLayout(FlowLayout.LEFT, 6, 0));
        actions.add(copyReport); actions.add(copyValue); actions.add(expand);
        heading.add(actions, BorderLayout.SOUTH);
        add(heading, BorderLayout.NORTH);
        facts.setFillsViewportHeight(true);
        facts.setRowHeight(28);
        facts.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        facts.setShowVerticalLines(false);
        facts.getTableHeader().setReorderingAllowed(false);
        facts.getColumnModel().getColumn(0).setPreferredWidth(155);
        facts.getColumnModel().getColumn(1).setPreferredWidth(260);
        facts.setDefaultRenderer(Object.class, new DefaultTableCellRenderer() {
            { putClientProperty("html.disable", Boolean.TRUE); }
            @Override public Component getTableCellRendererComponent(JTable table, Object value, boolean selected, boolean focused, int row, int column) {
                super.getTableCellRendererComponent(table, value, selected, focused, row, column);
                // Plain text: a server-controlled path or key must not become Swing HTML.
                setToolTipText(value == null ? null : value.toString().replace("<", "‹"));
                if (!selected) setForeground(column == 0 ? ViewerTheme.MUTED : ViewerTheme.TEXT);
                return this;
            }
        });
        add(new JScrollPane(facts), BorderLayout.CENTER);
        note.setEditable(false);
        note.setLineWrap(true); note.setWrapStyleWord(true);
        note.setFont(note.getFont().deriveFont(11f)); note.setForeground(ViewerTheme.MUTED);
        note.setRows(3);
        footer.add(new JScrollPane(note), BorderLayout.CENTER);
        footer.add(feedback, BorderLayout.SOUTH);
        add(footer, BorderLayout.SOUTH);
        copyReport.addActionListener(e -> copy(snapshot.report()));
        copyValue.addActionListener(e -> copySelectedValue());
        facts.getSelectionModel().addListSelectionListener(e -> copyValue.setEnabled(facts.getSelectedRow() >= 0));
        expand.addActionListener(e -> expand());
        facts.getInputMap().put(KeyStroke.getKeyStroke(KeyEvent.VK_C, (GraphicsEnvironment.isHeadless() ? InputEvent.CTRL_DOWN_MASK : Toolkit.getDefaultToolkit().getMenuShortcutKeyMaskEx())), "copy-value");
        facts.getActionMap().put("copy-value", new AbstractAction() {
            @Override public void actionPerformed(ActionEvent e) { copySelectedValue(); }
        });
        display(null);
    }

    @Override public void doLayout() {
        // At compact heights, prioritize actual file properties over explanatory text.
        footer.setVisible(getHeight() >= 300);
        super.doLayout();
    }

    public void display(Snapshot snapshot) {
        this.snapshot = snapshot;
        title.setText(snapshot == null ? "Select an SST" : snapshot.title());
        subtitle.setText(snapshot == null ? "Click a tile or select a file row" : snapshot.subtitle());
        note.setText(snapshot == null ? "File details appear here. Double-click a tile or press Enter in the file list for an expanded inspector." : snapshot.note());
        note.setCaretPosition(0);
        feedback.setText(" ");
        model.fireTableDataChanged();
        copyReport.setEnabled(snapshot != null);
        expand.setEnabled(snapshot != null);
        copyValue.setEnabled(false);
    }

    /** Stable text export also used by the full-size inspector. */
    public String report() { return snapshot == null ? "" : snapshot.report(); }

    public void expand() {
        if (snapshot == null) return;
        var dialog = new JDialog(SwingUtilities.getWindowAncestor(this), snapshot.title(), Dialog.ModalityType.MODELESS);
        var inspector = new SstFileInspector();
        inspector.display(snapshot);
        inspector.expand.setVisible(false);
        dialog.setContentPane(inspector);
        dialog.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
        dialog.setSize(780, 760);
        dialog.setMinimumSize(new Dimension(520, 400));
        dialog.setLocationRelativeTo(this);
        dialog.getRootPane().registerKeyboardAction(e -> dialog.dispose(), KeyStroke.getKeyStroke(KeyEvent.VK_ESCAPE, 0), JComponent.WHEN_IN_FOCUSED_WINDOW);
        dialog.setVisible(true);
    }

    private void copySelectedValue() {
        int row = facts.getSelectedRow();
        if (row >= 0) copy(Objects.toString(facts.getValueAt(row, 1), ""));
    }
    private void copy(String value) {
        try {
            Toolkit.getDefaultToolkit().getSystemClipboard().setContents(new StringSelection(value), null);
            feedback.setText("Copied to clipboard");
        } catch (IllegalStateException e) { feedback.setText("Clipboard busy; try again."); }
    }
}
