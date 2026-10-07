package it.cavallium.rockserver.core.gui;

import it.cavallium.rockserver.core.common.SstMaintenance;
import java.awt.*;
import java.awt.event.*;
import java.awt.geom.Rectangle2D;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.List;
import java.util.function.IntConsumer;
import javax.swing.*;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.DefaultTableCellRenderer;

/** An immutable metadata observation rendered as linked, read-only physical storage views. */
public final class SstExplorerPanel extends JPanel {
    private static final Color[] COLORS = {new Color(59,130,246), new Color(20,150,136), new Color(139,92,246),
            new Color(225,130,30), new Color(219,75,116), new Color(57,120,161), new Color(102,132,52)};
    private final JComboBox<String> levelFilter = new JComboBox<>();
    private final JComboBox<String> pathFilter = new JComboBox<>();
    private final JComboBox<String> colorBy = new JComboBox<>(new String[]{"Color by level", "Color by path"});
    private final JTextField fileFilter = new JTextField(12);
    private final JLabel summary = new JLabel();
    private final JLabel observed = new JLabel();
    private final JLabel legend = new JLabel();
    private final SstFileInspector inspector = new SstFileInspector();
    private final JButton resetFilters = new JButton("Reset filters");
    private final FileModel fileModel = new FileModel();
    private final JTable fileTable = new JTable(fileModel);
    private final Treemap map = new Treemap();
    private final JTabbedPane views = new JTabbedPane();
    private final Distribution levels = new Distribution("L", this::focusLevel);
    private final Distribution paths = new Distribution("Path ", this::focusPath);
    private SstMaintenance.Metadata metadata;
    private SstAnalysis.Index analysis;
    private SstAnalysis.Stats visibleStats = SstAnalysis.summarize(List.of());
    private List<SstMaintenance.File> visibleFiles = List.of();
    private SstMaintenance.File selectedFile;
    private boolean updating;

    public SstExplorerPanel() {
        super(new BorderLayout(0, 8));
        var header = new JPanel(new BorderLayout(0, 5));
        summary.setFont(summary.getFont().deriveFont(Font.BOLD, 16f));
        summary.putClientProperty("html.disable", Boolean.TRUE);
        observed.setForeground(ViewerTheme.MUTED);
        header.add(summary, BorderLayout.NORTH);
        header.add(observed, BorderLayout.CENTER);
        var filters = new JPanel(new FlowLayout(FlowLayout.LEFT, 8, 2));
        levelFilter.getAccessibleContext().setAccessibleName("SST level");
        pathFilter.getAccessibleContext().setAccessibleName("SST storage path");
        fileFilter.putClientProperty("JTextField.placeholderText", "Find an SST file…");
        fileFilter.putClientProperty("JTextField.showClearButton", true);
        fileFilter.getAccessibleContext().setAccessibleName("Find an SST file");
        filters.add(levelFilter); filters.add(pathFilter); filters.add(fileFilter); filters.add(colorBy);
        filters.add(resetFilters);
        header.add(filters, BorderLayout.SOUTH);
        add(header, BorderLayout.NORTH);
        resetFilters.addActionListener(e -> {
            updating = true;
            levelFilter.setSelectedIndex(0); pathFilter.setSelectedIndex(0); fileFilter.setText("");
            updating = false;
            filterFiles();
        });
        levelFilter.addActionListener(e -> filterFiles());
        pathFilter.addActionListener(e -> filterFiles());
        colorBy.addActionListener(e -> { updateLegend(); map.repaint(); });
        fileFilter.getDocument().addDocumentListener(new DocumentListener() {
            public void insertUpdate(DocumentEvent e) { filterFiles(); }
            public void removeUpdate(DocumentEvent e) { filterFiles(); }
            public void changedUpdate(DocumentEvent e) { filterFiles(); }
        });

        var treemap = new JPanel(new BorderLayout(0, 5));
        var help = new JLabel("Area = SST bytes · click groups to zoom, files to inspect · orange = compacting");
        help.setForeground(ViewerTheme.MUTED);
        treemap.add(help, BorderLayout.NORTH);
        treemap.add(map, BorderLayout.CENTER);
        var mapFooter = new JPanel(new BorderLayout());
        mapFooter.add(map.navigation, BorderLayout.NORTH);
        mapFooter.add(legend, BorderLayout.SOUTH);
        treemap.add(mapFooter, BorderLayout.SOUTH);
        views.addTab("Size treemap", treemap);
        views.addTab("Levels", new JScrollPane(levels));
        views.addTab("Storage paths", new JScrollPane(paths));
        fileTable.setAutoCreateRowSorter(true);
        fileTable.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        fileTable.setFillsViewportHeight(true);
        fileTable.setShowVerticalLines(false);
        fileTable.setDefaultRenderer(Object.class, new DefaultTableCellRenderer() {
            { putClientProperty("html.disable", Boolean.TRUE); }
        });
        fileTable.getColumnModel().getColumn(3).setCellRenderer(new DefaultTableCellRenderer() {
            @Override public Component getTableCellRendererComponent(JTable table, Object value, boolean selected, boolean focused, int row, int column) {
                return super.getTableCellRendererComponent(table, value instanceof Long size ? SstAnalysis.size(size) : "",
                        selected, focused, row, column);
            }
        });
        fileTable.getColumnModel().getColumn(0).setPreferredWidth(160);
        fileTable.getColumnModel().getColumn(1).setPreferredWidth(45);
        fileTable.getColumnModel().getColumn(2).setPreferredWidth(60);
        fileTable.getColumnModel().getColumn(3).setPreferredWidth(90);
        fileTable.getColumnModel().getColumn(4).setPreferredWidth(85);
        fileTable.getSelectionModel().addListSelectionListener(e -> {
            if (!e.getValueIsAdjusting() && !updating) {
                int row = fileTable.getSelectedRow();
                selectFile(row < 0 ? null : visibleFiles.get(fileTable.convertRowIndexToModel(row)));
            }
        });
        views.addTab("Files", new JScrollPane(fileTable));
        fileTable.getTableHeader().setReorderingAllowed(false);
        fileTable.getInputMap().put(KeyStroke.getKeyStroke(KeyEvent.VK_ENTER, 0), "inspect-file");
        fileTable.getActionMap().put("inspect-file", new AbstractAction() {
            @Override public void actionPerformed(ActionEvent e) { inspector.expand(); }
        });
        fileTable.addMouseListener(new MouseAdapter() {
            @Override public void mouseClicked(MouseEvent e) {
                if (e.getClickCount() == 2 && fileTable.rowAtPoint(e.getPoint()) >= 0) inspector.expand();
            }
        });
        inspector.setPreferredSize(new Dimension(390, 400));
        inspector.setMinimumSize(new Dimension(260, 170));
        views.setMinimumSize(new Dimension(280, 170));
        var split = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, views, inspector);
        split.setResizeWeight(0.60);
        split.setBorder(BorderFactory.createEmptyBorder());
        add(split, BorderLayout.CENTER);
        addComponentListener(new ComponentAdapter() {
            @Override public void componentResized(ComponentEvent e) {
                int orientation = getWidth() < 850 ? JSplitPane.VERTICAL_SPLIT : JSplitPane.HORIZONTAL_SPLIT;
                if (split.getOrientation() != orientation) {
                    split.setOrientation(orientation);
                    split.setDividerLocation(orientation == JSplitPane.VERTICAL_SPLIT ? 0.40 : 0.60);
                }
            }
        });
        clear();
    }

    public void clear() {
        metadata = null;
        analysis = null;
        updating = true;
        levelFilter.removeAllItems(); levelFilter.addItem("All levels");
        pathFilter.removeAllItems(); pathFilter.addItem("All paths");
        fileFilter.setText("");
        updating = false;
        visibleFiles = List.of();
        fileModel.fireTableDataChanged();
        map.invalidateLayout();
        levels.setGroups(List.of(), List.of(), -1);
        paths.setGroups(List.of(), List.of(), -1);
        summary.setText("SST explorer");
        observed.setText("Load SST layout to inspect physical files without scanning records.");
        legend.setText(" ");
        selectFile(null);
    }

    public void display(SstMaintenance.Metadata observation) {
        Objects.requireNonNull(observation);
        // Reject inconsistent input without changing the currently displayed observation.
        var prepared = SstAnalysis.index(observation);
        boolean sameColumn = metadata != null && metadata.columnId() == observation.columnId()
                && metadata.session().equals(observation.session());
        int levelSelection = sameColumn ? levelFilter.getSelectedIndex() : 0;
        int pathSelection = sameColumn ? pathFilter.getSelectedIndex() : 0;
        if (!sameColumn) selectedFile = null;
        metadata = observation;
        analysis = prepared;
        updating = true;
        levelFilter.removeAllItems(); levelFilter.addItem("All levels");
        for (int i = 0; i < metadata.numLevels(); i++) levelFilter.addItem("L" + i + (i == metadata.baseLevel() ? " · base" : ""));
        pathFilter.removeAllItems(); pathFilter.addItem("All paths");
        for (int i = 0; i < metadata.paths().size(); i++) pathFilter.addItem("Path " + i);
        levelFilter.setSelectedIndex(Math.min(levelSelection, levelFilter.getItemCount() - 1));
        pathFilter.setSelectedIndex(Math.min(pathSelection, pathFilter.getItemCount() - 1));
        if (!sameColumn) fileFilter.setText("");
        updating = false;
        String time = DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault()).format(Instant.now());
        observed.setText("Observed " + time + " · base level L" + metadata.baseLevel()
                + " · live file set, not pinned · excludes memtables, WAL and blobs");
        filterFiles();
    }

    private void filterFiles() {
        if (updating || metadata == null) return;
        String query = fileFilter.getText().strip().toLowerCase(Locale.ROOT);
        String selectedName = selectedFile == null ? null : selectedFile.name();
        visibleFiles = analysis.bySize().stream()
                .filter(f -> levelFilter.getSelectedIndex() == 0 || f.level() == levelFilter.getSelectedIndex() - 1)
                .filter(f -> pathFilter.getSelectedIndex() == 0 || f.pathId() == pathFilter.getSelectedIndex() - 1)
                .filter(f -> f.name().toLowerCase(Locale.ROOT).contains(query))
                .toList();
        var total = analysis.total();
        var stats = SstAnalysis.summarizeInSizeOrder(visibleFiles);
        visibleStats = stats;
        summary.setText(SstAnalysis.size(stats.bytes()) + " · " + stats.files() + " SST files"
                + (stats.files() == total.files() ? "" : " shown of " + total.files() + " (" + SstAnalysis.size(total.bytes()) + ")")
                + " · " + stats.compacting() + " compacting");
        updating = true;
        fileModel.fireTableDataChanged();
        updating = false;
        levels.setGroups(SstAnalysis.groupedInSizeOrder(visibleFiles, metadata.numLevels(), SstMaintenance.File::level), List.of(), metadata.baseLevel());
        paths.setGroups(SstAnalysis.groupedInSizeOrder(visibleFiles, metadata.paths().size(), SstMaintenance.File::pathId), metadata.paths(), -1);
        map.invalidateLayout();
        SstMaintenance.File selection = visibleFiles.stream().filter(f -> f.name().equals(selectedName)).findFirst().orElse(null);
        selectFile(selection);
        if (selection != null) {
            updating = true;
            int row = fileTable.convertRowIndexToView(visibleFiles.indexOf(selection));
            fileTable.setRowSelectionInterval(row, row);
            updating = false;
        }
        resetFilters.setEnabled(levelFilter.getSelectedIndex() > 0 || pathFilter.getSelectedIndex() > 0 || !query.isEmpty());
        updateLegend();
    }

    private void updateLegend() {
        if (metadata == null) return;
        StringBuilder text = new StringBuilder("<html>");
        int count = colorBy.getSelectedIndex() == 0 ? metadata.numLevels() : metadata.paths().size();
        for (int i = 0; i < count; i++) {
            text.append("<font color='#").append(String.format(Locale.ROOT, "%06x", color(i).getRGB() & 0xffffff))
                    .append("'>■</font> ").append(colorBy.getSelectedIndex() == 0 ? "L" : "Path ").append(i).append(" &nbsp; ");
        }
        legend.setText(text.append("<font color='#708094'>■</font> Mixed</html>").toString());
    }

    public boolean hasMetadata() { return metadata != null; }

    private void focusLevel(int level) {
        levelFilter.setSelectedIndex(level + 1);
        views.setSelectedIndex(0);
    }

    private void focusPath(int path) {
        pathFilter.setSelectedIndex(path + 1);
        views.setSelectedIndex(0);
    }

    private void selectFile(SstMaintenance.File file) {
        selectedFile = file;
        if (file == null) {
            if (metadata == null) inspector.display(null);
            else {
                var s = visibleStats;
                inspector.display(new SstFileInspector.Snapshot("Select an SST", "Click a tile or choose a file row", List.of(
                        new SstFileInspector.Fact("Filtered files", String.valueOf(s.files())),
                        new SstFileInspector.Fact("Total bytes", SstAnalysis.size(s.bytes())),
                        new SstFileInspector.Fact("Smallest SST", SstAnalysis.size(s.smallest())),
                        new SstFileInspector.Fact("Median SST", SstAnalysis.size(s.median())),
                        new SstFileInspector.Fact("Largest SST", SstAnalysis.size(s.largest())),
                        new SstFileInspector.Fact("Compacting", String.valueOf(s.compacting()))
                ), "Select a file for its exact location, key bounds and size comparisons. Double-click to expand. Unequal sizes alone do not establish a problem. Tiny files are accessible in Files."));
            }
        } else inspector.display(SstFileInspector.describe(analysis, file));
        map.repaint();
    }

    private void selectFromMap(SstMaintenance.File file) {
        int index = visibleFiles.indexOf(file);
        if (index < 0) return;
        int row = fileTable.convertRowIndexToView(index);
        updating = true;
        fileTable.setRowSelectionInterval(row, row);
        updating = false;
        fileTable.scrollRectToVisible(fileTable.getCellRect(row, 0, true));
        selectFile(file);
    }

    private static Color color(int id) { return id < 0 ? new Color(112, 128, 148) : COLORS[id % COLORS.length]; }
    private static String html(String value) { return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"); }

    private final class FileModel extends AbstractTableModel {
        private final String[] headers = {"SST file", "Level", "Path", "Size", "Compacting"};
        public int getRowCount() { return visibleFiles.size(); }
        public int getColumnCount() { return headers.length; }
        public String getColumnName(int column) { return headers[column]; }
        public Class<?> getColumnClass(int column) {
            return switch (column) { case 1, 2 -> Integer.class; case 3 -> Long.class; case 4 -> Boolean.class; default -> String.class; };
        }
        public Object getValueAt(int row, int column) {
            var file = visibleFiles.get(row);
            return switch (column) {
                case 0 -> file.name(); case 1 -> file.level(); case 2 -> file.pathId(); case 3 -> file.sizeBytes();
                case 4 -> file.beingCompacted(); default -> throw new IndexOutOfBoundsException();
            };
        }
    }

    private final class Treemap extends JPanel {
        private List<SstAnalysis.Tile> entries = List.of();
        private List<Rectangle2D.Double> tiles = List.of();
        private final Deque<List<SstAnalysis.Tile>> history = new ArrayDeque<>();
        private final Map<String, Integer> fileTiles = new HashMap<>();
        private final JPanel navigation = new JPanel(new FlowLayout(FlowLayout.LEFT, 6, 3));
        private final JButton back = new JButton("Back");
        private final JButton all = new JButton("All filtered files");
        private final JLabel scope = new JLabel();
        private int cachedWidth = -1, cachedHeight = -1;
        Treemap() {
            setPreferredSize(new Dimension(700, 290));
            setToolTipText("");
            setFocusable(true);
            getAccessibleContext().setAccessibleName("SST size treemap; groups zoom on click; arrow keys select files; Enter expands details");
            navigation.add(back); navigation.add(all); navigation.add(scope);
            back.addActionListener(e -> { if (!history.isEmpty()) { setEntries(history.pop()); showScope(); } });
            all.addActionListener(e -> { invalidateLayout(); showScope(); });
            addMouseListener(new MouseAdapter() {
                @Override public void mouseClicked(MouseEvent e) {
                    int index = hit(e.getPoint());
                    if (index < 0) return;
                    requestFocusInWindow();
                    var entry = entries.get(index);
                    if (entry.files().size() > 1) {
                        history.push(entries);
                        setEntries(SstAnalysis.tiles(entry.files()));
                        showScope();
                    } else {
                        selectFromMap(entry.files().getFirst());
                        if (e.getClickCount() == 2) inspector.expand();
                    }
                }
            });
            addMouseMotionListener(new MouseMotionAdapter() {
                @Override public void mouseMoved(MouseEvent e) {
                    setCursor(hit(e.getPoint()) >= 0 ? Cursor.getPredefinedCursor(Cursor.HAND_CURSOR) : Cursor.getDefaultCursor());
                }
            });
            for (int key : new int[]{KeyEvent.VK_LEFT, KeyEvent.VK_RIGHT, KeyEvent.VK_UP, KeyEvent.VK_DOWN}) {
                String action = "select-" + key;
                getInputMap().put(KeyStroke.getKeyStroke(key, 0), action);
                getActionMap().put(action, new AbstractAction() {
                    @Override public void actionPerformed(ActionEvent e) {
                        if (visibleFiles.isEmpty()) return;
                        int direction = key == KeyEvent.VK_LEFT || key == KeyEvent.VK_UP ? -1 : 1;
                        int current = visibleFiles.indexOf(selectedFile);
                        int index = current < 0 ? 0 : Math.floorMod(current + direction, visibleFiles.size());
                        selectFromMap(visibleFiles.get(index));
                    }
                });
            }
            getInputMap().put(KeyStroke.getKeyStroke(KeyEvent.VK_ENTER, 0), "expand");
            getActionMap().put("expand", new AbstractAction() {
                @Override public void actionPerformed(ActionEvent e) { if (selectedFile != null) inspector.expand(); }
            });
        }
        void invalidateLayout() {
            history.clear();
            setEntries(SstAnalysis.tiles(visibleFiles));
        }
        private void setEntries(List<SstAnalysis.Tile> entries) {
            this.entries = entries;
            fileTiles.clear();
            for (int i = 0; i < entries.size(); i++) {
                for (var file : entries.get(i).files()) fileTiles.put(file.name(), i);
            }
            cachedWidth = -1;
            back.setEnabled(!history.isEmpty());
            all.setEnabled(!history.isEmpty());
            int commonLevel = entries.isEmpty() ? -1 : entries.getFirst().level();
            int commonPath = entries.isEmpty() ? -1 : entries.getFirst().path();
            for (var tile : entries) {
                if (tile.level() != commonLevel) commonLevel = -1;
                if (tile.path() != commonPath) commonPath = -1;
            }
            scope.setText((commonLevel < 0 ? "" : "L" + commonLevel + " · ") + (commonPath < 0 ? "" : "Path " + commonPath + " · ")
                    + fileTiles.size() + " files · " + entries.size() + " tiles");
            scope.setToolTipText(scope.getText());
            repaint();
        }
        private void showScope() {
            updating = true;
            fileTable.clearSelection();
            updating = false;
            selectedFile = null;
            if (history.isEmpty()) { selectFile(null); return; }
            long bytes = 0; int compacting = 0;
            for (var entry : entries) { bytes = Math.addExact(bytes, entry.bytes()); compacting += entry.compacting(); }
            inspector.display(new SstFileInspector.Snapshot("Zoomed SST group", scope.getText(), List.of(
                    new SstFileInspector.Fact("Files in this zoom", String.valueOf(fileTiles.size())),
                    new SstFileInspector.Fact("SST bytes in this zoom", SstAnalysis.size(bytes)),
                    new SstFileInspector.Fact("Compacting", String.valueOf(compacting))
            ), "Click another group to zoom further, or an individual SST to inspect it. Back returns to the previous scope. The Files tab still contains every filtered file."));
            repaint();
        }
        private void ensureLayout() {
            if (getWidth() != cachedWidth || getHeight() != cachedHeight) {
                tiles = SstAnalysis.treemap(entries.stream().map(SstAnalysis.Tile::bytes).toList(), getWidth(), getHeight());
                cachedWidth = getWidth(); cachedHeight = getHeight();
            }
        }
        private int hit(Point p) {
            ensureLayout();
            for (int i = 0; i < tiles.size(); i++) if (tiles.get(i).contains(p)) return i;
            return -1;
        }
        @Override public String getToolTipText(MouseEvent e) {
            int index = hit(e.getPoint());
            if (index < 0) return null;
            var entry = entries.get(index);
            if (entry.files().size() > 1) return "<html>" + entry.label() + "<br>" + SstAnalysis.size(entry.bytes())
                    + " total · " + entry.compacting() + " compacting<br>Files from " + SstAnalysis.size(entry.files().getLast().sizeBytes())
                    + " to " + SstAnalysis.size(entry.files().getFirst().sizeBytes()) + "<br>Click to zoom; no files are omitted.</html>";
            var f = entry.files().getFirst();
            return "<html>" + html(f.name()) + " · L" + f.level() + " · Path " + f.pathId() + "<br>"
                    + SstAnalysis.size(f.sizeBytes()) + " (" + f.sizeBytes() + " bytes)<br>"
                    + html(metadata.paths().get(f.pathId())) + (f.beingCompacted() ? "<br>Compacting" : "") + "</html>";
        }
        @Override protected void paintComponent(Graphics graphics) {
            super.paintComponent(graphics);
            ensureLayout();
            int selected = selectedFile == null ? -1 : fileTiles.getOrDefault(selectedFile.name(), -1);
            var g = (Graphics2D) graphics.create();
            try {
                g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
                if (entries.stream().noneMatch(f -> f.bytes() > 0)) {
                    g.setColor(ViewerTheme.MUTED);
                    g.drawString(metadata == null ? "Load SST layout to see the file-size map." : "No non-zero SST files match these filters.", 16, 32);
                }
                for (int i = 0; i < tiles.size(); i++) {
                    var r = tiles.get(i);
                    if (r.isEmpty()) continue;
                    var entry = entries.get(i);
                    g.setColor(color(colorBy.getSelectedIndex() == 0 ? entry.level() : entry.path()));
                    g.fill(r);
                    g.setColor(Color.WHITE); g.setStroke(new BasicStroke(1)); g.draw(r);
                    if (entry.compacting() > 0 || i == selected) {
                        g.setColor(i == selected ? ViewerTheme.TEXT : new Color(255, 174, 46));
                        g.setStroke(new BasicStroke(i == selected ? 3 : 2));
                        g.draw(new Rectangle2D.Double(r.x + 1.5, r.y + 1.5, Math.max(0, r.width - 3), Math.max(0, r.height - 3)));
                    }
                    if (r.height > 38 && r.width > g.getFontMetrics().stringWidth(entry.label()) + 12) {
                        g.setColor(Color.WHITE);
                        g.drawString(entry.label(), (float) r.x + 6, (float) r.y + 17);
                        g.drawString(SstAnalysis.size(entry.bytes()), (float) r.x + 6, (float) r.y + 33);
                    }
                }
            } finally { g.dispose(); }
        }
    }

    /** Linear byte bars share a scale; empty groups remain visible and clickable. */
    private static final class Distribution extends JPanel implements Scrollable {
        private List<SstAnalysis.Stats> groups = List.of();
        private List<String> names = List.of();
        private int base = -1;
        private final String prefix;
        Distribution(String prefix, IntConsumer select) {
            this.prefix = prefix;
            setToolTipText("");
            addMouseListener(new MouseAdapter() {
                @Override public void mouseClicked(MouseEvent e) {
                    int row = (e.getY() - 36) / rowHeight();
                    if (e.getY() >= 36 && row < groups.size()) select.accept(row);
                }
            });
        }
        void setGroups(List<SstAnalysis.Stats> groups, List<String> names, int base) {
            this.groups = groups; this.names = names; this.base = base;
            setPreferredSize(new Dimension(700, 40 + (names.isEmpty() ? 44 : 74) * groups.size()));
            revalidate(); repaint();
        }
        private int rowHeight() {
            return names.isEmpty() ? Math.max(30, Math.min(58, (getHeight() - 40) / Math.max(1, groups.size()))) : 74;
        }
        @Override public Dimension getPreferredScrollableViewportSize() { return new Dimension(700, 290); }
        @Override public int getScrollableUnitIncrement(Rectangle visible, int orientation, int direction) { return rowHeight(); }
        @Override public int getScrollableBlockIncrement(Rectangle visible, int orientation, int direction) { return Math.max(30, visible.height - rowHeight()); }
        @Override public boolean getScrollableTracksViewportWidth() { return true; }
        @Override public boolean getScrollableTracksViewportHeight() {
            return getParent() instanceof JViewport viewport && viewport.getHeight() >= 40 + groups.size() * (names.isEmpty() ? 30 : 74);
        }
        @Override public String getToolTipText(MouseEvent event) {
            int row = (event.getY() - 36) / rowHeight();
            if (event.getY() < 36 || row >= groups.size()) return null;
            var s = groups.get(row);
            return "<html>" + prefix + row + (names.isEmpty() ? "" : " · " + html(names.get(row)))
                    + "<br>" + s.bytes() + " bytes · " + s.files() + " files · " + s.compacting() + " compacting"
                    + "<br>Min / median / max: " + SstAnalysis.size(s.smallest()) + " / " + SstAnalysis.size(s.median()) + " / " + SstAnalysis.size(s.largest()) + "</html>";
        }
        @Override protected void paintComponent(Graphics graphics) {
            super.paintComponent(graphics);
            var g = (Graphics2D) graphics.create();
            try {
                g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
                g.setColor(ViewerTheme.MUTED);
                g.drawString("Filtered SST bytes · shared linear scale · click a row to filter the file views", 12, 23);
                long max = groups.stream().mapToLong(SstAnalysis.Stats::bytes).max().orElse(0);
                for (int i = 0; i < groups.size(); i++) {
                    var s = groups.get(i);
                    int y = 36 + i * rowHeight();
                    g.setColor(ViewerTheme.TEXT);
                    g.drawString(prefix + i + (i == base ? " · base" : "") + "    " + SstAnalysis.size(s.bytes()) + " · " + s.files()
                            + " files · max " + SstAnalysis.size(s.largest()) + " · median " + SstAnalysis.size(s.median()), 12, y + 15);
                    int width = Math.max(0, getWidth() - 24);
                    int barHeight = names.isEmpty() ? 7 : 15;
                    int barY = y + (names.isEmpty() ? 20 : 24);
                    g.setColor(ViewerTheme.SURFACE); g.fillRoundRect(12, barY, width, barHeight, 4, 4);
                    g.setColor(color(i)); g.fillRoundRect(12, barY, max == 0 ? 0 : (int) (width * ((double) s.bytes() / max)), barHeight, 4, 4);
                    if (!names.isEmpty()) {
                        g.setColor(ViewerTheme.MUTED);
                        g.drawString(names.get(i), 12, y + 56);
                    }
                }
            } finally { g.dispose(); }
        }
    }
}
