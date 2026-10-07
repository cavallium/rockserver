package it.cavallium.rockserver.core.gui;

import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.*;
import java.awt.*;
import java.awt.event.*;
import java.time.Duration;
import java.util.*;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import javax.swing.*;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableRowSorter;

/** Read-only, bounded browser for logical rows and column storage metadata. */
public class DbViewerUI extends JFrame {
    record Table(String name, ColumnSchema schema) {
        @Override public String toString() { return name; }
    }

    private final RocksDBConnection apiClient;
    private final DefaultListModel<Table> tableListModel = new DefaultListModel<>();
    private final JList<Table> tableList = new JList<>(tableListModel);
    private final JTextField tableFilterField = new JTextField();
    private final List<Table> allTables = new ArrayList<>();
    private final DefaultTableModel tableModel = new DefaultTableModel() {
        @Override public boolean isCellEditable(int row, int column) { return false; }
        @Override public Class<?> getColumnClass(int column) { return byte[].class; }
    };
    private final JTable dataTable = new JTable(tableModel);
    private final TableRowSorter<DefaultTableModel> sorter = new TableRowSorter<>(tableModel);
    private final Map<Integer, CellInterpreter> columnInterpreters = new HashMap<>();
    private final Map<Table, Map<Integer, CellInterpreter>> savedDecoders = new LinkedHashMap<>(32, 0.75f, true) {
        @Override protected boolean removeEldestEntry(Map.Entry<Table, Map<Integer, CellInterpreter>> entry) { return size() > 256; }
    };
    private final ColumnFilterRow filterRow = new ColumnFilterRow(this::applyFilters);
    private final CellDetailViewerPanel cellDetailViewer = new CellDetailViewerPanel();
    private final JTextField startField = new JTextField(18);
    private final JTextField endField = new JTextField(18);
    private final JCheckBox reverseBox = new JCheckBox("Reverse");
    private final JSpinner pageSize = new JSpinner(new SpinnerNumberModel(250, 1, RangeBudget.DEFAULT_MAX_ITEMS, 50));
    private final JButton browseButton = new JButton("Refresh");
    private final JToggleButton rangeToggle = new JToggleButton("Range");
    private final JToggleButton filterToggle = new JToggleButton("Filter");
    private final JButton pageSizesButton = new JButton("Page sizes");
    private final JButton clearPageFilters = new JButton("Clear filters");
    private final JComboBox<String> inspectorDock = new JComboBox<>(new String[]{"Right", "Below", "Hidden"});
    private final JLabel querySummary = new JLabel("Select a column to browse");
    private JPanel rangePanel;
    private JPanel inspectorPanel;
    private JSplitPane dataSplit;
    private final JButton previousButton = new JButton("Previous");
    private final JButton nextButton = new JButton("Next");
    private final JButton refreshTablesButton = new JButton("Refresh columns");
    private final JButton sstButton = new JButton("Load / refresh SST layout");
    private final JButton stopComparisonButton = new JButton("Stop comparison");
    private final JButton columnSizesButton = new JButton("Compare all column sizes");
    private final SstExplorerPanel sstExplorer = new SstExplorerPanel();
    private final SstColumnOverviewPanel columnOverview = new SstColumnOverviewPanel(this::inspectColumnStorage);
    private JPanel storagePanel;
    private JPanel databasePanel;
    private final JProgressBar progress = new JProgressBar();
    private boolean pendingViewLoad;
    private final JButton storageButton = new JButton("Load SST properties");
    private final JLabel statusLabel = new JLabel("Ready");
    private final JButton errorDetails = new JButton("Error details…");
    private String lastError;
    private final JLabel pageLabel = new JLabel("No page loaded");
    private final JLabel columnTitle = new JLabel("Select a column");
    private final SchemaPanel schemaView = new SchemaPanel();
    private final JComboBox<CellInterpreter> decoder = new JComboBox<>(CellInterpreter.values());
    private boolean updatingDecoder;
    private final JTextArea storageView = readOnlyText();
    private final ColumnPropertiesPanel propertyCharts = new ColumnPropertiesPanel(storageView);
    private final JTabbedPane tabs = new JTabbedPane();
    private final List<Keys> pageStarts = new ArrayList<>();
    private final Map<Integer, String> pageKeyIdentities = new HashMap<>();
    private Table selectedTable;
    private ViewerQuery activeQuery;
    private RangePage<KV> currentPage;
    private int pageIndex;
    private boolean busy;
    private boolean updatingList;
    private volatile boolean closing;
    private volatile boolean stopComparison;
    private boolean comparingColumns;

    public DbViewerUI(RocksDBConnection apiClient) {
        super("Rockserver · Database explorer");
        this.apiClient = apiClient;
        setDefaultCloseOperation(WindowConstants.DO_NOTHING_ON_CLOSE);
        setMinimumSize(new Dimension(1000, 720));
        setSize(1280, 850);
        initComponents();
        setLocationRelativeTo(null);
        addWindowListener(new WindowAdapter() {
            @Override public void windowClosing(WindowEvent event) { dispose(); }
        });
        loadInitialTables();
    }

    private static JTextArea readOnlyText() {
        var area = new JTextArea();
        area.setEditable(false);
        area.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 13));
        area.setBorder(BorderFactory.createEmptyBorder(12, 12, 12, 12));
        return area;
    }

    private void initComponents() {
        var heading = new JPanel(new BorderLayout(0, 6));
        heading.setBackground(Color.WHITE);
        heading.setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createMatteBorder(0, 0, 1, 0, ViewerTheme.LINE),
                BorderFactory.createEmptyBorder(12, 24, 12, 24)));
        var title = new JLabel("Rockserver");
        title.setFont(title.getFont().deriveFont(Font.BOLD, 20f));
        title.setForeground(ViewerTheme.TEXT);
        var brand = new JPanel(new BorderLayout(20, 0));
        brand.setOpaque(false);
        brand.add(title, BorderLayout.WEST);
        heading.add(brand, BorderLayout.CENTER);
        var connectionLabel = new JLabel("Read-only browser · " + apiClient.getUrl());
        connectionLabel.putClientProperty("html.disable", Boolean.TRUE);
        connectionLabel.setForeground(ViewerTheme.MUTED);
        brand.add(connectionLabel, BorderLayout.CENTER);
        connectionLabel.setToolTipText(connectionLabel.getText());
        var newConnection = new JButton("New connection…");
        newConnection.addActionListener(e -> new DbConnectionUI().setVisible(true));
        heading.add(newConnection, BorderLayout.EAST);
        add(heading, BorderLayout.NORTH);

        var left = new JPanel(new BorderLayout(8, 8));
        left.setBorder(BorderFactory.createEmptyBorder(18, 16, 16, 16));
        left.setBackground(ViewerTheme.SURFACE);
        tableFilterField.setToolTipText("Find a column by name");
        tableFilterField.getAccessibleContext().setAccessibleName("Find a column");
        var columnSearch = new JPanel(new BorderLayout(0, 4));
        columnSearch.setOpaque(false);
        var columnsLabel = new JLabel("COLUMNS");
        columnsLabel.setForeground(ViewerTheme.MUTED);
        columnsLabel.setFont(columnsLabel.getFont().deriveFont(Font.BOLD, 11f));
        columnSearch.add(columnsLabel, BorderLayout.NORTH);
        tableFilterField.putClientProperty("JTextField.placeholderText", "Find a column…");
        tableFilterField.putClientProperty("JTextField.showClearButton", true);
        columnSearch.add(tableFilterField, BorderLayout.CENTER);
        left.add(columnSearch, BorderLayout.NORTH);
        var columnScroll = new JScrollPane(tableList);
        columnScroll.setBorder(BorderFactory.createEmptyBorder());
        columnScroll.getViewport().setBackground(ViewerTheme.SURFACE);
        tableList.setBackground(ViewerTheme.SURFACE);
        tableList.putClientProperty("FlatLaf.style", "selectionArc: 10; selectionInsets: 2,0,2,0");
        left.add(columnScroll, BorderLayout.CENTER);
        left.add(refreshTablesButton, BorderLayout.SOUTH);
        tableList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        tableList.setFixedCellHeight(36);
        tableList.setCellRenderer(new DefaultListCellRenderer() {
            @Override public Component getListCellRendererComponent(JList<?> list, Object value, int index,
                    boolean selected, boolean focused) {
                super.getListCellRendererComponent(list, value, index, selected, focused);
                putClientProperty("html.disable", Boolean.TRUE);
                return this;
            }
        });
        tableList.addListSelectionListener(e -> {
            if (!e.getValueIsAdjusting() && !updatingList && !busy) selectTable();
        });
        tableFilterField.getDocument().addDocumentListener(new DocumentListener() {
            @Override public void insertUpdate(DocumentEvent e) { filterTableList(); }
            @Override public void removeUpdate(DocumentEvent e) { filterTableList(); }
            @Override public void changedUpdate(DocumentEvent e) { filterTableList(); }
        });
        refreshTablesButton.addActionListener(e -> loadInitialTables());

        ViewerTheme.primary(browseButton);
        pageSize.setPreferredSize(new Dimension(68, 32));
        rangeToggle.setToolTipText("Set key bounds and scan direction. Apply with Refresh.");
        filterToggle.setToolTipText("Filter the current page locally; this does not query the database.");
        pageSizesButton.setToolTipText("Analyze sizes on the loaded page; no extra database reads.");
        inspectorDock.setToolTipText("Place the inspector beside or below the grid, or hide it.");
        inspectorDock.getAccessibleContext().setAccessibleName("Inspector placement");
        var browseControls = new JPanel(new FlowLayout(FlowLayout.LEFT, 6, 0));
        browseControls.add(browseButton);
        browseControls.add(new JLabel("Rows")); browseControls.add(pageSize);
        browseControls.add(rangeToggle); browseControls.add(filterToggle); browseControls.add(pageSizesButton);
        var dockControls = new JPanel(new FlowLayout(FlowLayout.RIGHT, 6, 0));
        dockControls.add(new JLabel("Inspector")); dockControls.add(inspectorDock);
        var toolbar = new JPanel(new BorderLayout(8, 0));
        toolbar.setBorder(BorderFactory.createEmptyBorder(8, 0, 8, 0));
        toolbar.add(browseControls, BorderLayout.CENTER); toolbar.add(dockControls, BorderLayout.EAST);

        rangePanel = new JPanel(new GridBagLayout());
        rangePanel.setBorder(BorderFactory.createCompoundBorder(BorderFactory.createMatteBorder(1, 0, 1, 0, ViewerTheme.LINE),
                BorderFactory.createEmptyBorder(8, 8, 8, 8)));
        startField.putClientProperty("JTextField.placeholderText", "Unbounded start");
        endField.putClientProperty("JTextField.placeholderText", "Unbounded end");
        var c = new GridBagConstraints();
        c.insets = new Insets(3, 5, 3, 5); c.anchor = GridBagConstraints.WEST;
        c.gridx = 0; c.gridy = 0;
        var startLabel = new JLabel("From · inclusive"); startLabel.setLabelFor(startField); rangePanel.add(startLabel, c);
        c.gridx = 1;
        var endLabel = new JLabel("To · exclusive"); endLabel.setLabelFor(endField); rangePanel.add(endLabel, c);
        c.gridy = 1; c.gridx = 0; c.weightx = 1; c.fill = GridBagConstraints.HORIZONTAL; rangePanel.add(startField, c);
        c.gridx = 1; rangePanel.add(endField, c);
        c.gridx = 2; c.weightx = 0; c.fill = GridBagConstraints.NONE; rangePanel.add(reverseBox, c);
        c.gridy = 2; c.gridx = 0; c.gridwidth = 3;
        var rangeHelp = new JLabel("Hex components separated by ;   ·   blank = unbounded   ·   - = empty variable key");
        rangeHelp.setForeground(ViewerTheme.MUTED); rangeHelp.setFont(rangeHelp.getFont().deriveFont(12f));
        rangePanel.add(rangeHelp, c); rangePanel.setVisible(false);
        rangeToggle.addActionListener(e -> rangePanel.setVisible(rangeToggle.isSelected()));
        filterToggle.addActionListener(e -> filterRow.setVisible(filterToggle.isSelected()));
        clearPageFilters.addActionListener(e -> filterRow.clear());
        clearPageFilters.setVisible(false);
        pageSizesButton.addActionListener(e -> showPageSizes());
        browseButton.addActionListener(e -> applyQuery());
        startField.addActionListener(e -> applyQuery());
        endField.addActionListener(e -> applyQuery());
        previousButton.setToolTipText("Previous page in the last applied range.");
        nextButton.setToolTipText("Next page in the last applied range.");
        previousButton.addActionListener(e -> loadPage(activeQuery, pageIndex - 1, pageStarts.get(pageIndex - 1), false));
        nextButton.addActionListener(e -> loadPage(activeQuery, pageIndex + 1, currentPage.resumeAfter(), false));

        dataTable.setRowSorter(sorter);
        dataTable.setDefaultRenderer(byte[].class, new PerColumnCellRenderer(columnInterpreters));
        dataTable.setFillsViewportHeight(true);
        dataTable.setCellSelectionEnabled(true);
        dataTable.setRowHeight(28);
        dataTable.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 12));
        dataTable.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
        dataTable.setShowVerticalLines(false);
        dataTable.setGridColor(new Color(226, 231, 238));
        dataTable.setSelectionBackground(new Color(216, 233, 252));
        dataTable.setSelectionForeground(Color.BLACK);
        dataTable.getTableHeader().setReorderingAllowed(false);
        dataTable.getSelectionModel().addListSelectionListener(e -> { if (!e.getValueIsAdjusting()) updateCellDetailView(); });
        dataTable.getColumnModel().getSelectionModel().addListSelectionListener(e -> { if (!e.getValueIsAdjusting()) updateCellDetailView(); });
        dataTable.getTableHeader().addMouseListener(new MouseAdapter() {
            private void popup(MouseEvent e) {
                int index = dataTable.columnAtPoint(e.getPoint());
                if (e.isPopupTrigger() && index >= 0) createColumnContextMenu(index).show(e.getComponent(), e.getX(), e.getY());
            }
            @Override public void mousePressed(MouseEvent e) { popup(e); }
            @Override public void mouseReleased(MouseEvent e) { popup(e); }
        });
        var rows = new JPanel(new BorderLayout(0, 0));
        var recordHeading = new JLabel("Records"); recordHeading.setFont(recordHeading.getFont().deriveFont(Font.BOLD, 14f));
        querySummary.setForeground(ViewerTheme.MUTED); querySummary.setFont(querySummary.getFont().deriveFont(12f));
        var recordHeader = new JPanel(new BorderLayout(8, 0));
        recordHeader.setBorder(BorderFactory.createEmptyBorder(10, 2, 10, 2));
        var recordInfo = new JPanel(new BorderLayout(10, 0));
        recordInfo.add(recordHeading, BorderLayout.WEST); recordInfo.add(querySummary, BorderLayout.CENTER);
        recordHeader.add(recordInfo, BorderLayout.CENTER); recordHeader.add(clearPageFilters, BorderLayout.EAST);
        var gridHeader = new JPanel(new BorderLayout());
        gridHeader.add(recordHeader, BorderLayout.NORTH); gridHeader.add(filterRow, BorderLayout.CENTER);
        filterRow.setVisible(false);
        rows.add(gridHeader, BorderLayout.NORTH);
        var dataScroll = new JScrollPane(dataTable);
        dataScroll.setBorder(BorderFactory.createMatteBorder(1, 0, 1, 0, ViewerTheme.LINE));
        dataScroll.getViewport().addComponentListener(new ComponentAdapter() {
            @Override public void componentResized(ComponentEvent e) { fitValueColumn(); }
        });
        rows.add(dataScroll, BorderLayout.CENTER);
        var pagination = new JPanel(new BorderLayout(8, 0));
        pagination.setBorder(BorderFactory.createEmptyBorder(8, 2, 8, 2));
        pageLabel.setForeground(ViewerTheme.MUTED);
        pagination.add(pageLabel, BorderLayout.CENTER);
        var pageActions = new JPanel(new FlowLayout(FlowLayout.RIGHT, 6, 0));
        pageActions.add(previousButton); pageActions.add(nextButton); pagination.add(pageActions, BorderLayout.EAST);
        rows.add(pagination, BorderLayout.SOUTH);

        inspectorPanel = new JPanel(new BorderLayout(0, 8));
        inspectorPanel.setBorder(BorderFactory.createCompoundBorder(BorderFactory.createMatteBorder(0, 1, 0, 0, ViewerTheme.LINE),
                BorderFactory.createEmptyBorder(10, 12, 0, 0)));
        var inspectorHeading = new JLabel("Cell inspector"); inspectorHeading.setFont(inspectorHeading.getFont().deriveFont(Font.BOLD, 14f));
        var displayControls = new JPanel(new BorderLayout(8, 0));
        var formatLabel = new JLabel("Grid preview"); formatLabel.setForeground(ViewerTheme.MUTED);
        displayControls.add(formatLabel, BorderLayout.WEST); displayControls.add(decoder, BorderLayout.CENTER);
        decoder.setPreferredSize(new Dimension(180, 32)); decoder.setEnabled(false);
        decoder.setToolTipText("Format the selected column in the record grid. Inspector tabs are remembered separately.");
        decoder.addActionListener(e -> {
            int column = dataTable.getSelectedColumn();
            if (!updatingDecoder && column >= 0) {
                columnInterpreters.put(dataTable.convertColumnIndexToModel(column), (CellInterpreter) decoder.getSelectedItem());
                dataTable.repaint(); filterRow.triggerFilterChange();
            }
        });
        var inspectorHeader = new JPanel(new BorderLayout(0, 4));
        inspectorHeader.add(inspectorHeading, BorderLayout.NORTH); inspectorHeader.add(displayControls, BorderLayout.SOUTH);
        inspectorPanel.add(inspectorHeader, BorderLayout.NORTH); inspectorPanel.add(cellDetailViewer, BorderLayout.CENTER);
        rows.setMinimumSize(new Dimension(300, 140));
        inspectorPanel.setMinimumSize(new Dimension(280, 160));
        dataSplit = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, rows, inspectorPanel);
        dataSplit.setResizeWeight(0.58); dataSplit.setBorder(BorderFactory.createEmptyBorder());
        dataSplit.addComponentListener(new ComponentAdapter() {
            private boolean positioned;
            @Override public void componentResized(ComponentEvent e) {
                if (!positioned && dataSplit.getWidth() > 0) { positioned = true; dataSplit.setDividerLocation(0.58); }
            }
        });
        inspectorDock.addActionListener(e -> updateInspectorDock());
        var queryControls = new JPanel(new BorderLayout(0, 6));
        queryControls.add(toolbar, BorderLayout.NORTH); queryControls.add(rangePanel, BorderLayout.CENTER);
        var browser = new JPanel(new BorderLayout(0, 8));
        browser.add(queryControls, BorderLayout.NORTH); browser.add(dataSplit, BorderLayout.CENTER);
        tabs.addTab("Data", browser);
        tabs.addTab("Schema", schemaView);
        storagePanel = new JPanel(new BorderLayout(0, 8));
        var storageActions = new JPanel(new FlowLayout(FlowLayout.LEFT, 8, 4));
        storageActions.add(sstButton);

        stopComparisonButton.setToolTipText("Stop after the current metadata request returns.");
        stopComparisonButton.addActionListener(e -> {
            stopComparison = true;
            stopComparisonButton.setEnabled(false);
            statusLabel.setText("Stopping comparison after the current metadata request…");
        });
        ViewerTheme.primary(sstButton);
        sstButton.setToolTipText("Read file metadata only; no record scan, flush or compaction.");
        storageButton.setToolTipText("Explicit request: may read table properties from every SST. No periodic refresh.");
        columnSizesButton.setToolTipText("Read SST metadata sequentially, one column at a time. Stop between requests.");
        storagePanel.add(storageActions, BorderLayout.NORTH);
        storagePanel.add(sstExplorer, BorderLayout.CENTER);
        var properties = new JPanel(new BorderLayout(0, 8));
        properties.add(storageButton, BorderLayout.NORTH);
        properties.add(propertyCharts, BorderLayout.CENTER);
        databasePanel = new JPanel(new BorderLayout(0, 8));
        var databaseActions = new JPanel(new FlowLayout(FlowLayout.LEFT, 8, 4));
        databaseActions.add(columnSizesButton);
        databaseActions.add(stopComparisonButton);
        databasePanel.add(databaseActions, BorderLayout.NORTH);
        databasePanel.add(columnOverview, BorderLayout.CENTER);
        storageButton.addActionListener(e -> loadStorage());
        sstButton.addActionListener(e -> loadSstLayout());
        columnSizesButton.addActionListener(e -> loadColumnSizes());
        tabs.addTab("SST explorer", storagePanel);
        tabs.addTab("Column properties", properties);
        tabs.addTab("Database overview", databasePanel);
        tabs.addChangeListener(e -> {
            columnTitle.setText(tabs.getSelectedComponent() == databasePanel ? "Database storage"
                    : selectedTable == null ? "Select a column" : selectedTable.name());
            if (busy) pendingViewLoad = true;
            else loadActiveView();
        });
        var right = new JPanel(new BorderLayout(0, 12));
        right.setBorder(BorderFactory.createEmptyBorder(16, 20, 12, 20));
        columnTitle.putClientProperty("html.disable", Boolean.TRUE);
        columnTitle.setFont(columnTitle.getFont().deriveFont(Font.BOLD, 17f));
        columnTitle.setBorder(BorderFactory.createEmptyBorder(0, 0, 0, 0));
        right.add(columnTitle, BorderLayout.NORTH);
        right.add(tabs, BorderLayout.CENTER);
        var main = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, left, right);
        main.setDividerLocation(250);
        main.setBorder(BorderFactory.createEmptyBorder());
        add(main, BorderLayout.CENTER);
        statusLabel.putClientProperty("html.disable", Boolean.TRUE);
        statusLabel.setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createMatteBorder(1, 0, 0, 0, ViewerTheme.LINE),
                BorderFactory.createEmptyBorder(10, 20, 10, 20)));
        statusLabel.setForeground(ViewerTheme.MUTED);
        statusLabel.setFont(statusLabel.getFont().deriveFont(12f));
        var statusBar = new JPanel(new BorderLayout());
        statusBar.add(statusLabel, BorderLayout.CENTER);
        progress.setIndeterminate(true);
        progress.setPreferredSize(new Dimension(100, 5));
        progress.setBorder(BorderFactory.createEmptyBorder(0, 0, 0, 12));
        var statusActions = new JPanel(new FlowLayout(FlowLayout.RIGHT, 8, 0));
        errorDetails.setVisible(false);
        errorDetails.addActionListener(e -> {
            var text = new JTextArea(lastError, 8, 65);
            text.setEditable(false); text.setLineWrap(true); text.setWrapStyleWord(true);
            JOptionPane.showMessageDialog(this, new JScrollPane(text), "Request failed", JOptionPane.ERROR_MESSAGE);
        });
        statusActions.add(errorDetails); statusActions.add(progress);
        statusBar.add(statusActions, BorderLayout.EAST);
        add(statusBar, BorderLayout.SOUTH);
        updateControls();
    }

    private void filterTableList() {
        if (busy || closing) return;
        String filter = tableFilterField.getText().strip().toLowerCase(Locale.ROOT);
        updatingList = true;
        try {
            tableListModel.clear();
            for (Table table : allTables) {
                if (table.name().toLowerCase(Locale.ROOT).contains(filter)) tableListModel.addElement(table);
            }
            if (selectedTable != null && tableListModel.contains(selectedTable)) tableList.setSelectedValue(selectedTable, true);
        } finally {
            updatingList = false;
        }
        selectTable();
    }

    private void selectTable() {
        Table table = tableList.getSelectedValue();
        if (Objects.equals(table, selectedTable)) return;
        if (selectedTable != null) savedDecoders.put(selectedTable, new HashMap<>(columnInterpreters));
        selectedTable = table;
        sstExplorer.clear();
        propertyCharts.clear();
        columnInterpreters.clear();
        if (table != null) columnInterpreters.putAll(savedDecoders.getOrDefault(table, Map.of()));
        activeQuery = null;
        currentPage = null;
        pageStarts.clear();
        pageKeyIdentities.clear();
        sorter.setRowFilter(null);
        tableModel.setDataVector(new Object[0][], new Object[0]);
        filterRow.setColumnCount(0);
        cellDetailViewer.displayCellData(null);
        startField.setText(""); endField.setText("");
        pageLabel.setText("No page loaded");
        querySummary.setText("Select a column to browse");
        clearPageFilters.setVisible(false);
        filterToggle.setText("Filter"); rangeToggle.setText("Range");
        storageView.setText("Load SST properties to inspect on-disk storage. This may read metadata from every SST.\n\nMemtables, WAL and blob files are excluded; no flush is performed.");
        columnTitle.setText(tabs.getSelectedComponent() == databasePanel ? "Database storage" : table == null ? "Select a column" : table.name());
        schemaView.display(table == null ? "" : table.name(), table == null ? null : table.schema());
        updateControls();
        loadActiveView();
    }

    private void applyQuery() {
        if (busy || selectedTable == null || closing) return;
        try {
            pageSize.commitEdit();
            ViewerQuery query = ViewerQuery.parse(selectedTable.schema(), startField.getText(), endField.getText(),
                    reverseBox.isSelected(), (Integer) pageSize.getValue());
            loadPage(query, 0, null, true);
        } catch (Exception e) {
            rangeToggle.setSelected(true); rangePanel.setVisible(true);
            showError(e);
        }
    }

    private void loadPage(ViewerQuery query, int index, Keys resume, boolean reset) {
        Table table = selectedTable;
        runTask("Loading " + table.name() + " · page " + (index + 1), () -> {
            var api = apiClient.getSyncApi(RequestContext.analytical(Duration.ofSeconds(30)));
            return query.load(api, api.getColumnId(table.name()), resume);
        }, page -> {
            boolean first = activeQuery == null;
            activeQuery = query;
            boolean bounded = query.start() != null || query.end() != null;
            querySummary.setText((bounded ? "Bounded keys" : "All keys") + (query.reverse() ? " · descending" : " · ascending"));
            rangeToggle.setText(bounded || query.reverse() ? "Range •" : "Range");
            pageLabel.setToolTipText("Applied range: " + Objects.toString(query.start(), "unbounded")
                    + " to " + Objects.toString(query.end(), "unbounded") + (query.reverse() ? " · reverse" : " · forward"));
            currentPage = page;
            pageIndex = index;
            if (reset) pageStarts.clear();
            while (pageStarts.size() > index) pageStarts.removeLast();
            pageStarts.add(resume);
            pageKeyIdentities.clear();
            if (first) {
                sorter.setRowFilter(null);
                tableModel.setDataVector(ViewerQuery.rows(table.schema(), page), ViewerQuery.columns(table.schema()).toArray());
                for (int i = 0; i < tableModel.getColumnCount(); i++) {
                    sorter.setComparator(i, (a, b) -> Arrays.compareUnsigned((byte[]) a, (byte[]) b));
                }
                filterRow.setColumns(ViewerQuery.columns(table.schema()));
                for (int i = 0; i < tableModel.getColumnCount(); i++) {
                    var column = dataTable.getColumnModel().getColumn(i);
                    column.setMinWidth(i < table.schema().keysCount() ? 90 : 170);
                    column.setPreferredWidth(i < table.schema().keysCount() ? 140 : 300);
                }
            } else {
                tableModel.setRowCount(0);
                for (Object[] row : ViewerQuery.rows(table.schema(), page)) tableModel.addRow(row);
            }
            fitValueColumn();
            cellDetailViewer.displayCellData(null);
            filterRow.triggerFilterChange();
            updatePageLabel();
            statusLabel.setText("Loaded " + table.name() + " · right-click a header to choose a decoder · sorting and filters apply to this page only");
        });
    }

    private void updatePageLabel() {
        if (currentPage != null) pageLabel.setText("Page " + (pageIndex + 1) + " · " + dataTable.getRowCount()
                + (dataTable.getRowCount() == currentPage.items().size() ? " rows" : " / " + currentPage.items().size() + " rows")
                + (currentPage.hasMore() ? "" : " · end"));
    }

    private void fitValueColumn() {
        if (selectedTable == null || !selectedTable.schema().hasValue() || dataTable.getParent() == null
                || dataTable.getColumnCount() != selectedTable.schema().keysCount() + 1) return;
        int keysWidth = 0;
        for (int i = 0; i < selectedTable.schema().keysCount(); i++) keysWidth += dataTable.getColumnModel().getColumn(i).getWidth();
        var value = dataTable.getColumnModel().getColumn(selectedTable.schema().keysCount());
        int width = Math.max(240, dataTable.getParent().getWidth() - keysWidth);
        value.setPreferredWidth(width); value.setWidth(width);
    }

    private void showPageSizes() {
        if (currentPage == null) return;
        var panel = new PageInsightsPanel(); panel.display(currentPage.items());
        var dialog = new JDialog(this, "Loaded page " + (pageIndex + 1) + " · " + selectedTable.name(), false);
        dialog.setContentPane(panel); dialog.setSize(720, 620); dialog.setLocationRelativeTo(this);
        dialog.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
        dialog.getRootPane().registerKeyboardAction(e -> dialog.dispose(), KeyStroke.getKeyStroke(KeyEvent.VK_ESCAPE, 0), JComponent.WHEN_IN_FOCUSED_WINDOW);
        dialog.setVisible(true);
    }

    private void updateInspectorDock() {
        String placement = (String) inspectorDock.getSelectedItem();
        boolean hidden = "Hidden".equals(placement), below = "Below".equals(placement);
        dataSplit.setRightComponent(hidden ? null : inspectorPanel);
        dataSplit.setDividerSize(hidden ? 0 : 6);
        dataSplit.setOrientation(below ? JSplitPane.VERTICAL_SPLIT : JSplitPane.HORIZONTAL_SPLIT);
        inspectorPanel.setBorder(BorderFactory.createCompoundBorder(BorderFactory.createMatteBorder(below ? 1 : 0, below ? 0 : 1, 0, 0, ViewerTheme.LINE),
                BorderFactory.createEmptyBorder(10, below ? 0 : 12, 0, 0)));
        dataSplit.revalidate();
        SwingUtilities.invokeLater(() -> {
            if (placement.equals(inspectorDock.getSelectedItem())) dataSplit.setDividerLocation(hidden ? 1.0 : below ? 0.55 : 0.58);
        });
        if (!hidden) updateCellDetailView();
    }

    private void applyFilters(List<String> filters) {
        List<RowFilter<Object, Object>> active = new ArrayList<>();
        for (int i = 0; i < filters.size(); i++) {
            if (!filters.get(i).isBlank() && i < tableModel.getColumnCount()) active.add(new ViewAwareFilter(filters.get(i), i, columnInterpreters));
        }
        sorter.setRowFilter(active.isEmpty() ? null : RowFilter.andFilter(active));
        clearPageFilters.setVisible(!active.isEmpty());
        filterToggle.setText(active.isEmpty() ? "Filter" : "Filter •");
        updatePageLabel();
    }

    private void updateCellDetailView() {
        if ("Hidden".equals(inspectorDock.getSelectedItem())) return;
        int row = dataTable.getSelectedRow(), column = dataTable.getSelectedColumn();
        updatingDecoder = true;
        decoder.setEnabled(column >= 0);
        if (column >= 0) decoder.setSelectedItem(columnInterpreters.getOrDefault(dataTable.convertColumnIndexToModel(column), CellInterpreter.HEX_SUMMARY));
        updatingDecoder = false;
        if (row < 0 || column < 0 || selectedTable == null) cellDetailViewer.displayCellData(null);
        else {
            int component = dataTable.convertColumnIndexToModel(column);
            int modelRow = dataTable.convertRowIndexToModel(row);
            String keyIdentity = pageKeyIdentities.computeIfAbsent(modelRow, this::keyIdentity);
            cellDetailViewer.displayCellData(tableModel.getValueAt(modelRow, component),
                    new CellDetailViewerPanel.CellContext(selectedTable.name(), selectedTable.schema(), component, keyIdentity));
        }
    }

    /** Only hashes are cached; never retain old row keys/values after page replacement. */
    private String keyIdentity(int modelRow) {
        try {
            var digest = java.security.MessageDigest.getInstance("SHA-256");
            for (int i = 0; i < selectedTable.schema().keysCount(); i++) {
                byte[] key = (byte[]) tableModel.getValueAt(modelRow, i);
                digest.update(java.nio.ByteBuffer.allocate(4).putInt(key.length).array());
                digest.update(key);
            }
            return java.util.HexFormat.of().formatHex(digest.digest());
        } catch (java.security.NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }

    private void loadInitialTables() {
        runTask("Fetching column definitions…", () -> apiClient.getSyncApi(RequestContext.latency(Duration.ofSeconds(5)))
                .getAllColumnDefinitions(), definitions -> {
            allTables.clear();
            definitions.entrySet().stream().map(e -> new Table(e.getKey(), e.getValue()))
                    .sorted(Comparator.comparing(Table::name)).forEach(allTables::add);
            statusLabel.setText(allTables.size() + " columns · select a column to browse");
            filterTableList();
        });
    }

    private void loadSstLayout() {
        if (selectedTable == null) return;
        Table table = selectedTable;
        runTask("Reading SST layout for " + table.name() + "…", () -> {
            var api = apiClient.getSyncApi(RequestContext.batch(Duration.ofSeconds(30)));
            return api.getSstMetadata(api.getColumnId(table.name()), -1);
        }, metadata -> {
            sstExplorer.display(metadata);

            statusLabel.setText("SST layout loaded · " + table.name() + " · " + metadata.files().size() + " files");
        });
    }

    private void loadColumnSizes() {
        if (busy || closing) return;
        stopComparison = false;
        comparingColumns = true;
        List<Table> columns = List.copyOf(allTables);
        runTask("Comparing SST sizes across " + columns.size() + " columns…", () -> {
            var result = new ArrayList<SstColumnOverviewPanel.Column>();
            String session = null;
            for (Table column : columns) {
                if (closing || stopComparison) break;
                int attempted = result.size() + 1;
                SwingUtilities.invokeLater(() -> {
                    if (!closing && !stopComparison) statusLabel.setText("Comparing column " + attempted + "/" + columns.size() + " · " + column.name());
                });
                SstMaintenance.Metadata metadata;
                try {
                    var api = apiClient.getSyncApi(RequestContext.batch(Duration.ofSeconds(30)));
                    metadata = api.getSstMetadata(api.getColumnId(column.name()), -1);
                } catch (Exception error) {
                    result.add(new SstColumnOverviewPanel.Column(column.name(), null, error.getMessage()));
                    continue;
                }
                if (session != null && !session.equals(metadata.session())) {
                    throw new IllegalStateException("Database reopened during inspection; refresh the overview.");
                }
                session = metadata.session();
                result.add(new SstColumnOverviewPanel.Column(column.name(), SstAnalysis.summarize(metadata.files()), null));
            }
            return result;
        }, result -> {
            columnOverview.display(result, columns.size());
            tabs.setSelectedComponent(databasePanel);
            statusLabel.setText(result.size() < columns.size() ? "Comparison stopped · partial results shown"
                    : "Column storage comparison loaded · inspect unavailable rows for errors");
        });
    }

    private void inspectColumnStorage(String name) {
        if (busy || closing) return;
        // Clearing a filter can update the list but must not accidentally start a second request.
        tableFilterField.setText("");
        // Suppress the tab listener until the requested column is selected.
        boolean wasBusy = busy;
        busy = true;
        tabs.setSelectedComponent(storagePanel);
        busy = wasBusy;
        pendingViewLoad = false;
        for (Table table : allTables) {
            if (table.name().equals(name)) {
                if (Objects.equals(selectedTable, table)) loadSstLayout();
                else tableList.setSelectedValue(table, true);
                return;
            }
        }
    }

    private void loadStorage() {
        Table table = selectedTable;
        runTask("Reading SST properties for " + table.name() + "…", () -> {
            var api = apiClient.getSyncApi(RequestContext.analytical(Duration.ofSeconds(30)));
            return api.getTableProperties(api.getColumnId(table.name()));
        }, p -> {
            propertyCharts.display(p);
            storageView.setText("ALL SST PROPERTIES IN COLUMN · " + table.name() + "\n\n"
                    + "SST files:                 " + p.tableCount()
                    + "\nPhysical entries:          " + p.numEntries()
                    + "\nPoint deletions:           " + p.numDeletions()
                    + "\nRange deletions:           " + p.numRangeDeletions()
                    + "\nMerge operands:            " + p.numMergeOperands()
                    + "\nData bytes:                " + p.dataSize()
                    + "\nIndex bytes:               " + p.indexSize()
                    + "\nFilter bytes:              " + p.filterSize()
                    + "\nRaw key bytes:             " + p.rawKeySize()
                    + "\nRaw value bytes:           " + p.rawValueSize()
                    + "\nData blocks:               " + p.numDataBlocks()
                    + "\n\nCompression (SST counts): " + p.compressions()
                    + "\nComparators: " + p.comparators()
                    + "\nMerge operators: " + p.mergeOperators()
                    + "\nFilter policies: " + p.filterPolicies()
                    + "\n\nPhysical entries include obsolete versions and tombstones, not live row counts."
                    + "\nBucket contents are not counted individually. Memtables, WAL and blob files are excluded."
                    + "\nThis is an observation of the current file set, not a pinned snapshot.");
            storageView.setCaretPosition(0);
            statusLabel.setText("SST properties loaded · " + table.name());
        });
    }

    /** One owned request at a time. Closing waits for its actual completion before closing the connection. */
    private <T> void runTask(String status, Callable<T> task, Consumer<T> success) {
        if (busy || closing) return;
        busy = true;
        updateControls();
        statusLabel.setText(status);
        statusLabel.setForeground(ViewerTheme.MUTED);
        errorDetails.setVisible(false);
        new SwingWorker<T, Void>() {
            @Override protected T doInBackground() throws Exception { return task.call(); }
            @Override protected void done() {
                busy = false;
                if (closing) { closeConnection(); return; }
                try { success.accept(get()); }
                catch (Exception e) { showError(e.getCause() == null ? e : e.getCause()); }
                finally {
                    comparingColumns = false;
                    updateControls();
                    if (pendingViewLoad) {
                        pendingViewLoad = false;
                        loadActiveView();
                    }
                }
            }
        }.execute();
    }

    private void loadActiveView() {
        if (busy || closing || selectedTable == null) return;
        if (tabs.getSelectedIndex() == 0 && activeQuery == null) applyQuery();
        else if (tabs.getSelectedComponent() == storagePanel && !sstExplorer.hasMetadata()) loadSstLayout();
    }

    private void updateControls() {
        boolean ready = !busy && !closing;
        progress.setVisible(busy && !closing);
        tableList.setEnabled(ready);
        tableFilterField.setEnabled(ready);
        refreshTablesButton.setEnabled(ready);
        columnSizesButton.setEnabled(ready && !allTables.isEmpty());
        stopComparisonButton.setVisible(comparingColumns);
        stopComparisonButton.setEnabled(busy && comparingColumns && !stopComparison && !closing);
        boolean selected = ready && selectedTable != null;
        for (JComponent component : List.of(startField, endField, reverseBox, pageSize, browseButton, storageButton, sstButton)) component.setEnabled(selected);
        rangeToggle.setEnabled(selected);
        filterToggle.setEnabled(selected && currentPage != null);
        pageSizesButton.setEnabled(selected && currentPage != null);
        previousButton.setEnabled(selected && currentPage != null && pageIndex > 0);
        nextButton.setEnabled(selected && currentPage != null && currentPage.hasMore());
    }

    private void showError(Throwable error) {
        lastError = Objects.toString(error.getMessage(), error.getClass().getSimpleName());
        statusLabel.setText("Request failed · " + lastError);
        statusLabel.setForeground(new Color(180, 50, 50));
        errorDetails.setVisible(true);
    }

    @Override public void dispose() {
        if (closing) return;
        closing = true;
        super.dispose();
        if (!busy) closeConnection();
    }

    private void closeConnection() {
        // Do not interrupt native work or block the EDT during connection shutdown.
        Thread.ofPlatform().name("viewer-close").start(() -> {
            try { apiClient.close(); }
            catch (Exception e) { e.printStackTrace(); }
        });
    }

	/** Creates a hierarchical context menu to select an interpreter for a column. */
	private JPopupMenu createColumnContextMenu(int viewColumnIndex) {
		int modelColumnIndex = dataTable.convertColumnIndexToModel(viewColumnIndex);
		JPopupMenu menu = new JPopupMenu();

		JLabel title = new JLabel(" View Column As...");
		title.setFont(title.getFont().deriveFont(Font.BOLD));
		menu.add(title);
		menu.addSeparator();

		ButtonGroup group = new ButtonGroup();
		CellInterpreter currentInterpreter = columnInterpreters.getOrDefault(modelColumnIndex, CellInterpreter.HEX_SUMMARY);

		// Helper to create and add radio button menu items
		BiConsumer<JComponent, CellInterpreter> addMenuItem = (JComponent parent, CellInterpreter interpreter) -> {
			JRadioButtonMenuItem item = new JRadioButtonMenuItem(interpreter.toString(), interpreter == currentInterpreter);
			item.addActionListener(e -> {
				columnInterpreters.put(modelColumnIndex, interpreter);
                updateCellDetailView();
				dataTable.repaint();
				// Also re-apply filters as the view has changed
				filterRow.triggerFilterChange();
			});
			group.add(item);
			parent.add(item);
		};

		// Add top-level items
		addMenuItem.accept(menu, CellInterpreter.HEX_SUMMARY);
		addMenuItem.accept(menu, CellInterpreter.TEXT_UTF8);
		addMenuItem.accept(menu, CellInterpreter.JSON);
		addMenuItem.accept(menu, CellInterpreter.BSON);
		menu.addSeparator();

		// Add numeric sub-menus
		JMenu int32Menu = new JMenu("Numeric (32-bit)");
		addMenuItem.accept(int32Menu, CellInterpreter.NUM_SIGNED_BE_32);
		addMenuItem.accept(int32Menu, CellInterpreter.NUM_UNSIGNED_BE_32);
		addMenuItem.accept(int32Menu, CellInterpreter.NUM_SIGNED_LE_32);
		addMenuItem.accept(int32Menu, CellInterpreter.NUM_UNSIGNED_LE_32);
		menu.add(int32Menu);

		JMenu long64Menu = new JMenu("Numeric (64-bit)");
		addMenuItem.accept(long64Menu, CellInterpreter.NUM_SIGNED_BE_64);
		addMenuItem.accept(long64Menu, CellInterpreter.NUM_UNSIGNED_BE_64);
		addMenuItem.accept(long64Menu, CellInterpreter.NUM_SIGNED_LE_64);
		addMenuItem.accept(long64Menu, CellInterpreter.NUM_UNSIGNED_LE_64);
		menu.add(long64Menu);

		JMenu u128Menu = new JMenu("Numeric (128-bit)");
		addMenuItem.accept(u128Menu, CellInterpreter.NUM_UNSIGNED_BE_128);
		addMenuItem.accept(u128Menu, CellInterpreter.NUM_UNSIGNED_LE_128);
		menu.add(u128Menu);

		return menu;
	}

	// --- UTILITY METHODS AND INNER CLASSES ---

	/**
	 * A RowFilter that matches if a cell's INTERPRETED value contains the filter text.
	 * This filter is aware of the view format selected for each column.
	 */
	private static class ViewAwareFilter extends RowFilter<Object, Object> {
		private final String filterText;
		private final int columnIndex;
		private final Map<Integer, CellInterpreter> interpreters;

		ViewAwareFilter(String filterText, int modelColumnIndex, Map<Integer, CellInterpreter> interpreters) {
			this.filterText = filterText.toLowerCase(Locale.ROOT);
			this.columnIndex = modelColumnIndex;
			this.interpreters = interpreters;
		}

		@Override
		public boolean include(Entry<?, ?> entry) {
			Object rawValue = entry.getValue(columnIndex);
			if (!(rawValue instanceof byte[] cellData)) {
				return false; // This filter only works on byte arrays.
			}

			// Find the correct interpreter for this column, defaulting to HEX_SUMMARY.
			CellInterpreter interpreter = interpreters.getOrDefault(columnIndex, CellInterpreter.HEX_SUMMARY);

			// Use the interpreter to get the displayed string.
			String interpretedValue = interpreter.interpret(cellData);

			// Perform the case-insensitive 'contains' check.
			return interpretedValue.toLowerCase(Locale.ROOT).contains(this.filterText);
		}
	}

	/**
	 * A UI panel containing a row of text fields for filtering, one for each table column.
	 */
	private static class ColumnFilterRow extends JPanel {
		private final Consumer<List<String>> onFilterChange;
		private final List<JTextField> filterFields = new ArrayList<>();
        private boolean changing;

		public ColumnFilterRow(Consumer<List<String>> onFilterChange) {
			this.onFilterChange = onFilterChange;
			setLayout(new BoxLayout(this, BoxLayout.X_AXIS));
			setBorder(BorderFactory.createEmptyBorder(0, 0, 8, 0));
		}

		public void setColumnCount(int count) {
			removeAll();
			filterFields.clear();
			if (count > 0) {
				for (int i = 0; i < count; i++) {
					JTextField field = new JTextField();
                    field.putClientProperty("JTextField.placeholderText", "Filter column " + (i + 1) + "…");
                    field.putClientProperty("JTextField.showClearButton", true);
					field.getDocument().addDocumentListener(new DocumentListener() {
						@Override public void insertUpdate(DocumentEvent e) { triggerFilterChange(); }
						@Override public void removeUpdate(DocumentEvent e) { triggerFilterChange(); }
						@Override public void changedUpdate(DocumentEvent e) { triggerFilterChange(); }
					});
					filterFields.add(field);
					add(field);
				}
			}
			revalidate();
			repaint();
		}

        public void setColumns(List<String> columns) {
            setColumnCount(columns.size());
            for (int i = 0; i < columns.size(); i++) {
                filterFields.get(i).putClientProperty("JTextField.placeholderText", columns.get(i));
                filterFields.get(i).setToolTipText("Filter " + columns.get(i) + " on this page, using its displayed preview");
            }
        }

        public void clear() {
            changing = true;
            try { for (var field : filterFields) field.setText(""); }
            finally { changing = false; }
            triggerFilterChange();
        }

		public void triggerFilterChange() {
            if (changing) return;
			List<String> filters = filterFields.stream().map(JTextField::getText).toList();
			onFilterChange.accept(filters);
		}
	}
}
