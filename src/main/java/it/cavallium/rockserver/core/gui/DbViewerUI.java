package it.cavallium.rockserver.core.gui;

import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.swing.*;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.event.ListSelectionListener;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.util.List;
import javax.swing.table.TableRowSorter;

/**
 * The main database viewer UI.
 * It features a left panel with a list of tables and a right panel to display
 * the selected table's data. All data fetching is done via the CustomDbApiClient
 * on background threads.
 */
public class DbViewerUI extends JFrame {

	record Table(String name, ColumnSchema schema) {

		@Override
		public String toString() {
			var sb = new StringBuilder();
			sb.append(name);
			sb.append(" ");
			sb.append( "(");
			if (schema.keys().size() == 1) {
				sb.append("key");
			} else {
				sb.append(schema.keys().size());
				sb.append(" keys");
			}
			if  (schema.hasValue()) {
				sb.append(", value");
			}
			sb.append(")");
			return sb.toString();
		}
	}
	record Column(String name, Long colId, ColumnSchema schema) {}
	private record TableData(Object[][] rows, List<String> columns, String tableName) {}


	private final RocksDBConnection apiClient;
	private JList<Table> tableList;
	private DefaultListModel<Table> tableListModel;
	private JTextField tableFilterField;
	private List<Table> allTables = new ArrayList<>();
	private ListSelectionListener tableSelectionListener;

	// ** NEW FIELD to track the currently displayed data's source table **
	private Table currentlyDisplayedTable;
	private volatile boolean isLoadingData = false; // Prevents concurrent loads

	private JTable dataTable;
	private DefaultTableModel tableModel;
	private JLabel statusLabel;
	private CellDetailViewerPanel cellDetailViewer;
	private TableRowSorter<DefaultTableModel> sorter;

	// --- UI COMPONENT ---
	private ColumnFilterRow filterRow;

    // --- KEY FIELD ---
    // Stores the user's interpreter choice for each column model index.
    private final Map<Integer, CellInterpreter> columnInterpreters = new ConcurrentHashMap<>();

	public DbViewerUI(RocksDBConnection apiClient) {
		this.apiClient = apiClient;

		setTitle("DB Viewer");
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setMinimumSize(new Dimension(900, 700));
		setLocationRelativeTo(null); // Center on screen

		initComponents();
		loadInitialTables();
	}

	private void initComponents() {
		// --- Left Panel (Table List & Filter) ---
		tableListModel = new DefaultListModel<>();
		tableList = new JList<>(tableListModel);
		tableFilterField = new JTextField();

		JPanel leftPanel = new JPanel(new BorderLayout(5, 5));
		leftPanel.setBorder(BorderFactory.createTitledBorder("Available Tables"));
		JPanel tableFilterPanel = new JPanel(new BorderLayout(5, 0));
		tableFilterPanel.add(new JLabel("Filter:"), BorderLayout.WEST);
		tableFilterPanel.add(tableFilterField, BorderLayout.CENTER);
		leftPanel.add(tableFilterPanel, BorderLayout.NORTH);
		leftPanel.add(new JScrollPane(tableList), BorderLayout.CENTER);


		// --- Right Panel (Table Data) ---
		tableModel = new DefaultTableModel();
		sorter = new TableRowSorter<>(tableModel);
		dataTable = new JTable(tableModel) {
				// Prevent cells from being editable.
				@Override public boolean isCellEditable(int row, int column) { return false; }
		};
		JPanel tablePanel = new JPanel(new BorderLayout());

		// Setup filter row and table
		filterRow = new ColumnFilterRow(this::applyFilters);
		tablePanel.add(filterRow, BorderLayout.NORTH);
		tablePanel.add(new JScrollPane(dataTable), BorderLayout.CENTER);

		// Apply the renderer & sorter
		dataTable.setRowSorter(sorter);
		dataTable.setDefaultRenderer(Object.class, new PerColumnCellRenderer(columnInterpreters));
		dataTable.setFillsViewportHeight(true);

		cellDetailViewer = new CellDetailViewerPanel();

		// --- Split Pane to hold both panels ---
		JSplitPane rightSplitPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT, tablePanel, cellDetailViewer);
		rightSplitPane.setResizeWeight(0.7);
		JSplitPane mainSplitPane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, leftPanel, rightSplitPane);
		mainSplitPane.setDividerLocation(250);

		// --- Status Bar ---
		statusLabel = new JLabel("Ready.");
		statusLabel.setBorder(BorderFactory.createEmptyBorder(5, 10, 5, 10));

		// --- Add Listeners ---
		this.tableSelectionListener = e -> {
			if (!e.getValueIsAdjusting()) loadDataForSelectedTable();
		};
		tableList.addListSelectionListener(tableSelectionListener);

		// **KEY FIX**: Add a MouseListener to handle clicks, even on already-selected items.
		tableList.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseClicked(MouseEvent e) {
				loadDataForSelectedTable();
			}
		});

		tableFilterField.getDocument().addDocumentListener(new DocumentListener() {
			@Override public void insertUpdate(DocumentEvent e) { filterTableList(); }
			@Override public void removeUpdate(DocumentEvent e) { filterTableList(); }
			@Override public void changedUpdate(DocumentEvent e) { filterTableList(); }
		});

		// Cell inspector listener
		dataTable.getSelectionModel().addListSelectionListener(e -> { if (!e.getValueIsAdjusting()) updateCellDetailView(); });
		dataTable.getColumnModel().getSelectionModel().addListSelectionListener(e -> { if (!e.getValueIsAdjusting()) updateCellDetailView(); });

		// Right-click listener for table header
		dataTable.getTableHeader().addMouseListener(new MouseAdapter() {
			@Override
			public void mousePressed(MouseEvent e) {
				if (SwingUtilities.isRightMouseButton(e)) {
					int columnIndex = dataTable.columnAtPoint(e.getPoint());
					if (columnIndex != -1) {
						createColumnContextMenu(columnIndex).show(e.getComponent(), e.getX(), e.getY());
					}
				}
			}
		});

		// --- Add components to frame ---
		setLayout(new BorderLayout());
		add(mainSplitPane, BorderLayout.CENTER);
		add(statusLabel, BorderLayout.SOUTH);
	}

	/**
	 * Filters the table list locally without triggering a data refetch on every keystroke.
	 * A data refresh is only triggered if the selected item changes as a result of the filter.
	 */
	private void filterTableList() {
		String filterText = tableFilterField.getText().toLowerCase().trim();
		Table oldSelectedValue = tableList.getSelectedValue();

		// ** KEY CHANGE: Temporarily disable the listener to prevent re-fetching on every change **
		tableList.removeListSelectionListener(tableSelectionListener);

		try {
			tableListModel.clear();
			for (Table table : allTables) {
				if (table.name().toLowerCase().contains(filterText)) {
					tableListModel.addElement(table);
				}
			}

			// Restore selection if the previously selected item is still visible
			if (oldSelectedValue != null && tableListModel.contains(oldSelectedValue)) {
				tableList.setSelectedValue(oldSelectedValue, true);
			} else if (!tableListModel.isEmpty()) {
				// If the old selection is gone, just highlight the first item.
				// This does NOT trigger a load.
				tableList.setSelectedIndex(0);
			}
		} finally {
			// ** ALWAYS re-add the listener **
			tableList.addListSelectionListener(tableSelectionListener);
		}
		// **REMOVED** The problematic block that called loadDataForSelectedTable is gone.
	}

	/**
	 * Applies all active column filters to the table's row sorter.
	 * This method now uses a ViewAwareFilter that filters based on the
	 * displayed text for each column.
	 * @param filters A list of filter strings, one for each column.
	 */
	private void applyFilters(List<String> filters) {
		List<RowFilter<Object, Object>> activeFilters = new ArrayList<>();

		for (int i = 0; i < filters.size(); i++) {
			String filterText = filters.get(i);
			if (filterText != null && !filterText.isBlank()) {
				if (i < dataTable.getColumnCount()) {
					int modelColumnIndex = dataTable.convertColumnIndexToModel(i);
					// **KEY CHANGE**: Use the new ViewAwareFilter
					activeFilters.add(new ViewAwareFilter(filterText, modelColumnIndex, columnInterpreters));
				}
			}
		}

		if (activeFilters.isEmpty()) {
			sorter.setRowFilter(null);
		} else {
			sorter.setRowFilter(RowFilter.andFilter(activeFilters));
		}
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

	/** When a cell is selected, get its raw data and pass it to the viewer. */
	private void updateCellDetailView() {
		int selectedRow = dataTable.getSelectedRow();
		int selectedColumn = dataTable.getSelectedColumn();

		if (selectedRow != -1 && selectedColumn != -1) {
			int modelRow = dataTable.convertRowIndexToModel(selectedRow);
			int modelColumn = dataTable.convertColumnIndexToModel(selectedColumn);
			Object cellValue = tableModel.getValueAt(modelRow, modelColumn);
			cellDetailViewer.displayCellData(cellValue);
		}
	}

	/**
	 * Fetches data for the currently selected table on a background thread.
	 */
	private void loadDataForSelectedTable() {
		final Table selectedTable = tableList.getSelectedValue();

		// **KEY FIX**: If we are asked to load the table that is already displayed, do nothing.
		if (selectedTable == currentlyDisplayedTable) {
			return;
		}

		// If selection is cleared, clear the view and update the state.
		if (selectedTable == null) {
			tableModel.setDataVector(new Object[][]{}, new Object[]{"No table selected"});
			filterRow.setColumnCount(0);
			this.currentlyDisplayedTable = null;
			return;
		}

		// **KEY FIX 2**: Set the lock
		isLoadingData = true;

		columnInterpreters.clear();
		this.currentlyDisplayedTable = null; // Mark as loading (neither old nor new)

		statusLabel.setText("Loading data for table '" + selectedTable.name() + "'...");
		tableList.setEnabled(false);
		tableFilterField.setEnabled(false);
		dataTable.setEnabled(false);

		tableModel.setDataVector(new Object[][]{{"Loading..."}}, new Object[]{"Status"});
		filterRow.setColumnCount(0);

		new SwingWorker<TableData, Void>() {
			@Override
			protected TableData doInBackground() throws Exception {
				var colId = apiClient.getSyncApi().getColumnId(selectedTable.name());
				var column = new Column(selectedTable.name(), colId, selectedTable.schema());

				try (Stream<KV> dataStream = apiClient.getSyncApi().getRange(0, column.colId, null, null, false, RequestType.allInRange(), 5_000)) {
					var rows = dataStream.toList();
					int numCols = column.schema().keysCount() + (column.schema.hasValue() ? 1 : 0);
					Object[][] rowData = new Object[rows.size()][numCols];

					for (int rowI = 0; rowI < rows.size(); rowI++) {
						var row = rows.get(rowI);
						Keys rowKeys = row.keys();
						for (int colI = 0; colI < column.schema.keysCount(); colI++) {
							rowData[rowI][colI] = rowKeys.keys()[colI].toByteArray();
						}
						if (column.schema.hasValue()) {
							rowData[rowI][column.schema.keysCount()] = row.value().toByteArray();
						}
					}
					List<String> columnNames = schemaToColumns(column.schema());
					return new TableData(rowData, columnNames, selectedTable.name());
				}
			}

			@Override
			protected void done() {
				try {
					TableData result = get();
					tableModel.setDataVector(result.rows(), result.columns().toArray());
					filterRow.setColumnCount(result.columns().size());
					statusLabel.setText("Displaying data for table '" + result.tableName() + "'. Right-click a column header to change view.");
					// **KEY FIX**: On success, update the state to the newly loaded table.
					currentlyDisplayedTable = selectedTable;
				} catch (Exception e) {
					handleError(e, "Failed to load data for table '" + selectedTable.name() + "'");
					tableModel.setDataVector(new Object[][]{}, new Object[]{"Error"});
					filterRow.setColumnCount(0);
					// On failure, currentlyDisplayedTable remains null, allowing a retry.
				} finally {
					// **KEY FIX 3**: Release the lock in the finally block
					isLoadingData = false;
					tableList.setEnabled(true);
					tableFilterField.setEnabled(true);
					dataTable.setEnabled(true);
				}
			}
		}.execute();
	}

	private List<String> schemaToColumns(ColumnSchema schema) {
		var columns = new ArrayList<String>();
		for (int i = 0; i < schema.fixedLengthKeysCount(); i++) {
			columns.add("Key" + i + " (size=" + schema.key(i) + ")");
		}
		for (int i = 0; i < schema.variableLengthKeysCount(); i++) {
			columns.add("Key" + (schema.fixedLengthKeysCount() + i) + " (size=" + schema.variableTailKey(i) + ")");
		}
		if (schema.hasValue()) {
			columns.add("Value");
		}
		return columns;
	}


	private void handleError(Exception e, String message) {
		e.printStackTrace();
		Throwable cause = e.getCause() != null ? e.getCause() : e;
		statusLabel.setText("Error: " + message);
		JOptionPane.showMessageDialog(this, message + ":\n" + cause.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
	}

	/**
	 * Fetches the list of tables from the API client on a background thread.
	 */
	private void loadInitialTables() {
		statusLabel.setText("Fetching table list...");
		tableList.setEnabled(false);
		tableFilterField.setEnabled(false);

		new SwingWorker<Map<String, ColumnSchema>, Void>() {
			@Override
			protected Map<String, ColumnSchema> doInBackground() throws Exception {
				return apiClient.getSyncApi().getAllColumnDefinitions();
			}

			@Override
			protected void done() {
				try {
					allTables.clear();
					get().entrySet().stream()
							.map(e -> new Table(e.getKey(), e.getValue()))
							.sorted(Comparator.comparing(Table::name))
							.forEach(allTables::add);

					filterTableList(); // Use the filter to perform initial population

					statusLabel.setText("Ready. Select a table to view its content.");
				} catch (Exception e) {
					handleError(e, "Failed to load table list");
				} finally {
					tableList.setEnabled(true);
					tableFilterField.setEnabled(true);
				}
			}
		}.execute();
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
			this.filterText = filterText.toLowerCase();
			this.columnIndex = modelColumnIndex;
			this.interpreters = interpreters;
		}

		@Override
		public boolean include(Entry<? extends Object, ? extends Object> entry) {
			Object rawValue = entry.getValue(columnIndex);
			if (!(rawValue instanceof byte[] cellData)) {
				return false; // This filter only works on byte arrays.
			}

			// Find the correct interpreter for this column, defaulting to HEX_SUMMARY.
			CellInterpreter interpreter = interpreters.getOrDefault(columnIndex, CellInterpreter.HEX_SUMMARY);

			// Use the interpreter to get the displayed string.
			String interpretedValue = interpreter.interpret(cellData);

			// Perform the case-insensitive 'contains' check.
			return interpretedValue.toLowerCase().contains(this.filterText);
		}
	}

	/**
	 * A UI panel containing a row of text fields for filtering, one for each table column.
	 */
	private static class ColumnFilterRow extends JPanel {
		private final Consumer<List<String>> onFilterChange;
		private final List<JTextField> filterFields = new ArrayList<>();

		public ColumnFilterRow(Consumer<List<String>> onFilterChange) {
			this.onFilterChange = onFilterChange;
			setLayout(new BoxLayout(this, BoxLayout.X_AXIS));
			setBorder(BorderFactory.createTitledBorder("Filter by Viewed Content (per column)"));
		}

		public void setColumnCount(int count) {
			removeAll();
			filterFields.clear();
			if (count > 0) {
				for (int i = 0; i < count; i++) {
					JTextField field = new JTextField();
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

		public void triggerFilterChange() {
			List<String> filters = filterFields.stream().map(JTextField::getText).toList();
			onFilterChange.accept(filters);
		}
	}
}