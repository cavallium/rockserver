package it.cavallium.rockserver.core.gui;

import javax.swing.*;
import javax.swing.table.DefaultTableCellRenderer;
import java.awt.*;
import java.util.Map;

/**
 * A renderer that formats cell data (byte[]) based on a user-defined
 * interpreter for each column.
 */
public class PerColumnCellRenderer extends DefaultTableCellRenderer {

	private final Map<Integer, CellInterpreter> columnInterpreters;

	public PerColumnCellRenderer(Map<Integer, CellInterpreter> columnInterpreters) {
		this.columnInterpreters = columnInterpreters;
	}

	@Override
	public Component getTableCellRendererComponent(JTable table, Object value,
			boolean isSelected, boolean hasFocus,
			int row, int column) {
		// Let the superclass handle selection colors, fonts, etc.
		super.getTableCellRendererComponent(table, value, isSelected, hasFocus, row, column);

		if (!(value instanceof byte[] bytes)) {
			// Handle non-byte[] data gracefully, though our model is all bytes.
			setText(value == null ? "(null)" : value.toString());
			return this;
		}

		// Get the interpreter for this column, defaulting to HEX_SUMMARY.
		// We use model index for consistency.
		int modelColumn = table.convertColumnIndexToModel(column);
		CellInterpreter interpreter = columnInterpreters.getOrDefault(
				modelColumn,
				CellInterpreter.HEX_SUMMARY
		);

		// Format the bytes and set the text
		setText(interpreter.interpret(bytes));

		return this;
	}
}