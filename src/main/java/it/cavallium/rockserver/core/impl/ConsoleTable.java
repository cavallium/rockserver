package it.cavallium.rockserver.core.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ConsoleTable {
	private final List<String> headers = new ArrayList<>();
	private final List<List<String>> rows = new ArrayList<>();

	public void setHeaders(String... headers) {
		this.headers.clear();
		this.headers.addAll(Arrays.asList(headers));
	}

	public void addRow(String... row) {
		this.rows.add(Arrays.asList(row));
	}

	@Override
	public String toString() {
		if (headers.isEmpty() && rows.isEmpty()) return "";

		int columns = headers.size();
		for (List<String> row : rows) {
			columns = Math.max(columns, row.size());
		}

		int[] widths = new int[columns];
		for (int i = 0; i < headers.size(); i++) {
			widths[i] = Math.max(widths[i], headers.get(i).length());
		}
		for (List<String> row : rows) {
			for (int i = 0; i < row.size(); i++) {
				if (row.get(i) != null) {
					widths[i] = Math.max(widths[i], row.get(i).length());
				}
			}
		}

		// Add padding
		for (int i = 0; i < widths.length; i++) {
			widths[i] += 2; // 1 space padding each side
		}

		StringBuilder sb = new StringBuilder();

		// Top border
		appendSeparator(sb, widths, "┌", "─", "┬", "┐");

		// Headers
		if (!headers.isEmpty()) {
			appendRow(sb, widths, headers);
			appendSeparator(sb, widths, "├", "─", "┼", "┤");
		}

		// Rows
		for (List<String> row : rows) {
			appendRow(sb, widths, row);
		}

		// Bottom border
		appendSeparator(sb, widths, "└", "─", "┴", "┘");

		return sb.toString();
	}

	private void appendRow(StringBuilder sb, int[] widths, List<String> row) {
		sb.append("│");
		for (int i = 0; i < widths.length; i++) {
			String cell = (i < row.size()) ? row.get(i) : "";
			if (cell == null) cell = "";
			sb.append(String.format(" %-" + (widths[i] - 2) + "s ", cell));
			sb.append("│");
		}
		sb.append("\n");
	}

	private void appendSeparator(StringBuilder sb, int[] widths, String left, String mid, String cross, String right) {
		sb.append(left);
		for (int i = 0; i < widths.length; i++) {
			sb.append(mid.repeat(widths[i]));
			if (i < widths.length - 1) {
				sb.append(cross);
			}
		}
		sb.append(right).append("\n");
	}
}
