package it.cavallium.rockserver.core.gui;

import java.awt.datatransfer.StringSelection;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.Date;
import java.util.HexFormat;
import javax.swing.*;
import javax.swing.border.TitledBorder;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeCellRenderer;
import javax.swing.tree.DefaultTreeModel;
import java.awt.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import javax.swing.tree.TreePath;
import org.bson.BsonBinaryReader;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.json.JsonWriterSettings;

/**
 * A panel that displays the details of a single cell's data in various formats.
 * Features a rich, navigable JTree for BSON data with a right-click copy menu.
 * NOTE: The BSON viewer requires the 'org.mongodb:mongodb-driver-core' dependency.
 */
public class CellDetailViewerPanel extends JPanel {

	private final JTextArea stringView;
	private final JTextArea hexView;
	private final JTextArea numericView;
	private final JLabel typeLabel;

	// BSON view components
	private final JPanel bsonPanel;
	private final JTree bsonTree;
	private final JLabel bsonErrorLabel;
	private final CardLayout bsonCardLayout;
	private BsonDocument currentBsonDocument; // Stores the last valid BSON doc

	private static final String BSON_TREE_VIEW = "BSON_TREE_VIEW";
	private static final String BSON_ERROR_VIEW = "BSON_ERROR_VIEW";
	private static final JsonWriterSettings BSON_PRETTY_PRINT_SETTINGS = JsonWriterSettings.builder().indent(true).build();
	private static final JsonWriterSettings BSON_COMPACT_PRINT_SETTINGS = JsonWriterSettings.builder().indent(false).build();

	public CellDetailViewerPanel() {
		super(new BorderLayout(5, 5));
		setBorder(BorderFactory.createTitledBorder(
				BorderFactory.createEtchedBorder(),
				"Cell Inspector",
				TitledBorder.CENTER,
				TitledBorder.TOP
		));

		typeLabel = new JLabel("Type: (no cell selected)");
		typeLabel.setBorder(BorderFactory.createEmptyBorder(5, 5, 5, 5));

		stringView = createReadOnlyTextArea();
		hexView = createReadOnlyTextArea();
		numericView = createReadOnlyTextArea();

		// Use a monospaced font for hex and numeric views for alignment
		Font monoFont = new Font(Font.MONOSPACED, Font.PLAIN, 12);
		hexView.setFont(monoFont);
		numericView.setFont(monoFont);

		// --- Set up the advanced BSON view ---
		bsonCardLayout = new CardLayout();
		bsonPanel = new JPanel(bsonCardLayout);

		bsonTree = new JTree(new DefaultTreeModel(new DefaultMutableTreeNode("No BSON data")));
		bsonTree.setCellRenderer(new BsonTreeCellRenderer());
		bsonTree.setRootVisible(false); // Hide the artificial root
		JScrollPane bsonTreeScroll = new JScrollPane(bsonTree);

		bsonErrorLabel = new JLabel("(Not a valid BSON document)", SwingConstants.CENTER);
		bsonErrorLabel.setFont(bsonErrorLabel.getFont().deriveFont(Font.ITALIC));
		JPanel bsonErrorPanel = new JPanel(new BorderLayout());
		bsonErrorPanel.add(bsonErrorLabel, BorderLayout.CENTER);

		bsonPanel.add(bsonTreeScroll, BSON_TREE_VIEW);
		bsonPanel.add(bsonErrorPanel, BSON_ERROR_VIEW);

		// **KEY CHANGE**: Add context menu listener
		bsonTree.addMouseListener(new MouseAdapter() {
			@Override
			public void mousePressed(MouseEvent e) {
				if (SwingUtilities.isRightMouseButton(e)) {
					TreePath path = bsonTree.getPathForLocation(e.getX(), e.getY());
					if (path != null) {
						bsonTree.setSelectionPath(path);
					}
					createBsonContextMenu(path).show(e.getComponent(), e.getX(), e.getY());
				}
			}
		});
		// --- End BSON view setup ---

		JTabbedPane tabbedPane = new JTabbedPane();
		tabbedPane.addTab("Text", new JScrollPane(stringView));
		tabbedPane.addTab("Hex", new JScrollPane(hexView));
		tabbedPane.addTab("Numeric", new JScrollPane(numericView));
		tabbedPane.addTab("BSON", bsonPanel); // Add the panel with CardLayout

		add(typeLabel, BorderLayout.NORTH);
		add(tabbedPane, BorderLayout.CENTER);
	}

	private JTextArea createReadOnlyTextArea() {
		JTextArea textArea = new JTextArea();
		textArea.setEditable(false);
		textArea.setWrapStyleWord(true);
		textArea.setLineWrap(true);
		textArea.setBorder(BorderFactory.createEmptyBorder(5, 5, 5, 5));
		return textArea;
	}

	/**
	 * Updates the viewer to display information about the provided data object.
	 * @param data The raw data object from the table cell.
	 */
	public void displayCellData(Object data) {
		if (data == null) {
			typeLabel.setText("Type: NULL");
			stringView.setText("(null)");
			hexView.setText("");
			numericView.setText("");
			displayBsonTree(null); // Clear BSON view
			return;
		}

		typeLabel.setText("Type: " + data.getClass().getSimpleName());
		byte[] bytes = convertToByteArray(data);
		if (bytes != null) {
			stringView.setText(new String(bytes, StandardCharsets.UTF_8));
			hexView.setText(toHexDump(bytes));
			numericView.setText(interpretAsNumbers(bytes));
			displayBsonTree(bytes); // Update BSON tree
		} else {
			stringView.setText(data.toString());
			hexView.setText("(Not a byte-convertible type)");
			numericView.setText(data instanceof Number ? data.toString() : "(Not a numeric type)");
			displayBsonTree(null);
		}

		// Reset scroll position to the top for all views
		SwingUtilities.invokeLater(() -> {
			stringView.setCaretPosition(0);
			hexView.setCaretPosition(0);
			numericView.setCaretPosition(0);
		});
	}

	/**
	 * Parses byte array as BSON and updates the JTree view.
	 * Shows an error message via CardLayout on failure.
	 */
	private void displayBsonTree(byte[] bytes) {
		this.currentBsonDocument = null; // Clear previous state

		if (bytes == null || bytes.length == 0) {
			bsonErrorLabel.setText("(No data)");
			bsonCardLayout.show(bsonPanel, BSON_ERROR_VIEW);
			return;
		}

		try {
			var reader = new BsonBinaryReader(ByteBuffer.wrap(bytes));
			var codec = new BsonDocumentCodec();
			BsonDocument document = codec.decode(reader, DecoderContext.builder().build());

			this.currentBsonDocument = document; // **KEY CHANGE**: Store the document

			DefaultMutableTreeNode root = new DefaultMutableTreeNode("BSON Root");
			buildTree(document, root);

			bsonTree.setModel(new DefaultTreeModel(root));
			// Expand the first level for better initial view
			for (int i = 0; i < bsonTree.getRowCount(); i++) {
				if (bsonTree.getPathForRow(i).getPathCount() <= 2) {
					bsonTree.expandRow(i);
				}
			}
			bsonCardLayout.show(bsonPanel, BSON_TREE_VIEW);
		} catch (Exception e) {
			bsonErrorLabel.setText("<html><center>(Not a valid BSON document)<br>Error: " + e.getClass().getSimpleName() + "</center></html>");
			bsonCardLayout.show(bsonPanel, BSON_ERROR_VIEW);
		}
	}

	/**
	 * Recursively populates a JTree node from a BSON value.
	 */
	private void buildTree(BsonValue bsonValue, DefaultMutableTreeNode parentNode) {
		if (bsonValue.isDocument()) {
			for (Map.Entry<String, BsonValue> entry : bsonValue.asDocument().entrySet()) {
				DefaultMutableTreeNode childNode = new DefaultMutableTreeNode(new BsonNodeInfo(entry.getKey(), entry.getValue()));
				parentNode.add(childNode);
				buildTree(entry.getValue(), childNode); // Recurse
			}
		} else if (bsonValue.isArray()) {
			int i = 0;
			for (BsonValue item : bsonValue.asArray()) {
				DefaultMutableTreeNode childNode = new DefaultMutableTreeNode(new BsonNodeInfo(String.valueOf(i), item));
				parentNode.add(childNode);
				buildTree(item, childNode); // Recurse
				i++;
			}
		}
	}

	/**
	 * Creates a more powerful context menu for the BSON tree with clearer copy options.
	 */
	private JPopupMenu createBsonContextMenu(TreePath path) {
		JPopupMenu menu = new JPopupMenu();
		JMenuItem copyRawValueItem = new JMenuItem("Copy Raw Value");
		JMenuItem copyAsJsonItem = new JMenuItem("Copy as JSON");
		JMenuItem copyAllItem = new JMenuItem("Copy All as JSON");

		if (path != null && path.getLastPathComponent() instanceof DefaultMutableTreeNode node
				&& node.getUserObject() instanceof BsonNodeInfo info) {
			BsonValue valueToCopy = info.value();
			copyRawValueItem.addActionListener(e -> copyToClipboard(getRawBsonValue(valueToCopy)));
			copyAsJsonItem.addActionListener(e -> {
				// Use pretty printing for documents/arrays, compact for simple values
				JsonWriterSettings settings = (valueToCopy.isDocument() || valueToCopy.isArray())
						? BSON_PRETTY_PRINT_SETTINGS
						: BSON_COMPACT_PRINT_SETTINGS;
				copyToClipboard(bsonValueToJson(valueToCopy, settings));
			});
		} else {
			copyRawValueItem.setEnabled(false);
      copyAsJsonItem.setEnabled(false);
		}

		if (currentBsonDocument != null) {
			copyAllItem.addActionListener(e -> copyToClipboard(currentBsonDocument.toJson(BSON_PRETTY_PRINT_SETTINGS)));
		} else {
			copyAllItem.setEnabled(false);
		}

		menu.add(copyRawValueItem);
		menu.add(copyAsJsonItem);
		menu.addSeparator();
		menu.add(copyAllItem);
		return menu;
	}

	/**
	 * **DEFINITIVE FIX**: Converts any BsonValue to its JSON string representation
	 * without relying on a potentially missing .toJson() method on the BsonValue itself.
	 */
	private String bsonValueToJson(BsonValue bsonValue, JsonWriterSettings settings) {
		if (bsonValue == null || bsonValue.isNull()) {
			return "null";
		}
		// A top-level document can be serialized directly, as it's known to have .toJson().
		if (bsonValue.isDocument()) {
			return bsonValue.asDocument().toJson(settings);
		}

		// For primitive BsonValues (String, Int32, etc.), we wrap them in a
		// temporary document to serialize them reliably.
		BsonDocument tempDoc = new BsonDocument("v", bsonValue);
		String tempJson = tempDoc.toJson(settings);

		// The result is like: { "v" : "some string" } or { "v" : 123 }.
		// We extract the value part after the colon.
		int colonIndex = tempJson.indexOf(':');
		if (colonIndex == -1) {
			return tempJson; // Fallback, should not happen with valid BSON.
		}
		return tempJson.substring(colonIndex + 1, tempJson.length() - 1).trim();
	}


	private String getRawBsonValue(BsonValue bsonValue) {
		if (bsonValue == null || bsonValue.isNull()) return "null";
		return switch (bsonValue.getBsonType()) {
			case DOCUMENT, ARRAY -> "(Complex value, use 'Copy as JSON' to see content)";
			case STRING -> bsonValue.asString().getValue();
			case INT32 -> String.valueOf(bsonValue.asInt32().getValue());
			case INT64 -> String.valueOf(bsonValue.asInt64().getValue());
			case DOUBLE -> String.valueOf(bsonValue.asDouble().getValue());
			case BOOLEAN -> String.valueOf(bsonValue.asBoolean().getValue());
			case OBJECT_ID -> bsonValue.asObjectId().getValue().toHexString();
			case BINARY -> HexFormat.of().formatHex(bsonValue.asBinary().getData());
			case DATE_TIME -> new Date(bsonValue.asDateTime().getValue()).toInstant().toString();
			// Fallback for other types like Decimal128, Symbol, etc.
			default -> bsonValue.toString();
		};
	}

	private void copyToClipboard(String text) {
		StringSelection selection = new StringSelection(text);
		Toolkit.getDefaultToolkit().getSystemClipboard().setContents(selection, null);
	}

	private byte[] convertToByteArray(Object data) {
		if (data instanceof byte[] arr) return arr;
		if (data instanceof String s) return s.getBytes(StandardCharsets.UTF_8);
		if (data instanceof Long l) return ByteBuffer.allocate(8).putLong(l).array();
		if (data instanceof Integer i) return ByteBuffer.allocate(4).putInt(i).array();
		if (data instanceof Double d) return ByteBuffer.allocate(8).putDouble(d).array();
		if (data instanceof Float f) return ByteBuffer.allocate(4).putFloat(f).array();
		return null;
	}

	private String interpretAsNumbers(byte[] bytes) {
		if (bytes.length < 1) return "(empty)";

		StringBuilder sb = new StringBuilder();
		ByteBuffer bbBE = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
		ByteBuffer bbLE = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);

		if (bytes.length >= 8) {
			sb.append(String.format("As Int64 (Big Endian):    %d\n", bbBE.getLong(0)));
			sb.append(String.format("As Int64 (Little Endian): %d\n", bbLE.getLong(0)));
			sb.append(String.format("As Double (Big Endian):   %f\n", bbBE.getDouble(0)));
			sb.append(String.format("As Double (Little Endian):%f\n", bbLE.getDouble(0)));
			sb.append("\n");
		}
		if (bytes.length >= 4) {
			sb.append(String.format("As Int32 (Big Endian):    %d\n", bbBE.getInt(0)));
			sb.append(String.format("As Int32 (Little Endian): %d\n", bbLE.getInt(0)));
			sb.append(String.format("As Float (Big Endian):    %f\n", bbBE.getFloat(0)));
			sb.append(String.format("As Float (Little Endian): %f\n", bbLE.getFloat(0)));
		}
		return sb.toString();
	}

	private String toHexDump(byte[] bytes) {
		if (bytes == null) return "(null)";
		final int bytesPerLine = 16;
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < bytes.length; i += bytesPerLine) {
			// Offset
			sb.append(String.format("%08X: ", i));

			// Hex values
			for (int j = 0; j < bytesPerLine; j++) {
				if (i + j < bytes.length) sb.append(String.format("%02X ", bytes[i + j]));
				else sb.append("   ");
			}

			// ASCII representation
			sb.append(" |");
			for (int j = 0; j < bytesPerLine; j++) {
				if (i + j < bytes.length) {
					char c = (char) bytes[i + j];
					sb.append(Character.isISOControl(c) || c > 255 ? '.' : c);
				}
			}
			sb.append("|\n");
		}
		return sb.toString();
	}

	/**
	 * A record to hold structured info for each node in the BSON JTree.
	 */
	private record BsonNodeInfo(String key, BsonValue value) {}

	/**
	 * A custom TreeCellRenderer that color-codes BSON types using HTML.
	 */
	private static class BsonTreeCellRenderer extends DefaultTreeCellRenderer {
		private static final Color KEY_COLOR = new Color(128, 0, 0); // Maroon
		private static final Color STRING_COLOR = new Color(0, 128, 0); // Green
		private static final Color NUMBER_COLOR = new Color(0, 0, 205); // Medium Blue
		private static final Color BOOLEAN_COLOR = new Color(170, 0, 170); // Purple
		private static final Color OTHER_COLOR = Color.GRAY;

		@Override
		public Component getTreeCellRendererComponent(JTree tree, Object value, boolean sel, boolean expanded, boolean leaf, int row, boolean hasFocus) {
			// Let the superclass handle the basics like selection background
			super.getTreeCellRendererComponent(tree, value, sel, expanded, leaf, row, hasFocus);

			if (value instanceof DefaultMutableTreeNode node && node.getUserObject() instanceof BsonNodeInfo info) {
				// We have our custom info object, format it with HTML
				setText(formatNode(info));
				setIcon(null); // Use text only, no folder/file icons
			}
			return this;
		}

		private String formatNode(BsonNodeInfo info) {
			String keyHtml = String.format("<font color='#%06x'>\"%s\"</font>",
					KEY_COLOR.getRGB() & 0xFFFFFF, info.key());

			BsonValue bsonValue = info.value();
			String valueHtml;

			if (bsonValue.isDocument()) {
				valueHtml = String.format("<font color='#%06x'>{...} (%d items)</font>",
						OTHER_COLOR.getRGB() & 0xFFFFFF, bsonValue.asDocument().size());
			} else if (bsonValue.isArray()) {
				valueHtml = String.format("<font color='#%06x'>[...] (%d items)</font>",
						OTHER_COLOR.getRGB() & 0xFFFFFF, bsonValue.asArray().size());
			} else if (bsonValue.isString()) {
				valueHtml = String.format("<font color='#%06x'>\"%s\"</font>",
						STRING_COLOR.getRGB() & 0xFFFFFF, escapeHtml(bsonValue.asString().getValue()));
			} else if (bsonValue.isNumber()) {
				valueHtml = String.format("<font color='#%06x'>%s</font>",
						NUMBER_COLOR.getRGB() & 0xFFFFFF, bsonValue);
			} else if (bsonValue.isBoolean()) {
				valueHtml = String.format("<font color='#%06x'>%s</font>",
						BOOLEAN_COLOR.getRGB() & 0xFFFFFF, bsonValue.asBoolean().getValue());
			} else if (bsonValue.isNull()) {
				valueHtml = String.format("<font color='#%06x'>null</font>",
						BOOLEAN_COLOR.getRGB() & 0xFFFFFF);
			} else {
				// For ObjectId, Binary, etc.
				valueHtml = String.format("<font color='#%06x'>(%s) %s</font>",
						OTHER_COLOR.getRGB() & 0xFFFFFF, bsonValue.getBsonType(), bsonValue);
			}

			return String.format("<html>%s: %s</html>", keyHtml, valueHtml);
		}

		private String escapeHtml(String text) {
			return text.replace("&", "&").replace("<", "<").replace(">", ">");
		}
	}
}