package it.cavallium.rockserver.core.gui;

import com.google.common.primitives.Ints;
import it.cavallium.rockserver.core.client.ClientBuilder;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import java.net.UnixDomainSocketAddress;
import java.nio.file.Path;
import javax.swing.*;
import java.awt.*;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

/**
 * A modern Java Swing UI for connecting to a custom database.
 * This UI dynamically adjusts input fields based on the selected connection mode
 * (e.g., Embedded, gRPC, Unix Socket) and runs connection attempts on a
 * background thread to keep the UI responsive.
 *
 * This implementation does NOT use JDBC.
 */
public class DbConnectionUI extends JFrame {

	// --- Connection Modes Enum ---
	// Using an enum is safer and cleaner than strings.
	private enum ConnectionMode {
		EMBEDDED("Embedded"),
		EMBEDDED_IN_MEMORY("Embedded (In-Memory)"),
		UNIX_SOCKET("Unix Socket"),
		GRPC("gRPC"),
		THRIFT("Thrift (Not Implemented)");

		private final String displayName;

		ConnectionMode(String displayName) {
			this.displayName = displayName;
		}

		@Override
		public String toString() {
			return displayName;
		}
	}

	// --- UI Components ---
	private JComboBox<ConnectionMode> modeComboBox;
	private JTextField dbNameField;
	private JTextField pathField;
	private JTextField socketField;
	private JTextField hostField;
	private JTextField portField;
	private JButton testButton;
	private JButton connectButton;
	private JTextArea statusArea;

	// --- Labels for dynamic show/hide ---
	private JLabel pathLabel;
	private JLabel socketLabel;
	private JLabel hostLabel;
	private JLabel portLabel;

	public DbConnectionUI() {
		super("Custom DB Connection");

		initComponents();
		layoutComponents();
		addListeners();

		// Set initial state based on the default selected mode
		updateUiForSelectedMode();

		// Frame setup
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		pack();
		setLocationRelativeTo(null);
		setMinimumSize(getSize());
	}

	private void initComponents() {
		modeComboBox = new JComboBox<>(ConnectionMode.values());

		// --- Add a custom renderer to disable the "Thrift" option ---
		modeComboBox.setRenderer(new DefaultListCellRenderer() {
			@Override
			public Component getListCellRendererComponent(JList<?> list, Object value, int index, boolean isSelected, boolean cellHasFocus) {
				super.getListCellRendererComponent(list, value, index, isSelected, cellHasFocus);
				if (value == ConnectionMode.THRIFT) {
					setEnabled(false);
					setForeground(Color.GRAY);
				}
				return this;
			}
		});

		// Set a default that is not disabled
		modeComboBox.setSelectedItem(ConnectionMode.GRPC);

		dbNameField = new JTextField("main", 25);
		pathField = new JTextField(25);
		socketField = new JTextField(25);
		hostField = new JTextField("10.0.0.9", 25);
		portField = new JTextField("5333", 8);

		pathLabel = new JLabel("Embedded Path:");
		socketLabel = new JLabel("Unix Socket Address:");
		hostLabel = new JLabel("Host:");
		portLabel = new JLabel("Port:");

		testButton = new JButton("Test Connection");
		connectButton = new JButton("Connect");

		statusArea = new JTextArea("Status: Ready", 4, 30);
		statusArea.setEditable(false);
		statusArea.setWrapStyleWord(true);
		statusArea.setLineWrap(true);
		statusArea.setFont(new Font("Monospaced", Font.PLAIN, 12));
		statusArea.setBorder(BorderFactory.createTitledBorder("Log"));
	}

	private void layoutComponents() {
		JPanel formPanel = new JPanel(new GridBagLayout());
		formPanel.setBorder(BorderFactory.createEmptyBorder(15, 15, 15, 15));
		var gbc = new GridBagConstraints();

		gbc.insets = new Insets(5, 5, 5, 5);
		gbc.anchor = GridBagConstraints.LINE_END;

		// Row 0: Connection Mode
		gbc.gridx = 0; gbc.gridy = 0;
		formPanel.add(new JLabel("Connection Mode:"), gbc);
		gbc.gridx = 1; gbc.anchor = GridBagConstraints.LINE_START;
		gbc.fill = GridBagConstraints.HORIZONTAL;
		formPanel.add(modeComboBox, gbc);

		// Row 1: DB Name (always visible)
		gbc.gridx = 0; gbc.gridy = 1; gbc.fill = GridBagConstraints.NONE; gbc.anchor = GridBagConstraints.LINE_END;
		formPanel.add(new JLabel("DB Name:"), gbc);
		gbc.gridx = 1; gbc.fill = GridBagConstraints.HORIZONTAL; gbc.anchor = GridBagConstraints.LINE_START;
		formPanel.add(dbNameField, gbc);

		// --- Dynamic Fields ---
		// Row 2: Path
		gbc.gridx = 0; gbc.gridy = 2; gbc.fill = GridBagConstraints.NONE; gbc.anchor = GridBagConstraints.LINE_END;
		formPanel.add(pathLabel, gbc);
		gbc.gridx = 1; gbc.fill = GridBagConstraints.HORIZONTAL; gbc.anchor = GridBagConstraints.LINE_START;
		formPanel.add(pathField, gbc);

		// Row 3: Socket
		gbc.gridx = 0; gbc.gridy = 3; gbc.fill = GridBagConstraints.NONE; gbc.anchor = GridBagConstraints.LINE_END;
		formPanel.add(socketLabel, gbc);
		gbc.gridx = 1; gbc.fill = GridBagConstraints.HORIZONTAL; gbc.anchor = GridBagConstraints.LINE_START;
		formPanel.add(socketField, gbc);

		// Row 4: Host
		gbc.gridx = 0; gbc.gridy = 4; gbc.fill = GridBagConstraints.NONE; gbc.anchor = GridBagConstraints.LINE_END;
		formPanel.add(hostLabel, gbc);
		gbc.gridx = 1; gbc.fill = GridBagConstraints.HORIZONTAL; gbc.anchor = GridBagConstraints.LINE_START;
		formPanel.add(hostField, gbc);

		// Row 5: Port
		gbc.gridx = 0; gbc.gridy = 5; gbc.fill = GridBagConstraints.NONE; gbc.anchor = GridBagConstraints.LINE_END;
		formPanel.add(portLabel, gbc);
		gbc.gridx = 1; gbc.fill = GridBagConstraints.NONE; gbc.anchor = GridBagConstraints.LINE_START; // Port field is smaller
		formPanel.add(portField, gbc);

		// --- Button Panel ---
		JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.RIGHT));
		buttonPanel.add(testButton);
		buttonPanel.add(connectButton);

		// --- Main Layout ---
		setLayout(new BorderLayout(10, 10));
		add(formPanel, BorderLayout.CENTER);
		add(buttonPanel, BorderLayout.NORTH);
		add(new JScrollPane(statusArea), BorderLayout.SOUTH);
	}

	private void addListeners() {
		modeComboBox.addActionListener(e -> updateUiForSelectedMode());
		testButton.addActionListener(e -> performConnectionAttempt());
		connectButton.addActionListener(e -> performConnectionAttempt());
	}

	/**
	 * Updates the visibility and enabled state of UI components based on the
	 * selected connection mode.
	 */
	private void updateUiForSelectedMode() {
		var selectedMode = (ConnectionMode) Objects.requireNonNull(modeComboBox.getSelectedItem());

		// Prevent selection of the disabled "Thrift" mode
		if (selectedMode == ConnectionMode.THRIFT) {
			modeComboBox.setSelectedItem(ConnectionMode.GRPC); // Revert to a safe default
			return;
		}

		// Reset all fields to a non-visible state first
		setFieldVisible(pathLabel, pathField, false);
		setFieldVisible(socketLabel, socketField, false);
		setFieldVisible(hostLabel, hostField, false);
		setFieldVisible(portLabel, portField, false);
		dbNameField.setEnabled(true);

		// Enable fields based on the selected mode
		switch (selectedMode) {
			case EMBEDDED ->
					setFieldVisible(pathLabel, pathField, true);
			case EMBEDDED_IN_MEMORY -> {
				// No extra fields needed, only DB Name
			}
			case UNIX_SOCKET ->
					setFieldVisible(socketLabel, socketField, true);
			case GRPC -> {
				setFieldVisible(hostLabel, hostField, true);
				setFieldVisible(portLabel, portField, true);
			}
			// Thrift case is handled by the renderer and selection listener
		}
	}

	/** Helper to toggle visibility of a label and its corresponding text field. */
	private void setFieldVisible(JLabel label, JTextField field, boolean visible) {
		label.setVisible(visible);
		field.setVisible(visible);
	}

	/**
	 * Simulates a connection attempt in a background thread.
	 */
	private void performConnectionAttempt() {
		testButton.setEnabled(false);
		connectButton.setEnabled(false);
		statusArea.setForeground(Color.BLACK);
		statusArea.setText("Status: Attempting to connect...");

		var worker = new SwingWorker<RocksDBConnection, Void>() {
			@Override
			protected RocksDBConnection doInBackground() throws Exception {
				// This runs on a background thread.
				// In a real app, you would call your custom connection library here.
				var mode = (ConnectionMode) modeComboBox.getSelectedItem();
				String dbName = dbNameField.getText().trim();

				if (dbName.isEmpty()) {
					throw new IllegalArgumentException("Database Name cannot be empty.");
				}

				var clientBuilder = new ClientBuilder();
				clientBuilder.setName(dbName);

				// Build a descriptive string based on the connection details
				return switch (Objects.requireNonNull(mode)) {
					case EMBEDDED -> {
						String path = pathField.getText().trim();
						if (path.isEmpty()) throw new IllegalArgumentException("Embedded Path cannot be empty.");
						System.out.printf("Connected in EMBEDDED mode.\nDB Name: %s\nPath: %s%n", dbName, path);
						clientBuilder.setEmbeddedInMemory(false);
						clientBuilder.setEmbeddedPath(Path.of(path));
						yield clientBuilder.build();
					}
					case EMBEDDED_IN_MEMORY -> {
						System.out.printf("Connected in IN-MEMORY mode.\nDB Name: %s%n", dbName);
						clientBuilder.setEmbeddedInMemory(true);
						yield clientBuilder.build();
					}
					case UNIX_SOCKET -> {
						String socket = socketField.getText().trim();
						if (socket.isEmpty()) throw new IllegalArgumentException("Unix Socket Address cannot be empty.");
						System.out.printf("Connected via UNIX SOCKET.\nDB Name: %s\nSocket: %s%n", dbName, socket);
						clientBuilder.setUnixSocket(UnixDomainSocketAddress.of(socket));
						yield clientBuilder.build();
					}
					case GRPC -> {
						String host = hostField.getText().trim();
						Integer port = Ints.tryParse(portField.getText().trim());
						if (host.isEmpty() || port == null || port <= 0) throw new IllegalArgumentException("Host and Port cannot be empty for gRPC.");

						clientBuilder.setUseThrift(false);
						clientBuilder.setHttpAddress(new HostAndPort(host, port));

						System.out.printf("Connected via gRPC.\nDB Name: %s\nAddress: %s:%s%n", dbName, host, port);
						yield clientBuilder.build();
					}
					default -> throw new IllegalStateException("Unsupported connection mode.");
				};
			}

			@Override
			protected void done() {
				// This runs on the Event Dispatch Thread (EDT) after completion.
				try {
					RocksDBConnection result = get(); // This will throw an exception if doInBackground() did.

					// 2. Create and show the viewer window.
					DbViewerUI viewer = new DbViewerUI(result);
					viewer.setVisible(true);

					// 3. Close this login window.
					DbConnectionUI.this.dispose();

					statusArea.setForeground(new Color(0, 128, 0)); // Dark Green
					statusArea.setText("Status: Success!\n" + result);
					// On success, you might dispose this window and open the main application
				} catch (InterruptedException | ExecutionException e) {
					statusArea.setForeground(Color.RED);
					Throwable cause = e.getCause() != null ? e.getCause() : e;
					statusArea.setText("Status: Connection Failed!\nError: " + cause.getMessage());
				} finally {
					testButton.setEnabled(true);
					connectButton.setEnabled(true);
				}
			}
		};
		worker.execute();
	}

	/**
	 * Calculates the system's DPI scaling factor.
	 * @return A string representation of the scaling factor (e.g., "1.0", "1.5", "2.0").
	 */
	private static String getSystemScalingFactor() {
		// On macOS, the scaling is handled by the OS automatically.
		// The uiScale property can sometimes interfere, so we can skip it.
		String os = System.getProperty("os.name").toLowerCase();
		if (os.contains("mac")) {
			return "1.0"; // Let macOS handle it
		}

		try {
			// This is the standard way to get the screen's default transform.
			GraphicsConfiguration gc = GraphicsEnvironment.getLocalGraphicsEnvironment()
					.getDefaultScreenDevice()
					.getDefaultConfiguration();

			// The scale factor is embedded in the AffineTransform.
			double scale = gc.getNormalizingTransform().getScaleX();
			return String.valueOf(scale);

		} catch (Exception e) {
			// In case of any error (e.g., headless environment), default to 1.0.
			return "1.0";
		}
	}

	public static void main(String[] args) {
		// 1. SET THE SCALING PROPERTY FIRST.
		// This is the critical step. Do it before touching any Swing code.
		System.setProperty("sun.java2d.uiScale", getSystemScalingFactor());

		// Create and show the GUI on the Event Dispatch Thread (EDT)
		SwingUtilities.invokeLater(() -> {
			try {
				// 3. SET THE LOOK AND FEEL *INSIDE* THE EDT.
				// Now that the scaling property is set, the L&F will respect it
				// when it initializes. The System L&F is best for this.
				UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
			} catch (Exception e) {
				System.err.println("Could not set the system look and feel.");
				e.printStackTrace();
			}
			new DbConnectionUI().setVisible(true);
		});
	}
}