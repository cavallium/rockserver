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
import java.time.Duration;
import it.cavallium.rockserver.core.common.RequestContext;

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
		GRPC("gRPC");

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
    private JTextField configField;
    private final JLabel configLabel = new JLabel("Config file (optional):");
    private final JButton browseConfig = new JButton("Browse…");
	private JTextField socketField;
	private JTextField hostField;
	private JTextField portField;
	private JButton testButton;
	private JButton connectButton;
	private JTextArea statusArea;
	private boolean closing;
    private final JTextArea connectionHint = new JTextArea(2, 25);
    private final JButton browsePath = new JButton("Browse…");
    private final JButton browseSocket = new JButton("Browse…");

	// --- Labels for dynamic show/hide ---
	private JLabel pathLabel;
	private JLabel socketLabel;
	private JLabel hostLabel;
	private JLabel portLabel;

	public DbConnectionUI() {
		super("Rockserver · Connect");

		initComponents();
		layoutComponents();
		addListeners();

		// Set initial state based on the default selected mode
		updateUiForSelectedMode();

		// Frame setup
		setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
		pack();
		setLocationRelativeTo(null);
		setMinimumSize(getSize());
	}

	private void initComponents() {
		modeComboBox = new JComboBox<>(ConnectionMode.values());

		// Set a default that is not disabled
		modeComboBox.setSelectedItem(ConnectionMode.GRPC);

		dbNameField = new JTextField("main", 25);
		pathField = new JTextField(25);
        configField = new JTextField(25);
        configField.putClientProperty("JTextField.placeholderText", "Default settings when empty");
        configLabel.setLabelFor(configField);
		socketField = new JTextField(25);
		hostField = new JTextField("localhost", 25);
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
        statusArea.setFont(UIManager.getFont("Label.font"));
        statusArea.setBackground(ViewerTheme.SURFACE);
        statusArea.setForeground(ViewerTheme.MUTED);
        statusArea.setBorder(BorderFactory.createEmptyBorder(12, 16, 12, 16));
        hostField.putClientProperty("JTextField.placeholderText", "localhost");
        pathField.putClientProperty("JTextField.placeholderText", "/path/to/database");
        socketField.putClientProperty("JTextField.placeholderText", "/path/to/rockserver.sock");
        ViewerTheme.primary(connectButton);
        connectionHint.setEditable(false); connectionHint.setLineWrap(true); connectionHint.setWrapStyleWord(true);
        connectionHint.setForeground(ViewerTheme.MUTED);
        connectionHint.setFont(UIManager.getFont("Label.font").deriveFont(12f));
        browsePath.addActionListener(e -> choosePath(pathField, true));
        browseConfig.addActionListener(e -> choosePath(configField, false));
        browseSocket.addActionListener(e -> choosePath(socketField, false));
	}

	private void layoutComponents() {
		JPanel formPanel = new JPanel(new GridBagLayout());
		formPanel.setBorder(BorderFactory.createEmptyBorder(12, 24, 12, 24));
		var gbc = new GridBagConstraints();

		gbc.insets = new Insets(7, 5, 7, 5);
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
        gbc.gridx = 2; formPanel.add(browsePath, gbc);

		// Row 3: Socket
		gbc.gridx = 0; gbc.gridy = 3; gbc.fill = GridBagConstraints.NONE; gbc.anchor = GridBagConstraints.LINE_END;
		formPanel.add(socketLabel, gbc);
		gbc.gridx = 1; gbc.fill = GridBagConstraints.HORIZONTAL; gbc.anchor = GridBagConstraints.LINE_START;
		formPanel.add(socketField, gbc);
        gbc.gridx = 2; formPanel.add(browseSocket, gbc);

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

        gbc.gridx = 0; gbc.gridy = 6; gbc.fill = GridBagConstraints.NONE; gbc.anchor = GridBagConstraints.LINE_END;
        formPanel.add(configLabel, gbc);
        gbc.gridx = 1; gbc.fill = GridBagConstraints.HORIZONTAL;
        formPanel.add(configField, gbc);
        gbc.gridx = 2; formPanel.add(browseConfig, gbc);
        gbc.gridx = 0; gbc.gridy = 7; gbc.gridwidth = 3; gbc.fill = GridBagConstraints.HORIZONTAL;
        formPanel.add(connectionHint, gbc);
        var heading = new JPanel(new BorderLayout(0, 8));
        heading.setBorder(BorderFactory.createEmptyBorder(24, 28, 8, 28));
        var title = new JLabel("Connect to Rockserver");
        title.setFont(title.getFont().deriveFont(Font.BOLD, 24f));
        var subtitle = new JLabel("Browse columns, inspect records, understand storage.");
        subtitle.setForeground(ViewerTheme.MUTED);
        heading.add(title, BorderLayout.NORTH);
        heading.add(subtitle, BorderLayout.SOUTH);
        var buttonPanel = new JPanel(new FlowLayout(FlowLayout.RIGHT, 10, 0));
        buttonPanel.setBorder(BorderFactory.createEmptyBorder(8, 24, 20, 24));
        buttonPanel.add(testButton);
        buttonPanel.add(connectButton);
        var footer = new JPanel(new BorderLayout());
        footer.add(buttonPanel, BorderLayout.NORTH);
        footer.add(statusArea, BorderLayout.SOUTH);
        setLayout(new BorderLayout());
        add(heading, BorderLayout.NORTH);
        add(formPanel, BorderLayout.CENTER);
        add(footer, BorderLayout.SOUTH);
        getRootPane().setDefaultButton(connectButton);
	}

	private void addListeners() {
		modeComboBox.addActionListener(e -> updateUiForSelectedMode());
		testButton.addActionListener(e -> performConnectionAttempt(true));
		connectButton.addActionListener(e -> performConnectionAttempt(false));
	}

	/**
	 * Updates the visibility and enabled state of UI components based on the
	 * selected connection mode.
	 */
	private void updateUiForSelectedMode() {
		var selectedMode = (ConnectionMode) Objects.requireNonNull(modeComboBox.getSelectedItem());

		// Reset all fields to a non-visible state first
		setFieldVisible(pathLabel, pathField, false);
		setFieldVisible(socketLabel, socketField, false);
		setFieldVisible(hostLabel, hostField, false);
		setFieldVisible(portLabel, portField, false);
        dbNameField.setEnabled(true);
        browsePath.setVisible(selectedMode == ConnectionMode.EMBEDDED);
        browseSocket.setVisible(selectedMode == ConnectionMode.UNIX_SOCKET);
        boolean embedded = selectedMode == ConnectionMode.EMBEDDED || selectedMode == ConnectionMode.EMBEDDED_IN_MEMORY;
        configLabel.setVisible(embedded); configField.setVisible(embedded); browseConfig.setVisible(embedded);
        connectionHint.setText(switch (selectedMode) {
            case GRPC -> "Connect to a running server. Browsing uses bounded requests; no automatic polling.";
            case UNIX_SOCKET -> "Connect to a local server through its Unix socket, without opening database files.";
            case EMBEDDED -> "Opens the local database directly and requires its lock. Opening may create files; use a server connection for a running database.";
            case EMBEDDED_IN_MEMORY -> "Creates a temporary, empty database for this session. No production data is loaded.";
        });

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
        }
        if (isDisplayable()) {
            Dimension preferred = getPreferredSize();
            setSize(Math.max(getWidth(), preferred.width), Math.max(getHeight(), preferred.height));
            revalidate();
        }
    }

    private void choosePath(JTextField target, boolean directory) {
        var chooser = new JFileChooser();
        chooser.setFileSelectionMode(directory ? JFileChooser.DIRECTORIES_ONLY : JFileChooser.FILES_ONLY);
        if (!target.getText().isBlank()) chooser.setSelectedFile(new java.io.File(target.getText()));
        if (chooser.showOpenDialog(this) == JFileChooser.APPROVE_OPTION) target.setText(chooser.getSelectedFile().getAbsolutePath());
    }

	/** Helper to toggle visibility of a label and its corresponding text field. */
	private void setFieldVisible(JLabel label, JTextField field, boolean visible) {
		label.setVisible(visible);
		field.setVisible(visible);
	}

    /** Capture Swing input on the EDT, then verify the server before transferring ownership. */
    private void performConnectionAttempt(boolean testOnly) {
        final ConnectionMode mode = (ConnectionMode) modeComboBox.getSelectedItem();
        final String dbName = dbNameField.getText().strip();
        final String path = pathField.getText().strip();
        final String config = configField.getText().strip();
        final String socket = socketField.getText().strip();
        final String host = hostField.getText().strip();
        final Integer port = Ints.tryParse(portField.getText().strip());
        setConnecting(true);
        statusArea.setForeground(UIManager.getColor("TextArea.foreground"));
        statusArea.setText("Checking connection and protocol compatibility…");
        new SwingWorker<RocksDBConnection, Void>() {
            @Override protected RocksDBConnection doInBackground() throws Exception {
                if (dbName.isEmpty()) throw new IllegalArgumentException("Database name cannot be empty.");
                var builder = new ClientBuilder();
                builder.setName(dbName);
                if ((mode == ConnectionMode.EMBEDDED || mode == ConnectionMode.EMBEDDED_IN_MEMORY) && !config.isEmpty()) {
                    if (!java.nio.file.Files.isRegularFile(Path.of(config))) throw new IllegalArgumentException("Config file does not exist.");
                    builder.setEmbeddedConfig(Path.of(config));
                }
                switch (Objects.requireNonNull(mode)) {
                    case EMBEDDED -> {
                        if (path.isEmpty()) throw new IllegalArgumentException("Embedded path cannot be empty.");
                        builder.setEmbeddedInMemory(false);
                        builder.setEmbeddedPath(Path.of(path));
                    }
                    case EMBEDDED_IN_MEMORY -> builder.setEmbeddedInMemory(true);
                    case UNIX_SOCKET -> {
                        if (socket.isEmpty()) throw new IllegalArgumentException("Unix socket cannot be empty.");
                        builder.setUnixSocket(UnixDomainSocketAddress.of(socket));
                    }
                    case GRPC -> {
                        if (host.isEmpty() || port == null || port < 1 || port > 65535) {
                            throw new IllegalArgumentException("Enter a host and a port between 1 and 65535.");
                        }
                        builder.setUseThrift(false);
                        builder.setHttpAddress(new HostAndPort(host, port));
                    }
                    default -> throw new IllegalArgumentException("Unsupported connection mode.");
                }
                RocksDBConnection connection = builder.build();
                try {
                    connection.getCapabilities().requireCompatible();
                    connection.getSyncApi(RequestContext.latency(Duration.ofSeconds(5))).getAllColumnDefinitions();
                } catch (Exception e) {
                    try { connection.close(); } catch (Exception closeError) { e.addSuppressed(closeError); }
                    throw e;
                }
                if (testOnly) {
                    connection.close();
                    return null;
                }
                return connection;
            }

            @Override protected void done() {
                try {
                    RocksDBConnection connection = get();
                    if (closing) {
                        if (connection != null) closeAsync(connection);
                        return;
                    }
                    if (testOnly) {
                        statusArea.setText("Connection verified. Compatible Rockserver protocol; column definitions are accessible.");
                    } else {
                        try {
                            new DbViewerUI(connection).setVisible(true);
                            DbConnectionUI.this.dispose();
                        } catch (RuntimeException e) {
                            closeAsync(connection);
                            throw e;
                        }
                    }
                } catch (Exception e) {
                    Throwable cause = e.getCause() == null ? e : e.getCause();
                    statusArea.setForeground(Color.RED);
                    statusArea.setText("Connection failed: " + cause.getMessage());
                } finally {
                    setConnecting(false);
                }
            }
        }.execute();
    }

    @Override public void dispose() {
        closing = true;
        super.dispose();
    }

    private static void closeAsync(RocksDBConnection connection) {
        Thread.ofPlatform().name("connection-close").start(() -> {
            try { connection.close(); } catch (Exception e) { e.printStackTrace(); }
        });
    }

    private void setConnecting(boolean connecting) {
        for (JComponent component : new JComponent[]{modeComboBox, dbNameField, pathField, socketField,
                hostField, portField, testButton, connectButton, browsePath, browseSocket, configField, browseConfig}) component.setEnabled(!connecting);
    }

	public static void main(String[] args) {
		// Create and show the GUI on the Event Dispatch Thread (EDT)
		SwingUtilities.invokeLater(() -> {
            ViewerTheme.install();
			new DbConnectionUI().setVisible(true);
		});
	}
}