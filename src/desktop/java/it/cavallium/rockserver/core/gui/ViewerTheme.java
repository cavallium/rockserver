package it.cavallium.rockserver.core.gui;

import com.formdev.flatlaf.FlatLightLaf;
import java.awt.Color;
import java.awt.Font;
import java.awt.Insets;
import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.UIManager;
import javax.swing.border.Border;
import javax.swing.border.TitledBorder;

/** Shared desktop styling; uses system fonts and FlatLaf core only. */
public final class ViewerTheme {
    static final Color TEXT = new Color(30, 41, 59);
    static final Color MUTED = new Color(100, 116, 139);
    static final Color LINE = new Color(226, 232, 240);
    static final Color SURFACE = new Color(248, 250, 252);
    static final Color ACCENT = new Color(37, 99, 235);

    private ViewerTheme() {}

    /** Invoke on the EDT before constructing either window. */
    public static void install() {
        if (!FlatLightLaf.setup()) throw new IllegalStateException("Cannot initialize the desktop look and feel");
        UIManager.put("defaultFont", new Font(Font.DIALOG, Font.PLAIN, 13));
        UIManager.put("Panel.background", Color.WHITE);
        UIManager.put("Label.foreground", TEXT);
        UIManager.put("Component.accentColor", ACCENT);
        UIManager.put("Component.focusColor", ACCENT);
        UIManager.put("Component.arc", 10);
        UIManager.put("Button.arc", 10);
        UIManager.put("TextComponent.arc", 10);
        UIManager.put("Component.focusWidth", 1);
        UIManager.put("Component.innerFocusWidth", 0);
        UIManager.put("Component.borderColor", LINE);
        UIManager.put("Button.margin", new Insets(7, 14, 7, 14));
        UIManager.put("TextField.margin", new Insets(7, 10, 7, 10));
        UIManager.put("Table.rowHeight", 32);
        UIManager.put("TableHeader.height", 34);
        UIManager.put("TableHeader.background", SURFACE);
        UIManager.put("TableHeader.foreground", MUTED);
        UIManager.put("Table.selectionBackground", new Color(219, 234, 254));
        UIManager.put("Table.selectionForeground", TEXT);
        UIManager.put("Table.selectionInactiveBackground", new Color(235, 243, 255));
        UIManager.put("List.selectionBackground", new Color(219, 234, 254));
        UIManager.put("List.selectionForeground", TEXT);
        UIManager.put("List.selectionInactiveBackground", new Color(235, 243, 255));
        UIManager.put("TabbedPane.tabHeight", 40);
        UIManager.put("TabbedPane.tabSelectionHeight", 3);
        UIManager.put("TabbedPane.underlineColor", ACCENT);
        UIManager.put("TabbedPane.selectedBackground", Color.WHITE);
        UIManager.put("ScrollBar.width", 10);
        UIManager.put("SplitPane.dividerSize", 6);
    }

    static Border section(String title) {
        return BorderFactory.createTitledBorder(BorderFactory.createEmptyBorder(10, 0, 0, 0), title,
                TitledBorder.LEADING, TitledBorder.TOP, UIManager.getFont("Label.font").deriveFont(Font.BOLD, 12f), MUTED);
    }

    static void primary(JButton button) {
        button.putClientProperty("FlatLaf.style", "background: #2563eb; foreground: #ffffff; focusedBackground: #2563eb; hoverBackground: #1d4ed8");
    }
}
