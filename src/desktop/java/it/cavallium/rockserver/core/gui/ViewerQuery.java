package it.cavallium.rockserver.core.gui;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

/** A committed query; navigation must keep its original bounds and direction. */
public record ViewerQuery(Keys start, Keys end, boolean reverse, RangeBudget budget) {

    public static ViewerQuery parse(ColumnSchema schema, String start, String end, boolean reverse, int pageSize) {
        if (pageSize > RangeBudget.DEFAULT_MAX_ITEMS) {
            throw new IllegalArgumentException("Page size exceeds " + RangeBudget.DEFAULT_MAX_ITEMS);
        }
        return new ViewerQuery(parseKey(schema, start), parseKey(schema, end), reverse,
                new RangeBudget(pageSize, RangeBudget.DEFAULT_MAX_BYTES));
    }

    /** Blank means unbounded; '-' denotes an empty variable key component. */
    public static Keys parseKey(ColumnSchema schema, String text) {
        if (text.isBlank()) return null;
        String[] parts = text.split(";", -1);
        if (parts.length != schema.keysCount()) {
            throw new IllegalArgumentException("Expected " + schema.keysCount() + " key components separated by ';'.");
        }
        Buf[] keys = new Buf[parts.length];
        for (int i = 0; i < parts.length; i++) {
            String hex = parts[i].strip().replaceAll("\\s+", "");
            if (hex.isEmpty()) throw new IllegalArgumentException("Use '-' for an empty variable key.");
            byte[] bytes;
            try {
                bytes = hex.equals("-") ? new byte[0] : HexFormat.of().parseHex(hex);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Key " + i + " must contain complete hexadecimal bytes.", e);
            }
            if (i < schema.fixedLengthKeysCount() && bytes.length != schema.key(i)) {
                throw new IllegalArgumentException("Key " + i + " requires " + schema.key(i) + " bytes.");
            }
            keys[i] = Buf.wrap(bytes);
        }
        return new Keys(keys);
    }

    public RangePage<KV> load(RocksDBSyncAPI api, long columnId, Keys resumeAfter) {
        return api.getRangePage(0, columnId, start, end, reverse, resumeAfter,
                RequestType.allInRangeNoCache(), budget);
    }

    public static List<String> columns(ColumnSchema schema) {
        var names = new ArrayList<String>();
        for (int i = 0; i < schema.keysCount(); i++) {
            names.add(i < schema.fixedLengthKeysCount()
                    ? "Key " + i + " · " + schema.key(i) + " bytes"
                    : "Key " + i + " · variable / " + schema.variableTailKey(i));
        }
        if (schema.hasValue()) names.add("Value");
        return names;
    }

    public static Object[][] rows(ColumnSchema schema, RangePage<KV> page) {
        Object[][] rows = new Object[page.items().size()][schema.keysCount() + (schema.hasValue() ? 1 : 0)];
        for (int r = 0; r < rows.length; r++) {
            KV item = page.items().get(r);
            for (int k = 0; k < schema.keysCount(); k++) rows[r][k] = item.keys().keys()[k].toByteArray();
            if (schema.hasValue()) rows[r][schema.keysCount()] = item.value() == null ? null : item.value().toByteArray();
        }
        return rows;
    }
}
