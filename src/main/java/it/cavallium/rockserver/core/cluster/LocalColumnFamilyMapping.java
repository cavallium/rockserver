package it.cavallium.rockserver.core.cluster;

import java.util.OptionalLong;

public record LocalColumnFamilyMapping(long columnId, String columnFamilyName) {

    public static final String COLUMN_FAMILY_PREFIX = "_rk_col_";

    public LocalColumnFamilyMapping {
        if (columnId < 0) {
            throw new IllegalArgumentException("columnId must be >= 0");
        }
        requireArgument(columnFamilyName, "columnFamilyName");
        if (columnFamilyName.isBlank()) {
            throw new IllegalArgumentException("columnFamilyName must not be blank");
        }
    }

    public static LocalColumnFamilyMapping forColumn(long columnId) {
        return new LocalColumnFamilyMapping(columnId, columnFamilyName(columnId));
    }

    public static String columnFamilyName(long columnId) {
        if (columnId < 0) {
            throw new IllegalArgumentException("columnId must be >= 0");
        }
        return COLUMN_FAMILY_PREFIX + columnId;
    }

    public static OptionalLong parseColumnId(String columnFamilyName) {
        requireArgument(columnFamilyName, "columnFamilyName");
        if (!columnFamilyName.startsWith(COLUMN_FAMILY_PREFIX)) {
            return OptionalLong.empty();
        }
        var idText = columnFamilyName.substring(COLUMN_FAMILY_PREFIX.length());
        if (idText.isEmpty()) {
            return OptionalLong.empty();
        }
        try {
            var columnId = Long.parseLong(idText);
            if (columnId < 0) {
                return OptionalLong.empty();
            }
            return OptionalLong.of(columnId);
        } catch (NumberFormatException ignored) {
            return OptionalLong.empty();
        }
    }

    public boolean isCanonical() {
        return columnFamilyName.equals(columnFamilyName(columnId));
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
