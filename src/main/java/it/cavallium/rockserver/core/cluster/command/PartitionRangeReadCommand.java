package it.cavallium.rockserver.core.cluster.command;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.Keys;

import org.jetbrains.annotations.Nullable;

public record PartitionRangeReadCommand(long schemaEpoch,
                                        long columnId,
                                        @Nullable Keys startKeysInclusive,
                                        @Nullable Keys endKeysExclusive,
                                        boolean reverse,
                                        int maxEntries) {

    public static final int MAX_ENTRIES = 1_000_000;

    public PartitionRangeReadCommand {
        if (schemaEpoch < 0) {
            throw new IllegalArgumentException("schemaEpoch must be >= 0");
        }
        if (columnId < 0) {
            throw new IllegalArgumentException("columnId must be >= 0");
        }
        if (maxEntries <= 0) {
            throw new IllegalArgumentException("maxEntries must be > 0");
        }
        if (maxEntries > MAX_ENTRIES) {
            throw new IllegalArgumentException("maxEntries must be <= " + MAX_ENTRIES);
        }
        startKeysInclusive = copyNullableKeys(startKeysInclusive);
        endKeysExclusive = copyNullableKeys(endKeysExclusive);
    }

    @Override
    public @Nullable Keys startKeysInclusive() {
        return copyNullableKeys(startKeysInclusive);
    }

    @Override
    public @Nullable Keys endKeysExclusive() {
        return copyNullableKeys(endKeysExclusive);
    }

    private static @Nullable Keys copyNullableKeys(@Nullable Keys source) {
        if (source == null) {
            return null;
        }
        var sourceKeys = requireArgument(source.keys(), "keys array");
        var copiedKeys = new Buf[sourceKeys.length];
        for (int i = 0; i < sourceKeys.length; i++) {
            copiedKeys[i] = Buf.wrap(requireArgument(sourceKeys[i], "keys[" + i + "]")
                    .toByteArray()).freeze();
        }
        return new Keys(copiedKeys);
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
