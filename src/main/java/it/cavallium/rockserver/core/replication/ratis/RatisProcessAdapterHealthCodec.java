package it.cavallium.rockserver.core.replication.ratis;

import java.util.LinkedHashMap;
import java.util.Map;

public final class RatisProcessAdapterHealthCodec {

    private static final String VERSION_KEY = "version";
    private static final String STATUS_KEY = "status";
    private static final String MESSAGE_KEY = "message";
    private static final int MAX_VERSION_CHARACTERS = Integer.toString(Integer.MIN_VALUE).length();
    private static final int MAX_STATUS_CHARACTERS = "DEGRADED".length();

    private RatisProcessAdapterHealthCodec() {
    }

    public static String encode(RatisProcessAdapterHealth health) {
        if (health == null) {
            throw new IllegalArgumentException("health must not be null");
        }
        return VERSION_KEY + "=" + health.protocolVersion() + "\n"
                + STATUS_KEY + "=" + health.status().name() + "\n"
                + MESSAGE_KEY + "=" + health.message() + "\n";
    }

    public static RatisProcessAdapterHealth decode(String payload) {
        var entries = new LinkedHashMap<String, String>();
        RatisProcessAdapterText.forEachCodecLine(payload, (source, start, end) -> {
            var separator = source.indexOf('=', start, end);
            if (separator <= start || separator >= end) {
                throw new IllegalArgumentException("Malformed Ratis adapter health line: "
                        + RatisProcessAdapterText.summarize(source, start, end));
            }
            var key = allowedKey(source, start, separator);
            if (key == null) {
                throw new IllegalArgumentException("Unknown Ratis adapter health key: "
                        + RatisProcessAdapterText.summarize(source, start, separator));
            }
            if (entries.containsKey(key)) {
                throw new IllegalArgumentException("Duplicate Ratis adapter health key: " + key);
            }
            var valueCharacters = end - separator - 1;
            var maxValueCharacters = maxValueCharacters(key);
            if (valueCharacters > maxValueCharacters) {
                throw new IllegalArgumentException("Ratis adapter health value for key " + key + " must not exceed "
                        + maxValueCharacters + " characters");
            }
            var value = source.substring(separator + 1, end);
            entries.put(key, value);
        });

        var protocolVersion = parseProtocolVersion(required(entries, VERSION_KEY));
        if (protocolVersion != RatisProcessAdapterHealth.CURRENT_PROTOCOL_VERSION) {
            throw new IllegalArgumentException("Unsupported Ratis adapter health protocol version: " + protocolVersion);
        }
        return new RatisProcessAdapterHealth(
                protocolVersion,
                parseStatus(required(entries, STATUS_KEY)),
                required(entries, MESSAGE_KEY)
        );
    }

    private static String allowedKey(String payload, int start, int end) {
        if (RatisProcessAdapterText.regionEquals(payload, start, end, VERSION_KEY)) {
            return VERSION_KEY;
        }
        if (RatisProcessAdapterText.regionEquals(payload, start, end, STATUS_KEY)) {
            return STATUS_KEY;
        }
        if (RatisProcessAdapterText.regionEquals(payload, start, end, MESSAGE_KEY)) {
            return MESSAGE_KEY;
        }
        return null;
    }

    private static int maxValueCharacters(String key) {
        return switch (key) {
            case VERSION_KEY -> MAX_VERSION_CHARACTERS;
            case STATUS_KEY -> MAX_STATUS_CHARACTERS;
            case MESSAGE_KEY -> RatisProcessAdapterHealth.MAX_MESSAGE_CHARACTERS;
            default -> throw new IllegalArgumentException("Unsupported Ratis adapter health key: " + key);
        };
    }

    private static String required(Map<String, String> entries, String key) {
        var value = entries.get(key);
        if (value == null) {
            throw new IllegalArgumentException("Missing Ratis adapter health key: " + key);
        }
        return value;
    }

    private static int parseProtocolVersion(String value) {
        try {
            var parsed = Integer.parseInt(value);
            if (!Integer.toString(parsed).equals(value)) {
                throw new IllegalArgumentException("Non-canonical Ratis adapter health protocol version: " + value);
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid Ratis adapter health protocol version: " + value, e);
        }
    }

    private static RatisProcessAdapterHealthStatus parseStatus(String value) {
        try {
            return RatisProcessAdapterHealthStatus.valueOf(value);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid Ratis adapter health status: " + value, e);
        }
    }
}
