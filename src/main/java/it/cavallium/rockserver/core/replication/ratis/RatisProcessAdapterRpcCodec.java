package it.cavallium.rockserver.core.replication.ratis;

import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public final class RatisProcessAdapterRpcCodec {

    private static final String VERSION_KEY = "version";
    private static final String REQUEST_ID_KEY = "requestId";
    private static final String PARTITION_ID_KEY = "partitionId";
    private static final String OPERATION_KEY = "operation";
    private static final String PAYLOAD_KEY = "payload";
    private static final String INDEX_KEY = "index";
    private static final String MIN_APPLIED_INDEX_KEY = "minAppliedIndex";
    private static final String SUCCESS_KEY = "success";
    private static final String COMMIT_INDEX_KEY = "commitIndex";
    private static final String COMMITTED_KEY = "committed";
    private static final String ERROR_TYPE_KEY = "errorType";
    private static final String MESSAGE_KEY = "message";
    private static final int MAX_LONG_CHARACTERS = Long.toString(Long.MIN_VALUE).length();
    private static final int MAX_OPERATION_CHARACTERS = "QUERY_LINEARIZABLE".length();
    private static final int MAX_BOOLEAN_CHARACTERS = "false".length();
    private static final Set<String> REQUEST_KEYS = Set.of(
            VERSION_KEY,
            REQUEST_ID_KEY,
            PARTITION_ID_KEY,
            OPERATION_KEY,
            PAYLOAD_KEY,
            INDEX_KEY,
            MIN_APPLIED_INDEX_KEY
    );
    private static final Set<String> RESPONSE_KEYS = Set.of(
            VERSION_KEY,
            REQUEST_ID_KEY,
            SUCCESS_KEY,
            PAYLOAD_KEY,
            COMMIT_INDEX_KEY,
            COMMITTED_KEY,
            ERROR_TYPE_KEY,
            MESSAGE_KEY
    );

    private RatisProcessAdapterRpcCodec() {
    }

    public static String encodeRequest(RatisProcessAdapterRpcRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("request must not be null");
        }
        return VERSION_KEY + "=" + request.protocolVersion() + "\n"
                + REQUEST_ID_KEY + "=" + request.requestId() + "\n"
                + PARTITION_ID_KEY + "=" + request.partitionId() + "\n"
                + OPERATION_KEY + "=" + request.operation().name() + "\n"
                + PAYLOAD_KEY + "=" + encodePayload(request.payload()) + "\n"
                + INDEX_KEY + "=" + request.index() + "\n"
                + MIN_APPLIED_INDEX_KEY + "=" + request.minAppliedIndex() + "\n";
    }

    public static RatisProcessAdapterRpcRequest decodeRequest(String payload) {
        var entries = parse(payload, REQUEST_KEYS);

        var protocolVersion = parseProtocolVersion(required(entries, VERSION_KEY));
        return new RatisProcessAdapterRpcRequest(
                protocolVersion,
                required(entries, REQUEST_ID_KEY),
                required(entries, PARTITION_ID_KEY),
                parseOperation(required(entries, OPERATION_KEY)),
                decodePayload(required(entries, PAYLOAD_KEY)),
                parseLong(entries, INDEX_KEY),
                parseLong(entries, MIN_APPLIED_INDEX_KEY)
        );
    }

    public static String encodeResponse(RatisProcessAdapterRpcResponse response) {
        if (response == null) {
            throw new IllegalArgumentException("response must not be null");
        }
        return VERSION_KEY + "=" + response.protocolVersion() + "\n"
                + REQUEST_ID_KEY + "=" + response.requestId() + "\n"
                + SUCCESS_KEY + "=" + response.success() + "\n"
                + PAYLOAD_KEY + "=" + encodePayload(response.payload()) + "\n"
                + COMMIT_INDEX_KEY + "=" + response.commitIndex() + "\n"
                + COMMITTED_KEY + "=" + response.committed() + "\n"
                + ERROR_TYPE_KEY + "=" + response.errorType() + "\n"
                + MESSAGE_KEY + "=" + response.message() + "\n";
    }

    public static RatisProcessAdapterRpcResponse decodeResponse(String payload) {
        var entries = parse(payload, RESPONSE_KEYS);

        var protocolVersion = parseProtocolVersion(required(entries, VERSION_KEY));
        return new RatisProcessAdapterRpcResponse(
                protocolVersion,
                required(entries, REQUEST_ID_KEY),
                parseBoolean(entries, SUCCESS_KEY),
                decodePayload(required(entries, PAYLOAD_KEY)),
                parseLong(entries, COMMIT_INDEX_KEY),
                parseBoolean(entries, COMMITTED_KEY),
                required(entries, ERROR_TYPE_KEY),
                required(entries, MESSAGE_KEY)
        );
    }

    private static LinkedHashMap<String, String> parse(String payload, Set<String> allowedKeys) {
        var entries = new LinkedHashMap<String, String>();
        RatisProcessAdapterText.forEachCodecLine(payload, (source, start, end) -> {
            var separator = source.indexOf('=', start, end);
            if (separator <= start || separator >= end) {
                throw new IllegalArgumentException("Malformed Ratis adapter RPC line: "
                        + RatisProcessAdapterText.summarize(source, start, end));
            }
            var key = allowedKey(source, start, separator, allowedKeys);
            if (key == null) {
                throw new IllegalArgumentException("Unknown Ratis adapter RPC key: "
                        + RatisProcessAdapterText.summarize(source, start, separator));
            }
            if (entries.containsKey(key)) {
                throw new IllegalArgumentException("Duplicate Ratis adapter RPC key: " + key);
            }
            var valueCharacters = end - separator - 1;
            var maxValueCharacters = maxValueCharacters(key);
            if (valueCharacters > maxValueCharacters) {
                throw new IllegalArgumentException("Ratis adapter RPC value for key " + key + " must not exceed "
                        + maxValueCharacters + " characters");
            }
            var value = source.substring(separator + 1, end);
            entries.put(key, value);
        });
        return entries;
    }

    private static String allowedKey(String payload, int start, int end, Set<String> allowedKeys) {
        for (var allowedKey : allowedKeys) {
            if (RatisProcessAdapterText.regionEquals(payload, start, end, allowedKey)) {
                return allowedKey;
            }
        }
        return null;
    }

    private static int maxValueCharacters(String key) {
        return switch (key) {
            case VERSION_KEY, INDEX_KEY, MIN_APPLIED_INDEX_KEY, COMMIT_INDEX_KEY -> MAX_LONG_CHARACTERS;
            case REQUEST_ID_KEY -> RatisProcessAdapterRpcLimits.MAX_REQUEST_ID_CHARACTERS;
            case PARTITION_ID_KEY -> RatisProcessAdapterRpcLimits.MAX_PARTITION_ID_CHARACTERS;
            case OPERATION_KEY -> MAX_OPERATION_CHARACTERS;
            case PAYLOAD_KEY -> RatisProcessAdapterRpcLimits.MAX_ENCODED_PAYLOAD_CHARACTERS;
            case SUCCESS_KEY, COMMITTED_KEY -> MAX_BOOLEAN_CHARACTERS;
            case ERROR_TYPE_KEY -> RatisProcessAdapterRpcLimits.MAX_ERROR_TYPE_CHARACTERS;
            case MESSAGE_KEY -> RatisProcessAdapterRpcLimits.MAX_ERROR_MESSAGE_CHARACTERS;
            default -> throw new IllegalArgumentException("Unsupported Ratis adapter RPC key: " + key);
        };
    }

    private static String required(Map<String, String> entries, String key) {
        var value = entries.get(key);
        if (value == null) {
            throw new IllegalArgumentException("Missing Ratis adapter RPC key: " + key);
        }
        return value;
    }

    private static int parseProtocolVersion(String value) {
        var parsed = parseLong(value, VERSION_KEY);
        if (parsed < Integer.MIN_VALUE || parsed > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Invalid Ratis adapter RPC protocol version: " + value);
        }
        var protocolVersion = (int) parsed;
        if (protocolVersion != RatisProcessAdapterRpcRequest.CURRENT_PROTOCOL_VERSION) {
            throw new IllegalArgumentException("Unsupported Ratis adapter RPC protocol version: " + protocolVersion);
        }
        return protocolVersion;
    }

    private static long parseLong(Map<String, String> entries, String key) {
        return parseLong(required(entries, key), key);
    }

    private static long parseLong(String value, String key) {
        try {
            var parsed = Long.parseLong(value);
            if (!Long.toString(parsed).equals(value)) {
                throw new IllegalArgumentException(
                        "Non-canonical Ratis adapter RPC number for key " + key + ": " + value
                );
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid Ratis adapter RPC number for key " + key + ": " + value, e);
        }
    }

    private static boolean parseBoolean(Map<String, String> entries, String key) {
        var value = required(entries, key);
        if ("true".equals(value)) {
            return true;
        }
        if ("false".equals(value)) {
            return false;
        }
        throw new IllegalArgumentException("Invalid Ratis adapter RPC boolean for key " + key + ": " + value);
    }

    private static RatisProcessAdapterRpcOperation parseOperation(String value) {
        try {
            return RatisProcessAdapterRpcOperation.valueOf(value);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid Ratis adapter RPC operation: " + value, e);
        }
    }

    private static String encodePayload(byte[] payload) {
        return Base64.getEncoder().encodeToString(RatisProcessAdapterRpcLimits.requirePayload(payload));
    }

    private static byte[] decodePayload(String payload) {
        payload = RatisProcessAdapterRpcLimits.requireEncodedPayload(payload);
        byte[] decoded;
        try {
            decoded = Base64.getDecoder().decode(payload);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid Base64 Ratis adapter RPC payload", e);
        }
        if (!Base64.getEncoder().encodeToString(decoded).equals(payload)) {
            throw new IllegalArgumentException("Non-canonical Base64 Ratis adapter RPC payload");
        }
        return decoded;
    }
}
