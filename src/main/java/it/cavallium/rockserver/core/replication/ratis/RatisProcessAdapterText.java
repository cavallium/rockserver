package it.cavallium.rockserver.core.replication.ratis;

final class RatisProcessAdapterText {

    private RatisProcessAdapterText() {
    }

    static boolean containsUnsafeLineCharacter(String value) {
        if (value == null) {
            throw new IllegalArgumentException("value must not be null");
        }
        for (int i = 0; i < value.length(); i++) {
            if (isUnsafeLineCharacter(value.charAt(i))) {
                return true;
            }
        }
        return false;
    }

    static String sanitizeSingleLine(String value, String fallback, int maxCharacters) {
        if (fallback == null) {
            throw new IllegalArgumentException("fallback must not be null");
        }
        if (maxCharacters <= 0) {
            throw new IllegalArgumentException("maxCharacters must be positive");
        }
        if (value == null || value.isBlank()) {
            value = fallback;
        }
        var sanitized = new StringBuilder(Math.min(value.length(), maxCharacters));
        for (int i = 0; i < value.length() && sanitized.length() < maxCharacters; i++) {
            var character = value.charAt(i);
            sanitized.append(isUnsafeLineCharacter(character) ? ' ' : character);
        }
        var result = sanitized.toString().trim();
        return result.isEmpty() ? "unknown failure" : result;
    }

    static void forEachCodecLine(String payload, CodecLineConsumer consumer) {
        if (payload == null) {
            throw new IllegalArgumentException("payload must not be null");
        }
        if (consumer == null) {
            throw new IllegalArgumentException("consumer must not be null");
        }
        var start = 0;
        for (int i = 0; i < payload.length(); i++) {
            var character = payload.charAt(i);
            if (!isCodecLineSeparator(character)) {
                continue;
            }
            if (start == i) {
                throw new IllegalArgumentException("Ratis adapter codec payload contains an empty line");
            }
            consumer.accept(payload, start, i);
            if (character == '\r' && i + 1 < payload.length() && payload.charAt(i + 1) == '\n') {
                i++;
            }
            start = i + 1;
        }
        if (start < payload.length()) {
            consumer.accept(payload, start, payload.length());
        }
    }

    static boolean regionEquals(String value, int start, int end, String expected) {
        return end - start == expected.length() && value.regionMatches(start, expected, 0, expected.length());
    }

    static String summarize(String value, int start, int end) {
        var summaryEnd = Math.min(end, start + 128);
        var summary = value.substring(start, summaryEnd);
        return summaryEnd == end ? summary : summary + "...";
    }

    private static boolean isUnsafeLineCharacter(char character) {
        return character == '\0' || isCodecLineSeparator(character);
    }

    private static boolean isCodecLineSeparator(char character) {
        return switch (character) {
            case '\n', '\u000B', '\f', '\r', '\u0085', '\u2028', '\u2029' -> true;
            default -> false;
        };
    }

    @FunctionalInterface
    interface CodecLineConsumer {

        void accept(String payload, int start, int end);
    }
}
