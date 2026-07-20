package it.cavallium.rockserver.core.cluster;

public final class SingleLineStrings {

    private SingleLineStrings() {
    }

    public static String requireNonBlank(String value, String description) {
        if (value == null) {
            throw new IllegalArgumentException(description + " must not be null");
        }
        if (value.isBlank()) {
            throw new IllegalArgumentException(description + " must not be blank");
        }
        if (value.indexOf('\r') >= 0 || value.indexOf('\n') >= 0) {
            throw new IllegalArgumentException(description + " must be a single line");
        }
        return value;
    }
}
