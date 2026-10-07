package it.cavallium.rockserver.core.gui;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.StreamReadConstraints;
import java.io.IOException;
import java.io.StringWriter;

/** Bounded JSON formatting of bytes already loaded by the viewer. */
public final class JsonPreview {
    public static final int MAX_BYTES = 64 * 1024;
    private static final int MAX_TOKENS = 4096;
    private static final int MAX_OUTPUT = 128 * 1024;
    private static final JsonFactory FACTORY = JsonFactory.builder().streamReadConstraints(
            StreamReadConstraints.builder().maxNestingDepth(64).maxStringLength(MAX_BYTES).maxNumberLength(1000).build()).build();
    private JsonPreview() {}
    public record Result(boolean valid, String text) {}

    public static Result format(byte[] bytes, boolean pretty) {
        if (bytes == null || bytes.length == 0) return new Result(false, "No JSON data selected.");
        if (bytes.length > MAX_BYTES) return new Result(false, "JSON preview limited to 64 KiB. Save raw bytes for the full document.");
        var output = new StringWriter();
        try (var parser = FACTORY.createParser(bytes); var writer = FACTORY.createGenerator(output)) {
            if (pretty) writer.useDefaultPrettyPrinter();
            int tokens = 0, depth = 0;
            boolean complete = false;
            JsonToken token;
            while ((token = parser.nextToken()) != null) {
                if (complete) return new Result(false, "Invalid JSON: trailing content after the root value.");
                if (++tokens > MAX_TOKENS) return new Result(false, "JSON preview limited to 4,096 tokens. Save raw bytes for the full document.");
                if (token.isStructStart()) depth++;
                else if (token.isStructEnd()) depth--;
                // Preserve large integers, decimals and exponents exactly; do not round through double.
                if (token.isNumeric()) writer.writeNumber(parser.getText()); else writer.copyCurrentEvent(parser);
                writer.flush();
                if (output.getBuffer().length() > MAX_OUTPUT) return new Result(false, "Formatted JSON exceeds the preview limit. Save raw bytes for the full document.");
                if (depth == 0) complete = true;
            }
            if (!complete) return new Result(false, "Invalid JSON: expected a complete root value.");
            return new Result(true, output.toString());
        } catch (IOException e) {
            return new Result(false, "Invalid JSON: " + e.getMessage());
        }
    }
}
