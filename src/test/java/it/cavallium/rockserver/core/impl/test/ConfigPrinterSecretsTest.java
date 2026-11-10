package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.config.ConfigPrinter;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * R60-1: the configuration that every database open logs at INFO ({@code "Database configuration: {}"}, unless
 * {@code -Drockserver.core.print-config=false}) prints no credential: the InfluxDB token and user, and a password in
 * the InfluxDB URL, are masked. The rest is printed as before, so that the line still tells what the node runs with.
 */
class ConfigPrinterSecretsTest {

	@TempDir
	Path tempDir;

	@Test
	void theInfluxCredentialsAreNotPrinted() throws Exception {
		Path conf = Files.writeString(tempDir.resolve("secrets.conf"), """
				database.metrics.influx.enabled = false
				database.metrics.influx.url = "https://grafana:hunter2@influx.example:8086/write"
				database.metrics.influx.bucket = "rockserver"
				database.metrics.influx.user = "metrics-writer"
				database.metrics.influx.token = "tok-0123456789-secret"
				database.metrics.influx.org = "home"
				""");
		var config = ConfigParser.parse(conf);
		assertEquals("tok-0123456789-secret", config.metrics().influx().token());

		var printed = ConfigPrinter.stringify(config);

		assertAll(
				() -> assertFalse(printed.contains("tok-0123456789-secret"), printed),
				() -> assertFalse(printed.contains("metrics-writer"), printed),
				() -> assertFalse(printed.contains("hunter2"), printed),
				() -> assertTrue(printed.contains("\"token\": \"<redacted>\""), printed),
				() -> assertTrue(printed.contains("\"user\": \"<redacted>\""), printed),
				// The URL keeps its host, port and path: only its user info is masked.
				() -> assertTrue(printed.contains("\"url\": \"https://<redacted>@influx.example:8086/write\""), printed),
				() -> assertTrue(printed.contains("\"bucket\": \"rockserver\""), printed),
				() -> assertTrue(printed.contains("\"org\": \"home\""), printed));
	}

	/** An empty or unset credential prints as it is: the printed default configuration still parses back the same. */
	@Test
	void emptyCredentialsPrintAsTheyAre() {
		var printed = ConfigPrinter.stringify(ConfigParser.parseDefault());

		assertAll(
				() -> assertTrue(printed.contains("\"token\": \"\""), printed),
				() -> assertTrue(printed.contains("\"user\": \"\""), printed),
				() -> assertTrue(printed.contains("\"url\": \"\""), printed),
				() -> assertFalse(printed.contains("<redacted>"), printed));
	}
}
