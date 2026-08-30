package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertFalse;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import org.junit.jupiter.api.Test;

class SchedulerStateModelCorpusTest {

	@Test
	void checkedInTracesRemainReplayableByOrdinaryMavenTests() throws Exception {
		try (var input = SchedulerStateModelCorpusTest.class.getResourceAsStream(
				"/scheduler-state-model/traces.hex")) {
			if (input == null) throw new AssertionError("missing scheduler state-model corpus");
			var lines = new String(input.readAllBytes(), StandardCharsets.UTF_8).lines()
					.map(String::trim)
					.filter(line -> !line.isEmpty() && !line.startsWith("#"))
					.toList();
			assertFalse(lines.isEmpty(), "scheduler state-model corpus is empty");
			for (String line : lines) {
				int separator = line.indexOf('=');
				if (separator <= 0) throw new AssertionError("invalid scheduler corpus line: " + line);
				String name = line.substring(0, separator);
				byte[] trace = HexFormat.of().parseHex(line.substring(separator + 1));
				try {
					SchedulerStateTraceRunner.run(trace);
				} catch (Throwable failure) {
					throw new AssertionError("checked-in scheduler trace failed: " + name, failure);
				}
			}
		}
	}
}
