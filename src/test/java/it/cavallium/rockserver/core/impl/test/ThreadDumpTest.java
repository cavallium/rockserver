package it.cavallium.rockserver.core.impl.test;

import org.junit.jupiter.api.Test;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class ThreadDumpTest {
    @Test
    public void runAndDump() throws Exception {
        // Run the logic from RandomizedLeakStressTest manually
        RandomizedLeakStressTest test = new RandomizedLeakStressTest();
        try {
            test.setUp();
            test.randomizedWorkloadHasNoLeaks();
        } finally {
            test.tearDown();
        }

        // Wait a bit for shutdown
        Thread.sleep(2000);

        StringBuilder dump = new StringBuilder();
        
        boolean foundNonDaemon = false;
        dump.append("=== THREAD DUMP ===\n");
        Map<Thread, StackTraceElement[]> traces = Thread.getAllStackTraces();
        for (Map.Entry<Thread, StackTraceElement[]> entry : traces.entrySet()) {
            Thread t = entry.getKey();
            if (t.getName().equals("main")) continue; 
            if (t.isDaemon()) continue;

            foundNonDaemon = true;
            dump.append("Thread: ").append(t.getName()).append(" ID:").append(t.getId()).append("\n");
            dump.append("State: ").append(t.getState()).append("\n");
            for (StackTraceElement ste : entry.getValue()) {
                dump.append("\t").append(ste.toString()).append("\n");
            }
            dump.append("\n");
        }

        Path out = Path.of("thread_dump.txt");
        Files.writeString(out, dump.toString(), StandardCharsets.UTF_8);
        
        if (foundNonDaemon) {
             throw new RuntimeException("Found non-daemon threads! See thread_dump.txt");
        }
    }
}
