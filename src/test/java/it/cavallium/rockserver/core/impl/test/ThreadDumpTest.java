package it.cavallium.rockserver.core.impl.test;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.fail;

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
            if (!isRockserverThread(t)) continue;

            foundNonDaemon = true;
            dump.append("Thread: ").append(t.getName()).append(" ID:").append(t.threadId()).append("\n");
            dump.append("State: ").append(t.getState()).append("\n");
            for (StackTraceElement ste : entry.getValue()) {
                dump.append("\t").append(ste.toString()).append("\n");
            }
            dump.append("\n");
        }

        if (foundNonDaemon) {
            Path out = Path.of("target", "thread-dumps", "thread_dump.txt");
            Files.createDirectories(out.getParent());
            Files.writeString(out, dump.toString(), StandardCharsets.UTF_8);
            fail("Found non-daemon Rockserver threads. See " + out.toAbsolutePath());
        }
    }

    private static boolean isRockserverThread(Thread t) {
        String name = t.getName();
        return name.startsWith("db[")
                || name.startsWith("db-leak-scheduler")
                || name.startsWith("grpc-db")
                || name.startsWith("nioEventLoopGroup")
                || name.startsWith("epollEventLoopGroup");
    }
}
