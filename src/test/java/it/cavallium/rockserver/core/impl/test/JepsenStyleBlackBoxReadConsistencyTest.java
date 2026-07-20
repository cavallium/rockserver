package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

class JepsenStyleBlackBoxReadConsistencyTest {

    @Test
    void acceptsPutGetDeleteAndMergeHistoryWithConsistentReads() {
        var client = new BlackBoxKvClient();

        client.put("k1", "a");
        client.get("k1");
        client.merge("k1", "b");
        client.get("k1");
        client.delete("k1");
        client.get("k1");
        client.merge("k2", "x");
        client.get("k2");

        assertEquals(List.of(), ReadConsistencyChecker.violations(client.history()));
        assertEquals(Arrays.asList("a", "ab", null, "x"), client.readOutputs());
    }

    @Test
    void rejectsStaleReadAfterCompletedWrite() {
        var history = List.of(
                Operation.put("k1", "a", 1, 2),
                Operation.get("k1", null, 3, 4)
        );

        assertEquals(List.of("read at 3 for key k1 returned <null> but expected <a>"),
                ReadConsistencyChecker.violations(history));
    }

    private enum OperationKind {
        PUT,
        GET,
        DELETE,
        MERGE
    }

    private record Operation(OperationKind kind,
                             String key,
                             @Nullable String input,
                             long invokeTime,
                             long okTime,
                             @Nullable String output) {

        private static Operation put(String key, String value, long invokeTime, long okTime) {
            return new Operation(OperationKind.PUT, key, value, invokeTime, okTime, null);
        }

        private static Operation get(String key, @Nullable String output, long invokeTime, long okTime) {
            return new Operation(OperationKind.GET, key, null, invokeTime, okTime, output);
        }

        private static Operation delete(String key, long invokeTime, long okTime) {
            return new Operation(OperationKind.DELETE, key, null, invokeTime, okTime, null);
        }

        private static Operation merge(String key, String value, long invokeTime, long okTime) {
            return new Operation(OperationKind.MERGE, key, value, invokeTime, okTime, null);
        }
    }

    private static final class BlackBoxKvClient {

        private final Map<String, String> state = new HashMap<>();
        private final ArrayList<Operation> history = new ArrayList<>();
        private long clock;

        private void put(String key, String value) {
            var invoke = nextTime();
            state.put(key, value);
            history.add(Operation.put(key, value, invoke, nextTime()));
        }

        private void delete(String key) {
            var invoke = nextTime();
            state.remove(key);
            history.add(Operation.delete(key, invoke, nextTime()));
        }

        private void merge(String key, String value) {
            var invoke = nextTime();
            state.merge(key, value, String::concat);
            history.add(Operation.merge(key, value, invoke, nextTime()));
        }

        private @Nullable String get(String key) {
            var invoke = nextTime();
            var output = state.get(key);
            history.add(Operation.get(key, output, invoke, nextTime()));
            return output;
        }

        private List<Operation> history() {
            return List.copyOf(history);
        }

        private List<String> readOutputs() {
            return history.stream()
                    .filter(operation -> operation.kind() == OperationKind.GET)
                    .map(Operation::output)
                    .toList();
        }

        private long nextTime() {
            return ++clock;
        }
    }

    private static final class ReadConsistencyChecker {

        private static List<String> violations(List<Operation> history) {
            var violations = new ArrayList<String>();
            for (Operation operation : history) {
                if (operation.kind() != OperationKind.GET) {
                    continue;
                }
                var expected = expectedValue(history, operation);
                if (!java.util.Objects.equals(expected, operation.output())) {
                    violations.add("read at " + operation.invokeTime()
                            + " for key " + operation.key()
                            + " returned <" + format(operation.output())
                            + "> but expected <" + format(expected) + ">");
                }
            }
            return List.copyOf(violations);
        }

        private static @Nullable String expectedValue(List<Operation> history, Operation read) {
            var state = new HashMap<String, String>();
            history.stream()
                    .filter(operation -> operation.kind() != OperationKind.GET)
                    .filter(operation -> operation.okTime() <= read.invokeTime())
                    .sorted(Comparator.comparingLong(Operation::okTime))
                    .forEach(operation -> apply(state, operation));
            return state.get(read.key());
        }

        private static void apply(Map<String, String> state, Operation operation) {
            switch (operation.kind()) {
                case PUT -> state.put(operation.key(), operation.input());
                case DELETE -> state.remove(operation.key());
                case MERGE -> state.merge(operation.key(), operation.input(), String::concat);
                case GET -> throw new IllegalArgumentException("read operations do not update model state");
            }
        }

        private static String format(@Nullable String value) {
            return value == null ? "null" : value;
        }
    }
}
