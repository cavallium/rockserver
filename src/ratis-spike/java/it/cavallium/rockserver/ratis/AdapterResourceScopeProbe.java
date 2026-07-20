package it.cavallium.rockserver.ratis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class AdapterResourceScopeProbe {

    private AdapterResourceScopeProbe() {
    }

    public static void run() throws IOException {
        var closed = new ArrayList<String>();
        var scope = new AdapterResourceScope();
        scope.add(() -> closed.add("first"));
        scope.add(() -> {
            closed.add("second");
            throw new IOException("second close failure");
        });
        scope.add(() -> {
            closed.add("third");
            throw new IllegalStateException("third close failure");
        });

        IOException failure;
        try {
            scope.close();
            throw new IllegalStateException("Adapter resource scope must report close failures");
        } catch (IOException e) {
            failure = e;
        }
        require(closed.equals(List.of("third", "second", "first")),
                "Adapter resource scope must drain resources in reverse order");
        require(failure.getSuppressed().length == 2,
                "Adapter resource scope must aggregate close failures");

        scope.close();
        var rejectedAddition = false;
        try {
            scope.add(() -> {
            });
        } catch (IllegalStateException expected) {
            rejectedAddition = true;
        }
        require(rejectedAddition, "Closed adapter resource scope must reject additions");

        var fatalClosed = new ArrayList<String>();
        var fatalScope = new AdapterResourceScope();
        var fatalFailure = new AssertionError("fourth close failure");
        fatalScope.add(() -> fatalClosed.add("first"));
        fatalScope.add(() -> {
            fatalClosed.add("second");
            throw new IOException("second close failure");
        });
        fatalScope.add(() -> {
            fatalClosed.add("third");
            throw new IllegalStateException("third close failure");
        });
        fatalScope.add(() -> {
            fatalClosed.add("fourth");
            throw fatalFailure;
        });

        Error reportedFatalFailure;
        try {
            fatalScope.close();
            throw new IllegalStateException("Adapter resource scope must report fatal close failures");
        } catch (Error e) {
            reportedFatalFailure = e;
        }
        require(reportedFatalFailure == fatalFailure,
                "Adapter resource scope must preserve its first fatal close failure");
        require(fatalClosed.equals(List.of("fourth", "third", "second", "first")),
                "Adapter resource scope must drain resources after a fatal close failure");
        require(fatalFailure.getSuppressed().length == 2,
                "Adapter resource scope must aggregate failures after a fatal close failure");
        fatalScope.close();

        System.out.println("Ratis adapter resource-scope probe OK");
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }
}
