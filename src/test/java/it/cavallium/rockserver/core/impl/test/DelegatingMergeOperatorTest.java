package it.cavallium.rockserver.core.impl.test;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.impl.DelegatingMergeOperator;
import it.cavallium.rockserver.core.impl.FFMAbstractMergeOperator;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

public class DelegatingMergeOperatorTest {

    @Test
    void rejectsNullInputsWithExplicitDiagnostics() {
        assertIllegalArgument("name must not be null",
            DelegatingMergeOperatorTest::createWithNullName);
        assertIllegalArgument("delegate must not be null",
            DelegatingMergeOperatorTest::createWithNullDelegate);

        var original = new TrackingMergeOperator("original");
        try (var operator = new DelegatingMergeOperator("delegating", original)) {
            assertIllegalArgument("newDelegate must not be null",
                () -> operator.setDelegate(null));
            Assertions.assertSame(original, operator.getDelegate());
        }
    }

    @Test
    void replacingDelegateClosesPreviousDelegate() {
        var original = new TrackingMergeOperator("original");
        var replacement = new TrackingMergeOperator("replacement");

        try (var operator = new DelegatingMergeOperator("delegating", original)) {
            operator.setDelegate(replacement);

            Assertions.assertSame(replacement, operator.getDelegate());
            Assertions.assertEquals(1, original.closeCalls);
            Assertions.assertEquals(0, replacement.closeCalls);
        }

        Assertions.assertEquals(1, replacement.closeCalls);
    }

    private static void assertIllegalArgument(String message, Executable executable) {
        var error = Assertions.assertThrows(IllegalArgumentException.class, executable);
        Assertions.assertEquals(message, error.getMessage());
    }

    private static void createWithNullName() {
        try (var ignored = new DelegatingMergeOperator(null, null)) {
            Assertions.fail("constructor should reject null name");
        }
    }

    private static void createWithNullDelegate() {
        try (var ignored = new DelegatingMergeOperator("delegating", null)) {
            Assertions.fail("constructor should reject null delegate");
        }
    }

    private static final class TrackingMergeOperator extends FFMAbstractMergeOperator {

        private int closeCalls;

        private TrackingMergeOperator(String name) {
            super(name);
        }

        @Override
        public Buf merge(Buf key, Buf existingValue, List<Buf> operands) {
            return existingValue;
        }

        @Override
        public void close() {
            closeCalls++;
            super.close();
        }
    }
}
