package it.cavallium.rockserver.core.cluster;

@FunctionalInterface
public interface ReplicationFailureInjector {

    ReplicationFailureInjector NONE = ignored -> {
    };

    void inject(ReplicationFailurePhase phase);

    static ReplicationFailureInjector none() {
        return NONE;
    }

    static ReplicationFailureInjector failOn(ReplicationFailurePhase failedPhase, RuntimeException failure) {
        var requiredFailedPhase = requireArgument(failedPhase, "failedPhase");
        var requiredFailure = requireArgument(failure, "failure");
        return phase -> {
            if (phase == requiredFailedPhase) {
                throw requiredFailure;
            }
        };
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
