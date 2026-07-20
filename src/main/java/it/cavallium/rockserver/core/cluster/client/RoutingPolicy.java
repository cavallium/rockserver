package it.cavallium.rockserver.core.cluster.client;

import it.cavallium.rockserver.core.cluster.ReadConsistency;

public record RoutingPolicy(ReadConsistency consistency,
                            long minAppliedIndex,
                            boolean preferFollowers) {

    public RoutingPolicy {
        requireArgument(consistency, "consistency");
        if (minAppliedIndex < 0) {
            throw new IllegalArgumentException("minAppliedIndex must be >= 0");
        }
        if (!consistency.followerAllowed() && preferFollowers) {
            throw new IllegalArgumentException("linearizable routing cannot prefer followers");
        }
    }

    public static RoutingPolicy linearizable() {
        return new RoutingPolicy(ReadConsistency.LINEARIZABLE, 0, false);
    }

    public static RoutingPolicy follower(long minAppliedIndex) {
        return new RoutingPolicy(ReadConsistency.FOLLOWER, minAppliedIndex, true);
    }

    public static RoutingPolicy boundedStaleness(long minAppliedIndex) {
        return new RoutingPolicy(ReadConsistency.BOUNDED_STALENESS, minAppliedIndex, true);
    }

    public static RoutingPolicy local() {
        return new RoutingPolicy(ReadConsistency.LOCAL, 0, false);
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
