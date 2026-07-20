package it.cavallium.rockserver.core.cluster.placement;

public record LeaderBalanceConstraints(int maxTransfers,
                                       long maxHealthReportAgeMillis) {

    public LeaderBalanceConstraints {
        if (maxTransfers < 0) {
            throw new IllegalArgumentException("maxTransfers must be >= 0");
        }
        if (maxHealthReportAgeMillis < 0) {
            throw new IllegalArgumentException("maxHealthReportAgeMillis must be >= 0");
        }
    }
}
