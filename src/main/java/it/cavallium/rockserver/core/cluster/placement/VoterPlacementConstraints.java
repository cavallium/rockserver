package it.cavallium.rockserver.core.cluster.placement;

public record VoterPlacementConstraints(int targetVoterCount,
                                        int minFailureDomains,
                                        long minAvailableDiskBytes,
                                        double maxMemoryUtilization,
                                        long maxHealthReportAgeMillis) {

    public VoterPlacementConstraints {
        if (targetVoterCount <= 0) {
            throw new IllegalArgumentException("targetVoterCount must be > 0");
        }
        if (minFailureDomains <= 0) {
            throw new IllegalArgumentException("minFailureDomains must be > 0");
        }
        if (minFailureDomains > targetVoterCount) {
            throw new IllegalArgumentException("minFailureDomains must be <= targetVoterCount");
        }
        if (minAvailableDiskBytes < 0) {
            throw new IllegalArgumentException("minAvailableDiskBytes must be >= 0");
        }
        if (Double.isNaN(maxMemoryUtilization)
                || maxMemoryUtilization < 0.0d
                || maxMemoryUtilization > 1.0d) {
            throw new IllegalArgumentException("maxMemoryUtilization must be between 0.0 and 1.0");
        }
        if (maxHealthReportAgeMillis < 0) {
            throw new IllegalArgumentException("maxHealthReportAgeMillis must be >= 0");
        }
    }
}
