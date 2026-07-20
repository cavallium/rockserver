package it.cavallium.rockserver.core.cluster.rebalance;

public record AdminRebalanceDecision(AdminRebalanceRequest request,
                                     String message) {

    public AdminRebalanceDecision {
        if (request == null) {
            throw new IllegalArgumentException("request must not be null");
        }
        if (message == null || message.isBlank()) {
            throw new IllegalArgumentException("message must not be blank");
        }
    }
}
