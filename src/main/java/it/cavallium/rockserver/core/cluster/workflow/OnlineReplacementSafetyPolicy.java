package it.cavallium.rockserver.core.cluster.workflow;

import it.cavallium.rockserver.core.replication.ratis.RatisNonVotingCatchUpAssessment;

public record OnlineReplacementSafetyPolicy(boolean nonVotingCatchUpProven) {

    public static OnlineReplacementSafetyPolicy from(RatisNonVotingCatchUpAssessment assessment) {
        var requiredAssessment = requireArgument(assessment, "assessment");
        return new OnlineReplacementSafetyPolicy(requiredAssessment.permitsNonVotingOnlineReplacement());
    }

    public void requireAllowed(ReplacementWorkflowMode mode, boolean existingQuorumHealthy) {
        requireArgument(mode, "mode");
        if (!existingQuorumHealthy) {
            throw new IllegalStateException("online replacement requires an existing healthy quorum");
        }
        if (mode == ReplacementWorkflowMode.NON_VOTING_CATCH_UP && !nonVotingCatchUpProven) {
            throw new IllegalStateException("Ratis non-voting catch-up and promotion behavior is not proven");
        }
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
