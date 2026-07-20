package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.cluster.workflow.OnlineReplacementSafetyPolicy;
import it.cavallium.rockserver.core.cluster.workflow.ReplacementWorkflowMode;
import it.cavallium.rockserver.core.replication.ratis.RatisNonVotingCatchUpAssessment;

import org.junit.jupiter.api.Test;

class RatisNonVotingCatchUpAssessmentTest {

    @Test
    void assessesRatis322ClasspathConservativelyWithoutPromotionProof() {
        var assessment = RatisNonVotingCatchUpAssessment.assessClasspath("3.2.2");

        assertEquals("3.2.2", assessment.ratisVersion());
        assertFalse(assessment.observerRolePresent());
        if (assessment.listenerRolePresent()) {
            assertTrue(assessment.listenerStartupRoleSupported());
            assertTrue(assessment.listenerConfigurationApiPresent());
        }
        assertFalse(assessment.catchUpAndPromotionProven());
        assertFalse(assessment.permitsNonVotingOnlineReplacement());
    }

    @Test
    void safetyPolicyBlocksUnprovenNonVotingReplacementButAllowsConservativeHealthyVoterChange() {
        var policy = OnlineReplacementSafetyPolicy.from(RatisNonVotingCatchUpAssessment.assessClasspath("3.2.2"));

        policy.requireAllowed(ReplacementWorkflowMode.CONSERVATIVE_VOTER_CHANGE, true);
        assertThrows(IllegalStateException.class,
                () -> policy.requireAllowed(ReplacementWorkflowMode.CONSERVATIVE_VOTER_CHANGE, false));
        assertThrows(IllegalStateException.class,
                () -> policy.requireAllowed(ReplacementWorkflowMode.NON_VOTING_CATCH_UP, true));
    }

    @Test
    void safetyPolicyRejectsNullInputsWithExplicitDiagnostics() {
        assertIllegalArgument("assessment must not be null", () -> OnlineReplacementSafetyPolicy.from(null));
        assertIllegalArgument("mode must not be null",
                () -> new OnlineReplacementSafetyPolicy(true).requireAllowed(null, true));
    }

    private static void assertIllegalArgument(String message, Runnable operation) {
        var failure = assertThrows(IllegalArgumentException.class, operation::run);
        assertEquals(message, failure.getMessage());
    }
}
