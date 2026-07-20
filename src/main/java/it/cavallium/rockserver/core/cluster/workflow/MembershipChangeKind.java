package it.cavallium.rockserver.core.cluster.workflow;

public enum MembershipChangeKind {
    ADD_REPLICA,
    PROMOTE_REPLICA,
    REMOVE_REPLICA,
    TRANSFER_LEADER
}
