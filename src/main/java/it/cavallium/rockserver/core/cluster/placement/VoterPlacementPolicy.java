package it.cavallium.rockserver.core.cluster.placement;

import it.cavallium.rockserver.core.cluster.ClusterNodeMetadata;
import it.cavallium.rockserver.core.cluster.NodeHealthReport;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.ReplicaMember;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public final class VoterPlacementPolicy {

    private static final Comparator<Candidate> CANDIDATE_ORDER = Comparator
            .comparingInt(Candidate::partitionReplicaCount)
            .thenComparingDouble(Candidate::processCpuLoad)
            .thenComparing(candidate -> candidate.node().nodeId());

    public List<ClusterNodeMetadata> selectVoters(Collection<ClusterNodeMetadata> nodes,
                                                  Map<NodeId, NodeHealthReport> healthReports,
                                                  VoterPlacementConstraints constraints,
                                                  long nowMillis) {
        var requiredNodes = requireArgument(nodes, "nodes");
        var requiredHealthReports = validateHealthReports(requireArgument(healthReports, "healthReports"));
        var requiredConstraints = requireArgument(constraints, "constraints");
        if (nowMillis < 0) {
            throw new IllegalArgumentException("nowMillis must be >= 0");
        }

        var candidatesByDomain = new TreeMap<String, List<Candidate>>();
        var eligibleCandidates = new ArrayList<Candidate>();
        for (ClusterNodeMetadata node : requiredNodes) {
            var requiredNode = requireArgument(node, "nodes entry");
            var healthReport = requiredHealthReports.get(requiredNode.nodeId());
            if (healthReport == null) {
                continue;
            }
            if (!healthReport.nodeId().equals(requiredNode.nodeId())) {
                throw new IllegalArgumentException("health report node id does not match node metadata: "
                        + requiredNode.nodeId());
            }
            if (!healthReport.canAcceptReplica(
                    requiredConstraints.minAvailableDiskBytes(),
                    requiredConstraints.maxMemoryUtilization(),
                    nowMillis,
                    requiredConstraints.maxHealthReportAgeMillis())) {
                continue;
            }
            var candidate = new Candidate(requiredNode, healthReport);
            candidatesByDomain.computeIfAbsent(requiredNode.failureDomain(), ignored -> new ArrayList<>()).add(candidate);
            eligibleCandidates.add(candidate);
        }

        if (eligibleCandidates.size() < requiredConstraints.targetVoterCount()) {
            throw new IllegalStateException("not enough healthy nodes for target voter count");
        }
        if (candidatesByDomain.size() < requiredConstraints.minFailureDomains()) {
            throw new IllegalStateException("not enough healthy failure domains for voter placement");
        }

        for (List<Candidate> domainCandidates : candidatesByDomain.values()) {
            domainCandidates.sort(CANDIDATE_ORDER);
        }
        eligibleCandidates.sort(CANDIDATE_ORDER);

        var selected = new ArrayList<Candidate>();
        var selectedNodeIds = new HashSet<NodeId>();
        var selectedDomains = new HashMap<String, Integer>();

        var domainOrder = new ArrayList<>(candidatesByDomain.entrySet());
        domainOrder.sort((left, right) -> {
            int comparison = CANDIDATE_ORDER.compare(left.getValue().getFirst(), right.getValue().getFirst());
            if (comparison != 0) {
                return comparison;
            }
            return left.getKey().compareTo(right.getKey());
        });
        for (Map.Entry<String, List<Candidate>> entry : domainOrder) {
            if (selectedDomains.size() >= requiredConstraints.minFailureDomains()) {
                break;
            }
            addCandidate(entry.getValue().getFirst(), selected, selectedNodeIds, selectedDomains);
        }

        while (selected.size() < requiredConstraints.targetVoterCount()) {
            var next = eligibleCandidates.stream()
                    .filter(candidate -> !selectedNodeIds.contains(candidate.node().nodeId()))
                    .min(Comparator
                            .comparingInt((Candidate candidate) -> selectedDomains.getOrDefault(
                                    candidate.failureDomain(), 0))
                            .thenComparing(CANDIDATE_ORDER))
                    .orElseThrow(() -> new IllegalStateException("not enough healthy nodes for target voter count"));
            addCandidate(next, selected, selectedNodeIds, selectedDomains);
        }

        return selected.stream()
                .map(Candidate::node)
                .toList();
    }

    public void validateVoterPlacement(Collection<ReplicaMember> peers,
                                       Map<NodeId, ClusterNodeMetadata> nodesById,
                                       VoterPlacementConstraints constraints) {
        var requiredPeers = requireArgument(peers, "peers");
        var requiredNodesById = validateNodesById(requireArgument(nodesById, "nodesById"));
        var requiredConstraints = requireArgument(constraints, "constraints");

        var voterNodeIds = new HashSet<NodeId>();
        var failureDomains = new HashSet<String>();
        int voterCount = 0;
        for (ReplicaMember peer : requiredPeers) {
            var requiredPeer = requireArgument(peer, "peers entry");
            if (!requiredPeer.isVoter()) {
                continue;
            }
            voterCount++;
            if (!voterNodeIds.add(requiredPeer.nodeId())) {
                throw new IllegalStateException("duplicate voter node: " + requiredPeer.nodeId());
            }
            var node = requiredNodesById.get(requiredPeer.nodeId());
            if (node == null) {
                throw new IllegalStateException("missing node metadata for voter: " + requiredPeer.nodeId());
            }
            failureDomains.add(node.failureDomain());
        }

        if (voterCount != requiredConstraints.targetVoterCount()) {
            throw new IllegalStateException("voter count does not match target voter count");
        }
        if (failureDomains.size() < requiredConstraints.minFailureDomains()) {
            throw new IllegalStateException("voter placement does not satisfy minimum failure domains");
        }
    }

    private static void addCandidate(Candidate candidate,
                                     List<Candidate> selected,
                                     HashSet<NodeId> selectedNodeIds,
                                     HashMap<String, Integer> selectedDomains) {
        selected.add(candidate);
        selectedNodeIds.add(candidate.node().nodeId());
        selectedDomains.merge(candidate.failureDomain(), 1, Integer::sum);
    }

    private record Candidate(ClusterNodeMetadata node, NodeHealthReport healthReport) {

        String failureDomain() {
            return node.failureDomain();
        }

        int partitionReplicaCount() {
            return healthReport.partitionReplicaCount();
        }

        double processCpuLoad() {
            return healthReport.processCpuLoad();
        }
    }

    private static Map<NodeId, NodeHealthReport> validateHealthReports(Map<NodeId, NodeHealthReport> healthReports) {
        for (Map.Entry<NodeId, NodeHealthReport> entry : healthReports.entrySet()) {
            requireArgument(entry.getKey(), "healthReports key");
            var report = requireArgument(entry.getValue(), "healthReports value");
            if (!report.nodeId().equals(entry.getKey())) {
                throw new IllegalArgumentException("health report node id does not match healthReports key: "
                        + entry.getKey());
            }
        }
        return healthReports;
    }

    private static Map<NodeId, ClusterNodeMetadata> validateNodesById(Map<NodeId, ClusterNodeMetadata> nodesById) {
        for (Map.Entry<NodeId, ClusterNodeMetadata> entry : nodesById.entrySet()) {
            var nodeId = requireArgument(entry.getKey(), "nodesById key");
            var node = requireArgument(entry.getValue(), "nodesById value");
            if (!node.nodeId().equals(nodeId)) {
                throw new IllegalArgumentException("node metadata node id does not match nodesById key: " + nodeId);
            }
        }
        return nodesById;
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
