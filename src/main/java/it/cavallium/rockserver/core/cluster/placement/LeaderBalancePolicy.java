package it.cavallium.rockserver.core.cluster.placement;

import it.cavallium.rockserver.core.cluster.NodeHealthReport;
import it.cavallium.rockserver.core.cluster.NodeHealthStatus;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.PartitionDescriptor;
import it.cavallium.rockserver.core.cluster.ReplicaMember;
import it.cavallium.rockserver.core.cluster.ShardId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class LeaderBalancePolicy {

    public List<LeaderTransferRecommendation> planTransfers(Collection<PartitionDescriptor> partitions,
                                                            Map<NodeId, NodeHealthReport> healthReports,
                                                            LeaderBalanceConstraints constraints,
                                                            long nowMillis) {
        var requiredPartitions = requireArgument(partitions, "partitions");
        var requiredHealthReports = validateHealthReports(requireArgument(healthReports, "healthReports"));
        var requiredConstraints = requireArgument(constraints, "constraints");
        if (nowMillis < 0) {
            throw new IllegalArgumentException("nowMillis must be >= 0");
        }
        if (requiredConstraints.maxTransfers() == 0) {
            return List.of();
        }

        var sortedPartitions = new ArrayList<PartitionDescriptor>();
        for (PartitionDescriptor partition : requiredPartitions) {
            var requiredPartition = requireArgument(partition, "partitions entry");
            sortedPartitions.add(requiredPartition);
        }
        sortedPartitions.sort(Comparator.comparing(PartitionDescriptor::partitionId));

        var leaderCounts = leaderCounts(sortedPartitions, requiredHealthReports);
        var plannedPartitionIds = new HashSet<ShardId>();
        var recommendations = new ArrayList<LeaderTransferRecommendation>();

        while (recommendations.size() < requiredConstraints.maxTransfers()) {
            var source = mostLoadedLeader(leaderCounts).orElse(null);
            if (source == null) {
                break;
            }
            var recommendation = nextTransfer(
                    sortedPartitions,
                    requiredHealthReports,
                    requiredConstraints,
                    nowMillis,
                    leaderCounts,
                    plannedPartitionIds,
                    source
            ).orElse(null);
            if (recommendation == null) {
                break;
            }
            recommendations.add(recommendation);
            plannedPartitionIds.add(recommendation.partitionId());
            leaderCounts.merge(recommendation.fromLeader(), -1, Integer::sum);
            leaderCounts.merge(recommendation.toLeader(), 1, Integer::sum);
        }

        return List.copyOf(recommendations);
    }

    private Optional<LeaderTransferRecommendation> nextTransfer(List<PartitionDescriptor> partitions,
                                                                Map<NodeId, NodeHealthReport> healthReports,
                                                                LeaderBalanceConstraints constraints,
                                                                long nowMillis,
                                                                Map<NodeId, Integer> leaderCounts,
                                                                HashSet<ShardId> plannedPartitionIds,
                                                                NodeId source) {
        var sourceLeaderCount = leaderCounts.getOrDefault(source, 0);
        for (PartitionDescriptor partition : partitions) {
            if (plannedPartitionIds.contains(partition.partitionId())
                    || partition.isSplitting()
                    || !source.equals(partition.leaderHint())) {
                continue;
            }
            var sourcePeer = findPeer(partition, source).orElse(null);
            if (sourcePeer == null || !sourcePeer.isVoter()) {
                continue;
            }
            var target = partition.peers().stream()
                    .filter(peer -> !peer.nodeId().equals(source))
                    .filter(peer -> isEligibleTarget(peer, sourcePeer, healthReports, constraints, nowMillis))
                    .min(Comparator
                            .comparingInt((ReplicaMember peer) -> leaderCounts.getOrDefault(peer.nodeId(), 0))
                            .thenComparingInt(peer -> healthReports.get(peer.nodeId()).partitionReplicaCount())
                            .thenComparingDouble(peer -> healthReports.get(peer.nodeId()).processCpuLoad())
                            .thenComparing(ReplicaMember::nodeId))
                    .orElse(null);
            if (target == null) {
                continue;
            }
            var targetLeaderCount = leaderCounts.getOrDefault(target.nodeId(), 0);
            if (sourceLeaderCount - targetLeaderCount <= 1) {
                continue;
            }
            return Optional.of(new LeaderTransferRecommendation(
                    partition.partitionId(),
                    partition.epoch(),
                    source,
                    target.nodeId(),
                    sourceLeaderCount - 1,
                    targetLeaderCount + 1
            ));
        }
        return Optional.empty();
    }

    private static boolean isEligibleTarget(ReplicaMember candidate,
                                            ReplicaMember sourcePeer,
                                            Map<NodeId, NodeHealthReport> healthReports,
                                            LeaderBalanceConstraints constraints,
                                            long nowMillis) {
        if (!candidate.isVoter() || !candidate.servingReads()) {
            return false;
        }
        if (candidate.matchIndex() < sourcePeer.matchIndex()
                || candidate.appliedIndex() < sourcePeer.appliedIndex()) {
            return false;
        }
        var healthReport = healthReports.get(candidate.nodeId());
        if (healthReport == null) {
            return false;
        }
        if (!healthReport.nodeId().equals(candidate.nodeId())) {
            throw new IllegalArgumentException("health report node id does not match peer: " + candidate.nodeId());
        }
        return healthReport.status() == NodeHealthStatus.HEALTHY
                && healthReport.isFresh(nowMillis, constraints.maxHealthReportAgeMillis());
    }

    private static Map<NodeId, Integer> leaderCounts(List<PartitionDescriptor> partitions,
                                                     Map<NodeId, NodeHealthReport> healthReports) {
        var leaderCounts = new HashMap<NodeId, Integer>();
        for (NodeId nodeId : healthReports.keySet()) {
            leaderCounts.putIfAbsent(nodeId, 0);
        }
        for (PartitionDescriptor partition : partitions) {
            if (partition.leaderHint() != null) {
                leaderCounts.merge(partition.leaderHint(), 1, Integer::sum);
            }
        }
        return leaderCounts;
    }

    private static Optional<NodeId> mostLoadedLeader(Map<NodeId, Integer> leaderCounts) {
        return leaderCounts.entrySet().stream()
                .filter(entry -> entry.getValue() > 0)
                .max(Map.Entry.<NodeId, Integer>comparingByValue()
                        .thenComparing(Map.Entry::getKey))
                .map(Map.Entry::getKey);
    }

    private static Optional<ReplicaMember> findPeer(PartitionDescriptor partition, NodeId nodeId) {
        return partition.peers().stream()
                .filter(peer -> peer.nodeId().equals(nodeId))
                .findFirst();
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

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
