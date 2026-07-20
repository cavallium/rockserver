package it.cavallium.rockserver.core.cluster.workflow;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.PartitionDescriptor;
import it.cavallium.rockserver.core.cluster.PartitionSplitState;
import it.cavallium.rockserver.core.cluster.RockserverKeyEncoder;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.SingleLineStrings;
import it.cavallium.rockserver.core.cluster.command.ReplicatedCdcToken;
import it.cavallium.rockserver.core.cluster.metadata.CdcTokenTranslation;
import it.cavallium.rockserver.core.cluster.metadata.MetadataCommand;

public final class PartitionTopologyWorkflowPlanner {

    public PartitionSplitPlan planSplit(ClusterMetadata metadata,
                                        long expectedMetadataEpoch,
                                        ShardId sourcePartitionId,
                                        long expectedSourceEpoch,
                                        ShardId leftPartitionId,
                                        ShardId rightPartitionId,
                                        Buf splitKey,
                                        String leftRatisGroupId,
                                        String rightRatisGroupId,
                                        ReplicatedCdcToken sourceEndToken) {
        var requiredMetadata = requireArgument(metadata, "metadata");
        var requiredSourcePartitionId = requireArgument(sourcePartitionId, "sourcePartitionId");
        var requiredLeftPartitionId = requireArgument(leftPartitionId, "leftPartitionId");
        var requiredRightPartitionId = requireArgument(rightPartitionId, "rightPartitionId");
        var requiredSplitKey = requireArgument(splitKey, "splitKey");
        var requiredSourceEndToken = requireArgument(sourceEndToken, "sourceEndToken");
        requireMetadataEpoch(requiredMetadata, expectedMetadataEpoch);
        var source = requiredMetadata.requirePartition(requiredSourcePartitionId);
        requiredMetadata.requireGroup(requiredSourcePartitionId);
        if (source.epoch() != expectedSourceEpoch) {
            throw new IllegalArgumentException("source partition epoch does not match expectedSourceEpoch");
        }
        if (source.splitState() != PartitionSplitState.ACTIVE) {
            throw new IllegalStateException("source partition must be active before split");
        }
        if (requiredLeftPartitionId.equals(requiredSourcePartitionId)
                || requiredRightPartitionId.equals(requiredSourcePartitionId)) {
            throw new IllegalArgumentException("split child partition id must not reuse source partition id");
        }
        if (requiredLeftPartitionId.equals(requiredRightPartitionId)) {
            throw new IllegalArgumentException("split child partition ids must be distinct");
        }
        rejectExistingPartition(requiredMetadata, requiredLeftPartitionId, "leftPartitionId");
        rejectExistingPartition(requiredMetadata, requiredRightPartitionId, "rightPartitionId");
        rejectExistingRouteGroup(requiredMetadata, requiredLeftPartitionId, "leftPartitionId");
        rejectExistingRouteGroup(requiredMetadata, requiredRightPartitionId, "rightPartitionId");
        requireStrictSplitBoundary(source, requiredSplitKey);
        requireToken(requiredSourceEndToken, expectedMetadataEpoch, requiredSourcePartitionId, expectedSourceEpoch,
                "sourceEndToken");

        var childEpoch = source.epoch() + 1;
        var translationMetadataEpoch = expectedMetadataEpoch + 1;
        var left = descriptor(source, requiredLeftPartitionId, source.startKeyInclusive(), requiredSplitKey, childEpoch,
                leftRatisGroupId);
        var right = descriptor(source, requiredRightPartitionId, requiredSplitKey, source.endKeyExclusive(), childEpoch,
                rightRatisGroupId);
        var splitCommand = new MetadataCommand.SplitPartition(
                expectedMetadataEpoch,
                requiredSourcePartitionId,
                expectedSourceEpoch,
                left,
                right
        );
        var translation = new CdcTokenTranslation.Split(
                translationMetadataEpoch,
                requiredSourceEndToken,
                new ReplicatedCdcToken(translationMetadataEpoch, requiredLeftPartitionId, childEpoch, 0, 0),
                new ReplicatedCdcToken(translationMetadataEpoch, requiredRightPartitionId, childEpoch, 0, 0)
        );
        var translationCommand = new MetadataCommand.RecordCdcTokenTranslation(translationMetadataEpoch, translation);
        return new PartitionSplitPlan(splitCommand, translationCommand);
    }

    public PartitionMergePlan planMerge(ClusterMetadata metadata,
                                        long expectedMetadataEpoch,
                                        ShardId leftPartitionId,
                                        ShardId rightPartitionId,
                                        ShardId mergedPartitionId,
                                        String mergedRatisGroupId,
                                        ReplicatedCdcToken leftEndToken,
                                        ReplicatedCdcToken rightEndToken) {
        var requiredMetadata = requireArgument(metadata, "metadata");
        var requiredLeftPartitionId = requireArgument(leftPartitionId, "leftPartitionId");
        var requiredRightPartitionId = requireArgument(rightPartitionId, "rightPartitionId");
        var requiredMergedPartitionId = requireArgument(mergedPartitionId, "mergedPartitionId");
        var requiredLeftEndToken = requireArgument(leftEndToken, "leftEndToken");
        var requiredRightEndToken = requireArgument(rightEndToken, "rightEndToken");
        requireMetadataEpoch(requiredMetadata, expectedMetadataEpoch);
        if (requiredMergedPartitionId.equals(requiredLeftPartitionId)
                || requiredMergedPartitionId.equals(requiredRightPartitionId)) {
            throw new IllegalArgumentException("merged partition id must not reuse parent partition id");
        }
        rejectExistingPartition(requiredMetadata, requiredMergedPartitionId, "mergedPartitionId");
        rejectExistingRouteGroup(requiredMetadata, requiredMergedPartitionId, "mergedPartitionId");
        var left = requiredMetadata.requirePartition(requiredLeftPartitionId);
        var right = requiredMetadata.requirePartition(requiredRightPartitionId);
        requiredMetadata.requireGroup(requiredLeftPartitionId);
        requiredMetadata.requireGroup(requiredRightPartitionId);
        requireActive(left, "left partition");
        requireActive(right, "right partition");
        requireAdjacent(left, right);
        requireSameMembership(left, right);
        requireToken(requiredLeftEndToken, expectedMetadataEpoch, requiredLeftPartitionId, left.epoch(), "leftEndToken");
        requireToken(requiredRightEndToken, expectedMetadataEpoch, requiredRightPartitionId, right.epoch(),
                "rightEndToken");

        var mergedEpoch = Math.max(left.epoch(), right.epoch()) + 1;
        var translationMetadataEpoch = expectedMetadataEpoch + 1;
        var merged = new PartitionDescriptor(
                requiredMergedPartitionId,
                left.startKeyInclusive(),
                right.endKeyExclusive(),
                mergedEpoch,
                SingleLineStrings.requireNonBlank(mergedRatisGroupId, "mergedRatisGroupId"),
                left.peers(),
                left.leaderHint(),
                left.placementConstraints(),
                PartitionSplitState.ACTIVE
        );
        var mergeCommand = new MetadataCommand.MergePartitions(
                expectedMetadataEpoch,
                requiredLeftPartitionId,
                requiredRightPartitionId,
                merged
        );
        var translation = new CdcTokenTranslation.Merge(
                translationMetadataEpoch,
                requiredLeftEndToken,
                requiredRightEndToken,
                new ReplicatedCdcToken(translationMetadataEpoch, requiredMergedPartitionId, mergedEpoch, 0, 0)
        );
        var translationCommand = new MetadataCommand.RecordCdcTokenTranslation(translationMetadataEpoch, translation);
        return new PartitionMergePlan(mergeCommand, translationCommand);
    }

    private static PartitionDescriptor descriptor(PartitionDescriptor source,
                                                  ShardId partitionId,
                                                  Buf startKeyInclusive,
                                                  Buf endKeyExclusive,
                                                  long epoch,
                                                  String ratisGroupId) {
        return new PartitionDescriptor(
                partitionId,
                startKeyInclusive,
                endKeyExclusive,
                epoch,
                SingleLineStrings.requireNonBlank(ratisGroupId, "ratisGroupId"),
                source.peers(),
                source.leaderHint(),
                source.placementConstraints(),
                PartitionSplitState.ACTIVE
        );
    }

    private static void requireMetadataEpoch(ClusterMetadata metadata, long expectedMetadataEpoch) {
        if (expectedMetadataEpoch < 0) {
            throw new IllegalArgumentException("expectedMetadataEpoch must be >= 0");
        }
        if (metadata.epoch() != expectedMetadataEpoch) {
            throw new IllegalArgumentException("metadata epoch does not match expectedMetadataEpoch");
        }
    }

    private static void rejectExistingPartition(ClusterMetadata metadata, ShardId partitionId, String name) {
        if (metadata.findPartition(partitionId).isPresent()) {
            throw new IllegalArgumentException(name + " already exists: " + partitionId);
        }
    }

    private static void rejectExistingRouteGroup(ClusterMetadata metadata, ShardId partitionId, String name) {
        if (metadata.findGroup(partitionId).isPresent()) {
            throw new IllegalArgumentException(name + " route group already exists: " + partitionId);
        }
    }

    private static void requireStrictSplitBoundary(PartitionDescriptor source, Buf splitKey) {
        if (source.startKeyInclusive() != null
                && RockserverKeyEncoder.compareCalculatedKeys(source.startKeyInclusive(), splitKey) >= 0) {
            throw new IllegalArgumentException("split key must be greater than source start key");
        }
        if (source.endKeyExclusive() != null
                && RockserverKeyEncoder.compareCalculatedKeys(splitKey, source.endKeyExclusive()) >= 0) {
            throw new IllegalArgumentException("split key must be less than source end key");
        }
    }

    private static void requireActive(PartitionDescriptor partition, String name) {
        if (partition.splitState() != PartitionSplitState.ACTIVE) {
            throw new IllegalStateException(name + " must be active before merge");
        }
    }

    private static void requireAdjacent(PartitionDescriptor left, PartitionDescriptor right) {
        if (left.endKeyExclusive() == null || right.startKeyInclusive() == null) {
            throw new IllegalArgumentException("merge parents must share a concrete boundary");
        }
        if (RockserverKeyEncoder.compareCalculatedKeys(left.endKeyExclusive(), right.startKeyInclusive()) != 0) {
            throw new IllegalArgumentException("merge parents must be adjacent");
        }
    }

    private static void requireSameMembership(PartitionDescriptor left, PartitionDescriptor right) {
        if (!left.peers().equals(right.peers())
                || !sameNullable(left.leaderHint(), right.leaderHint())
                || !left.placementConstraints().equals(right.placementConstraints())) {
            throw new IllegalArgumentException("merge parents must have identical membership and placement metadata");
        }
    }

    private static void requireToken(ReplicatedCdcToken token,
                                     long expectedMetadataEpoch,
                                     ShardId partitionId,
                                     long partitionEpoch,
                                     String name) {
        if (token.metadataEpoch() > expectedMetadataEpoch) {
            throw new IllegalArgumentException(name + " metadata epoch must not exceed expected metadata epoch");
        }
        if (!token.partitionId().equals(partitionId)) {
            throw new IllegalArgumentException(name + " partition id does not match workflow partition");
        }
        if (token.partitionEpoch() != partitionEpoch) {
            throw new IllegalArgumentException(name + " partition epoch does not match workflow partition");
        }
    }

    private static boolean sameNullable(Object left, Object right) {
        return left == right || (left != null && left.equals(right));
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
