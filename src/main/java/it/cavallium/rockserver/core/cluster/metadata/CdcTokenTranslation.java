package it.cavallium.rockserver.core.cluster.metadata;

import it.cavallium.rockserver.core.cluster.command.ReplicatedCdcToken;

public sealed interface CdcTokenTranslation permits CdcTokenTranslation.Split,
        CdcTokenTranslation.Merge {

    long metadataEpoch();

    TranslationKind kind();

    enum TranslationKind {
        SPLIT,
        MERGE
    }

    record Split(long metadataEpoch,
                 ReplicatedCdcToken sourceEndToken,
                 ReplicatedCdcToken leftStartToken,
                 ReplicatedCdcToken rightStartToken) implements CdcTokenTranslation {

        public Split {
            requireMetadataEpoch(metadataEpoch);
            var requiredSourceEndToken = requireArgument(sourceEndToken, "sourceEndToken");
            var requiredLeftStartToken = requireArgument(leftStartToken, "leftStartToken");
            var requiredRightStartToken = requireArgument(rightStartToken, "rightStartToken");
            if (requiredLeftStartToken.partitionId().equals(requiredRightStartToken.partitionId())) {
                throw new IllegalArgumentException("split CDC token translation children must be distinct");
            }
            if (requiredSourceEndToken.partitionId().equals(requiredLeftStartToken.partitionId())
                    || requiredSourceEndToken.partitionId().equals(requiredRightStartToken.partitionId())) {
                throw new IllegalArgumentException("split CDC token translation child must not reuse source partition");
            }
            validateChildToken(metadataEpoch, requiredSourceEndToken, requiredLeftStartToken, "leftStartToken");
            validateChildToken(metadataEpoch, requiredSourceEndToken, requiredRightStartToken, "rightStartToken");
        }

        @Override
        public TranslationKind kind() {
            return TranslationKind.SPLIT;
        }
    }

    record Merge(long metadataEpoch,
                 ReplicatedCdcToken leftEndToken,
                 ReplicatedCdcToken rightEndToken,
                 ReplicatedCdcToken mergedStartToken) implements CdcTokenTranslation {

        public Merge {
            requireMetadataEpoch(metadataEpoch);
            var requiredLeftEndToken = requireArgument(leftEndToken, "leftEndToken");
            var requiredRightEndToken = requireArgument(rightEndToken, "rightEndToken");
            var requiredMergedStartToken = requireArgument(mergedStartToken, "mergedStartToken");
            if (requiredLeftEndToken.partitionId().equals(requiredRightEndToken.partitionId())) {
                throw new IllegalArgumentException("merge CDC token translation parents must be distinct");
            }
            if (requiredMergedStartToken.partitionId().equals(requiredLeftEndToken.partitionId())
                    || requiredMergedStartToken.partitionId().equals(requiredRightEndToken.partitionId())) {
                throw new IllegalArgumentException("merge CDC token translation target must not reuse parent partition");
            }
            validateChildToken(metadataEpoch, requiredLeftEndToken, requiredMergedStartToken, "mergedStartToken");
            validateChildToken(metadataEpoch, requiredRightEndToken, requiredMergedStartToken, "mergedStartToken");
        }

        @Override
        public TranslationKind kind() {
            return TranslationKind.MERGE;
        }
    }

    private static void validateChildToken(long metadataEpoch,
                                           ReplicatedCdcToken parentToken,
                                           ReplicatedCdcToken childToken,
                                           String childName) {
        if (childToken.metadataEpoch() != metadataEpoch) {
            throw new IllegalArgumentException(childName + " metadata epoch must match translation metadata epoch");
        }
        if (parentToken.metadataEpoch() > metadataEpoch) {
            throw new IllegalArgumentException("parent token metadata epoch must not exceed translation metadata epoch");
        }
        if (childToken.partitionEpoch() <= parentToken.partitionEpoch()) {
            throw new IllegalArgumentException(childName + " partition epoch must be greater than parent epoch");
        }
    }

    private static void requireMetadataEpoch(long metadataEpoch) {
        if (metadataEpoch < 0) {
            throw new IllegalArgumentException("metadataEpoch must be >= 0");
        }
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
