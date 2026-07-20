package it.cavallium.rockserver.core.cluster.command;

import it.cavallium.rockserver.core.cluster.ShardId;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class ClusterCdcStreamMerger {

    public static final int MAX_EVENTS_PER_PAGE = 1_000_000;

    public ClusterCdcPage mergePage(Map<ShardId, List<ReplicatedCdcEvent>> partitionEvents,
                                    int maxEvents) {
        var requiredPartitionEvents = requireArgument(partitionEvents, "partitionEvents");
        if (maxEvents <= 0) {
            throw new IllegalArgumentException("maxEvents must be positive");
        }
        if (maxEvents > MAX_EVENTS_PER_PAGE) {
            throw new IllegalArgumentException("maxEvents must be <= " + MAX_EVENTS_PER_PAGE + ": " + maxEvents);
        }
        var orderedPartitionIds = requiredPartitionEvents.keySet().stream()
                .map(partitionId -> requireArgument(partitionId, "partitionId"))
                .sorted()
                .toList();
        var eventsByPartition = new HashMap<ShardId, List<ReplicatedCdcEvent>>();
        for (ShardId partitionId : orderedPartitionIds) {
            var events = requireArgument(requiredPartitionEvents.get(partitionId), "events");
            validatePartitionEvents(partitionId, events);
            eventsByPartition.put(partitionId, List.copyOf(events));
        }

        var offsets = new HashMap<ShardId, Integer>();
        var merged = new ArrayList<ReplicatedCdcEvent>(maxEvents);
        while (merged.size() < maxEvents) {
            boolean produced = false;
            for (ShardId partitionId : orderedPartitionIds) {
                var events = eventsByPartition.get(partitionId);
                int offset = offsets.getOrDefault(partitionId, 0);
                if (offset >= events.size()) {
                    continue;
                }
                merged.add(events.get(offset));
                offsets.put(partitionId, offset + 1);
                produced = true;
                if (merged.size() == maxEvents) {
                    break;
                }
            }
            if (!produced) {
                break;
            }
        }

        var nextTokens = new HashMap<ShardId, ReplicatedCdcToken>();
        for (ShardId partitionId : orderedPartitionIds) {
            int emitted = offsets.getOrDefault(partitionId, 0);
            if (emitted > 0) {
                nextTokens.put(partitionId, eventsByPartition.get(partitionId).get(emitted - 1).token());
            }
        }
        return new ClusterCdcPage(merged, nextTokens);
    }

    private static void validatePartitionEvents(ShardId partitionId, List<ReplicatedCdcEvent> events) {
        ReplicatedCdcToken previous = null;
        for (ReplicatedCdcEvent event : events) {
            var requiredEvent = requireArgument(event, "event");
            var token = requiredEvent.token();
            if (!token.partitionId().equals(partitionId)) {
                throw new IllegalArgumentException("CDC event partition " + token.partitionId()
                        + " does not match stream partition " + partitionId);
            }
            if (previous != null && comparePartitionPosition(previous, token) >= 0) {
                throw new IllegalArgumentException("CDC events for partition " + partitionId
                        + " are not strictly ordered");
            }
            previous = token;
        }
    }

    private static int comparePartitionPosition(ReplicatedCdcToken left, ReplicatedCdcToken right) {
        int indexCompare = Long.compare(left.appliedIndex(), right.appliedIndex());
        if (indexCompare != 0) {
            return indexCompare;
        }
        return Integer.compare(left.eventOrdinal(), right.eventOrdinal());
    }

    public record ClusterCdcPage(List<ReplicatedCdcEvent> events,
                                 Map<ShardId, ReplicatedCdcToken> nextPartitionTokens) {

        public ClusterCdcPage {
            events = copyEvents(events);
            nextPartitionTokens = copyNextPartitionTokens(nextPartitionTokens);
        }

        public List<ShardId> partitionsWithProgress() {
            return nextPartitionTokens.keySet().stream()
                    .sorted(Comparator.naturalOrder())
                    .toList();
        }
    }

    private static List<ReplicatedCdcEvent> copyEvents(List<ReplicatedCdcEvent> events) {
        var requiredEvents = requireArgument(events, "events");
        var copiedEvents = new ArrayList<ReplicatedCdcEvent>(requiredEvents.size());
        for (int i = 0; i < requiredEvents.size(); i++) {
            copiedEvents.add(requireArgument(requiredEvents.get(i), "events[" + i + "]"));
        }
        return List.copyOf(copiedEvents);
    }

    private static Map<ShardId, ReplicatedCdcToken> copyNextPartitionTokens(
            Map<ShardId, ReplicatedCdcToken> nextPartitionTokens) {
        var requiredNextPartitionTokens = requireArgument(nextPartitionTokens, "nextPartitionTokens");
        var copiedTokens = new HashMap<ShardId, ReplicatedCdcToken>(requiredNextPartitionTokens.size());
        for (var entry : requiredNextPartitionTokens.entrySet()) {
            var partitionId = requireArgument(entry.getKey(), "partitionId");
            var token = requireArgument(entry.getValue(), "nextPartitionToken");
            copiedTokens.put(partitionId, token);
        }
        return Map.copyOf(copiedTokens);
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
