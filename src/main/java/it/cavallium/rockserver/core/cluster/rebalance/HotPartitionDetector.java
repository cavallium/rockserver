package it.cavallium.rockserver.core.cluster.rebalance;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

public final class HotPartitionDetector {

    public List<HotPartition> detect(Collection<PartitionLoadReport> reports,
                                     HotPartitionThresholds thresholds,
                                     long nowMillis) {
        var requiredReports = requireArgument(reports, "reports");
        var requiredThresholds = requireArgument(thresholds, "thresholds");
        if (nowMillis < 0) {
            throw new IllegalArgumentException("nowMillis must be >= 0");
        }
        var hotPartitions = new ArrayList<HotPartition>();
        for (PartitionLoadReport report : requiredReports) {
            var requiredReport = requireArgument(report, "reports entry");
            if (!requiredReport.isFresh(nowMillis, requiredThresholds.maxReportAgeMillis())) {
                continue;
            }
            var reasons = reasons(requiredReport, requiredThresholds);
            if (!reasons.isEmpty()) {
                hotPartitions.add(new HotPartition(requiredReport, reasons));
            }
        }
        hotPartitions.sort(Comparator.comparing(hotPartition -> hotPartition.report().partitionId()));
        return List.copyOf(hotPartitions);
    }

    private static List<HotPartitionReason> reasons(PartitionLoadReport report,
                                                    HotPartitionThresholds thresholds) {
        var reasons = new ArrayList<HotPartitionReason>();
        if (report.totalOpsPerSecond() >= thresholds.minTotalOpsPerSecond()) {
            reasons.add(HotPartitionReason.TOTAL_OPS);
        }
        if (report.writeOpsPerSecond() >= thresholds.minWriteOpsPerSecond()) {
            reasons.add(HotPartitionReason.WRITE_OPS);
        }
        if (report.bytesPerSecond() >= thresholds.minBytesPerSecond()) {
            reasons.add(HotPartitionReason.BYTES);
        }
        if (report.averageLatencyMillis() >= thresholds.maxAverageLatencyMillis()) {
            reasons.add(HotPartitionReason.LATENCY);
        }
        if (report.pendingRequests() >= thresholds.maxPendingRequests()) {
            reasons.add(HotPartitionReason.QUEUE_DEPTH);
        }
        return reasons;
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
