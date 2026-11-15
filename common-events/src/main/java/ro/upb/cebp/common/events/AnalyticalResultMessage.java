package ro.upb.cebp.common.events;

import ro.upb.cebp.common.enums.AnomalySeverity;
import ro.upb.cebp.common.enums.JobType;
import ro.upb.cebp.common.enums.TransitSource;
import ro.upb.cebp.common.enums.TransportMode;

import java.time.Instant;
import java.util.List;

/**
 * Analytical result produced by an {@code analysis-worker} onto Kafka topic {@code analytics.results}
 * and consumed by {@code analytics-results-service}.
 */
public record AnalyticalResultMessage(
        String resultId,
        String jobId,
        String processedByWorkerId,
        JobType jobType,
        TransitSource source,
        TransportMode transportMode,
        String routeId,
        String routeName,
        int sampleSize,
        double averageDelaySeconds,
        double p95DelaySeconds,
        double maxDelaySeconds,
        double onTimePercentage,
        int bunchedVehiclePairsCount,
        int stalledOrAnomalyVehiclesCount,
        double averageSpeedMps,
        long processingDurationMs,
        Instant analyzedAt,
        List<DetectedAnomaly> anomalies
) {
    public record DetectedAnomaly(
            String anomalyId,
            String anomalyType,
            AnomalySeverity severity,
            String vehicleId,
            String stopName,
            String description,
            double metricValue,
            Instant detectedAt
    ) {
    }
}
