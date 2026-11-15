package ro.upb.cebp.common.dto;

import java.time.Instant;
import java.util.Map;

/**
 * Global KPI overview returned by {@code analytics-results-service} at {@code GET /api/v1/analytics/overview}.
 */
public record TransitOverviewDto(
        long totalBatchesAnalyzed,
        long totalEventsAnalyzed,
        int activeRoutesTracked,
        double globalAverageDelaySeconds,
        double globalOnTimePercentage,
        int activeHighSeverityAnomalies,
        Map<String, Double> averageDelayBySource,
        Instant updatedAt
) {
}
