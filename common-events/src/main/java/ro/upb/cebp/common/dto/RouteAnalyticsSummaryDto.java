package ro.upb.cebp.common.dto;

import ro.upb.cebp.common.enums.TransitSource;
import ro.upb.cebp.common.enums.TransportMode;

import java.time.Instant;

/**
 * Aggregated route performance projection exposed by {@code analytics-results-service}
 * at {@code GET /api/v1/analytics/routes}.
 */
public record RouteAnalyticsSummaryDto(
        TransitSource source,
        TransportMode transportMode,
        String routeId,
        String routeName,
        long analyzedEventsCount,
        double averageDelaySeconds,
        double p95DelaySeconds,
        double maxDelaySeconds,
        double onTimePercentage,
        int bunchedVehiclesCount,
        int speedAnomaliesCount,
        double averageSpeedMps,
        String healthStatus,
        Instant lastUpdatedAt
) {
}
