package ro.upb.cebp.common.dto;

import ro.upb.cebp.common.enums.AnomalySeverity;
import ro.upb.cebp.common.enums.TransitSource;

import java.time.Instant;

/**
 * Transit anomaly exposed by {@code analytics-results-service} at {@code GET /api/v1/analytics/anomalies}.
 */
public record TransitAnomalyDto(
        String anomalyId,
        TransitSource source,
        String routeId,
        String routeName,
        String anomalyType,
        AnomalySeverity severity,
        String vehicleId,
        String stopName,
        String description,
        double metricValue,
        Instant detectedAt
) {
}
