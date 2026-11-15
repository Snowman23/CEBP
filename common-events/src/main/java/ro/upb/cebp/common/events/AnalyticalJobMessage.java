package ro.upb.cebp.common.events;

import ro.upb.cebp.common.enums.JobType;
import ro.upb.cebp.common.enums.TransitSource;

import java.time.Instant;
import java.util.List;

/**
 * Analytical job dispatched by {@code worker-manager-service} onto Kafka topic {@code analytics.jobs}
 * and consumed by available {@code analysis-worker} instances.
 */
public record AnalyticalJobMessage(
        String jobId,
        JobType jobType,
        TransitSource source,
        String routeId,
        String routeName,
        int attemptCount,
        Instant createdAt,
        List<NormalizedTransitEvent> events
) {
}
