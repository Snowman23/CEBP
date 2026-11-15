package ro.upb.cebp.common.dto;

import ro.upb.cebp.common.enums.JobType;
import ro.upb.cebp.common.enums.TransitSource;

import java.time.Instant;

/**
 * Represents a failed or Dead-Letter Queue (DLQ) analytical job tracked by {@code worker-manager-service}
 * at {@code GET /api/v1/jobs/failed}.
 */
public record FailedJobDto(
        String jobId,
        JobType jobType,
        TransitSource source,
        String routeId,
        int attemptCount,
        String lastWorkerId,
        String errorMessage,
        Instant failedAt,
        boolean replayed
) {
}
