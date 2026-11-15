package ro.upb.cebp.common.dto;

import ro.upb.cebp.common.enums.WorkerStatus;

import java.time.Instant;
import java.util.List;

/**
 * Cluster health, queue lag, latency percentiles, and worker registry state exposed by {@code worker-manager-service}
 * at {@code GET /api/v1/workers/status}.
 */
public record WorkerClusterStatusDto(
        int activeWorkersCount,
        int deadWorkersCount,
        int waitingTasksCount,
        long completedTasksTotal,
        long failedTasksTotal,
        double throughputTasksPerSec,
        double p50ProcessingTimeMs,
        double p95ProcessingTimeMs,
        double p99ProcessingTimeMs,
        boolean autoScalingEnabled,
        String lastScalingDecision,
        Instant updatedAt,
        List<WorkerNodeDto> workers
) {
    public record WorkerNodeDto(
            String workerId,
            WorkerStatus status,
            String currentJobId,
            long processedJobsCount,
            long failedJobsCount,
            Instant lastHeartbeatAt
    ) {
    }
}
