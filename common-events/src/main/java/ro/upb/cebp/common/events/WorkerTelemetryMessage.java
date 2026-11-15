package ro.upb.cebp.common.events;

import ro.upb.cebp.common.enums.TelemetryEventType;
import ro.upb.cebp.common.enums.WorkerStatus;

import java.time.Instant;

/**
 * Unified telemetry event emitted by {@code analysis-worker} instances onto Kafka topic {@code worker.telemetry}
 * and consumed by {@code worker-manager-service}.
 *
 * Used for both periodic 5-second worker heartbeats AND job lifecycle transitions (STARTED, COMPLETED, FAILED).
 */
public record WorkerTelemetryMessage(
        String workerId,
        TelemetryEventType eventType,
        WorkerStatus workerStatus,
        String currentJobId,
        Long durationMs,
        long processedJobsTotal,
        long failedJobsTotal,
        String errorMessage,
        Instant timestamp
) {
}
