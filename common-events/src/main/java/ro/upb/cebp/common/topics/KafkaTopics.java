package ro.upb.cebp.common.topics;

/**
 * Canonical Apache Kafka / Redpanda topic names shared across all CEBP components.
 *
 * Flow:
 * 1. core-management-service -> TRANSIT_EVENTS_RAW -> worker-manager-service
 * 2. worker-manager-service  -> ANALYTICS_JOBS     -> analysis-worker
 * 3. analysis-worker         -> WORKER_TELEMETRY   -> worker-manager-service
 * 4. analysis-worker         -> ANALYTICS_RESULTS  -> analytics-results-service
 */
public final class KafkaTopics {

    private KafkaTopics() {
    }

    /** Raw normalized public transit events from TfNSW, TfL, HSL, MTA, or Simulator. */
    public static final String TRANSIT_EVENTS_RAW = "transit.events.raw";

    /** Batched analytical jobs dispatched by Worker Manager to the Analysis Worker pool. */
    public static final String ANALYTICS_JOBS = "analytics.jobs";

    /** Unified worker telemetry stream: 5-second heartbeats + job STARTED/COMPLETED/FAILED status. */
    public static final String WORKER_TELEMETRY = "worker.telemetry";

    /** Computed route delay, headway bunching, and speed anomaly results. */
    public static final String ANALYTICS_RESULTS = "analytics.results";
}
