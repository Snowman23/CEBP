package ro.upb.cebp.workermanager.scheduler;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ro.upb.cebp.common.enums.JobType;
import ro.upb.cebp.common.enums.TransitSource;
import ro.upb.cebp.common.events.AnalyticalJobMessage;
import ro.upb.cebp.common.events.NormalizedTransitEvent;
import ro.upb.cebp.common.topics.KafkaTopics;
import ro.upb.cebp.workermanager.autoscaler.WorkloadMetricsTracker;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Publishes jobs and keeps the transient in-flight job projection needed for retries/timeouts. */
@Service
public class JobDispatcher {
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final WorkloadMetricsTracker metrics;
    private final Map<String, AnalyticalJobMessage> inFlight = new ConcurrentHashMap<>();
    private final Map<String, Instant> dispatchedAt = new ConcurrentHashMap<>();

    public JobDispatcher(KafkaTemplate<String, Object> kafkaTemplate, WorkloadMetricsTracker metrics) {
        this.kafkaTemplate = kafkaTemplate;
        this.metrics = metrics;
    }

    public AnalyticalJobMessage createAndDispatch(TransitSource source, String routeId, List<NormalizedTransitEvent> events) {
        String routeName = events == null || events.isEmpty() ? null : events.getFirst().routeName();
        AnalyticalJobMessage job = new AnalyticalJobMessage("job-" + java.util.UUID.randomUUID(),
                JobType.COMPREHENSIVE_ROUTE_ANALYSIS, source, routeId, routeName, 1, Instant.now(),
                events == null ? List.of() : List.copyOf(events));
        dispatch(job);
        return job;
    }

    public void dispatch(AnalyticalJobMessage job) {
        inFlight.put(job.jobId(), job);
        dispatchedAt.put(job.jobId(), Instant.now());
        metrics.jobDispatched(job.jobId());
        kafkaTemplate.send(KafkaTopics.ANALYTICS_JOBS, job.jobId(), job);
    }

    /** Atomically removes an active job so duplicate terminal events cannot complete/retry it twice. */
    public AnalyticalJobMessage removeInFlight(String jobId) {
        if (jobId == null) return null;
        AnalyticalJobMessage removed = inFlight.remove(jobId);
        if (removed != null) dispatchedAt.remove(jobId);
        return removed;
    }

    public void completed(String jobId, Long durationMs) {
        AnalyticalJobMessage job = removeInFlight(jobId);
        if (job != null) metrics.jobCompleted(jobId, durationMs);
    }

    public AnalyticalJobMessage getInFlight(String jobId) { return inFlight.get(jobId); }

    public Map<String, AnalyticalJobMessage> inFlightJobs() { return Map.copyOf(inFlight); }

    public Map<String, Instant> timedOutJobs(Instant now, java.time.Duration timeout) {
        Map<String, Instant> expired = new java.util.HashMap<>();
        dispatchedAt.forEach((id, started) -> {
            if (java.time.Duration.between(started, now).compareTo(timeout) > 0) expired.put(id, started);
        });
        return expired;
    }

    public void markRetry(AnalyticalJobMessage job) { dispatch(job); }
}
