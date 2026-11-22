package ro.upb.cebp.workermanager.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ro.upb.cebp.common.dto.FailedJobDto;
import ro.upb.cebp.common.enums.JobType;
import ro.upb.cebp.common.enums.TransitSource;
import ro.upb.cebp.common.events.AnalyticalJobMessage;
import ro.upb.cebp.common.topics.KafkaTopics;
import ro.upb.cebp.workermanager.autoscaler.WorkloadMetricsTracker;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/** Retries failed jobs and retains exhausted work for operator/MCP inspection and replay. */
@Service
public class RetryAndDlqManager {
    public static final String DLQ_TOPIC = "analytics.jobs.dlq";
    private static final Logger log = LoggerFactory.getLogger(RetryAndDlqManager.class);
    private static final int MAX_ATTEMPTS = 3;

    private final JobDispatcher dispatcher;
    private final WorkloadMetricsTracker metrics;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final List<FailedJobDto> failures = new CopyOnWriteArrayList<>();
    private final Map<String, AnalyticalJobMessage> dlqJobs = new ConcurrentHashMap<>();

    public RetryAndDlqManager(JobDispatcher dispatcher, WorkloadMetricsTracker metrics,
                              KafkaTemplate<String, Object> kafkaTemplate) {
        this.dispatcher = dispatcher;
        this.metrics = metrics;
        this.kafkaTemplate = kafkaTemplate;
    }

    public void handleFailure(String jobId, String workerId, String reason) {
        AnalyticalJobMessage job = dispatcher.removeInFlight(jobId);
        if (job == null) return;
        metrics.jobAttemptFailed();
        int attempt = Math.max(1, job.attemptCount());
        failures.add(0, new FailedJobDto(job.jobId(), job.jobType(), job.source(), job.routeId(), attempt,
                workerId, reason == null ? "Worker failed or timed out" : reason, Instant.now(), false));

        if (attempt < MAX_ATTEMPTS) {
            AnalyticalJobMessage retry = new AnalyticalJobMessage(job.jobId(), job.jobType(), job.source(),
                    job.routeId(), job.routeName(), attempt + 1, Instant.now(), job.events());
            log.warn("Retrying job {} as attempt {} after failure", jobId, attempt + 1);
            dispatcher.markRetry(retry);
        } else {
            dlqJobs.put(job.jobId(), job);
            metrics.jobMovedToDlq(job.jobId());
            kafkaTemplate.send(DLQ_TOPIC, job.jobId(), job);
            log.error("Job {} exhausted {} attempts and was routed to {}", jobId, MAX_ATTEMPTS, DLQ_TOPIC);
        }
    }

    public List<FailedJobDto> listFailures() { return List.copyOf(failures); }

    public List<String> replay(List<String> jobIds) {
        List<String> replayed = new java.util.ArrayList<>();
        for (String id : jobIds) {
            AnalyticalJobMessage job = dlqJobs.remove(id);
            if (job == null) continue;
            AnalyticalJobMessage replay = new AnalyticalJobMessage(job.jobId(), job.jobType(), job.source(),
                    job.routeId(), job.routeName(), 1, Instant.now(), job.events());
            dispatcher.markRetry(replay);
            replayed.add(id);
        }
        return replayed;
    }
}
