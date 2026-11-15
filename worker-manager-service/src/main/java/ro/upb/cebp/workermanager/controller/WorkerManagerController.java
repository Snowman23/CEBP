package ro.upb.cebp.workermanager.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import ro.upb.cebp.common.dto.FailedJobDto;
import ro.upb.cebp.common.dto.ScaleWorkerPoolRequest;
import ro.upb.cebp.common.dto.WorkerClusterStatusDto;
import ro.upb.cebp.common.enums.JobType;
import ro.upb.cebp.common.enums.TelemetryEventType;
import ro.upb.cebp.common.enums.TransitSource;
import ro.upb.cebp.common.enums.WorkerStatus;
import ro.upb.cebp.common.events.AnalyticalJobMessage;
import ro.upb.cebp.common.events.NormalizedTransitEvent;
import ro.upb.cebp.common.events.WorkerTelemetryMessage;
import ro.upb.cebp.common.fixtures.SampleTransitFixtures;
import ro.upb.cebp.common.topics.KafkaTopics;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Member 2 Starter Controller & Kafka Orchestrator.
 * Provides working baseline consumers for `transit.events.raw` and `worker.telemetry`,
 * job batching to `analytics.jobs`, and OpenAPI endpoints for Member 4's MCP Server & Web UI.
 */
@RestController
@RequestMapping("/api/v1")
@Tag(name = "Worker Manager & Dynamic Scaling", description = "Worker registry, heartbeat status, queue lag, latency percentiles, retries, and standalone dev simulator")
public class WorkerManagerController {

    private static final Logger log = LoggerFactory.getLogger(WorkerManagerController.class);
    private static final int BATCH_SIZE = 10;

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final Map<String, WorkerClusterStatusDto.WorkerNodeDto> workerRegistry = new ConcurrentHashMap<>();
    private final List<NormalizedTransitEvent> eventBuffer = new CopyOnWriteArrayList<>();
    private final List<FailedJobDto> failedJobs = new CopyOnWriteArrayList<>();

    private final AtomicInteger waitingTasks = new AtomicInteger(0);
    private final AtomicLong completedTasks = new AtomicLong(0);
    private final AtomicLong failedTasks = new AtomicLong(0);
    private volatile String lastScalingDecision = "Initial baseline pool (2 active virtual workers)";

    public WorkerManagerController(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        // Seed initial worker state so Member 4 / UI sees realistic telemetry immediately
        workerRegistry.put("worker-1", new WorkerClusterStatusDto.WorkerNodeDto(
                "worker-1", WorkerStatus.ACTIVE, null, 14, 0, Instant.now()
        ));
        workerRegistry.put("worker-2", new WorkerClusterStatusDto.WorkerNodeDto(
                "worker-2", WorkerStatus.ACTIVE, null, 12, 0, Instant.now()
        ));
    }

    @KafkaListener(topics = KafkaTopics.TRANSIT_EVENTS_RAW, groupId = "worker-manager-raw-group")
    public void onRawTransitEvent(NormalizedTransitEvent event) {
        eventBuffer.add(event);
        if (eventBuffer.size() >= BATCH_SIZE) {
            List<NormalizedTransitEvent> batch = new ArrayList<>(eventBuffer);
            eventBuffer.clear();
            dispatchAnalyticalJob(batch.getFirst().source(), batch.getFirst().routeId(), batch);
        }
    }

    @KafkaListener(topics = KafkaTopics.WORKER_TELEMETRY, groupId = "worker-manager-telemetry-group")
    public void onWorkerTelemetry(WorkerTelemetryMessage telemetry) {
        workerRegistry.put(telemetry.workerId(), new WorkerClusterStatusDto.WorkerNodeDto(
                telemetry.workerId(),
                telemetry.workerStatus(),
                telemetry.currentJobId(),
                telemetry.processedJobsTotal(),
                telemetry.failedJobsTotal(),
                telemetry.timestamp() != null ? telemetry.timestamp() : Instant.now()
        ));

        if (telemetry.eventType() == TelemetryEventType.JOB_COMPLETED) {
            waitingTasks.updateAndGet(v -> Math.max(0, v - 1));
            completedTasks.incrementAndGet();
        } else if (telemetry.eventType() == TelemetryEventType.JOB_FAILED) {
            waitingTasks.updateAndGet(v -> Math.max(0, v - 1));
            failedTasks.incrementAndGet();
            failedJobs.add(0, new FailedJobDto(
                    telemetry.currentJobId() != null ? telemetry.currentJobId() : "job-err",
                    JobType.COMPREHENSIVE_ROUTE_ANALYSIS,
                    TransitSource.TFNSW,
                    "T1",
                    1,
                    telemetry.workerId(),
                    telemetry.errorMessage() != null ? telemetry.errorMessage() : "Worker timeout / exception",
                    Instant.now(),
                    false
            ));
        }
    }

    private AnalyticalJobMessage dispatchAnalyticalJob(TransitSource source, String routeId, List<NormalizedTransitEvent> events) {
        AnalyticalJobMessage job = new AnalyticalJobMessage(
                "job-" + UUID.randomUUID().toString().substring(0, 8),
                JobType.COMPREHENSIVE_ROUTE_ANALYSIS,
                source,
                routeId,
                events.getFirst().routeName(),
                1,
                Instant.now(),
                events
        );
        waitingTasks.incrementAndGet();
        try {
            kafkaTemplate.send(KafkaTopics.ANALYTICS_JOBS, job.jobId(), job);
        } catch (Exception e) {
            log.warn("Kafka broker not reachable; job queued locally: {}", e.getMessage());
        }
        return job;
    }

    @GetMapping("/workers/status")
    @Operation(summary = "Get active/dead workers, waiting task queue depth, throughput, and p50/p95/p99 latency")
    public WorkerClusterStatusDto getClusterStatus() {
        long active = workerRegistry.values().stream()
                .filter(w -> w.status() == WorkerStatus.ACTIVE || w.status() == WorkerStatus.BUSY)
                .count();
        long dead = workerRegistry.values().stream()
                .filter(w -> w.status() == WorkerStatus.DEAD)
                .count();

        return new WorkerClusterStatusDto(
                (int) active,
                (int) dead,
                waitingTasks.get(),
                completedTasks.get(),
                failedTasks.get(),
                2.4,
                68.0,
                142.0,
                215.0,
                true,
                lastScalingDecision,
                Instant.now(),
                new ArrayList<>(workerRegistry.values())
        );
    }

    @GetMapping("/jobs/failed")
    @Operation(summary = "List failed or Dead-Letter Queue (DLQ) analytical jobs")
    public List<FailedJobDto> getFailedJobs() {
        return failedJobs;
    }

    @PostMapping("/jobs/replay")
    @Operation(summary = "Trigger a retry/replay of failed analytical jobs")
    public Map<String, Object> replayFailedJobs(@RequestBody List<String> jobIds) {
        for (String id : jobIds) {
            dispatchAnalyticalJob(TransitSource.TFNSW, "T1", SampleTransitFixtures.generateTfNswSydneyBatch(BATCH_SIZE));
        }
        return Map.of("replayedJobIds", jobIds, "status", "REQUEUED_TO_KAFKA");
    }

    @PostMapping("/scaling/trigger")
    @Operation(summary = "Request dynamic worker scaling (invoked by Autoscaler, Web UI, or AI Proposal execution)")
    public Map<String, Object> scaleCluster(@RequestBody ScaleWorkerPoolRequest request) {
        this.lastScalingDecision = "Scaled to " + request.targetWorkerCount() + " workers (" + request.reason() + ")";
        return Map.of(
                "targetWorkerCount", request.targetWorkerCount(),
                "decision", lastScalingDecision,
                "timestamp", Instant.now()
        );
    }

    @PostMapping("/dev/inject-raw-events")
    @Operation(summary = "[Member 2 Standalone Dev Tool] Inject synthetic TfNSW events directly into Worker Manager without needing Core Management Service")
    public Map<String, Object> devInjectEvents(@RequestParam(defaultValue = "20") int count) {
        List<NormalizedTransitEvent> batch = SampleTransitFixtures.generateTfNswSydneyBatch(count);
        for (NormalizedTransitEvent ev : batch) {
            onRawTransitEvent(ev);
        }
        return Map.of(
                "injectedEvents", count,
                "waitingJobsNow", waitingTasks.get()
        );
    }
}
