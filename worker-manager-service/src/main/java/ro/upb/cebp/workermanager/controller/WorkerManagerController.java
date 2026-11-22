package ro.upb.cebp.workermanager.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import ro.upb.cebp.common.dto.FailedJobDto;
import ro.upb.cebp.common.dto.ScaleWorkerPoolRequest;
import ro.upb.cebp.common.dto.WorkerClusterStatusDto;
import ro.upb.cebp.common.enums.TelemetryEventType;
import ro.upb.cebp.common.events.WorkerTelemetryMessage;
import ro.upb.cebp.common.fixtures.SampleTransitFixtures;
import ro.upb.cebp.common.events.NormalizedTransitEvent;
import ro.upb.cebp.common.topics.KafkaTopics;
import ro.upb.cebp.workermanager.autoscaler.DynamicWorkerScaler;
import ro.upb.cebp.workermanager.autoscaler.WorkloadMetricsTracker;
import ro.upb.cebp.workermanager.registry.WorkerRegistryService;
import ro.upb.cebp.workermanager.scheduler.JobDispatcher;
import ro.upb.cebp.workermanager.scheduler.RawEventWindowBatcher;
import ro.upb.cebp.workermanager.scheduler.RetryAndDlqManager;

import java.util.List;
import java.util.Map;

/** HTTP façade for Worker Manager APIs; orchestration lives in focused services. */
@RestController
@RequestMapping("/api/v1")
@Tag(name = "Worker Manager & Dynamic Scaling", description = "Worker registry, queue metrics, retries and scaling")
public class WorkerManagerController {
    private final WorkerRegistryService registry;
    private final WorkloadMetricsTracker metrics;
    private final JobDispatcher dispatcher;
    private final RetryAndDlqManager retryManager;
    private final DynamicWorkerScaler scaler;
    private final RawEventWindowBatcher batcher;

    public WorkerManagerController(WorkerRegistryService registry, WorkloadMetricsTracker metrics,
                                   JobDispatcher dispatcher, RetryAndDlqManager retryManager,
                                   DynamicWorkerScaler scaler, RawEventWindowBatcher batcher) {
        this.registry = registry;
        this.metrics = metrics;
        this.dispatcher = dispatcher;
        this.retryManager = retryManager;
        this.scaler = scaler;
        this.batcher = batcher;
    }

    @KafkaListener(topics = KafkaTopics.WORKER_TELEMETRY, groupId = "worker-manager-telemetry-group")
    public void onWorkerTelemetry(WorkerTelemetryMessage telemetry) {
        if (telemetry == null) return;
        registry.accept(telemetry);
        if (telemetry.eventType() == TelemetryEventType.JOB_STARTED) {
            registry.markJobStarted(telemetry.workerId(), telemetry.currentJobId());
        } else if (telemetry.eventType() == TelemetryEventType.JOB_COMPLETED) {
            dispatcher.completed(telemetry.currentJobId(), telemetry.durationMs());
        } else if (telemetry.eventType() == TelemetryEventType.JOB_FAILED) {
            retryManager.handleFailure(telemetry.currentJobId(), telemetry.workerId(), telemetry.errorMessage());
        }
    }

    @GetMapping("/workers/status")
    @Operation(summary = "Get worker states, pending jobs, throughput and latency percentiles")
    public WorkerClusterStatusDto getClusterStatus() {
        return new WorkerClusterStatusDto(registry.activeCount(), registry.deadCount(), metrics.pendingJobs(),
                metrics.completedTotal(), metrics.failedTotal(), metrics.throughputJobsPerSecond(),
                metrics.percentile(50), metrics.percentile(95), metrics.percentile(99), scaler.isEnabled(),
                scaler.lastDecision(), java.time.Instant.now(), registry.list());
    }

    @GetMapping("/jobs/failed")
    @Operation(summary = "List failed analytical attempts and exhausted jobs")
    public List<FailedJobDto> getFailedJobs() { return retryManager.listFailures(); }

    @PostMapping("/jobs/replay")
    @Operation(summary = "Replay selected jobs currently retained in the dead-letter queue")
    public Map<String, Object> replayFailedJobs(@RequestBody List<String> jobIds) {
        List<String> replayed = retryManager.replay(jobIds);
        return Map.of("replayedJobIds", replayed, "status", "REQUEUED_TO_KAFKA");
    }

    @PostMapping("/scaling/trigger")
    @Operation(summary = "Request analysis worker pool scaling")
    public Map<String, Object> scaleCluster(@RequestBody ScaleWorkerPoolRequest request) {
        return scaler.scaleTo(request.targetWorkerCount(), request.reason());
    }

    @PostMapping("/workers/register")
    @Operation(summary = "Register or refresh an analysis worker")
    public Map<String, Object> registerWorker(@RequestParam String workerId,
            @RequestParam(defaultValue = "localhost") String host,
            @RequestParam(defaultValue = "8091") int port,
            @RequestParam(defaultValue = "1") int capacity) {
        registry.register(workerId);
        return Map.of("workerId", workerId, "host", host, "port", port, "capacity", capacity, "status", "ACTIVE");
    }

    @PostMapping("/dev/inject-raw-events")
    @Operation(summary = "Inject sample transit events through the production batcher")
    public Map<String, Object> devInjectEvents(@RequestParam(defaultValue = "20") int count) {
        List<NormalizedTransitEvent> events = SampleTransitFixtures.generateTfNswSydneyBatch(Math.max(0, Math.min(count, 10000)));
        events.forEach(batcher::accept);
        return Map.of("injectedEvents", events.size(), "waitingJobsNow", metrics.pendingJobs());
    }
}
