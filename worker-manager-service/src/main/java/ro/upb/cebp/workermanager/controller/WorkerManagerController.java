package ro.upb.cebp.workermanager.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
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

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** Member 2 Worker Manager: batching, registry, watchdog, retries, metrics, and autoscaling. */
@RestController
@RequestMapping("/api/v1")
@Tag(name = "Worker Manager & Dynamic Scaling", description = "Worker registry, heartbeats, queue lag, latency, retries, and scaling")
public class WorkerManagerController {

    private static final Logger log = LoggerFactory.getLogger(WorkerManagerController.class);
    private static final int BATCH_SIZE = 10;
    private static final int MAX_RETRIES = 3;
    private static final Duration HEARTBEAT_TIMEOUT = Duration.ofSeconds(15);
    private static final Duration JOB_TIMEOUT = Duration.ofSeconds(30);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RestTemplate restTemplate = new RestTemplate();
    private final Map<String, WorkerClusterStatusDto.WorkerNodeDto> workers = new ConcurrentHashMap<>();
    private final Map<String, AnalyticalJobMessage> inFlightJobs = new ConcurrentHashMap<>();
    private final Map<String, Instant> inFlightStartedAt = new ConcurrentHashMap<>();
    private final Map<String, Integer> attemptCounts = new ConcurrentHashMap<>();
    private final List<NormalizedTransitEvent> eventBuffer = new CopyOnWriteArrayList<>();
    private final List<FailedJobDto> failedJobs = new CopyOnWriteArrayList<>();
    private final ConcurrentLinkedDeque<Double> latencySamples = new ConcurrentLinkedDeque<>();

    private final AtomicInteger waitingTasks = new AtomicInteger();
    private final AtomicLong completedTasks = new AtomicLong();
    private final AtomicLong failedTasks = new AtomicLong();
    private final AtomicLong completedWindow = new AtomicLong();
    private volatile Instant throughputWindowStarted = Instant.now();
    private volatile String lastScalingDecision = "Initial baseline pool (2 active virtual workers)";
    private volatile boolean autoScalingEnabled = true;

    public WorkerManagerController(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        Instant now = Instant.now();
        workers.put("worker-1", new WorkerClusterStatusDto.WorkerNodeDto("worker-1", WorkerStatus.ACTIVE, null, 14, 0, now));
        workers.put("worker-2", new WorkerClusterStatusDto.WorkerNodeDto("worker-2", WorkerStatus.ACTIVE, null, 12, 0, now));
    }

    @KafkaListener(topics = KafkaTopics.TRANSIT_EVENTS_RAW, groupId = "worker-manager-raw-group")
    public void onRawTransitEvent(NormalizedTransitEvent event) {
        if (event == null) return;
        synchronized (eventBuffer) {
            eventBuffer.add(event);
            if (eventBuffer.size() >= BATCH_SIZE) dispatchBuffer();
        }
    }

    @Scheduled(fixedDelay = 5000)
    public void flushRawEventWindow() {
        synchronized (eventBuffer) {
            if (!eventBuffer.isEmpty()) dispatchBuffer();
        }
    }

    private void dispatchBuffer() {
        List<NormalizedTransitEvent> batch = new ArrayList<>(eventBuffer);
        eventBuffer.clear();
        if (batch.isEmpty()) return;
        Map<String, List<NormalizedTransitEvent>> grouped = new LinkedHashMap<>();
        for (NormalizedTransitEvent event : batch) {
            String source = event.source() != null ? event.source().name() : "UNKNOWN";
            String route = event.routeId() != null ? event.routeId() : "DEFAULT";
            grouped.computeIfAbsent(source + ":" + route, key -> new ArrayList<>()).add(event);
        }
        for (List<NormalizedTransitEvent> routeEvents : grouped.values()) {
            dispatchAnalyticalJob(routeEvents.getFirst().source(), routeEvents.getFirst().routeId(), routeEvents, 1);
        }
    }

    private void dispatchAnalyticalJob(TransitSource source, String routeId, List<NormalizedTransitEvent> events, int attemptCount) {
        AnalyticalJobMessage job = new AnalyticalJobMessage("job-" + UUID.randomUUID().toString().substring(0, 8),
                JobType.COMPREHENSIVE_ROUTE_ANALYSIS, source, routeId,
                events.isEmpty() ? null : events.getFirst().routeName(), attemptCount, Instant.now(), events);
        waitingTasks.incrementAndGet();
        inFlightJobs.put(job.jobId(), job);
        inFlightStartedAt.put(job.jobId(), Instant.now());
        attemptCounts.put(job.jobId(), attemptCount);
        kafkaTemplate.send(KafkaTopics.ANALYTICS_JOBS, job.jobId(), job);
    }

    @KafkaListener(topics = KafkaTopics.WORKER_TELEMETRY, groupId = "worker-manager-telemetry-group")
    public void onWorkerTelemetry(WorkerTelemetryMessage telemetry) {
        if (telemetry == null || telemetry.workerId() == null) return;
        workers.put(telemetry.workerId(), new WorkerClusterStatusDto.WorkerNodeDto(
                telemetry.workerId(), telemetry.workerStatus(), telemetry.currentJobId(),
                telemetry.processedJobsTotal(), telemetry.failedJobsTotal(),
                telemetry.timestamp() != null ? telemetry.timestamp() : Instant.now()));
        if (telemetry.eventType() == TelemetryEventType.JOB_COMPLETED) {
            completeJob(telemetry.currentJobId(), telemetry.durationMs(), telemetry.workerId());
        } else if (telemetry.eventType() == TelemetryEventType.JOB_FAILED) {
            waitingTasks.updateAndGet(v -> Math.max(0, v - 1));
            failedTasks.incrementAndGet();
            handleFailure(telemetry.currentJobId(), telemetry.workerId(), telemetry.errorMessage());
        }
    }

    private void completeJob(String jobId, Long durationMs, String workerId) {
        if (jobId == null) return;
        if (inFlightJobs.remove(jobId) != null) {
            waitingTasks.updateAndGet(v -> Math.max(0, v - 1));
            completedTasks.incrementAndGet();
            completedWindow.incrementAndGet();
        }
        inFlightStartedAt.remove(jobId);
        attemptCounts.remove(jobId);
        if (durationMs != null) {
            latencySamples.addLast(durationMs.doubleValue());
            while (latencySamples.size() > 2000) latencySamples.removeFirst();
        }
    }

    private void handleFailure(String jobId, String workerId, String errorMessage) {
        AnalyticalJobMessage original = inFlightJobs.get(jobId);
        int attempt = attemptCounts.getOrDefault(jobId, 1);
        failedJobs.add(0, new FailedJobDto(jobId != null ? jobId : "job-unknown",
                original != null ? original.jobType() : JobType.COMPREHENSIVE_ROUTE_ANALYSIS,
                original != null ? original.source() : TransitSource.SIMULATOR,
                original != null ? original.routeId() : "UNKNOWN", attempt, workerId,
                errorMessage != null ? errorMessage : "Worker timeout / exception", Instant.now(), false));
        if (original != null && attempt <= MAX_RETRIES) {
            inFlightJobs.remove(jobId); inFlightStartedAt.remove(jobId); attemptCounts.remove(jobId);
            dispatchAnalyticalJob(original.source(), original.routeId(), original.events(), attempt + 1);
            lastScalingDecision = "Retried " + jobId + " (attempt " + (attempt + 1) + ")";
        }
    }

    @Scheduled(fixedDelay = 3000)
    public void detectDeadWorkersAndTimedOutJobs() {
        Instant now = Instant.now();
        for (Map.Entry<String, WorkerClusterStatusDto.WorkerNodeDto> entry : workers.entrySet()) {
            WorkerClusterStatusDto.WorkerNodeDto worker = entry.getValue();
            if (worker.status() != WorkerStatus.DEAD && worker.lastHeartbeatAt() != null
                    && Duration.between(worker.lastHeartbeatAt(), now).compareTo(HEARTBEAT_TIMEOUT) > 0) {
                workers.put(entry.getKey(), new WorkerClusterStatusDto.WorkerNodeDto(worker.workerId(),
                        WorkerStatus.DEAD, worker.currentJobId(), worker.processedJobsCount(),
                        worker.failedJobsCount(), worker.lastHeartbeatAt()));
                log.warn("Worker {} marked DEAD after heartbeat timeout", entry.getKey());
            }
        }
        for (Map.Entry<String, Instant> entry : new HashMap<>(inFlightStartedAt).entrySet()) {
            if (Duration.between(entry.getValue(), now).compareTo(JOB_TIMEOUT) > 0) {
                AnalyticalJobMessage job = inFlightJobs.get(entry.getKey());
                if (job != null) {
                    inFlightStartedAt.remove(entry.getKey());
                    waitingTasks.updateAndGet(v -> Math.max(0, v - 1));
                    failedTasks.incrementAndGet();
                    handleFailure(entry.getKey(), "timeout", "Job exceeded 30-second timeout");
                }
            }
        }
    }

    @Scheduled(fixedDelay = 10000)
    public void evaluateAutoscaling() {
        if (!autoScalingEnabled) return;
        int active = activeWorkers();
        int pending = waitingTasks.get();
        double p95 = percentile(95);
        if ((pending > active * 10 || p95 > 2500) && active < 8) {
            scalePool(Math.min(8, active + 1), "Queue pressure or latency threshold reached");
        } else if (pending == 0 && active > 1) {
            scalePool(Math.max(1, active - 1), "Queue drained; scale down after low utilization");
        }
    }

    private int activeWorkers() {
        return (int) workers.values().stream()
                .filter(w -> w.status() == WorkerStatus.ACTIVE || w.status() == WorkerStatus.BUSY).count();
    }

    private void scalePool(int target, String reason) {
        target = Math.max(1, Math.min(8, target));
        ScaleWorkerPoolRequest request = new ScaleWorkerPoolRequest(target, reason);
        HttpHeaders headers = new HttpHeaders(); headers.setContentType(MediaType.APPLICATION_JSON);
        try {
            restTemplate.postForEntity(System.getenv().getOrDefault("WORKER_POOL_BASE_URL", "http://localhost:8091")
                    + "/api/v1/worker-pool/scale", new HttpEntity<>(request, headers), Map.class);
            lastScalingDecision = "Scaled to " + target + " workers (" + reason + ")";
        } catch (RestClientException exception) {
            lastScalingDecision = "Requested " + target + " workers; pool endpoint unavailable (" + reason + ")";
            log.warn("Unable to reach analysis worker pool: {}", exception.getMessage());
        }
    }

    @GetMapping("/workers/status")
    @Operation(summary = "Get workers, queue depth, throughput, and latency percentiles")
    public WorkerClusterStatusDto getClusterStatus() {
        Duration elapsed = Duration.between(throughputWindowStarted, Instant.now());
        double seconds = Math.max(1.0, elapsed.toMillis() / 1000.0);
        double throughput = completedWindow.get() / seconds;
        if (elapsed.compareTo(Duration.ofSeconds(10)) > 0) {
            completedWindow.set(0); throughputWindowStarted = Instant.now();
        }
        return new WorkerClusterStatusDto(activeWorkers(), deadWorkers(), waitingTasks.get(),
                completedTasks.get(), failedTasks.get(), throughput, percentile(50), percentile(95), percentile(99),
                autoScalingEnabled, lastScalingDecision, Instant.now(), new ArrayList<>(workers.values()));
    }

    private int deadWorkers() {
        return (int) workers.values().stream().filter(w -> w.status() == WorkerStatus.DEAD).count();
    }

    private double percentile(int percentile) {
        if (latencySamples.isEmpty()) return 0.0;
        List<Double> values = new ArrayList<>(latencySamples); Collections.sort(values);
        int index = (int) Math.ceil(percentile / 100.0 * values.size()) - 1;
        return values.get(Math.max(0, Math.min(values.size() - 1, index)));
    }

    @GetMapping("/jobs/failed")
    @Operation(summary = "List failed analytical jobs")
    public List<FailedJobDto> getFailedJobs() { return failedJobs; }

    @PostMapping("/jobs/replay")
    @Operation(summary = "Replay selected failed analytical jobs")
    public Map<String, Object> replayFailedJobs(@RequestBody List<String> jobIds) {
        for (String id : jobIds) {
            FailedJobDto existing = failedJobs.stream().filter(f -> f.jobId().equals(id)).findFirst().orElse(null);
            if (existing != null) {
                dispatchAnalyticalJob(existing.source(), existing.routeId(),
                        SampleTransitFixtures.generateTfNswSydneyBatch(BATCH_SIZE), existing.attemptCount() + 1);
            }
        }
        return Map.of("replayedJobIds", jobIds, "status", "REQUEUED_TO_KAFKA");
    }

    @PostMapping("/scaling/trigger")
    @Operation(summary = "Trigger dynamic worker scaling")
    public Map<String, Object> scaleCluster(@RequestBody ScaleWorkerPoolRequest request) {
        scalePool(request.targetWorkerCount(), request.reason());
        return Map.of("targetWorkerCount", request.targetWorkerCount(), "decision", lastScalingDecision, "timestamp", Instant.now());
    }

    @PostMapping("/workers/register")
    @Operation(summary = "Register or refresh an analysis worker")
    public Map<String, Object> registerWorker(@RequestParam String workerId,
            @RequestParam(defaultValue = "localhost") String host,
            @RequestParam(defaultValue = "8091") int port,
            @RequestParam(defaultValue = "1") int capacity) {
        workers.put(workerId, new WorkerClusterStatusDto.WorkerNodeDto(workerId, WorkerStatus.ACTIVE,
                null, 0, 0, Instant.now()));
        return Map.of("workerId", workerId, "host", host, "port", port,
                "capacity", capacity, "status", WorkerStatus.ACTIVE);
    }

    @PostMapping("/dev/inject-raw-events")
    @Operation(summary = "Inject synthetic TfNSW events for standalone development")
    public Map<String, Object> devInjectEvents(@RequestParam(defaultValue = "20") int count) {
        List<NormalizedTransitEvent> events = SampleTransitFixtures.generateTfNswSydneyBatch(count);
        synchronized (eventBuffer) { eventBuffer.addAll(events); dispatchBuffer(); }
        return Map.of("injectedEvents", count, "waitingJobsNow", waitingTasks.get());
    }
}
