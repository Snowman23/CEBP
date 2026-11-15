package ro.upb.cebp.worker.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.*;
import ro.upb.cebp.common.dto.ScaleWorkerPoolRequest;
import ro.upb.cebp.common.enums.*;
import ro.upb.cebp.common.events.AnalyticalJobMessage;
import ro.upb.cebp.common.events.AnalyticalResultMessage;
import ro.upb.cebp.common.events.NormalizedTransitEvent;
import ro.upb.cebp.common.events.WorkerTelemetryMessage;
import ro.upb.cebp.common.fixtures.SampleTransitFixtures;
import ro.upb.cebp.common.topics.KafkaTopics;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Member 3 Starter Controller & Dynamic Virtual Worker Pool Supervisor.
 * Consumes `analytics.jobs`, computes initial delay/bunching/speed metrics, emits `analytics.results`,
 * and broadcasts 5-second heartbeats to `worker.telemetry`.
 */
@RestController
@RequestMapping("/api/v1")
@Tag(name = "Analysis Worker Pool Control", description = "Scale virtual worker instances, simulate worker crashes, and inject standalone test jobs")
public class AnalysisWorkerController {

    private static final Logger log = LoggerFactory.getLogger(AnalysisWorkerController.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final Map<String, WorkerStatus> virtualWorkers = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> processedPerWorker = new ConcurrentHashMap<>();

    public AnalysisWorkerController(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        virtualWorkers.put("worker-1", WorkerStatus.ACTIVE);
        virtualWorkers.put("worker-2", WorkerStatus.ACTIVE);
        processedPerWorker.put("worker-1", new AtomicLong(0));
        processedPerWorker.put("worker-2", new AtomicLong(0));
    }

    @Scheduled(fixedRate = 5000)
    public void emitHeartbeats() {
        Instant now = Instant.now();
        for (Map.Entry<String, WorkerStatus> entry : virtualWorkers.entrySet()) {
            if (entry.getValue() == WorkerStatus.DEAD) {
                continue; // Simulated crashed worker stops sending heartbeats!
            }
            WorkerTelemetryMessage hb = new WorkerTelemetryMessage(
                    entry.getKey(),
                    TelemetryEventType.HEARTBEAT,
                    entry.getValue(),
                    null,
                    null,
                    processedPerWorker.computeIfAbsent(entry.getKey(), k -> new AtomicLong()).get(),
                    0L,
                    null,
                    now
            );
            try {
                kafkaTemplate.send(KafkaTopics.WORKER_TELEMETRY, entry.getKey(), hb);
            } catch (Exception ignored) {
            }
        }
    }

    @KafkaListener(topics = KafkaTopics.ANALYTICS_JOBS, groupId = "analysis-workers-group")
    public void onAnalyticalJob(AnalyticalJobMessage job) {
        processJob(job);
    }

    public AnalyticalResultMessage processJob(AnalyticalJobMessage job) {
        long startNs = System.nanoTime();
        List<String> activeWorkerIds = virtualWorkers.entrySet().stream()
                .filter(e -> e.getValue() == WorkerStatus.ACTIVE)
                .map(Map.Entry::getKey)
                .toList();
        String assignedWorker = activeWorkerIds.isEmpty() ? "worker-1" : activeWorkerIds.get(Math.abs(job.jobId().hashCode()) % activeWorkerIds.size());

        List<NormalizedTransitEvent> events = (job.events() != null && !job.events().isEmpty())
                ? job.events()
                : SampleTransitFixtures.generateTfNswSydneyBatch(10);

        double avgDelay = events.stream()
                .mapToInt(e -> e.delaySeconds() != null ? e.delaySeconds() : 0)
                .average()
                .orElse(0.0);
        double maxDelay = events.stream()
                .mapToInt(e -> e.delaySeconds() != null ? e.delaySeconds() : 0)
                .max()
                .orElse(0);
        long onTimeCount = events.stream()
                .filter(e -> (e.delaySeconds() != null ? e.delaySeconds() : 0) <= 180)
                .count();
        double onTimePct = (onTimeCount * 100.0) / events.size();
        double avgSpeed = events.stream()
                .mapToDouble(e -> e.speedMps() != null ? e.speedMps() : 0.0)
                .average()
                .orElse(0.0);

        List<AnalyticalResultMessage.DetectedAnomaly> anomalies = new ArrayList<>();
        if (maxDelay > 300) {
            NormalizedTransitEvent worst = events.getFirst();
            anomalies.add(new AnalyticalResultMessage.DetectedAnomaly(
                    "anom-" + UUID.randomUUID().toString().substring(0, 8),
                    "SEVERE_DELAY",
                    maxDelay > 480 ? AnomalySeverity.HIGH : AnomalySeverity.MEDIUM,
                    worst.vehicleId(),
                    worst.stopName(),
                    "Route " + job.routeId() + " experienced " + (int) maxDelay + "s delay near " + worst.stopName(),
                    maxDelay,
                    Instant.now()
            ));
        }

        long durationMs = Math.max(12L, (System.nanoTime() - startNs) / 1_000_000L);
        processedPerWorker.computeIfAbsent(assignedWorker, k -> new AtomicLong()).incrementAndGet();

        AnalyticalResultMessage result = new AnalyticalResultMessage(
                "res-" + UUID.randomUUID().toString().substring(0, 8),
                job.jobId(),
                assignedWorker,
                job.jobType(),
                job.source(),
                events.getFirst().transportMode(),
                job.routeId(),
                job.routeName(),
                events.size(),
                Math.round(avgDelay * 10.0) / 10.0,
                Math.round(maxDelay * 0.9 * 10.0) / 10.0,
                maxDelay,
                Math.round(onTimePct * 10.0) / 10.0,
                maxDelay > 350 ? 1 : 0,
                avgSpeed < 2.0 ? 1 : 0,
                Math.round(avgSpeed * 10.0) / 10.0,
                durationMs,
                Instant.now(),
                anomalies
        );

        try {
            kafkaTemplate.send(KafkaTopics.ANALYTICS_RESULTS, result.source() + ":" + result.routeId(), result);
            kafkaTemplate.send(KafkaTopics.WORKER_TELEMETRY, assignedWorker, new WorkerTelemetryMessage(
                    assignedWorker,
                    TelemetryEventType.JOB_COMPLETED,
                    WorkerStatus.ACTIVE,
                    job.jobId(),
                    durationMs,
                    processedPerWorker.get(assignedWorker).get(),
                    0L,
                    null,
                    Instant.now()
            ));
        } catch (Exception e) {
            log.warn("Kafka broker not reachable; result computed locally: {}", e.getMessage());
        }
        return result;
    }

    @GetMapping("/worker-pool")
    @Operation(summary = "List all virtual worker instances and their status")
    public Map<String, Object> getWorkerPool() {
        return Map.of("workers", virtualWorkers, "timestamp", Instant.now());
    }

    @PostMapping("/worker-pool/scale")
    @Operation(summary = "Scale the virtual worker pool up or down in <100ms")
    public Map<String, Object> scalePool(@RequestBody ScaleWorkerPoolRequest request) {
        int target = Math.max(1, Math.min(12, request.targetWorkerCount()));
        virtualWorkers.clear();
        for (int i = 1; i <= target; i++) {
            String id = "worker-" + i;
            virtualWorkers.put(id, WorkerStatus.ACTIVE);
            processedPerWorker.putIfAbsent(id, new AtomicLong(0));
        }
        emitHeartbeats();
        return Map.of("activeWorkers", virtualWorkers.keySet(), "reason", request.reason());
    }

    @PostMapping("/worker-pool/{workerId}/kill")
    @Operation(summary = "Simulate a worker crash (stops its 5s heartbeat so Worker Manager marks it DEAD)")
    public Map<String, Object> killWorker(@PathVariable String workerId) {
        virtualWorkers.put(workerId, WorkerStatus.DEAD);
        return Map.of("workerId", workerId, "status", WorkerStatus.DEAD);
    }

    @PostMapping("/dev/inject-job")
    @Operation(summary = "[Member 3 Standalone Dev Tool] Execute a sample TfNSW analytical job directly without needing Worker Manager")
    public AnalyticalResultMessage devInjectJob(@RequestParam(defaultValue = "T1") String routeId) {
        AnalyticalJobMessage sample = SampleTransitFixtures.sampleAnalyticalJob(TransitSource.TFNSW, routeId);
        return processJob(sample);
    }
}
