package ro.upb.cebp.workermanager.autoscaler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import ro.upb.cebp.common.dto.ScaleWorkerPoolRequest;
import ro.upb.cebp.workermanager.registry.WorkerRegistryService;

import java.time.Instant;
import java.util.Map;

/** Applies bounded queue/latency-based scaling to the remote virtual worker pool. */
@Service
public class DynamicWorkerScaler {
    private static final Logger log = LoggerFactory.getLogger(DynamicWorkerScaler.class);
    private static final int MIN_WORKERS = 1;
    private static final int MAX_WORKERS = 8;
    private final WorkerRegistryService registry;
    private final WorkloadMetricsTracker metrics;
    private final RestTemplate restTemplate = new RestTemplate();
    private volatile String lastDecision = "Autoscaler initialized";
    private volatile boolean enabled = true;
    private volatile Instant idleSince;

    @Value("${cebp.worker-pool.base-url:http://localhost:8091}")
    private String workerPoolBaseUrl;

    public DynamicWorkerScaler(WorkerRegistryService registry, WorkloadMetricsTracker metrics) {
        this.registry = registry;
        this.metrics = metrics;
    }

    @Scheduled(fixedDelay = 10000)
    public void evaluate() {
        if (!enabled) return;
        int active = registry.activeCount();
        int pending = metrics.pendingJobs();
        double p95 = metrics.percentile(95);
        if ((pending > Math.max(1, active) * 10 || p95 > 2500) && active < MAX_WORKERS) {
            idleSince = null;
            scaleTo(Math.min(MAX_WORKERS, active + 1), "Queue pressure or p95 latency threshold");
            return;
        }
        if (pending == 0 && active > MIN_WORKERS) {
            if (idleSince == null) idleSince = Instant.now();
            else if (java.time.Duration.between(idleSince, Instant.now()).toSeconds() >= 60) {
                scaleTo(Math.max(MIN_WORKERS, active - 1), "Queue idle for at least 60 seconds");
                idleSince = Instant.now();
            }
        } else idleSince = null;
    }

    public Map<String, Object> scaleTo(int requestedWorkers, String reason) {
        int target = Math.max(MIN_WORKERS, Math.min(MAX_WORKERS, requestedWorkers));
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        try {
            restTemplate.postForEntity(workerPoolBaseUrl + "/api/v1/worker-pool/scale",
                    new HttpEntity<>(new ScaleWorkerPoolRequest(target, reason), headers), Map.class);
            lastDecision = "Scaled/requested " + target + " workers: " + reason;
        } catch (RestClientException exception) {
            lastDecision = "Scale request for " + target + " workers failed: " + exception.getMessage();
            log.warn("Worker pool scale request failed: {}", exception.getMessage());
        }
        return Map.of("targetWorkerCount", target, "decision", lastDecision, "timestamp", Instant.now());
    }

    public String lastDecision() { return lastDecision; }
    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
}
