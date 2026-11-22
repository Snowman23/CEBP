package ro.upb.cebp.workermanager.registry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import ro.upb.cebp.workermanager.scheduler.JobDispatcher;
import ro.upb.cebp.workermanager.scheduler.RetryAndDlqManager;

import java.time.Duration;
import java.time.Instant;

/** Marks workers dead after a missed heartbeat and recovers their active jobs. */
@Component
public class HeartbeatWatchdog {
    private static final Logger log = LoggerFactory.getLogger(HeartbeatWatchdog.class);
    private static final Duration HEARTBEAT_TIMEOUT = Duration.ofSeconds(15);
    private final WorkerRegistryService registry;
    private final JobDispatcher dispatcher;
    private final RetryAndDlqManager retryManager;

    public HeartbeatWatchdog(WorkerRegistryService registry, JobDispatcher dispatcher, RetryAndDlqManager retryManager) {
        this.registry = registry;
        this.dispatcher = dispatcher;
        this.retryManager = retryManager;
    }

    @Scheduled(fixedDelay = 3000)
    public void detectDeadWorkers() {
        for (WorkerRegistryService.DeadWorker worker : registry.markDeadWorkers(Instant.now(), HEARTBEAT_TIMEOUT)) {
            log.warn("Worker {} marked DEAD after missing heartbeat", worker.workerId());
            if (worker.currentJobId() != null && dispatcher.getInFlight(worker.currentJobId()) != null) {
                retryManager.handleFailure(worker.currentJobId(), worker.workerId(), "Worker heartbeat expired");
            }
        }
    }
}
