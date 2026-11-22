package ro.upb.cebp.workermanager;

import org.junit.jupiter.api.Test;
import ro.upb.cebp.common.enums.TransitSource;
import ro.upb.cebp.common.enums.WorkerStatus;
import ro.upb.cebp.common.events.WorkerTelemetryMessage;
import ro.upb.cebp.common.enums.TelemetryEventType;
import ro.upb.cebp.workermanager.registry.WorkerRegistryService;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class WorkerRegistryServiceTest {
    @Test
    void heartbeatRefreshesWorkerAndStaleWorkerIsMarkedDead() {
        WorkerRegistryService registry = new WorkerRegistryService();
        Instant now = Instant.now();
        registry.accept(new WorkerTelemetryMessage("worker-a", TelemetryEventType.HEARTBEAT,
                WorkerStatus.ACTIVE, null, null, 0, 0, null, now.minusSeconds(20)));

        var dead = registry.markDeadWorkers(now, Duration.ofSeconds(15));

        assertEquals(1, dead.size());
        assertEquals("worker-a", dead.getFirst().workerId());
        assertEquals(WorkerStatus.DEAD, registry.get("worker-a").status());
    }

    @Test
    void completedTelemetryClearsTheWorkersCurrentJob() {
        WorkerRegistryService registry = new WorkerRegistryService();
        registry.accept(new WorkerTelemetryMessage("worker-a", TelemetryEventType.JOB_STARTED,
                WorkerStatus.BUSY, "job-a", null, 0, 0, null, Instant.now()));
        registry.accept(new WorkerTelemetryMessage("worker-a", TelemetryEventType.JOB_COMPLETED,
                WorkerStatus.ACTIVE, "job-a", 50L, 1, 0, null, Instant.now()));

        assertEquals(WorkerStatus.ACTIVE, registry.get("worker-a").status());
        assertNull(registry.get("worker-a").currentJobId());
        assertEquals(1, registry.get("worker-a").processedJobsCount());
    }
}
