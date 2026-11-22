package ro.upb.cebp.workermanager.registry;

import org.springframework.stereotype.Service;
import ro.upb.cebp.common.dto.WorkerClusterStatusDto;
import ro.upb.cebp.common.enums.TelemetryEventType;
import ro.upb.cebp.common.enums.WorkerStatus;
import ro.upb.cebp.common.events.WorkerTelemetryMessage;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** In-memory worker projection built from registration requests and Kafka telemetry. */
@Service
public class WorkerRegistryService {
    private final Map<String, WorkerState> workers = new ConcurrentHashMap<>();

    public WorkerClusterStatusDto.WorkerNodeDto register(String workerId) {
        WorkerState state = workers.computeIfAbsent(workerId, WorkerState::new);
        synchronized (state) {
            state.status = WorkerStatus.ACTIVE;
            state.lastHeartbeatAt = Instant.now();
            state.currentJobId = null;
            return state.toDto();
        }
    }

    public void accept(WorkerTelemetryMessage telemetry) {
        if (telemetry == null || telemetry.workerId() == null) return;
        WorkerState state = workers.computeIfAbsent(telemetry.workerId(), WorkerState::new);
        synchronized (state) {
            if (telemetry.workerStatus() != null) state.status = telemetry.workerStatus();
            if (telemetry.timestamp() != null) state.lastHeartbeatAt = telemetry.timestamp();
            else state.lastHeartbeatAt = Instant.now();
            switch (telemetry.eventType()) {
                case HEARTBEAT -> { }
                case JOB_STARTED -> state.currentJobId = telemetry.currentJobId();
                case JOB_COMPLETED -> {
                    state.currentJobId = null;
                    state.processedJobsCount = Math.max(state.processedJobsCount, telemetry.processedJobsTotal());
                    state.status = WorkerStatus.ACTIVE;
                }
                case JOB_FAILED -> {
                    state.currentJobId = null;
                    state.failedJobsCount = Math.max(state.failedJobsCount, telemetry.failedJobsTotal());
                    state.status = WorkerStatus.ACTIVE;
                }
            }
        }
    }

    /** Marks stale workers DEAD once, returning their in-flight job for recovery. */
    public List<DeadWorker> markDeadWorkers(Instant now, Duration timeout) {
        List<DeadWorker> dead = new ArrayList<>();
        for (WorkerState state : workers.values()) {
            synchronized (state) {
                if (state.status != WorkerStatus.DEAD && state.lastHeartbeatAt != null
                        && Duration.between(state.lastHeartbeatAt, now).compareTo(timeout) > 0) {
                    state.status = WorkerStatus.DEAD;
                    dead.add(new DeadWorker(state.workerId, state.currentJobId));
                }
            }
        }
        return dead;
    }

    public boolean markJobStarted(String workerId, String jobId) {
        if (workerId == null) return false;
        WorkerState state = workers.computeIfAbsent(workerId, WorkerState::new);
        synchronized (state) {
            state.currentJobId = jobId;
            state.status = WorkerStatus.BUSY;
            state.lastHeartbeatAt = Instant.now();
        }
        return true;
    }

    public WorkerClusterStatusDto.WorkerNodeDto get(String workerId) {
        WorkerState state = workers.get(workerId);
        if (state == null) return null;
        synchronized (state) { return state.toDto(); }
    }

    public List<WorkerClusterStatusDto.WorkerNodeDto> list() {
        return workers.values().stream().map(state -> {
            synchronized (state) { return state.toDto(); }
        }).toList();
    }

    public int activeCount() {
        return (int) workers.values().stream().filter(state -> {
            synchronized (state) { return state.status == WorkerStatus.ACTIVE || state.status == WorkerStatus.BUSY; }
        }).count();
    }

    public int deadCount() {
        return (int) workers.values().stream().filter(state -> {
            synchronized (state) { return state.status == WorkerStatus.DEAD; }
        }).count();
    }

    public record DeadWorker(String workerId, String currentJobId) { }

    private static final class WorkerState {
        private final String workerId;
        private WorkerStatus status = WorkerStatus.ACTIVE;
        private String currentJobId;
        private long processedJobsCount;
        private long failedJobsCount;
        private Instant lastHeartbeatAt = Instant.now();

        private WorkerState(String workerId) { this.workerId = workerId; }

        private WorkerClusterStatusDto.WorkerNodeDto toDto() {
            return new WorkerClusterStatusDto.WorkerNodeDto(workerId, status, currentJobId,
                    processedJobsCount, failedJobsCount, lastHeartbeatAt);
        }
    }
}
