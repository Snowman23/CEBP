package ro.upb.cebp.workermanager.autoscaler;

import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/** Tracks in-memory job queue depth, completion throughput, failures and recent latency samples. */
@Service
public class WorkloadMetricsTracker {
    private static final int MAX_LATENCY_SAMPLES = 2000;
    private final Set<String> pendingJobIds = ConcurrentHashMap.newKeySet();
    private final ArrayDeque<Double> latencySamples = new ArrayDeque<>();
    private final AtomicLong completedTotal = new AtomicLong();
    private final AtomicLong failedTotal = new AtomicLong();
    private final AtomicLong completedInWindow = new AtomicLong();
    private volatile Instant throughputWindowStarted = Instant.now();

    public void jobDispatched(String jobId) { pendingJobIds.add(jobId); }
    public int pendingJobs() { return pendingJobIds.size(); }
    public long completedTotal() { return completedTotal.get(); }
    public long failedTotal() { return failedTotal.get(); }

    public void jobCompleted(String jobId, Long durationMs) {
        if (!pendingJobIds.remove(jobId)) return;
        completedTotal.incrementAndGet();
        completedInWindow.incrementAndGet();
        if (durationMs != null && durationMs >= 0) {
            synchronized (latencySamples) {
                latencySamples.addLast(durationMs.doubleValue());
                while (latencySamples.size() > MAX_LATENCY_SAMPLES) latencySamples.removeFirst();
            }
        }
    }

    /** Counts an attempt failure; leave it pending while retries remain. */
    public void jobAttemptFailed() { failedTotal.incrementAndGet(); }
    public void jobMovedToDlq(String jobId) { pendingJobIds.remove(jobId); }

    public double percentile(int percentile) {
        ArrayList<Double> samples;
        synchronized (latencySamples) { samples = new ArrayList<>(latencySamples); }
        if (samples.isEmpty()) return 0;
        Collections.sort(samples);
        int index = (int) Math.ceil(percentile / 100.0 * samples.size()) - 1;
        return samples.get(Math.max(0, Math.min(samples.size() - 1, index)));
    }

    public double throughputJobsPerSecond() {
        Instant now = Instant.now();
        Duration elapsed = Duration.between(throughputWindowStarted, now);
        double rate = completedInWindow.get() / Math.max(1.0, elapsed.toMillis() / 1000.0);
        if (elapsed.compareTo(Duration.ofSeconds(10)) >= 0) {
            synchronized (this) {
                if (Duration.between(throughputWindowStarted, Instant.now()).compareTo(Duration.ofSeconds(10)) >= 0) {
                    completedInWindow.set(0);
                    throughputWindowStarted = Instant.now();
                }
            }
        }
        return rate;
    }
}
