package ro.upb.cebp.common.enums;

public enum JobType {
    /** Evaluates route delay distribution, mean/p95 delay, and severe delay alerts. */
    DELAY_ANALYSIS,
    /** Detects consecutive vehicles on the same route/direction that are bunched together. */
    HEADWAY_BUNCHING,
    /** Detects stalled vehicles, sudden speed drops, or GPS telemetry anomalies. */
    SPEED_ANOMALY,
    /** Runs all three analytical processors concurrently on a route window batch. */
    COMPREHENSIVE_ROUTE_ANALYSIS
}
