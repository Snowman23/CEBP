package ro.upb.cebp.common.dto;

/**
 * Request body sent between {@code worker-manager-service} and {@code analysis-worker}
 * ({@code POST /api/v1/worker-pool/scale}) to dynamically scale virtual worker threads/consumers.
 */
public record ScaleWorkerPoolRequest(
        int targetWorkerCount,
        String reason
) {
}
