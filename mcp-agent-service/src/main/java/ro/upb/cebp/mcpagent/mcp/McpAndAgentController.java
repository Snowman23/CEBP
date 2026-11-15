package ro.upb.cebp.mcpagent.mcp;

import io.swagger.v3.oas.annotations.Hidden;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestClient;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import ro.upb.cebp.common.dto.*;
import ro.upb.cebp.common.enums.AnomalySeverity;
import ro.upb.cebp.common.enums.ProposalType;
import ro.upb.cebp.common.enums.TransitSource;
import ro.upb.cebp.common.enums.WorkerStatus;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Member 4 Starter — Remote MCP Server (`/mcp/sse` & `/mcp/message` JSON-RPC 2.0) +
 * Autonomous AI Agent Investigation Controller (`/api/v1/agent/...`).
 *
 * Includes automatic fallback data when Member 1/2/3 services are offline so Member 4 can develop independently!
 */
@RestController
public class McpAndAgentController {

    private final RestClient restClient;
    private final String coreUrl;
    private final String workerManagerUrl;
    private final String resultsUrl;
    private final List<Map<String, Object>> investigationRuns = new CopyOnWriteArrayList<>();

    public McpAndAgentController(
            RestClient restClient,
            @Value("${cebp.services.core-management-url}") String coreUrl,
            @Value("${cebp.services.worker-manager-url}") String workerManagerUrl,
            @Value("${cebp.services.analytics-results-url}") String resultsUrl
    ) {
        this.restClient = restClient;
        this.coreUrl = coreUrl;
        this.workerManagerUrl = workerManagerUrl;
        this.resultsUrl = resultsUrl;
    }

    // ========================================================================
    // 1. REMOTE MCP SERVER ENDPOINTS (/mcp/sse & /mcp/message - Excluded from OpenAPI per spec)
    // ========================================================================

    @Hidden
    @GetMapping(path = "/mcp/sse", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter connectMcpSse() throws IOException {
        SseEmitter emitter = new SseEmitter(0L);
        emitter.send(SseEmitter.event().name("endpoint").data("/mcp/message"));
        return emitter;
    }

    @Hidden
    @PostMapping("/mcp/message")
    public Map<String, Object> handleMcpJsonRpc(@RequestBody Map<String, Object> rpcRequest) {
        Object id = rpcRequest.getOrDefault("id", 1);
        String method = String.valueOf(rpcRequest.getOrDefault("method", "tools/list"));

        if ("initialize".equals(method)) {
            return Map.of(
                    "jsonrpc", "2.0",
                    "id", id,
                    "result", Map.of(
                            "protocolVersion", "2024-11-05",
                            "serverInfo", Map.of("name", "cebp-transit-mcp-server", "version", "0.1.0"),
                            "capabilities", Map.of("tools", Map.of())
                    )
            );
        }

        if ("tools/list".equals(method)) {
            return Map.of(
                    "jsonrpc", "2.0",
                    "id", id,
                    "result", Map.of("tools", listMcpToolDefinitions())
            );
        }

        if ("tools/call".equals(method)) {
            @SuppressWarnings("unchecked")
            Map<String, Object> params = (Map<String, Object>) rpcRequest.getOrDefault("params", Map.of());
            String toolName = String.valueOf(params.getOrDefault("name", ""));
            @SuppressWarnings("unchecked")
            Map<String, Object> args = (Map<String, Object>) params.getOrDefault("arguments", Map.of());

            Object toolResult = executeMcpTool(toolName, args);
            return Map.of(
                    "jsonrpc", "2.0",
                    "id", id,
                    "result", Map.of(
                            "content", List.of(Map.of("type", "text", "text", String.valueOf(toolResult))),
                            "structuredContent", toolResult
                    )
            );
        }

        return Map.of("jsonrpc", "2.0", "id", id, "result", Map.of("status", "OK"));
    }

    public List<Map<String, Object>> listMcpToolDefinitions() {
        return List.of(
                Map.of(
                        "name", "get_transit_anomalies",
                        "description", "[READ TOOL] Fetch recent public transit delays, vehicle bunching, and speed anomalies from Analytics Results Service",
                        "inputSchema", Map.of("type", "object", "properties", Map.of("source", Map.of("type", "string"), "severity", Map.of("type", "string")))
                ),
                Map.of(
                        "name", "get_worker_cluster_status",
                        "description", "[READ TOOL] Fetch active/dead workers, waiting task queue depth, p50/p95/p99 latency, and throughput from Worker Manager",
                        "inputSchema", Map.of("type", "object", "properties", Map.of())
                ),
                Map.of(
                        "name", "list_failed_analytical_jobs",
                        "description", "[READ TOOL] List failed/DLQ analytical jobs with error reasons from Worker Manager",
                        "inputSchema", Map.of("type", "object", "properties", Map.of())
                ),
                Map.of(
                        "name", "propose_worker_scaling",
                        "description", "[PROPOSE CHANGE TOOL] Propose scaling the Analysis Worker pool up or down based on queue lag or p95 latency",
                        "inputSchema", Map.of("type", "object", "required", List.of("target_workers", "reason"),
                                "properties", Map.of("target_workers", Map.of("type", "integer"), "reason", Map.of("type", "string")))
                ),
                Map.of(
                        "name", "propose_failed_jobs_replay",
                        "description", "[PROPOSE CHANGE TOOL] Propose replaying failed analytical jobs from the Dead-Letter Queue",
                        "inputSchema", Map.of("type", "object", "required", List.of("job_ids", "reason"),
                                "properties", Map.of("job_ids", Map.of("type", "array"), "reason", Map.of("type", "string")))
                ),
                Map.of(
                        "name", "propose_transit_mitigation",
                        "description", "[PROPOSE CHANGE TOOL] Propose an operational transit mitigation (e.g., headway regulation or express dispatch on delayed Sydney TfNSW / London TfL routes)",
                        "inputSchema", Map.of("type", "object", "required", List.of("source", "route_id", "recommended_action", "reason"),
                                "properties", Map.of(
                                        "source", Map.of("type", "string"),
                                        "route_id", Map.of("type", "string"),
                                        "recommended_action", Map.of("type", "string"),
                                        "reason", Map.of("type", "string")
                                ))
                )
        );
    }

    public Object executeMcpTool(String toolName, Map<String, Object> args) {
        return switch (toolName) {
            case "get_transit_anomalies" -> fetchAnomaliesSafe();
            case "get_worker_cluster_status" -> fetchWorkerClusterSafe();
            case "list_failed_analytical_jobs" -> fetchFailedJobsSafe();
            case "propose_worker_scaling" -> submitProposalSafe(new CreateProposalRequest(
                    ProposalType.SCALE_WORKERS,
                    TransitSource.TFNSW,
                    null,
                    args.containsKey("target_workers") ? Integer.parseInt(String.valueOf(args.get("target_workers"))) : 4,
                    null,
                    "Scale Analysis Worker Pool to " + args.getOrDefault("target_workers", 4) + " instances",
                    String.valueOf(args.getOrDefault("reason", "Elevated queue depth and p95 processing latency")),
                    "Trigger POST /api/v1/worker-pool/scale with targetWorkerCount=" + args.getOrDefault("target_workers", 4),
                    "AutonomousTransitOpsAgent (via MCP)"
            ));
            case "propose_failed_jobs_replay" -> submitProposalSafe(new CreateProposalRequest(
                    ProposalType.REPLAY_FAILED_JOBS,
                    TransitSource.TFNSW,
                    "T1",
                    null,
                    List.of("job-dlq-01"),
                    "Replay Failed Analytical Jobs in DLQ",
                    String.valueOf(args.getOrDefault("reason", "Transient worker heartbeat timeout resolved")),
                    "Re-publish failed jobs to Kafka topic `analytics.jobs`",
                    "AutonomousTransitOpsAgent (via MCP)"
            ));
            case "propose_transit_mitigation" -> submitProposalSafe(new CreateProposalRequest(
                    ProposalType.ROUTE_MITIGATION_ALERT,
                    TransitSource.valueOf(String.valueOf(args.getOrDefault("source", "TFNSW"))),
                    String.valueOf(args.getOrDefault("route_id", "T1")),
                    null,
                    null,
                    "Operational Mitigation for Route " + args.getOrDefault("route_id", "T1"),
                    String.valueOf(args.getOrDefault("reason", "Severe route delay and vehicle bunching detected")),
                    String.valueOf(args.getOrDefault("recommended_action", "Insert headway regulation hold at Central Station & increase polling rate")),
                    "AutonomousTransitOpsAgent (via MCP)"
            ));
            default -> Map.of("error", "Unknown MCP tool: " + toolName);
        };
    }

    // ========================================================================
    // 2. OPENAPI AGENT CONTROL & INSPECTION ENDPOINTS (/api/v1/agent/...)
    // ========================================================================

    @Tag(name = "Autonomous Transit & Ops AI Agent", description = "Trigger autonomous MCP investigations and inspect reasoning traces")
    @PostMapping("/api/v1/agent/investigate")
    @Operation(summary = "Run an autonomous AI Agent investigation cycle over MCP tools (reads anomalies & worker lag, proposes improvements)")
    public Map<String, Object> triggerInvestigation() {
        List<TransitAnomalyDto> anomalies = fetchAnomaliesSafe();
        WorkerClusterStatusDto cluster = fetchWorkerClusterSafe();

        List<OperationalProposalDto> createdProposals = new ArrayList<>();

        if (!anomalies.isEmpty()) {
            TransitAnomalyDto top = anomalies.getFirst();
            createdProposals.add(submitProposalSafe(new CreateProposalRequest(
                    ProposalType.ROUTE_MITIGATION_ALERT,
                    top.source(),
                    top.routeId(),
                    null,
                    null,
                    "Mitigate " + top.anomalyType() + " on " + top.source() + " Route " + top.routeId(),
                    top.description(),
                    "Apply headway spacing regulation at " + top.stopName() + " and prioritize route partition in Worker Manager",
                    "AutonomousTransitOpsAgent"
            )));
        }

        if (cluster.waitingTasksCount() > 5 || cluster.deadWorkersCount() > 0) {
            int recommendedWorkers = Math.max(3, cluster.activeWorkersCount() + 2);
            createdProposals.add(submitProposalSafe(new CreateProposalRequest(
                    ProposalType.SCALE_WORKERS,
                    TransitSource.TFNSW,
                    null,
                    recommendedWorkers,
                    null,
                    "Scale Worker Pool to " + recommendedWorkers + " Workers",
                    "Detected " + cluster.waitingTasksCount() + " waiting tasks and " + cluster.deadWorkersCount() + " dead worker(s)",
                    "Scale virtual worker pool to " + recommendedWorkers + " instances and redistribute in-flight partitions",
                    "AutonomousTransitOpsAgent"
            )));
        }

        Map<String, Object> runRecord = Map.of(
                "runId", "run-" + UUID.randomUUID().toString().substring(0, 8),
                "timestamp", Instant.now(),
                "mcpToolsInvoked", List.of("get_worker_cluster_status", "get_transit_anomalies", "propose_transit_mitigation"),
                "anomaliesInspected", anomalies.size(),
                "workerQueueDepthInspected", cluster.waitingTasksCount(),
                "proposalsCreated", createdProposals
        );
        investigationRuns.add(0, runRecord);
        return runRecord;
    }

    @Tag(name = "Autonomous Transit & Ops AI Agent")
    @GetMapping("/api/v1/agent/runs")
    @Operation(summary = "List recent autonomous agent investigation runs and MCP tool call traces")
    public List<Map<String, Object>> getInvestigationRuns() {
        return investigationRuns;
    }

    // --- Safe HTTP Helpers with Automatic Standalone Fallback for Member 4 ---

    private List<TransitAnomalyDto> fetchAnomaliesSafe() {
        try {
            List<TransitAnomalyDto> res = restClient.get()
                    .uri(resultsUrl + "/api/v1/analytics/anomalies")
                    .retrieve()
                    .body(new ParameterizedTypeReference<>() {});
            if (res != null && !res.isEmpty()) {
                return res;
            }
        } catch (Exception ignored) {
        }
        return List.of(new TransitAnomalyDto(
                "anom-fallback-01",
                TransitSource.TFNSW,
                "T1",
                "T1 North Shore & Western Line",
                "SEVERE_DELAY",
                AnomalySeverity.HIGH,
                "NSW-T1-104",
                "Central Station",
                "Sydney Trains T1 delayed by 610s approaching Central Station Platform 16",
                610.0,
                Instant.now()
        ));
    }

    private WorkerClusterStatusDto fetchWorkerClusterSafe() {
        try {
            WorkerClusterStatusDto dto = restClient.get()
                    .uri(workerManagerUrl + "/api/v1/workers/status")
                    .retrieve()
                    .body(WorkerClusterStatusDto.class);
            if (dto != null) {
                return dto;
            }
        } catch (Exception ignored) {
        }
        return new WorkerClusterStatusDto(
                2, 0, 8, 26, 0, 2.4, 68.0, 142.0, 215.0, true,
                "Standalone mock cluster status (Worker Manager offline)",
                Instant.now(),
                List.of(new WorkerClusterStatusDto.WorkerNodeDto("worker-1", WorkerStatus.ACTIVE, null, 14, 0, Instant.now()))
        );
    }

    private List<FailedJobDto> fetchFailedJobsSafe() {
        try {
            List<FailedJobDto> res = restClient.get()
                    .uri(workerManagerUrl + "/api/v1/jobs/failed")
                    .retrieve()
                    .body(new ParameterizedTypeReference<>() {});
            if (res != null) {
                return res;
            }
        } catch (Exception ignored) {
        }
        return List.of();
    }

    private OperationalProposalDto submitProposalSafe(CreateProposalRequest req) {
        try {
            OperationalProposalDto created = restClient.post()
                    .uri(coreUrl + "/api/v1/proposals")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(req)
                    .retrieve()
                    .body(OperationalProposalDto.class);
            if (created != null) {
                return created;
            }
        } catch (Exception ignored) {
        }
        return new OperationalProposalDto(
                "prop-" + UUID.randomUUID().toString().substring(0, 8),
                req.proposalType(),
                ro.upb.cebp.common.enums.ProposalStatus.PENDING,
                req.targetSource(),
                req.targetRouteId(),
                req.requestedWorkerCount(),
                req.targetJobIds(),
                req.title(),
                req.reason(),
                req.recommendedAction(),
                req.createdByAgent(),
                Instant.now(),
                null
        );
    }
}
