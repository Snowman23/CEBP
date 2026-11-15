package ro.upb.cebp.results.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.MediaType;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import ro.upb.cebp.common.dto.RouteAnalyticsSummaryDto;
import ro.upb.cebp.common.dto.TransitAnomalyDto;
import ro.upb.cebp.common.dto.TransitOverviewDto;
import ro.upb.cebp.common.enums.AnomalySeverity;
import ro.upb.cebp.common.enums.TransitSource;
import ro.upb.cebp.common.events.AnalyticalResultMessage;
import ro.upb.cebp.common.fixtures.SampleTransitFixtures;
import ro.upb.cebp.common.topics.KafkaTopics;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Member 3 Starter Results Projection Controller.
 * Consumes `analytics.results` from Kafka (strictly adhering to the Single Source of Truth rule — never writing to core_db)
 * and serves OpenAPI + SSE endpoints to Member 4's MCP Server and React Web Client.
 */
@RestController
@RequestMapping("/api/v1/analytics")
@Tag(name = "Analytics Results & Projections", description = "Query route delay rankings, bunching anomalies, and subscribe to live SSE analytics")
public class AnalyticsResultsController {

    private final Map<String, RouteAnalyticsSummaryDto> routeSummaries = new ConcurrentHashMap<>();
    private final List<TransitAnomalyDto> anomalies = new CopyOnWriteArrayList<>();
    private final List<SseEmitter> sseEmitters = new CopyOnWriteArrayList<>();
    private final AtomicLong totalBatches = new AtomicLong(0);
    private final AtomicLong totalEvents = new AtomicLong(0);

    public AnalyticsResultsController() {
        // Seed initial Sydney TfNSW result so Member 4's MCP Server & Web UI have immediate data on startup
        ingestResult(SampleTransitFixtures.sampleAnalyticalResult("worker-1"));
    }

    @KafkaListener(topics = KafkaTopics.ANALYTICS_RESULTS, groupId = "analytics-results-group")
    public void onAnalyticalResult(AnalyticalResultMessage result) {
        ingestResult(result);
    }

    public void ingestResult(AnalyticalResultMessage result) {
        totalBatches.incrementAndGet();
        totalEvents.addAndGet(result.sampleSize());

        String key = result.source().name() + ":" + result.routeId();
        String health = result.averageDelaySeconds() > 300 ? "CRITICAL_DELAY"
                : (result.averageDelaySeconds() > 150 ? "MODERATE_DELAY" : "HEALTHY");

        RouteAnalyticsSummaryDto summary = new RouteAnalyticsSummaryDto(
                result.source(),
                result.transportMode(),
                result.routeId(),
                result.routeName(),
                result.sampleSize(),
                result.averageDelaySeconds(),
                result.p95DelaySeconds(),
                result.maxDelaySeconds(),
                result.onTimePercentage(),
                result.bunchedVehiclePairsCount(),
                result.stalledOrAnomalyVehiclesCount(),
                result.averageSpeedMps(),
                health,
                result.analyzedAt() != null ? result.analyzedAt() : Instant.now()
        );
        routeSummaries.put(key, summary);

        if (result.anomalies() != null) {
            for (AnalyticalResultMessage.DetectedAnomaly det : result.anomalies()) {
                anomalies.add(0, new TransitAnomalyDto(
                        det.anomalyId(),
                        result.source(),
                        result.routeId(),
                        result.routeName(),
                        det.anomalyType(),
                        det.severity(),
                        det.vehicleId(),
                        det.stopName(),
                        det.description(),
                        det.metricValue(),
                        det.detectedAt()
                ));
            }
        }

        for (SseEmitter emitter : sseEmitters) {
            try {
                emitter.send(SseEmitter.event().name("analytical-result").data(summary));
            } catch (IOException e) {
                sseEmitters.remove(emitter);
            }
        }
    }

    @GetMapping("/overview")
    @Operation(summary = "Get global transit KPI overview across all active sources (TfNSW, TfL, HSL, MTA)")
    public TransitOverviewDto getOverview() {
        double avgDelay = routeSummaries.values().stream()
                .mapToDouble(RouteAnalyticsSummaryDto::averageDelaySeconds)
                .average()
                .orElse(0.0);
        double avgOnTime = routeSummaries.values().stream()
                .mapToDouble(RouteAnalyticsSummaryDto::onTimePercentage)
                .average()
                .orElse(100.0);
        long highSeverity = anomalies.stream()
                .filter(a -> a.severity() == AnomalySeverity.HIGH || a.severity() == AnomalySeverity.CRITICAL)
                .count();

        return new TransitOverviewDto(
                totalBatches.get(),
                totalEvents.get(),
                routeSummaries.size(),
                Math.round(avgDelay * 10.0) / 10.0,
                Math.round(avgOnTime * 10.0) / 10.0,
                (int) highSeverity,
                Map.of("TFNSW", Math.round(avgDelay * 10.0) / 10.0, "TFL", 95.0, "HSL", 42.0),
                Instant.now()
        );
    }

    @GetMapping("/routes")
    @Operation(summary = "List route-by-route analytical summaries (optionally filtered by source)")
    public List<RouteAnalyticsSummaryDto> getRoutes(@RequestParam(required = false) TransitSource source) {
        return routeSummaries.values().stream()
                .filter(r -> source == null || r.source() == source)
                .toList();
    }

    @GetMapping("/anomalies")
    @Operation(summary = "List recent transit anomalies (delays, vehicle bunching, speed drops)")
    public List<TransitAnomalyDto> getAnomalies(
            @RequestParam(required = false) TransitSource source,
            @RequestParam(required = false) AnomalySeverity severity
    ) {
        return anomalies.stream()
                .filter(a -> source == null || a.source() == source)
                .filter(a -> severity == null || a.severity() == severity)
                .toList();
    }

    @GetMapping(path = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    @Operation(summary = "Subscribe to real-time Server-Sent Events (SSE) stream of newly computed analytics")
    public SseEmitter streamResults() {
        SseEmitter emitter = new SseEmitter(0L);
        sseEmitters.add(emitter);
        emitter.onCompletion(() -> sseEmitters.remove(emitter));
        emitter.onTimeout(() -> sseEmitters.remove(emitter));
        return emitter;
    }
}
