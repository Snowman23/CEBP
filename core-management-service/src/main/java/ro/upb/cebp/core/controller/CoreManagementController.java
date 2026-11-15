package ro.upb.cebp.core.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import ro.upb.cebp.common.dto.CreateProposalRequest;
import ro.upb.cebp.common.dto.OperationalProposalDto;
import ro.upb.cebp.common.dto.SourceStatusDto;
import ro.upb.cebp.common.enums.ProposalStatus;
import ro.upb.cebp.common.enums.TransitSource;
import ro.upb.cebp.common.events.NormalizedTransitEvent;
import ro.upb.cebp.common.fixtures.SampleTransitFixtures;
import ro.upb.cebp.common.topics.KafkaTopics;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Member 1 Starter Controller — Exposes the exact OpenAPI contract needed by Member 2, Member 3, and Member 4.
 * Member 1 will expand this with JPA persistence into `core_db` and live HTTP/MQTT polling connectors.
 */
@RestController
@RequestMapping("/api/v1")
@Tag(name = "Core Management & Transit Ingestion", description = "Manage public transit sources, simulate event bursts, and manage AI operational proposals")
public class CoreManagementController {

    private static final Logger log = LoggerFactory.getLogger(CoreManagementController.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final Map<TransitSource, AtomicLong> ingestedCounts = new ConcurrentHashMap<>();
    private final List<OperationalProposalDto> proposals = new CopyOnWriteArrayList<>();

    public CoreManagementController(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        for (TransitSource source : TransitSource.values()) {
            ingestedCounts.put(source, new AtomicLong(0));
        }
    }

    @GetMapping("/sources")
    @Operation(summary = "List configured public transit data sources and ingestion telemetry")
    public List<SourceStatusDto> getSources() {
        Instant now = Instant.now();
        return List.of(
                new SourceStatusDto(
                        TransitSource.TFNSW,
                        "Transport for NSW Open Data (Sydney - Initial Scope)",
                        true,
                        "REPLAY_READY / API_BASICS_V1",
                        ingestedCounts.get(TransitSource.TFNSW).get(),
                        now,
                        "https://api.transport.nsw.gov.au/v1/tp/departure_mon"
                ),
                new SourceStatusDto(
                        TransitSource.TFL,
                        "Transport for London Unified API (StopPoint Arrivals)",
                        true,
                        "LIVE_KEYLESS_JSON",
                        ingestedCounts.get(TransitSource.TFL).get(),
                        now,
                        "https://api.tfl.gov.uk/StopPoint/{id}/Arrivals"
                ),
                new SourceStatusDto(
                        TransitSource.HSL,
                        "Helsinki Regional Transport (HFP v2 MQTT Stream)",
                        true,
                        "LIVE_KEYLESS_MQTT",
                        ingestedCounts.get(TransitSource.HSL).get(),
                        now,
                        "mqtt://mqtt.hsl.fi:1883 (/hfp/v2/journey/ongoing/vp/#)"
                ),
                new SourceStatusDto(
                        TransitSource.MTA,
                        "NYC MTA Real-Time GTFS-RT Feeds",
                        false,
                        "OPTIONAL_STRETCH",
                        ingestedCounts.get(TransitSource.MTA).get(),
                        now,
                        "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace"
                )
        );
    }

    @PostMapping("/ingest/simulate-burst")
    @Operation(summary = "Generate and publish a batch of normalized transit events (TfNSW Sydney / Multi-city) to Kafka `transit.events.raw`")
    public Map<String, Object> simulateBurst(
            @RequestParam(defaultValue = "50") int eventCount,
            @RequestParam(defaultValue = "true") boolean sydneyOnly
    ) {
        List<NormalizedTransitEvent> batch = sydneyOnly
                ? SampleTransitFixtures.generateTfNswSydneyBatch(eventCount)
                : SampleTransitFixtures.generateMultiCityMixBatch(eventCount);

        int published = 0;
        for (NormalizedTransitEvent event : batch) {
            ingestedCounts.computeIfAbsent(event.source(), k -> new AtomicLong()).incrementAndGet();
            try {
                kafkaTemplate.send(KafkaTopics.TRANSIT_EVENTS_RAW, event.partitionKey(), event);
                published++;
            } catch (Exception e) {
                log.warn("Kafka broker not reachable yet; event logged locally: {}", e.getMessage());
            }
        }
        return Map.of(
                "status", "PUBLISHED",
                "topic", KafkaTopics.TRANSIT_EVENTS_RAW,
                "requestedCount", eventCount,
                "publishedToKafka", published,
                "sampleEvent", batch.getFirst()
        );
    }

    @GetMapping("/proposals")
    @Operation(summary = "List all operational proposals submitted by the AI Agent via MCP tools")
    public List<OperationalProposalDto> listProposals() {
        return proposals;
    }

    @PostMapping("/proposals")
    @Operation(summary = "Submit a new operational change proposal (called by the Remote MCP Server tools)")
    public OperationalProposalDto createProposal(@RequestBody CreateProposalRequest request) {
        OperationalProposalDto dto = new OperationalProposalDto(
                "prop-" + UUID.randomUUID().toString().substring(0, 8),
                request.proposalType(),
                ProposalStatus.PENDING,
                request.targetSource() != null ? request.targetSource() : TransitSource.TFNSW,
                request.targetRouteId(),
                request.requestedWorkerCount(),
                request.targetJobIds(),
                request.title(),
                request.reason(),
                request.recommendedAction(),
                request.createdByAgent() != null ? request.createdByAgent() : "AutonomousTransitOpsAgent",
                Instant.now(),
                null
        );
        proposals.add(0, dto);
        return dto;
    }

    @PostMapping("/proposals/{proposalId}/status")
    @Operation(summary = "Approve or reject a pending AI Agent proposal from the Web Client")
    public OperationalProposalDto updateProposalStatus(
            @PathVariable String proposalId,
            @RequestParam ProposalStatus status
    ) {
        for (int i = 0; i < proposals.size(); i++) {
            OperationalProposalDto existing = proposals.get(i);
            if (existing.proposalId().equals(proposalId)) {
                OperationalProposalDto updated = new OperationalProposalDto(
                        existing.proposalId(),
                        existing.proposalType(),
                        status,
                        existing.targetSource(),
                        existing.targetRouteId(),
                        existing.requestedWorkerCount(),
                        existing.targetJobIds(),
                        existing.title(),
                        existing.reason(),
                        existing.recommendedAction(),
                        existing.createdByAgent(),
                        existing.createdAt(),
                        Instant.now()
                );
                proposals.set(i, updated);
                return updated;
            }
        }
        throw new IllegalArgumentException("Proposal not found: " + proposalId);
    }
}
