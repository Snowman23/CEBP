package ro.upb.cebp.common.dto;

import ro.upb.cebp.common.enums.TransitSource;

import java.time.Instant;

/**
 * Status of an ingestion connector in {@code core-management-service}.
 */
public record SourceStatusDto(
        TransitSource source,
        String displayName,
        boolean enabled,
        String mode,
        long ingestedEventsCount,
        Instant lastEventAt,
        String endpointSummary
) {
}
