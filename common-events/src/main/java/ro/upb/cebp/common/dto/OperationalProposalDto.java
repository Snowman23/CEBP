package ro.upb.cebp.common.dto;

import ro.upb.cebp.common.enums.ProposalStatus;
import ro.upb.cebp.common.enums.ProposalType;
import ro.upb.cebp.common.enums.TransitSource;

import java.time.Instant;
import java.util.List;

/**
 * Shared DTO representing an AI-Agent proposed operational or scaling change stored in {@code core-management-service}.
 */
public record OperationalProposalDto(
        String proposalId,
        ProposalType proposalType,
        ProposalStatus status,
        TransitSource targetSource,
        String targetRouteId,
        Integer requestedWorkerCount,
        List<String> targetJobIds,
        String title,
        String reason,
        String recommendedAction,
        String createdByAgent,
        Instant createdAt,
        Instant resolvedAt
) {
}
