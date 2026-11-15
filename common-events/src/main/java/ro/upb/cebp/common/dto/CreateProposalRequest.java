package ro.upb.cebp.common.dto;

import ro.upb.cebp.common.enums.ProposalType;
import ro.upb.cebp.common.enums.TransitSource;

import java.util.List;

/**
 * Request body sent by the Remote MCP Server tools (`propose_worker_scaling`, `propose_failed_jobs_replay`,
 * `propose_transit_mitigation`) to {@code POST /api/v1/proposals} on {@code core-management-service}.
 */
public record CreateProposalRequest(
        ProposalType proposalType,
        TransitSource targetSource,
        String targetRouteId,
        Integer requestedWorkerCount,
        List<String> targetJobIds,
        String title,
        String reason,
        String recommendedAction,
        String createdByAgent
) {
}
