package ro.upb.cebp.common.events;

import ro.upb.cebp.common.enums.TransitSource;
import ro.upb.cebp.common.enums.TransportMode;

import java.time.Instant;
import java.util.Map;

/**
 * Canonical normalized public transit event produced by {@code core-management-service}
 * onto Kafka topic {@code transit.events.raw}.
 *
 * Unifies TfNSW (Sydney), TfL (London), HSL (Helsinki MQTT), and MTA (NYC) feeds.
 */
public record NormalizedTransitEvent(
        String eventId,
        TransitSource source,
        Instant ingestedAt,
        Instant eventTimestamp,
        TransportMode transportMode,
        String agencyId,
        String routeId,
        String routeName,
        String directionId,
        String vehicleId,
        String stopId,
        String stopName,
        Double latitude,
        Double longitude,
        Double speedMps,
        Integer heading,
        Integer delaySeconds,
        Integer timeToStationSeconds,
        Instant expectedArrival,
        String occupancyStatus,
        Map<String, String> rawAttributes
) {
    public String partitionKey() {
        return (source != null ? source.name() : "UNKNOWN") + ":" + (routeId != null ? routeId : "DEFAULT");
    }
}
