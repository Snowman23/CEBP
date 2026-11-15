package ro.upb.cebp.common.fixtures;

import ro.upb.cebp.common.enums.AnomalySeverity;
import ro.upb.cebp.common.enums.JobType;
import ro.upb.cebp.common.enums.TransitSource;
import ro.upb.cebp.common.enums.TransportMode;
import ro.upb.cebp.common.events.AnalyticalJobMessage;
import ro.upb.cebp.common.events.AnalyticalResultMessage;
import ro.upb.cebp.common.events.NormalizedTransitEvent;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Deterministic & realistic sample data generator for Sydney (TfNSW Initial Scope),
 * London (TfL), and Helsinki (HSL).
 *
 * Allows all 4 team members to develop and test their microservices completely independently!
 */
public final class SampleTransitFixtures {

    private SampleTransitFixtures() {
    }

    public static List<NormalizedTransitEvent> generateTfNswSydneyBatch(int count) {
        List<NormalizedTransitEvent> list = new ArrayList<>(count);
        String[][] sydneyRoutes = {
                {"T1", "T1 North Shore & Western Line", "TRAIN", "200060", "Central Station", "-33.8830", "151.2070"},
                {"T4", "T4 Eastern Suburbs & Illawarra", "TRAIN", "200080", "Town Hall Station", "-33.8731", "151.2071"},
                {"333", "PrePay Only - North Bondi to City Circular Quay", "BUS", "202618", "Bondi Beach, Campbell Pde", "-33.8908", "151.2743"},
                {"F1", "F1 Manly to Circular Quay", "FERRY", "200020", "Circular Quay Wharf 3", "-33.8610", "151.2108"},
                {"L2", "L2 Randwick Line", "TRAM", "200065", "Chalmers St Light Rail", "-33.8842", "151.2091"}
        };

        Instant now = Instant.now();
        ThreadLocalRandom rng = ThreadLocalRandom.current();

        for (int i = 0; i < count; i++) {
            String[] r = sydneyRoutes[i % sydneyRoutes.length];
            int delaySec = (i % 5 == 0) ? rng.nextInt(320, 680) : rng.nextInt(-30, 180);
            int ttsSec = rng.nextInt(30, 600);
            double lat = Double.parseDouble(r[5]) + rng.nextDouble(-0.015, 0.015);
            double lon = Double.parseDouble(r[6]) + rng.nextDouble(-0.015, 0.015);
            double speed = (delaySec > 400) ? rng.nextDouble(0.0, 0.4) : rng.nextDouble(5.5, 18.5);

            list.add(new NormalizedTransitEvent(
                    UUID.randomUUID().toString(),
                    TransitSource.TFNSW,
                    now,
                    now.minusSeconds(rng.nextInt(1, 5)),
                    TransportMode.valueOf(r[2]),
                    "TfNSW-Sydney",
                    r[0],
                    r[1],
                    (i % 2 == 0) ? "1" : "2",
                    "NSW-" + r[0] + "-" + (100 + (i % 20)),
                    r[3],
                    r[4],
                    lat,
                    lon,
                    speed,
                    rng.nextInt(0, 360),
                    delaySec,
                    ttsSec,
                    now.plusSeconds(ttsSec),
                    (delaySec > 300) ? "STANDING_ROOM_ONLY" : "MANY_SEATS_AVAILABLE",
                    Map.of(
                            "apiEndpoint", "https://api.transport.nsw.gov.au/v1/tp/departure_mon",
                            "platformName", "Platform " + ((i % 8) + 1)
                    )
            ));
        }
        return list;
    }

    public static List<NormalizedTransitEvent> generateMultiCityMixBatch(int count) {
        List<NormalizedTransitEvent> list = new ArrayList<>(count);
        list.addAll(generateTfNswSydneyBatch(Math.max(1, count / 2)));

        Instant now = Instant.now();
        ThreadLocalRandom rng = ThreadLocalRandom.current();

        for (int i = list.size(); i < count; i++) {
            if (i % 2 == 0) {
                // London TfL StopPoint Arrivals sample
                int tts = rng.nextInt(45, 480);
                int delay = rng.nextInt(0, 360);
                list.add(new NormalizedTransitEvent(
                        UUID.randomUUID().toString(),
                        TransitSource.TFL,
                        now,
                        now.minusSeconds(2),
                        TransportMode.METRO,
                        "TfL-London",
                        "victoria",
                        "Victoria Line",
                        "inbound",
                        "TFL-VIC-" + (200 + (i % 15)),
                        "940GZZLUOXC",
                        "Oxford Circus Underground Station",
                        51.5152,
                        -0.1419,
                        12.4,
                        180,
                        delay,
                        tts,
                        now.plusSeconds(tts),
                        "FEW_SEATS_AVAILABLE",
                        Map.of("towards", "Brixton", "currentLocation", "Between Warren Street and Oxford Circus")
                ));
            } else {
                // Helsinki HSL MQTT High-Frequency Positioning sample
                int delay = rng.nextInt(-60, 420);
                list.add(new NormalizedTransitEvent(
                        UUID.randomUUID().toString(),
                        TransitSource.HSL,
                        now,
                        now.minusSeconds(1),
                        TransportMode.BUS,
                        "HSL-Helsinki",
                        "1040",
                        "Route 40 Elielinaukio - Kannelmäki",
                        "2",
                        "HSL-128" + (i % 9),
                        "HSL:10401",
                        "Kamppi",
                        60.1685 + rng.nextDouble(-0.01, 0.01),
                        24.9324 + rng.nextDouble(-0.01, 0.01),
                        rng.nextDouble(6.0, 14.0),
                        145,
                        delay,
                        rng.nextInt(30, 300),
                        now.plusSeconds(120),
                        "MANY_SEATS_AVAILABLE",
                        Map.of("mqttTopic", "/hfp/v2/journey/ongoing/vp/bus/0022/01281/1040/2/Kannelmaki")
                ));
            }
        }
        return list;
    }

    public static AnalyticalJobMessage sampleAnalyticalJob(TransitSource source, String routeId) {
        List<NormalizedTransitEvent> events = generateTfNswSydneyBatch(12);
        return new AnalyticalJobMessage(
                "job-" + UUID.randomUUID().toString().substring(0, 8),
                JobType.COMPREHENSIVE_ROUTE_ANALYSIS,
                source,
                routeId,
                "Sydney Route " + routeId,
                1,
                Instant.now(),
                events
        );
    }

    public static AnalyticalResultMessage sampleAnalyticalResult(String workerId) {
        Instant now = Instant.now();
        return new AnalyticalResultMessage(
                "res-" + UUID.randomUUID().toString().substring(0, 8),
                "job-demo-001",
                workerId,
                JobType.COMPREHENSIVE_ROUTE_ANALYSIS,
                TransitSource.TFNSW,
                TransportMode.TRAIN,
                "T1",
                "T1 North Shore & Western Line",
                25,
                245.0,
                490.0,
                610.0,
                64.0,
                2,
                1,
                9.8,
                84L,
                now,
                List.of(
                        new AnalyticalResultMessage.DetectedAnomaly(
                                "anom-" + UUID.randomUUID().toString().substring(0, 8),
                                "SEVERE_DELAY",
                                AnomalySeverity.HIGH,
                                "NSW-T1-104",
                                "Central Station",
                                "Sydney Trains T1 service delayed by 610s approaching Central Station Platform 16",
                                610.0,
                                now
                        ),
                        new AnalyticalResultMessage.DetectedAnomaly(
                                "anom-" + UUID.randomUUID().toString().substring(0, 8),
                                "VEHICLE_BUNCHING",
                                AnomalySeverity.MEDIUM,
                                "NSW-333-102",
                                "Bondi Beach, Campbell Pde",
                                "Two Route 333 buses bunched within 42 seconds headway toward Circular Quay",
                                42.0,
                                now
                        )
                )
        );
    }
}
