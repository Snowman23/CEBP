package ro.upb.cebp.common.enums;

public enum TransitSource {
    /** Transport for NSW Open Data (Sydney, Australia) — Primary Initial Scope */
    TFNSW,
    /** Transport for London Unified API (London, UK) */
    TFL,
    /** Helsinki Regional Transport High-Frequency Positioning MQTT (Helsinki, Finland) */
    HSL,
    /** Metropolitan Transportation Authority GTFS-RT (New York City, USA) */
    MTA,
    /** Synthetic burst / replay simulator for dynamic scaling & fault tolerance testing */
    SIMULATOR
}
