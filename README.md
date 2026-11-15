# CEBP — Distributed Public Transit Data Analytics Platform with Dynamic Worker Management

A distributed event-driven software application built on the **Java 21 & Spring Boot** platform using **Apache Kafka (Redpanda)**, **PostgreSQL**, **OpenAPI 3**, **Docker Compose**, and a **Remote Model Context Protocol (MCP) Server + Autonomous AI Operations Agent**.

---

## Documentation & Agent Onboarding

- **[`AGENTS.md`](./AGENTS.md)** — General Scope, Academic Requirements, Public Transit Data Sources (TfNSW Initial Scope, TfL, HSL, MTA), Canonical Event Schema, and Multi-Agent Coordination Guide.
- **[`docs/IMPLEMENTATION_PLAN.md`](./docs/IMPLEMENTATION_PLAN.md)** — Master Phased Implementation Plan, Project Directory Structure, Kafka Topic Contracts, OpenAPI Endpoints, Worker Manager Scaling Protocol, and Remote MCP Server Tools.

---

## System Architecture (6 Components)

| # | Component | Port | Responsibility |
| :-: | :--- | :-: | :--- |
| **1** | **`core-management-service`** | `8081` | **Single Source of Truth (SSOT)**. Owns `core_db` (PostgreSQL), ingests real-time public transit feeds (**Transport for NSW** initial scope + TfL, HSL MQTT, NYC MTA), stores canonical events & operational proposals, and publishes raw business events to Kafka (`transit.events.raw`). |
| **2** | **`worker-manager-service`** | `8082` | Registers workers, monitors 5s heartbeats (`worker.heartbeats`), groups raw events into analytical jobs (`analytics.jobs`), tracks waiting tasks & p50/p95/p99 latency, detects dead workers, dynamically scales workers, and retries failed tasks (`analytics.jobs.dlq`). |
| **3** | **`analysis-worker`** (`1..N`) | `8091+` | Horizontally scalable worker nodes consuming `analytics.jobs`, executing concurrent route delay, vehicle bunching, and speed anomaly analyses, and publishing results to `analytics.results`. |
| **4** | **`analytics-results-service`** | `8083` | Consumes `analytics.results` from Kafka, builds an isolated read-optimized projection store (`results_db`), and exposes OpenAPI-documented HTTP REST & SSE streaming APIs. |
| **5** | **`web-client`** | `3000` | React + TypeScript dashboard displaying real-time transit analytics, worker cluster health, Kafka throughput/lag, and AI agent operational proposals. |
| **6** | **`mcp-agent-service`** | `8084` | Exposes a **Remote MCP Server** (`/mcp/sse`) with 7 tools (read anomalies/worker lag + propose worker scaling/job replay/transit mitigations) and runs an **Autonomous Transit & Operations AI Agent**. |

---

## Real-Time Public Transit Data Sources

Selected from [`conduktor/public-streaming-api` (Transportation / Public Transit)](https://github.com/conduktor/public-streaming-api#development--testing):

1. **Initial Scope (Primary) — [Transport for NSW (TfNSW) Open Data](https://opendata.transport.nsw.gov.au/developers/api-basics)** (Sydney, Australia)
   - Base URL: `https://api.transport.nsw.gov.au/v1/` (`Authorization: apikey <TFNSW_API_KEY>`)
   - Feeds: Trip Planner Departure Monitor JSON (`/v1/tp/departure_mon`) & GTFS-Realtime Protobuf (`/v1/gtfs/realtime/buses`, `/v1/gtfs/vehiclepos/buses`), with built-in deterministic replay fallback when offline.
2. **[Transport for London (TfL) API](https://tfl.gov.uk/info-for/open-data-users/our-open-data)** (London, UK)
   - Endpoint: `https://api.tfl.gov.uk/StopPoint/{id}/Arrivals` (Keyless JSON arrival predictions).
3. **[Helsinki Regional Transport (HSL)](https://digitransit.fi/en/developers/)** (Helsinki, Finland)
   - Broker: `mqtt://mqtt.hsl.fi:1883` (`/hfp/v2/journey/ongoing/vp/#` — Keyless 1 Hz real-time MQTT JSON vehicle telemetry).
4. **[MTA Real-Time Data Feeds](https://new.mta.info/developers)** (New York City, USA)
   - Endpoint: `https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/...` (GTFS-Realtime Protobuf feeds).
