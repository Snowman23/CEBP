# CEBP Project — Master Implementation Plan & Project Structure

> **System Title**: A Distributed Public Transit Data Analytics Platform with Dynamic Worker Management  
> **Platform**: Java 21 LTS · Spring Boot 3.4+ · Apache Kafka (Redpanda) · PostgreSQL · SpringDoc OpenAPI · Remote MCP Server & Autonomous AI Agent · React/TypeScript Web Client · Docker Compose

---

## 1. Proposed Repository & Directory Structure

All source code will live inside the `CEBP/` Git repository as a multi-module Maven project alongside the React Web Client and Docker orchestration files:

```text
CEBP/
├── pom.xml                                  # Parent Maven POM (Java 21, Spring Boot 3.4.x, Kafka, Protobuf, OpenAPI)
├── docker-compose.yml                       # Full stack orchestration: Redpanda/Kafka, Postgres, 5 Java services, Web UI
├── .env.example                             # Template for TFNSW_API_KEY, MTA_API_KEY, GEMINI_API_KEY, etc.
├── README.md                                # Project overview, quickstart, and architecture diagrams
├── AGENTS.md                                # Multi-agent scope, resources, and current status tracker
├── docs/
│   ├── IMPLEMENTATION_PLAN.md               # This master implementation plan
│   └── api-specs/                           # Exported OpenAPI 3.0 YAML/JSON specifications per service
│
├── common-events/                           # Shared Java library (DTOs, Kafka event schemas, GTFS-RT protobuf bindings)
│   ├── pom.xml
│   └── src/main/java/ro/upb/cebp/common/
│       ├── events/
│       │   ├── NormalizedTransitEvent.java  # Canonical event produced by Core Management Service
│       │   ├── AnalyticalJobMessage.java    # Job payload dispatched by Worker Manager
│       │   ├── WorkerHeartbeatMessage.java  # Heartbeat emitted by Analysis Workers
│       │   ├── JobStatusMessage.java        # STARTED / COMPLETED / FAILED / RETRY event
│       │   └── AnalyticalResultMessage.java # Computed transit analytics emitted by Analysis Workers
│       ├── enums/
│       │   ├── TransitSource.java           # TFNSW, TFL, HSL, MTA, SIMULATOR
│       │   ├── TransportMode.java           # TRAIN, METRO, BUS, TRAM, FERRY
│       │   ├── JobType.java                 # DELAY_ANALYSIS, HEADWAY_BUNCHING, SPEED_ANOMALY
│       │   └── WorkerStatus.java            # ACTIVE, BUSY, STALE, DEAD, DRAINING
│       └── topics/
│           └── KafkaTopics.java             # Constants for all Kafka topic names
│
├── core-management-service/                 # Component 1: Owns Main DB (SSOT) & Ingests Transit Streams (:8081)
│   ├── Dockerfile
│   ├── pom.xml
│   └── src/main/
│       ├── java/ro/upb/cebp/core/
│       │   ├── CoreManagementApplication.java
│       │   ├── config/                      # Kafka producer, OpenAPI, RestClient, Scheduler config
│       │   ├── domain/                      # JPA Entities: TransitRouteEntity, IngestedEventEntity, OperationalProposalEntity
│       │   ├── repository/                  # Spring Data JPA repositories (ONLY service writing to core_db)
│       │   ├── ingestion/
│       │   │   ├── TransitSourceConnector.java       # Common interface for all transit feeds
│       │   │   ├── tfnsw/
│       │   │   │   ├── TfNswApiClient.java           # Initial Scope: https://api.transport.nsw.gov.au/v1/ (Authorization: apikey)
│       │   │   │   ├── TfNswGtfsRealtimeParser.java  # Parses GTFS-RT Protobuf (/v1/gtfs/realtime/buses, vehiclepos)
│       │   │   │   ├── TfNswDepartureMonClient.java  # Parses JSON Trip Planner /v1/tp/departure_mon
│       │   │   │   └── TfNswReplayFallback.java      # Deterministic Sydney dataset replay when offline / no API key
│       │   │   ├── tfl/
│       │   │   │   └── TflStopArrivalsConnector.java # Polls https://api.tfl.gov.uk/StopPoint/{id}/Arrivals
│       │   │   ├── hsl/
│       │   │   │   └── HslMqttStreamingConnector.java# Subscribes to mqtt://mqtt.hsl.fi:1883 (/hfp/v2/journey/ongoing/vp/#)
│       │   │   ├── mta/
│       │   │   │   └── MtaGtfsRtConnector.java       # Polls https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/...
│       │   │   └── simulator/
│       │   │       └── WorkloadBurstGenerator.java   # Generates high-volume event bursts to test dynamic worker scaling
│       │   ├── messaging/
│       │   │   └── TransitEventProducer.java         # Publishes NormalizedTransitEvent to Kafka `transit.events.raw`
│       │   └── controller/                           # OpenAPI-documented REST APIs
│       │       ├── IngestionController.java          # Start/stop connectors, trigger bursts, inspect source metrics
│       │       └── ProposalController.java           # CRUD & Approval workflow for AI Agent proposals
│       └── resources/
│           ├── application.yml
│           └── sample-data/tfnsw-seed-events.json
│
├── worker-manager-service/                  # Component 2: Worker Registry, Job Assignment, Heartbeats & Scaling (:8082)
│   ├── Dockerfile
│   ├── pom.xml
│   └── src/main/java/ro/upb/cebp/workermanager/
│       ├── WorkerManagerApplication.java
│       ├── registry/
│       │   ├── WorkerRegistryService.java            # Tracks registered workers, status, current load
│       │   └── HeartbeatWatchdog.java                # Detects unresponsive workers (>15s silence -> DEAD)
│       ├── scheduler/
│       │   ├── RawEventWindowBatcher.java            # Consumes `transit.events.raw`, groups by source/route into jobs
│       │   ├── JobDispatcher.java                    # Assigns and publishes jobs to `analytics.jobs`
│       │   └── RetryAndDlqManager.java               # Tracks failed tasks, triggers retries with backoff, routes to DLQ
│       ├── autoscaler/
│       │   ├── WorkloadMetricsTracker.java           # Tracks waiting tasks, p50/p95/p99 processing time, throughput
│       │   └── DynamicWorkerScaler.java              # Starts/stops logical or Docker worker instances based on queue lag
│       └── controller/                               # OpenAPI-documented REST APIs
│           ├── WorkerClusterController.java          # GET /api/v1/workers, POST /api/v1/workers/register, scale up/down
│           └── JobMetricsController.java             # GET /api/v1/jobs/metrics, /api/v1/jobs/failed, POST /api/v1/jobs/replay
│
├── analysis-worker/                         # Component 3: Scalable Analytical Worker Nodes (:8091..809N)
│   ├── Dockerfile
│   ├── pom.xml
│   └── src/main/java/ro/upb/cebp/worker/
│       ├── AnalysisWorkerApplication.java
│       ├── lifecycle/
│       │   ├── WorkerRegistrationClient.java         # Registers with Worker Manager on startup / deregisters on shutdown
│       │   └── WorkerHeartbeatPublisher.java         # Publishes 5s heartbeat + thread/CPU load to `worker.heartbeats`
│       ├── consumer/
│       │   └── AnalyticalJobConsumer.java            # Consumes `analytics.jobs`, executes analysis, emits results & status
│       ├── processors/
│       │   ├── DelayAndPunctualityAnalyzer.java      # Computes mean/p95 delay, on-time %, severe delay flags per route
│       │   ├── HeadwayBunchingAnalyzer.java          # Detects vehicle bunching (<2 min headway or <250m spacing)
│       │   └── SpeedAndTelemetryAnomalyAnalyzer.java # Detects stalled vehicles, abnormal speed drops, GPS jumps
│       └── controller/
│           └── WorkerNodeStatusController.java       # OpenAPI health/status/control endpoint for the worker instance
│
├── analytics-results-service/               # Component 4: Stores Analytical Results & Exposes HTTP APIs (:8083)
│   ├── Dockerfile
│   ├── pom.xml
│   └── src/main/java/ro/upb/cebp/results/
│       ├── AnalyticsResultsApplication.java
│       ├── consumer/
│       │   └── AnalyticalResultsConsumer.java        # Consumes `analytics.results` from Kafka and updates local read DB
│       ├── domain/                                   # Local read-projection entities (RouteAnalyticsView, AnomalyAlertView)
│       ├── repository/                               # Local read store repositories (strictly isolated from core_db)
│       └── controller/                               # OpenAPI-documented REST + SSE APIs
│           ├── AnalyticsQueryController.java         # GET /api/v1/analytics/summary, /routes, /anomalies, /delays
│           └── LiveAnalyticsStreamController.java    # GET /api/v1/analytics/stream (Server-Sent Events for Web Client)
│
├── mcp-agent-service/                       # Component 6: Remote MCP Server + Autonomous AI Operations Agent (:8084)
│   ├── Dockerfile
│   ├── pom.xml
│   └── src/main/java/ro/upb/cebp/mcpagent/
│       ├── McpAgentApplication.java
│       ├── mcp/
│       │   ├── RemoteMcpServerConfig.java            # Exposes MCP over SSE / Streamable HTTP (/mcp/sse, /mcp/message)
│       │   └── TransitOperationsMcpTools.java        # 6 MCP Tools (4 Read tools + 3 Propose Change tools)
│       ├── agent/
│       │   ├── AutonomousTransitOpsAgent.java        # Scheduled & on-demand agent investigating delays, lag & failures
│       │   └── AgentInvestigationLogService.java     # Stores reasoning traces and tool calls for inspection in Web UI
│       └── controller/
│           └── AgentControlController.java           # OpenAPI REST API to inspect agent runs or trigger manual run
│
└── web-client/                              # Component 5: Web Client Dashboard (:3000)
    ├── Dockerfile
    ├── package.json
    ├── vite.config.ts
    └── src/
        ├── api/                                      # Typed REST + SSE clients for Core, Worker Manager, Results, Agent
        ├── components/
        │   ├── TransitMapAndFeedPanel.tsx            # Real-time Sydney TfNSW, London TfL, Helsinki HSL, NYC MTA view
        │   ├── WorkerManagerClusterPanel.tsx         # Worker heartbeats, queue depth, throughput, dynamic scaling controls
        │   ├── AnalyticsChartsPanel.tsx              # Route delays, vehicle bunching alerts, p50/p95 latency charts
        │   └── AiAgentProposalsPanel.tsx             # Autonomous MCP Agent findings & 1-click Approve/Reject proposals
        └── App.tsx
```

---

## 2. Detailed Component Specifications & Contracts

### 2.1 Kafka Topics & Partitioning Strategy
| Topic Name | Partitions | Producer(s) | Consumer(s) | Key | Purpose |
| :--- | :---: | :--- | :--- | :--- | :--- |
| `transit.events.raw` | 6 | `core-management-service` | `worker-manager-service` | `source:routeId` | Raw normalized public transit events from TfNSW, TfL, HSL, MTA. |
| `analytics.jobs` | 6 | `worker-manager-service` | `analysis-worker` (group: `analysis-workers`) | `jobId` (or `routeKey`) | Batched analytical tasks distributed across available workers. |
| `worker.heartbeats` | 3 | `analysis-worker` | `worker-manager-service` | `workerId` | Periodic (5s) liveness, active task count, and resource telemetry. |
| `analytics.job-status` | 3 | `analysis-worker` | `worker-manager-service` | `jobId` | Lifecycle transitions (`STARTED`, `COMPLETED`, `FAILED`) + `durationMs`. |
| `analytics.results` | 6 | `analysis-worker` | `analytics-results-service` | `source:routeId` | Computed analytical metrics, delay aggregations, and anomaly detections. |
| `analytics.jobs.dlq` | 3 | `worker-manager-service` | `worker-manager-service` / MCP replay | `jobId` | Dead-letter queue for jobs exceeding `maxRetries` (3 attempts). |

---

### 2.2 Component 1: `core-management-service` (Port `8081`) — Initial Scope & SSOT
1. **Single Source of Truth (`core_db` PostgreSQL)**:
   - Tables:
     - `monitored_sources`: Configuration and status (`ENABLED`, `PAUSED`, `MOCK_FALLBACK`) for `TFNSW`, `TFL`, `HSL`, `MTA`.
     - `ingested_transit_events`: Append-only log / recent window of canonical transit events written before/alongside Kafka publication (transactional outbox or dual-write with idempotency key).
     - `operational_proposals`: Proposals created by the AI Agent via MCP tools (`PENDING`, `APPROVED`, `REJECTED`, `EXECUTED`) with `proposal_type` (`SCALE_WORKERS`, `REPLAY_FAILED_JOBS`, `ROUTE_MITIGATION_ALERT`).
2. **Initial Scope — Transport for NSW (`TfNswConnector`)**:
   - Implements the **TfNSW API Basics** specification (`https://opendata.transport.nsw.gov.au/developers/api-basics`):
     - Base URL: `https://api.transport.nsw.gov.au/v1/`
     - Header: `Authorization: apikey ${TFNSW_API_KEY}`
     - **JSON Endpoint**: `/v1/tp/departure_mon?outputFormat=rapidJSON&coordOutputFormat=EPSG:4326&mode=direct&type_dm=stop&name_dm=200060&departureMonitorMacro=true&TfNSWDM=true&version=10.2.1.42` (monitors live departures at Sydney hubs such as `200060` Central Station, `200020` Circular Quay, `215010` Parramatta).
     - **GTFS-RT Endpoint**: `/v1/gtfs/realtime/buses` and `/v1/gtfs/vehiclepos/buses` parsed via `com.google.transit:gtfs-realtime-bindings`.
     - **Automatic Fallback**: If `TFNSW_API_KEY` is blank or returns `401/429`, automatically switches to `TfNswReplayFallback` which streams realistic Sydney Trains (`T1`, `T2`, `T4`, `T8`), Sydney Buses (`333`, `380`, `M52`), and Sydney Ferries (`F1 Manly`) events with realistic delays and GPS positions.
3. **Additional Live Connectors**:
   - **TfL Connector**: Polls `https://api.tfl.gov.uk/StopPoint/{stopId}/Arrivals` every 15s for configured London stops (`940GZZLUOXC` Oxford Circus, `940GZZLUVIC` Victoria, `490000015K` Barking).
   - **HSL MQTT Connector**: Connects to `mqtt://mqtt.hsl.fi:1883`, subscribes to `/hfp/v2/journey/ongoing/vp/#`, parses `VP` JSON payloads, and applies a configurable rate-limiter (e.g., 10 to 500 events/sec) so the user can dial up real traffic to trigger dynamic worker scaling.
   - **MTA Connector**: Polls `https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace` and `nyct%2Fgtfs` using `gtfs-realtime-bindings`.
4. **OpenAPI Endpoints (`/swagger-ui.html`)**:
   - `GET /api/v1/sources` — List all transit sources, connection mode (LIVE vs REPLAY), and ingestion rates.
   - `POST /api/v1/sources/{source}/toggle` — Enable/disable or change rate limit of a transit connector.
   - `POST /api/v1/ingest/simulate-burst` — Inject $N$ synthetic transit events (e.g., 1,000 events across 20 routes) to demonstrate Worker Manager queue buildup and autoscaling.
   - `GET /api/v1/proposals` — List all AI-generated operational proposals.
   - `POST /api/v1/proposals` — Create a new proposal (invoked by the MCP Server tools).
   - `POST /api/v1/proposals/{id}/approve` / `reject` — Approve or reject a proposal (triggers scaling/replay webhook to `worker-manager-service`).

---

### 2.3 Component 2: `worker-manager-service` (Port `8082`) — Dynamic Worker Management
Directly implements all 7 responsibilities from Page 2 of `CEBP-w7 tldr.pdf`:
1. **Register workers and track their status**:
   - Workers call `POST /api/v1/workers/register` on startup with `workerId`, `host`, `port`, `capacity`.
   - Registry maintains state: `ACTIVE`, `BUSY`, `STALE`, `DEAD`, `DRAINING`.
2. **Monitor worker heartbeats**:
   - Consumes `worker.heartbeats` topic; updates `lastHeartbeatAt`, `inFlightJobs`, `cpuUsage`, `processedCount`.
3. **Track how many tasks are waiting to be processed**:
   - Buffers incoming `transit.events.raw` into micro-batches (e.g., 25 events or 5-second tumbling window per `source:routeId`) and creates an `AnalyticalJobMessage`.
   - Tracks `pendingJobsCount` (dispatched minus completed/failed) and Kafka consumer group lag.
4. **Monitor processing time and throughput**:
   - Consumes `analytics.job-status`; records `durationMs` into a sliding 5-minute histogram to compute `p50LatencyMs`, `p95LatencyMs`, `p99LatencyMs`, and `throughputJobsPerSec`.
5. **Detect workers that stop responding**:
   - `HeartbeatWatchdog` runs every 3 seconds: if `now - lastHeartbeatAt > 15s`, marks worker `DEAD`, logs a cluster alert, and marks any job currently assigned to that worker as eligible for immediate retry.
6. **Decide when additional workers should be started**:
   - `DynamicWorkerScaler` evaluates scaling rules every 10 seconds:
     - **Scale Up Rule**: If `pendingJobsCount > (activeWorkers * 10)` OR `p95LatencyMs > 2500ms`, start an additional worker instance (up to `maxWorkers = 8`).
     - **Scale Down Rule**: If `pendingJobsCount == 0` and utilization < 20% for 60 seconds, gracefully drain an extra worker (down to `minWorkers = 1`).
     - *Implementation Mechanism*: Supports both **Logical Worker Pool Scaling** (spawning/stopping isolated `AnalysisWorker` Spring contexts/containers or activating dormant worker nodes via control API) AND **Docker Socket Scaling** so it works reliably on any student/grading machine (Windows/Linux/macOS).
7. **Track failed tasks and trigger retries**:
   - When a `JobStatusMessage` reports `FAILED` or a job times out (`>30s`), increments `attemptCount`.
   - If `attemptCount <= 3`, re-publishes `AnalyticalJobMessage` to `analytics.jobs`.
   - If `attemptCount > 3`, publishes to `analytics.jobs.dlq` and stores the failure record so the AI Agent can inspect and propose a replay via MCP!

---

### 2.4 Component 3: `analysis-worker` (Ports `8091..809N`)
- Stateless, horizontally scalable Java worker service.
- Consumes `AnalyticalJobMessage` from `analytics.jobs`.
- Runs three analytical engines in parallel (using Java 21 `StructuredTaskScope` / `CompletableFuture` on Virtual Threads):
  1. **Delay & Punctuality Engine**: Computes average delay, max delay, standard deviation, on-time percentage ($\le 180\text{s}$), and flags `SEVERE_DELAY` if mean delay $> 300\text{s}$.
  2. **Headway & Vehicle Bunching Engine**: Orders vehicles on the same `(source, routeId, directionId)` by `expectedArrival` or GPS distance along route; flags `VEHICLE_BUNCHING` when two consecutive vehicles are $< 90\text{s}$ or $< 200\text{m}$ apart.
  3. **Speed & Telemetry Anomaly Engine**: Flags `STALLED_VEHICLE` ($v < 0.5\text{ m/s}$ outside a stop with high delay) or `TELEMETRY_OUTLIER` ($v > 45\text{ m/s}$ for bus/tram).
- Emits `AnalyticalResultMessage` to `analytics.results` and `JobStatusMessage(COMPLETED, durationMs)` to `analytics.job-status`.

---

### 2.5 Component 4: `analytics-results-service` (Port `8083`)
- Consumes `AnalyticalResultMessage` from Kafka `analytics.results`.
- Persists projections in its own isolated database (`results_db` — never touches `core_db`).
- Exposes OpenAPI-documented REST endpoints:
  - `GET /api/v1/analytics/overview` — Global KPIs across TfNSW, TfL, HSL, MTA (total events analyzed, active routes, average delay by city, anomaly counts).
  - `GET /api/v1/analytics/routes?source=TFNSW` — Route-by-route analytical breakdown (delay p50/p95, bunching count, active vehicles).
  - `GET /api/v1/analytics/anomalies?sinceMinutes=30&severity=HIGH` — Recent detected anomalies (`SEVERE_DELAY`, `VEHICLE_BUNCHING`, `STALLED_VEHICLE`).
  - `GET /api/v1/analytics/stream` — Server-Sent Events (SSE) stream pushing newly computed analytical results to the Web Client in real time.

---

### 2.6 Component 6: `mcp-agent-service` (Port `8084`) — Remote MCP Server + Autonomous AI Agent
Fulfills all requirements from the *"Agentic AI and AI integration"* section of `project submission requirements.txt`:

#### A. Remote MCP Server (`/mcp/sse` & `/mcp/message`)
Exposes **6 MCP Tools** over remote HTTP/SSE transport:

| Tool Name | Category | Parameters | Returns |
| :--- | :---: | :--- | :--- |
| `get_transit_anomalies` | **Read** | `source` (optional), `severity` (optional), `since_minutes` (int) | List of recent route anomalies (bunching, severe delays, stalled vehicles) with timestamps and metrics. |
| `get_route_performance` | **Read** | `source` (string), `route_id` (string) | Detailed punctuality, mean/p95 delay, vehicle count, and trend for the route. |
| `get_worker_cluster_status` | **Read** | *(none)* | Active/dead workers, waiting task queue depth, p50/p95/p99 processing latency, throughput. |
| `list_failed_analytical_jobs` | **Read** | `since_minutes` (int) | Failed/DLQ analytical jobs with `jobId`, `routeId`, `errorReason`, and `attemptCount`. |
| `propose_worker_scaling` | **Propose Change** | `target_workers` (int), `reason` (string), `urgency` (string) | Creates a pending `SCALE_WORKERS` proposal in `core-management-service` and returns `proposalId`. |
| `propose_failed_jobs_replay` | **Propose Change** | `job_ids` (list of strings), `reason` (string) | Creates a pending `REPLAY_FAILED_JOBS` proposal for operator approval or auto-execution. |
| `propose_transit_mitigation` | **Propose Change** | `source` (string), `route_id` (string), `issue_type` (string), `recommended_action` (string) | Creates a pending `ROUTE_MITIGATION_ALERT` operational proposal (e.g., headway regulation or express dispatch on Sydney T1 / London Bus). |

#### B. Autonomous Agentic Component (`AutonomousTransitOpsAgent`)
- **Concrete Operational Task**: Every $N$ minutes (configurable, e.g., every 2 minutes in demo mode, plus manual trigger `/api/v1/agent/investigate`), the agent wakes up and executes a structured multi-step investigation using the MCP tools:
  1. Calls `get_worker_cluster_status()` and `list_failed_analytical_jobs(60)`.
  2. Calls `get_transit_anomalies(null, "HIGH", 30)`.
  3. For the top affected routes (e.g., TfNSW Sydney `T1` or `333`), calls `get_route_performance(source, route_id)`.
  4. Synthesizes root causes (e.g., *"HSL + TfNSW burst caused 85 waiting jobs and p99 latency of 4.2s; 2 jobs failed due to timeout; Sydney Route 333 shows 3 vehicle bunching incidents at Bondi Junction"*) and calls `propose_worker_scaling(...)`, `propose_failed_jobs_replay(...)`, and `propose_transit_mitigation(...)`.
- **LLM Provider Flexibility**: Supports Google Gemini Free Tier (`GEMINI_API_KEY`), OpenAI-compatible endpoints, or a deterministic Rule+Template Reasoning Fallback when running without an external LLM key so the project always works 100% out-of-the-box during grading.

---

## 3. Pragmatic Simplifications & 4-Way Team Split

### 3.1 Key Simplifications (100% In-Scope, Zero Unnecessary Friction)
1. **Virtual Worker Pool instead of Docker-in-Docker**:
   - Instead of mounting `/var/run/docker.sock` to spin up heavy JVM containers (which takes 15+ seconds and behaves differently on Windows vs. Linux), `analysis-worker` manages a **Dynamic Virtual Worker Pool** (`worker-1..N` running on Java 21 Virtual Threads with independent Kafka consumers and 5s heartbeat loops).
   - `worker-manager-service` scales the pool in `<100ms` via `POST /api/v1/worker-pool/scale`, and any individual virtual worker can be killed on demand (`POST /api/v1/worker-pool/{workerId}/kill`) during the lab presentation to demonstrate heartbeat timeout & automatic task retry.
2. **JSON-First Transit Connectors**:
   - Prioritize JSON feeds first: **TfNSW Departure Monitor JSON** (`/v1/tp/departure_mon` + built-in `TfNswReplayFallback`), **TfL Stop Arrivals JSON** (keyless), and **HSL MQTT JSON** (`mqtt://mqtt.hsl.fi:1883`, keyless 1 Hz stream). Binary GTFS-RT Protobuf parsing is kept optional/secondary so nobody gets blocked on `.proto` schemas.
3. **Single PostgreSQL Container (`core_db` Only)**:
   - Only `core-management-service` connects to PostgreSQL (strictly enforcing the **Single Source of Truth** rule).
   - `worker-manager-service` and `analytics-results-service` maintain their projections in **H2 in-memory / concurrent state** rebuilt purely from Kafka topics.
4. **4 Core Kafka Topics**:
   - `transit.events.raw` (Core -> Worker Manager)
   - `analytics.jobs` (Worker Manager -> Analysis Workers)
   - `worker.telemetry` (Analysis Workers -> Worker Manager: unified 5s heartbeats + job `STARTED`/`COMPLETED`/`FAILED` status)
   - `analytics.results` (Analysis Workers -> Analytics Results Service)

---

### 3.2 4-Way Team Split (Maximum Independence, Zero Merge Conflicts)

After a **30-minute Day-1 kickoff** where the team locks down `common-events` (the 4 Kafka Java records) and `docker-compose.yml`, the 4 members work in completely separate folders:

| Team Member | Folders Owned | Deliverables | How to Work & Test 100% Independently |
| :--- | :--- | :--- | :--- |
| **Member 1: Ingestion & SSOT** | `core-management-service/` | • PostgreSQL `core_db` entities (Sources, Raw Events, AI Proposals)<br/>• **TfNSW** JSON connector (`Authorization: apikey`) + Sydney Replay Fallback<br/>• **TfL** REST & **HSL** MQTT connectors + Burst Simulator<br/>• Publishes `transit.events.raw`<br/>• OpenAPI endpoints (`:8081`) | Starts Postgres + Redpanda; tests ingestion & `/api/v1/ingest/simulate-burst` via Swagger UI and verifies events in Redpanda Console. |
| **Member 2: Worker Manager** | `worker-manager-service/` | • Consumes `transit.events.raw` -> dispatches batched `analytics.jobs`<br/>• Consumes `worker.telemetry` -> tracks heartbeats, waiting tasks, p50/p95/p99 latency, throughput<br/>• Dead-worker detector (>15s silence), Retry engine (max 3 attempts), and Dynamic Autoscaler<br/>• OpenAPI endpoints (`:8082`) | Uses a built-in dev-profile `MockTrafficAndWorkerSimulator` that emits synthetic raw events & worker heartbeats to test scaling, dead-worker detection, and retries without Member 1 or 3. |
| **Member 3: Workers & Results** | `analysis-worker/`<br/>`analytics-results-service/` | • Virtual Worker Pool (`worker-1..N`) consuming `analytics.jobs` and emitting 5s heartbeats to `worker.telemetry`<br/>• Route Delay, Vehicle Bunching & Speed Anomaly engines -> publishes `analytics.results`<br/>• Results Consumer + H2 Read Store + OpenAPI REST & SSE stream (`:8083`) | Uses a dev-profile endpoint on `:8091` to inject a test `AnalyticalJobMessage` into `analytics.jobs` and verifies computed results appear on `:8083/swagger-ui.html`. |
| **Member 4: MCP, AI Agent & UI** | `mcp-agent-service/`<br/>`web-client/` | • **Remote MCP Server** (`/mcp/sse` on `:8084`) with Read tools & Propose Change tools<br/>• **Autonomous Ops AI Agent** (Gemini Free Tier + rule fallback) investigating lag/delays & submitting proposals<br/>• **React + TS Dashboard** (`:3000`) for transit analytics, worker scaling/kill demo, and proposal approvals | Develops against a switchable Mock REST Adapter (`USE_MOCK_API=true`) matching the agreed OpenAPI contracts, then flips to live `:8081`, `:8082`, `:8083` ports for integration. |

---

## 4. Phased Execution Plan (Step-by-Step Roadmap)

### Phase 1: Foundation, Infrastructure & Common Contracts (COMPLETED)
- [x] **Task 1.1**: Create parent `pom.xml` in `CEBP/` with Java 21, Spring Boot 3.4.3 BOM, Spring for Apache Kafka, SpringDoc OpenAPI (`2.8.5`), Jackson JSR310, and Eclipse Paho MQTT.
- [x] **Task 1.2**: Implement `common-events` module containing `NormalizedTransitEvent`, `AnalyticalJobMessage`, `WorkerTelemetryMessage`, `AnalyticalResultMessage`, shared cross-service REST DTOs, `SampleTransitFixtures`, and `KafkaTopics`.
- [x] **Task 1.3**: Create `docker-compose.yml` (`redpanda`, `redpanda-console`, `core-postgres`, plus microservice containers) and runnable starter shells with standalone dev simulators for all 4 team members.

### Phase 2: Core Management Service & Initial Scope (TfNSW + Multi-Source Ingestion)
- [ ] **Task 2.1**: Scaffold `core-management-service` (`:8081`) with PostgreSQL JPA entities (`MonitoredSourceEntity`, `IngestedTransitEventEntity`, `OperationalProposalEntity`) and Flyway/DDL auto-schema initialization.
- [ ] **Task 2.2 (Initial Scope Priority)**: Implement **Transport for NSW (TfNSW)** connector (`TfNswApiClient`, `TfNswDepartureMonClient`, `TfNswGtfsRealtimeParser`, and `TfNswReplayFallback`) adhering to `https://opendata.transport.nsw.gov.au/developers/api-basics` (`https://api.transport.nsw.gov.au/v1/`, header `Authorization: apikey <key>`).
- [ ] **Task 2.3**: Implement the secondary connectors:
  - `TflStopArrivalsConnector` (`https://api.tfl.gov.uk/StopPoint/{id}/Arrivals`)
  - `HslMqttStreamingConnector` (`mqtt://mqtt.hsl.fi:1883`, topic `/hfp/v2/journey/ongoing/vp/#`)
  - `MtaGtfsRtConnector` (`https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace`)
  - `WorkloadBurstGenerator` (for on-demand stress testing & fault injection)
- [ ] **Task 2.4**: Implement Kafka producer (`transit.events.raw`) and OpenAPI-annotated REST controllers (`IngestionController`, `ProposalController`).

### Phase 3: Worker Manager & Dynamic Analysis Workers
- [ ] **Task 3.1**: Scaffold `worker-manager-service` (`:8082`) with `WorkerRegistryService`, `HeartbeatWatchdog`, `RawEventWindowBatcher`, `JobDispatcher`, `WorkloadMetricsTracker`, `RetryAndDlqManager`, and `DynamicWorkerScaler`.
- [ ] **Task 3.2**: Expose OpenAPI-documented REST endpoints on `worker-manager-service` for worker status, latency percentiles (`p50`/`p95`/`p99`), waiting tasks, manual/auto scaling, and DLQ job replay.
- [ ] **Task 3.3**: Scaffold `analysis-worker` (`:8091+`) with registration client, 5-second Kafka heartbeat emitter, `AnalyticalJobConsumer`, and the 3 concurrent processors (`DelayAndPunctualityAnalyzer`, `HeadwayBunchingAnalyzer`, `SpeedAndTelemetryAnomalyAnalyzer`).
- [ ] **Task 3.4**: Verify dynamic scaling and fault recovery: simulate a burst of 500 events, verify Worker Manager scales up workers, kill a worker mid-job, and verify `HeartbeatWatchdog` marks it `DEAD` and `RetryAndDlqManager` retries the task.

### Phase 4: Analytics Results Service
- [ ] **Task 4.1**: Scaffold `analytics-results-service` (`:8083`) connected to `results_db`.
- [ ] **Task 4.2**: Implement `AnalyticalResultsConsumer` consuming `analytics.results` and updating route performance summaries and anomaly logs.
- [ ] **Task 4.3**: Expose OpenAPI-documented REST endpoints (`/api/v1/analytics/overview`, `/routes`, `/anomalies`) and SSE live stream (`/api/v1/analytics/stream`).

### Phase 5: Remote MCP Server & Autonomous AI Agent
- [ ] **Task 5.1**: Scaffold `mcp-agent-service` (`:8084`) with a remotely accessible MCP Server (SSE / HTTP) exposing the 7 tools (`get_transit_anomalies`, `get_route_performance`, `get_worker_cluster_status`, `list_failed_analytical_jobs`, `propose_worker_scaling`, `propose_failed_jobs_replay`, `propose_transit_mitigation`).
- [ ] **Task 5.2**: Implement `AutonomousTransitOpsAgent` that periodically and on-demand invokes the MCP tools, analyzes transit delays and worker bottlenecks, and creates concrete proposals in `core-management-service`.
- [ ] **Task 5.3**: Add OpenAPI endpoints for viewing agent investigation logs and triggering an immediate investigation cycle.

### Phase 6: Web Client Dashboard & End-to-End Docker Integration
- [ ] **Task 6.1**: Build `web-client` (`:3000`) with 4 views:
  1. **Live Ingestion & Sources Control** (TfNSW Sydney focus + TfL, HSL, MTA toggles + "Trigger Traffic Burst" button)
  2. **Worker Manager & Scaling Telemetry** (Live worker nodes, heartbeats, queue depth, p50/p95/p99 latency, scale controls)
  3. **Public Transit Analytics** (Sydney/London/Helsinki/NYC delay leaderboards, bunching alerts, speed anomalies)
  4. **AI Agent & MCP Operations Center** (Live agent reasoning trace, MCP tool calls, and Approve/Reject buttons for proposals)
- [ ] **Task 6.2**: Finalize all Dockerfiles and `docker-compose.yml` so `docker compose up --build` starts the entire distributed platform cleanly.
- [ ] **Task 6.3**: Run full verification suite and export OpenAPI specs to `docs/api-specs/`.

---

## 4. Verification & Grading Acceptance Criteria

To guarantee maximum points at the Stage 2 evaluation (65% of the lab grade):
1. **OpenAPI Compliance**: Every HTTP service (`:8081`, `:8082`, `:8083`, `:8084`, `:8091`) serves a valid OpenAPI 3 specification at `/v3/api-docs` and interactive Swagger UI at `/swagger-ui.html`.
2. **Single Source of Truth Verification**: Inspect `docker-compose.yml` and datasource configs to verify only `core-management-service` connects to `core-postgres`, while `analytics-results-service` and `worker-manager-service` derive state strictly from Kafka topics.
3. **Kafka Distribution & Worker Manager Demo**:
   - Trigger a 1,000-event TfNSW/HSL burst -> observe `transit.events.raw` -> `analytics.jobs` -> Worker Manager detects queue lag and starts additional workers -> workers publish to `analytics.results`.
   - Stop an `analysis-worker` container -> observe Worker Manager detect missed heartbeats within 15s, mark worker `DEAD`, and retry its uncompleted jobs.
4. **Remote MCP + Agentic Task Demo**:
   - Connect any MCP client (or the built-in `AutonomousTransitOpsAgent`) to `http://localhost:8084/mcp/sse`.
   - Trigger an investigation cycle -> verify the agent reads anomalies & worker lag via MCP tools and submits a pending proposal via `propose_worker_scaling` / `propose_transit_mitigation` -> approve proposal in the Web Client and observe the system execute the action.
