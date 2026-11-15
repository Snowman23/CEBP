import React, { useState } from 'react';

/**
 * Member 4 Starter Dashboard — Connects to:
 * - Core Management Service (:8081)
 * - Worker Manager Service (:8082)
 * - Analytics Results Service (:8083)
 * - MCP & Autonomous AI Agent Service (:8084)
 * - Analysis Worker Pool (:8091)
 */
export default function App() {
  const [statusLog, setStatusLog] = useState<string>(
    'Ready. All 4 team workstreams have pre-wired endpoints and standalone dev simulators.'
  );

  async function callEndpoint(label: string, url: string, method: 'GET' | 'POST' = 'POST', body?: unknown) {
    try {
      setStatusLog(`Calling ${label} (${method} ${url})...`);
      const res = await fetch(url, {
        method,
        headers: body ? { 'Content-Type': 'application/json' } : undefined,
        body: body ? JSON.stringify(body) : undefined
      });
      const data = await res.json();
      setStatusLog(`${label} response:\n` + JSON.stringify(data, null, 2));
    } catch (err) {
      setStatusLog(`${label} offline or unreachable (${String(err)}). Start the corresponding service or use Mock Mode!`);
    }
  }

  return (
    <div style={{ maxWidth: 1180, margin: '0 auto', padding: '2rem' }}>
      <header style={{ borderBottom: '1px solid #334155', paddingBottom: '1rem', marginBottom: '1.5rem' }}>
        <h1 style={{ margin: 0, fontSize: '1.6rem' }}>
          CEBP Distributed Public Transit Analytics &amp; Dynamic Worker Management
        </h1>
        <p style={{ color: '#94a3b8', margin: '0.5rem 0 0' }}>
          Initial Scope: <strong>Transport for NSW (Sydney)</strong> + <strong>TfL (London)</strong> +{' '}
          <strong>HSL (Helsinki MQTT)</strong> + <strong>NYC MTA</strong>
        </p>
      </header>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(260px, 1fr))', gap: '1rem' }}>
        <div style={{ background: '#1e293b', padding: '1.25rem', borderRadius: 10 }}>
          <h3 style={{ marginTop: 0 }}>1. Core Management (:8081)</h3>
          <p style={{ fontSize: '0.88rem', color: '#cbd5e1' }}>
            Member 1: Single Source of Truth DB, TfNSW/TfL/HSL Ingestion &amp; Proposals Registry.
          </p>
          <button
            onClick={() => callEndpoint('Simulate Sydney TfNSW Burst', 'http://localhost:8081/api/v1/ingest/simulate-burst?eventCount=50&sydneyOnly=true')}
            style={{ padding: '0.5rem 0.9rem', cursor: 'pointer', borderRadius: 6, border: 'none', background: '#3b82f6', color: '#fff' }}
          >
            Trigger TfNSW Burst (50 events)
          </button>
        </div>

        <div style={{ background: '#1e293b', padding: '1.25rem', borderRadius: 10 }}>
          <h3 style={{ marginTop: 0 }}>2. Worker Manager (:8082)</h3>
          <p style={{ fontSize: '0.88rem', color: '#cbd5e1' }}>
            Member 2: Heartbeat Watchdog, Waiting Tasks, p50/p95/p99 Latency &amp; Autoscaler.
          </p>
          <button
            onClick={() => callEndpoint('Worker Cluster Status', 'http://localhost:8082/api/v1/workers/status', 'GET')}
            style={{ padding: '0.5rem 0.9rem', cursor: 'pointer', borderRadius: 6, border: 'none', background: '#10b981', color: '#fff' }}
          >
            Inspect Worker Cluster
          </button>
        </div>

        <div style={{ background: '#1e293b', padding: '1.25rem', borderRadius: 10 }}>
          <h3 style={{ marginTop: 0 }}>3. Workers (:8091) &amp; Results (:8083)</h3>
          <p style={{ fontSize: '0.88rem', color: '#cbd5e1' }}>
            Member 3: Virtual Worker Pool (Delay/Bunching/Speed) &amp; Analytics Read Projections.
          </p>
          <button
            onClick={() => callEndpoint('Analytics Overview', 'http://localhost:8083/api/v1/analytics/overview', 'GET')}
            style={{ padding: '0.5rem 0.9rem', cursor: 'pointer', borderRadius: 6, border: 'none', background: '#8b5cf6', color: '#fff' }}
          >
            Fetch Transit Analytics
          </button>
        </div>

        <div style={{ background: '#1e293b', padding: '1.25rem', borderRadius: 10 }}>
          <h3 style={{ marginTop: 0 }}>4. Remote MCP &amp; AI Agent (:8084)</h3>
          <p style={{ fontSize: '0.88rem', color: '#cbd5e1' }}>
            Member 4: Remote MCP Server (/mcp/sse) + Autonomous Transit &amp; Ops Agent.
          </p>
          <button
            onClick={() => callEndpoint('Autonomous AI Investigation', 'http://localhost:8084/api/v1/agent/investigate')}
            style={{ padding: '0.5rem 0.9rem', cursor: 'pointer', borderRadius: 6, border: 'none', background: '#f59e0b', color: '#fff' }}
          >
            Run AI Agent Investigation
          </button>
        </div>
      </div>

      <pre
        style={{
          marginTop: '1.5rem',
          background: '#020617',
          border: '1px solid #334155',
          padding: '1rem',
          borderRadius: 8,
          overflowX: 'auto',
          fontSize: '0.85rem'
        }}
      >
        {statusLog}
      </pre>
    </div>
  );
}
