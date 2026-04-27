import { createMemo } from "solid-js";
import { createStore, reconcile } from "solid-js/store";

export interface PodSummary {
  total: number;
  ready: number;
  crashing: number;
}

export interface ExecStats {
  total_runs: number;
  avg_runtime_ms: number;
  max_runtime_ms: number;
  p50_runtime_ms: number;
  p99_runtime_ms: number;
  failures: number;
  fail_rate: number;
  last_run: string;
}

export interface PipelineStage {
  name: string;
  entrypoint: string;
  replicas: number;
  dependsOn: string[];
}

export interface PipelineState {
  id: string;
  mode: "stream" | "once" | "batch";
  status: "Running" | "Enabled" | "Idling" | "Streaming" | "Failed" | "Stopped" | "ZOMBIE" | "PENDING";
  uptime: number;
  cpu: number;
  memory: number;
  progress: number;
  progress_total: number;
  progress_current: number;
  trace_id: string;
  image: string;
  desired_replicas: number;
  last_seen: string;
  last_invocation: string;
  created_at: string;
  records_in: number;
  records_out: number;
  records_failed: number;
  records_filtered: number;
  bytes_processed: number;
  records_per_sec: number;
  records_out_per_sec: number;
  pod_summary: PodSummary;
  exec_stats: ExecStats;
  branch?: string;
  git_repository?: string;
  stages?: PipelineStage[];
  message_bus_type?: string;
  error_message?: string;
  sdk_version?: string;
  latest_sdk_version?: string;
  has_build_config: boolean;
}

const [pipelines, setPipelines] = createStore<Record<string, PipelineState>>({});

// Connection state for UI feedback
let _connectionStatus: "connected" | "disconnected" | "never-connected" = "never-connected";
export const getConnectionStatus = () => _connectionStatus;

const toNumber = (value: unknown): number => {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
};

const normalizeStatus = (status: unknown, phase: unknown): PipelineState["status"] => {
  const raw = String(status || phase || "PENDING").toLowerCase();
  switch (raw) {
    case "running":
      return "Running";
    case "enabled":
      return "Enabled";
    case "idling":
      return "Idling";
    case "streaming":
      return "Streaming";
    case "failed":
      return "Failed";
    case "stopped":
      return "Stopped";
    case "zombie":
      return "ZOMBIE";
    default:
      return "PENDING";
  }
};

export { pipelines, setPipelines };

export const pipelineList = createMemo(() => Object.values(pipelines));
export const pipelineCount = createMemo(() => Object.keys(pipelines).length);

export const connectTelemetryStream = (getEnvironment: () => string = () => "production") => {
  const API_URL = import.meta.env.VITE_API_URL || "http://localhost:3000";
  let consecutiveFailures = 0;

  setInterval(async () => {
    try {
      const env = getEnvironment();
      const resp = await fetch(`${API_URL}/v1/pipelines?environment=${encodeURIComponent(env)}`);
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      
      const data = await resp.json();
      _connectionStatus = "connected";
      consecutiveFailures = 0;
      
      const next: Record<string, PipelineState> = {};
      for (const p of data) {
        const recordsIn = toNumber(p.records_in ?? p.recordsIn);
        const recordsOut = toNumber(p.records_out ?? p.recordsOut);
        const recordsFailed = toNumber(p.records_failed ?? p.recordsFailed);
        const recordsFiltered = toNumber(p.records_filtered ?? p.recordsFiltered);
        const bytesProcessed = toNumber(p.bytes_processed ?? p.bytesProcessed);

        // Parse stages from API response
        const stages: PipelineStage[] | undefined = p.stages
          ? p.stages.map((s: any) => ({
              name: String(s.name || ""),
              entrypoint: String(s.entrypoint || ""),
              replicas: toNumber(s.replicas) || 1,
              dependsOn: Array.isArray(s.dependsOn) ? s.dependsOn.map(String) : [],
            }))
          : undefined;

        next[p.id] = {
          id: p.id,
          mode: p.mode || "stream",
          status: normalizeStatus(p.status, p.phase),
          uptime: toNumber(p.uptime_ms),
          cpu: Math.min(100, Math.max(0, (toNumber(p.cpu_millicores) / 1000) * 100)),
          memory: Math.floor(toNumber(p.memory_bytes) / 1024 / 1024),
          progress: Math.min(100, Math.max(0, toNumber(p.progress_percent))),
          progress_total: toNumber(p.progress_total),
          progress_current: toNumber(p.progress_current),
          trace_id: p.trace_id || "unknown",
          image: p.image || "unknown",
          desired_replicas: toNumber(p.desired_replicas) || 1,
          last_seen: p.last_seen || new Date().toISOString(),
          last_invocation: p.last_invocation || "",
          created_at: p.created_at || "",
          records_in: recordsIn,
          records_out: recordsOut,
          records_failed: recordsFailed,
          records_filtered: recordsFiltered,
          bytes_processed: bytesProcessed,
          records_per_sec: toNumber(p.records_per_sec),
          records_out_per_sec: toNumber(p.records_out_per_sec),
          pod_summary: p.pod_summary || { total: 0, ready: 0, crashing: 0 },
          exec_stats: p.exec_stats || { total_runs: 0, avg_runtime_ms: 0, max_runtime_ms: 0, p50_runtime_ms: 0, p99_runtime_ms: 0, failures: 0, fail_rate: 0, last_run: "" },
          branch: p.branch,
          error_message: p.error_message,
          git_repository: p.git_repository,
          stages,
          message_bus_type: p.message_bus_type || p.messageBusType,
          sdk_version: p.sdk_version || undefined,
          latest_sdk_version: p.latest_sdk_version || undefined,
          has_build_config: !!p.has_build_config,
        };
      }
      setPipelines(reconcile(next));
    } catch (e) {
      consecutiveFailures++;
      _connectionStatus = "disconnected";
      // Only log every 10th failure to reduce console noise
      if (consecutiveFailures <= 1 || consecutiveFailures % 10 === 0) {
        console.warn(`[Clotho] API unreachable (${consecutiveFailures} consecutive failures)`, e);
      }
      // Don't clear existing pipeline data — keep showing stale data with disconnected indicator
    }
  }, 1000);
};
