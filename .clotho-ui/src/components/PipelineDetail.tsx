import { For, Show, createSignal, createResource, onCleanup, createMemo } from "solid-js";
import type { Component } from "solid-js";
import { pipelines, getConnectionStatus } from "../store/pipelines";
import type { PipelineStage } from "../store/pipelines";
import type { Environment } from "./EnvironmentSwitcher";
import PipelineDAGVisualizer, { type StageWithMetrics, type BusConnection, type BranchInfo } from "./PipelineDAGVisualizer";

interface PipelineDetailProps {
  pipelineId: string;
  onBack: () => void;
  environment?: Environment;
}

interface ExecutionRecord {
  id: number;
  pipeline_id: string;
  started_at: string;
  duration_ms: number;
  status: string;
  error_msg: string;
  log_snapshot: string;
  records_in: number;
  records_out: number;
  records_failed: number;
  bytes_processed: number;
}

interface MetricsBucket {
  bucket_ts: string;
  records_in: number;
  records_out: number;
  records_failed: number;
  bytes_processed: number;
  invocations: number;
  avg_duration_ms: number;
  max_duration_ms: number;
}

interface LifecycleEvent {
  id: number;
  event: string;
  version: string;
  message: string;
  timestamp: string;
}

interface DlqRecord {
  id: number;
  trace_id: string;
  error: string;
  step: string;
  payload: string;
  status: string;
  created_at: string;
}

interface DlqGroup {
  error: string;
  step: string;
  count: number;
  first_seen: string;
  last_seen: string;
  sample_id: number;
}

interface StepMetrics {
  pipeline_id: string;
  stage_name: string;
  step_name: string;
  step_type: string;
  records_in: number;
  records_out: number;
  records_failed: number;
  records_filtered: number;
  records_branched: number;
  duration_ms: number;
  timestamp: number;
}

interface DataSample {
  pipeline_id: string;
  stage_name: string;
  step_name: string;
  payload_in: string;
  payload_out: string;
  timestamp: number;
}

interface PodInfo {
  name: string;
  uid: string;
  node: string;
  phase: string;
  pod_ip: string;
  ready: boolean;
  restarts: number;
  start_time: string;
  container_state: string;
  state_detail: string;
  image: string;
}

const API_URL = import.meta.env.VITE_API_URL || "http://localhost:3000";

const toNumber = (value: unknown): number => {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
};

const PipelineDetail: Component<PipelineDetailProps> = (props) => {
  const envQ = () => `environment=${encodeURIComponent(props.environment || 'production')}`;
  const [expandedPod, setExpandedPod] = createSignal<string | null>(null);
  const [podLogs, setPodLogs] = createSignal<Record<string, string>>({});
  const [loadingLogs, setLoadingLogs] = createSignal<Record<string, boolean>>({});
  const [actionLoading, setActionLoading] = createSignal("");
  const [actionError, setActionError] = createSignal("");
  const [confirmDelete, setConfirmDelete] = createSignal(false);
  const [dlqPage, setDlqPage] = createSignal(0);
  const DLQ_PAGE_SIZE = 10;
  const [canaryLoading, setCanaryLoading] = createSignal<string | null>(null);
  const [canaryResult, setCanaryResult] = createSignal<{ key: string; status: string; message: string } | null>(null);
  const [rebuildTriggered, setRebuildTriggered] = createSignal(false);

  const pipeline = () => pipelines[props.pipelineId];
  const storeEmpty = () => Object.keys(pipelines).length === 0;
  const storeKeys = () => Object.keys(pipelines);

  const [pods, { refetch: refetchPods }] = createResource(
    () => props.pipelineId,
    async (id) => {
      try {
        const resp = await fetch(`${API_URL}/v1/pipelines/${id}/pods?${envQ()}`);
        if (!resp.ok) return [];
        return (await resp.json()) as PodInfo[];
      } catch {
        return [];
      }
    }
  );

  const [events] = createResource(
    () => props.pipelineId,
    async (id) => {
      try {
        const resp = await fetch(`${API_URL}/v1/pipelines/${id}/events?${envQ()}`);
        if (!resp.ok) return [];
        return await resp.json();
      } catch {
        return [];
      }
    }
  );

  const isStream = () => pipeline()?.mode === "stream";
  const isDag = () => (pipeline()?.stages?.length ?? 0) > 0;

  // Step metrics - MUST be declared BEFORE dagStages memo that uses it
  const [stepMetricsList, { refetch: refetchStepMetrics }] = createResource(
    () => props.pipelineId,
    async (id) => {
      try {
        const resp = await fetch(`${API_URL}/v1/pipelines/${id}/steps/metrics?${envQ()}`);
        if (!resp.ok) return [];
        return await resp.json() as StepMetrics[];
      } catch (err) {
        return [];
      }
    }
  );

  // Build DAG stages from pipeline spec (metrics added separately to avoid circular deps)
  const dagStages = createMemo((): StageWithMetrics[] => {
    const p = pipeline();
    if (!p) return [];
    
    // For simple pipelines without explicit stages, generate a synthetic single-stage view
    if (!p.stages || p.stages.length === 0) {
      const metricsList = stepMetricsList() || [];
      const statusMap: Record<string, "running" | "pending" | "failed" | "completed"> = {
        "Running": "running", "Streaming": "running", "Enabled": "running",
        "Failed": "failed", "ZOMBIE": "failed"
      };
      return [{
        name: props.pipelineId,
        entrypoint: "main",
        replicas: p.desired_replicas || 1,
        dependsOn: [],
        status: statusMap[p.status] || "completed",
        metrics: (p.records_in > 0 || p.records_out > 0) ? {
          recordsIn: p.records_in,
          recordsOut: p.records_out,
          recordsFailed: p.records_failed,
          recordsBranched: p.records_filtered || 0,
          throughputPerSec: p.records_per_sec || 0,
          lagMs: 0,
        } : undefined,
        steps: metricsList.length > 0
          ? metricsList.map((sm: StepMetrics, idx: number) => ({
              name: sm.step_name || `step_${idx}`,
              stepType: (sm.step_type as any) || "transform",
              metrics: {
                recordsIn: sm.records_in,
                recordsOut: sm.records_out,
                recordsFailed: sm.records_failed,
                recordsBranched: sm.records_branched + sm.records_filtered,
                throughputPerSec: 0,
                lagMs: sm.duration_ms,
              }
            }))
          : [
              { name: "source", stepType: "source" as const, metrics: undefined },
              { name: "sink", stepType: "sink" as const, metrics: undefined },
            ]
      }];
    }
    
    return p.stages.map((stage: PipelineStage) => {
      // Calculate running status from pods only
      const stagePods = pods()?.filter(pod => 
        pod.name.includes(`-${stage.name}-`) || 
        pod.name.includes(`-${stage.name}`)
      ) || [];
      
      const runningPods = stagePods.filter(pod => pod.phase === "Running").length;
      
      const metricsList = stepMetricsList() || [];
      const stageStepMetrics = metricsList.filter(m => m.stage_name === stage.name || m.stage_name === "");
      
      // Calculate aggregate metrics for the stage
      let recordsIn = 0;
      let recordsOut = 0;
      let recordsFailed = 0;
      let recordsBranched = 0;
      
      // If there are steps, we can find the source/sink step bounds
      if (stageStepMetrics.length > 0) {
          recordsIn = Math.max(...stageStepMetrics.map(m => m.records_in));
          recordsOut = Math.max(...stageStepMetrics.map(m => m.records_out));
          recordsFailed = stageStepMetrics.reduce((acc, m) => acc + m.records_failed, 0);
          recordsBranched = stageStepMetrics.reduce((acc, m) => acc + m.records_branched + m.records_filtered, 0);
      }

      return {
        ...stage,
        status: runningPods > 0 ? "running" : p.status === "Running" ? "pending" : "completed",
        metrics: {
          recordsIn,
          recordsOut,
          recordsFailed,
          recordsBranched,
          throughputPerSec: 0,
          lagMs: 0,
        },
        steps: stageStepMetrics.length > 0
          ? stageStepMetrics.map((sm, idx) => ({
              name: sm.step_name || `step_${idx}`,
              stepType: (sm.step_type as any) || "transform",
              metrics: {
                recordsIn: sm.records_in,
                recordsOut: sm.records_out,
                recordsFailed: sm.records_failed,
                recordsBranched: sm.records_branched + sm.records_filtered,
                throughputPerSec: 0,
                lagMs: sm.duration_ms,
              }
            }))
          : [
              { name: stage.entrypoint || "source", stepType: "source" as const, metrics: undefined },
              { name: "sink", stepType: "sink" as const, metrics: undefined },
            ]
      };
    });
  });

  // Build bus connections from stage dependencies (no metrics to avoid circular deps)
  const dagConnections = createMemo((): BusConnection[] => {
    const stages = pipeline()?.stages || [];
    const connections: BusConnection[] = [];
    
    stages.forEach((stage: PipelineStage) => {
      if (stage.dependsOn) {
        stage.dependsOn.forEach((depName: string) => {
          connections.push({
            from: depName,
            to: stage.name,
            throughput: 0,
            lagMs: 0,
            pending: 0
          });
        });
      }
    });
    
    return connections;
  });

  const [executions, { refetch: refetchExecs }] = createResource(
    () => ({ id: props.pipelineId, stream: isStream() }),
    async ({ id, stream }) => {
      if (stream) return []; // Stream pipelines don't use execution history
      try {
        const resp = await fetch(`${API_URL}/v1/pipelines/${id}/executions?limit=50&${envQ()}`);
        if (!resp.ok) return [];
        const rows = await resp.json();
        return (rows as any[]).map((r) => ({
          id: toNumber(r.id),
          pipeline_id: String(r.pipeline_id || r.pipelineId || ""),
          started_at: String(r.started_at || r.startedAt || ""),
          duration_ms: toNumber(r.duration_ms ?? r.durationMs),
          status: String(r.status || "unknown"),
          error_msg: String(r.error_msg || r.errorMsg || ""),
          log_snapshot: String(r.log_snapshot || r.logSnapshot || ""),
          records_in: toNumber(r.records_in ?? r.recordsIn),
          records_out: toNumber(r.records_out ?? r.recordsOut),
          records_failed: toNumber(r.records_failed ?? r.recordsFailed),
          bytes_processed: toNumber(r.bytes_processed ?? r.bytesProcessed),
        })) as ExecutionRecord[];
      } catch {
        return [];
      }
    }
  );

  // Stream-only resources
  const [metricsBuckets, { refetch: refetchMetrics }] = createResource(
    () => ({ id: props.pipelineId, stream: isStream() }),
    async ({ id, stream }) => {
      if (!stream) return [];
      try {
        const resp = await fetch(`${API_URL}/v1/pipelines/${id}/metrics?minutes=60&${envQ()}`);
        if (!resp.ok) return [];
        return (await resp.json()) as MetricsBucket[];
      } catch {
        return [];
      }
    }
  );

  const [lifecycleEvents, { refetch: refetchLifecycle }] = createResource(
    () => ({ id: props.pipelineId, stream: isStream() }),
    async ({ id, stream }) => {
      if (!stream) return [];
      try {
        const resp = await fetch(`${API_URL}/v1/pipelines/${id}/lifecycle?limit=100&${envQ()}`);
        if (!resp.ok) return [];
        return (await resp.json()) as LifecycleEvent[];
      } catch {
        return [];
      }
    }
  );

  const [dlqGroups, { refetch: refetchDlq }] = createResource(
    () => ({ id: props.pipelineId, stream: isStream() }),
    async ({ id, stream }) => {
      if (!stream) return { groups: [] as DlqGroup[], total_pending: 0 };
      try {
        // Add timeout to prevent hanging on data proxy issues
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 5000);
        const resp = await fetch(`${API_URL}/v1/pipelines/${id}/dlq/groups?${envQ()}`, {
          signal: controller.signal
        });
        clearTimeout(timeout);
        if (!resp.ok) return { groups: [] as DlqGroup[], total_pending: 0 };
        return await resp.json() as { groups: DlqGroup[]; total_pending: number };
      } catch (err) {
        // Silently fail on timeout or network error - don't break the UI
        console.warn("DLQ fetch failed (data proxy may be unavailable):", err);
        return { groups: [] as DlqGroup[], total_pending: 0 };
      }
    }
  );

  // Data samples
  const [dataSamplesMap, { refetch: refetchDataSamples }] = createResource(
    () => props.pipelineId,
    async (id) => {
      try {
        const resp = await fetch(`${API_URL}/v1/pipelines/${id}/steps/samples?${envQ()}`);
        if (!resp.ok) return {} as Record<string, DataSample>;
        return await resp.json() as Record<string, DataSample>;
      } catch (err) {
        return {} as Record<string, DataSample>;
      }
    }
  );

  // Build branch info from DLQ groups (must be defined AFTER dlqGroups resource)
  const dagBranches = createMemo((): BranchInfo[] => {
    const p = pipeline();
    if (!p) return [];
    
    const groups = dlqGroups()?.groups || [];
    const branches: BranchInfo[] = [];
    const stageErrors = new Map<string, Map<string, number>>();
    
    groups.forEach((group: DlqGroup) => {
      if (!stageErrors.has(group.step)) {
        stageErrors.set(group.step, new Map());
      }
      stageErrors.get(group.step)!.set(group.error, group.count);
    });
    
    stageErrors.forEach((errors, stageName) => {
      let rejectedCount = 0;
      let condition = "filter";
      
      errors.forEach((count, error) => {
        const errorLower = error.toLowerCase();
        if (errorLower.includes("reject") || errorLower.includes("filter") || 
            errorLower.includes("skip") || errorLower.includes("drop")) {
          rejectedCount += count;
          condition = errorLower.includes("sieve") ? "AI sieve" : 
                     errorLower.includes("classifier") ? "classifier" : "filter";
        }
      });
      
      if (rejectedCount > 0) {
        const stageMetrics = dagStages().find(s => s.name === stageName)?.metrics;
        const totalOut = stageMetrics?.recordsOut || 0;
        
        branches.push({
          stage: stageName,
          condition,
          rejectedCount,
          acceptedCount: Math.max(0, totalOut)
        });
      }
    });
    
    return branches;
  });

  const [expandedExec, setExpandedExec] = createSignal<number | null>(null);

  // Auto-refresh pods + mode-appropriate data every 5s
  const podInterval = setInterval(() => {
    refetchPods();
    refetchStepMetrics(); refetchDataSamples();
    if (isStream()) {
      refetchMetrics(); refetchLifecycle(); refetchDlq();
    } else {
      refetchExecs();
    }
  }, 5000);
  onCleanup(() => clearInterval(podInterval));

  const formatDuration = (ms: number) => {
    if (ms <= 0) return "—";
    if (ms < 1) return `${(ms * 1000).toFixed(0)}µs`;
    if (ms < 1000) return `${ms.toFixed(1)}ms`;
    if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
    return `${Math.floor(ms / 60000)}m ${Math.floor((ms % 60000) / 1000)}s`;
  };

  const togglePodLogs = async (podName: string) => {
    if (expandedPod() === podName) {
      setExpandedPod(null);
      return;
    }

    setExpandedPod(podName);

    if (!podLogs()[podName]) {
      setLoadingLogs(prev => ({ ...prev, [podName]: true }));
      try {
        const resp = await fetch(`${API_URL}/v1/pods/${podName}/logs?tail=200&${envQ()}`);
        if (resp.ok) {
          const data = await resp.json();
          const reversed = (data.logs || "").split("\n").reverse().join("\n");
          setPodLogs(prev => ({ ...prev, [podName]: reversed || "No logs available" }));
        } else {
          setPodLogs(prev => ({ ...prev, [podName]: "Failed to fetch logs" }));
        }
      } catch {
        setPodLogs(prev => ({ ...prev, [podName]: "Error fetching logs" }));
      }
      setLoadingLogs(prev => ({ ...prev, [podName]: false }));
    }
  };

  const refreshLogs = async (podName: string) => {
    setLoadingLogs(prev => ({ ...prev, [podName]: true }));
    try {
      const resp = await fetch(`${API_URL}/v1/pods/${podName}/logs?tail=200`);
      if (resp.ok) {
        const data = await resp.json();
        const reversed = (data.logs || "").split("\n").reverse().join("\n");
        setPodLogs(prev => ({ ...prev, [podName]: reversed || "No logs available" }));
      }
    } catch { /* silent */ }
    setLoadingLogs(prev => ({ ...prev, [podName]: false }));
  };

  const runAction = async (name: string, url: string, method: string = "POST") => {
    setActionLoading(name);
    setActionError("");
    try {
      const resp = await fetch(url, { method });
      if (!resp.ok) {
        const body = await resp.json().catch(() => ({ error: `HTTP ${resp.status}` }));
        setActionError(`${name}: ${body.error || resp.statusText}`);
      }
      return resp.ok;
    } catch (e: any) {
      setActionError(`${name}: ${e.message || "network error"}`);
      return false;
    } finally {
      setActionLoading("");
    }
  };

  const handlePause = () => runAction("pause", `${API_URL}/v1/pipelines/${props.pipelineId}/pause?${envQ()}`);
  const handleResume = () => runAction("resume", `${API_URL}/v1/pipelines/${props.pipelineId}/resume?${envQ()}`);
  const handleRestart = () => runAction("restart", `${API_URL}/v1/pipelines/${props.pipelineId}/restart?${envQ()}`);

  const handleRebuild = async () => {
    const ok = await runAction("rebuild", `${API_URL}/v1/pipelines/${props.pipelineId}/rebuild?${envQ()}`);
    if (ok) setRebuildTriggered(true);
  };

  const sdkUpdateAvailable = () => {
    const p = pipeline();
    if (!p?.sdk_version || !p?.latest_sdk_version) return false;
    return p.sdk_version !== p.latest_sdk_version;
  };

  const handleDelete = async () => {
    const ok = await runAction("delete", `${API_URL}/v1/pipelines/${props.pipelineId}?${envQ()}`, "DELETE");
    if (ok) props.onBack();
    setConfirmDelete(false);
  };

  const handleCanaryTest = async (group: DlqGroup) => {
    const key = `${group.error}::${group.step}`;
    setCanaryLoading(key);
    setCanaryResult(null);
    try {
      const resp = await fetch(`${API_URL}/v1/pipelines/${props.pipelineId}/dlq/canary?${envQ()}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ error: group.error, step: group.step }),
      });
      const data = await resp.json();
      setCanaryResult({ key, status: data.status, message: data.message || data.error || "Unknown" });
    } catch (e: any) {
      setCanaryResult({ key, status: "error", message: e.message || "Network error" });
    }
    setCanaryLoading(null);
  };

  const handleReplayGroup = async (group: DlqGroup) => {
    try {
      await fetch(`${API_URL}/v1/pipelines/${props.pipelineId}/dlq/replay-group?${envQ()}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ error: group.error, step: group.step }),
      });
      refetchDlq();
    } catch { /* silent */ }
  };

  const handleDismissGroup = async (group: DlqGroup) => {
    try {
      await fetch(`${API_URL}/v1/pipelines/${props.pipelineId}/dlq/dismiss-group?${envQ()}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ error: group.error, step: group.step }),
      });
      refetchDlq();
    } catch { /* silent */ }
  };

  const statusColor = (status: string) => {
    switch (status) {
      case "Running": return "bg-green-500/20 text-green-400 border-green-500/30";
      case "Streaming": return "bg-green-500/20 text-green-400 border-green-500/30";
      case "Enabled": return "bg-blue-500/20 text-blue-400 border-blue-500/30";
      case "Idling": return "bg-slate-500/20 text-slate-400 border-slate-500/30";
      case "Failed": return "bg-red-500/20 text-red-400 border-red-500/30";
      case "ZOMBIE": return "bg-yellow-500/20 text-yellow-400 border-yellow-500/30";
      default: return "bg-slate-500/20 text-slate-400 border-slate-500/30";
    }
  };

  const statusDot = (status: string) => {
    switch (status) {
      case "Running": return "bg-green-500";
      case "Streaming": return "bg-green-500";
      case "Enabled": return "bg-blue-500";
      case "Idling": return "bg-slate-400";
      case "Failed": return "bg-red-500";
      case "ZOMBIE": return "bg-yellow-500";
      default: return "bg-slate-500";
    }
  };

  const podPhaseColor = (phase: string) => {
    switch (phase) {
      case "Running": return "text-green-400";
      case "Succeeded": return "text-blue-400";
      case "Failed": return "text-red-400";
      case "Pending": return "text-yellow-400";
      default: return "text-slate-400";
    }
  };

  const formatRelativeTime = (timestamp: string) => {
    if (!timestamp) return "—";
    const now = new Date().getTime();
    const then = new Date(timestamp).getTime();
    const diff = now - then;
    if (diff < 60000) return `${Math.floor(diff / 1000)}s ago`;
    if (diff < 3600000) return `${Math.floor(diff / 60000)}m ago`;
    if (diff < 86400000) return `${Math.floor(diff / 3600000)}h ago`;
    return `${Math.floor(diff / 86400000)}d ago`;
  };

  const formatBytes = (bytes: number) => {
    if (bytes <= 0) return "0 B";
    const units = ["B", "KB", "MB", "GB", "TB"];
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    return `${(bytes / Math.pow(1024, i)).toFixed(i > 0 ? 1 : 0)} ${units[i]}`;
  };

  const isPaused = () => {
    const p = pipeline();
    return p && p.desired_replicas === 0;
  };

  return (
    <div class="space-y-6 pb-20">
      {/* Header */}
      <div class="flex items-center justify-between">
        <div class="flex items-center gap-4">
          <button
            onClick={props.onBack}
            class="p-1.5 rounded-md text-slate-400 hover:text-white hover:bg-slate-800 transition-all"
          >
            <svg class="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor">
              <path stroke-linecap="round" stroke-linejoin="round" d="M10.5 19.5L3 12m0 0l7.5-7.5M3 12h18" />
            </svg>
          </button>
          <div class="flex items-center gap-3">
            <div class={`w-3 h-3 rounded-full ${statusDot(pipeline()?.status || "")} ${pipeline()?.status === "Running" ? "animate-pulse-glow" : ""}`} />
            <h1 class="text-xl font-mono font-bold text-white">{props.pipelineId}</h1>
            <Show when={pipeline()}>
              <span class={`px-2.5 py-1 rounded text-xs font-semibold border ${statusColor(pipeline()!.status)}`}>
                {pipeline()!.status.toUpperCase()}
              </span>
              <span class="px-2 py-1 rounded text-[10px] font-semibold font-mono uppercase tracking-wider bg-slate-700/50 text-slate-400 border border-slate-600/50">
                {pipeline()!.mode}
              </span>
              <Show when={pipeline()!.sdk_version}>
                <span class={`flex items-center gap-1 px-2 py-1 rounded text-[10px] font-semibold font-mono border ${
                  sdkUpdateAvailable()
                    ? "bg-amber-500/10 text-amber-400 border-amber-500/30"
                    : "bg-slate-700/50 text-slate-400 border-slate-600/50"
                }`}>
                  <svg class="w-3 h-3" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor">
                    <path stroke-linecap="round" stroke-linejoin="round" d="M17.25 6.75L22.5 12l-5.25 5.25m-10.5 0L1.5 12l5.25-5.25m7.5-3l-4.5 16.5" />
                  </svg>
                  SDK v{pipeline()!.sdk_version}
                  <Show when={sdkUpdateAvailable()}>
                    <span class="text-amber-500">→ v{pipeline()!.latest_sdk_version}</span>
                  </Show>
                </span>
              </Show>
            </Show>
          </div>
        </div>

        {/* Action Buttons */}
        <div class="flex items-center gap-2">
          <Show when={isPaused()}>
            <button
              onClick={handleResume}
              disabled={!!actionLoading()}
              class="flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-semibold bg-green-500/10 text-green-400 border border-green-500/20 hover:bg-green-500/20 transition-all disabled:opacity-50"
            >
              <svg class="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" d="M5.25 5.653c0-.856.917-1.398 1.667-.986l11.54 6.348a1.125 1.125 0 010 1.971l-11.54 6.347a1.125 1.125 0 01-1.667-.985V5.653z" />
              </svg>
              {actionLoading() === "resume" ? "Starting..." : "Start"}
            </button>
          </Show>
          <Show when={!isPaused()}>
            <button
              onClick={handlePause}
              disabled={!!actionLoading()}
              class="flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-semibold bg-yellow-500/10 text-yellow-400 border border-yellow-500/20 hover:bg-yellow-500/20 transition-all disabled:opacity-50"
            >
              <svg class="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" d="M15.75 5.25v13.5m-7.5-13.5v13.5" />
              </svg>
              {actionLoading() === "pause" ? "Pausing..." : "Pause"}
            </button>
          </Show>
          <button
            onClick={handleRestart}
            disabled={!!actionLoading()}
            class="flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-semibold bg-blue-500/10 text-blue-400 border border-blue-500/20 hover:bg-blue-500/20 transition-all disabled:opacity-50"
          >
            <svg class="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor">
              <path stroke-linecap="round" stroke-linejoin="round" d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.993 0l3.181 3.183a8.25 8.25 0 0013.803-3.7M4.031 9.865a8.25 8.25 0 0113.803-3.7l3.181 3.182" />
            </svg>
            {actionLoading() === "restart" ? "Restarting..." : "Restart"}
          </button>

          <Show when={pipeline()?.has_build_config}>
            <Show
              when={!rebuildTriggered()}
              fallback={
                <span class="flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-semibold bg-green-500/10 text-green-400 border border-green-500/20">
                  <svg class="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor">
                    <path stroke-linecap="round" stroke-linejoin="round" d="M4.5 12.75l6 6 9-13.5" />
                  </svg>
                  Build Queued
                </span>
              }
            >
              <button
                onClick={handleRebuild}
                disabled={!!actionLoading()}
                class={`flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-semibold transition-all disabled:opacity-50 ${
                  sdkUpdateAvailable()
                    ? "bg-amber-500/10 text-amber-400 border border-amber-500/30 hover:bg-amber-500/20"
                    : "bg-slate-700/50 text-slate-400 border border-slate-600/50 hover:bg-slate-700"
                }`}
              >
                <svg class="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor">
                  <path stroke-linecap="round" stroke-linejoin="round" d="M3.75 13.5l10.5-11.25L12 10.5h8.25L9.75 21.75 12 13.5H3.75z" />
                </svg>
                {actionLoading() === "rebuild"
                  ? "Triggering..."
                  : sdkUpdateAvailable()
                    ? "Update SDK"
                    : "Rebuild"}
              </button>
            </Show>
          </Show>

          <Show when={!confirmDelete()}>
            <button
              onClick={() => setConfirmDelete(true)}
              disabled={!!actionLoading()}
              class="flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-semibold bg-red-500/10 text-red-400 border border-red-500/20 hover:bg-red-500/20 transition-all disabled:opacity-50"
            >
              <svg class="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" d="M14.74 9l-.346 9m-4.788 0L9.26 9m9.968-3.21c.342.052.682.107 1.022.166m-1.022-.165L18.16 19.673a2.25 2.25 0 01-2.244 2.077H8.084a2.25 2.25 0 01-2.244-2.077L4.772 5.79m14.456 0a48.108 48.108 0 00-3.478-.397m-12 .562c.34-.059.68-.114 1.022-.165m0 0a48.11 48.11 0 013.478-.397m7.5 0v-.916c0-1.18-.91-2.164-2.09-2.201a51.964 51.964 0 00-3.32 0c-1.18.037-2.09 1.022-2.09 2.201v.916m7.5 0a48.667 48.667 0 00-7.5 0" />
              </svg>
              Delete
            </button>
          </Show>
          <Show when={confirmDelete()}>
            <div class="flex items-center gap-2 px-3 py-1.5 rounded-md bg-red-500/20 border border-red-500/30">
              <span class="text-xs text-red-400 font-semibold">Confirm?</span>
              <button
                onClick={handleDelete}
                class="px-2 py-0.5 rounded text-xs font-bold bg-red-500 text-white hover:bg-red-600 transition-all"
              >
                Yes, Delete
              </button>
              <button
                onClick={() => setConfirmDelete(false)}
                class="px-2 py-0.5 rounded text-xs font-semibold text-slate-400 hover:text-white transition-all"
              >
                Cancel
              </button>
            </div>
          </Show>
        </div>
      </div>

      {/* Connection Status Banner */}
      <Show when={getConnectionStatus() === "disconnected"}>
        <div class="flex items-center gap-2 px-4 py-2 rounded-lg bg-amber-500/10 border border-amber-500/20">
          <div class="w-2 h-2 rounded-full bg-amber-500 animate-pulse" />
          <span class="text-xs text-amber-400 font-semibold">API Disconnected</span>
          <span class="text-xs text-slate-500">— showing cached data. Retrying...</span>
        </div>
      </Show>

      {/* Action Error Banner */}
      <Show when={actionError()}>
        <div class="flex items-center gap-2 px-4 py-2 rounded-lg bg-red-500/10 border border-red-500/20">
          <svg class="w-4 h-4 text-red-400 shrink-0" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" d="M12 9v3.75m9-.75a9 9 0 11-18 0 9 9 0 0118 0zm-9 3.75h.008v.008H12v-.008z" />
          </svg>
          <span class="text-xs text-red-400 font-mono">{actionError()}</span>
          <button onClick={() => setActionError("")} class="ml-auto text-xs text-slate-500 hover:text-white">dismiss</button>
        </div>
      </Show>

      {/* Pipeline Error Banner */}
      <Show when={pipeline()?.status === "Failed" && pipeline()?.error_message}>
        <div class="flex items-start gap-3 px-4 py-3 rounded-lg bg-red-500/10 border border-red-500/20">
          <svg class="w-5 h-5 text-red-400 shrink-0 mt-0.5" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
          </svg>
          <div>
            <h3 class="text-sm font-semibold text-red-400 mb-1">Workload Error</h3>
            <p class="text-xs text-red-300 font-mono break-all">{pipeline()?.error_message}</p>
          </div>
        </div>
      </Show>

      {/* Info Grid */}
      <Show when={pipeline()}>
        {(p) => (
          <div class="grid grid-cols-2 lg:grid-cols-4 gap-4">
            <InfoCard label="Image" value={p().image} mono />
            <InfoCard label="Replicas" value={`${p().desired_replicas}`} />
            <InfoCard label="CPU" value={`${p().cpu.toFixed(1)}%`} color="text-blue-400" />
            <InfoCard label="Memory" value={`${p().memory} MB`} color="text-purple-400" />
            <InfoCard label="Created" value={formatRelativeTime(p().created_at)} />
            <InfoCard label="Last Seen" value={formatRelativeTime(p().last_seen)} />
            <InfoCard label="Progress" value={p().progress_total > 0 ? `${p().progress_current} / ${p().progress_total}` : "—"} />
            <InfoCard label="Pods" value={p().pod_summary ? `${p().pod_summary.ready}/${p().pod_summary.total} ready` : "—"} />
          </div>
        )}
      </Show>

      {/* Data Throughput */}
      <Show when={pipeline()}>
        {(p) => (
            <div>
              <h2 class="text-sm font-semibold text-slate-300 uppercase tracking-wider mb-3">
                Data Throughput
              </h2>
              <div class="grid grid-cols-2 lg:grid-cols-7 gap-4">
                <div class="bg-slate-800/50 rounded-lg border border-slate-700/50 px-4 py-3">
                  <div class="text-[10px] text-slate-500 uppercase tracking-wider font-semibold mb-1">Records In</div>
                  <div class="text-lg font-bold font-mono text-cyan-400">{p().records_in.toLocaleString()}</div>
                </div>
                <div class="bg-slate-800/50 rounded-lg border border-slate-700/50 px-4 py-3">
                  <div class="text-[10px] text-slate-500 uppercase tracking-wider font-semibold mb-1">Records Out</div>
                  <div class="text-lg font-bold font-mono text-green-400">{p().records_out.toLocaleString()}</div>
                </div>
                <div class="bg-slate-800/50 rounded-lg border border-slate-700/50 px-4 py-3">
                  <div class="text-[10px] text-slate-500 uppercase tracking-wider font-semibold mb-1">Records Failed</div>
                  <div class={`text-lg font-bold font-mono ${p().records_failed > 0 ? "text-red-400" : "text-slate-500"}`}>
                    {p().records_failed.toLocaleString()}
                  </div>
                </div>
                <div class="bg-slate-800/50 rounded-lg border border-slate-700/50 px-4 py-3">
                  <div class="text-[10px] text-slate-500 uppercase tracking-wider font-semibold mb-1">Filtered</div>
                  <div class="text-lg font-bold font-mono text-slate-400">{p().records_filtered.toLocaleString()}</div>
                </div>
                <div class="bg-slate-800/50 rounded-lg border border-slate-700/50 px-4 py-3">
                  <div class="text-[10px] text-slate-500 uppercase tracking-wider font-semibold mb-1">Bytes Processed</div>
                  <div class="text-lg font-bold font-mono text-purple-400">{formatBytes(p().bytes_processed)}</div>
                </div>
                <div class="bg-slate-800/50 rounded-lg border border-slate-700/50 px-4 py-3">
                  <div class="text-[10px] text-slate-500 uppercase tracking-wider font-semibold mb-1">In / sec</div>
                  <div class="text-lg font-bold font-mono text-cyan-400">
                    {p().records_per_sec > 0 ? p().records_per_sec.toFixed(1) : "—"}
                  </div>
                </div>
                <div class="bg-slate-800/50 rounded-lg border border-slate-700/50 px-4 py-3">
                  <div class="text-[10px] text-slate-500 uppercase tracking-wider font-semibold mb-1">Out / sec</div>
                  <div class="text-lg font-bold font-mono text-green-400">
                    {p().records_out_per_sec > 0 ? p().records_out_per_sec.toFixed(1) : "—"}
                  </div>
                </div>
              </div>
              <Show when={p().records_in > 0}>
                <div class="mt-3 flex items-center gap-4">
                  <div class="flex-1 bg-slate-800 rounded-full h-2 overflow-hidden">
                    <div
                      class="h-full bg-gradient-to-r from-green-500 to-green-400 rounded-full transition-all"
                      style={{ width: `${Math.min(100, (p().records_out / p().records_in) * 100)}%` }}
                    />
                  </div>
                  <span class="text-xs text-slate-400 font-mono whitespace-nowrap">
                    {((p().records_out / p().records_in) * 100).toFixed(1)}% yield
                  </span>
                </div>
              </Show>
            </div>
        )}
      </Show>

      {/* DAG Topology Visualizer - Shows stage flow, metrics, and branching */}
      <PipelineDAGVisualizer
        stages={dagStages()}
        busConnections={dagConnections()}
        branches={dagBranches()}
        pipelineMode={pipeline()?.mode || "unknown"}
        dataSamplesMap={dataSamplesMap() || {}}
      />

      {/* Pods Section */}
      <div>
        <h2 class="text-sm font-semibold text-slate-300 uppercase tracking-wider mb-3">
          Pods
          <span class="ml-2 text-slate-500 font-normal normal-case">
            ({pods()?.length ?? 0})
          </span>
        </h2>

        <div class="bg-slate-800/50 rounded-lg border border-slate-700 overflow-hidden">
          <Show when={pods() && pods()!.length > 0} fallback={
            <div class="px-6 py-8 text-center">
              <p class="text-xs text-slate-500">No pods found for this pipeline</p>
            </div>
          }>
            <table class="w-full">
              <thead class="bg-slate-900/50 border-b border-slate-700">
                <tr>
                  <th class="px-4 py-2.5 text-left text-xs font-semibold text-slate-400 uppercase tracking-wider w-8"></th>
                  <th class="px-4 py-2.5 text-left text-xs font-semibold text-slate-400 uppercase tracking-wider">Pod</th>
                  <th class="px-4 py-2.5 text-left text-xs font-semibold text-slate-400 uppercase tracking-wider">Phase</th>
                  <th class="px-4 py-2.5 text-left text-xs font-semibold text-slate-400 uppercase tracking-wider">Node</th>
                  <th class="px-4 py-2.5 text-left text-xs font-semibold text-slate-400 uppercase tracking-wider">IP</th>
                  <th class="px-4 py-2.5 text-right text-xs font-semibold text-slate-400 uppercase tracking-wider">Restarts</th>
                  <th class="px-4 py-2.5 text-left text-xs font-semibold text-slate-400 uppercase tracking-wider">State</th>
                  <th class="px-4 py-2.5 text-left text-xs font-semibold text-slate-400 uppercase tracking-wider">Started</th>
                </tr>
              </thead>
              <tbody class="divide-y divide-slate-700/50">
                <For each={pods()}>
                  {(pod) => (
                    <>
                      <tr
                        class="hover:bg-slate-700/30 transition-colors cursor-pointer"
                        onClick={() => togglePodLogs(pod.name)}
                      >
                        <td class="px-4 py-2.5">
                          <svg
                            class={`w-3.5 h-3.5 text-slate-500 transition-transform ${expandedPod() === pod.name ? "rotate-90" : ""}`}
                            fill="none" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor"
                          >
                            <path stroke-linecap="round" stroke-linejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
                          </svg>
                        </td>
                        <td class="px-4 py-2.5">
                          <span class="font-mono text-xs text-white">{pod.name}</span>
                        </td>
                        <td class="px-4 py-2.5">
                          <span class={`text-xs font-semibold ${podPhaseColor(pod.phase)}`}>
                            {pod.phase}
                          </span>
                        </td>
                        <td class="px-4 py-2.5">
                          <span class="text-xs text-slate-400 font-mono">{pod.node || "—"}</span>
                        </td>
                        <td class="px-4 py-2.5">
                          <span class="text-xs text-slate-400 font-mono">{pod.pod_ip || "—"}</span>
                        </td>
                        <td class="px-4 py-2.5 text-right">
                          <span class={`text-xs font-mono ${pod.restarts > 3 ? "text-red-400" : "text-slate-400"}`}>
                            {pod.restarts}
                          </span>
                        </td>
                        <td class="px-4 py-2.5">
                          <span class="text-xs text-slate-400">{pod.container_state}</span>
                        </td>
                        <td class="px-4 py-2.5">
                          <span class="text-xs text-slate-500">{formatRelativeTime(pod.start_time)}</span>
                        </td>
                      </tr>
                      {/* Collapsible Log Panel */}
                      <Show when={expandedPod() === pod.name}>
                        <tr>
                          <td colspan={8} class="p-0">
                            <div class="bg-slate-950 border-t border-slate-800">
                              <div class="flex items-center justify-between px-4 py-2 border-b border-slate-800">
                                <span class="text-[10px] text-slate-500 uppercase tracking-wider font-semibold">
                                  Pod Logs — {pod.name}
                                </span>
                                <button
                                  onClick={(e) => { e.stopPropagation(); refreshLogs(pod.name); }}
                                  class="text-[10px] text-blue-400 hover:text-blue-300 font-semibold uppercase tracking-wider"
                                >
                                  {loadingLogs()[pod.name] ? "Loading..." : "Refresh"}
                                </button>
                              </div>
                              <div class="max-h-72 overflow-y-auto px-4 py-2">
                                <Show when={loadingLogs()[pod.name]} fallback={
                                  <pre class="text-xs text-slate-300 font-mono leading-relaxed whitespace-pre-wrap break-all">
                                    {podLogs()[pod.name] || "Loading..."}
                                  </pre>
                                }>
                                  <div class="flex items-center gap-2 py-4">
                                    <div class="w-3 h-3 border border-blue-400 border-t-transparent rounded-full animate-spin" />
                                    <span class="text-xs text-slate-500">Fetching logs...</span>
                                  </div>
                                </Show>
                              </div>
                            </div>
                          </td>
                        </tr>
                      </Show>
                    </>
                  )}
                </For>
              </tbody>
            </table>
          </Show>
        </div>
      </div>

      {/* === STREAM MODE: Metrics Timeline + DLQ + Lifecycle === */}
      <Show when={isStream()}>
        {/* Metrics Timeline — 1-minute throughput buckets */}
        <div class="mt-6">
          <h2 class="text-sm font-semibold text-slate-300 uppercase tracking-wider mb-3">
            Metrics Timeline
            <span class="ml-2 text-slate-500 font-normal normal-case">
              (last 60 min, {metricsBuckets()?.length ?? 0} buckets)
            </span>
          </h2>
          <div class="bg-slate-800/50 rounded-lg border border-slate-700 overflow-hidden">
            <Show when={metricsBuckets() && metricsBuckets()!.length > 0} fallback={
              <div class="px-6 py-8 text-center">
                <p class="text-xs text-slate-500">No metrics data yet. Buckets appear as the stream processes records.</p>
              </div>
            }>
              {/* Simple bar visualization of throughput per bucket */}
              <div class="px-4 py-3">
                <div class="flex items-end gap-px h-24">
                  <For each={metricsBuckets()}>
                    {(bucket) => {
                      const maxIn = () => Math.max(1, ...metricsBuckets()!.map(b => b.records_in));
                      const heightPct = () => Math.max(2, (bucket.records_in / maxIn()) * 100);
                      return (
                        <div class="flex-1 flex flex-col items-center group relative">
                          <div
                            class="w-full bg-cyan-500/60 hover:bg-cyan-400/80 rounded-t transition-all"
                            style={{ height: `${heightPct()}%` }}
                          />
                          {/* Tooltip */}
                          <div class="absolute bottom-full mb-2 hidden group-hover:block z-10 bg-slate-900 border border-slate-700 rounded px-2 py-1 whitespace-nowrap">
                            <div class="text-[10px] text-slate-400 font-mono">{new Date(bucket.bucket_ts).toLocaleTimeString("en-US", { hour12: false })}</div>
                            <div class="text-[10px] text-cyan-400">In: {bucket.records_in}</div>
                            <div class="text-[10px] text-green-400">Out: {bucket.records_out}</div>
                            <Show when={bucket.records_failed > 0}>
                              <div class="text-[10px] text-red-400">Failed: {bucket.records_failed}</div>
                            </Show>
                            <div class="text-[10px] text-slate-400">Avg: {formatDuration(bucket.avg_duration_ms)}</div>
                          </div>
                        </div>
                      );
                    }}
                  </For>
                </div>
                <div class="flex justify-between mt-1">
                  <span class="text-[9px] text-slate-600 font-mono">
                    {metricsBuckets()![0] ? new Date(metricsBuckets()![0].bucket_ts).toLocaleTimeString("en-US", { hour12: false }) : ""}
                  </span>
                  <span class="text-[9px] text-slate-600 font-mono">
                    {metricsBuckets()!.length > 0 ? new Date(metricsBuckets()![metricsBuckets()!.length - 1].bucket_ts).toLocaleTimeString("en-US", { hour12: false }) : ""}
                  </span>
                </div>
              </div>
              {/* Summary row */}
              <div class="grid grid-cols-4 gap-4 px-4 py-3 border-t border-slate-700/50">
                <div>
                  <div class="text-[10px] text-slate-500 uppercase tracking-wider font-semibold">Total Invocations</div>
                  <div class="text-sm font-bold font-mono text-white">
                    {metricsBuckets()!.reduce((s, b) => s + b.invocations, 0).toLocaleString()}
                  </div>
                </div>
                <div>
                  <div class="text-[10px] text-slate-500 uppercase tracking-wider font-semibold">Avg Duration</div>
                  <div class="text-sm font-bold font-mono text-slate-300">
                    {formatDuration(metricsBuckets()!.length > 0
                      ? metricsBuckets()!.reduce((s, b) => s + b.avg_duration_ms, 0) / metricsBuckets()!.length
                      : 0)}
                  </div>
                </div>
                <div>
                  <div class="text-[10px] text-slate-500 uppercase tracking-wider font-semibold">Max Duration</div>
                  <div class="text-sm font-bold font-mono text-amber-400">
                    {formatDuration(Math.max(0, ...metricsBuckets()!.map(b => b.max_duration_ms)))}
                  </div>
                </div>
                <div>
                  <div class="text-[10px] text-slate-500 uppercase tracking-wider font-semibold">Bytes (1h)</div>
                  <div class="text-sm font-bold font-mono text-purple-400">
                    {formatBytes(metricsBuckets()!.reduce((s, b) => s + b.bytes_processed, 0))}
                  </div>
                </div>
              </div>
            </Show>
          </div>
        </div>

        {/* DLQ Inbox — Pattern-Centric Grouped View */}
        <div>
          <h2 class="text-sm font-semibold text-slate-300 uppercase tracking-wider mb-3">
            Dead Letter Queue
            <span class="ml-2 text-slate-500 font-normal normal-case">
              ({dlqGroups()?.total_pending ?? 0} pending across {dlqGroups()?.groups?.length ?? 0} patterns)
            </span>
          </h2>
          <div class="bg-slate-800/50 rounded-lg border border-slate-700 overflow-hidden">
            <Show when={dlqGroups()?.groups && dlqGroups()!.groups.length > 0} fallback={
              <div class="px-6 py-8 text-center">
                <p class="text-xs text-slate-500">No failed records. DLQ entries appear when transforms fail.</p>
              </div>
            }>
              <table class="w-full">
                <thead class="bg-slate-900/50 border-b border-slate-700">
                  <tr>
                    <th class="px-3 py-2.5 text-left text-xs font-semibold text-slate-400 uppercase tracking-wider">Error Pattern</th>
                    <th class="px-3 py-2.5 text-left text-xs font-semibold text-slate-400 uppercase tracking-wider">Step</th>
                    <th class="px-3 py-2.5 text-right text-xs font-semibold text-slate-400 uppercase tracking-wider">Count</th>
                    <th class="px-3 py-2.5 text-left text-xs font-semibold text-slate-400 uppercase tracking-wider">First / Last</th>
                    <th class="px-3 py-2.5 text-right text-xs font-semibold text-slate-400 uppercase tracking-wider">Actions</th>
                  </tr>
                </thead>
                <tbody class="divide-y divide-slate-700/50">
                  <For each={dlqGroups()!.groups.slice(dlqPage() * DLQ_PAGE_SIZE, (dlqPage() + 1) * DLQ_PAGE_SIZE)}>
                    {(group) => {
                      const groupKey = () => `${group.error}::${group.step}`;
                      return (
                        <>
                          <tr class="hover:bg-slate-700/30 transition-colors">
                            <td class="px-3 py-2.5 max-w-[300px]">
                              <span class="text-xs text-red-400 break-all line-clamp-2">{group.error}</span>
                            </td>
                            <td class="px-3 py-2.5">
                              <span class="text-xs text-slate-400 font-mono">{group.step || "—"}</span>
                            </td>
                            <td class="px-3 py-2.5 text-right">
                              <span class="text-sm font-bold font-mono text-amber-400">{group.count.toLocaleString()}</span>
                            </td>
                            <td class="px-3 py-2.5">
                              <div class="text-[10px] text-slate-500">{formatRelativeTime(group.first_seen)}</div>
                              <div class="text-[10px] text-slate-400">{formatRelativeTime(group.last_seen)}</div>
                            </td>
                            <td class="px-3 py-2.5 text-right">
                              <div class="flex items-center justify-end gap-1.5">
                                <button
                                  onClick={(e) => { e.stopPropagation(); handleCanaryTest(group); }}
                                  disabled={canaryLoading() === groupKey()}
                                  class="px-2 py-0.5 rounded text-[10px] font-semibold bg-cyan-500/10 text-cyan-400 border border-cyan-500/20 hover:bg-cyan-500/20 transition-all disabled:opacity-50"
                                  title="Test 1 record against live pipeline"
                                >
                                  {canaryLoading() === groupKey() ? "Testing..." : "Test"}
                                </button>
                                <button
                                  onClick={(e) => { e.stopPropagation(); handleReplayGroup(group); }}
                                  class="px-2 py-0.5 rounded text-[10px] font-semibold bg-blue-500/10 text-blue-400 border border-blue-500/20 hover:bg-blue-500/20 transition-all"
                                  title="Replay all records in this group"
                                >
                                  Replay All
                                </button>
                                <button
                                  onClick={(e) => { e.stopPropagation(); handleDismissGroup(group); }}
                                  class="px-2 py-0.5 rounded text-[10px] font-semibold bg-slate-500/10 text-slate-400 border border-slate-500/20 hover:bg-slate-500/20 transition-all"
                                  title="Dismiss all records in this group"
                                >
                                  Dismiss
                                </button>
                              </div>
                            </td>
                          </tr>
                          {/* Canary result row */}
                          <Show when={canaryResult()?.key === groupKey()}>
                            <tr>
                              <td colspan={5} class="px-4 py-2">
                                <div class={`flex items-center gap-2 px-3 py-1.5 rounded text-xs font-mono ${
                                  canaryResult()!.status === "success"
                                    ? "bg-green-500/10 text-green-400 border border-green-500/20"
                                    : canaryResult()!.status === "failed"
                                    ? "bg-red-500/10 text-red-400 border border-red-500/20"
                                    : "bg-yellow-500/10 text-yellow-400 border border-yellow-500/20"
                                }`}>
                                  <span class="font-semibold uppercase">{canaryResult()!.status}</span>
                                  <span class="text-slate-400">—</span>
                                  <span>{canaryResult()!.message}</span>
                                </div>
                              </td>
                            </tr>
                          </Show>
                        </>
                      );
                    }}
                  </For>
                </tbody>
              </table>
              {/* Pagination */}
              <Show when={dlqGroups()!.groups.length > DLQ_PAGE_SIZE}>
                <div class="flex items-center justify-between px-4 py-2 border-t border-slate-700/50 bg-slate-900/30">
                  <span class="text-[10px] text-slate-500 font-mono">
                    Page {dlqPage() + 1} of {Math.ceil(dlqGroups()!.groups.length / DLQ_PAGE_SIZE)}
                  </span>
                  <div class="flex items-center gap-2">
                    <button
                      onClick={() => setDlqPage(p => Math.max(0, p - 1))}
                      disabled={dlqPage() === 0}
                      class="px-2 py-0.5 rounded text-[10px] font-semibold text-slate-400 hover:text-white disabled:opacity-30 transition-all"
                    >
                      Prev
                    </button>
                    <button
                      onClick={() => setDlqPage(p => Math.min(Math.ceil(dlqGroups()!.groups.length / DLQ_PAGE_SIZE) - 1, p + 1))}
                      disabled={(dlqPage() + 1) * DLQ_PAGE_SIZE >= dlqGroups()!.groups.length}
                      class="px-2 py-0.5 rounded text-[10px] font-semibold text-slate-400 hover:text-white disabled:opacity-30 transition-all"
                    >
                      Next
                    </button>
                  </div>
                </div>
              </Show>
            </Show>
          </div>
        </div>

        {/* Worker Telemetry — DAG-specific worker breakdown */}
        <Show when={isDag() && pods() && pods()!.length > 0}>
          <div>
            <h2 class="text-sm font-semibold text-slate-300 uppercase tracking-wider mb-3">
              Worker Telemetry
              <span class="ml-2 text-slate-500 font-normal normal-case">
                ({pods()!.filter(p => p.name.includes('-worker') || p.name.includes('-ingest')).length} active workers)
              </span>
            </h2>
            <div class="bg-slate-800/50 rounded-lg border border-slate-700 overflow-hidden">
              <Show when={pods()!.some(p => p.name.includes('-worker') || p.name.includes('-ingest'))} fallback={
                <div class="px-6 py-8 text-center">
                  <p class="text-xs text-slate-500">No worker pods found. Workers appear when the DAG pipeline is running.</p>
                </div>
              }>
                <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 p-4">
                  <For each={pods()!.filter(p => p.name.includes('-worker') || p.name.includes('-ingest'))}>
                    {(pod) => {
                      const role = pod.name.includes('-worker') ? 'worker' : 'ingest';
                      const roleColor = role === 'worker' ? 'text-amber-400' : 'text-cyan-400';
                      const roleBg = role === 'worker' ? 'bg-amber-500/10 border-amber-500/20' : 'bg-cyan-500/10 border-cyan-500/20';
                      return (
                        <div class={`rounded-lg border p-4 ${pod.phase === 'Running' ? 'border-slate-600/50 bg-slate-800/80' : 'border-slate-700/30 bg-slate-800/30'}`}>
                          <div class="flex items-center justify-between mb-3">
                            <div class="flex items-center gap-2">
                              <div class={`w-2 h-2 rounded-full ${pod.phase === 'Running' ? 'bg-green-500 animate-pulse' : pod.phase === 'Failed' ? 'bg-red-500' : 'bg-slate-500'}`} />
                              <span class="text-xs font-mono text-white truncate max-w-[180px]">{pod.name}</span>
                            </div>
                            <span class={`px-2 py-0.5 rounded text-[10px] font-bold border ${roleBg} ${roleColor}`}>
                              {role}
                            </span>
                          </div>
                          <div class="grid grid-cols-2 gap-2">
                            <div>
                              <div class="text-[9px] text-slate-500 uppercase tracking-wider">Phase</div>
                              <div class={`text-xs font-semibold ${podPhaseColor(pod.phase)}`}>{pod.phase}</div>
                            </div>
                            <div>
                              <div class="text-[9px] text-slate-500 uppercase tracking-wider">Restarts</div>
                              <div class={`text-xs font-mono ${pod.restarts > 3 ? 'text-red-400' : 'text-slate-400'}`}>{pod.restarts}</div>
                            </div>
                            <div>
                              <div class="text-[9px] text-slate-500 uppercase tracking-wider">Node</div>
                              <div class="text-xs text-slate-400 font-mono truncate">{pod.node || '—'}</div>
                            </div>
                            <div>
                              <div class="text-[9px] text-slate-500 uppercase tracking-wider">Uptime</div>
                              <div class="text-xs text-slate-400">{formatRelativeTime(pod.start_time)}</div>
                            </div>
                          </div>
                          <Show when={pod.container_state === 'waiting' || pod.container_state === 'terminated'}>
                            <div class="mt-2 px-2 py-1 rounded bg-yellow-500/10 border border-yellow-500/20">
                              <span class="text-[10px] text-yellow-400 font-mono">{pod.state_detail || pod.container_state}</span>
                            </div>
                          </Show>
                        </div>
                      );
                    }}
                  </For>
                </div>
                {/* Aggregate worker stats */}
                <div class="grid grid-cols-3 gap-4 px-4 py-3 border-t border-slate-700/50 bg-slate-900/30">
                  <div>
                    <div class="text-[10px] text-slate-500 uppercase tracking-wider font-semibold">Running Workers</div>
                    <div class="text-sm font-bold font-mono text-green-400">
                      {pods()!.filter(p => (p.name.includes('-worker') || p.name.includes('-ingest')) && p.phase === 'Running').length}
                      <span class="text-slate-500 font-normal"> / {pods()!.filter(p => p.name.includes('-worker') || p.name.includes('-ingest')).length}</span>
                    </div>
                  </div>
                  <div>
                    <div class="text-[10px] text-slate-500 uppercase tracking-wider font-semibold">Total Restarts</div>
                    <div class={`text-sm font-bold font-mono ${pods()!.filter(p => p.name.includes('-worker') || p.name.includes('-ingest')).reduce((s, p) => s + p.restarts, 0) > 3 ? 'text-amber-400' : 'text-slate-300'}`}>
                      {pods()!.filter(p => p.name.includes('-worker') || p.name.includes('-ingest')).reduce((s, p) => s + p.restarts, 0)}
                    </div>
                  </div>
                  <div>
                    <div class="text-[10px] text-slate-500 uppercase tracking-wider font-semibold">Throughput</div>
                    <div class="text-sm font-bold font-mono text-cyan-400">
                      {pipeline()?.records_per_sec > 0 ? `${pipeline()!.records_per_sec.toFixed(1)} r/s` : '—'}
                    </div>
                  </div>
                </div>
              </Show>
            </div>
          </div>
        </Show>

        {/* Lifecycle Log */}
        <div>
          <h2 class="text-sm font-semibold text-slate-300 uppercase tracking-wider mb-3">
            Lifecycle Log
            <span class="ml-2 text-slate-500 font-normal normal-case">
              ({lifecycleEvents()?.length ?? 0} events)
            </span>
          </h2>
          <div class="bg-slate-800/50 rounded-lg border border-slate-700 overflow-hidden max-h-64 overflow-y-auto">
            <Show when={lifecycleEvents() && lifecycleEvents()!.length > 0} fallback={
              <div class="px-6 py-8 text-center">
                <p class="text-xs text-slate-500">No lifecycle events yet</p>
              </div>
            }>
              <table class="w-full">
                <tbody>
                  <For each={lifecycleEvents()}>
                    {(event) => (
                      <tr class="border-b border-slate-700/30 hover:bg-slate-700/20">
                        <td class="px-4 py-1.5 text-xs text-slate-600 font-mono whitespace-nowrap w-28">
                          {formatRelativeTime(event.timestamp)}
                        </td>
                        <td class="px-2 py-1.5">
                          <span class={`px-1.5 py-0.5 rounded text-[10px] font-bold border ${
                            event.event === "CRASH" || event.event === "FAIL"
                              ? "bg-red-500/20 text-red-400 border-red-500/30"
                              : event.event === "STARTUP" || event.event === "DEPLOY"
                              ? "bg-green-500/20 text-green-400 border-green-500/30"
                              : event.event === "RESTART"
                              ? "bg-yellow-500/20 text-yellow-400 border-yellow-500/30"
                              : "bg-slate-500/20 text-slate-400 border-slate-500/30"
                          }`}>
                            {event.event}
                          </span>
                        </td>
                        <td class="px-2 py-1.5 text-xs text-slate-400 font-mono">{event.version || ""}</td>
                        <td class="px-2 py-1.5 text-xs text-slate-400 break-all">{event.message}</td>
                      </tr>
                    )}
                  </For>
                </tbody>
              </table>
            </Show>
          </div>
        </div>
      </Show>

      {/* === ONCE/BATCH MODE: Execution History === */}
      <Show when={!isStream()}>
        <div>
          <h2 class="text-sm font-semibold text-slate-300 uppercase tracking-wider mb-3">
            Execution History
            <span class="ml-2 text-slate-500 font-normal normal-case">
              ({executions()?.length ?? 0} runs)
            </span>
          </h2>

          <div class="bg-slate-800/50 rounded-lg border border-slate-700 overflow-hidden">
            <Show when={executions() && executions()!.length > 0} fallback={
              <div class="px-6 py-8 text-center">
                <p class="text-xs text-slate-500">No executions recorded yet. Runs will appear here as the pipeline executes.</p>
              </div>
            }>
              <table class="w-full">
                <thead class="bg-slate-900/50 border-b border-slate-700">
                  <tr>
                    <th class="px-3 py-2.5 text-left text-xs font-semibold text-slate-400 uppercase tracking-wider w-8"></th>
                    <th class="px-3 py-2.5 text-left text-xs font-semibold text-slate-400 uppercase tracking-wider">Run</th>
                    <th class="px-3 py-2.5 text-left text-xs font-semibold text-slate-400 uppercase tracking-wider">Started</th>
                    <th class="px-3 py-2.5 text-right text-xs font-semibold text-slate-400 uppercase tracking-wider">Duration</th>
                    <th class="px-3 py-2.5 text-right text-xs font-semibold text-slate-400 uppercase tracking-wider">In</th>
                    <th class="px-3 py-2.5 text-right text-xs font-semibold text-slate-400 uppercase tracking-wider">Out</th>
                    <th class="px-3 py-2.5 text-right text-xs font-semibold text-slate-400 uppercase tracking-wider">Failed</th>
                    <th class="px-3 py-2.5 text-left text-xs font-semibold text-slate-400 uppercase tracking-wider">Status</th>
                    <th class="px-3 py-2.5 text-left text-xs font-semibold text-slate-400 uppercase tracking-wider">Logs</th>
                  </tr>
                </thead>
                <tbody class="divide-y divide-slate-700/50">
                  <For each={executions()}>
                    {(exec, idx) => {
                      const hasLogs = () => exec.log_snapshot && exec.log_snapshot.length > 0;
                      return (
                        <>
                          <tr
                            class={`hover:bg-slate-700/30 transition-colors ${hasLogs() ? "cursor-pointer" : ""}`}
                            onClick={() => hasLogs() && setExpandedExec(expandedExec() === exec.id ? null : exec.id)}
                          >
                            <td class="px-3 py-2.5">
                              <Show when={hasLogs()}>
                                <svg
                                  class={`w-3.5 h-3.5 text-slate-500 transition-transform ${expandedExec() === exec.id ? "rotate-90" : ""}`}
                                  fill="none" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor"
                                >
                                  <path stroke-linecap="round" stroke-linejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
                                </svg>
                              </Show>
                            </td>
                            <td class="px-3 py-2.5">
                              <span class="font-mono text-xs text-white">#{exec.id}</span>
                            </td>
                            <td class="px-3 py-2.5">
                              <span class="text-xs text-slate-400">{formatRelativeTime(exec.started_at)}</span>
                              <span class="text-[10px] text-slate-600 ml-2 font-mono">
                                {new Date(exec.started_at).toLocaleTimeString("en-US", { hour12: false })}
                              </span>
                            </td>
                            <td class="px-3 py-2.5 text-right">
                              <span class="font-mono text-xs text-slate-300">{formatDuration(exec.duration_ms)}</span>
                            </td>
                            <td class="px-3 py-2.5 text-right">
                              <span class="font-mono text-xs text-cyan-400">{exec.records_in > 0 ? exec.records_in.toLocaleString() : "—"}</span>
                            </td>
                            <td class="px-3 py-2.5 text-right">
                              <span class="font-mono text-xs text-green-400">{exec.records_out > 0 ? exec.records_out.toLocaleString() : "—"}</span>
                            </td>
                            <td class="px-3 py-2.5 text-right">
                              <span class={`font-mono text-xs ${exec.records_failed > 0 ? "text-red-400" : "text-slate-600"}`}>
                                {exec.records_failed > 0 ? exec.records_failed.toLocaleString() : "0"}
                              </span>
                            </td>
                            <td class="px-3 py-2.5">
                              <span class={`px-1.5 py-0.5 rounded text-[10px] font-semibold border ${
                                exec.status === "completed" 
                                  ? "bg-green-500/20 text-green-400 border-green-500/30" 
                                  : exec.status === "failed"
                                  ? "bg-red-500/20 text-red-400 border-red-500/30"
                                  : "bg-slate-500/20 text-slate-400 border-slate-500/30"
                              }`}>
                                {exec.status.toUpperCase()}
                              </span>
                              <Show when={exec.error_msg}>
                                <span class="text-[10px] text-red-400 ml-2">{exec.error_msg}</span>
                              </Show>
                            </td>
                            <td class="px-3 py-2.5">
                              <Show when={hasLogs()} fallback={
                                <span class="text-[10px] text-slate-600">—</span>
                              }>
                                <span class="text-[10px] text-blue-400 font-semibold">
                                  {exec.log_snapshot.split("\n").filter(Boolean).length} lines
                                </span>
                              </Show>
                            </td>
                          </tr>
                          {/* Collapsible Log Snapshot */}
                          <Show when={expandedExec() === exec.id && hasLogs()}>
                            <tr>
                              <td colspan={9} class="p-0">
                                <div class="bg-slate-950 border-t border-slate-800">
                                  <div class="flex items-center justify-between px-4 py-2 border-b border-slate-800">
                                    <span class="text-[10px] text-slate-500 uppercase tracking-wider font-semibold">
                                      Run #{exec.id} Log Snapshot
                                    </span>
                                    <span class="text-[10px] text-slate-600 font-mono">
                                      {new Date(exec.started_at).toLocaleString("en-US", { hour12: false })}
                                    </span>
                                  </div>
                                  <div class="max-h-64 overflow-y-auto px-4 py-2">
                                    <pre class="text-xs text-slate-300 font-mono leading-relaxed whitespace-pre-wrap break-all">
                                      {exec.log_snapshot}
                                    </pre>
                                  </div>
                                </div>
                              </td>
                            </tr>
                          </Show>
                        </>
                      );
                    }}
                  </For>
                </tbody>
              </table>
            </Show>
          </div>
        </div>
      </Show>

      {/* Recent Events */}
      <div>
        <h2 class="text-sm font-semibold text-slate-300 uppercase tracking-wider mb-3">
          Recent Events
          <span class="ml-2 text-slate-500 font-normal normal-case">
            ({events()?.length ?? 0})
          </span>
        </h2>

        <div class="bg-slate-800/50 rounded-lg border border-slate-700 overflow-hidden max-h-64 overflow-y-auto">
          <Show when={events() && events()!.length > 0} fallback={
            <div class="px-6 py-8 text-center">
              <p class="text-xs text-slate-500">No events recorded yet</p>
            </div>
          }>
            <table class="w-full">
              <tbody>
                <For each={events()}>
                  {(event: any) => (
                    <tr class="border-b border-slate-700/30 hover:bg-slate-700/20">
                      <td class="px-4 py-1.5 text-xs text-slate-600 font-mono whitespace-nowrap w-28">
                        {formatRelativeTime(event.timestamp)}
                      </td>
                      <td class="px-2 py-1.5 text-xs font-semibold text-blue-400 whitespace-nowrap w-24">
                        {event.type}
                      </td>
                      <td class="px-2 py-1.5 text-xs text-slate-400 break-all">
                        {event.payload ? Object.entries(event.payload).map(([k, v]) => `${k}=${v}`).join(" ") : ""}
                      </td>
                    </tr>
                  )}
                </For>
              </tbody>
            </table>
          </Show>
        </div>
      </div>
    </div>
  );
};

const InfoCard: Component<{ label: string; value: string; mono?: boolean; color?: string }> = (props) => (
  <div class="bg-slate-800/50 rounded-lg border border-slate-700/50 px-4 py-3">
    <div class="text-[10px] text-slate-500 uppercase tracking-wider font-semibold mb-1">{props.label}</div>
    <div class={`text-sm font-semibold truncate ${props.mono ? "font-mono" : ""} ${props.color || "text-white"}`}>
      {props.value}
    </div>
  </div>
);

export default PipelineDetail;
