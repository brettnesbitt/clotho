import { For, Show, createSignal, createResource } from "solid-js";
import type { Component } from "solid-js";

interface DlqRecord {
  id: number;
  pipeline_id: string;
  trace_id: string;
  error: string;
  step: string;
  payload: string;
  status: string;
  created_at: string;
  replayed_at: string;
}

const API_URL = "";

const DlqInbox: Component = () => {
  const [statusFilter, setStatusFilter] = createSignal("pending");
  const [pipelineFilter, setPipelineFilter] = createSignal("");
  const [selectedRecord, setSelectedRecord] = createSignal<DlqRecord | null>(null);
  const [actionLoading, setActionLoading] = createSignal("");

  const [records, { refetch }] = createResource(
    () => ({ status: statusFilter(), pipeline: pipelineFilter() }),
    async (filters) => {
      try {
        let url = `${API_URL}/api/v1/dlq?limit=200`;
        if (filters.status) url += `&status=${filters.status}`;
        if (filters.pipeline) url += `&pipeline_id=${filters.pipeline}`;
        const resp = await fetch(url);
        if (!resp.ok) return [];
        return (await resp.json()) as DlqRecord[];
      } catch {
        return [];
      }
    }
  );

  const [summary] = createResource(async () => {
    try {
      const resp = await fetch(`${API_URL}/api/v1/dlq/summary`);
      if (!resp.ok) return [];
      return await resp.json();
    } catch {
      return [];
    }
  });

  const pendingCount = () => {
    const s = summary();
    if (!s) return 0;
    return (s as any[]).filter((r: any) => r.status === "pending").reduce((sum: number, r: any) => sum + r.count, 0);
  };

  const handleReplay = async (id: number) => {
    setActionLoading(`replay-${id}`);
    try {
      await fetch(`${API_URL}/api/v1/dlq/${id}/replay`, { method: "POST" });
      refetch();
    } catch { /* silent */ }
    setActionLoading("");
  };

  const handleDismiss = async (id: number) => {
    setActionLoading(`dismiss-${id}`);
    try {
      await fetch(`${API_URL}/api/v1/dlq/${id}/dismiss`, { method: "POST" });
      refetch();
    } catch { /* silent */ }
    setActionLoading("");
  };

  const handleReplayAll = async (pipelineId: string) => {
    setActionLoading("replay-all");
    try {
      await fetch(`${API_URL}/api/v1/dlq/replay-all?pipeline_id=${pipelineId}`, { method: "POST" });
      refetch();
    } catch { /* silent */ }
    setActionLoading("");
  };

  const formatTime = (ts: string) => {
    if (!ts) return "—";
    try {
      const d = new Date(ts);
      return d.toLocaleString("en-US", { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false });
    } catch {
      return ts;
    }
  };

  const statusBadge = (status: string) => {
    switch (status) {
      case "pending": return "bg-red-500/20 text-red-400 border-red-500/30";
      case "replayed": return "bg-green-500/20 text-green-400 border-green-500/30";
      case "dismissed": return "bg-slate-500/20 text-slate-400 border-slate-500/30";
      default: return "bg-slate-500/20 text-slate-400 border-slate-500/30";
    }
  };

  return (
    <div class="space-y-6">
      {/* Header Stats */}
      <div class="flex items-center justify-between">
        <div class="flex items-center gap-3">
          <div class="w-10 h-10 rounded-lg bg-red-500/10 border border-red-500/20 flex items-center justify-center">
            <svg class="w-5 h-5 text-red-400" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor">
              <path stroke-linecap="round" stroke-linejoin="round" d="M12 9v3.75m-9.303 3.376c-.866 1.5.217 3.374 1.948 3.374h14.71c1.73 0 2.813-1.874 1.948-3.374L13.949 3.378c-.866-1.5-3.032-1.5-3.898 0L2.697 16.126zM12 15.75h.007v.008H12v-.008z" />
            </svg>
          </div>
          <div>
            <h2 class="text-sm font-semibold text-white">Dead Letter Queue</h2>
            <p class="text-xs text-slate-500">
              {pendingCount()} pending failure{pendingCount() !== 1 ? "s" : ""} awaiting action
            </p>
          </div>
        </div>
      </div>

      {/* Filters */}
      <div class="flex items-center gap-3">
        <div class="flex gap-1 bg-slate-800 rounded-lg p-0.5 border border-slate-700">
          {["pending", "replayed", "dismissed", ""].map(s => (
            <button
              onClick={() => setStatusFilter(s)}
              class={`px-3 py-1.5 rounded text-xs font-semibold transition-all ${
                statusFilter() === s
                  ? "bg-slate-700 text-white"
                  : "text-slate-500 hover:text-slate-300"
              }`}
            >
              {s || "All"}
            </button>
          ))}
        </div>

        <input
          type="text"
          placeholder="Filter by pipeline..."
          class="bg-slate-800 border border-slate-700 rounded-md px-3 py-1.5 text-xs text-white placeholder-slate-500 focus:outline-none focus:border-blue-500/50 font-mono"
          onInput={(e) => setPipelineFilter(e.currentTarget.value)}
        />

        <div class="text-xs text-slate-500 font-mono ml-auto">
          {records()?.length ?? 0} records
        </div>
      </div>

      {/* Split View: List + Detail */}
      <div class="flex gap-4 h-[calc(100vh-14rem)]">
        {/* Left: Record List */}
        <div class="w-1/2 bg-slate-800/50 rounded-lg border border-slate-700 overflow-y-auto">
          <Show when={records() && records()!.length > 0} fallback={
            <div class="flex items-center justify-center h-full">
              <div class="text-center">
                <div class="w-12 h-12 rounded-full bg-slate-800 border border-slate-700 flex items-center justify-center mx-auto mb-3">
                  <svg class="w-6 h-6 text-green-400" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor">
                    <path stroke-linecap="round" stroke-linejoin="round" d="M9 12.75L11.25 15 15 9.75M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                  </svg>
                </div>
                <p class="text-sm text-slate-300 font-semibold">All clear</p>
                <p class="text-xs text-slate-500 mt-1">No {statusFilter() || ""} DLQ records</p>
              </div>
            </div>
          }>
            <div class="divide-y divide-slate-700/50">
              <For each={records()}>
                {(record) => (
                  <div
                    class={`px-4 py-3 cursor-pointer transition-all hover:bg-slate-700/30 ${
                      selectedRecord()?.id === record.id ? "bg-slate-700/50 border-l-2 border-l-blue-400" : "border-l-2 border-l-transparent"
                    }`}
                    onClick={() => setSelectedRecord(record)}
                  >
                    <div class="flex items-center justify-between mb-1">
                      <span class="font-mono text-xs text-cyan-300">{record.pipeline_id}</span>
                      <span class={`px-1.5 py-0.5 rounded text-[10px] font-semibold border ${statusBadge(record.status)}`}>
                        {record.status.toUpperCase()}
                      </span>
                    </div>
                    <p class="text-xs text-red-400 truncate mb-1">{record.error}</p>
                    <div class="flex items-center gap-2 text-[10px] text-slate-500">
                      <span>Step: {record.step}</span>
                      <span>|</span>
                      <span>{formatTime(record.created_at)}</span>
                    </div>
                  </div>
                )}
              </For>
            </div>
          </Show>
        </div>

        {/* Right: Detail Panel */}
        <div class="w-1/2 bg-slate-800/50 rounded-lg border border-slate-700 overflow-y-auto">
          <Show when={selectedRecord()} fallback={
            <div class="flex items-center justify-center h-full">
              <div class="text-center">
                <svg class="w-8 h-8 text-slate-600 mx-auto mb-2" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor">
                  <path stroke-linecap="round" stroke-linejoin="round" d="M19.5 14.25v-2.625a3.375 3.375 0 00-3.375-3.375h-1.5A1.125 1.125 0 0113.5 7.125v-1.5a3.375 3.375 0 00-3.375-3.375H8.25m0 12.75h7.5m-7.5 3H12M10.5 2.25H5.625c-.621 0-1.125.504-1.125 1.125v17.25c0 .621.504 1.125 1.125 1.125h12.75c.621 0 1.125-.504 1.125-1.125V11.25a9 9 0 00-9-9z" />
                </svg>
                <p class="text-xs text-slate-500">Select a record to inspect</p>
              </div>
            </div>
          }>
            {(record) => {
              const r = record();
              return (
                <div class="flex flex-col h-full">
                  {/* Detail Header */}
                  <div class="px-4 py-3 border-b border-slate-700 flex-shrink-0">
                    <div class="flex items-center justify-between mb-2">
                      <div>
                        <span class="font-mono text-sm text-white">{r.pipeline_id}</span>
                        <span class="text-xs text-slate-500 ml-2">#{r.id}</span>
                      </div>
                      <span class={`px-2 py-0.5 rounded text-xs font-semibold border ${statusBadge(r.status)}`}>
                        {r.status.toUpperCase()}
                      </span>
                    </div>
                    <div class="flex items-center gap-3 text-[10px] text-slate-500">
                      <span>Trace: <span class="text-slate-400 font-mono">{r.trace_id || "—"}</span></span>
                      <span>Step: <span class="text-slate-400">{r.step}</span></span>
                      <span>{formatTime(r.created_at)}</span>
                    </div>
                  </div>

                  {/* Error */}
                  <div class="px-4 py-3 border-b border-slate-700 flex-shrink-0">
                    <div class="text-[10px] text-slate-500 uppercase tracking-wider font-semibold mb-1.5">Error</div>
                    <div class="bg-red-500/5 border border-red-500/20 rounded-md px-3 py-2">
                      <pre class="text-xs text-red-400 font-mono whitespace-pre-wrap break-all">{r.error}</pre>
                    </div>
                  </div>

                  {/* Payload */}
                  <div class="flex-1 min-h-0 px-4 py-3 overflow-y-auto">
                    <div class="text-[10px] text-slate-500 uppercase tracking-wider font-semibold mb-1.5">Failed Payload</div>
                    <div class="bg-slate-950 rounded-md border border-slate-800 p-3 max-h-full overflow-y-auto">
                      <pre class="text-xs text-slate-300 font-mono whitespace-pre-wrap break-all">
                        {(() => {
                          try {
                            return JSON.stringify(JSON.parse(r.payload), null, 2);
                          } catch {
                            return r.payload || "(no payload captured)";
                          }
                        })()}
                      </pre>
                    </div>
                  </div>

                  {/* Actions */}
                  <Show when={r.status === "pending"}>
                    <div class="px-4 py-3 border-t border-slate-700 flex items-center gap-2 flex-shrink-0">
                      <button
                        onClick={() => handleReplay(r.id)}
                        disabled={!!actionLoading()}
                        class="flex items-center gap-1.5 px-4 py-2 rounded-md text-xs font-semibold bg-blue-500/10 text-blue-400 border border-blue-500/20 hover:bg-blue-500/20 transition-all disabled:opacity-50"
                      >
                        <svg class="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor">
                          <path stroke-linecap="round" stroke-linejoin="round" d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.993 0l3.181 3.183a8.25 8.25 0 0013.803-3.7M4.031 9.865a8.25 8.25 0 0113.803-3.7l3.181 3.182" />
                        </svg>
                        {actionLoading() === `replay-${r.id}` ? "Replaying..." : "Replay"}
                      </button>
                      <button
                        onClick={() => handleDismiss(r.id)}
                        disabled={!!actionLoading()}
                        class="flex items-center gap-1.5 px-4 py-2 rounded-md text-xs font-semibold bg-slate-800 text-slate-400 border border-slate-700 hover:text-slate-300 hover:bg-slate-700 transition-all disabled:opacity-50"
                      >
                        Dismiss
                      </button>
                      <button
                        onClick={() => handleReplayAll(r.pipeline_id)}
                        disabled={!!actionLoading()}
                        class="ml-auto flex items-center gap-1.5 px-4 py-2 rounded-md text-xs font-semibold bg-green-500/10 text-green-400 border border-green-500/20 hover:bg-green-500/20 transition-all disabled:opacity-50"
                      >
                        {actionLoading() === "replay-all" ? "Replaying..." : "Replay All Pending"}
                      </button>
                    </div>
                  </Show>
                </div>
              );
            }}
          </Show>
        </div>
      </div>
    </div>
  );
};

export default DlqInbox;
