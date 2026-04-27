import { For, createSignal, createMemo } from "solid-js";
import type { Component } from "solid-js";
import type { PipelineState } from "../store/pipelines";

type SortField = "name" | "status" | "runs" | "avg_runtime" | "failures" | "last_run" | "created_at" | "cpu" | "memory" | "max_runtime";
type SortDirection = "asc" | "desc";

interface PipelineTableProps {
  pipelines: PipelineState[];
  onSelect?: (pipelineId: string) => void;
  currentPage?: number;
  totalPages?: number;
  onPageChange?: (page: number) => void;
  itemsPerPage?: number;
}

const PipelineTable: Component<PipelineTableProps> = (props) => {
  const [sortField, setSortField] = createSignal<SortField>("name");
  const [sortDirection, setSortDirection] = createSignal<SortDirection>("asc");

  const currentPage = () => props.currentPage ?? 1;
  const totalPages = () => props.totalPages ?? 1;
  const itemsPerPage = () => props.itemsPerPage ?? 10;

  const toggleSort = (field: SortField) => {
    if (sortField() === field) {
      setSortDirection(sortDirection() === "asc" ? "desc" : "asc");
    } else {
      setSortField(field);
      setSortDirection("asc");
    }
  };

  const sortedPipelines = createMemo(() => {
    const list = [...props.pipelines];
    const field = sortField();
    const dir = sortDirection();

    return list.sort((a, b) => {
      let aVal: any;
      let bVal: any;

      switch (field) {
        case "name":
          aVal = a.id;
          bVal = b.id;
          break;
        case "status":
          aVal = a.status;
          bVal = b.status;
          break;
        case "runs":
          aVal = a.exec_stats.total_runs;
          bVal = b.exec_stats.total_runs;
          break;
        case "avg_runtime":
          aVal = a.exec_stats.avg_runtime_ms;
          bVal = b.exec_stats.avg_runtime_ms;
          break;
        case "max_runtime":
          aVal = a.exec_stats.max_runtime_ms;
          bVal = b.exec_stats.max_runtime_ms;
          break;
        case "failures":
          aVal = a.exec_stats.failures;
          bVal = b.exec_stats.failures;
          break;
        case "last_run":
          aVal = a.exec_stats.last_run || a.last_invocation || a.last_seen;
          bVal = b.exec_stats.last_run || b.last_invocation || b.last_seen;
          break;
        case "created_at":
          aVal = a.created_at;
          bVal = b.created_at;
          break;
        case "cpu":
          aVal = a.cpu;
          bVal = b.cpu;
          break;
        case "memory":
          aVal = a.memory;
          bVal = b.memory;
          break;
        default:
          return 0;
      }

      if (aVal < bVal) return dir === "asc" ? -1 : 1;
      if (aVal > bVal) return dir === "asc" ? 1 : -1;
      return 0;
    });
  });

  const paginatedPipelines = createMemo(() => {
    const start = (currentPage() - 1) * itemsPerPage();
    const end = start + itemsPerPage();
    return sortedPipelines().slice(start, end);
  });

  const statusColor = (status: string) => {
    switch (status) {
      case "Running": return "bg-green-500/20 text-green-400 border-green-500/30";
      case "Streaming": return "bg-cyan-500/20 text-cyan-400 border-cyan-500/30";
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
      case "Streaming": return "bg-cyan-500";
      case "Enabled": return "bg-blue-500";
      case "Idling": return "bg-slate-400";
      case "Failed": return "bg-red-500";
      case "ZOMBIE": return "bg-yellow-500";
      default: return "bg-slate-500";
    }
  };

  const formatDuration = (ms: number) => {
    if (ms < 1000) return `${ms}ms`;
    if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
    return `${(ms / 60000).toFixed(1)}m`;
  };

  const formatRelativeTime = (timestamp: string) => {
    if (!timestamp) return "—";
    const now = new Date().getTime();
    const then = new Date(timestamp).getTime();
    if (Number.isNaN(then)) return "—";
    const diff = now - then;
    
    if (diff < 60000) return `${Math.floor(diff / 1000)}s ago`;
    if (diff < 3600000) return `${Math.floor(diff / 60000)}m ago`;
    if (diff < 86400000) return `${Math.floor(diff / 3600000)}h ago`;
    return `${Math.floor(diff / 86400000)}d ago`;
  };

  const SortIcon: Component<{ field: SortField }> = (props) => {
    const isActive = () => sortField() === props.field;
    const direction = () => sortDirection();
    
    return (
      <span class="ml-1 inline-block">
        {isActive() ? (
          direction() === "asc" ? "↑" : "↓"
        ) : (
          <span class="text-slate-600">↕</span>
        )}
      </span>
    );
  };


  return (
    <div class="bg-slate-800/50 rounded-lg border border-slate-700 overflow-hidden">
      <div class="overflow-x-auto">
        <table class="w-full">
          <thead class="bg-slate-900/50 border-b border-slate-700">
            <tr>
              <th 
                class="px-4 py-3 text-left text-xs font-semibold text-slate-400 uppercase tracking-wider cursor-pointer hover:text-slate-300 transition-colors"
                onClick={() => toggleSort("name")}
              >
                Pipeline <SortIcon field="name" />
              </th>
              <th 
                class="px-4 py-3 text-left text-xs font-semibold text-slate-400 uppercase tracking-wider cursor-pointer hover:text-slate-300 transition-colors"
                onClick={() => toggleSort("status")}
              >
                Status <SortIcon field="status" />
              </th>
              <th 
                class="px-4 py-3 text-right text-xs font-semibold text-slate-400 uppercase tracking-wider cursor-pointer hover:text-slate-300 transition-colors"
                onClick={() => toggleSort("runs")}
              >
                Runs <SortIcon field="runs" />
              </th>
              <th 
                class="px-4 py-3 text-right text-xs font-semibold text-slate-400 uppercase tracking-wider cursor-pointer hover:text-slate-300 transition-colors"
                onClick={() => toggleSort("avg_runtime")}
              >
                Avg Runtime <SortIcon field="avg_runtime" />
              </th>
              <th class="px-4 py-3 text-right text-xs font-semibold text-slate-400 uppercase tracking-wider">
                P50 / P99
              </th>
              <th 
                class="px-4 py-3 text-right text-xs font-semibold text-slate-400 uppercase tracking-wider cursor-pointer hover:text-slate-300 transition-colors"
                onClick={() => toggleSort("failures")}
              >
                Failures <SortIcon field="failures" />
              </th>
              <th 
                class="px-4 py-3 text-left text-xs font-semibold text-slate-400 uppercase tracking-wider cursor-pointer hover:text-slate-300 transition-colors"
                onClick={() => toggleSort("last_run")}
              >
                Last Run <SortIcon field="last_run" />
              </th>
              <th 
                class="px-4 py-3 text-left text-xs font-semibold text-slate-400 uppercase tracking-wider cursor-pointer hover:text-slate-300 transition-colors"
                onClick={() => toggleSort("created_at")}
              >
                Created <SortIcon field="created_at" />
              </th>
              <th 
                class="px-4 py-3 text-right text-xs font-semibold text-slate-400 uppercase tracking-wider cursor-pointer hover:text-slate-300 transition-colors"
                onClick={() => toggleSort("cpu")}
              >
                CPU <SortIcon field="cpu" />
              </th>
              <th 
                class="px-4 py-3 text-right text-xs font-semibold text-slate-400 uppercase tracking-wider cursor-pointer hover:text-slate-300 transition-colors"
                onClick={() => toggleSort("memory")}
              >
                Memory <SortIcon field="memory" />
              </th>
            </tr>
          </thead>
          <tbody class="divide-y divide-slate-700/50">
            <For each={paginatedPipelines()}>
              {(pipeline) => {
                const s = pipeline.exec_stats;
                
                return (
                  <tr class="hover:bg-slate-700/30 transition-colors cursor-pointer" onClick={() => props.onSelect?.(pipeline.id)}>
                    <td class="px-4 py-3">
                      <div class="flex items-center gap-2">
                        <div class={`w-2 h-2 rounded-full ${statusDot(pipeline.status)}`} />
                        <span class="font-mono text-sm text-blue-400 hover:text-blue-300 hover:underline">{pipeline.id}</span>
                      </div>
                    </td>
                    <td class="px-4 py-3">
                      <span class={`px-2 py-1 rounded text-xs font-semibold border ${statusColor(pipeline.status)}`}>
                        {pipeline.status.toUpperCase()}
                      </span>
                    </td>
                    <td class="px-4 py-3 text-right">
                      <span class="font-mono text-sm text-slate-300">
                        {pipeline.mode === "stream" ? (s.total_runs > 0 ? "1" : "—") : s.total_runs.toLocaleString()}
                      </span>
                    </td>
                    <td class="px-4 py-3 text-right">
                      <span class="font-mono text-sm text-slate-300">
                        {pipeline.mode === "stream"
                          ? (pipeline.records_per_sec > 0 ? `${pipeline.records_per_sec.toFixed(1)} rec/s` : "—")
                          : formatDuration(s.avg_runtime_ms)}
                      </span>
                    </td>
                    <td class="px-4 py-3 text-right">
                      <span class="font-mono text-xs text-slate-400">
                        {pipeline.mode === "stream" ? "—" : `${formatDuration(s.p50_runtime_ms)} / ${formatDuration(s.p99_runtime_ms)}`}
                      </span>
                    </td>
                    <td class="px-4 py-3 text-right">
                      <div class="flex items-center justify-end gap-2">
                        <span class="font-mono text-sm text-red-400">{s.failures}</span>
                        <span class="text-xs text-slate-500">({s.fail_rate}%)</span>
                      </div>
                    </td>
                    <td class="px-4 py-3">
                      <span class="text-sm text-slate-400">{formatRelativeTime(pipeline.exec_stats.last_run || pipeline.last_invocation || pipeline.last_seen)}</span>
                    </td>
                    <td class="px-4 py-3">
                      <span class="text-sm text-slate-400">{formatRelativeTime(pipeline.created_at)}</span>
                    </td>
                    <td class="px-4 py-3 text-right">
                      <span class="font-mono text-sm text-blue-300">{pipeline.cpu.toFixed(1)}%</span>
                    </td>
                    <td class="px-4 py-3 text-right">
                      <span class="font-mono text-sm text-purple-300">{pipeline.memory} MB</span>
                    </td>
                  </tr>
                );
              }}
            </For>
          </tbody>
        </table>
      </div>
      
      {/* Pagination Controls */}
      {totalPages() > 1 && props.onPageChange && (
        <div class="flex items-center justify-center gap-2 py-4 border-t border-slate-700">
          <button
            onClick={() => props.onPageChange?.(Math.max(1, currentPage() - 1))}
            disabled={currentPage() === 1}
            class={`px-3 py-1.5 rounded text-xs font-medium transition-all ${
              currentPage() === 1
                ? "bg-slate-800 text-slate-600 cursor-not-allowed"
                : "bg-slate-700 text-slate-300 hover:bg-slate-600"
            }`}
          >
            ← Prev
          </button>
          
          <div class="flex gap-1">
            <For each={Array.from({ length: totalPages() }, (_, i) => i + 1)}>
              {(page) => (
                <button
                  onClick={() => props.onPageChange?.(page)}
                  class={`w-8 h-8 rounded text-xs font-medium transition-all ${
                    currentPage() === page
                      ? "bg-blue-600 text-white"
                      : "bg-slate-800 text-slate-400 hover:bg-slate-700 hover:text-slate-200"
                  }`}
                >
                  {page}
                </button>
              )}
            </For>
          </div>
          
          <button
            onClick={() => props.onPageChange?.(Math.min(totalPages(), currentPage() + 1))}
            disabled={currentPage() === totalPages()}
            class={`px-3 py-1.5 rounded text-xs font-medium transition-all ${
              currentPage() === totalPages()
                ? "bg-slate-800 text-slate-600 cursor-not-allowed"
                : "bg-slate-700 text-slate-300 hover:bg-slate-600"
            }`}
          >
            Next →
          </button>
        </div>
      )}
    </div>
  );
};

export default PipelineTable;
