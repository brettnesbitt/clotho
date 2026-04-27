import { Show } from "solid-js";
import type { Component } from "solid-js";
import type { PipelineState } from "../store/pipelines";

interface Props {
  data: PipelineState;
}

const PipelineCard: Component<Props> = (props) => {
  const statusColor = () => {
    switch (props.data.status) {
      case "Running": return "bg-green-500 shadow-green-500/50";
      case "Streaming": return "bg-cyan-500 shadow-cyan-500/50";
      case "Enabled": return "bg-blue-500 shadow-blue-500/50";
      case "Idling": return "bg-slate-400 shadow-slate-400/50";
      case "Failed": return "bg-red-500 shadow-red-500/50";
      case "ZOMBIE": return "bg-yellow-500 shadow-yellow-500/50";
      default: return "bg-slate-500";
    }
  };

  const statusText = () => {
    switch (props.data.status) {
      case "Running": return "text-green-400";
      case "Streaming": return "text-cyan-400";
      case "Enabled": return "text-blue-400";
      case "Idling": return "text-slate-400";
      case "Failed": return "text-red-400";
      case "ZOMBIE": return "text-yellow-400";
      default: return "text-slate-400";
    }
  };

  const formatUptime = (ms: number) => {
    const seconds = Math.floor(ms / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);
    if (hours > 0) return `${hours}h ${minutes % 60}m`;
    if (minutes > 0) return `${minutes}m ${seconds % 60}s`;
    return `${seconds}s`;
  };

  const titleColor = () => {
    switch (props.data.status) {
      case "Running": return "text-green-400 hover:text-green-300";
      case "Streaming": return "text-cyan-400 hover:text-cyan-300";
      case "Enabled": return "text-blue-400 hover:text-blue-300";
      case "Idling": return "text-slate-400 hover:text-slate-300";
      case "Failed": return "text-red-400 hover:text-red-300";
      case "ZOMBIE": return "text-yellow-400 hover:text-yellow-300";
      default: return "text-slate-400 hover:text-slate-300";
    }
  };

  const cardBorderClass = () => {
    switch (props.data.status) {
      case "Running": return "border-green-500/50";
      case "Streaming": return "border-cyan-500/50";
      default: return "border-slate-700";
    }
  };

  return (
    <div class={`relative overflow-hidden bg-slate-800 rounded-xl p-5 shadow-xl transition-all hover:shadow-2xl ${
      props.data.status === "Running" || props.data.status === "Streaming"
        ? `border ${cardBorderClass()}`
        : "border border-slate-700 hover:border-slate-600"
    }`}>
      {/* Glow effect for active pipelines */}
      <Show when={props.data.status === "Running"}>
        <div class="absolute -top-20 -right-20 w-40 h-40 bg-green-500/10 rounded-full blur-3xl" />
      </Show>
      <Show when={props.data.status === "Streaming"}>
        <div class="absolute -top-20 -right-20 w-40 h-40 bg-cyan-500/10 rounded-full blur-3xl" />
      </Show>
      <Show when={props.data.status === "Enabled"}>
        <div class="absolute -top-20 -right-20 w-40 h-40 bg-blue-500/5 rounded-full blur-3xl" />
      </Show>

      {/* Header */}
      <div class="flex justify-between items-start mb-4 relative min-h-[3.5rem]">
        <div class="flex-1 min-w-0 pr-2">
          <h3 class={`font-bold text-lg tracking-tight cursor-pointer hover:underline truncate ${titleColor()}`}>{props.data.id}</h3>
          <p class="text-xs text-slate-500 font-mono mt-0.5 truncate">{props.data.image}</p>
        </div>
        <div class={`h-3 w-3 rounded-full shadow-lg flex-shrink-0 mt-1.5 ${statusColor()} animate-pulse-glow`} />
      </div>

      {/* Status Badge */}
      <div class="mb-4">
        <span class={`text-xs font-semibold uppercase tracking-wider ${statusText()}`}>
          {props.data.status}
        </span>
        <Show when={props.data.uptime > 0}>
          <span class="text-xs text-slate-500 ml-2">
            Uptime: {formatUptime(props.data.uptime)}
          </span>
        </Show>
      </div>

      {/* Metrics Grid */}
      <div class="grid grid-cols-2 gap-3 mb-4">
        {/* CPU */}
        <div class="bg-slate-900/50 rounded-lg p-3 border border-slate-800">
          <p class="text-[10px] text-slate-500 uppercase tracking-wider font-semibold">CPU</p>
          <div class="flex items-center gap-2 mt-1">
            <div class="flex-1 h-1.5 bg-slate-700 rounded-full overflow-hidden">
              <div
                class="h-full bg-blue-500 rounded-full transition-all duration-500"
                style={{ width: `${Math.min(100, props.data.cpu)}%` }}
              />
            </div>
            <span class="text-xs font-mono text-blue-300 w-10 text-right">{props.data.cpu.toFixed(1)}%</span>
          </div>
        </div>

        {/* Memory */}
        <div class="bg-slate-900/50 rounded-lg p-3 border border-slate-800">
          <p class="text-[10px] text-slate-500 uppercase tracking-wider font-semibold">Memory</p>
          <p class="text-sm font-mono text-white mt-0.5">{props.data.memory} <span class="text-xs text-slate-500">MB</span></p>
        </div>
      </div>

      {/* Progress Bar - Always shown for consistent sizing */}
      <div class="w-full bg-slate-700 rounded-full h-2 mb-2 overflow-hidden">
        <div
          class={`h-full rounded-full transition-all duration-500 ease-linear ${
            props.data.status === "Running" || props.data.progress > 0
              ? "bg-gradient-to-r from-green-500 to-emerald-400"
              : "bg-slate-600"
          }`}
          style={{ width: `${props.data.status === "Running" || props.data.progress > 0 ? props.data.progress : 100}%` }}
        />
      </div>
      <div class="flex justify-between text-[10px] text-slate-400">
        <span class={props.data.status === "Running" || props.data.progress > 0 ? "text-slate-500" : "text-slate-600"}>
          {props.data.status === "Running" || props.data.progress > 0
            ? `${props.data.progress_current.toLocaleString()} / ${props.data.progress_total.toLocaleString()}`
            : "Idle"
          }
        </span>
        <span class={`font-mono ${
          props.data.status === "Running" || props.data.progress > 0 ? "text-green-400" : "text-slate-600"
        }`}>
          {props.data.status === "Running" || props.data.progress > 0
            ? `${Math.round(props.data.progress)}%`
            : "—"
          }
        </span>
      </div>

      {/* Last Seen */}
      <div class="mt-3 pt-3 border-t border-slate-700/50">
        <p class="text-[10px] text-slate-600 font-mono">
          Last seen: {new Date(props.data.last_seen).toLocaleTimeString()}
        </p>
      </div>
    </div>
  );
};

export default PipelineCard;
