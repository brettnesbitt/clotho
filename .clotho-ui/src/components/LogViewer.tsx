import { For, createSignal, onMount, onCleanup } from "solid-js";
import type { Component } from "solid-js";

interface LogEvent {
  pipeline_id: string;
  type: string;
  payload: Record<string, any> | null;
  timestamp: string;
}

const LogViewer: Component = () => {
  const [events, setEvents] = createSignal<LogEvent[]>([]);
  const [autoScroll, setAutoScroll] = createSignal(true);
  const [filter, setFilter] = createSignal("");
  const [typeFilter, setTypeFilter] = createSignal("ALL");
  let containerRef: HTMLDivElement | undefined;
  let interval: ReturnType<typeof setInterval>;

  const API_URL = import.meta.env.VITE_API_URL || "http://localhost:3000";

  const fetchEvents = async () => {
    try {
      const resp = await fetch(`${API_URL}/v1/events?limit=500`);
      if (!resp.ok) return;
      const data: LogEvent[] = await resp.json();
      setEvents(data.reverse());

      if (autoScroll() && containerRef) {
        requestAnimationFrame(() => {
          containerRef!.scrollTop = containerRef!.scrollHeight;
        });
      }
    } catch (e) {
      // Silent fail
    }
  };

  onMount(() => {
    fetchEvents();
    interval = setInterval(fetchEvents, 2000);
  });

  onCleanup(() => {
    clearInterval(interval);
  });

  const eventTypes = () => {
    const types = new Set(events().map(e => e.type));
    return ["ALL", ...Array.from(types).sort()];
  };

  const filteredEvents = () => {
    let filtered = events();
    const f = filter().toLowerCase();
    const t = typeFilter();

    if (t !== "ALL") {
      filtered = filtered.filter(e => e.type === t);
    }
    if (f) {
      filtered = filtered.filter(e =>
        e.pipeline_id.toLowerCase().includes(f) ||
        e.type.toLowerCase().includes(f) ||
        JSON.stringify(e.payload).toLowerCase().includes(f)
      );
    }
    return filtered;
  };

  const typeColor = (type: string) => {
    switch (type) {
      case "PROGRESS": return "text-blue-400";
      case "HEARTBEAT": return "text-green-400";
      case "ERROR": return "text-red-400";
      case "START": return "text-emerald-400";
      case "STOP": return "text-yellow-400";
      case "METRIC": return "text-purple-400";
      default: return "text-slate-400";
    }
  };

  const formatTimestamp = (ts: string) => {
    try {
      const d = new Date(ts);
      return d.toLocaleTimeString("en-US", { hour12: false, hour: "2-digit", minute: "2-digit", second: "2-digit" });
    } catch {
      return ts;
    }
  };

  const formatPayload = (payload: Record<string, any> | null) => {
    if (!payload) return "";
    return Object.entries(payload)
      .map(([k, v]) => `${k}=${typeof v === "object" ? JSON.stringify(v) : v}`)
      .join(" ");
  };

  return (
    <div class="flex flex-col h-[calc(100vh-7rem)]">
      {/* Toolbar */}
      <div class="flex items-center gap-3 mb-3">
        <div class="flex-1 relative">
          <svg class="absolute left-3 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-slate-500" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" d="M21 21l-5.197-5.197m0 0A7.5 7.5 0 105.196 5.196a7.5 7.5 0 0010.607 10.607z" />
          </svg>
          <input
            type="text"
            placeholder="Filter logs..."
            class="w-full bg-slate-800 border border-slate-700 rounded-md pl-9 pr-3 py-1.5 text-xs text-white placeholder-slate-500 focus:outline-none focus:border-blue-500/50 font-mono"
            onInput={(e) => setFilter(e.currentTarget.value)}
          />
        </div>

        <select
          class="bg-slate-800 border border-slate-700 rounded-md px-3 py-1.5 text-xs text-white font-mono focus:outline-none focus:border-blue-500/50 appearance-none cursor-pointer"
          onChange={(e) => setTypeFilter(e.currentTarget.value)}
        >
          <For each={eventTypes()}>
            {(type) => <option value={type}>{type}</option>}
          </For>
        </select>

        <button
          onClick={() => setAutoScroll(!autoScroll())}
          class={`px-3 py-1.5 rounded-md text-xs font-semibold border transition-all ${
            autoScroll()
              ? "bg-blue-500/10 text-blue-400 border-blue-500/20"
              : "bg-slate-800 text-slate-500 border-slate-700"
          }`}
        >
          Auto-scroll {autoScroll() ? "ON" : "OFF"}
        </button>

        <div class="text-xs text-slate-500 font-mono">
          {filteredEvents().length} events
        </div>
      </div>

      {/* Log Output */}
      <div
        ref={containerRef}
        class="flex-1 bg-slate-950 rounded-lg border border-slate-800 overflow-y-auto font-mono text-xs leading-relaxed"
      >
        {filteredEvents().length === 0 ? (
          <div class="flex items-center justify-center h-full">
            <div class="text-center">
              <div class="w-10 h-10 rounded-full bg-slate-800 border border-slate-700 flex items-center justify-center mx-auto mb-3">
                <svg class="w-5 h-5 text-slate-500" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor">
                  <path stroke-linecap="round" stroke-linejoin="round" d="M9.348 14.651a3.75 3.75 0 010-5.303m5.304 0a3.75 3.75 0 010 5.303m-7.425 2.122a6.75 6.75 0 010-9.546m9.546 0a6.75 6.75 0 010 9.546M5.106 18.894c-3.808-3.808-3.808-9.98 0-13.789m13.788 0c3.808 3.808 3.808 9.981 0 13.79M12 12h.008v.007H12V12zm.375 0a.375.375 0 11-.75 0 .375.375 0 01.75 0z" />
                </svg>
              </div>
              <p class="text-xs text-slate-500">Waiting for telemetry events...</p>
              <p class="text-[10px] text-slate-600 mt-1">Events will appear here as pipelines emit UDP telemetry</p>
            </div>
          </div>
        ) : (
          <table class="w-full">
            <tbody>
              <For each={filteredEvents()}>
                {(event) => (
                  <tr class="hover:bg-slate-800/50 border-b border-slate-800/30">
                    <td class="px-3 py-1 text-slate-600 whitespace-nowrap align-top w-20">
                      {formatTimestamp(event.timestamp)}
                    </td>
                    <td class={`px-2 py-1 whitespace-nowrap align-top w-24 font-semibold ${typeColor(event.type)}`}>
                      {event.type}
                    </td>
                    <td class="px-2 py-1 text-cyan-300 whitespace-nowrap align-top w-32">
                      {event.pipeline_id}
                    </td>
                    <td class="px-2 py-1 text-slate-400 break-all">
                      {formatPayload(event.payload)}
                    </td>
                  </tr>
                )}
              </For>
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
};

export default LogViewer;
