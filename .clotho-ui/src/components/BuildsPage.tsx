import { createSignal, createEffect, For, onMount, Show } from "solid-js";
import type { Component } from "solid-js";
import TopBar from "./TopBar";
import Sidebar from "./Sidebar";
import type { Environment } from "./EnvironmentSwitcher";
import {
  builds,
  setBuilds,
  fetchBuilds,
  connectBuildStream,
  formatDuration,
  formatRelativeTime,
  statusColor,
  statusIcon,
} from "../store/builds";

interface BuildsPageProps {
  environment: Environment;
  onSwitchEnvironment: (env: Environment) => void;
}

const BuildsPage: Component<BuildsPageProps> = (props) => {
  const [filter, setFilter] = createSignal<"all" | "running" | "completed" | "failed">("all");
  const [searchQuery, setSearchQuery] = createSignal("");
  const [loading, setLoading] = createSignal(true);

  onMount(async () => {
    setLoading(true);
    await fetchBuilds(props.environment);
    setLoading(false);
    connectBuildStream(() => props.environment);
  });

  createEffect(() => {
    fetchBuilds(props.environment);
  });

  const filteredBuilds = () => {
    let result = [...builds];
    const q = searchQuery().toLowerCase();
    if (q) {
      result = result.filter(
        (b) =>
          b.pipeline_name.toLowerCase().includes(q) ||
          b.pipeline_id.toLowerCase().includes(q) ||
          b.target_image.toLowerCase().includes(q) ||
          b.reference.toLowerCase().includes(q)
      );
    }
    if (filter() !== "all") {
      result = result.filter((b) => b.status === filter());
    }
    // Sort by started_at descending (newest first)
    return result.sort((a, b) => new Date(b.started_at).getTime() - new Date(a.started_at).getTime());
  };

  const stats = () => {
    const total = builds.length;
    const running = builds.filter((b) => b.status === "running").length;
    const completed = builds.filter((b) => b.status === "completed").length;
    const failed = builds.filter((b) => b.status === "failed").length;
    return { total, running, completed, failed };
  };

  return (
    <div class="min-h-screen bg-slate-900 text-white">
      <Sidebar activePage="builds" onNavigate={() => {}} />

      <div class="ml-56 flex flex-col min-h-screen">
        <TopBar
          pageTitle="Build History"
          environment={props.environment}
          onSwitchEnvironment={props.onSwitchEnvironment}
        />

        <main class="flex-1 px-8 py-6">
          {/* Stats Cards */}
          <div class="grid grid-cols-4 gap-4 mb-6">
            <div class="bg-slate-800/50 border border-slate-700 rounded-lg p-4">
              <div class="text-xs text-slate-400 uppercase tracking-wider">Total Builds</div>
              <div class="text-2xl font-bold text-white mt-1">{stats().total}</div>
            </div>
            <div class="bg-slate-800/50 border border-slate-700 rounded-lg p-4">
              <div class="text-xs text-slate-400 uppercase tracking-wider">Running</div>
              <div class="text-2xl font-bold text-blue-400 mt-1">{stats().running}</div>
            </div>
            <div class="bg-slate-800/50 border border-slate-700 rounded-lg p-4">
              <div class="text-xs text-slate-400 uppercase tracking-wider">Completed</div>
              <div class="text-2xl font-bold text-green-400 mt-1">{stats().completed}</div>
            </div>
            <div class="bg-slate-800/50 border border-slate-700 rounded-lg p-4">
              <div class="text-xs text-slate-400 uppercase tracking-wider">Failed</div>
              <div class="text-2xl font-bold text-red-400 mt-1">{stats().failed}</div>
            </div>
          </div>

          {/* Filters */}
          <div class="flex items-center gap-4 mb-6">
            <div class="flex items-center gap-2">
              {(["all", "running", "completed", "failed"] as const).map((f) => (
                <button
                  onClick={() => setFilter(f)}
                  class={`px-3 py-1.5 rounded-md text-xs font-medium transition-all ${
                    filter() === f
                      ? "bg-blue-500/20 text-blue-400 border border-blue-500/30"
                      : "text-slate-400 hover:text-slate-200 border border-slate-700 hover:border-slate-600"
                  }`}
                >
                  {f.charAt(0).toUpperCase() + f.slice(1)}
                </button>
              ))}
            </div>
            <div class="flex-1" />
            <input
              type="text"
              placeholder="Search pipelines, images, branches..."
              value={searchQuery()}
              onInput={(e) => setSearchQuery(e.currentTarget.value)}
              class="bg-slate-800 border border-slate-700 rounded-md px-3 py-1.5 text-sm text-white placeholder-slate-500 focus:outline-none focus:border-blue-500 w-72"
            />
          </div>

          {/* Build Table */}
          <Show when={!loading()} fallback={
            <div class="text-center py-20">
              <div class="w-8 h-8 border-2 border-blue-500 border-t-transparent rounded-full animate-spin mx-auto mb-4" />
              <p class="text-sm text-slate-400">Loading builds...</p>
            </div>
          }>
            <Show when={filteredBuilds().length > 0} fallback={
              <div class="text-center py-20">
                <div class="w-12 h-12 rounded-full bg-slate-800 border border-slate-700 flex items-center justify-center mx-auto mb-4">
                  <span class="text-slate-500 text-lg">⚙</span>
                </div>
                <h3 class="text-sm font-semibold text-slate-300">No build records</h3>
                <p class="text-xs text-slate-500 mt-1">
                  Builds are sourced from Kubernetes builder Jobs. Records appear after a pipeline
                  with a <span class="font-mono text-slate-400">gitRepository</span> has been built at least once.
                </p>
              </div>
            }>
              <div class="bg-slate-800/30 border border-slate-700 rounded-lg overflow-hidden">
                <table class="w-full text-sm">
                  <thead>
                    <tr class="border-b border-slate-700 bg-slate-800/50">
                      <th class="text-left px-4 py-3 text-xs font-medium text-slate-400 uppercase tracking-wider">Status</th>
                      <th class="text-left px-4 py-3 text-xs font-medium text-slate-400 uppercase tracking-wider">Pipeline</th>
                      <th class="text-left px-4 py-3 text-xs font-medium text-slate-400 uppercase tracking-wider">Branch</th>
                      <th class="text-left px-4 py-3 text-xs font-medium text-slate-400 uppercase tracking-wider">Image</th>
                      <th class="text-left px-4 py-3 text-xs font-medium text-slate-400 uppercase tracking-wider">Duration</th>
                      <th class="text-left px-4 py-3 text-xs font-medium text-slate-400 uppercase tracking-wider">Started</th>
                    </tr>
                  </thead>
                  <tbody>
                    <For each={filteredBuilds()}>
                      {(build) => (
                        <tr class="border-b border-slate-800 hover:bg-slate-800/30 transition-colors">
                          <td class="px-4 py-3">
                            <div class="flex items-center gap-2">
                              <span class={`text-sm ${statusColor(build.status)}`}>
                                {statusIcon(build.status)}
                              </span>
                              <span class={`text-xs font-medium capitalize ${statusColor(build.status)}`}>
                                {build.status}
                              </span>
                              {build.status === "running" && (
                                <span class="w-2 h-2 rounded-full bg-blue-400 animate-pulse" />
                              )}
                            </div>
                          </td>
                          <td class="px-4 py-3">
                            <div class="font-medium text-white">{build.pipeline_name}</div>
                            <div class="text-xs text-slate-500 truncate max-w-[200px]">{build.pipeline_id}</div>
                          </td>
                          <td class="px-4 py-3">
                            <div class="text-slate-300 font-mono text-xs">{build.reference}</div>
                            {build.path && build.path !== "" && (
                              <div class="text-xs text-slate-500">{build.path}</div>
                            )}
                          </td>
                          <td class="px-4 py-3">
                            <div class="font-mono text-xs text-blue-300 truncate max-w-[250px]" title={build.target_image}>
                              {build.target_image}
                            </div>
                          </td>
                          <td class="px-4 py-3">
                            <span class="text-slate-300 text-xs">
                              {build.status === "running"
                                ? "running..."
                                : formatDuration(build.duration_ms)}
                            </span>
                          </td>
                          <td class="px-4 py-3">
                            <span class="text-slate-400 text-xs" title={build.started_at}>
                              {formatRelativeTime(build.started_at)}
                            </span>
                          </td>
                        </tr>
                      )}
                    </For>
                  </tbody>
                </table>
              </div>
            </Show>
          </Show>
        </main>
      </div>
    </div>
  );
};

export default BuildsPage;