import { createSignal, createResource, createEffect, For, Show } from "solid-js";
import type { Component } from "solid-js";
import { GitHubService } from "../services/github";
import { PipelineService } from "../services/pipeline";
import type { PipelineInfo } from "../services/pipeline";

// A deployed pipeline matched to this repo
export interface RepoPipeline {
  name: string;
  status: string;
  phase: string;
  branch: string;
  path: string;
  mode: string;
}

interface BranchInfo {
  name: string;
  sha: string;
  protected: boolean;
  category: "production" | "staging" | "draft" | "other";
  pipelines: RepoPipeline[];
}

interface PipelineContextProps {
  githubService: GitHubService;
  pipelineService: PipelineService;
  owner: string;
  repo: string;
  defaultBranch: string;
  activeBranch: string | null;
  onSelectBranch: (branch: string) => void;
  onCreateDraft: (baseBranch: string) => void;
  onChangeRepo: () => void;
  onPipelinePaths: (paths: string[]) => void;
  onSelectPipeline?: (pipelineId: string) => void;
}

const categorizeBranch = (name: string, defaultBranch: string): BranchInfo["category"] => {
  if (name === defaultBranch || name === "main" || name === "master" || name === "production") {
    return "production";
  }
  if (name === "staging" || name === "preview" || name === "develop" || name.startsWith("release/")) {
    return "staging";
  }
  if (name.startsWith("clotho-draft/")) {
    return "draft";
  }
  return "other";
};

const categoryLabel: Record<string, string> = {
  production: "Production",
  staging: "Staging / Preview",
  draft: "Your Drafts",
  other: "Other Branches",
};

const categoryOrder: Record<string, number> = {
  production: 0,
  staging: 1,
  draft: 2,
  other: 3,
};

const PipelineContext: Component<PipelineContextProps> = (props) => {
  const [expandedSection, setExpandedSection] = createSignal<string | null>("pipelines");
  const [isPipelineModalOpen, setIsPipelineModalOpen] = createSignal(false);
  const [pipelineSearch, setPipelineSearch] = createSignal("");

  // Fetch pipelines matched to this repo via git_repository URL
  const [repoPipelines] = createResource(
    () => ({ owner: props.owner, repo: props.repo }),
    async ({ owner, repo }) => {
      try {
        return await props.pipelineService.listPipelinesByRepo(owner, repo);
      } catch (err) {
        console.error('[PipelineContext] listPipelinesByRepo failed:', err);
        return [] as PipelineInfo[];
      }
    }
  );

  // Fetch branches
  const [branches] = createResource(
    () => ({ owner: props.owner, repo: props.repo }),
    async ({ owner, repo }) => {
      try {
        return await props.githubService.listBranches(owner, repo);
      } catch (e) {
        console.error("Failed to list branches:", e);
        return [];
      }
    }
  );

  // Whenever pipelines load, emit the unique paths so IDEPage can scope the file tree
  createEffect(() => {
    const pList = repoPipelines();
    if (pList && pList.length > 0) {
      const paths = [...new Set(pList.map((p) => p.path || "").filter(Boolean))];
      props.onPipelinePaths(paths);
    }
  });

  // Map pipelines onto branches
  const enrichedBranches = (): BranchInfo[] => {
    const branchList = branches() || [];
    const pipelineList = repoPipelines() || [];

    return branchList
      .map((b) => {
        const category = categorizeBranch(b.name, props.defaultBranch);
        // Match pipelines whose git_ref === this branch name
        const matched = pipelineList
          .filter((p) => (p.git_ref || props.defaultBranch) === b.name)
          .map((p) => ({
            name: p.id,
            status: p.status || p.phase,
            phase: p.phase,
            branch: p.git_ref || props.defaultBranch,
            path: p.path || "/",
            mode: p.mode,
          }));

        return { ...b, category, pipelines: matched };
      })
      .sort((a, b) => {
        // Branches with pipelines first within each category
        const catDiff = (categoryOrder[a.category] ?? 9) - (categoryOrder[b.category] ?? 9);
        if (catDiff !== 0) return catDiff;
        if (a.pipelines.length !== b.pipelines.length) return b.pipelines.length - a.pipelines.length;
        return a.name.localeCompare(b.name);
      });
  };

  const groupedBranches = () => {
    const groups: Record<string, BranchInfo[]> = {};
    for (const b of enrichedBranches()) {
      if (!groups[b.category]) groups[b.category] = [];
      groups[b.category].push(b);
    }
    return groups;
  };

  const allRepoPipelines = (): RepoPipeline[] => {
    return (repoPipelines() || []).map((p) => ({
      name: p.id,
      status: p.status || p.phase,
      phase: p.phase,
      branch: p.git_ref || props.defaultBranch,
      path: p.path || "/",
      mode: p.mode,
    }));
  };

  const username = () => localStorage.getItem("github_username") || "user";

  const myDrafts = () =>
    enrichedBranches().filter(
      (b) => b.category === "draft" && b.name.includes(`/${username()}/`)
    );

  const statusDot = (status?: string) => {
    if (!status) return "bg-slate-600";
    switch (status.toLowerCase()) {
      case "running":
      case "streaming":
        return "bg-green-500";
      case "enabled":
        return "bg-blue-500";
      case "failed":
        return "bg-red-500";
      case "stopped":
      case "idling":
        return "bg-slate-400";
      default:
        return "bg-yellow-500";
    }
  };

  const statusText = (status?: string) => {
    if (!status) return "text-slate-500";
    switch (status.toLowerCase()) {
      case "running":
      case "streaming":
        return "text-green-400";
      case "failed":
        return "text-red-400";
      default:
        return "text-slate-500";
    }
  };

  const toggleSection = (section: string) => {
    setExpandedSection((prev) => (prev === section ? null : section));
  };

  const openPipelineModal = () => {
    setIsPipelineModalOpen(true);
  };

  const closePipelineModal = () => {
    setIsPipelineModalOpen(false);
    setPipelineSearch("");
  };

  const filteredPipelines = () => {
    const search = pipelineSearch().toLowerCase();
    if (!search) return allRepoPipelines();
    return allRepoPipelines().filter(p =>
      p.name.toLowerCase().includes(search) ||
      p.branch.toLowerCase().includes(search) ||
      p.path.toLowerCase().includes(search)
    );
  };

  return (
    <div class="h-full flex flex-col bg-slate-900 border-r border-slate-800 overflow-hidden">
      <div class="flex-1 overflow-y-auto">
        {/* ── Deployed Pipelines Section ────────────────────────────────── */}
        <div class="px-3 py-2 border-b border-slate-800">
          <button
            onClick={openPipelineModal}
            class="w-full flex items-center justify-between px-2 py-1.5 text-xs font-medium text-slate-300 bg-slate-800/50 hover:bg-slate-800 border border-slate-700 rounded transition-colors"
          >
            <span class="flex items-center gap-1.5">
              <svg class="w-3.5 h-3.5 text-slate-500" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" d="M2.25 12.75V12A2.25 2.25 0 014.5 9.75h15A2.25 2.25 0 0121.75 12v.75m-8.69-6.44l-2.12-2.12a1.5 1.5 0 00-1.061-.44H4.5A2.25 2.25 0 002.25 6v12a2.25 2.25 0 002.25 2.25h15A2.25 2.25 0 0021.75 18V9a2.25 2.25 0 00-2.25-2.25h-5.379a1.5 1.5 0 01-1.06-.44z" />
              </svg>
              Pipelines
            </span>
            <span class="text-[10px] text-slate-500">{allRepoPipelines().length}</span>
          </button>

          {/* Show currently selected pipeline */}
          <Show when={props.onSelectPipeline && allRepoPipelines().length > 0}>
            <div class="mt-2 space-y-1">
              <For each={allRepoPipelines().slice(0, 3)}>
                {(pipeline) => (
                  <button
                    onClick={() => {
                      props.onSelectBranch(pipeline.branch);
                      props.onSelectPipeline?.(pipeline.name);
                    }}
                    class="w-full text-left px-2 py-1.5 rounded text-[10px] transition-colors hover:bg-slate-800/50 border border-transparent"
                  >
                    <div class="flex items-center gap-1.5">
                      <div class={`w-1.5 h-1.5 rounded-full flex-shrink-0 ${statusDot(pipeline.status)}`} />
                      <span class="text-slate-300 truncate font-medium">{pipeline.name}</span>
                      <span class={`text-[9px] ml-auto ${statusText(pipeline.status)}`}>
                        {pipeline.status}
                      </span>
                    </div>
                  </button>
                )}
              </For>
              <Show when={allRepoPipelines().length > 3}>
                <div class="text-[9px] text-slate-600 px-2">
                  +{allRepoPipelines().length - 3} more
                </div>
              </Show>
            </div>
          </Show>
        </div>

        {/* ── Branches Section ─────────────────────────────────────────── */}
        <div>
          <button
            onClick={() => toggleSection("branches")}
            class="w-full flex items-center gap-1.5 px-3 py-2 text-[10px] font-semibold uppercase tracking-wider text-slate-500 hover:text-slate-300 transition-colors border-b border-slate-800/50"
          >
            <svg
              class={`w-3 h-3 transition-transform ${expandedSection() === "branches" ? "rotate-90" : ""}`}
              fill="none" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor"
            >
              <path stroke-linecap="round" stroke-linejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
            </svg>
            Branches
            <span class="text-slate-600 ml-auto">{(branches() || []).length}</span>
          </button>

          <Show when={expandedSection() === "branches"}>
            <Show when={branches.loading}>
              <div class="flex items-center justify-center py-4 gap-2">
                <div class="w-3 h-3 border-2 border-slate-600 border-t-blue-400 rounded-full animate-spin" />
                <span class="text-[10px] text-slate-500">Loading branches...</span>
              </div>
            </Show>

            <Show when={!branches.loading}>
              <div class="py-1">
                <For each={Object.entries(groupedBranches())}>
                  {([category, categoryBranches]) => (
                    <div class="mb-1">
                      <div class="px-3 py-1 text-[9px] font-semibold uppercase tracking-wider text-slate-600">
                        {categoryLabel[category] || category}
                      </div>
                      <div class="px-1">
                        <For each={categoryBranches}>
                          {(branch) => {
                            const isActive = () => props.activeBranch === branch.name;
                            const hasPipeline = () => branch.pipelines.length > 0;
                            return (
                              <button
                                onClick={() => props.onSelectBranch(branch.name)}
                                class={`w-full text-left px-3 py-1.5 rounded-md text-xs transition-all ${
                                  isActive()
                                    ? "bg-blue-500/10 text-blue-400 border border-blue-500/20"
                                    : "text-slate-400 hover:text-slate-200 hover:bg-slate-800/50 border border-transparent"
                                }`}
                              >
                                <div class="flex items-center gap-2">
                                  <div class={`w-1.5 h-1.5 rounded-full flex-shrink-0 ${
                                    hasPipeline() ? statusDot(branch.pipelines[0].status) : "bg-slate-700"
                                  }`} />
                                  <span class="font-mono truncate">{branch.name}</span>
                                  <Show when={branch.protected}>
                                    <svg class="w-3 h-3 text-slate-600 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor">
                                      <path stroke-linecap="round" stroke-linejoin="round" d="M16.5 10.5V6.75a4.5 4.5 0 10-9 0v3.75m-.75 11.25h10.5a2.25 2.25 0 002.25-2.25v-6.75a2.25 2.25 0 00-2.25-2.25H6.75a2.25 2.25 0 00-2.25 2.25v6.75a2.25 2.25 0 002.25 2.25z" />
                                    </svg>
                                  </Show>
                                  <Show when={hasPipeline()}>
                                    <span class="ml-auto text-[9px] text-slate-600">
                                      {branch.pipelines.length} pipeline{branch.pipelines.length !== 1 ? "s" : ""}
                                    </span>
                                  </Show>
                                </div>
                                {/* Show pipeline details under branch */}
                                <Show when={hasPipeline()}>
                                  <For each={branch.pipelines}>
                                    {(p) => (
                                      <div class="flex items-center gap-1.5 mt-0.5 ml-3.5 text-[10px]">
                                        <span class={`font-semibold ${statusText(p.status)}`}>
                                          {p.name}
                                        </span>
                                        <Show when={p.path !== "/"}>
                                          <span class="text-slate-600 font-mono">{p.path}</span>
                                        </Show>
                                      </div>
                                    )}
                                  </For>
                                </Show>
                              </button>
                            );
                          }}
                        </For>

                        {/* Create draft from production/staging branches */}
                        <Show when={category === "production" || category === "staging"}>
                          <For each={categoryBranches}>
                            {(branch) => (
                              <button
                                onClick={() => props.onCreateDraft(branch.name)}
                                class="ml-5 mt-0.5 mb-1 flex items-center gap-1 text-[10px] text-blue-400/60 hover:text-blue-400 transition-colors"
                              >
                                <svg class="w-3 h-3" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor">
                                  <path stroke-linecap="round" stroke-linejoin="round" d="M12 4.5v15m7.5-7.5h-15" />
                                </svg>
                                Draft from {branch.name}
                              </button>
                            )}
                          </For>
                        </Show>
                      </div>
                    </div>
                  )}
                </For>
              </div>

              <Show when={myDrafts().length > 0}>
                <div class="px-3 py-2 border-t border-slate-800">
                  <div class="text-[10px] text-slate-600">
                    You have {myDrafts().length} active draft{myDrafts().length !== 1 ? "s" : ""}
                  </div>
                </div>
              </Show>
            </Show>
          </Show>
        </div>
      </div>

      {/* Legend */}
      <div class="px-3 py-2 border-t border-slate-800 flex-shrink-0">
        <div class="flex flex-wrap gap-x-3 gap-y-1">
          <div class="flex items-center gap-1">
            <div class="w-1.5 h-1.5 rounded-full bg-green-500" />
            <span class="text-[9px] text-slate-600">Running</span>
          </div>
          <div class="flex items-center gap-1">
            <div class="w-1.5 h-1.5 rounded-full bg-blue-500" />
            <span class="text-[9px] text-slate-600">Enabled</span>
          </div>
          <div class="flex items-center gap-1">
            <div class="w-1.5 h-1.5 rounded-full bg-red-500" />
            <span class="text-[9px] text-slate-600">Failed</span>
          </div>
          <div class="flex items-center gap-1">
            <div class="w-1.5 h-1.5 rounded-full bg-slate-700" />
            <span class="text-[9px] text-slate-600">No pipeline</span>
          </div>
        </div>
      </div>

      {/* Pipeline Selector Modal */}
      <Show when={isPipelineModalOpen()}>
        <div
          class="fixed inset-0 bg-black/50 backdrop-blur-sm z-50 flex items-center justify-center p-4"
          onClick={(e) => {
            if (e.target === e.currentTarget) closePipelineModal();
          }}
        >
          <div class="bg-slate-900 border border-slate-700 rounded-lg shadow-2xl w-full max-w-lg max-h-[80vh] overflow-hidden flex flex-col">
            {/* Modal Header */}
            <div class="flex items-center justify-between px-4 py-3 border-b border-slate-800 bg-slate-800/50">
              <div class="flex items-center gap-2">
                <svg class="w-4 h-4 text-slate-400" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor">
                  <path stroke-linecap="round" stroke-linejoin="round" d="M2.25 12.75V12A2.25 2.25 0 014.5 9.75h15A2.25 2.25 0 0121.75 12v.75m-8.69-6.44l-2.12-2.12a1.5 1.5 0 00-1.061-.44H4.5A2.25 2.25 0 002.25 6v12a2.25 2.25 0 002.25 2.25h15A2.25 2.25 0 0021.75 18V9a2.25 2.25 0 00-2.25-2.25h-5.379a1.5 1.5 0 01-1.06-.44z" />
                </svg>
                <span class="text-sm font-semibold text-slate-200">Select Pipeline</span>
                <span class="text-xs text-slate-500 font-mono">{filteredPipelines().length} / {allRepoPipelines().length}</span>
              </div>
              <button
                onClick={closePipelineModal}
                class="text-slate-400 hover:text-slate-200 transition-colors p-1 rounded hover:bg-slate-700/50"
              >
                <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                  <line x1="18" y1="6" x2="6" y2="18" />
                  <line x1="6" y1="6" x2="18" y2="18" />
                </svg>
              </button>
            </div>

            {/* Search */}
            <div class="px-4 py-3 border-b border-slate-800">
              <div class="relative">
                <svg class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-500" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor">
                  <path stroke-linecap="round" stroke-linejoin="round" d="M21 21l-5.197-5.197m0 0A7.5 7.5 0 105.196 5.196a7.5 7.5 0 0010.607 10.607z" />
                </svg>
                <input
                  type="text"
                  placeholder="Search pipelines..."
                  value={pipelineSearch()}
                  onInput={(e) => setPipelineSearch(e.currentTarget.value)}
                  class="w-full pl-9 pr-3 py-2 text-sm bg-slate-800 border border-slate-700 rounded text-slate-200 placeholder-slate-500 outline-none focus:border-blue-500"
                />
              </div>
            </div>

            {/* Pipeline List */}
            <div class="flex-1 overflow-y-auto p-2">
              <Show when={filteredPipelines().length === 0}>
                <div class="text-center py-8 text-slate-500 text-sm">
                  <Show when={pipelineSearch()} fallback={"No pipelines deployed from this repo yet."}>
                    No pipelines match "{pipelineSearch()}"
                  </Show>
                </div>
              </Show>

              <For each={filteredPipelines()}>
                {(pipeline) => (
                  <button
                    onClick={() => {
                      props.onSelectBranch(pipeline.branch);
                      props.onSelectPipeline?.(pipeline.name);
                      closePipelineModal();
                    }}
                    class={`w-full text-left p-3 rounded-lg mb-1 transition-all ${
                      props.activeBranch === pipeline.branch
                        ? "bg-blue-500/10 border border-blue-500/20"
                        : "hover:bg-slate-800/50 border border-transparent"
                    }`}
                  >
                    <div class="flex items-center justify-between">
                      <div class="flex items-center gap-2 min-w-0">
                        <div class={`w-2 h-2 rounded-full flex-shrink-0 ${statusDot(pipeline.status)}`} />
                        <span class="font-semibold text-white truncate">{pipeline.name}</span>
                      </div>
                      <span class={`text-xs font-medium flex-shrink-0 ${statusText(pipeline.status)}`}>
                        {pipeline.status}
                      </span>
                    </div>
                    <div class="flex items-center gap-3 mt-1.5 text-xs text-slate-500">
                      <span class="flex items-center gap-1">
                        <svg class="w-3 h-3" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor">
                          <path stroke-linecap="round" stroke-linejoin="round" d="M3 7.5L7.5 3m0 0L12 7.5M7.5 3v13.5m13.5-3L16.5 18m0 0L12 13.5m4.5 4.5V6" />
                        </svg>
                        {pipeline.branch}
                      </span>
                      <Show when={pipeline.path !== "/"}>
                        <span class="font-mono">{pipeline.path}</span>
                      </Show>
                      <span class="text-slate-600">{pipeline.mode}</span>
                    </div>
                  </button>
                )}
              </For>
            </div>

            {/* Footer - New Pipeline Button */}
            <div class="px-4 py-3 border-t border-slate-800 bg-slate-800/30">
              <button
                onClick={() => {
                  // Dead functionality - no backend API yet
                  alert("New Pipeline creation coming soon!");
                }}
                class="w-full flex items-center justify-center gap-2 px-3 py-2 text-sm font-medium text-slate-300 bg-slate-800 hover:bg-slate-700 border border-slate-700 rounded transition-colors"
              >
                <svg class="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor">
                  <path stroke-linecap="round" stroke-linejoin="round" d="M12 4.5v15m7.5-7.5h-15" />
                </svg>
                New Pipeline
              </button>
            </div>
          </div>
        </div>
      </Show>
    </div>
  );
};

export default PipelineContext;
