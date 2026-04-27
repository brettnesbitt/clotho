import { createSignal, createResource, For, Show } from "solid-js";
import type { Component } from "solid-js";
import { GitHubService } from "../services/github";

interface Repo {
  owner: string;
  name: string;
  description: string;
  default_branch: string;
}

interface RepoSelectorProps {
  githubService: GitHubService;
  onSelect: (owner: string, repo: string, defaultBranch: string) => void;
  onBack: () => void;
}

const RepoSelector: Component<RepoSelectorProps> = (props) => {
  const [search, setSearch] = createSignal("");
  const [manualMode, setManualMode] = createSignal(false);
  const [manualOwner, setManualOwner] = createSignal("");
  const [manualRepo, setManualRepo] = createSignal("");

  const [repos] = createResource(async () => {
    try {
      return await props.githubService.listRepositories();
    } catch (e: any) {
      console.error("Failed to list repos:", e);
      return [] as Repo[];
    }
  });

  const filtered = () => {
    const q = search().toLowerCase();
    if (!q) return repos() || [];
    return (repos() || []).filter(
      (r) =>
        r.name.toLowerCase().includes(q) ||
        r.owner.toLowerCase().includes(q) ||
        r.description.toLowerCase().includes(q)
    );
  };

  const handleManualSubmit = () => {
    const owner = manualOwner().trim();
    const repo = manualRepo().trim();
    if (owner && repo) {
      props.onSelect(owner, repo, "main");
    }
  };

  return (
    <div class="max-w-2xl mx-auto py-8 px-4">
      <div class="mb-6">
        <button
          onClick={props.onBack}
          class="text-xs text-slate-500 hover:text-slate-300 font-mono uppercase tracking-wider transition-colors"
        >
          &larr; Disconnect
        </button>
      </div>

      <h2 class="text-lg font-semibold text-white mb-1">Select Repository</h2>
      <p class="text-xs text-slate-500 mb-6">
        Choose the repository that contains your Clotho pipeline code.
      </p>

      {/* Search + Manual toggle */}
      <div class="flex items-center gap-2 mb-4">
        <div class="flex-1 relative">
          <svg
            class="w-4 h-4 text-slate-500 absolute left-3 top-1/2 -translate-y-1/2"
            fill="none"
            viewBox="0 0 24 24"
            stroke-width="1.5"
            stroke="currentColor"
          >
            <path
              stroke-linecap="round"
              stroke-linejoin="round"
              d="M21 21l-5.197-5.197m0 0A7.5 7.5 0 105.196 5.196a7.5 7.5 0 0010.607 10.607z"
            />
          </svg>
          <input
            type="text"
            placeholder="Search repositories..."
            value={search()}
            onInput={(e) => setSearch(e.currentTarget.value)}
            class="w-full pl-9 pr-3 py-2 bg-slate-800 border border-slate-700 rounded-md text-sm text-white placeholder-slate-500 focus:outline-none focus:border-blue-500 transition-colors"
          />
        </div>
        <button
          onClick={() => setManualMode(!manualMode())}
          class={`px-3 py-2 rounded-md text-xs font-semibold border transition-all ${
            manualMode()
              ? "bg-blue-500/10 text-blue-400 border-blue-500/30"
              : "text-slate-400 border-slate-700 hover:border-slate-600 hover:text-slate-300"
          }`}
        >
          Manual
        </button>
      </div>

      {/* Manual entry */}
      <Show when={manualMode()}>
        <div class="bg-slate-800/50 border border-slate-700 rounded-lg p-4 mb-4">
          <div class="text-xs text-slate-400 font-semibold uppercase tracking-wider mb-3">
            Enter Repository
          </div>
          <div class="flex gap-2">
            <input
              type="text"
              placeholder="owner"
              value={manualOwner()}
              onInput={(e) => setManualOwner(e.currentTarget.value)}
              class="flex-1 px-3 py-2 bg-slate-900 border border-slate-700 rounded text-sm text-white font-mono placeholder-slate-600 focus:outline-none focus:border-blue-500"
            />
            <span class="text-slate-600 self-center font-mono text-lg">/</span>
            <input
              type="text"
              placeholder="repo"
              value={manualRepo()}
              onInput={(e) => setManualRepo(e.currentTarget.value)}
              onKeyDown={(e) => e.key === "Enter" && handleManualSubmit()}
              class="flex-1 px-3 py-2 bg-slate-900 border border-slate-700 rounded text-sm text-white font-mono placeholder-slate-600 focus:outline-none focus:border-blue-500"
            />
            <button
              onClick={handleManualSubmit}
              disabled={!manualOwner().trim() || !manualRepo().trim()}
              class="px-4 py-2 bg-blue-500 text-white text-xs font-semibold rounded hover:bg-blue-600 transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
            >
              Open
            </button>
          </div>
        </div>
      </Show>

      {/* Repo list */}
      <Show when={repos.loading}>
        <div class="flex items-center justify-center py-12 gap-3">
          <div class="w-4 h-4 border-2 border-slate-600 border-t-blue-400 rounded-full animate-spin" />
          <span class="text-xs text-slate-500">Loading repositories...</span>
        </div>
      </Show>

      <Show when={!repos.loading}>
        <div class="space-y-1 max-h-[60vh] overflow-y-auto">
          <For each={filtered()}>
            {(repo) => (
              <button
                onClick={() => props.onSelect(repo.owner, repo.name, repo.default_branch)}
                class="w-full text-left px-4 py-3 rounded-lg border border-transparent hover:border-slate-700 hover:bg-slate-800/50 transition-all group"
              >
                <div class="flex items-center justify-between">
                  <div class="flex items-center gap-2">
                    <span class="text-sm font-mono text-slate-300 group-hover:text-white transition-colors">
                      {repo.owner}/<span class="font-semibold text-white">{repo.name}</span>
                    </span>
                  </div>
                  <span class="text-[10px] font-mono text-slate-600 bg-slate-800 px-2 py-0.5 rounded">
                    {repo.default_branch}
                  </span>
                </div>
                <Show when={repo.description}>
                  <p class="text-xs text-slate-500 mt-1 truncate">{repo.description}</p>
                </Show>
              </button>
            )}
          </For>

          <Show when={filtered().length === 0 && !repos.loading}>
            <div class="text-center py-12">
              <p class="text-xs text-slate-500">
                {search() ? "No repositories match your search." : "No repositories found."}
              </p>
              <button
                onClick={() => setManualMode(true)}
                class="mt-2 text-xs text-blue-400 hover:text-blue-300 transition-colors"
              >
                Enter a repository manually
              </button>
            </div>
          </Show>
        </div>
      </Show>
    </div>
  );
};

export default RepoSelector;
