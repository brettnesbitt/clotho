import { For, Show, createSignal, createMemo } from "solid-js";
import type { Component } from "solid-js";
import { pipelineList, pipelineCount, type PipelineState } from "../store/pipelines";
import PipelineCard from "./PipelineCard";
import PipelineTable from "./PipelineTable";

type ViewMode = "cards" | "table";
type StatusFilter = "all" | "Running" | "Enabled" | "Idling" | "Failed" | "Streaming" | "ZOMBIE";

interface OverviewProps {
  onSelectPipeline?: (id: string) => void;
}

const ITEMS_PER_PAGE = 9;

const Overview: Component<OverviewProps> = (props) => {
  const [viewMode, setViewMode] = createSignal<ViewMode>("cards");
  const [statusFilter, setStatusFilter] = createSignal<StatusFilter>("all");
  const [currentPage, setCurrentPage] = createSignal(1);
  const [searchQuery, setSearchQuery] = createSignal("");

  const recentlyInvoked = (iso?: string) => {
    if (!iso) return false;
    const ts = new Date(iso).getTime();
    if (Number.isNaN(ts)) return false;
    return Date.now() - ts < 30_000;
  };

  const enabledCount = () =>
    pipelineList().filter(
      p => p.status === "Enabled" || p.status === "Running" || p.status === "Streaming"
    ).length;

  const activeCount = () =>
    pipelineList().filter(
      p => p.status === "Running" || p.status === "Streaming" || recentlyInvoked(p.last_invocation)
    ).length;

  const failedCount = () =>
    pipelineList().filter(p => p.status === "Failed").length;

  // Sort pipelines: Running/Streaming first, then by name
  const sortedPipelines = createMemo(() => {
    const list = pipelineList();
    return list.sort((a, b) => {
      const aIsActive = a.status === "Running" || a.status === "Streaming";
      const bIsActive = b.status === "Running" || b.status === "Streaming";
      if (aIsActive && !bIsActive) return -1;
      if (!aIsActive && bIsActive) return 1;
      return a.id.localeCompare(b.id);
    });
  });

  // Filter pipelines by status and search query
  const filteredPipelines = createMemo(() => {
    let result = sortedPipelines();

    // Apply status filter
    const filter = statusFilter();
    if (filter !== "all") {
      result = result.filter(p => p.status === filter);
    }

    // Apply search filter
    const query = searchQuery().toLowerCase().trim();
    if (query) {
      result = result.filter(p =>
        p.id.toLowerCase().includes(query) ||
        p.status.toLowerCase().includes(query) ||
        p.mode?.toLowerCase().includes(query) ||
        p.branch?.toLowerCase().includes(query) ||
        p.git_repository?.toLowerCase().includes(query)
      );
    }

    return result;
  });

  // Pagination
  const totalPages = () => Math.max(1, Math.ceil(filteredPipelines().length / ITEMS_PER_PAGE));
  const paginatedPipelines = createMemo(() => {
    const start = (currentPage() - 1) * ITEMS_PER_PAGE;
    const end = start + ITEMS_PER_PAGE;
    return filteredPipelines().slice(start, end);
  });

  // Reset page when filter or search changes
  const handleFilterChange = (filter: StatusFilter) => {
    setStatusFilter(filter);
    setCurrentPage(1);
  };

  const handleSearchChange = (query: string) => {
    setSearchQuery(query);
    setCurrentPage(1);
  };

  const statusOptions: { value: StatusFilter; label: string; color: string }[] = [
    { value: "all", label: "All", color: "text-slate-400" },
    { value: "Running", label: "Running", color: "text-green-400" },
    { value: "Streaming", label: "Streaming", color: "text-emerald-400" },
    { value: "Enabled", label: "Enabled", color: "text-blue-400" },
    { value: "Idling", label: "Idling", color: "text-slate-400" },
    { value: "Failed", label: "Failed", color: "text-red-400" },
    { value: "ZOMBIE", label: "Zombie", color: "text-yellow-400" },
  ];

  const getStatusCount = (status: StatusFilter) => {
    if (status === "all") return pipelineCount();
    return pipelineList().filter(p => p.status === status).length;
  };

  return (
    <div class="space-y-6">
      {/* Stats Row */}
      <div class="grid grid-cols-4 gap-4">
        <div class="bg-slate-800/50 rounded-lg border border-slate-700/50 p-4">
          <div class="text-xs text-slate-500 uppercase tracking-wider font-semibold mb-1">Total Pipelines</div>
          <div class="text-3xl font-mono font-bold text-white">
            {pipelineCount()}
          </div>
        </div>
        <div class="bg-slate-800/50 rounded-lg border border-slate-700/50 p-4">
          <div class="text-xs text-slate-500 uppercase tracking-wider font-semibold mb-1">Enabled</div>
          <div class="text-3xl font-mono font-bold text-blue-400">
            {enabledCount()}
          </div>
        </div>
        <div class="bg-slate-800/50 rounded-lg border border-slate-700/50 p-4">
          <div class="text-xs text-slate-500 uppercase tracking-wider font-semibold mb-1">Active</div>
          <div class="text-3xl font-mono font-bold text-green-400">
            {activeCount()}
          </div>
        </div>
        <div class="bg-slate-800/50 rounded-lg border border-slate-700/50 p-4">
          <div class="text-xs text-slate-500 uppercase tracking-wider font-semibold mb-1">Failed</div>
          <div class="text-3xl font-mono font-bold text-red-400">
            {failedCount()}
          </div>
        </div>
      </div>

      {/* Controls Row: Search + Filter + View Toggle */}
      <div class="flex items-center justify-between flex-wrap gap-4">
        {/* Search */}
        <div class="flex-1 min-w-[200px] max-w-md">
          <div class="relative">
            <svg class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-500" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor">
              <path stroke-linecap="round" stroke-linejoin="round" d="M21 21l-5.197-5.197m0 0A7.5 7.5 0 105.196 5.196a7.5 7.5 0 0010.607 10.607z" />
            </svg>
            <input
              type="text"
              placeholder="Search pipelines..."
              value={searchQuery()}
              onInput={(e) => handleSearchChange(e.currentTarget.value)}
              class="w-full pl-9 pr-3 py-1.5 text-sm bg-slate-800 border border-slate-700 rounded-lg text-slate-200 placeholder-slate-500 outline-none focus:border-blue-500 transition-colors"
            />
            <Show when={searchQuery()}>
              <button
                onClick={() => handleSearchChange("")}
                class="absolute right-2 top-1/2 -translate-y-1/2 text-slate-500 hover:text-slate-300 p-0.5"
              >
                <svg class="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor">
                  <path stroke-linecap="round" stroke-linejoin="round" d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </Show>
          </div>
        </div>

        <div class="flex items-center gap-4">
          {/* Status Filter */}
          <div class="flex items-center gap-2">
            <span class="text-xs text-slate-500 uppercase tracking-wider font-semibold">Filter:</span>
            <div class="flex gap-1 bg-slate-800 rounded-lg p-0.5 border border-slate-700">
              <For each={statusOptions}>
                {(option) => (
                  <button
                    onClick={() => handleFilterChange(option.value)}
                    class={`px-2.5 py-1 rounded text-xs font-medium transition-all ${
                      statusFilter() === option.value
                        ? "bg-slate-700 text-white"
                        : "text-slate-500 hover:text-slate-300"
                    }`}
                    title={`${option.label} (${getStatusCount(option.value)})`}
                  >
                    <span class={statusFilter() === option.value ? option.color : ""}>
                      {option.label}
                    </span>
                    <span class="ml-1 text-slate-500">({getStatusCount(option.value)})</span>
                  </button>
                )}
              </For>
            </div>
          </div>

          {/* View Toggle */}
          <div class="flex gap-1 bg-slate-800 rounded-lg p-0.5 border border-slate-700">
            <button
              onClick={() => setViewMode("cards")}
              class={`px-3 py-1.5 rounded text-xs font-semibold transition-all ${
                viewMode() === "cards"
                  ? "bg-slate-700 text-white"
                  : "text-slate-500 hover:text-slate-300"
              }`}
            >
              <svg class="w-3.5 h-3.5 inline mr-1.5 -mt-0.5" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" d="M3.75 6A2.25 2.25 0 016 3.75h2.25A2.25 2.25 0 0110.5 6v2.25a2.25 2.25 0 01-2.25 2.25H6a2.25 2.25 0 01-2.25-2.25V6zM3.75 15.75A2.25 2.25 0 016 13.5h2.25a2.25 2.25 0 012.25 2.25V18a2.25 2.25 0 01-2.25 2.25H6A2.25 2.25 0 013.75 18v-2.25zM13.5 6a2.25 2.25 0 012.25-2.25H18A2.25 2.25 0 0120.25 6v2.25A2.25 2.25 0 0118 10.5h-2.25a2.25 2.25 0 01-2.25-2.25V6zM13.5 15.75a2.25 2.25 0 012.25-2.25H18a2.25 2.25 0 012.25 2.25V18A2.25 2.25 0 0118 20.25h-2.25A2.25 2.25 0 0113.5 18v-2.25z" />
              </svg>
              Cards
            </button>
            <button
              onClick={() => setViewMode("table")}
              class={`px-3 py-1.5 rounded text-xs font-semibold transition-all ${
                viewMode() === "table"
                  ? "bg-slate-700 text-white"
                  : "text-slate-500 hover:text-slate-300"
              }`}
            >
              <svg class="w-3.5 h-3.5 inline mr-1.5 -mt-0.5" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" d="M3.75 12h16.5m-16.5 3.75h16.5M3.75 19.5h16.5M5.625 4.5h12.75a1.875 1.875 0 010 3.75H5.625a1.875 1.875 0 010-3.75z" />
              </svg>
              Table
            </button>
          </div>
        </div>
      </div>

      {/* Results Count */}
      <div class="text-xs text-slate-500">
        Showing {paginatedPipelines().length} of {filteredPipelines().length} pipelines
        {statusFilter() !== "all" && ` (filtered by ${statusFilter()})`}
      </div>

      {/* Cards View */}
      {viewMode() === "cards" && (
        <>
          <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-5">
            <For each={paginatedPipelines()}>
              {(pipeline) => (
                <div onClick={() => props.onSelectPipeline?.(pipeline.id)} class="cursor-pointer">
                  <PipelineCard data={pipeline} />
                </div>
              )}
            </For>
          </div>
          
          {/* Pagination Controls */}
          {totalPages() > 1 && (
            <div class="flex items-center justify-center gap-2 mt-6">
              <button
                onClick={() => setCurrentPage(p => Math.max(1, p - 1))}
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
                      onClick={() => setCurrentPage(page)}
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
                onClick={() => setCurrentPage(p => Math.min(totalPages(), p + 1))}
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
        </>
      )}

      {/* Table View */}
      {viewMode() === "table" && (
        <PipelineTable 
          pipelines={filteredPipelines()} 
          onSelect={props.onSelectPipeline}
          currentPage={currentPage()}
          totalPages={totalPages()}
          onPageChange={setCurrentPage}
          itemsPerPage={ITEMS_PER_PAGE}
        />
      )}

      {/* Empty State */}
      {filteredPipelines().length === 0 && (
        <div class="text-center py-20">
          <div class="w-12 h-12 rounded-full bg-slate-800 border border-slate-700 flex items-center justify-center mx-auto mb-4">
            <svg class="w-6 h-6 text-slate-500" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor">
              <path stroke-linecap="round" stroke-linejoin="round" d="M9.348 14.651a3.75 3.75 0 010-5.303m5.304 0a3.75 3.75 0 010 5.303m-7.425 2.122a6.75 6.75 0 010-9.546m9.546 0a6.75 6.75 0 010 9.546M5.106 18.894c-3.808-3.808-3.808-9.98 0-13.789m13.788 0c3.808 3.808 3.808 9.981 0 13.79M12 12h.008v.007H12V12zm.375 0a.375.375 0 11-.75 0 .375.375 0 01.75 0z" />
            </svg>
          </div>
          <h3 class="text-sm font-semibold text-slate-300">
            {statusFilter() === "all" ? "No pipelines detected" : `No ${statusFilter()} pipelines`}
          </h3>
          <p class="text-xs text-slate-500 mt-1 max-w-xs mx-auto">
            {statusFilter() === "all" 
              ? "Deploy a Pipeline CR to your cluster to see it here."
              : "Try selecting a different filter to see more pipelines."
            }
          </p>
        </div>
      )}
    </div>
  );
};

export default Overview;
