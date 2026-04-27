/**
 * PipelineDAGVisualizer.tsx
 * 
 * Interactive DAG visualization for multi-stage pipelines.
 * Shows stages, bus connections, branching points, and real-time metrics.
 */
import { For, Show, createSignal, createMemo, createEffect, onMount } from "solid-js";
import type { Component } from "solid-js";
import type { PipelineStage } from "../store/pipelines";
import DataSamplePanel from "./DataSamplePanel";

interface StageMetrics {
  recordsIn: number;
  recordsOut: number;
  recordsFailed: number;
  recordsBranched: number;
  throughputPerSec: number;
  lagMs: number;
}

interface PipelineStep {
  name: string;
  stepType: "source" | "transform" | "filter" | "branch" | "tee" | "sink";
  metrics?: StageMetrics;
}

interface StageWithMetrics extends PipelineStage {
  metrics?: StageMetrics;
  status?: "running" | "pending" | "failed" | "completed";
  steps?: PipelineStep[]; // Internal pipeline steps
}

interface BusConnection {
  from: string;
  to: string;
  throughput: number;
  lagMs: number;
  pending: number;
}

interface BranchInfo {
  stage: string;
  condition: string;
  rejectedCount: number;
  acceptedCount: number;
}

interface DataSample {
  pipeline_id: string;
  stage_name: string;
  step_name: string;
  payload_in: string;
  payload_out: string;
  timestamp: number;
}

interface PipelineDAGVisualizerProps {
  stages: StageWithMetrics[];
  busConnections: BusConnection[];
  branches: BranchInfo[];
  pipelineMode: string;
  dataSamplesMap?: Record<string, DataSample>;
}

const PipelineDAGVisualizer: Component<PipelineDAGVisualizerProps> = (props) => {
  const [selectedStage, setSelectedStage] = createSignal<string | null>(null);
  const [selectedStep, setSelectedStep] = createSignal<string | null>(null);
  const [hoveredConnection, setHoveredConnection] = createSignal<string | null>(null);
  
  // Pan and zoom state
  const [zoom, setZoom] = createSignal(1);
  const [panX, setPanX] = createSignal(0);
  const [panY, setPanY] = createSignal(0);
  const [isDragging, setIsDragging] = createSignal(false);
  const [dragStart, setDragStart] = createSignal({ x: 0, y: 0 });
  let containerRef: HTMLDivElement | undefined;

  const stagePositions = createMemo(() => {
    const positions: Record<string, { x: number; y: number; level: number }> = {};
    const levels: string[][] = [];

    const findRoots = () => {
      return props.stages.filter(s => !s.dependsOn || s.dependsOn.length === 0).map(s => s.name);
    };

    const assigned = new Set<string>();
    let currentLevel = findRoots();
    let levelIdx = 0;

    while (currentLevel.length > 0) {
      levels[levelIdx] = currentLevel;
      currentLevel.forEach(name => assigned.add(name));

      const nextLevel: string[] = [];
      props.stages.forEach(s => {
        if (assigned.has(s.name)) return;
        if (s.dependsOn?.some(dep => assigned.has(dep))) {
          if (!nextLevel.includes(s.name)) nextLevel.push(s.name);
        }
      });

      currentLevel = nextLevel;
      levelIdx++;
    }

    const STAGE_WIDTH = 180;
    const STAGE_HEIGHT = 80;
    const LEVEL_GAP = 240;
    const STAGE_GAP = 20;

    levels.forEach((level, idx) => {
      const totalHeight = level.length * STAGE_HEIGHT + (level.length - 1) * STAGE_GAP;
      const startY = -totalHeight / 2;

      level.forEach((stageName, sIdx) => {
        positions[stageName] = {
          x: idx * LEVEL_GAP,
          y: startY + sIdx * (STAGE_HEIGHT + STAGE_GAP),
          level: idx
        };
      });
    });

    return positions;
  });

  // Calculate content bounds for centering
  const contentBounds = createMemo(() => {
    const positions = Object.values(stagePositions());
    if (positions.length === 0) return { width: 0, height: 0, minX: 0, minY: 0 };
    
    const xs = positions.map(p => p.x);
    const ys = positions.map(p => p.y);
    const minX = Math.min(...xs) - 100;
    const maxX = Math.max(...xs) + 280;
    const minY = Math.min(...ys) - 60;
    const maxY = Math.max(...ys) + 140;
    
    return {
      width: maxX - minX,
      height: maxY - minY,
      minX,
      minY,
      centerX: (minX + maxX) / 2,
      centerY: (minY + maxY) / 2
    };
  });

  // Auto-center and fit content within the viewport
  const centerContent = () => {
    const bounds = contentBounds();
    if (bounds.width === 0 || bounds.height === 0) return;
    
    const cw = containerRef?.clientWidth || 800;
    const ch = containerRef?.clientHeight || 384;
    const PADDING = 40;
    
    const scaleX = (cw - PADDING * 2) / bounds.width;
    const scaleY = (ch - PADDING * 2) / bounds.height;
    const fitZoom = Math.min(scaleX, scaleY, 1.5); // cap at 1.5x
    const clampedZoom = Math.max(0.4, fitZoom);
    
    // Center: offset so content center maps to viewport center
    const offsetX = (cw / 2) - ((bounds.minX + bounds.width / 2) * clampedZoom);
    const offsetY = (ch / 2) - ((bounds.minY + bounds.height / 2) * clampedZoom);
    
    setZoom(clampedZoom);
    setPanX(offsetX);
    setPanY(offsetY);
  };

  // Re-center when stages change
  createEffect(() => {
    const _ = props.stages.length; // track dependency
    centerContent();
  });

  // Also center once container is mounted
  onMount(() => {
    // Small delay to ensure container has layout dimensions
    requestAnimationFrame(() => centerContent());
  });

  const formatNumber = (n: number) => {
    if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
    if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
    return n.toString();
  };

  const formatDuration = (ms: number) => {
    if (ms < 1000) return `${ms}ms`;
    return `${(ms / 1000).toFixed(1)}s`;
  };

  const getStageColor = (status?: string) => {
    switch (status) {
      case "running": return "#22c55e";  // Green
      case "failed": return "#ef4444";   // Red
      case "completed": return "#3b82f6"; // Blue
      case "pending": return "#f59e0b";   // Amber/Yellow
      default: return "#64748b";           // Slate/Gray
    }
  };

  const getStepTypeIcon = (stepType?: string) => {
    switch (stepType) {
      case "source": return "→";
      case "transform": return "⚡";
      case "filter": return "🔍";
      case "branch": return "⚡";
      case "tee": return "├";
      case "sink": return "■";
      default: return "●";
    }
  };

  const getStepTypeColor = (stepType?: string) => {
    switch (stepType) {
      case "source": return "#22c55e";    // Green
      case "transform": return "#3b82f6"; // Blue
      case "filter": return "#f59e0b";    // Amber
      case "branch": return "#a855f7";    // Purple
      case "tee": return "#06b6d4";       // Cyan
      case "sink": return "#64748b";      // Slate
      default: return "#94a3b8";
    }
  };

  const getConnectionPath = (from: string, to: string) => {
    const pos = stagePositions();
    const fromPos = pos[from];
    const toPos = pos[to];
    if (!fromPos || !toPos) return "";

    const startX = fromPos.x + 90;
    const startY = fromPos.y + 40;
    const endX = toPos.x - 90;
    const endY = toPos.y + 40;

    const midX = (startX + endX) / 2;
    return `M ${startX} ${startY} C ${midX} ${startY}, ${midX} ${endY}, ${endX} ${endY}`;
  };

  const getBranchPath = (stage: string) => {
    const pos = stagePositions();
    const stagePos = pos[stage];
    if (!stagePos) return "";

    const startX = stagePos.x;
    const startY = stagePos.y + 80;
    const endX = stagePos.x + 120;
    const endY = stagePos.y + 150;

    return `M ${startX} ${startY} Q ${startX} ${endY}, ${endX} ${endY}`;
  };

  const maxLevel = createMemo(() => {
    const positions = Object.values(stagePositions());
    return positions.length > 0 ? Math.max(...positions.map(p => p.level)) : 0;
  });

  // Mouse event handlers for pan/zoom
  const handleWheel = (e: WheelEvent) => {
    e.preventDefault();
    if (e.ctrlKey || e.metaKey) {
      // Zoom with ctrl+wheel
      const delta = e.deltaY > 0 ? 0.9 : 1.1;
      setZoom(prev => Math.max(0.3, Math.min(3, prev * delta)));
    } else {
      // Pan with wheel
      setPanX(prev => prev - e.deltaX);
      setPanY(prev => prev - e.deltaY);
    }
  };

  const handleMouseDown = (e: MouseEvent) => {
    if (e.button === 0) { // Left click
      setIsDragging(true);
      setDragStart({ x: e.clientX - panX(), y: e.clientY - panY() });
    }
  };

  const handleMouseMove = (e: MouseEvent) => {
    if (isDragging()) {
      setPanX(e.clientX - dragStart().x);
      setPanY(e.clientY - dragStart().y);
    }
  };

  const handleMouseUp = () => {
    setIsDragging(false);
  };

  const handleReset = () => {
    centerContent();
  };

  return (
    <div class="w-full">
      <h2 class="text-sm font-semibold text-slate-300 uppercase tracking-wider mb-4">
        Data Flow Topology
        <span class="ml-2 text-slate-500 font-normal normal-case">
          ({props.stages.length} stages, {props.busConnections.length} connections)
        </span>
      </h2>

      <div class="bg-slate-800/50 rounded-lg border border-slate-700 overflow-hidden relative">
        {/* Controls */}
        <div class="absolute top-3 right-3 z-20 flex items-center gap-2 bg-slate-900/80 backdrop-blur-sm rounded-lg px-2 py-1.5 border border-slate-700 shadow-lg">
          <button
            onClick={() => setZoom(prev => Math.max(0.3, prev * 0.9))}
            class="px-2 py-1 bg-slate-700 hover:bg-slate-600 text-slate-300 rounded text-xs"
          >
            -
          </button>
          <span class="text-xs text-slate-400 w-12 text-center">{Math.round(zoom() * 100)}%</span>
          <button
            onClick={() => setZoom(prev => Math.min(3, prev * 1.1))}
            class="px-2 py-1 bg-slate-700 hover:bg-slate-600 text-slate-300 rounded text-xs"
          >
            +
          </button>
          <button
            onClick={handleReset}
            class="px-2 py-1 bg-slate-700 hover:bg-slate-600 text-slate-300 rounded text-xs ml-1"
          >
            Reset
          </button>
        </div>
        <div
          ref={containerRef}
          class="relative w-full h-96 bg-slate-900/50 cursor-grab active:cursor-grabbing"
          style={{ overflow: "hidden" }}
          onWheel={handleWheel}
          onMouseDown={handleMouseDown}
          onMouseMove={handleMouseMove}
          onMouseUp={handleMouseUp}
          onMouseLeave={handleMouseUp}
        >
          <svg
            class="absolute top-0 left-0"
            style={{
              width: "3000px",
              height: "2000px",
              transform: `translate(${panX()}px, ${panY()}px) scale(${zoom()})`,
              "transform-origin": "0 0",
              overflow: "visible"
            }}
          >
            <defs>
              <marker
                id="arrowhead"
                markerWidth="10"
                markerHeight="7"
                refX="9"
                refY="3.5"
                orient="auto"
              >
                <polygon points="0 0, 10 3.5, 0 7" fill="#64748b" />
              </marker>
              <marker
                id="arrowhead-active"
                markerWidth="10"
                markerHeight="7"
                refX="9"
                refY="3.5"
                orient="auto"
              >
                <polygon points="0 0, 10 3.5, 0 7" fill="#22c55e" />
              </marker>
              <marker
                id="arrowhead-rejected"
                markerWidth="10"
                markerHeight="7"
                refX="9"
                refY="3.5"
                orient="auto"
              >
                <polygon points="0 0, 10 3.5, 0 7" fill="#ef4444" />
              </marker>
            </defs>

            <For each={props.busConnections}>
              {(conn) => {
                const path = getConnectionPath(conn.from, conn.to);
                const isHovered = hoveredConnection() === `${conn.from}-${conn.to}`;
                const hasFlow = conn.throughput > 0;

                return (
                  <g
                    onMouseEnter={() => setHoveredConnection(`${conn.from}-${conn.to}`)}
                    onMouseLeave={() => setHoveredConnection(null)}
                    class="cursor-pointer"
                  >
                    <path
                      d={path}
                      stroke="transparent"
                      stroke-width="12"
                      fill="none"
                    />
                    <path
                      d={path}
                      stroke={hasFlow ? "#22c55e" : "#64748b"}
                      stroke-width={isHovered ? 3 : 2}
                      fill="none"
                      marker-end={hasFlow ? "url(#arrowhead-active)" : "url(#arrowhead)"}
                      class={hasFlow ? "animate-pulse" : ""}
                      opacity={hasFlow ? 0.8 : 0.4}
                    />
                  </g>
                );
              }}
            </For>

            <For each={props.branches}>
              {(branch) => {
                const path = getBranchPath(branch.stage);
                return (
                  <g>
                    <path
                      d={path}
                      stroke="#ef4444"
                      stroke-width="2"
                      stroke-dasharray="5,5"
                      fill="none"
                      marker-end="url(#arrowhead-rejected)"
                      opacity={0.6}
                    />
                  </g>
                );
              }}
            </For>

            <For each={props.stages}>
              {(stage) => {
                const pos = stagePositions()[stage.name];
                if (!pos) return null;

                const isSelected = selectedStage() === stage.name;
                const metrics = stage.metrics;
                const hasMetrics = metrics && (metrics.recordsIn > 0 || metrics.recordsOut > 0);

                return (
                  <g
                    transform={`translate(${pos.x - 90}, ${pos.y})`}
                    onClick={() => setSelectedStage(isSelected ? null : stage.name)}
                    class="cursor-pointer"
                  >
                    <rect
                      width="180"
                      height="80"
                      rx="8"
                      fill={isSelected ? "#1e293b" : "#0f172a"}
                      stroke={getStageColor(stage.status)}
                      stroke-width={isSelected ? 3 : 2}
                    />

                    <text
                      x="90"
                      y="20"
                      text-anchor="middle"
                      class="text-xs font-semibold fill-white"
                    >
                      {stage.name}
                    </text>

                    <text
                      x="90"
                      y="38"
                      text-anchor="middle"
                      class="text-[10px] fill-slate-400 font-mono"
                    >
                      {stage.entrypoint}
                    </text>

                    <rect
                      x="140"
                      y="8"
                      width="32"
                      height="16"
                      rx="4"
                      fill="#334155"
                    />
                    <text
                      x="156"
                      y="20"
                      text-anchor="middle"
                      class="text-[10px] fill-slate-300 font-mono"
                    >
                      {stage.replicas}x
                    </text>

                    <Show when={hasMetrics}>
                      <g transform="translate(10, 50)">
                        <text class="text-[10px] fill-slate-400 font-mono">
                          In: {formatNumber(metrics!.recordsIn)}
                        </text>
                        <text
                          x="55"
                          class="text-[10px] fill-green-400 font-mono"
                        >
                          Out: {formatNumber(metrics!.recordsOut)}
                        </text>
                        <Show when={metrics!.recordsBranched > 0}>
                          <text
                            x="110"
                            class="text-[10px] fill-red-400 font-mono"
                          >
                            Branch: {formatNumber(metrics!.recordsBranched)}
                          </text>
                        </Show>
                      </g>
                    </Show>

                    <Show when={!hasMetrics}>
                      <text
                        x="90"
                        y="62"
                        text-anchor="middle"
                        class="text-[10px] fill-slate-600"
                      >
                        No metrics yet
                      </text>
                    </Show>

                    {/* Internal Steps Mini-Visualization */}
                    <Show when={stage.steps && stage.steps.length > 0}>
                      <g transform="translate(10, 68)">
                        <rect
                          width="160"
                          height="10"
                          rx="2"
                          fill="#1e293b"
                        />
                        <For each={stage.steps}>
                          {(step, idx) => {
                            const stepWidth = 160 / (stage.steps?.length || 1);
                            return (
                              <g 
                                onClick={(e) => { e.stopPropagation(); setSelectedStep(step.name); }}
                                class="cursor-pointer"
                              >
                                <rect
                                  x={idx() * stepWidth}
                                  y="0"
                                  width={stepWidth - 2}
                                  height="10"
                                  rx="1"
                                  fill={getStepTypeColor(step.stepType)}
                                  opacity={step.metrics?.recordsOut ? 1 : 0.5}
                                />
                                <title>{step.name} ({step.metrics?.recordsIn} in, {step.metrics?.recordsOut} out)</title>
                              </g>
                            );
                          }}
                        </For>
                      </g>
                    </Show>
                  </g>
                );
              }}
            </For>
          </svg>
        </div>

        <div class="px-4 py-3 border-t border-slate-700 bg-slate-900/30 flex items-center gap-6 text-xs">
          <div class="flex items-center gap-2">
            <div class="w-3 h-0.5 bg-green-500 rounded" />
            <span class="text-slate-400">Active Flow</span>
          </div>
          <div class="flex items-center gap-2">
            <div class="w-3 h-0.5 bg-slate-500 rounded opacity-40" />
            <span class="text-slate-400">Idle Connection</span>
          </div>
          <div class="flex items-center gap-2">
            <div class="w-3 h-0.5 bg-red-500 rounded" />
            <span class="text-slate-400">Rejected/Branched</span>
          </div>
          <div class="flex items-center gap-2">
            <div class="w-3 h-3 rounded-full border-2 border-green-500 bg-slate-800" />
            <span class="text-slate-400">Running</span>
          </div>
        </div>
      </div>

      <Show when={selectedStage()}>
        {(stageName) => {
          const stage = props.stages.find(s => s.name === stageName());
          if (!stage) return null;

          const metrics = stage.metrics;
          const branchInfo = props.branches.find(b => b.stage === stageName());

          return (
            <div class="mt-4 bg-slate-800/50 rounded-lg border border-slate-700 p-4">
              <div class="flex items-center justify-between mb-3">
                <h3 class="text-sm font-semibold text-white">{stage.name}</h3>
                <button
                  onClick={() => setSelectedStage(null)}
                  class="text-xs text-slate-400 hover:text-white"
                >
                  Close
                </button>
              </div>

              <div class="grid grid-cols-4 gap-4">
                <div class="bg-slate-900/50 rounded p-3">
                  <div class="text-[10px] text-slate-500 uppercase">Records In</div>
                  <div class="text-lg font-mono text-white">
                    {formatNumber(metrics?.recordsIn || 0)}
                  </div>
                </div>
                <div class="bg-slate-900/50 rounded p-3">
                  <div class="text-[10px] text-slate-500 uppercase">Records Out</div>
                  <div class="text-lg font-mono text-green-400">
                    {formatNumber(metrics?.recordsOut || 0)}
                  </div>
                </div>
                <div class="bg-slate-900/50 rounded p-3">
                  <div class="text-[10px] text-slate-500 uppercase">Failed</div>
                  <div class="text-lg font-mono text-red-400">
                    {formatNumber(metrics?.recordsFailed || 0)}
                  </div>
                </div>
                <div class="bg-slate-900/50 rounded p-3">
                  <div class="text-[10px] text-slate-500 uppercase">Branched</div>
                  <div class="text-lg font-mono text-orange-400">
                    {formatNumber(metrics?.recordsBranched || 0)}
                  </div>
                </div>
              </div>

              <Show when={branchInfo}>
                <div class="mt-3 pt-3 border-t border-slate-700">
                  <div class="text-[10px] text-slate-500 uppercase mb-2">Branching</div>
                  <div class="flex items-center gap-4 text-sm">
                    <span class="text-green-400">
                      Accepted: {formatNumber(branchInfo!.acceptedCount)}
                    </span>
                    <span class="text-red-400">
                      Rejected: {formatNumber(branchInfo!.rejectedCount)}
                    </span>
                    <span class="text-slate-500">
                      Condition: {branchInfo!.condition}
                    </span>
                  </div>
                </div>
              </Show>

              {/* Stage Sub-Steps List (Drill-down) */}
              <Show when={stage.steps && stage.steps.length > 0}>
                <div class="mt-4 pt-3 border-t border-slate-700">
                  <div class="text-[10px] text-slate-500 uppercase tracking-wider mb-3">Internal Layout ({stage.steps!.length} steps)</div>
                  <div class="space-y-2">
                    <For each={stage.steps}>
                      {(step) => (
                        <div 
                          class="flex items-center justify-between p-2 rounded bg-slate-900 border border-slate-800 hover:border-cyan-500/50 cursor-pointer transition-colors"
                          onClick={() => setSelectedStep(step.name)}
                        >
                          <div class="flex items-center gap-3">
                            <div class="text-lg w-6 text-center">{getStepTypeIcon(step.stepType)}</div>
                            <div>
                              <div class="text-xs font-semibold text-slate-300 font-mono">{step.name}</div>
                              <div class="text-[10px] text-slate-500 uppercase tracking-wider">{step.stepType}</div>
                            </div>
                          </div>
                          <div class="flex items-center gap-4 text-[10px] font-mono">
                            <span class="text-slate-400">{formatDuration(step.metrics?.lagMs || 0)}</span>
                            <div class="flex gap-2">
                              <span class="text-slate-500 border border-slate-700 px-1.5 py-0.5 rounded">↑ {formatNumber(step.metrics?.recordsIn || 0)}</span>
                              <span class="text-green-400 border border-green-900/50 bg-green-900/20 px-1.5 py-0.5 rounded">↓ {formatNumber(step.metrics?.recordsOut || 0)}</span>
                              <Show when={(step.metrics?.recordsFailed || 0) > 0}>
                                <span class="text-red-400 border border-red-900/50 bg-red-900/20 px-1.5 py-0.5 rounded">! {formatNumber(step.metrics?.recordsFailed || 0)}</span>
                              </Show>
                              <Show when={(step.metrics?.recordsBranched || 0) > 0}>
                                <span class="text-orange-400 border border-orange-900/50 bg-orange-900/20 px-1.5 py-0.5 rounded">⑂ {formatNumber(step.metrics?.recordsBranched || 0)}</span>
                              </Show>
                            </div>
                          </div>
                        </div>
                      )}
                    </For>
                  </div>
                </div>
              </Show>
            </div>
          );
        }}
      </Show>

      {/* Data Sampling Drawer Panel */}
      <Show when={selectedStep()}>
        {(step) => (
          <DataSamplePanel 
            stepName={step()} 
            sample={props.dataSamplesMap ? props.dataSamplesMap[step()] : null}
            onClose={() => setSelectedStep(null)}
          />
        )}
      </Show>
    </div>
  );
};

export default PipelineDAGVisualizer;
export type { StageWithMetrics, BusConnection, BranchInfo };
