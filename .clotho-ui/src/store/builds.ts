import { createStore, reconcile } from "solid-js/store";

export interface BuildRecord {
  id: string;
  pipeline_id: string;
  pipeline_name: string;
  job_name: string;
  git_repository: string;
  reference: string;
  path: string;
  target_image: string;
  status: "pending" | "running" | "completed" | "failed";
  started_at: string;
  finished_at: string | null;
  duration_ms: number | null;
  error: string | null;
  created_at: string;
}

const [builds, setBuilds] = createStore<BuildRecord[]>([]);

export { builds, setBuilds };

export const fetchBuilds = async (environment: string = "production"): Promise<BuildRecord[]> => {
  const API_URL = import.meta.env.VITE_API_URL || "http://localhost:3000";
  try {
    const resp = await fetch(`${API_URL}/v1/builds?environment=${encodeURIComponent(environment)}`);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();
    setBuilds(reconcile(data));
    return data;
  } catch (e) {
    console.error("Failed to fetch builds:", e);
    return [];
  }
};

export const connectBuildStream = (getEnvironment: () => string = () => "production") => {
  // Poll for build updates every 3 seconds
  setInterval(() => {
    fetchBuilds(getEnvironment());
  }, 3000);
};

// Utility functions
export const formatDuration = (ms: number | null): string => {
  if (ms === null) return "—";
  if (ms < 1000) return `${ms}ms`;
  const seconds = Math.floor(ms / 1000);
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  const remainingSeconds = seconds % 60;
  if (minutes < 60) return `${minutes}m ${remainingSeconds}s`;
  const hours = Math.floor(minutes / 60);
  const remainingMinutes = minutes % 60;
  return `${hours}h ${remainingMinutes}m`;
};

export const formatRelativeTime = (dateStr: string): string => {
  const date = new Date(dateStr);
  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  const diffSec = Math.floor(diffMs / 1000);
  
  if (diffSec < 10) return "just now";
  if (diffSec < 60) return `${diffSec}s ago`;
  const diffMin = Math.floor(diffSec / 60);
  if (diffMin < 60) return `${diffMin}m ago`;
  const diffHr = Math.floor(diffMin / 60);
  if (diffHr < 24) return `${diffHr}h ago`;
  const diffDay = Math.floor(diffHr / 24);
  return `${diffDay}d ago`;
};

export const statusColor = (status: BuildRecord["status"]): string => {
  switch (status) {
    case "running": return "text-blue-400";
    case "completed": return "text-green-400";
    case "failed": return "text-red-400";
    case "pending": return "text-yellow-400";
    default: return "text-slate-400";
  }
};

export const statusIcon = (status: BuildRecord["status"]): string => {
  switch (status) {
    case "running": return "⟳";
    case "completed": return "✓";
    case "failed": return "✗";
    case "pending": return "◷";
    default: return "?";
  }
};