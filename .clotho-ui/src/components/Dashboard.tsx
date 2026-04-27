import { Show, createSignal, createEffect, onMount } from "solid-js";
import type { Component } from "solid-js";
import Sidebar from "./Sidebar";
import TopBar from "./TopBar";
import Overview from "./Overview";
import LogViewer from "./LogViewer";
import PipelineDetail from "./PipelineDetail";
import DlqInbox from "./DlqInbox";
import IDEPage from "./IDEPage";
import type { Environment } from "./EnvironmentSwitcher";
import { connectTelemetryStream, pipelineCount } from "../store/pipelines";

const pageLabels: Record<string, string> = {
  overview: "Overview",
  pipelines: "Pipelines",
  ide: "Editor",
  builds: "Builds",
  logs: "Logs",
  dlq: "Dead Letters",
  settings: "Settings",
};

interface DashboardProps {
  activePage: string;
  selectedPipeline: string | null;
  environment: Environment;
  onNavigate: (page: string) => void;
  onSelectPipeline: (id: string) => void;
  onBackFromDetail: () => void;
  onSwitchEnvironment: (env: Environment) => void;
}

const Dashboard: Component<DashboardProps> = (props) => {
  const [dataReady, setDataReady] = createSignal(false);

  onMount(() => {
    connectTelemetryStream(() => props.environment);
    // Fallback: mark ready after 3s even if no pipelines found
    setTimeout(() => setDataReady(true), 3000);
  });

  createEffect(() => {
    if (pipelineCount() > 0) setDataReady(true);
  });

  const pageTitle = () => {
    if (props.selectedPipeline) {
      return `Pipelines / ${props.selectedPipeline}`;
    }
    return pageLabels[props.activePage] || props.activePage;
  };

  return (
    <div class="min-h-screen bg-slate-900 text-white">
      <Sidebar
        activePage={props.activePage}
        onNavigate={props.onNavigate}
      />

      <div class="ml-56 flex flex-col min-h-screen">
        <TopBar 
          pageTitle={pageTitle()} 
          environment={props.environment}
          onSwitchEnvironment={props.onSwitchEnvironment}
        />

        <main class="px-8 py-6 overflow-y-auto h-[calc(100vh-64px)]">
          <Show when={dataReady()} fallback={
            <div class="flex items-center justify-center py-20">
              <div class="w-8 h-8 border-2 border-blue-500 border-t-transparent rounded-full animate-spin mr-3" />
              <span class="text-sm text-slate-400">Loading pipelines...</span>
            </div>
          }>
            {props.activePage === "overview" && !props.selectedPipeline && (
              <Overview onSelectPipeline={props.onSelectPipeline} />
            )}

            <Show when={props.activePage === "pipelines" && props.selectedPipeline}>
              <PipelineDetail
                pipelineId={props.selectedPipeline!}
                onBack={props.onBackFromDetail}
                environment={props.environment}
              />
            </Show>

            {props.activePage === "builds" && (
              <div class="text-center py-20">
                <div class="w-12 h-12 rounded-full bg-slate-800 border border-slate-700 flex items-center justify-center mx-auto mb-4">
                  <span class="text-slate-500 text-lg">⚙</span>
                </div>
                <h3 class="text-sm font-semibold text-slate-300">Build History</h3>
                <p class="text-xs text-slate-500 mt-1">
                  View detailed build history at{" "}
                  <a href={`/builds/${props.environment}`} class="text-blue-400 hover:underline">
                    /builds
                  </a>
                </p>
              </div>
            )}

            {props.activePage === "ide" && (
              <IDEPage pipelineId={props.selectedPipeline} environment={props.environment} />
            )}

            {props.activePage === "logs" && <LogViewer />}

            {props.activePage === "dlq" && <DlqInbox />}

            {props.activePage === "settings" && (
              <div class="text-center py-20">
                <div class="w-12 h-12 rounded-full bg-slate-800 border border-slate-700 flex items-center justify-center mx-auto mb-4">
                  <span class="text-slate-500 text-lg">⛭</span>
                </div>
                <h3 class="text-sm font-semibold text-slate-300">Settings</h3>
                <p class="text-xs text-slate-500 mt-1">Coming soon</p>
              </div>
            )}
          </Show>
        </main>
      </div>
    </div>
  );
};

export default Dashboard;