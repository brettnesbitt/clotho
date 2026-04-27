import { createSignal, Show, onCleanup } from "solid-js";
import type { Component } from "solid-js";
import MonacoEditor from "./MonacoEditor";

interface DataSample {
  pipeline_id: string;
  stage_name: string;
  step_name: string;
  payload_in: string;
  payload_out: string;
  timestamp: number;
}

interface DataSamplePanelProps {
  stepName: string;
  sample: DataSample | null;
  onClose: () => void;
}

const DataSamplePanel: Component<DataSamplePanelProps> = (props) => {
  const [isPaused, setIsPaused] = createSignal(false);
  const [frozenSample, setFrozenSample] = createSignal<DataSample | null>(null);

  // When paused, we freeze the current sample. When unpaused, we clear the freeze.
  const handleTogglePause = () => {
    if (isPaused()) {
      setIsPaused(false);
      setFrozenSample(null);
    } else {
      setIsPaused(true);
      setFrozenSample(props.sample);
    }
  };

  const activeSample = () => isPaused() ? frozenSample() : props.sample;

  const formatJson = (str: string) => {
    if (!str) return "{}";
    try {
      const parsed = JSON.parse(str);
      return JSON.stringify(parsed, null, 2);
    } catch {
      return str;
    }
  };

  const payloadIn = () => formatJson(activeSample()?.payload_in || "");
  const payloadOut = () => formatJson(activeSample()?.payload_out || "");
  const hasData = () => !!activeSample();

  return (
    <div class="fixed inset-y-0 right-0 w-[800px] bg-slate-900 border-l border-slate-700 shadow-2xl flex flex-col z-50 transform transition-transform duration-300">
      <div class="flex items-center justify-between px-6 py-4 border-b border-slate-800 bg-slate-900/90 backdrop-blur">
        <div class="flex items-center gap-3">
          <div class="w-8 h-8 rounded bg-cyan-500/20 flex items-center justify-center border border-cyan-500/30">
            <svg class="w-4 h-4 text-cyan-400" fill="none" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor">
              <path stroke-linecap="round" stroke-linejoin="round" d="M19.5 14.25v-2.625a3.375 3.375 0 00-3.375-3.375h-1.5A1.125 1.125 0 0113.5 7.125v-1.5a3.375 3.375 0 00-3.375-3.375H8.25m3.75 9v6m3-3H9m1.5-12H5.625c-.621 0-1.125.504-1.125 1.125v17.25c0 .621.504 1.125 1.125 1.125h12.75c.621 0 1.125-.504 1.125-1.125V11.25a9 9 0 00-9-9z" />
            </svg>
          </div>
          <div>
            <h2 class="text-sm font-semibold text-white">Data Inspector</h2>
            <div class="text-[10px] text-slate-400 font-mono tracking-wider flex items-center gap-2">
              STEP: <span class="text-cyan-400">{props.stepName}</span>
              <Show when={hasData() && !isPaused()}>
                <span class="flex h-2 w-2 relative ml-1">
                  <span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75"></span>
                  <span class="relative inline-flex rounded-full h-2 w-2 bg-green-500"></span>
                </span>
              </Show>
              <Show when={isPaused()}>
                <span class="flex h-2 w-2 relative ml-1">
                  <span class="relative inline-flex rounded-full h-2 w-2 bg-amber-500"></span>
                </span>
                <span class="text-amber-400">PAUSED</span>
              </Show>
            </div>
          </div>
        </div>
        
        <div class="flex items-center gap-3">
          <button
            onClick={handleTogglePause}
            class={`px-3 py-1.5 rounded text-xs font-semibold flex items-center gap-1.5 transition-colors ${
              isPaused() 
                ? "bg-amber-500/20 text-amber-400 border border-amber-500/30 hover:bg-amber-500/30" 
                : "bg-slate-800 text-slate-300 border border-slate-700 hover:bg-slate-700"
            }`}
          >
            {isPaused() ? (
              <>
                <svg class="w-3.5 h-3.5" fill="currentColor" viewBox="0 0 24 24"><path d="M5.25 5.653c0-.856.917-1.398 1.667-.986l11.54 6.348a1.125 1.125 0 010 1.971l-11.54 6.347a1.125 1.125 0 01-1.667-.985V5.653z" /></svg>
                Resume Run
              </>
            ) : (
              <>
                <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M15.75 5.25v13.5m-7.5-13.5v13.5" /></svg>
                Pause Incoming
              </>
            )}
          </button>
          
          <button onClick={props.onClose} class="text-slate-400 hover:text-white bg-slate-800 p-1.5 rounded-md">
            <svg class="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor">
              <path stroke-linecap="round" stroke-linejoin="round" d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>
      </div>

      <div class="flex-1 flex flex-col p-4 bg-[#0d1117] overflow-hidden">
        <Show when={!hasData()}>
          <div class="flex-1 flex flex-col items-center justify-center text-slate-500">
            <div class="w-12 h-12 mb-4 rounded-full border-2 border-slate-700 border-t-cyan-500 animate-spin" />
            <p class="text-sm">Waiting for sampled record...</p>
          </div>
        </Show>

        <Show when={hasData()}>
          <div class="flex-1 grid grid-rows-2 gap-4">
            <div class="flex flex-col border border-slate-800 rounded-lg overflow-hidden bg-slate-900/50 relative">
              <div class="absolute top-0 right-0 left-0 h-1 bg-gradient-to-r from-cyan-500/0 via-cyan-500/50 to-cyan-500/0 opacity-50" />
              <div class="px-3 py-1.5 bg-slate-900 border-b border-slate-800 flex justify-between items-center z-10">
                <span class="text-[10px] text-cyan-400 font-mono tracking-wider">INPUT_PAYLOAD</span>
              </div>
              <div class="flex-1 relative">
                <MonacoEditor 
                  code={payloadIn()} 
                  language="json" 
                  readOnly={true} 
                />
              </div>
            </div>

            <div class="flex flex-col border border-slate-800 rounded-lg overflow-hidden bg-slate-900/50 relative">
              <div class="absolute top-0 right-0 left-0 h-1 bg-gradient-to-r from-green-500/0 via-green-500/50 to-green-500/0 opacity-50" />
              <div class="px-3 py-1.5 bg-slate-900 border-b border-slate-800 flex justify-between items-center z-10">
                <span class="text-[10px] text-green-400 font-mono tracking-wider">OUTPUT_PAYLOAD</span>
              </div>
              <div class="flex-1 relative">
                <MonacoEditor 
                  code={payloadOut()} 
                  language="json" 
                  readOnly={true} 
                />
              </div>
            </div>
          </div>
        </Show>
      </div>
    </div>
  );
};

export default DataSamplePanel;
