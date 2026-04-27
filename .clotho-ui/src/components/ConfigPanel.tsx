import { createSignal, For, Show } from "solid-js";
import type { Component } from "solid-js";
import type { PipelineService, ConfigEntry, ConfigUpdate } from "../services/pipeline";
import type { Environment } from "./EnvironmentSwitcher";

const API_URL = import.meta.env.VITE_API_URL || "http://localhost:3000";

interface SecretInfo {
  name: string;
  type: string;
}

interface ConfigPanelProps {
  pipelineService: PipelineService;
  pipelineId: string | null;
  environment?: Environment;
  onLog: (message: string) => void;
}

const ConfigPanel: Component<ConfigPanelProps> = (props) => {
  const [config, setConfig] = createSignal<ConfigEntry[]>([]);
  const [isLoading, setIsLoading] = createSignal(false);
  const [isSaving, setIsSaving] = createSignal(false);
  const [isOpen, setIsOpen] = createSignal(false);
  const [editingIndex, setEditingIndex] = createSignal<number | null>(null);
  const [newVarName, setNewVarName] = createSignal("");
  const [newVarValue, setNewVarValue] = createSignal("");
  const [newVarType, setNewVarType] = createSignal<"literal" | "secret">("literal");
  const [newSecretName, setNewSecretName] = createSignal("");
  const [newSecretKey, setNewSecretKey] = createSignal("");
  const [showAddForm, setShowAddForm] = createSignal(false);

  // Secret browser state
  const [availableSecrets, setAvailableSecrets] = createSignal<SecretInfo[]>([]);
  const [availableKeys, setAvailableKeys] = createSignal<string[]>([]);
  const [loadingSecrets, setLoadingSecrets] = createSignal(false);
  const [loadingKeys, setLoadingKeys] = createSignal(false);

  const envQ = () => `environment=${encodeURIComponent(props.environment || 'production')}`;

  async function fetchSecrets() {
    setLoadingSecrets(true);
    try {
      const resp = await fetch(`${API_URL}/v1/secrets?${envQ()}`);
      if (resp.ok) {
        setAvailableSecrets(await resp.json());
      }
    } catch { /* silent */ }
    setLoadingSecrets(false);
  }

  async function fetchKeys(secretName: string) {
    if (!secretName) { setAvailableKeys([]); return; }
    setLoadingKeys(true);
    try {
      const resp = await fetch(`${API_URL}/v1/secrets/${encodeURIComponent(secretName)}/keys?${envQ()}`);
      if (resp.ok) {
        const data = await resp.json();
        setAvailableKeys(data.keys || []);
      }
    } catch { /* silent */ }
    setLoadingKeys(false);
  }

  async function loadConfig() {
    if (!props.pipelineId) return;
    setIsLoading(true);
    try {
      const result = await props.pipelineService.getConfig(props.pipelineId, props.environment || 'production');
      setConfig(result.config || []);
    } catch (error: any) {
      const msg = String(error?.message || error);
      if (msg.includes("503")) {
        props.onLog("Config: Control Plane not connected to cluster.");
      } else if (msg.includes("404")) {
        setConfig([]);
      } else {
        props.onLog(`Config load error: ${msg}`);
      }
    } finally {
      setIsLoading(false);
    }
  }

  function togglePanel() {
    const next = !isOpen();
    setIsOpen(next);
    if (next && props.pipelineId) {
      loadConfig();
    }
  }

  function buildConfigUpdates(entries: ConfigEntry[]): ConfigUpdate[] {
    return entries.map((e) => {
      if (e.source === "secret" && e.secret_name && e.secret_key) {
        return {
          name: e.name,
          valueFrom: {
            secretKeyRef: {
              name: e.secret_name,
              key: e.secret_key,
            },
          },
        };
      }
      return { name: e.name, value: e.value || "" };
    });
  }

  async function saveConfig() {
    if (!props.pipelineId) return;
    setIsSaving(true);
    try {
      const updates = buildConfigUpdates(config());
      await props.pipelineService.updateConfig(props.pipelineId, updates, props.environment || 'production');
      props.onLog(`Config updated for ${props.pipelineId}`);
    } catch (error: any) {
      props.onLog(`Config save error: ${error?.message || error}`);
    } finally {
      setIsSaving(false);
    }
  }

  function removeVar(index: number) {
    setConfig((prev) => prev.filter((_, i) => i !== index));
  }

  function updateVarValue(index: number, value: string) {
    setConfig((prev) =>
      prev.map((entry, i) => (i === index ? { ...entry, value } : entry))
    );
  }

  function addVariable() {
    const name = newVarName().trim();
    if (!name) return;

    const entry: ConfigEntry =
      newVarType() === "secret"
        ? {
            name,
            source: "secret",
            secret_name: newSecretName().trim(),
            secret_key: newSecretKey().trim(),
          }
        : {
            name,
            value: newVarValue(),
            source: "literal",
          };

    setConfig((prev) => [...prev, entry]);
    setNewVarName("");
    setNewVarValue("");
    setNewSecretName("");
    setNewSecretKey("");
    setShowAddForm(false);
  }

  return (
    <div class="border-t border-slate-700">
      {/* Header toggle */}
      <button
        class="w-full flex items-center justify-between px-3 py-2 text-[11px] font-medium text-slate-400 uppercase tracking-wider hover:bg-slate-800/50 transition-colors"
        onClick={togglePanel}
      >
        <span class="flex items-center gap-1.5">
          <svg
            width="12"
            height="12"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            stroke-width="2"
          >
            <path d="M12.22 2h-.44a2 2 0 00-2 2v.18a2 2 0 01-1 1.73l-.43.25a2 2 0 01-2 0l-.15-.08a2 2 0 00-2.73.73l-.22.38a2 2 0 00.73 2.73l.15.1a2 2 0 011 1.72v.51a2 2 0 01-1 1.74l-.15.09a2 2 0 00-.73 2.73l.22.38a2 2 0 002.73.73l.15-.08a2 2 0 012 0l.43.25a2 2 0 011 1.73V20a2 2 0 002 2h.44a2 2 0 002-2v-.18a2 2 0 011-1.73l.43-.25a2 2 0 012 0l.15.08a2 2 0 002.73-.73l.22-.39a2 2 0 00-.73-2.73l-.15-.08a2 2 0 01-1-1.74v-.5a2 2 0 011-1.74l.15-.09a2 2 0 00.73-2.73l-.22-.38a2 2 0 00-2.73-.73l-.15.08a2 2 0 01-2 0l-.43-.25a2 2 0 01-1-1.73V4a2 2 0 00-2-2z" />
            <circle cx="12" cy="12" r="3" />
          </svg>
          Config / Env Vars
        </span>
        <svg
          width="10"
          height="10"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          stroke-width="2"
          style={{
            transform: isOpen() ? "rotate(180deg)" : "rotate(0deg)",
            transition: "transform 0.2s",
          }}
        >
          <polyline points="6 9 12 15 18 9" />
        </svg>
      </button>

      <Show when={isOpen()}>
        <div class="px-3 pb-3">
          <Show when={!props.pipelineId}>
            <div class="text-[11px] text-slate-500 italic py-2">
              Select a pipeline to manage config.
            </div>
          </Show>

          <Show when={props.pipelineId}>
            <Show when={isLoading()}>
              <div class="text-[11px] text-slate-500 py-2 flex items-center gap-2">
                <div class="spinner-small" />
                Loading config...
              </div>
            </Show>

            <Show when={!isLoading()}>
              {/* Config entries table */}
              <Show when={config().length > 0}>
                <div class="space-y-1 mb-2">
                  <For each={config()}>
                    {(entry, i) => (
                      <div class="flex items-center gap-1.5 group">
                        <span
                          class="text-[10px] px-1 py-0.5 rounded font-mono"
                          classList={{
                            "bg-emerald-900/30 text-emerald-400":
                              entry.source === "literal",
                            "bg-amber-900/30 text-amber-400":
                              entry.source === "secret",
                          }}
                        >
                          {entry.source === "secret" ? "SEC" : "ENV"}
                        </span>
                        <span class="text-[11px] text-slate-300 font-mono flex-1 truncate">
                          {entry.name}
                        </span>
                        <Show when={entry.source === "literal"}>
                          <Show
                            when={editingIndex() === i()}
                            fallback={
                              <span
                                class="text-[11px] text-slate-500 font-mono truncate max-w-[100px] cursor-pointer hover:text-slate-300"
                                onClick={() => setEditingIndex(i())}
                              >
                                {entry.value || '""'}
                              </span>
                            }
                          >
                            <input
                              type="text"
                              value={entry.value || ""}
                              onInput={(e) =>
                                updateVarValue(i(), e.currentTarget.value)
                              }
                              onBlur={() => setEditingIndex(null)}
                              onKeyDown={(e) => {
                                if (e.key === "Enter") setEditingIndex(null);
                              }}
                              class="text-[11px] font-mono bg-slate-800 border border-slate-600 rounded px-1 py-0.5 text-slate-200 w-[100px] outline-none focus:border-emerald-500"
                              autofocus
                            />
                          </Show>
                        </Show>
                        <Show when={entry.source === "secret"}>
                          <span class="text-[11px] text-slate-500 font-mono truncate max-w-[100px]">
                            {entry.secret_name}/{entry.secret_key}
                          </span>
                        </Show>
                        <button
                          class="opacity-0 group-hover:opacity-100 text-slate-500 hover:text-red-400 transition-all p-0.5"
                          onClick={() => removeVar(i())}
                          title="Remove"
                        >
                          <svg
                            width="12"
                            height="12"
                            viewBox="0 0 24 24"
                            fill="none"
                            stroke="currentColor"
                            stroke-width="2"
                          >
                            <line x1="18" y1="6" x2="6" y2="18" />
                            <line x1="6" y1="6" x2="18" y2="18" />
                          </svg>
                        </button>
                      </div>
                    )}
                  </For>
                </div>
              </Show>

              <Show when={config().length === 0 && !showAddForm()}>
                <div class="text-[11px] text-slate-500 italic py-1">
                  No config vars set.
                </div>
              </Show>

              {/* Add form */}
              <Show when={showAddForm()}>
                <div class="border border-slate-700 rounded p-2 space-y-1.5 mb-2 bg-slate-800/50">
                  <div class="flex gap-1.5">
                    <select
                      value={newVarType()}
                      onChange={(e) =>
                        setNewVarType(
                          e.currentTarget.value as "literal" | "secret"
                        )
                      }
                      class="text-[10px] bg-slate-700 border border-slate-600 rounded px-1 py-0.5 text-slate-300 outline-none"
                    >
                      <option value="literal">ENV</option>
                      <option value="secret">Secret</option>
                    </select>
                    <input
                      type="text"
                      placeholder="VAR_NAME"
                      value={newVarName()}
                      onInput={(e) => setNewVarName(e.currentTarget.value)}
                      class="flex-1 text-[11px] font-mono bg-slate-800 border border-slate-600 rounded px-1.5 py-0.5 text-slate-200 outline-none focus:border-emerald-500"
                    />
                  </div>
                  <Show when={newVarType() === "literal"}>
                    <input
                      type="text"
                      placeholder="value"
                      value={newVarValue()}
                      onInput={(e) => setNewVarValue(e.currentTarget.value)}
                      class="w-full text-[11px] font-mono bg-slate-800 border border-slate-600 rounded px-1.5 py-0.5 text-slate-200 outline-none focus:border-emerald-500"
                    />
                  </Show>
                  <Show when={newVarType() === "secret"}>
                    <div class="space-y-1.5">
                      {/* Secret selector */}
                      <div class="flex gap-1.5 items-center">
                        <select
                          value={newSecretName()}
                          onChange={(e) => {
                            const name = e.currentTarget.value;
                            setNewSecretName(name);
                            setNewSecretKey("");
                            fetchKeys(name);
                          }}
                          class="flex-1 text-[11px] font-mono bg-slate-800 border border-slate-600 rounded px-1.5 py-1 text-slate-200 outline-none focus:border-amber-500"
                        >
                          <option value="">Select secret...</option>
                          <For each={availableSecrets()}>
                            {(s) => <option value={s.name}>{s.name}</option>}
                          </For>
                        </select>
                        <Show when={loadingSecrets()}>
                          <div class="w-3 h-3 border border-slate-600 border-t-amber-400 rounded-full animate-spin" />
                        </Show>
                      </div>
                      {/* Key selector */}
                      <Show when={newSecretName()}>
                        <div class="flex gap-1.5 items-center">
                          <select
                            value={newSecretKey()}
                            onChange={(e) => setNewSecretKey(e.currentTarget.value)}
                            class="flex-1 text-[11px] font-mono bg-slate-800 border border-slate-600 rounded px-1.5 py-1 text-slate-200 outline-none focus:border-amber-500"
                          >
                            <option value="">Select key...</option>
                            <For each={availableKeys()}>
                              {(k) => <option value={k}>{k}</option>}
                            </For>
                          </select>
                          <Show when={loadingKeys()}>
                            <div class="w-3 h-3 border border-slate-600 border-t-amber-400 rounded-full animate-spin" />
                          </Show>
                        </div>
                      </Show>
                    </div>
                  </Show>
                  <div class="flex gap-1.5 justify-end">
                    <button
                      class="text-[10px] px-2 py-0.5 text-slate-400 hover:text-slate-200"
                      onClick={() => setShowAddForm(false)}
                    >
                      Cancel
                    </button>
                    <button
                      class="text-[10px] px-2 py-0.5 bg-emerald-600 text-white rounded hover:bg-emerald-500"
                      onClick={addVariable}
                    >
                      Add
                    </button>
                  </div>
                </div>
              </Show>

              {/* Action buttons */}
              <div class="flex gap-1.5 mt-2">
                <button
                  class="text-[10px] px-2 py-1 border border-slate-600 text-slate-400 rounded hover:border-slate-500 hover:text-slate-300 transition-colors"
                  onClick={() => { setShowAddForm(true); setNewVarType("literal"); }}
                >
                  + Add Variable
                </button>
                <button
                  class="text-[10px] px-2 py-1 border border-amber-700/50 text-amber-500 rounded hover:border-amber-600 hover:text-amber-400 transition-colors"
                  onClick={() => { setShowAddForm(true); setNewVarType("secret"); fetchSecrets(); }}
                >
                  + Map Secret
                </button>
                <button
                  class="text-[10px] px-2 py-1 bg-emerald-600 text-white rounded hover:bg-emerald-500 transition-colors disabled:opacity-50"
                  onClick={saveConfig}
                  disabled={isSaving()}
                >
                  {isSaving() ? "Saving..." : "Save Config"}
                </button>
              </div>
            </Show>
          </Show>
        </div>
      </Show>
    </div>
  );
};

export default ConfigPanel;
