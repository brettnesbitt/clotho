import { createSignal, For, Show } from "solid-js";
import type { Component } from "solid-js";

export type Environment = "production" | "preview";

interface EnvironmentConfig {
  name: Environment;
  label: string;
  color: string;
  dotColor: string;
  bgActive: string;
  borderActive: string;
  description: string;
}

const ENVIRONMENTS: EnvironmentConfig[] = [
  {
    name: "production",
    label: "Production",
    color: "text-green-400",
    dotColor: "bg-green-500",
    bgActive: "bg-green-500/10",
    borderActive: "border-green-500/30",
    description: "Live pipelines, real data",
  },
  {
    name: "preview",
    label: "Preview",
    color: "text-yellow-400",
    dotColor: "bg-yellow-500",
    bgActive: "bg-yellow-500/10",
    borderActive: "border-yellow-500/30",
    description: "Test builds, staging data",
  },
];

interface EnvironmentSwitcherProps {
  environment: Environment;
  onSwitch: (env: Environment) => void;
}

const EnvironmentSwitcher: Component<EnvironmentSwitcherProps> = (props) => {
  const [isOpen, setIsOpen] = createSignal(false);

  const current = () => ENVIRONMENTS.find((e) => e.name === props.environment) || ENVIRONMENTS[0];

  return (
    <div class="relative">
      {/* Trigger */}
      <button
        onClick={() => setIsOpen(!isOpen())}
        class={`flex items-center gap-2 px-2 py-1 rounded text-xs font-medium transition-all hover:bg-slate-800/50`}
      >
        <div class={`w-1.5 h-1.5 rounded-full ${current().dotColor}`} />
        <span class={current().color}>{current().label}</span>
        <svg
          class={`w-3 h-3 text-slate-500 transition-transform ${isOpen() ? "rotate-180" : ""}`}
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
          stroke-width="2"
        >
          <path stroke-linecap="round" stroke-linejoin="round" d="M19 9l-7 7-7-7" />
        </svg>
      </button>

      {/* Dropdown */}
      <Show when={isOpen()}>
        <div class="absolute right-0 top-full mt-1 w-48 bg-slate-900 border border-slate-700 rounded-md shadow-xl z-50 overflow-hidden">
          <For each={ENVIRONMENTS}>
            {(env) => {
              const isActive = () => props.environment === env.name;
              return (
                <button
                  onClick={() => {
                    props.onSwitch(env.name);
                    setIsOpen(false);
                  }}
                  class={`w-full flex items-center gap-2.5 px-3 py-2.5 text-left transition-colors ${
                    isActive()
                      ? `${env.bgActive} ${env.borderActive}`
                      : "hover:bg-slate-800/60"
                  }`}
                >
                  <div class={`w-2 h-2 rounded-full ${env.dotColor} ${isActive() ? "ring-2 ring-offset-1 ring-offset-slate-900" : ""}`} />
                  <div class="flex-1 min-w-0">
                    <div class={`text-xs font-medium ${isActive() ? env.color : "text-slate-300"}`}>
                      {env.label}
                    </div>
                    <div class="text-[10px] text-slate-500 truncate">{env.description}</div>
                  </div>
                  <Show when={isActive()}>
                    <svg class={`w-3.5 h-3.5 ${env.color}`} fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2.5">
                      <path stroke-linecap="round" stroke-linejoin="round" d="M5 13l4 4L19 7" />
                    </svg>
                  </Show>
                </button>
              );
            }}
          </For>
        </div>
      </Show>
    </div>
  );
};

export default EnvironmentSwitcher;
