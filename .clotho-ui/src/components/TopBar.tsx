import type { Component } from "solid-js";
import EnvironmentSwitcher from "./EnvironmentSwitcher";
import type { Environment } from "./EnvironmentSwitcher";

interface TopBarProps {
  pageTitle: string;
  environment: Environment;
  onSwitchEnvironment: (env: Environment) => void;
}

const TopBar: Component<TopBarProps> = (props) => {
  return (
    <header class="h-12 bg-slate-900 border-b border-slate-800 flex items-center justify-between px-6 sticky top-0 z-10">
      {/* Breadcrumb */}
      <div class="flex items-center gap-2 font-mono text-xs tracking-wider">
        <span class="text-slate-500 hover:text-blue-400 transition-colors cursor-pointer">
          <svg class="w-3.5 h-3.5 inline" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" d="M2.25 12l8.954-8.955a1.126 1.126 0 011.591 0L21.75 12M4.5 9.75v10.125c0 .621.504 1.125 1.125 1.125H9.75v-4.875c0-.621.504-1.125 1.125-1.125h2.25c.621 0 1.125.504 1.125 1.125V21h4.125c.621 0 1.125-.504 1.125-1.125V9.75M8.25 21h8.25" />
          </svg>
        </span>
        <span class="text-slate-600">/</span>
        <span class="text-slate-300 uppercase">{props.pageTitle}</span>
      </div>

      {/* Right side */}
      <div class="flex items-center gap-4">
        {/* Environment Switcher */}
        <EnvironmentSwitcher
          environment={props.environment}
          onSwitch={props.onSwitchEnvironment}
        />

        <span class="text-[10px] text-slate-500 font-mono uppercase tracking-wider">
          UDP :8125 → API :3000
        </span>
        <div class="flex items-center gap-1.5">
          <div class="w-1.5 h-1.5 rounded-full bg-green-500" />
          <span class="text-[10px] text-green-400 font-semibold uppercase tracking-wider">Operational</span>
        </div>
      </div>
    </header>
  );
};

export default TopBar;
