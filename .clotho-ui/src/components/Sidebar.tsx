import { For } from "solid-js";
import { A, useLocation } from "@solidjs/router";
import type { Component } from "solid-js";

interface NavItem {
  id: string;
  label: string;
  icon: string;
  href?: string;
}

interface SidebarProps {
  activePage: string;
  onNavigate: (page: string) => void;
}

const navItems: NavItem[] = [
  { id: "overview", label: "Overview", icon: "◉" },
  { id: "ide", label: "Editor", icon: "⌨" },
  { id: "builds", label: "Builds", icon: "⚙", href: "/builds" },
  { id: "logs", label: "Logs", icon: "⟟" },
  { id: "dlq", label: "Dead Letters", icon: "⚠" },
  { id: "settings", label: "Settings", icon: "⛭" },
];

const Sidebar: Component<SidebarProps> = (props) => {
  const location = useLocation();

  const isActive = (item: NavItem) => {
    if (item.href) {
      return location.pathname.startsWith(item.href);
    }
    return props.activePage === item.id;
  };

  const handleClick = (item: NavItem) => {
    if (item.href) {
      // Let the <A> component handle navigation
      return;
    }
    props.onNavigate(item.id);
  };

  return (
    <aside class="w-56 bg-slate-950 border-r border-slate-800 flex flex-col h-screen fixed left-0 top-0 z-20">
      {/* Brand */}
      <div class="px-5 py-5 border-b border-slate-800">
        <div class="flex items-center gap-2.5">
          <div class="w-7 h-7 bg-blue-500 rounded flex items-center justify-center text-white text-xs font-bold">
            C
          </div>
          <div>
            <div class="text-sm font-semibold text-white tracking-tight">Clotho</div>
            <div class="text-[10px] text-slate-500 uppercase tracking-widest">Mission Control</div>
          </div>
        </div>
      </div>

      {/* Navigation */}
      <nav class="flex-1 px-3 py-4 space-y-1">
        <For each={navItems}>
          {(item) => {
            const active = isActive(item);
            const baseClass = "w-full flex items-center gap-3 px-3 py-2 rounded-md text-sm font-medium transition-all";
            const activeClass = "bg-blue-500/10 text-blue-400 border border-blue-500/20";
            const inactiveClass = "text-slate-400 hover:text-slate-200 hover:bg-slate-800/50 border border-transparent";
            const iconClass = `text-base ${active ? "text-blue-400" : "text-slate-500"}`;

            if (item.href) {
              return (
                <A
                  href={item.href}
                  class={`${baseClass} ${active ? activeClass : inactiveClass}`}
                >
                  <span class={iconClass}>{item.icon}</span>
                  {item.label}
                </A>
              );
            }

            return (
              <button
                onClick={() => handleClick(item)}
                class={`${baseClass} ${active ? activeClass : inactiveClass}`}
              >
                <span class={iconClass}>{item.icon}</span>
                {item.label}
              </button>
            );
          }}
        </For>
      </nav>

      {/* Cluster Info */}
      <div class="px-4 py-4 border-t border-slate-800">
        <div class="flex items-center gap-2">
          <div class="w-2 h-2 rounded-full bg-green-500 animate-pulse-glow" />
          <span class="text-xs text-slate-400 font-medium">Cluster Connected</span>
        </div>
      </div>
    </aside>
  );
};

export default Sidebar;
