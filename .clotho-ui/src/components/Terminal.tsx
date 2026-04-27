import { createEffect, onMount } from 'solid-js';

interface TerminalProps {
  logs: string[];
}

export default function Terminal(props: TerminalProps) {
  let containerRef: HTMLDivElement | undefined;

  // Auto-scroll to bottom when new logs arrive
  createEffect(() => {
    const _ = props.logs.length;
    if (containerRef) {
      containerRef.scrollTop = containerRef.scrollHeight;
    }
  });

  return (
    <div class="terminal">
      <div class="terminal-header">
        <div class="terminal-tabs">
          <span class="terminal-tab active">Output</span>
          <span class="terminal-tab">DLQ</span>
        </div>
        <div class="terminal-actions">
          <button
            class="terminal-btn"
            title="Clear"
            onClick={() => {
              // Dispatch clear event — parent handles state
              window.dispatchEvent(new CustomEvent('terminal-clear'));
            }}
          >
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <circle cx="12" cy="12" r="10"/>
              <line x1="4.93" y1="4.93" x2="19.07" y2="19.07"/>
            </svg>
          </button>
        </div>
      </div>

      <div class="terminal-output" ref={containerRef}>
        {props.logs.length === 0 ? (
          <div class="terminal-empty">
            <span class="terminal-prompt">&gt;</span> Ready. Click "Test Pipeline" to run.
          </div>
        ) : (
          props.logs.map((line) => (
            <div class={`terminal-line ${getLogLevel(line)}`}>
              <pre>{line}</pre>
            </div>
          ))
        )}
      </div>
    </div>
  );
}

function getLogLevel(line: string): string {
  if (line.includes('ERROR') || line.includes('FAILED') || line.includes('error:')) return 'level-error';
  if (line.includes('WARN') || line.includes('warning:')) return 'level-warn';
  if (line.includes('SUCCESS') || line.includes('completed')) return 'level-success';
  if (line.includes('Testing') || line.includes('Building') || line.includes('Starting')) return 'level-info';
  return '';
}
