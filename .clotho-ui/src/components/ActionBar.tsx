import { Show } from 'solid-js';

interface ActionBarProps {
  isDraftMode: boolean;
  onEdit: () => void;
  onSave: () => void;
  onTest: () => void;
  onPublish: () => void;
  onOpenConfig: () => void;
  isSaving: boolean;
  isTesting: boolean;
  isPublishing: boolean;
}

export default function ActionBar(props: ActionBarProps) {
  return (
    <div class="action-bar">
      {/* Mode indicator */}
      <div class="mode-badge">
        <span class={`mode-dot ${props.isDraftMode ? 'draft' : 'readonly'}`} />
        <span class="mode-label">
          {props.isDraftMode ? 'Draft' : 'Read-Only'}
        </span>
      </div>

      <div class="action-buttons">
        {/* Edit button — only shown in read-only mode */}
        <Show when={!props.isDraftMode}>
          <button class="action-btn edit" onClick={props.onEdit}>
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7"/>
              <path d="M18.5 2.5a2.121 2.121 0 013 3L12 15l-4 1 1-4 9.5-9.5z"/>
            </svg>
            Edit Pipeline
          </button>

          {/* Config button — shown in read-only mode */}
          <button class="action-btn config" onClick={props.onOpenConfig}>
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M12.22 2h-.44a2 2 0 00-2 2v.18a2 2 0 01-1 1.73l-.43.25a2 2 0 01-2 0l-.15-.08a2 2 0 00-2.73.73l-.22.38a2 2 0 00.73 2.73l.15.1a2 2 0 011 1.72v.51a2 2 0 01-1 1.74l-.15.09a2 2 0 00-.73 2.73l.22.38a2 2 0 002.73.73l.15-.08a2 2 0 012 0l.43.25a2 2 0 011 1.73V20a2 2 0 002 2h.44a2 2 0 002-2v-.18a2 2 0 011-1.73l.43-.25a2 2 0 012 0l.15.08a2 2 0 002.73-.73l.22-.39a2 2 0 00-.73-2.73l-.15-.08a2 2 0 01-1-1.74v-.5a2 2 0 011-1.74l.15-.09a2 2 0 00.73-2.73l-.22-.38a2 2 0 00-2.73-.73l-.15.08a2 2 0 01-2 0l-.43-.25a2 2 0 01-1-1.73V4a2 2 0 00-2-2z" />
              <circle cx="12" cy="12" r="3" />
            </svg>
            Config
          </button>
        </Show>

        {/* Draft mode actions */}
        <Show when={props.isDraftMode}>
          {/* Config button — shown in draft mode */}
          <button class="action-btn config" onClick={props.onOpenConfig}>
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M12.22 2h-.44a2 2 0 00-2 2v.18a2 2 0 01-1 1.73l-.43.25a2 2 0 01-2 0l-.15-.08a2 2 0 00-2.73.73l-.22.38a2 2 0 00.73 2.73l.15.1a2 2 0 011 1.72v.51a2 2 0 01-1 1.74l-.15.09a2 2 0 00-.73 2.73l.22.38a2 2 0 002.73.73l.15-.08a2 2 0 012 0l.43.25a2 2 0 011 1.73V20a2 2 0 002 2h.44a2 2 0 002-2v-.18a2 2 0 011-1.73l.43-.25a2 2 0 012 0l.15.08a2 2 0 002.73-.73l.22-.39a2 2 0 00-.73-2.73l-.15-.08a2 2 0 01-1-1.74v-.5a2 2 0 011-1.74l.15-.09a2 2 0 00.73-2.73l-.22-.38a2 2 0 00-2.73-.73l-.15.08a2 2 0 01-2 0l-.43-.25a2 2 0 01-1-1.73V4a2 2 0 00-2-2z" />
              <circle cx="12" cy="12" r="3" />
            </svg>
            Config
          </button>

          {/* Save Draft */}
          <button
            class={`action-btn save ${props.isSaving ? 'running' : ''}`}
            onClick={props.onSave}
            disabled={props.isSaving}
          >
            <Show when={props.isSaving} fallback={
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M19 21H5a2 2 0 01-2-2V5a2 2 0 012-2h11l5 5v11a2 2 0 01-2 2z"/>
                <polyline points="17,21 17,13 7,13 7,21"/>
                <polyline points="7,3 7,8 15,8"/>
              </svg>
            }>
              <div class="spinner-small" />
            </Show>
            {props.isSaving ? 'Saving...' : 'Save Draft'}
            <Show when={!props.isSaving}>
              <kbd>Ctrl+S</kbd>
            </Show>
          </button>

          {/* Test Pipeline */}
          <button
            class={`action-btn test ${props.isTesting ? 'running' : ''}`}
            onClick={props.onTest}
            disabled={props.isTesting}
          >
            <Show when={props.isTesting} fallback={
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <polygon points="5,3 19,12 5,21 5,3"/>
              </svg>
            }>
              <div class="spinner-small" />
            </Show>
            {props.isTesting ? 'Testing...' : 'Test Pipeline'}
          </button>

          {/* Publish to Production */}
          <button
            class={`action-btn publish ${props.isPublishing ? 'running' : ''}`}
            onClick={props.onPublish}
            disabled={props.isPublishing || props.isTesting}
          >
            <Show when={props.isPublishing} fallback={
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <polyline points="16,16 12,12 8,16"/>
                <line x1="12" y1="12" x2="12" y2="21"/>
                <path d="M20.39 18.39A5 5 0 0018 9h-1.26A8 8 0 103 16.3"/>
              </svg>
            }>
              <div class="spinner-small" />
            </Show>
            {props.isPublishing ? 'Publishing...' : 'Publish'}
          </button>
        </Show>
      </div>
    </div>
  );
}
