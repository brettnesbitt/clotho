import { createSignal, For, Show } from 'solid-js';
import type { FileNode } from '../types';

interface FileTreeProps {
  files: FileNode[];
  onSelectFile: (path: string) => void;
  selectedFile: string | null;
}

export default function FileTree(props: FileTreeProps) {
  return (
    <div class="file-tree">
      <div class="file-tree-header">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <path d="M22 19a2 2 0 01-2 2H4a2 2 0 01-2-2V5a2 2 0 012-2h5l2 3h9a2 2 0 012 2z"/>
        </svg>
        <span>Explorer</span>
      </div>
      <div class="file-tree-list">
        <For each={props.files}>
          {(node) => (
            <TreeNode
              node={node}
              depth={0}
              onSelectFile={props.onSelectFile}
              selectedFile={props.selectedFile}
            />
          )}
        </For>
      </div>
    </div>
  );
}

interface TreeNodeProps {
  node: FileNode;
  depth: number;
  onSelectFile: (path: string) => void;
  selectedFile: string | null;
}

function TreeNode(props: TreeNodeProps) {
  const [expanded, setExpanded] = createSignal(props.depth < 2);

  function handleClick() {
    if (props.node.type === 'dir') {
      setExpanded(!expanded());
    } else {
      props.onSelectFile(props.node.path);
    }
  }

  function getFileIcon(name: string, type: string): string {
    if (type === 'dir') return expanded() ? '\u25BE' : '\u25B8';

    const ext = name.split('.').pop()?.toLowerCase();
    switch (ext) {
      case 'rs': return '\u{1F9F1}';
      case 'toml': return '\u2699';
      case 'json': return '{}';
      case 'yaml':
      case 'yml': return '\u2630';
      case 'md': return '\u2756';
      case 'lock': return '\u{1F512}';
      default: return '\u2022';
    }
  }

  const isSelected = () => props.selectedFile === props.node.path;
  const indent = () => `${props.depth * 16 + 8}px`;

  return (
    <>
      <div
        class={`tree-node ${isSelected() ? 'selected' : ''} ${props.node.type === 'dir' ? 'directory' : 'file'}`}
        style={{ 'padding-left': indent() }}
        onClick={handleClick}
      >
        <span class="tree-icon">
          {getFileIcon(props.node.name, props.node.type)}
        </span>
        <span class="tree-name">{props.node.name}</span>
        <Show when={props.node.size}>
          <span class="tree-size">{formatSize(props.node.size!)}</span>
        </Show>
      </div>

      <Show when={props.node.type === 'dir' && expanded() && props.node.children}>
        <For each={props.node.children}>
          {(child) => (
            <TreeNode
              node={child}
              depth={props.depth + 1}
              onSelectFile={props.onSelectFile}
              selectedFile={props.selectedFile}
            />
          )}
        </For>
      </Show>
    </>
  );
}

function formatSize(bytes: number): string {
  if (bytes < 1024) return `${bytes}B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)}K`;
  return `${(bytes / (1024 * 1024)).toFixed(1)}M`;
}
