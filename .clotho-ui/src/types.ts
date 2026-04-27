// Shared types for the Clotho IDE

export interface FileNode {
  name: string;
  path: string;
  type: 'file' | 'dir';
  sha: string;
  size?: number;
  children?: FileNode[];
}
