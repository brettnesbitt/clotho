// GitHub Trees API Service
// Stateless file operations — no git clone, no local filesystem.
// All state lives in GitHub. The UI just reads/writes via the REST API.

const GITHUB_API = 'https://api.github.com';

interface GitHubTreeItem {
  path: string;
  mode: string;
  type: 'blob' | 'tree';
  sha: string;
  size?: number;
  url: string;
}

interface GitHubRef {
  ref: string;
  object: { sha: string; type: string; url: string };
}

interface GitHubCommit {
  sha: string;
  tree: { sha: string };
  parents: { sha: string }[];
}

interface GitHubBlob {
  sha: string;
  content: string;
  encoding: string;
  size: number;
}

interface GitHubPullRequest {
  number: number;
  html_url: string;
  state: string;
  title: string;
}

export interface FileNode {
  name: string;
  path: string;
  type: 'file' | 'dir';
  sha: string;
  size?: number;
  children?: FileNode[];
}

export class GitHubService {
  private token: string | null = null;

  setToken(token: string) {
    this.token = token;
  }

  private headers(): Record<string, string> {
    const h: Record<string, string> = {
      'Accept': 'application/vnd.github+json',
      'X-GitHub-Api-Version': '2022-11-28',
    };
    if (this.token) {
      h['Authorization'] = `Bearer ${this.token}`;
    }
    return h;
  }

  private async request<T>(path: string, options: RequestInit = {}): Promise<T> {
    const url = path.startsWith('http') ? path : `${GITHUB_API}${path}`;
    const contentHeaders: Record<string, string> = {};
    if (options.body) {
      contentHeaders['Content-Type'] = 'application/json';
    }
    const res = await fetch(url, {
      ...options,
      headers: { ...this.headers(), ...contentHeaders, ...(options.headers || {}) },
    });

    if (!res.ok) {
      const body = await res.text();
      throw new Error(`GitHub API ${res.status}: ${body}`);
    }

    return res.json();
  }

  // ── Tree Operations ─────────────────────────────────────────────────────────
  // Uses the Git Database API for blazing-fast, stateless file access.

  /**
   * Fetch the full recursive tree for a repo at a given ref.
   * Returns a flat list which we convert into a nested FileNode tree.
   */
  async getTree(owner: string, repo: string, ref: string = 'main'): Promise<FileNode[]> {
    const data = await this.request<{ tree: GitHubTreeItem[]; sha: string; truncated: boolean }>(
      `/repos/${owner}/${repo}/git/trees/${ref}?recursive=1`
    );

    return this.buildFileTree(data.tree);
  }

  /**
   * Convert flat GitHub tree items into a nested FileNode structure.
   */
  private buildFileTree(items: GitHubTreeItem[]): FileNode[] {
    const root: FileNode[] = [];
    const dirMap = new Map<string, FileNode>();

    // Sort so directories come before files, then alphabetically
    const sorted = [...items].sort((a, b) => {
      if (a.type !== b.type) return a.type === 'tree' ? -1 : 1;
      return a.path.localeCompare(b.path);
    });

    for (const item of sorted) {
      const parts = item.path.split('/');
      const name = parts[parts.length - 1];
      const parentPath = parts.slice(0, -1).join('/');

      const node: FileNode = {
        name,
        path: item.path,
        type: item.type === 'tree' ? 'dir' : 'file',
        sha: item.sha,
        size: item.size,
        children: item.type === 'tree' ? [] : undefined,
      };

      if (item.type === 'tree') {
        dirMap.set(item.path, node);
      }

      if (parentPath === '') {
        root.push(node);
      } else {
        const parent = dirMap.get(parentPath);
        if (parent && parent.children) {
          parent.children.push(node);
        }
      }
    }

    return root;
  }

  // ── Blob Operations (Read/Write Files) ──────────────────────────────────────

  /**
   * Read a file's content via the Git Blobs API.
   * Returns decoded UTF-8 string.
   */
  async getFileContent(owner: string, repo: string, path: string, ref: string = 'main'): Promise<string> {
    // Use the Contents API for simplicity — returns Base64 for files < 1MB
    const data = await this.request<{ content: string; encoding: string; sha: string }>(
      `/repos/${owner}/${repo}/contents/${path}?ref=${encodeURIComponent(ref)}`
    );

    if (data.encoding === 'base64') {
      return atob(data.content.replace(/\n/g, ''));
    }

    return data.content;
  }

  /**
   * Get the SHA of a file (needed for updates via Contents API).
   */
  async getFileSha(owner: string, repo: string, path: string, ref: string = 'main'): Promise<string> {
    const data = await this.request<{ sha: string }>(
      `/repos/${owner}/${repo}/contents/${path}?ref=${encodeURIComponent(ref)}`
    );
    return data.sha;
  }

  // ── Stateless Write Pipeline (Trees API) ────────────────────────────────────
  // Instead of git add/commit/push, we: create blob → create tree → create commit → update ref.

  /**
   * Write a file using the Git Database API (Trees pipeline).
   * This is the stateless write path — no local clone needed.
   */
  async updateFile(
    owner: string,
    repo: string,
    path: string,
    content: string,
    branch: string,
    message: string
  ): Promise<void> {
    // 1. Get the current commit SHA for this branch
    const ref = await this.request<GitHubRef>(
      `/repos/${owner}/${repo}/git/ref/heads/${encodeURIComponent(branch)}`
    );
    const currentCommitSha = ref.object.sha;

    // 2. Get the tree SHA from the current commit
    const commit = await this.request<GitHubCommit>(
      `/repos/${owner}/${repo}/git/commits/${currentCommitSha}`
    );
    const baseTreeSha = commit.tree.sha;

    // 3. Create a new blob with the file content
    const blob = await this.request<{ sha: string }>(
      `/repos/${owner}/${repo}/git/blobs`,
      {
        method: 'POST',
        body: JSON.stringify({ content: btoa(unescape(encodeURIComponent(content))), encoding: 'base64' }),
      }
    );

    // 4. Create a new tree with the updated blob
    const tree = await this.request<{ sha: string }>(
      `/repos/${owner}/${repo}/git/trees`,
      {
        method: 'POST',
        body: JSON.stringify({
          base_tree: baseTreeSha,
          tree: [{ path, mode: '100644', type: 'blob', sha: blob.sha }],
        }),
      }
    );

    // 5. Create a new commit pointing to the new tree
    const newCommit = await this.request<{ sha: string }>(
      `/repos/${owner}/${repo}/git/commits`,
      {
        method: 'POST',
        body: JSON.stringify({
          message,
          tree: tree.sha,
          parents: [currentCommitSha],
        }),
      }
    );

    // 6. Update the branch ref to point to the new commit
    await this.request(
      `/repos/${owner}/${repo}/git/refs/heads/${encodeURIComponent(branch)}`,
      {
        method: 'PATCH',
        body: JSON.stringify({ sha: newCommit.sha }),
      }
    );
  }

  // ── Branch Operations ───────────────────────────────────────────────────────

  /**
   * Create a new branch from a source branch.
   * Used for the "Draft" workflow: main → clotho-draft/{user}/{pipeline}
   */
  async createBranch(owner: string, repo: string, newBranch: string, fromBranch: string = 'main'): Promise<void> {
    // Check if branch already exists
    try {
      await this.request(`/repos/${owner}/${repo}/git/ref/heads/${encodeURIComponent(newBranch)}`);
      // Branch exists — fast-forward it to latest from source
      const sourceRef = await this.request<GitHubRef>(
        `/repos/${owner}/${repo}/git/ref/heads/${encodeURIComponent(fromBranch)}`
      );
      await this.request(
        `/repos/${owner}/${repo}/git/refs/heads/${encodeURIComponent(newBranch)}`,
        {
          method: 'PATCH',
          body: JSON.stringify({ sha: sourceRef.object.sha, force: true }),
        }
      );
      return;
    } catch {
      // Branch doesn't exist — create it
    }

    const sourceRef = await this.request<GitHubRef>(
      `/repos/${owner}/${repo}/git/ref/heads/${encodeURIComponent(fromBranch)}`
    );

    await this.request(
      `/repos/${owner}/${repo}/git/refs`,
      {
        method: 'POST',
        body: JSON.stringify({
          ref: `refs/heads/${newBranch}`,
          sha: sourceRef.object.sha,
        }),
      }
    );
  }

  /**
   * Delete a branch (cleanup after merge).
   */
  async deleteBranch(owner: string, repo: string, branch: string): Promise<void> {
    await this.request(
      `/repos/${owner}/${repo}/git/refs/heads/${encodeURIComponent(branch)}`,
      { method: 'DELETE' }
    );
  }

  // ── Pull Request Operations ─────────────────────────────────────────────────

  /**
   * Create a pull request from draft branch to main.
   * This is the "Publish" step.
   */
  async createPullRequest(
    owner: string,
    repo: string,
    head: string,
    base: string,
    title: string,
    body: string
  ): Promise<GitHubPullRequest> {
    return this.request<GitHubPullRequest>(
      `/repos/${owner}/${repo}/pulls`,
      {
        method: 'POST',
        body: JSON.stringify({ title, body, head, base }),
      }
    );
  }

  /**
   * Merge a pull request (squash merge for clean history).
   */
  async mergePullRequest(owner: string, repo: string, pullNumber: number): Promise<void> {
    await this.request(
      `/repos/${owner}/${repo}/pulls/${pullNumber}/merge`,
      {
        method: 'PUT',
        body: JSON.stringify({ merge_method: 'squash' }),
      }
    );
  }

  // ── Branch Operations (Read) ───────────────────────────────────────────────

  /**
   * List all branches in a repository.
   * Returns branch name, SHA, and whether it's protected.
   */
  async listBranches(owner: string, repo: string): Promise<{ name: string; sha: string; protected: boolean }[]> {
    const data = await this.request<any[]>(
      `/repos/${owner}/${repo}/branches?per_page=100`
    );
    return data.map(b => ({
      name: b.name,
      sha: b.commit.sha,
      protected: b.protected,
    }));
  }

  /**
   * Compare two branches — returns ahead/behind counts and diff stats.
   */
  async compareBranches(owner: string, repo: string, base: string, head: string): Promise<{
    ahead_by: number;
    behind_by: number;
    total_commits: number;
    files_changed: number;
  }> {
    const data = await this.request<any>(
      `/repos/${owner}/${repo}/compare/${encodeURIComponent(base)}...${encodeURIComponent(head)}`
    );
    return {
      ahead_by: data.ahead_by,
      behind_by: data.behind_by,
      total_commits: data.total_commits,
      files_changed: data.files?.length ?? 0,
    };
  }

  // ── Repository Discovery ────────────────────────────────────────────────────

  /**
   * List repositories accessible via the current token.
   * Used on the "Connect Repository" flow.
   */
  async listRepositories(): Promise<{ owner: string; name: string; description: string; default_branch: string }[]> {
    const data = await this.request<any[]>(`/user/repos?per_page=100&sort=updated`);
    return data.map(r => ({
      owner: r.owner.login,
      name: r.name,
      description: r.description || '',
      default_branch: r.default_branch,
    }));
  }

  /**
   * List installations for a GitHub App (used for GitHub App OAuth).
   */
  async listInstallations(): Promise<{ id: number; account: { login: string } }[]> {
    const data = await this.request<{ installations: any[] }>(`/user/installations`);
    return data.installations.map(i => ({
      id: i.id,
      account: { login: i.account.login },
    }));
  }

  /**
   * List repos accessible via a specific GitHub App installation.
   */
  async listInstallationRepos(installationId: number): Promise<{ owner: string; name: string; description: string }[]> {
    const data = await this.request<{ repositories: any[] }>(
      `/user/installations/${installationId}/repositories`
    );
    return data.repositories.map(r => ({
      owner: r.owner.login,
      name: r.name,
      description: r.description || '',
    }));
  }
}
