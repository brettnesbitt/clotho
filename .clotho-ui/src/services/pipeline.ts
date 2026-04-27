// Pipeline Service
// Communicates with the Clotho Control Plane API to test and manage pipelines.
// The Control Plane orchestrates the Kubernetes Operator for build/deploy.

const DEFAULT_API_URL = 'http://localhost:3000';

interface TestResult {
  success: boolean;
  logs: string[];
  duration_ms: number;
  records_processed?: number;
  dlq_count?: number;
}

export interface TestBuildResponse {
  test_id: string;
  status: string;
  environment: string;
}

export interface TestBuildStatus {
  test_id: string;
  status: 'pending' | 'running' | 'succeeded' | 'failed';
  start_time?: string;
  completion_time?: string;
}

export interface ConfigEntry {
  name: string;
  value?: string;
  source: 'literal' | 'secret';
  secret_name?: string;
  secret_key?: string;
}

export interface ConfigUpdate {
  name: string;
  value?: string;
  valueFrom?: {
    secretKeyRef?: {
      name: string;
      key: string;
    };
  };
}

export interface PipelineInfo {
  id: string;
  environment: string;
  phase: string;
  status: string;
  mode: string;
  image?: string;
  git_repository?: string;
  git_ref?: string;
  path?: string;
  desired_replicas?: number;
  created_at?: string;
}

interface LogEntry {
  timestamp: string;
  level: string;
  message: string;
  pipeline?: string;
  stage?: string;
}

export class PipelineService {
  private apiUrl: string;

  constructor(apiUrl?: string) {
    this.apiUrl = apiUrl || localStorage.getItem('clotho_api_url') || DEFAULT_API_URL;
  }

  private async request<T>(path: string, options: RequestInit = {}): Promise<T> {
    const token = localStorage.getItem('clotho_api_token');
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
    };
    if (token) {
      headers['Authorization'] = `Bearer ${token}`;
    }

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 15000);

    let res: Response;
    try {
      res = await fetch(`${this.apiUrl}${path}`, {
        ...options,
        headers: { ...headers, ...(options.headers || {}) },
        signal: controller.signal,
      });
    } catch (err: any) {
      clearTimeout(timeout);
      if (err.name === 'AbortError') {
        throw new Error(`Request timeout: ${this.apiUrl}${path} did not respond within 15s`);
      }
      throw new Error(`Network error: ${err.message} (url: ${this.apiUrl}${path})`);
    } finally {
      clearTimeout(timeout);
    }

    if (!res.ok) {
      const body = await res.text();
      throw new Error(`API ${res.status}: ${body}`);
    }

    return res.json();
  }

  // ── Test Pipeline ───────────────────────────────────────────────────────────
  // Tells the Control Plane to build a temporary pod from the draft branch,
  // run it against sample data, and stream the output back.

  /**
   * Trigger a test build for a pipeline from a draft branch.
   * Creates an ephemeral builder Job in the cluster.
   * Returns a test_id to poll status and stream logs.
   */
  async testPipeline(owner: string, repo: string, branch: string, path?: string): Promise<TestBuildResponse> {
    return this.request<TestBuildResponse>('/v1/pipelines/test', {
      method: 'POST',
      body: JSON.stringify({
        git_repository: `https://github.com/${owner}/${repo}`,
        reference: branch,
        path: path || '',
      }),
    });
  }

  /**
   * Poll the status of a test build.
   */
  async getTestStatus(testId: string): Promise<TestBuildStatus> {
    return this.request<TestBuildStatus>(`/v1/pipelines/test/${encodeURIComponent(testId)}/status`);
  }

  /**
   * Delete a completed test build job.
   */
  async deleteTestBuild(testId: string): Promise<void> {
    await this.request<any>(`/v1/pipelines/test/${encodeURIComponent(testId)}`, {
      method: 'DELETE',
    });
  }

  /**
   * Stream test logs via SSE (Server-Sent Events).
   * Returns an EventSource that emits log lines in real-time.
   */
  streamTestLogs(testId: string, onLog: (entry: LogEntry) => void, onDone: () => void): () => void {
    const token = localStorage.getItem('clotho_api_token');
    const url = `${this.apiUrl}/v1/pipelines/test/${testId}/logs`;

    const eventSource = new EventSource(
      token ? `${url}?token=${encodeURIComponent(token)}` : url
    );

    eventSource.onmessage = (event) => {
      try {
        const entry: LogEntry = JSON.parse(event.data);
        onLog(entry);
      } catch {
        onLog({ timestamp: new Date().toISOString(), level: 'info', message: event.data });
      }
    };

    eventSource.addEventListener('done', () => {
      onDone();
      eventSource.close();
    });

    eventSource.onerror = () => {
      onDone();
      eventSource.close();
    };

    // Return cleanup function
    return () => eventSource.close();
  }

  // ── Pipeline CRUD ───────────────────────────────────────────────────────────

  /**
   * List all pipelines in the cluster.
   */
  async listPipelines(environment: string = 'production'): Promise<PipelineInfo[]> {
    return this.request<PipelineInfo[]>(`/v1/pipelines?environment=${encodeURIComponent(environment)}`);
  }

  /**
   * Get details for a specific pipeline.
   */
  async getPipeline(name: string, environment: string = 'production'): Promise<PipelineInfo> {
    return this.request<PipelineInfo>(
      `/v1/pipelines/${encodeURIComponent(name)}?environment=${encodeURIComponent(environment)}`
    );
  }

  /**
   * Trigger an on-demand invocation of a pipeline.
   */
  async invokePipeline(name: string, environment: string = 'production'): Promise<{ execution_id: string }> {
    return this.request<{ execution_id: string }>(
      `/v1/pipelines/${encodeURIComponent(name)}/invoke?environment=${encodeURIComponent(environment)}`,
      {
        method: 'POST',
      }
    );
  }

  // ── Pod Logs ────────────────────────────────────────────────────────────────

  /**
   * Stream live pod logs for a running pipeline via SSE.
   */
  streamPodLogs(
    name: string,
    environment: string,
    onLog: (line: string) => void,
    onError: (err: string) => void
  ): () => void {
    const token = localStorage.getItem('clotho_api_token');
    const url = `${this.apiUrl}/v1/pipelines/${encodeURIComponent(name)}/logs?environment=${encodeURIComponent(environment)}&follow=true`;

    const eventSource = new EventSource(
      token ? `${url}&token=${encodeURIComponent(token)}` : url
    );

    eventSource.onmessage = (event) => {
      onLog(event.data);
    };

    eventSource.onerror = () => {
      onError('Log stream disconnected');
      eventSource.close();
    };

    return () => eventSource.close();
  }

  // ── Config Management ──────────────────────────────────────────────────────

  /**
   * Get pipeline config vars (env vars and secret references).
   */
  async getConfig(pipelineId: string, environment: string = 'production'): Promise<{ pipeline_id: string; config: ConfigEntry[] }> {
    return this.request<{ pipeline_id: string; config: ConfigEntry[] }>(
      `/v1/pipelines/${encodeURIComponent(pipelineId)}/config?environment=${encodeURIComponent(environment)}`
    );
  }

  /**
   * Update pipeline config vars.
   */
  async updateConfig(pipelineId: string, config: ConfigUpdate[], environment: string = 'production'): Promise<void> {
    await this.request<any>(
      `/v1/pipelines/${encodeURIComponent(pipelineId)}/config?environment=${encodeURIComponent(environment)}`,
      {
        method: 'PATCH',
        body: JSON.stringify({ config }),
      }
    );
  }

  // ── DLQ ─────────────────────────────────────────────────────────────────────

  /**
   * Fetch Dead Letter Queue records for a pipeline.
   */
  async getDLQRecords(pipelineName: string, limit: number = 50): Promise<any[]> {
    return this.request<any[]>(
      `/v1/pipelines/${encodeURIComponent(pipelineName)}/dlq?limit=${limit}`
    );
  }

  /**
   * List pipelines that source from a specific GitHub repository.
   * Matches on the git_repository field which contains the full URL.
   */
  async listPipelinesByRepo(owner: string, repo: string, environment: string = 'production'): Promise<PipelineInfo[]> {
    const all = await this.listPipelines(environment);
    const repoUrl = `https://github.com/${owner}/${repo}`;
    console.log('[listPipelinesByRepo] owner=%s repo=%s repoUrl=%s allCount=%d gitRepos=%o', owner, repo, repoUrl, all.length, all.map(p => p.git_repository));
    const matched = all.filter(p =>
      p.git_repository?.toLowerCase() === repoUrl.toLowerCase() ||
      p.git_repository?.toLowerCase() === `${repoUrl.toLowerCase()}.git`
    );
    console.log('[listPipelinesByRepo] matched=%d', matched.length);
    return matched;
  }
}
