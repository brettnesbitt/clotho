import { createSignal, onMount, Show } from "solid-js";
import type { Component } from "solid-js";
import FileTree from "./FileTree";
import MonacoEditor from "./MonacoEditor";
import ActionBar from "./ActionBar";
import Terminal from "./Terminal";
import GitHubConnect from "./GitHubConnect";
import RepoSelector from "./RepoSelector";
import PipelineContext from "./PipelineContext";
import ConfigPanel from "./ConfigPanel";
import { GitHubService } from "../services/github";
import { PipelineService } from "../services/pipeline";
import type { FileNode } from "../types";
import type { Environment } from "./EnvironmentSwitcher";

type IDEStep = "connect" | "repo" | "editor";

interface IDEPageProps {
  pipelineId?: string | null;
  environment?: Environment;
}

const IDEPage: Component<IDEPageProps> = (props) => {
  // ── Navigation state ────────────────────────────────────────────────────
  const [step, setStep] = createSignal<IDEStep>("connect");

  // ── GitHub state ────────────────────────────────────────────────────────
  const [githubConnected, setGithubConnected] = createSignal(false);
  const [githubUsername, setGithubUsername] = createSignal<string | null>(null);

  // ── Repo state ──────────────────────────────────────────────────────────
  const [repoOwner, setRepoOwner] = createSignal<string | null>(null);
  const [repoName, setRepoName] = createSignal<string | null>(null);
  const [defaultBranch, setDefaultBranch] = createSignal("main");

  // ── Branch / editor state ───────────────────────────────────────────────
  const [activeBranch, setActiveBranch] = createSignal<string | null>(null);
  const [isDraftBranch, setIsDraftBranch] = createSignal(false);
  const [files, setFiles] = createSignal<FileNode[]>([]);
  const [selectedFile, setSelectedFile] = createSignal<string | null>(null);
  const [fileContent, setFileContent] = createSignal<string>("");
  const [logs, setLogs] = createSignal<string[]>([]);
  const [isSaving, setIsSaving] = createSignal(false);
  const [isTesting, setIsTesting] = createSignal(false);
  const [isPublishing, setIsPublishing] = createSignal(false);
  const [isLoadingFiles, setIsLoadingFiles] = createSignal(false);
  const [pipelinePaths, setPipelinePaths] = createSignal<string[]>([]);
  const [selectedPipelineId, setSelectedPipelineId] = createSignal<string | null>(null);

  // ── Modal state ─────────────────────────────────────────────────────────
  const [isConfigModalOpen, setIsConfigModalOpen] = createSignal(false);
  const [isRepoDropdownOpen, setIsRepoDropdownOpen] = createSignal(false);

  // ── Services ────────────────────────────────────────────────────────────
  const githubService = new GitHubService();
  const pipelineService = new PipelineService();

  // ── Init — restore from localStorage ────────────────────────────────────
  onMount(() => {
    const token = localStorage.getItem("github_token");
    const user = localStorage.getItem("github_username");
    if (token && user) {
      githubService.setToken(token);
      setGithubConnected(true);
      setGithubUsername(user);

      const savedOwner = localStorage.getItem("repo_owner");
      const savedRepo = localStorage.getItem("repo_name");
      const savedBranch = localStorage.getItem("repo_default_branch");

      if (savedOwner && savedRepo) {
        setRepoOwner(savedOwner);
        setRepoName(savedRepo);
        setDefaultBranch(savedBranch || "main");
        setActiveBranch(savedBranch || "main");
        setStep("editor");
        loadFiles(savedOwner, savedRepo, savedBranch || "main");
      } else {
        setStep("repo");
      }
    }
  });

  // ── GitHub connect ──────────────────────────────────────────────────────
  function handleGitHubConnect(token: string, username: string) {
    localStorage.setItem("github_token", token);
    localStorage.setItem("github_username", username);
    githubService.setToken(token);
    setGithubConnected(true);
    setGithubUsername(username);
    setStep("repo");
    addLog(`Connected to GitHub as ${username}`);
  }

  function handleDisconnect() {
    localStorage.removeItem("github_token");
    localStorage.removeItem("github_username");
    localStorage.removeItem("repo_owner");
    localStorage.removeItem("repo_name");
    localStorage.removeItem("repo_default_branch");
    setGithubConnected(false);
    setGithubUsername(null);
    setRepoOwner(null);
    setRepoName(null);
    setActiveBranch(null);
    setFiles([]);
    setSelectedFile(null);
    setFileContent("");
    setLogs([]);
    setStep("connect");
  }

  // ── Repo selection ──────────────────────────────────────────────────────
  function handleRepoSelect(owner: string, repo: string, defBranch: string) {
    localStorage.setItem("repo_owner", owner);
    localStorage.setItem("repo_name", repo);
    localStorage.setItem("repo_default_branch", defBranch);
    setRepoOwner(owner);
    setRepoName(repo);
    setDefaultBranch(defBranch);
    setActiveBranch(defBranch);
    setStep("editor");
    addLog(`Opened repository: ${owner}/${repo}`);
    loadFiles(owner, repo, defBranch);
  }

  function handleChangeRepo() {
    setRepoOwner(null);
    setRepoName(null);
    setActiveBranch(null);
    setFiles([]);
    setSelectedFile(null);
    setFileContent("");
    localStorage.removeItem("repo_owner");
    localStorage.removeItem("repo_name");
    localStorage.removeItem("repo_default_branch");
    setStep("repo");
  }

  // ── Branch selection ────────────────────────────────────────────────────
  function handleSelectBranch(branch: string) {
    setActiveBranch(branch);
    setIsDraftBranch(branch.startsWith("clotho-draft/"));
    setSelectedFile(null);
    setFileContent("");
    addLog(`Switched to branch: ${branch}`);
    if (repoOwner() && repoName()) {
      loadFiles(repoOwner()!, repoName()!, branch);
    }
  }

  async function handleCreateDraft(baseBranch: string) {
    if (!repoOwner() || !repoName()) return;

    const username = githubUsername() || "user";
    const timestamp = Date.now().toString(36);
    const branchName = `clotho-draft/${username}/${timestamp}`;

    try {
      addLog(`Creating draft branch from ${baseBranch}...`);
      await githubService.createBranch(
        repoOwner()!,
        repoName()!,
        branchName,
        baseBranch
      );
      setActiveBranch(branchName);
      setIsDraftBranch(true);
      addLog(`Draft branch created: ${branchName}`);
      loadFiles(repoOwner()!, repoName()!, branchName);
    } catch (error) {
      addLog(`Error creating draft: ${error}`);
    }
  }

  // ── File operations ─────────────────────────────────────────────────────
  async function loadFiles(owner: string, repo: string, branch: string) {
    setIsLoadingFiles(true);
    try {
      const tree = await githubService.getTree(owner, repo, branch);
      setFiles(tree);
      addLog(`Loaded ${tree.length} items from ${branch}`);
    } catch (error) {
      addLog(`Error loading files: ${error}`);
      setFiles([]);
    } finally {
      setIsLoadingFiles(false);
    }
  }

  async function loadFileContent(path: string) {
    if (!repoOwner() || !repoName() || !activeBranch()) return;

    try {
      const content = await githubService.getFileContent(
        repoOwner()!,
        repoName()!,
        path,
        activeBranch()!
      );
      setFileContent(content);
      setSelectedFile(path);
      addLog(`Opened: ${path}`);
    } catch (error) {
      addLog(`Error loading file: ${error}`);
    }
  }

  async function saveFile() {
    if (!selectedFile() || !repoOwner() || !repoName() || !activeBranch()) return;
    if (isSaving()) {
      addLog("Save already in progress...");
      return;
    }

    setIsSaving(true);
    try {
      await githubService.updateFile(
        repoOwner()!,
        repoName()!,
        selectedFile()!,
        fileContent(),
        activeBranch()!,
        `Update ${selectedFile()}`
      );
      addLog(`Saved: ${selectedFile()}`);
    } catch (error) {
      addLog(`Error saving: ${error}`);
    } finally {
      setIsSaving(false);
    }
  }

  async function enterDraftMode() {
    if (!activeBranch()) return;
    // If already on a draft branch, no-op
    if (isDraftBranch()) return;
    // Create a new draft from the current branch
    await handleCreateDraft(activeBranch()!);
  }

  async function testPipeline() {
    if (!repoOwner() || !repoName() || !activeBranch()) return;

    setIsTesting(true);
    addLog("Starting pipeline test build...");

    const repo = `https://github.com/${repoOwner()}/${repoName()}`;
    const branch = activeBranch()!;
    const path = pipelinePaths()[0] || "";
    addLog(`  repo: ${repo}`);
    addLog(`  branch: ${branch}`);
    addLog(`  path: ${path || "(root)"}`);

    let cleanupSSE: (() => void) | null = null;

    try {
      // 1. Create the ephemeral builder Job
      addLog("Creating builder job...");
      const build = await pipelineService.testPipeline(
        repoOwner()!,
        repoName()!,
        branch,
        path
      );
      addLog(`Builder job created: ${build.test_id} (env: ${build.environment})`);

      // 2. Stream logs via SSE
      addLog("Connecting to build log stream...");
      cleanupSSE = pipelineService.streamTestLogs(
        build.test_id,
        (entry) => {
          addLog(entry.message || String(entry));
        },
        () => {
          addLog("Log stream ended.");
        }
      );

      // 3. Poll for completion
      addLog("Polling build status...");
      let finalStatus = "pending";
      let lastReported = "";
      for (let i = 0; i < 180; i++) {
        await new Promise((r) => setTimeout(r, 5000));
        try {
          const status = await pipelineService.getTestStatus(build.test_id);
          if (status.status !== lastReported) {
            addLog(`Build status: ${status.status}`);
            lastReported = status.status;
          }
          if (status.status === "succeeded" || status.status === "failed") {
            finalStatus = status.status;
            break;
          }
        } catch (pollErr: any) {
          if (i === 0) addLog(`Waiting for builder pod... (${pollErr?.message || pollErr})`);
        }
      }

      if (finalStatus === "succeeded") {
        addLog("Build succeeded.");
      } else if (finalStatus === "failed") {
        addLog("Build failed. Check logs above for details.");
      } else {
        addLog("Build timed out (15 min). Check the cluster for status.");
      }

      // 4. Cleanup the test job
      try {
        await pipelineService.deleteTestBuild(build.test_id);
      } catch {
        // Non-critical — TTL will clean it up
      }
    } catch (error: any) {
      const msg = String(error?.message || error);
      console.error("[testPipeline] Error:", error);
      if (msg.includes("503")) {
        addLog("ERROR: Control Plane K8s client not available. The API must be running in-cluster.");
      } else if (msg.includes("Network error") || msg.includes("Failed to fetch")) {
        addLog(`ERROR: Cannot reach API. Is the Control Plane running? (${msg})`);
      } else if (msg.includes("timeout")) {
        addLog(`ERROR: API request timed out — no response from the Control Plane. (${msg})`);
      } else if (msg.includes("404") || msg.includes("405")) {
        addLog("ERROR: /v1/pipelines/test endpoint not found. The API needs to be redeployed with the test build endpoints.");
      } else {
        addLog(`ERROR: ${msg}`);
      }
    } finally {
      if (cleanupSSE) cleanupSSE();
      setIsTesting(false);
      addLog("Test build flow complete.");
    }
  }

  async function publishPipeline() {
    if (!repoOwner() || !repoName() || !activeBranch() || !isDraftBranch()) return;

    setIsPublishing(true);

    try {
      // Pre-flight: check if draft has commits ahead of base
      addLog(`Comparing ${activeBranch()} with ${defaultBranch()}...`);
      const comparison = await githubService.compareBranches(
        repoOwner()!,
        repoName()!,
        defaultBranch(),
        activeBranch()!
      );

      if (comparison.ahead_by === 0) {
        addLog("Nothing to publish — no changes on this draft branch yet. Save a file first.");
        return;
      }

      addLog(`Found ${comparison.ahead_by} commit(s) ahead, ${comparison.files_changed} file(s) changed. Creating pull request...`);
      const pr = await githubService.createPullRequest(
        repoOwner()!,
        repoName()!,
        activeBranch()!,
        defaultBranch(),
        `Clotho: update pipeline from ${activeBranch()}`,
        `Automated update from Clotho IDE.\n\nBranch: \`${activeBranch()}\`\nTarget: \`${defaultBranch()}\`\n\n${comparison.ahead_by} commit(s), ${comparison.files_changed} file(s) changed.`
      );
      addLog(`Pull request created: ${pr.html_url}`);

      if (localStorage.getItem("auto_merge") === "true") {
        await githubService.mergePullRequest(repoOwner()!, repoName()!, pr.number);
        addLog("Pull request merged successfully");
      }
    } catch (error) {
      addLog(`Publish error: ${error}`);
    } finally {
      setIsPublishing(false);
    }
  }

  function addLog(message: string) {
    const timestamp = new Date().toLocaleTimeString();
    setLogs((prev) => [...prev, `[${timestamp}] ${message}`]);
  }

  // ── Config modal handlers ───────────────────────────────────────────────
  function openConfigModal() {
    setIsConfigModalOpen(true);
  }

  function closeConfigModal() {
    setIsConfigModalOpen(false);
  }

  // ── Repo dropdown handlers ──────────────────────────────────────────────
  function toggleRepoDropdown() {
    setIsRepoDropdownOpen(!isRepoDropdownOpen());
  }

  function closeRepoDropdown() {
    setIsRepoDropdownOpen(false);
  }

  // ── Render ──────────────────────────────────────────────────────────────

  return (
    <>
      {/* Step 1: GitHub Connect */}
      <Show when={step() === "connect"}>
        <div class="flex items-center justify-center h-[calc(100vh-48px)]">
          <GitHubConnect onConnect={handleGitHubConnect} />
        </div>
      </Show>

      {/* Step 2: Repo Selector */}
      <Show when={step() === "repo"}>
        <RepoSelector
          githubService={githubService}
          onSelect={handleRepoSelect}
          onBack={handleDisconnect}
        />
      </Show>

      {/* Step 3: Branch-aware Editor */}
      <Show when={step() === "editor" && repoOwner() && repoName()}>
        <div class="ide-container" style="grid-template-columns: 260px 1fr; grid-template-rows: 40px 1fr 200px; display: grid;">
          {/* Header — spans full width */}
          <div class="ide-header" style="grid-column: 1 / -1;">
            <div class="ide-pipeline-name">
              {/* Repository dropdown */}
              <div class="relative">
                <button
                  onClick={toggleRepoDropdown}
                  class="inline-flex items-center gap-1.5 px-2 py-0.5 rounded text-[11px] font-mono bg-slate-800 text-slate-300 border border-slate-700 hover:border-slate-600 transition-colors"
                >
                  <svg class="w-3 h-3 text-slate-500" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor">
                    <path stroke-linecap="round" stroke-linejoin="round" d="M2.25 12.75V12A2.25 2.25 0 014.5 9.75h15A2.25 2.25 0 0121.75 12v.75m-8.69-6.44l-2.12-2.12a1.5 1.5 0 00-1.061-.44H4.5A2.25 2.25 0 002.25 6v12a2.25 2.25 0 002.25 2.25h15A2.25 2.25 0 0021.75 18V9a2.25 2.25 0 00-2.25-2.25h-5.379a1.5 1.5 0 01-1.06-.44z" />
                  </svg>
                  {repoOwner()}/{repoName()}
                  <svg class="w-3 h-3 text-slate-500" fill="none" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor">
                    <path stroke-linecap="round" stroke-linejoin="round" d="M19.5 8.25l-7.5 7.5-7.5-7.5" />
                  </svg>
                </button>

                <Show when={isRepoDropdownOpen()}>
                  <div class="absolute top-full left-0 mt-1 w-56 bg-slate-900 border border-slate-700 rounded-lg shadow-xl z-50 py-1">
                    <div class="px-3 py-2 border-b border-slate-800">
                      <span class="text-[10px] text-slate-500 uppercase tracking-wider">Current Repository</span>
                      <div class="text-xs text-slate-300 font-mono mt-0.5">{repoOwner()}/{repoName()}</div>
                    </div>
                    <button
                      onClick={() => { closeRepoDropdown(); handleChangeRepo(); }}
                      class="w-full px-3 py-2 text-left text-xs text-slate-300 hover:bg-slate-800/50 flex items-center gap-2 transition-colors"
                    >
                      <svg class="w-4 h-4 text-slate-500" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor">
                        <path stroke-linecap="round" stroke-linejoin="round" d="M7.5 21L3 16.5m0 0L7.5 12M3 16.5h13.5m0-13.5L21 7.5m0 0L16.5 12M21 7.5H7.5" />
                      </svg>
                      Change Repository
                    </button>
                  </div>
                </Show>
              </div>

              <Show when={activeBranch()}>
                <span
                  class={`inline-flex items-center gap-1.5 px-2 py-0.5 rounded text-[11px] font-mono ${
                    isDraftBranch()
                      ? "bg-orange-500/10 text-orange-400 border border-orange-500/20"
                      : "bg-blue-500/10 text-blue-400 border border-blue-500/20"
                  }`}
                >
                  <svg class="w-3 h-3" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor">
                    <path stroke-linecap="round" stroke-linejoin="round" d="M3 7.5L7.5 3m0 0L12 7.5M7.5 3v13.5m13.5-3L16.5 18m0 0L12 13.5m4.5 4.5V6" />
                  </svg>
                  {activeBranch()}
                </span>
              </Show>
              <Show when={isDraftBranch()}>
                <span class="text-[10px] text-orange-400/60 font-mono">
                  &rarr; {defaultBranch()}
                </span>
              </Show>
              <Show when={selectedFile()}>
                <span class="text-slate-600 mx-1">/</span>
                <span class="text-[11px] text-slate-400 font-mono">{selectedFile()}</span>
              </Show>
            </div>
            <ActionBar
              isDraftMode={isDraftBranch()}
              onEdit={enterDraftMode}
              onSave={saveFile}
              onTest={testPipeline}
              onPublish={publishPipeline}
              onOpenConfig={openConfigModal}
              isSaving={isSaving()}
              isTesting={isTesting()}
              isPublishing={isPublishing()}
            />
          </div>

          {/* Left panel: Pipeline Context (top) + File Tree (bottom) */}
          <div class="flex flex-col overflow-hidden" style="grid-row: 2 / 4;">
            <div class="flex-shrink-0" style="max-height: 45%;">
              <PipelineContext
                githubService={githubService}
                pipelineService={pipelineService}
                owner={repoOwner()!}
                repo={repoName()!}
                defaultBranch={defaultBranch()}
                activeBranch={activeBranch()}
                onSelectBranch={handleSelectBranch}
                onCreateDraft={handleCreateDraft}
                onChangeRepo={handleChangeRepo}
                onPipelinePaths={setPipelinePaths}
                onSelectPipeline={setSelectedPipelineId}
              />
            </div>
            <ConfigPanel
              pipelineService={pipelineService}
              pipelineId={selectedPipelineId()}
              environment={props.environment}
              onLog={addLog}
            />
            <div class="flex-1 overflow-hidden ide-sidebar border-t border-slate-800">
              <div class="px-3 py-2 border-b border-slate-800 flex-shrink-0">
                <div class="flex items-center justify-between">
                  <span class="text-[10px] text-slate-500 uppercase tracking-wider font-semibold">
                    Files
                  </span>
                  <Show when={isLoadingFiles()}>
                    <div class="w-3 h-3 border-2 border-slate-600 border-t-blue-400 rounded-full animate-spin" />
                  </Show>
                </div>
                <Show when={pipelinePaths().length > 0}>
                  <div class="mt-1.5 flex flex-wrap gap-1">
                    {pipelinePaths().map((p) => (
                      <span class="inline-flex items-center gap-1 px-1.5 py-0.5 rounded bg-green-500/10 border border-green-500/20 text-[9px] font-mono text-green-400">
                        <svg class="w-2.5 h-2.5" fill="none" viewBox="0 0 24 24" stroke-width="1.5" stroke="currentColor">
                          <path stroke-linecap="round" stroke-linejoin="round" d="M2.25 12.75V12A2.25 2.25 0 014.5 9.75h15A2.25 2.25 0 0121.75 12v.75m-8.69-6.44l-2.12-2.12a1.5 1.5 0 00-1.061-.44H4.5A2.25 2.25 0 002.25 6v12a2.25 2.25 0 002.25 2.25h15A2.25 2.25 0 0021.75 18V9a2.25 2.25 0 00-2.25-2.25h-5.379a1.5 1.5 0 01-1.06-.44z" />
                        </svg>
                        {p}
                      </span>
                    ))}
                  </div>
                </Show>
              </div>
              <FileTree
                files={files()}
                onSelectFile={loadFileContent}
                selectedFile={selectedFile()}
              />
            </div>
          </div>

          {/* Editor */}
          <div class="ide-editor">
            <Show
              when={selectedFile()}
              fallback={
                <div class="flex flex-col items-center justify-center h-full text-center px-8">
                  <svg class="w-10 h-10 text-slate-700 mb-3" fill="none" viewBox="0 0 24 24" stroke-width="1" stroke="currentColor">
                    <path stroke-linecap="round" stroke-linejoin="round" d="M19.5 14.25v-2.625a3.375 3.375 0 00-3.375-3.375h-1.5A1.125 1.125 0 0113.5 7.125v-1.5a3.375 3.375 0 00-3.375-3.375H8.25m0 12.75h7.5m-7.5 3H12M10.5 2.25H5.625c-.621 0-1.125.504-1.125 1.125v17.25c0 .621.504 1.125 1.125 1.125h12.75c.621 0 1.125-.504 1.125-1.125V11.25a9 9 0 00-9-9z" />
                  </svg>
                  <p class="text-xs text-slate-500 mb-1">Select a file from the tree to begin editing</p>
                  <p class="text-[10px] text-slate-600">
                    Branch: <span class="font-mono text-slate-400">{activeBranch()}</span>
                  </p>
                  <Show when={!isDraftBranch()}>
                    <p class="text-[10px] text-slate-600 mt-3">
                      Files are <span class="text-yellow-400">read-only</span> on this branch.
                      <br />
                      Click <span class="text-blue-400">Edit</span> to create a draft.
                    </p>
                  </Show>
                </div>
              }
            >
              <MonacoEditor
                content={fileContent()}
                language="rust"
                readOnly={!isDraftBranch()}
                onChange={setFileContent}
                filePath={selectedFile()}
              />
            </Show>
          </div>

          {/* Terminal */}
          <div class="ide-terminal">
            <Terminal logs={logs()} />
          </div>
        </div>

        {/* Config Modal */}
        <Show when={isConfigModalOpen()}>
          <div
            class="fixed inset-0 bg-black/50 backdrop-blur-sm z-50 flex items-center justify-center p-4"
            onClick={(e) => {
              if (e.target === e.currentTarget) closeConfigModal();
            }}
          >
            <div class="bg-slate-900 border border-slate-700 rounded-lg shadow-2xl w-full max-w-2xl max-h-[80vh] overflow-hidden flex flex-col">
              {/* Modal Header */}
              <div class="flex items-center justify-between px-4 py-3 border-b border-slate-800 bg-slate-800/50">
                <div class="flex items-center gap-2">
                  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="text-slate-400">
                    <path d="M12.22 2h-.44a2 2 0 00-2 2v.18a2 2 0 01-1 1.73l-.43.25a2 2 0 01-2 0l-.15-.08a2 2 0 00-2.73.73l-.22.38a2 2 0 00.73 2.73l.15.1a2 2 0 011 1.72v.51a2 2 0 01-1 1.74l-.15.09a2 2 0 00-.73 2.73l.22.38a2 2 0 002.73.73l.15-.08a2 2 0 012 0l.43.25a2 2 0 011 1.73V20a2 2 0 002 2h.44a2 2 0 002-2v-.18a2 2 0 011-1.73l.43-.25a2 2 0 012 0l.15.08a2 2 0 002.73-.73l.22-.39a2 2 0 00-.73-2.73l-.15-.08a2 2 0 01-1-1.74v-.5a2 2 0 011-1.74l.15-.09a2 2 0 00.73-2.73l-.22-.38a2 2 0 00-2.73-.73l-.15.08a2 2 0 01-2 0l-.43-.25a2 2 0 01-1-1.73V4a2 2 0 00-2-2z" />
                    <circle cx="12" cy="12" r="3" />
                  </svg>
                  <span class="text-sm font-semibold text-slate-200">Pipeline Configuration</span>
                  <Show when={selectedPipelineId()}>
                    <span class="text-xs text-slate-500 font-mono">{selectedPipelineId()}</span>
                  </Show>
                </div>
                <button
                  onClick={closeConfigModal}
                  class="text-slate-400 hover:text-slate-200 transition-colors p-1 rounded hover:bg-slate-700/50"
                >
                  <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <line x1="18" y1="6" x2="6" y2="18" />
                    <line x1="6" y1="6" x2="18" y2="18" />
                  </svg>
                </button>
              </div>

              {/* Modal Body */}
              <div class="flex-1 overflow-y-auto p-4">
                <ConfigPanel
                  pipelineService={pipelineService}
                  pipelineId={selectedPipelineId()}
                  environment={props.environment}
                  onLog={addLog}
                />
              </div>
            </div>
          </div>
        </Show>
      </Show>
    </>
  );
};

export default IDEPage;
