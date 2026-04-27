import { createSignal, Show, For } from 'solid-js';
import { GitHubService } from '../services/github';

// GitHub App Client ID — set via environment variable at build time
const GITHUB_CLIENT_ID = import.meta.env.VITE_GITHUB_CLIENT_ID || '';
const GITHUB_APP_SLUG = import.meta.env.VITE_GITHUB_APP_SLUG || 'clotho-ide';

interface GitHubConnectProps {
  onConnect: (token: string, username: string) => void;
}

export default function GitHubConnect(props: GitHubConnectProps) {
  const [step, setStep] = createSignal<'auth' | 'token' | 'loading'>('auth');
  const [tokenInput, setTokenInput] = createSignal('');
  const [error, setError] = createSignal<string | null>(null);

  // GitHub App OAuth flow
  function startOAuth() {
    if (!GITHUB_CLIENT_ID) {
      // Fallback: manual token entry if no GitHub App is configured
      setStep('token');
      return;
    }

    const redirectUri = `${window.location.origin}/auth/callback`;
    const state = crypto.randomUUID();
    sessionStorage.setItem('oauth_state', state);

    const url = `https://github.com/login/oauth/authorize?client_id=${GITHUB_CLIENT_ID}&redirect_uri=${encodeURIComponent(redirectUri)}&scope=repo&state=${state}`;
    window.location.href = url;
  }

  // GitHub App installation flow
  function installApp() {
    const url = `https://github.com/apps/${GITHUB_APP_SLUG}/installations/new`;
    window.open(url, '_blank');
  }

  // Manual token submission
  async function submitToken() {
    const token = tokenInput().trim();
    if (!token) {
      setError('Token is required');
      return;
    }

    setStep('loading');
    setError(null);

    try {
      // Validate the token by fetching the user
      const res = await fetch('https://api.github.com/user', {
        headers: {
          'Authorization': `Bearer ${token}`,
          'Accept': 'application/vnd.github+json',
        },
      });

      if (!res.ok) {
        throw new Error(`Invalid token (HTTP ${res.status})`);
      }

      const user = await res.json();
      props.onConnect(token, user.login);
    } catch (err: any) {
      setError(err.message || 'Failed to validate token');
      setStep('token');
    }
  }

  return (
    <div class="github-connect">
      <div class="connect-card">
        <div class="connect-icon">
          <svg width="48" height="48" viewBox="0 0 24 24" fill="currentColor">
            <path d="M12 0C5.37 0 0 5.37 0 12c0 5.31 3.435 9.795 8.205 11.385.6.105.825-.255.825-.57 0-.285-.015-1.23-.015-2.235-3.015.555-3.795-.735-4.035-1.41-.135-.345-.72-1.41-1.23-1.695-.42-.225-1.02-.78-.015-.795.945-.015 1.62.87 1.845 1.23 1.08 1.815 2.805 1.305 3.495.99.105-.78.42-1.305.765-1.605-2.67-.3-5.46-1.335-5.46-5.925 0-1.305.465-2.385 1.23-3.225-.12-.3-.54-1.53.12-3.18 0 0 1.005-.315 3.3 1.23.96-.27 1.98-.405 3-.405s2.04.135 3 .405c2.295-1.56 3.3-1.23 3.3-1.23.66 1.65.24 2.88.12 3.18.765.84 1.23 1.905 1.23 3.225 0 4.605-2.805 5.625-5.475 5.925.435.375.81 1.095.81 2.22 0 1.605-.015 2.895-.015 3.3 0 .315.225.69.825.57A12.02 12.02 0 0024 12c0-6.63-5.37-12-12-12z"/>
          </svg>
        </div>

        <h2>Connect to GitHub</h2>
        <p class="connect-subtitle">
          Link your repository to edit pipeline code directly in the browser
        </p>

        <Show when={error()}>
          <div class="connect-error">{error()}</div>
        </Show>

        <Show when={step() === 'auth'}>
          <div class="connect-actions">
            <Show when={GITHUB_CLIENT_ID}>
              <button class="btn-primary" onClick={startOAuth}>
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                  <path d="M15 3h4a2 2 0 012 2v14a2 2 0 01-2 2h-4M10 17l5-5-5-5M13.8 12H3"/>
                </svg>
                Sign in with GitHub
              </button>
            </Show>

            <button class="btn-secondary" onClick={() => setStep('token')}>
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <rect x="3" y="11" width="18" height="11" rx="2" ry="2"/>
                <path d="M7 11V7a5 5 0 0110 0v4"/>
              </svg>
              Use Personal Access Token
            </button>
          </div>
        </Show>

        <Show when={step() === 'token'}>
          <div class="token-form">
            <label>GitHub Personal Access Token</label>
            <p class="token-hint">
              Generate a <strong>classic</strong> token at{' '}
              <a href="https://github.com/settings/tokens/new?scopes=repo&description=Clotho+IDE" target="_blank" rel="noopener">
                github.com/settings/tokens
              </a>{' '}
              with the following scopes:
            </p>
            <ul style="margin: 6px 0 10px 16px; font-size: 11px; color: #858585; line-height: 1.8; list-style: disc;">
              <li><code>repo</code> — Full access to read/write repository contents and create branches</li>
              <li><code>read:user</code> — Read your GitHub profile (used to identify draft branches)</li>
            </ul>
            <p class="token-hint" style="margin-bottom: 10px;">
              For <strong>fine-grained</strong> tokens, grant <em>Contents</em> (read &amp; write) and <em>Pull requests</em> (read &amp; write) on the target repository.
            </p>
            <input
              type="password"
              placeholder="ghp_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
              value={tokenInput()}
              onInput={(e) => setTokenInput(e.currentTarget.value)}
              onKeyDown={(e) => e.key === 'Enter' && submitToken()}
              autofocus
            />
            <div class="token-actions">
              <button class="btn-ghost" onClick={() => { setStep('auth'); setError(null); }}>
                Back
              </button>
              <button class="btn-primary" onClick={submitToken}>
                Connect
              </button>
            </div>
          </div>
        </Show>

        <Show when={step() === 'loading'}>
          <div class="connect-loading">
            <div class="spinner" />
            <span>Validating credentials...</span>
          </div>
        </Show>
      </div>
    </div>
  );
}
