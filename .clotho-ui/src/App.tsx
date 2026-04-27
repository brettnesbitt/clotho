import { Router, Route, useNavigate, useParams, useLocation } from "@solidjs/router";
import { createSignal, createEffect, Show } from "solid-js";
import Dashboard from "./components/Dashboard";
import BuildsPage from "./components/BuildsPage";
import type { Environment } from "./components/EnvironmentSwitcher";

// Deep-linked dashboard that reads route params
const DeepDashboard = () => {
  const params = useParams();
  const navigate = useNavigate();
  const location = useLocation();
  const [activePage, setActivePage] = createSignal(params.page || "overview");
  const [selectedPipeline, setSelectedPipeline] = createSignal<string | null>(params.pipeline || null);
  const [environment, setEnvironment] = createSignal<Environment>((params.environment as Environment) || "production");

  // Sync URL params → state
  createEffect(() => {
    const p = params.page || "overview";
    setActivePage(p);
    if (params.pipeline) {
      setSelectedPipeline(params.pipeline);
    }
    if (params.environment) {
      setEnvironment(params.environment as Environment);
    }
  });

  // Sync state → URL (deep linking)
  const syncUrl = (page: string, pipeline?: string | null, env?: string) => {
    const parts = ["/dashboard", env || environment()];
    if (page !== "overview") parts.push(page);
    if (pipeline) parts.push(pipeline);
    navigate(parts.join("/"), { replace: true });
  };

  const handleNavigate = (page: string) => {
    setActivePage(page);
    setSelectedPipeline(null);
    syncUrl(page, null, environment());
  };

  const handleSelectPipeline = (id: string) => {
    setSelectedPipeline(id);
    setActivePage("pipelines");
    syncUrl("pipelines", id, environment());
  };

  const handleBackFromDetail = () => {
    setSelectedPipeline(null);
    setActivePage("overview");
    syncUrl("overview", null, environment());
  };

  const handleSwitchEnvironment = (env: Environment) => {
    setEnvironment(env);
    syncUrl(activePage(), selectedPipeline(), env);
  };

  return (
    <Dashboard
      activePage={activePage()}
      selectedPipeline={selectedPipeline()}
      environment={environment()}
      onNavigate={handleNavigate}
      onSelectPipeline={handleSelectPipeline}
      onBackFromDetail={handleBackFromDetail}
      onSwitchEnvironment={handleSwitchEnvironment}
    />
  );
};

// Wrapper for builds page to get environment from URL
const BuildsPageWrapper = () => {
  const params = useParams();
  const navigate = useNavigate();
  const [environment, setEnvironment] = createSignal<Environment>((params.environment as Environment) || "production");

  const handleSwitchEnvironment = (env: Environment) => {
    setEnvironment(env);
    navigate(`/builds/${env}`, { replace: true });
  };

  return <BuildsPage environment={environment()} onSwitchEnvironment={handleSwitchEnvironment} />;
};

export default function App() {
  return (
    <Router>
      <Route path="/" component={() => {
        const navigate = useNavigate();
        navigate("/dashboard/production", { replace: true });
        return null;
      }} />
      <Route path="/dashboard" component={DeepDashboard} />
      <Route path="/dashboard/:environment" component={DeepDashboard} />
      <Route path="/dashboard/:environment/:page" component={DeepDashboard} />
      <Route path="/dashboard/:environment/:page/:pipeline" component={DeepDashboard} />
      <Route path="/builds" component={BuildsPageWrapper} />
      <Route path="/builds/:environment" component={BuildsPageWrapper} />
    </Router>
  );
}