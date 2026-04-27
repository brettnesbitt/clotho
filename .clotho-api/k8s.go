package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// K8sClient provides lightweight access to the Kubernetes API using in-cluster config.
// No client-go dependency required.
type K8sClient struct {
	baseURL string
	token   string
	client  *http.Client
}

// NewK8sClient creates a client from in-cluster service account credentials.
func NewK8sClient() (*K8sClient, error) {
	tokenBytes, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
	if err != nil {
		return nil, fmt.Errorf("reading SA token: %w", err)
	}

	caBytes, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
	if err != nil {
		return nil, fmt.Errorf("reading CA cert: %w", err)
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caBytes)

	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs: caCertPool,
		},
	}

	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	port := os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, fmt.Errorf("not running in-cluster (KUBERNETES_SERVICE_HOST/PORT not set)")
	}

	return &K8sClient{
		baseURL: fmt.Sprintf("https://%s:%s", host, port),
		token:   string(tokenBytes),
		client: &http.Client{
			Transport: transport,
			Timeout:   10 * time.Second,
		},
	}, nil
}

// get performs an authenticated GET request to the K8s API.
func (k *K8sClient) get(path string) ([]byte, error) {
	req, err := http.NewRequest("GET", k.baseURL+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+k.token)
	req.Header.Set("Accept", "application/json")

	resp, err := k.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("K8s API %d: %s", resp.StatusCode, string(body[:min(len(body), 200)]))
	}

	return body, nil
}

// delete performs an authenticated DELETE request to the K8s API.
func (k *K8sClient) delete(path string) error {
	req, err := http.NewRequest("DELETE", k.baseURL+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+k.token)
	req.Header.Set("Accept", "application/json")

	resp, err := k.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 && resp.StatusCode != 202 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("K8s API %d: %s", resp.StatusCode, string(body[:min(len(body), 200)]))
	}

	return nil
}

// --- Pipeline CR types ---

type PipelineList struct {
	Items []PipelineCR `json:"items"`
}

type PipelineCR struct {
	Metadata K8sMeta        `json:"metadata"`
	Spec     PipelineSpec   `json:"spec"`
	Status   PipelineStatus `json:"status"`
}

type K8sMeta struct {
	Name              string            `json:"name"`
	Namespace         string            `json:"namespace"`
	UID               string            `json:"uid"`
	CreationTimestamp string            `json:"creationTimestamp"`
	Labels            map[string]string `json:"labels,omitempty"`
}

// MessageBusSpec for K8s CR messageBus field
type MessageBusSpec struct {
	Type       string            `json:"type,omitempty"`
	ClusterRef string            `json:"clusterRef,omitempty"`
	Config     map[string]string `json:"config,omitempty"`
}

type PipelineStageSpec struct {
	Name       string   `json:"name"`
	Entrypoint string   `json:"entrypoint"`
	Replicas   int      `json:"replicas,omitempty"`
	DependsOn  []string `json:"dependsOn,omitempty"`
}

type BuildSpec struct {
	Builder              string `json:"builder,omitempty"`
	Registry             string `json:"registry,omitempty"`
	CredentialsSecret    string `json:"credentialsSecret,omitempty"`
	ServiceAccountSecret string `json:"serviceAccountSecret,omitempty"`
	Timeout              string `json:"timeout,omitempty"`
}

type PipelineSpec struct {
	Mode                 string              `json:"mode,omitempty"`
	GitRepository        string              `json:"gitRepository"`
	Reference            string              `json:"reference"`
	Path                 string              `json:"path"`
	Image                string              `json:"image"`
	Replicas             int                 `json:"replicas"`
	GitCredentialsSecret string              `json:"gitCredentialsSecret,omitempty"`
	Build                *BuildSpec          `json:"build,omitempty"`
	Config               []ConfigVar         `json:"config,omitempty"`
	Resources            *ResourceSpec       `json:"resources,omitempty"`
	Policy               *PolicySpec         `json:"policy,omitempty"`
	Schedule             *ScheduleSpec       `json:"schedule,omitempty"`
	Stages               []PipelineStageSpec `json:"stages,omitempty"`
	MessageBus           *MessageBusSpec     `json:"messageBus,omitempty"`
}

// ConfigVar represents an environment variable for a pipeline.
type ConfigVar struct {
	Name      string           `json:"name"`
	Value     string           `json:"value,omitempty"`
	ValueFrom *ConfigVarSource `json:"valueFrom,omitempty"`
}

// ConfigVarSource allows selecting a value from a Secret.
type ConfigVarSource struct {
	SecretKeyRef *SecretKeyRef `json:"secretKeyRef,omitempty"`
}

type SecretKeyRef struct {
	Name     string `json:"name"`
	Key      string `json:"key"`
	Optional *bool  `json:"optional,omitempty"`
}

type ScheduleSpec struct {
	Mode     string `json:"mode"`
	Interval string `json:"interval,omitempty"`
	Cron     string `json:"cron,omitempty"`
}

type ResourceSpec struct {
	Limits   map[string]string `json:"limits,omitempty"`
	Requests map[string]string `json:"requests,omitempty"`
}

type PolicySpec struct {
	MaxRetries     int `json:"maxRetries,omitempty"`
	TimeoutSeconds int `json:"timeoutSeconds,omitempty"`
}

type PipelineStatus struct {
	Phase              string `json:"phase"`
	Message            string `json:"message,omitempty"`
	ObservedGeneration int64  `json:"observedGeneration"`
	LastInvocation     string `json:"lastInvocation,omitempty"`
}

// --- Pod types ---

type PodList struct {
	Items []PodItem `json:"items"`
}

type PodItem struct {
	Metadata K8sMeta   `json:"metadata"`
	Spec     PodSpec   `json:"spec"`
	Status   PodStatus `json:"status"`
}

type PodSpec struct {
	NodeName string `json:"nodeName"`
}

type PodStatus struct {
	Phase             string            `json:"phase"`
	PodIP             string            `json:"podIP"`
	StartTime         string            `json:"startTime"`
	ContainerStatuses []ContainerStatus `json:"containerStatuses"`
}

type ContainerStatus struct {
	Name         string         `json:"name"`
	Ready        bool           `json:"ready"`
	RestartCount int            `json:"restartCount"`
	State        ContainerState `json:"state"`
	Image        string         `json:"image"`
}

type ContainerState struct {
	Running    *StateRunning    `json:"running,omitempty"`
	Waiting    *StateWaiting    `json:"waiting,omitempty"`
	Terminated *StateTerminated `json:"terminated,omitempty"`
}

type StateRunning struct {
	StartedAt string `json:"startedAt"`
}

type StateWaiting struct {
	Reason  string `json:"reason"`
	Message string `json:"message"`
}

type StateTerminated struct {
	ExitCode   int    `json:"exitCode"`
	Reason     string `json:"reason"`
	StartedAt  string `json:"startedAt"`
	FinishedAt string `json:"finishedAt"`
}

// --- Job types ---

type JobList struct {
	Items []JobItem `json:"items"`
}

type JobItem struct {
	Metadata K8sMeta   `json:"metadata"`
	Status   JobStatus `json:"status"`
}

type JobStatus struct {
	Succeeded      int            `json:"succeeded"`
	Failed         int            `json:"failed"`
	Active         int            `json:"active"`
	StartTime      string         `json:"startTime"`
	CompletionTime string         `json:"completionTime"`
	Conditions     []JobCondition `json:"conditions"`
}

type JobCondition struct {
	Type   string `json:"type"`
	Status string `json:"status"`
	Reason string `json:"reason"`
}

// --- Metrics API types ---

type PodMetricsList struct {
	Items []PodMetrics `json:"items"`
}

type PodMetrics struct {
	Metadata   K8sMeta            `json:"metadata"`
	Containers []ContainerMetrics `json:"containers"`
}

type ContainerMetrics struct {
	Name  string            `json:"name"`
	Usage map[string]string `json:"usage"`
}

// --- Query methods ---

// GetPipelines returns all Pipeline CRs across all namespaces.
func (k *K8sClient) GetPipelines(namespace string) ([]PipelineCR, error) {
	path := fmt.Sprintf("/apis/core.clotho.run/v1alpha1/namespaces/%s/pipelines", namespace)
	data, err := k.get(path)
	if err != nil {
		return nil, err
	}

	var list PipelineList
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, fmt.Errorf("parsing pipelines: %w", err)
	}
	return list.Items, nil
}

// GetPipeline returns a single Pipeline CR.
func (k *K8sClient) GetPipeline(namespace, name string) (*PipelineCR, error) {
	path := fmt.Sprintf("/apis/core.clotho.run/v1alpha1/namespaces/%s/pipelines/%s", namespace, name)
	data, err := k.get(path)
	if err != nil {
		return nil, err
	}

	var p PipelineCR
	if err := json.Unmarshal(data, &p); err != nil {
		return nil, fmt.Errorf("parsing pipeline: %w", err)
	}
	return &p, nil
}

// GetPodsForPipeline returns pods matching a pipeline's deployment.
// Tries SpinKube label first (WASM), then clotho.run/pipeline (native deployments).
func (k *K8sClient) GetPodsForPipeline(namespace, pipelineName string) ([]PodItem, error) {
	// Try SpinKube label first (WASM runtime)
	path := fmt.Sprintf("/api/v1/namespaces/%s/pods?labelSelector=core.spinkube.dev/app-name=%s", namespace, pipelineName)
	data, err := k.get(path)
	if err != nil {
		return nil, err
	}

	var list PodList
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, fmt.Errorf("parsing pods: %w", err)
	}

	// Fall back to clotho.run/pipeline label (native runtime)
	if len(list.Items) == 0 {
		path = fmt.Sprintf("/api/v1/namespaces/%s/pods?labelSelector=clotho.run/pipeline=%s", namespace, pipelineName)
		data, err = k.get(path)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(data, &list); err != nil {
			return nil, fmt.Errorf("parsing pods: %w", err)
		}
	}

	return list.Items, nil
}

// GetBuildsForPipeline returns builder jobs for a pipeline.
func (k *K8sClient) GetBuildsForPipeline(namespace, pipelineName string) ([]JobItem, error) {
	jobName := "builder-" + pipelineName
	path := fmt.Sprintf("/apis/batch/v1/namespaces/%s/jobs?fieldSelector=metadata.name=%s", namespace, jobName)
	data, err := k.get(path)
	if err != nil {
		return nil, err
	}

	var list JobList
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, fmt.Errorf("parsing jobs: %w", err)
	}
	return list.Items, nil
}

// RestartPipelinePods performs a rolling restart of all workloads for a pipeline.
// For DAG pipelines it discovers stage names from the CRD and restarts each stage workload.
// Native deployments are restarted via a rollout-restart annotation patch.
// SpinApps fall back to pod deletion (no Deployment to patch).
func (k *K8sClient) RestartPipelinePods(namespace, pipelineName string) ([]string, error) {
	// Collect all workload names: parent + any DAG stages
	workloadNames := []string{pipelineName}
	if cr, err := k.GetPipeline(namespace, pipelineName); err == nil {
		for _, stage := range cr.Spec.Stages {
			workloadNames = append(workloadNames, fmt.Sprintf("%s-%s", pipelineName, stage.Name))
		}
	}

	var restarted []string
	for _, name := range workloadNames {
		// Attempt rollout restart on the Deployment (native runtime)
		if err := k.rolloutRestartDeployment(namespace, name); err == nil {
			restarted = append(restarted, name)
			continue
		}
		// Fall back: delete pods (SpinApp / WASM runtime)
		pods, err := k.GetPodsForPipeline(namespace, name)
		if err != nil {
			continue
		}
		for _, pod := range pods {
			path := fmt.Sprintf("/api/v1/namespaces/%s/pods/%s", namespace, pod.Metadata.Name)
			_ = k.delete(path) // best-effort; ignore individual pod errors
		}
		if len(pods) > 0 {
			restarted = append(restarted, name)
		}
	}

	if len(restarted) == 0 {
		return nil, fmt.Errorf("no workloads found for pipeline %s", pipelineName)
	}
	return restarted, nil
}

// rolloutRestartDeployment patches a Deployment's pod template annotation to trigger
// a rolling restart — equivalent to `kubectl rollout restart deployment/<name>`.
func (k *K8sClient) rolloutRestartDeployment(namespace, name string) error {
	path := fmt.Sprintf("/apis/apps/v1/namespaces/%s/deployments/%s", namespace, name)
	restartedAt := time.Now().UTC().Format(time.RFC3339)
	patch := fmt.Sprintf(`{"spec":{"template":{"metadata":{"annotations":{"kubectl.kubernetes.io/restartedAt":%q}}}}}`, restartedAt)
	return k.patch(path, patch)
}

// patch performs an authenticated PATCH (strategic merge) request to the K8s API.
func (k *K8sClient) patch(path, body string) error {
	req, err := http.NewRequest("PATCH", k.baseURL+path, strings.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+k.token)
	req.Header.Set("Content-Type", "application/strategic-merge-patch+json")

	resp, err := k.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 && resp.StatusCode != 201 && resp.StatusCode != 202 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("K8s API %d: %s", resp.StatusCode, string(body[:min(len(body), 200)]))
	}
	return nil
}

// GetPodMetrics returns CPU/memory usage for pods matching a pipeline via the metrics API.
// Tries SpinKube label first (WASM), then clotho.run/pipeline (native deployments).
func (k *K8sClient) GetPodMetrics(namespace, pipelineName string) ([]PodMetrics, error) {
	path := fmt.Sprintf("/apis/metrics.k8s.io/v1beta1/namespaces/%s/pods?labelSelector=core.spinkube.dev/app-name=%s", namespace, pipelineName)
	data, err := k.get(path)
	if err != nil {
		return nil, err
	}

	var list PodMetricsList
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, fmt.Errorf("parsing pod metrics: %w", err)
	}

	// Fall back to clotho.run/pipeline label (native runtime)
	if len(list.Items) == 0 {
		path = fmt.Sprintf("/apis/metrics.k8s.io/v1beta1/namespaces/%s/pods?labelSelector=clotho.run/pipeline=%s", namespace, pipelineName)
		data, err = k.get(path)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(data, &list); err != nil {
			return nil, fmt.Errorf("parsing pod metrics: %w", err)
		}
	}

	return list.Items, nil
}

// GetPodLogs returns the log output for a specific pod.
func (k *K8sClient) GetPodLogs(namespace, podName string, tailLines int) (string, error) {
	path := fmt.Sprintf("/api/v1/namespaces/%s/pods/%s/log?tailLines=%d&timestamps=true", namespace, podName, tailLines)
	data, err := k.get(path)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// PatchPipelineCR patches a Pipeline CR (e.g. to set replicas to 0 for pause).
func (k *K8sClient) PatchPipelineCR(namespace, name string, patchJSON []byte) error {
	path := fmt.Sprintf("/apis/core.clotho.run/v1alpha1/namespaces/%s/pipelines/%s", namespace, name)
	return k.patchMerge(path, patchJSON)
}

// DeletePipelineCR deletes a Pipeline CR.
func (k *K8sClient) DeletePipelineCR(namespace, name string) error {
	path := fmt.Sprintf("/apis/core.clotho.run/v1alpha1/namespaces/%s/pipelines/%s", namespace, name)
	return k.delete(path)
}

// patchMerge performs a strategic merge patch request.
func (k *K8sClient) patchMerge(path string, patchJSON []byte) error {
	req, err := http.NewRequest("PATCH", k.baseURL+path, bytes.NewReader(patchJSON))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+k.token)
	req.Header.Set("Content-Type", "application/merge-patch+json")
	req.Header.Set("Accept", "application/json")

	resp, err := k.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("K8s API %d: %s", resp.StatusCode, string(body[:min(len(body), 200)]))
	}

	return nil
}

// parseK8sQuantity converts Kubernetes resource quantity strings to base units.
// CPU: "5m" -> 5 (millicores), "1" -> 1000 (millicores)
// Memory: "6Mi" -> 6291456 (bytes), "100Ki" -> 102400 (bytes)
func parseK8sQuantity(s string, isCPU bool) int64 {
	if s == "" {
		return 0
	}

	// CPU: nanocores ("123456789n") or millicores ("5m") or cores ("1")
	if isCPU {
		if s[len(s)-1] == 'n' {
			val := parseInt64(s[:len(s)-1])
			return val / 1000000 // nanocores to millicores
		}
		if s[len(s)-1] == 'm' {
			return parseInt64(s[:len(s)-1])
		}
		return parseInt64(s) * 1000
	}

	// Memory
	if len(s) >= 2 {
		suffix := s[len(s)-2:]
		switch suffix {
		case "Ki":
			return parseInt64(s[:len(s)-2]) * 1024
		case "Mi":
			return parseInt64(s[:len(s)-2]) * 1024 * 1024
		case "Gi":
			return parseInt64(s[:len(s)-2]) * 1024 * 1024 * 1024
		}
	}
	return parseInt64(s)
}

func parseInt64(s string) int64 {
	var v int64
	fmt.Sscanf(s, "%d", &v)
	return v
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// --- Test Build methods ---

// CreateTestBuildJob creates an ephemeral builder Job for testing a draft branch.
// Returns the job name.
func (k *K8sClient) CreateTestBuildJob(namespace, jobName, gitRepo, reference, path, targetImage, gitSecretName string) error {
	if gitSecretName == "" {
		gitSecretName = "clotho-git-credentials"
	}

	job := map[string]interface{}{
		"apiVersion": "batch/v1",
		"kind":       "Job",
		"metadata": map[string]interface{}{
			"name":      jobName,
			"namespace": namespace,
			"labels": map[string]string{
				"clotho.run/test-build": "true",
			},
			"annotations": map[string]string{
				"clotho.run/target-image": targetImage,
			},
		},
		"spec": map[string]interface{}{
			"ttlSecondsAfterFinished": 600,
			"activeDeadlineSeconds":   1800,
			"backoffLimit":            0,
			"template": map[string]interface{}{
				"spec": map[string]interface{}{
					"restartPolicy": "Never",
					"containers": []map[string]interface{}{
						{
							"name":  "builder",
							"image": "us-central1-docker.pkg.dev/quotopia-391900/clotho/clotho-builder:latest",
							"args":  []string{gitRepo, reference, targetImage, path},
							"env": []map[string]interface{}{
								{
									"name": "GIT_TOKEN",
									"valueFrom": map[string]interface{}{
										"secretKeyRef": map[string]interface{}{
											"name":     gitSecretName,
											"key":      "token",
											"optional": true,
										},
									},
								},
							},
							"resources": map[string]interface{}{
								"requests": map[string]string{"memory": "256Mi"},
								"limits":   map[string]string{"memory": "2560Mi"},
							},
							"volumeMounts": []map[string]interface{}{
								{"name": "cargo-cache", "mountPath": "/usr/local/cargo/registry"},
								{"name": "build-cache", "mountPath": "/app/target"},
								{"name": "registry-ca", "mountPath": "/tmp/registry-ca", "readOnly": true},
							},
						},
					},
					"volumes": []map[string]interface{}{
						{"name": "cargo-cache", "persistentVolumeClaim": map[string]string{"claimName": "clotho-builder-cache-pvc"}},
						{"name": "build-cache", "persistentVolumeClaim": map[string]string{"claimName": "clotho-project-cache-pvc"}},
						{"name": "registry-ca", "secret": map[string]string{"secretName": "clotho-registry-tls"}},
					},
				},
			},
		},
	}

	body, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshaling job: %w", err)
	}

	return k.create(fmt.Sprintf("/apis/batch/v1/namespaces/%s/jobs", namespace), body)
}

// GetJob returns a single Job by name.
func (k *K8sClient) GetJob(namespace, name string) (*JobItem, error) {
	path := fmt.Sprintf("/apis/batch/v1/namespaces/%s/jobs/%s", namespace, name)
	data, err := k.get(path)
	if err != nil {
		return nil, err
	}

	var job JobItem
	if err := json.Unmarshal(data, &job); err != nil {
		return nil, fmt.Errorf("parsing job: %w", err)
	}
	return &job, nil
}

// GetPodsForJob returns pods owned by a specific Job.
func (k *K8sClient) GetPodsForJob(namespace, jobName string) ([]PodItem, error) {
	path := fmt.Sprintf("/api/v1/namespaces/%s/pods?labelSelector=job-name=%s", namespace, jobName)
	data, err := k.get(path)
	if err != nil {
		return nil, err
	}

	var list PodList
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, fmt.Errorf("parsing pods: %w", err)
	}
	return list.Items, nil
}

// StreamPodLogs returns a ReadCloser for streaming pod logs.
func (k *K8sClient) StreamPodLogs(namespace, podName string, follow bool) (io.ReadCloser, error) {
	followStr := "false"
	if follow {
		followStr = "true"
	}
	path := fmt.Sprintf("/api/v1/namespaces/%s/pods/%s/log?follow=%s&timestamps=true", namespace, podName, followStr)

	req, err := http.NewRequest("GET", k.baseURL+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+k.token)

	// Use a separate client with no timeout for streaming
	streamClient := &http.Client{
		Transport: k.client.Transport,
		Timeout:   0,
	}

	resp, err := streamClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("K8s API %d: %s", resp.StatusCode, string(body[:min(len(body), 200)]))
	}

	return resp.Body, nil
}

// DeleteJob deletes a Job and its pods.
func (k *K8sClient) DeleteJob(namespace, name string) error {
	// propagationPolicy=Background ensures pods are cleaned up
	path := fmt.Sprintf("/apis/batch/v1/namespaces/%s/jobs/%s?propagationPolicy=Background", namespace, name)
	return k.delete(path)
}

// --- Secret types (names + keys only, never values) ---

type SecretList struct {
	Items []SecretItem `json:"items"`
}

type SecretItem struct {
	Metadata K8sMeta           `json:"metadata"`
	Data     map[string]string `json:"data,omitempty"`
	Type     string            `json:"type"`
}

// ListSecrets returns secret names (and types) in a namespace.
// Excludes service-account-token and helm secrets to reduce noise.
func (k *K8sClient) ListSecrets(namespace string) ([]SecretItem, error) {
	path := fmt.Sprintf("/api/v1/namespaces/%s/secrets", namespace)
	data, err := k.get(path)
	if err != nil {
		return nil, err
	}

	var list SecretList
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, fmt.Errorf("parsing secrets: %w", err)
	}

	// Filter out system secrets
	filtered := make([]SecretItem, 0, len(list.Items))
	for _, s := range list.Items {
		if s.Type == "kubernetes.io/service-account-token" {
			continue
		}
		if s.Type == "helm.sh/release.v1" {
			continue
		}
		// Strip data values — only keep the keys
		s.Data = nil
		filtered = append(filtered, s)
	}
	return filtered, nil
}

// GetSecretKeys returns the key names (not values) for a specific secret.
func (k *K8sClient) GetSecretKeys(namespace, name string) ([]string, error) {
	path := fmt.Sprintf("/api/v1/namespaces/%s/secrets/%s", namespace, name)
	data, err := k.get(path)
	if err != nil {
		return nil, err
	}

	var secret SecretItem
	if err := json.Unmarshal(data, &secret); err != nil {
		return nil, fmt.Errorf("parsing secret: %w", err)
	}

	keys := make([]string, 0, len(secret.Data))
	for k := range secret.Data {
		keys = append(keys, k)
	}
	return keys, nil
}

// PodDetail is the full pod info for the /pods endpoint
type PodDetail struct {
	Name           string `json:"name"`
	UID            string `json:"uid"`
	Node           string `json:"node"`
	Phase          string `json:"phase"`
	PodIP          string `json:"pod_ip"`
	Ready          bool   `json:"ready"`
	Restarts       int    `json:"restarts"`
	StartTime      string `json:"start_time,omitempty"`
	ContainerState string `json:"container_state"`
	StateDetail    string `json:"state_detail,omitempty"`
	Image          string `json:"image"`
}

// BuildDetail is the full build job info for the /builds endpoint
type BuildDetail struct {
	Name           string `json:"name"`
	Status         string `json:"status"`
	StartTime      string `json:"start_time,omitempty"`
	CompletionTime string `json:"completion_time,omitempty"`
	DurationSec    int64  `json:"duration_sec,omitempty"`
}

// getPodDetails returns detailed pod info for a pipeline.
func getPodDetails(k8s *K8sClient, namespace, pipelineName string) ([]PodDetail, error) {
	pods, err := k8s.GetPodsForPipeline(namespace, pipelineName)
	if err != nil {
		return nil, err
	}
	details := make([]PodDetail, 0, len(pods))
	for _, p := range pods {
		state := "unknown"
		detail := ""
		ready := false
		restarts := 0
		image := ""
		if len(p.Status.ContainerStatuses) > 0 {
			cs := p.Status.ContainerStatuses[0]
			ready = cs.Ready
			restarts = cs.RestartCount
			image = cs.Image
			if cs.State.Running != nil {
				state = "running"
			} else if cs.State.Waiting != nil {
				state = "waiting"
				detail = cs.State.Waiting.Reason
			} else if cs.State.Terminated != nil {
				state = "terminated"
				detail = cs.State.Terminated.Reason
			}
		}
		details = append(details, PodDetail{
			Name:           p.Metadata.Name,
			UID:            p.Metadata.UID,
			Node:           p.Spec.NodeName,
			Phase:          p.Status.Phase,
			PodIP:          p.Status.PodIP,
			Ready:          ready,
			Restarts:       restarts,
			StartTime:      p.Status.StartTime,
			ContainerState: state,
			StateDetail:    detail,
			Image:          image,
		})
	}
	return details, nil
}

// getBuildDetails returns detailed build job info for a pipeline.
func getBuildDetails(k8s *K8sClient, namespace, pipelineName string) ([]BuildDetail, error) {
	jobs, err := k8s.GetBuildsForPipeline(namespace, pipelineName)
	if err != nil {
		return nil, err
	}
	details := make([]BuildDetail, 0, len(jobs))
	for _, j := range jobs {
		status := "pending"
		if j.Status.Succeeded > 0 {
			status = "completed"
		} else if j.Status.Failed > 0 {
			status = "failed"
		} else if j.Status.Active > 0 {
			status = "running"
		}
		var durationSec int64
		if j.Status.StartTime != "" && j.Status.CompletionTime != "" {
			if start, err := time.Parse(time.RFC3339, j.Status.StartTime); err == nil {
				if end, err := time.Parse(time.RFC3339, j.Status.CompletionTime); err == nil {
					durationSec = int64(end.Sub(start).Seconds())
				}
			}
		}
		details = append(details, BuildDetail{
			Name:           j.Metadata.Name,
			Status:         status,
			StartTime:      j.Status.StartTime,
			CompletionTime: j.Status.CompletionTime,
			DurationSec:    durationSec,
		})
	}
	return details, nil
}

// restartPipelinePods triggers a rolling restart of all workloads for a pipeline (parent + DAG stages).
func restartPipelinePods(k8s *K8sClient, namespace, pipelineName string) ([]string, error) {
	return k8s.RestartPipelinePods(namespace, pipelineName)
}

// crToPipeline converts a PipelineCR to the Pipeline struct used by the API.
func crToPipeline(cr PipelineCR) Pipeline {
	mode := cr.Spec.Mode
	if mode == "" {
		mode = "stream"
	}
	status := cr.Status.Phase
	if status == "" {
		status = "PENDING"
	}

	// Convert stages from CR spec
	stages := make([]PipelineStage, 0, len(cr.Spec.Stages))
	for _, s := range cr.Spec.Stages {
		stages = append(stages, PipelineStage{
			Name:       s.Name,
			Entrypoint: s.Entrypoint,
			Replicas:   s.Replicas,
			DependsOn:  s.DependsOn,
		})
	}

	// Extract message bus type
	messageBusType := ""
	if cr.Spec.MessageBus != nil {
		messageBusType = cr.Spec.MessageBus.Type
	}

	// Extract SDK version from image tag: registry/pipeline-id:sdkX.Y.Z-gitref
	sdkVersion := ""
	if img := cr.Spec.Image; img != "" {
		if colonIdx := strings.LastIndex(img, ":"); colonIdx != -1 {
			tag := img[colonIdx+1:]
			if sdkIdx := strings.Index(tag, "sdk"); sdkIdx != -1 {
				end := strings.Index(tag[sdkIdx:], "-")
				if end == -1 {
					sdkVersion = tag[sdkIdx+3:]
				} else {
					sdkVersion = tag[sdkIdx+3 : sdkIdx+end]
				}
			}
		}
	}

	return Pipeline{
		ID:              cr.Metadata.Name,
		Mode:            mode,
		Status:          status,
		ErrorMessage:    cr.Status.Message,
		Phase:           cr.Status.Phase,
		Image:           cr.Spec.Image,
		GitRepository:   cr.Spec.GitRepository,
		GitRef:          cr.Spec.Reference,
		Path:            cr.Spec.Path,
		DesiredReplicas: cr.Spec.Replicas,
		CreatedAt:       cr.Metadata.CreationTimestamp,
		LastInvocation:  cr.Status.LastInvocation,
		Stages:          stages,
		MessageBusType:  messageBusType,
		SdkVersion:      sdkVersion,
		HasBuildConfig:  cr.Spec.Build != nil || cr.Spec.GitRepository != "",
	}
}

// TriggerRebuild annotates the Pipeline CR to signal the operator to re-run the build.
// It sets a `clotho.run/rebuild` annotation with the current timestamp, which the
// operator watches and uses to trigger a new Cloud Build / builder Job.
func (k *K8sClient) TriggerRebuild(namespace, name string) error {
	annotation := fmt.Sprintf(`{"metadata":{"annotations":{"clotho.run/rebuild":%q}}}`, time.Now().UTC().Format(time.RFC3339))
	path := fmt.Sprintf("/apis/core.clotho.run/v1alpha1/namespaces/%s/pipelines/%s", namespace, name)
	return k.patchMerge(path, []byte(annotation))
}

// jobToBuildSummary converts a JobItem to a BuildSummary.
func jobToBuildSummary(job JobItem) *BuildSummary {
	status := "pending"
	if job.Status.Succeeded > 0 {
		status = "completed"
	} else if job.Status.Failed > 0 {
		status = "failed"
	} else if job.Status.Active > 0 {
		status = "running"
	}
	var durationSec int64
	if job.Status.StartTime != "" && job.Status.CompletionTime != "" {
		if start, err := time.Parse(time.RFC3339, job.Status.StartTime); err == nil {
			if end, err := time.Parse(time.RFC3339, job.Status.CompletionTime); err == nil {
				durationSec = int64(end.Sub(start).Seconds())
			}
		}
	}
	return &BuildSummary{
		Status:         status,
		StartTime:      job.Status.StartTime,
		CompletionTime: job.Status.CompletionTime,
		DurationSec:    durationSec,
	}
}

// create performs an authenticated POST (create) request to the K8s API.
func (k *K8sClient) create(path string, body []byte) error {
	req, err := http.NewRequest("POST", k.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+k.token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := k.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("K8s API %d: %s", resp.StatusCode, string(respBody[:min(len(respBody), 300)]))
	}

	return nil
}
