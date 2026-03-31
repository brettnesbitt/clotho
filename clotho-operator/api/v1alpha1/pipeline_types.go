package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PipelineRuntime defines whether the pipeline runs as a WASM module (SpinApp) or a native container (Deployment).
// +kubebuilder:validation:Enum=wasm;native
type PipelineRuntime string

const (
	// PipelineRuntimeWasm runs the pipeline as a SpinApp via SpinKube.
	PipelineRuntimeWasm PipelineRuntime = "wasm"

	// PipelineRuntimeNative runs the pipeline as a standard Kubernetes Deployment.
	PipelineRuntimeNative PipelineRuntime = "native"
)

// PipelineMode defines the execution model for a pipeline.
// +kubebuilder:validation:Enum=stream;once;batch
type PipelineMode string

const (
	// PipelineModeStream processes records continuously from a source (e.g. Kafka, MQTT).
	// Telemetry: time-bucket aggregation, DLQ inbox, lifecycle log.
	PipelineModeStream PipelineMode = "stream"

	// PipelineModeOnce processes a single request/payload (webhook-style).
	// Telemetry: discrete execution records with duration, status, logs.
	PipelineModeOnce PipelineMode = "once"

	// PipelineModeBatch processes a finite set of records in one run.
	// Telemetry: discrete execution records with duration, records in/out, status.
	PipelineModeBatch PipelineMode = "batch"
)

// PipelineSpec defines the desired state of Pipeline
type PipelineSpec struct {
	// Runtime selects the execution target: "wasm" (SpinApp) or "native" (Deployment).
	// +kubebuilder:default:="wasm"
	// +kubebuilder:validation:Enum=wasm;native
	Runtime PipelineRuntime `json:"runtime,omitempty"`

	// Mode is the execution model for this pipeline.
	// Determines how telemetry is stored and displayed.
	// +kubebuilder:default:="stream"
	// +kubebuilder:validation:Enum=stream;once;batch
	Mode PipelineMode `json:"mode,omitempty"`

	// 1. Source Control (The "Code")
	// If provided, the Clotho Builder will clone, compile, and push to the internal registry.
	// If omitted and Image is provided, the builder is skipped entirely (Tier 2: BYOR).
	// +optional
	GitRepository string `json:"gitRepository,omitempty"`

	// Reference is the branch, tag, or commit. Defaults to "main".
	// +kubebuilder:default:="main"
	Reference string `json:"reference,omitempty"`

	// GitCredentialsSecret is the name of a Secret containing git credentials.
	// The secret should have a "token" key with a GitHub PAT or similar token.
	// +optional
	GitCredentialsSecret string `json:"gitCredentialsSecret,omitempty"`

	// Path is the subdirectory within the repository containing the pipeline code.
	// Use this for monorepos where the Cargo.toml is not at the root.
	// +optional
	Path string `json:"path,omitempty"`

	// Image is the OCI image reference for the built pipeline.
	// For Tier 1 (builder): populated automatically after build completes.
	// For Tier 2 (BYOR): set by the user to skip the builder entirely.
	// +optional
	Image string `json:"image,omitempty"`

	// ImagePullSecrets is a list of references to secrets for pulling from private registries.
	// Only needed for Tier 2 (BYOR) when the image is in a private external registry (GCP, AWS, etc).
	// These are passed through to the SpinApp so Kubernetes can authenticate the pull.
	// +optional
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`

	// 2. Runtime Configuration (The "Variables")
	// Defines environment variables and secret injections.
	// +optional
	Config []ConfigVar `json:"config,omitempty"`

	// 3. Compute Requirements (The "Iron")
	// Defines CPU/Memory limits. If not set, defaults to a "Tiny" profile.
	// +kubebuilder:default:={requests: {cpu: "5m", memory: "32Mi"}, limits: {cpu: "100m", memory: "128Mi"}}
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// 4. Operational Policy (The "Guardrails")
	// Defines timeouts, retries, and scaling constraints.
	// +optional
	Policy PolicySpec `json:"policy,omitempty"`

	// replicas is the desired number of replicas.
	// +kubebuilder:default:=1
	Replicas int32 `json:"replicas,omitempty"`

	// 5. Schedule (The "When")
	// Defines how and when the operator invokes this pipeline.
	// If omitted, defaults to "trigger" mode (on-demand only via API).
	// +optional
	Schedule *ScheduleSpec `json:"schedule,omitempty"`

	// 6. DAG Stages (The "Topology")
	// Defines multi-stage pipeline topology for DAG-based pipelines.
	// When stages are defined, the operator creates separate workloads for each stage.
	// +optional
	Stages []PipelineStage `json:"stages,omitempty"`

	// 7. Message Bus Configuration (The "Glue")
	// Defines the message bus used for inter-stage communication.
	// +optional
	MessageBus *MessageBusSpec `json:"messageBus,omitempty"`
}

// PipelineStage defines a single stage in a DAG pipeline
type PipelineStage struct {
	// Name is the unique identifier for this stage within the pipeline.
	Name string `json:"name"`

	// Entrypoint is the path to the Rust source file for this stage.
	// Example: "src/ingest.rs" or "src/worker.rs"
	Entrypoint string `json:"entrypoint"`

	// Replicas is the number of replicas for this stage.
	// +kubebuilder:default:=1
	Replicas int32 `json:"replicas,omitempty"`

	// DependsOn defines which stages must complete before this stage can start.
	// This creates the DAG edges.
	// +optional
	DependsOn []string `json:"dependsOn,omitempty"`

	// Resources defines compute requirements for this stage.
	// If not set, inherits from the parent pipeline spec.
	// +optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`

	// Config defines environment variables specific to this stage.
	// Merged with the parent pipeline config.
	// +optional
	Config []ConfigVar `json:"config,omitempty"`

	// Schedule defines when this stage should be invoked.
	// Only valid for entry stages (no dependsOn).
	// +optional
	Schedule *ScheduleSpec `json:"schedule,omitempty"`
}

// MessageBusSpec defines the message bus configuration for inter-stage communication
type MessageBusSpec struct {
	// Type specifies the message bus implementation.
	// +kubebuilder:default:="nats-jetstream"
	// +kubebuilder:validation:Enum=nats-jetstream;kafka;redis-streams
	Type string `json:"type"`

	// ClusterRef references a NATS/Kafka/Redis cluster in the same namespace.
	// +optional
	ClusterRef string `json:"clusterRef,omitempty"`

	// Config defines additional configuration for the message bus.
	// +optional
	Config map[string]string `json:"config,omitempty"`
}

// ScheduleSpec defines when the operator invokes a pipeline.
type ScheduleSpec struct {
	// Mode: "trigger" (on-demand), "interval" (every N seconds), or "cron" (cron expression).
	// +kubebuilder:default:="trigger"
	// +kubebuilder:validation:Enum=trigger;interval;cron
	Mode string `json:"mode"`

	// Interval is the duration between invocations (e.g. "30s", "5m", "1h").
	// Only used when mode is "interval".
	// +optional
	Interval string `json:"interval,omitempty"`

	// Cron is a standard cron expression (e.g. "0 9 * * *").
	// Only used when mode is "cron".
	// +optional
	Cron string `json:"cron,omitempty"`
}

// ConfigVar allows injecting values or secrets into the runtime
type ConfigVar struct {
	// Name of the environment variable (e.g. "DB_PASSWORD")
	Name string `json:"name"`

	// Value is a literal string value (e.g. "production")
	// +optional
	Value string `json:"value,omitempty"`

	// ValueFrom allows selecting a key from a Secret or ConfigMap
	// +optional
	ValueFrom *ConfigVarSource `json:"valueFrom,omitempty"`
}

type ConfigVarSource struct {
	// Selects a key from a Secret in the same namespace
	// +optional
	SecretKeyRef *corev1.SecretKeySelector `json:"secretKeyRef,omitempty"`
}

// PolicySpec defines operational constraints
type PolicySpec struct {
	// TimeoutSeconds: Max execution time before the worker is killed.
	// +kubebuilder:default:=30
	// +kubebuilder:validation:Minimum=1
	TimeoutSeconds int32 `json:"timeoutSeconds,omitempty"`

	// MaxRetries: How many times to retry on failure.
	// +kubebuilder:default:=3
	MaxRetries int32 `json:"maxRetries,omitempty"`
}

// PipelineStatus defines the observed state of Pipeline
type PipelineStatus struct {
	// Phase: Pending, Provisioning, Running, Failed
	Phase string `json:"phase,omitempty"`

	// Conditions provides detailed reasons for the current state
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// URL is the public endpoint if exposed
	URL string `json:"url,omitempty"`

	// ObservedGeneration represents the .metadata.generation that the condition was set based upon.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// LastInvocation is the timestamp of the last scheduled invocation.
	// Used by the scheduler to determine when the next invocation should occur.
	// +optional
	LastInvocation *metav1.Time `json:"lastInvocation,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Runtime",type=string,JSONPath=`.spec.runtime`
// +kubebuilder:printcolumn:name="Mode",type=string,JSONPath=`.spec.mode`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="URL",type=string,JSONPath=`.status.url`
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// Pipeline is the Schema for the pipelines API
type Pipeline struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PipelineSpec   `json:"spec,omitempty"`
	Status PipelineStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PipelineList contains a list of Pipeline
type PipelineList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Pipeline `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Pipeline{}, &PipelineList{})
}
