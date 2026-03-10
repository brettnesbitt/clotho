package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PipelineSpec defines the desired state of Pipeline
type PipelineSpec struct {
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
	// +kubebuilder:default:={requests: {cpu: "100m", memory: "64Mi"}, limits: {cpu: "500m", memory: "128Mi"}}
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// 4. Operational Policy (The "Guardrails")
	// Defines timeouts, retries, and scaling constraints.
	// +optional
	Policy PolicySpec `json:"policy,omitempty"`

	// replicas is the desired number of replicas.
	// +kubebuilder:default:=1
	Replicas int32 `json:"replicas,omitempty"`
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

	// ADD THIS FIELD:
	// ObservedGeneration represents the .metadata.generation that the condition was set based upon.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
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
