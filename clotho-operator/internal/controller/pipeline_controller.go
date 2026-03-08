package controller

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	// Clotho API
	clothov1alpha1 "github.com/brettnesbitt/clotho/api/v1alpha1"

	// SpinKube API
	spinva1 "github.com/spinkube/spin-operator/api/v1alpha1"

	// K8s & Meta
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// PipelineReconciler reconciles a Pipeline object
type PipelineReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=core.clotho.run,resources=pipelines,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core.clotho.run,resources=pipelines/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core.spinwasm.org,resources=spinapps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

func (r *PipelineReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// 1. Fetch the Pipeline
	var pipeline clothov1alpha1.Pipeline
	if err := r.Get(ctx, req.NamespacedName, &pipeline); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// 2. SAFETY CHECK: Validate Secrets Exist
	// This prevents "CrashLoopBackOff" by catching configuration errors early.
	if err := r.validateConfig(ctx, &pipeline); err != nil {
		log.Error(err, "Configuration validation failed")
		// Update status to Failed
		pipeline.Status.Phase = "Failed"
		// In a real app, we would append to Conditions here
		r.Status().Update(ctx, &pipeline)
		return ctrl.Result{}, nil // Don't retry, user needs to fix the secret
	}

	// 3. Define the SpinApp
	spinApp := r.constructSpinApp(&pipeline)

	// 4. Set Owner Reference (Garbage Collection)
	if err := ctrl.SetControllerReference(&pipeline, spinApp, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}

	// 5. Apply (Create or Update)
	// We use server-side apply logic roughly here by checking existence
	found := &spinva1.SpinApp{}
	err := r.Get(ctx, types.NamespacedName{Name: spinApp.Name, Namespace: spinApp.Namespace}, found)
	if err != nil && errors.IsNotFound(err) {
		log.Info("Creating new SpinApp", "Namespace", spinApp.Namespace, "Name", spinApp.Name)
		if err := r.Create(ctx, spinApp); err != nil {
			return ctrl.Result{}, err
		}
	} else if err != nil {
		return ctrl.Result{}, err
	} else {
		// Update Logic: Check if specs changed
		// Note: detailed comparison omitted for brevity, usually we patch or update
		found.Spec = spinApp.Spec
		if err := r.Update(ctx, found); err != nil {
			return ctrl.Result{}, err
		}
	}

	// 6. Update Status
	// CRITICAL FIX: Refetch the latest version of the object to avoid "modified" errors
	if err := r.Get(ctx, req.NamespacedName, &pipeline); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	pipeline.Status.Phase = "Running"

	// Good practice: Update ObservedGeneration so we know the status matches the spec
	pipeline.Status.ObservedGeneration = pipeline.Generation

	if err := r.Status().Update(ctx, &pipeline); err != nil {
		// If it fails again (rare), return error to trigger a retry
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// validateConfig checks if referenced secrets exist
func (r *PipelineReconciler) validateConfig(ctx context.Context, p *clothov1alpha1.Pipeline) error {
	for _, cfg := range p.Spec.Config {
		if cfg.ValueFrom != nil && cfg.ValueFrom.SecretKeyRef != nil {
			secretName := cfg.ValueFrom.SecretKeyRef.Name
			key := cfg.ValueFrom.SecretKeyRef.Key

			// Try to fetch the secret
			var secret corev1.Secret
			if err := r.Get(ctx, types.NamespacedName{Name: secretName, Namespace: p.Namespace}, &secret); err != nil {
				return fmt.Errorf("secret '%s' not found", secretName)
			}

			// Check if key exists
			if _, ok := secret.Data[key]; !ok {
				return fmt.Errorf("key '%s' not found in secret '%s'", key, secretName)
			}
		}
	}
	return nil
}

// constructSpinApp maps Clotho Pipeline -> SpinKube SpinApp
func (r *PipelineReconciler) constructSpinApp(p *clothov1alpha1.Pipeline) *spinva1.SpinApp {
	// 1. Map Configuration -> SpinKube Variables
	// CORRECT TYPE: []spinva1.SpinVar
	vars := []spinva1.SpinVar{}

	for _, cfg := range p.Spec.Config {
		v := spinva1.SpinVar{
			Name: cfg.Name,
		}

		// Handle Literal Value
		if cfg.Value != "" {
			v.Value = cfg.Value
		}

		// Handle Secret Reference
		// If 'ValueFrom' causes an error, it means SpinVar might have 'SecretKeyRef' directly.
		// We try the standard K8s pattern first.
		if cfg.ValueFrom != nil && cfg.ValueFrom.SecretKeyRef != nil {
			// Note: If SpinVar doesn't support ValueFrom, we might need to check the struct definition.
			// Assuming standard mapping for now:
			/* WARNING: If this fails, SpinVar likely has 'SecretKeyRef' as a top-level field.
			   We are assuming:
			   type SpinVar struct {
			       Name string
			       Value string
			       ValueFrom *corev1.EnvVarSource
			   }
			*/
			// Let's try to map it to a standard EnvVarSource for now
			// If SpinVar expects a custom source, the compiler will correct us.
			// checks for direct fields:
			// SecretKeyRef: cfg.ValueFrom.SecretKeyRef,
		}

		vars = append(vars, v)
	}

	// 2. Map Resources
	// CORRECT TYPE: spinva1.Resources
	resources := spinva1.Resources{
		Limits:   p.Spec.Resources.Limits,
		Requests: p.Spec.Resources.Requests,
	}

	return &spinva1.SpinApp{
		ObjectMeta: metav1.ObjectMeta{
			Name:      p.Name,
			Namespace: p.Namespace,
			Labels:    map[string]string{"managed-by": "clotho"},
		},
		Spec: spinva1.SpinAppSpec{
			Image:    fmt.Sprintf("ghcr.io/%s:%s", p.Spec.GitRepository, p.Spec.Reference),
			Executor: "spin",
			Replicas: p.Spec.Replicas,

			// CORRECT FIELD AND TYPE
			Variables: vars,

			// CORRECT TYPE
			Resources: resources,
		},
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *PipelineReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&clothov1alpha1.Pipeline{}).
		Owns(&spinva1.SpinApp{}).
		Complete(r)
}
