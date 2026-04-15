package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	// Clotho API
	clothov1alpha1 "github.com/brettnesbitt/clotho/api/v1alpha1"

	// SpinKube API
	spinva1 "github.com/spinkube/spin-operator/api/v1alpha1"

	// K8s & Meta
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
)

// PipelineReconciler reconciles a Pipeline object
type PipelineReconciler struct {
	client.Client
	Scheme          *runtime.Scheme
	ControlPlaneURL string // e.g. "http://clotho-api.clotho-system.svc.cluster.local:3000"
}

// +kubebuilder:rbac:groups=core.clotho.run,resources=pipelines,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core.clotho.run,resources=pipelines/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core.spinkube.dev,resources=spinapps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch

func (r *PipelineReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// TODO: Add sidecar injection for telemetry agent
	// pass this in via an Environment Variable in the Operator Deployment
	// operatorVersion := os.Getenv("OPERATOR_VERSION") // e.g. "0.0.1"
	// sidecar := corev1.Container{
	// 	Name:  "clotho-agent",
	// 	Image: fmt.Sprintf("ghcr.io/clotho/agent:%s", operatorVersion),
	// }

	// 1. Fetch the Pipeline
	var pipeline clothov1alpha1.Pipeline
	if err := r.Get(ctx, req.NamespacedName, &pipeline); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// 2. Handle Build (Source -> Image)
	// If this returns a Requeue/RequeueAfter or Error, stop here.
	// We can't deploy a SpinApp until we have an Image.
	if res, err := r.reconcileBuild(ctx, &pipeline); err != nil || res.Requeue || res.RequeueAfter > 0 {
		return res, err
	}

	// 3. SAFETY CHECK: Validate Secrets Exist
	// This prevents "CrashLoopBackOff" by catching configuration errors early.
	if err := r.validateConfig(ctx, &pipeline); err != nil {
		log.Error(err, "Configuration validation failed")
		// Update status to Failed
		pipeline.Status.Phase = "Failed"
		// In a real app, we would append to Conditions here
		r.Status().Update(ctx, &pipeline)
		return ctrl.Result{}, nil // Don't retry, user needs to fix the secret
	}

	// 3. Deploy based on Runtime
	// Check if this is a DAG pipeline with stages
	if len(pipeline.Spec.Stages) > 0 {
		if err := r.reconcileDAGPipeline(ctx, &pipeline); err != nil {
			return ctrl.Result{}, err
		}
	} else {
		// Single-stage pipeline (legacy behavior)
		switch pipeline.Spec.Runtime {
		case clothov1alpha1.PipelineRuntimeNative:
			if err := r.reconcileNativeDeployment(ctx, &pipeline); err != nil {
				return ctrl.Result{}, err
			}
		default: // wasm (default)
			if err := r.reconcileSpinApp(ctx, &pipeline); err != nil {
				return ctrl.Result{}, err
			}
		}
	}

	// 6. Update Status
	// CRITICAL FIX: Refetch the latest version of the object to avoid "modified" errors
	if err := r.Get(ctx, req.NamespacedName, &pipeline); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Phase reflects pipeline lifecycle, not pod state:
	// - Idling:  deployed, trigger-only (no automatic schedule)
	// - Enabled: deployed with an active schedule
	// - Running: currently being invoked (set transiently by scheduler)
	if pipeline.Spec.Schedule != nil && pipeline.Spec.Schedule.Mode != "trigger" {
		pipeline.Status.Phase = "Enabled"
	} else {
		pipeline.Status.Phase = "Idling"
	}
	pipeline.Status.Message = ""

	// Bubble up underlying workload failures
	if pipeline.Spec.Runtime != clothov1alpha1.PipelineRuntimeNative {
		var spinApp spinva1.SpinApp
		if err := r.Get(ctx, types.NamespacedName{Name: pipeline.Name, Namespace: pipeline.Namespace}, &spinApp); err == nil {
			for _, cond := range spinApp.Status.Conditions {
				if cond.Status == "False" || cond.Status == "Unknown" {
					pipeline.Status.Phase = "Failed"
					pipeline.Status.Message = fmt.Sprintf("SpinApp %s: %s", cond.Reason, cond.Message)
					break
				}
			}
		}
	}

	// Good practice: Update ObservedGeneration so we know the status matches the spec
	pipeline.Status.ObservedGeneration = pipeline.Generation

	if err := r.Status().Update(ctx, &pipeline); err != nil {
		// If it fails again (rare), return error to trigger a retry
		return ctrl.Result{}, err
	}

	// 7. Handle Schedule (The "When")
	// If a schedule is configured, calculate the next invocation and requeue.
	return r.reconcileSchedule(ctx, &pipeline)
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
	vars := []spinva1.SpinVar{
		{Name: "CLOTHO_PIPELINE_ID", Value: p.Name},
		// Inject node IP so SDK telemetry (HTTP) reaches the agent DaemonSet on hostNetwork
		{
			Name: "CLOTHO_AGENT_HOST",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "status.hostIP",
				},
			},
		},
	}

	userDefinedVars := make(map[string]bool)
	for _, v := range vars {
		userDefinedVars[v.Name] = true
	}

	for _, cfg := range p.Spec.Config {
		if userDefinedVars[cfg.Name] {
			// Skip user-defined vars that match injected ones to avoid duplicate env errors
			continue
		}

		v := spinva1.SpinVar{
			Name: cfg.Name,
		}

		// Handle Literal Value
		if cfg.Value != "" {
			v.Value = cfg.Value
		}

		// Handle Secret Reference
		if cfg.ValueFrom != nil && cfg.ValueFrom.SecretKeyRef != nil {
			selector := *cfg.ValueFrom.SecretKeyRef
			v.ValueFrom = &corev1.EnvVarSource{
				SecretKeyRef: &selector,
			}
		}

		vars = append(vars, v)
	}

	// 2. Map Resources
	// CORRECT TYPE: spinva1.Resources
	resources := spinva1.Resources{
		Limits:   p.Spec.Resources.Limits,
		Requests: p.Spec.Resources.Requests,
	}

	replicas := p.Spec.Replicas
	if replicas == 0 {
		replicas = 1
	}

	return &spinva1.SpinApp{
		ObjectMeta: metav1.ObjectMeta{
			Name:      p.Name,
			Namespace: p.Namespace,
			Labels:    map[string]string{"managed-by": "clotho"},
		},
		Spec: spinva1.SpinAppSpec{
			Image:            p.Spec.Image,
			Executor:         "containerd-shim-spin", // Native Kubelet integration
			Replicas:         replicas,
			ImagePullSecrets: p.Spec.ImagePullSecrets,
			Variables:        vars,
			Resources:        resources,
		},
	}
}

// reconcileSpinApp handles WASM pipelines by creating/updating a SpinApp
func (r *PipelineReconciler) reconcileSpinApp(ctx context.Context, pipeline *clothov1alpha1.Pipeline) error {
	log := log.FromContext(ctx)

	spinApp := r.constructSpinApp(pipeline)
	if err := ctrl.SetControllerReference(pipeline, spinApp, r.Scheme); err != nil {
		return err
	}

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		found := &spinva1.SpinApp{}
		err := r.Get(ctx, types.NamespacedName{Name: spinApp.Name, Namespace: spinApp.Namespace}, found)
		if err != nil && errors.IsNotFound(err) {
			log.Info("Creating new SpinApp", "Namespace", spinApp.Namespace, "Name", spinApp.Name)
			return r.Create(ctx, spinApp)
		} else if err != nil {
			return err
		}

		found.Spec = spinApp.Spec
		return r.Update(ctx, found)
	})
}

// reconcileNativeDeployment handles native pipelines by creating/updating a Deployment
func (r *PipelineReconciler) reconcileNativeDeployment(ctx context.Context, pipeline *clothov1alpha1.Pipeline) error {
	log := log.FromContext(ctx)

	deploy := r.constructDeployment(pipeline)
	if err := ctrl.SetControllerReference(pipeline, deploy, r.Scheme); err != nil {
		return err
	}

	found := &appsv1.Deployment{}
	err := r.Get(ctx, types.NamespacedName{Name: deploy.Name, Namespace: deploy.Namespace}, found)
	if err != nil && errors.IsNotFound(err) {
		log.Info("Creating native Deployment", "Namespace", deploy.Namespace, "Name", deploy.Name)
		return r.Create(ctx, deploy)
	} else if err != nil {
		return err
	}

	// Update container image, env, and resources
	found.Spec.Replicas = deploy.Spec.Replicas
	found.Spec.Template = deploy.Spec.Template
	return r.Update(ctx, found)
}

// constructDeployment maps Clotho Pipeline -> Kubernetes Deployment (native runtime)
func (r *PipelineReconciler) constructDeployment(p *clothov1alpha1.Pipeline) *appsv1.Deployment {
	labels := map[string]string{
		"app":                 p.Name,
		"clotho.run/pipeline": p.Name,
		"clotho.run/mode":     string(p.Spec.Mode),
		"clotho.run/runtime":  "native",
		"managed-by":          "clotho",
	}

	envVars := []corev1.EnvVar{
		{Name: "CLOTHO_PIPELINE_ID", Value: p.Name},
		{Name: "CLOTHO_API_URL", Value: r.ControlPlaneURL},
		{Name: "RUST_LOG", Value: "info"},
		// Inject node IP so SDK telemetry (UDP) reaches the agent DaemonSet on hostNetwork
		{
			Name: "CLOTHO_AGENT_HOST",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "status.hostIP",
				},
			},
		},
	}

	for _, cfg := range p.Spec.Config {
		ev := corev1.EnvVar{Name: cfg.Name}
		if cfg.Value != "" {
			ev.Value = cfg.Value
		}
		if cfg.ValueFrom != nil && cfg.ValueFrom.SecretKeyRef != nil {
			ev.ValueFrom = &corev1.EnvVarSource{
				SecretKeyRef: cfg.ValueFrom.SecretKeyRef,
			}
		}
		envVars = append(envVars, ev)
	}

	replicas := p.Spec.Replicas
	if replicas == 0 {
		replicas = 1
	}

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      p.Name,
			Namespace: p.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": p.Name},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:            "pipeline",
						Image:           p.Spec.Image,
						ImagePullPolicy: corev1.PullAlways,
						Env:             envVars,
						Resources:       p.Spec.Resources,
					}},
					ImagePullSecrets: p.Spec.ImagePullSecrets,
					RestartPolicy:    corev1.RestartPolicyAlways,
				},
			},
		},
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *PipelineReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&clothov1alpha1.Pipeline{}).
		Owns(&spinva1.SpinApp{}).
		Owns(&appsv1.Deployment{}).
		Owns(&batchv1.Job{}). // Watch builder jobs to detect completion
		Complete(r)
}

// internalRegistry is the in-cluster OCI registry deployed as part of the Clotho control plane.
// Builder jobs push to HTTPS endpoint. Kubelet pulls with TLS verification.
const internalRegistry = "clotho-registry.clotho-system.svc.cluster.local:5000"

func sanitizeImageTagPart(input string) string {
	replacer := strings.NewReplacer(
		"/", "-",
		":", "-",
		"@", "-",
		" ", "-",
	)
	sanitized := replacer.Replace(input)
	sanitized = strings.Trim(sanitized, "-.")
	if sanitized == "" {
		return "main"
	}
	return sanitized
}

func (r *PipelineReconciler) reconcileBuild(ctx context.Context, pipeline *clothov1alpha1.Pipeline) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// -----------------------------------------------------------
	// Tier 2: BYOR (Bring Your Own Registry)
	// If the user set spec.image AND there is no gitRepository,
	// skip the builder entirely. The image is already built.
	// -----------------------------------------------------------
	if pipeline.Spec.GitRepository == "" && pipeline.Spec.Build == nil {
		if pipeline.Spec.Image != "" {
			log.Info("Tier 2: Using pre-built image, skipping builder", "image", pipeline.Spec.Image)
		}
		return ctrl.Result{}, nil
	}

	// -----------------------------------------------------------
	// Tier 1.5: External Builder (Cloud Build, GitHub Actions, etc.)
	// If spec.build is set, trigger external build instead of in-cluster.
	// -----------------------------------------------------------
	if pipeline.Spec.Build != nil && pipeline.Spec.Image == "" {
		return r.reconcileExternalBuild(ctx, pipeline)
	}

	// -----------------------------------------------------------
	// Tier 1: Batteries Included (Internal Registry)
	// Builder compiles from Git and pushes to the in-cluster registry.
	// Each build uses a unique timestamp-based tag so containerd
	// always pulls the fresh OCI artifact (no stale cache).
	// -----------------------------------------------------------

	jobName := fmt.Sprintf("builder-%s", pipeline.Name)
	existingJob := &batchv1.Job{}
	err := r.Get(ctx, types.NamespacedName{Name: jobName, Namespace: pipeline.Namespace}, existingJob)

	if err == nil {
		// Job exists - check its status
		if existingJob.Status.Succeeded > 0 {
			// Read the target image from the job annotation
			targetImage := existingJob.Annotations["clotho.run/target-image"]
			if targetImage == "" {
				// Fallback for jobs created before this change
				targetImage = fmt.Sprintf("%s/%s:%s", internalRegistry, pipeline.Name, pipeline.Spec.Reference)
			}
			// Only update spec.image if it's different (avoid unnecessary updates)
			if pipeline.Spec.Image != targetImage {
				log.Info("Build job completed, updating image", "image", targetImage)
				pipeline.Spec.Image = targetImage
				return ctrl.Result{}, r.Update(ctx, pipeline)
			}
			log.Info("Build already completed", "image", targetImage)
			return ctrl.Result{}, nil
		}
		if existingJob.Status.Failed > 0 {
			log.Info("Build job failed", "job", jobName)
			return ctrl.Result{}, nil
		}
		log.Info("Build job still running", "job", jobName)
		return ctrl.Result{RequeueAfter: time.Second * 10}, nil
	}

	// Job not found
	if !errors.IsNotFound(err) {
		return ctrl.Result{}, err
	}

	// If image is already set and job is gone, assume build completed successfully
	// UNLESS the image tag doesn't match the current reference (user may want rebuild)
	if pipeline.Spec.Image != "" {
		// Check if the image was built from the current reference
		// If not, the user may have changed the reference and wants a new build
		expectedTagPrefix := sanitizeImageTagPart(pipeline.Spec.Reference)
		if !strings.Contains(pipeline.Spec.Image, expectedTagPrefix) {
			log.Info("Reference changed since last build, triggering rebuild", "image", pipeline.Spec.Image, "reference", pipeline.Spec.Reference)
			// Clear the image to force a new build
			pipeline.Spec.Image = ""
			if err := r.Update(ctx, pipeline); err != nil {
				return ctrl.Result{}, err
			}
			// Requeue to trigger the new build
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		log.Info("Build job not found but image is set, assuming build completed", "image", pipeline.Spec.Image)
		return ctrl.Result{}, nil
	}

	// Generate a unique tag: <reference>-<unix-timestamp>
	// This ensures containerd pulls a fresh artifact on every build.
	tag := fmt.Sprintf("%s-%d", sanitizeImageTagPart(pipeline.Spec.Reference), time.Now().Unix())
	targetImage := fmt.Sprintf("%s/%s:%s", internalRegistry, pipeline.Name, tag)

	// Create the Build Job
	log.Info("Creating build job", "job", jobName, "targetImage", targetImage)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: pipeline.Namespace,
			Annotations: map[string]string{
				"clotho.run/target-image": targetImage,
			},
		},
		Spec: batchv1.JobSpec{
			TTLSecondsAfterFinished: int32Ptr(600),
			ActiveDeadlineSeconds:   int64Ptr(3600),
			BackoffLimit:            int32Ptr(2),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyOnFailure,
					Containers: []corev1.Container{{
						Name:  "builder",
						Image: "us-central1-docker.pkg.dev/quotopia-391900/clotho/clotho-builder:latest",
						Args: []string{
							pipeline.Spec.GitRepository,
							pipeline.Spec.Reference,
							targetImage,
							pipeline.Spec.Path,
							string(pipeline.Spec.Runtime),
						},
						Env: r.buildEnvVars(pipeline),
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceMemory: resource.MustParse("128Mi"),
							},
							Limits: corev1.ResourceList{
								corev1.ResourceMemory: resource.MustParse("1024Mi"),
							},
						},
						VolumeMounts: []corev1.VolumeMount{
							{
								Name:      "cargo-cache",
								MountPath: "/usr/local/cargo/registry",
							},
							{
								Name:      "build-cache",
								MountPath: "/app/target",
							},
							{
								Name:      "registry-ca",
								MountPath: "/tmp/registry-ca",
								ReadOnly:  true,
							},
						},
					}},
					Volumes: []corev1.Volume{
						{
							Name: "cargo-cache",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "clotho-builder-cache-pvc",
								},
							},
						},
						{
							Name: "build-cache",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "clotho-project-cache-pvc",
								},
							},
						},
						{
							Name: "registry-ca",
							VolumeSource: corev1.VolumeSource{
								Secret: &corev1.SecretVolumeSource{
									SecretName: "clotho-registry-tls",
								},
							},
						},
					},
				},
			},
		},
	}

	if err := controllerutil.SetControllerReference(pipeline, job, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, r.Create(ctx, job)
}

// reconcileExternalBuild handles Tier 1.5: External builders like Cloud Build
func (r *PipelineReconciler) reconcileExternalBuild(ctx context.Context, pipeline *clothov1alpha1.Pipeline) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	buildConfig := pipeline.Spec.Build

	if buildConfig == nil {
		return ctrl.Result{}, nil
	}

	jobName := fmt.Sprintf("cloudbuild-trigger-%s", pipeline.Name)
	existingJob := &batchv1.Job{}
	err := r.Get(ctx, types.NamespacedName{Name: jobName, Namespace: pipeline.Namespace}, existingJob)

	if err == nil {
		if existingJob.Status.Succeeded > 0 {
			targetImage := existingJob.Annotations["clotho.run/target-image"]
			if targetImage == "" {
				targetImage = pipeline.Annotations["clotho.run/target-image"]
			}
			if pipeline.Spec.Image != targetImage {
				log.Info("Tier 1.5: External build completed", "image", targetImage)
				pipeline.Spec.Image = targetImage
				// Reset failure counter on success
				pipeline.Status.BuildFailures = 0
				if updateErr := r.Status().Update(ctx, pipeline); updateErr != nil {
					log.Error(updateErr, "Could not reset BuildFailures after successful build")
				}
				return ctrl.Result{}, r.Update(ctx, pipeline)
			}
			log.Info("Tier 1.5: External build already completed")
			return ctrl.Result{}, nil
		}
		if existingJob.Status.Failed > 0 {
			// Determine retry limits from spec (defaults: max=3, base=1m)
			maxRetries := buildConfig.MaxBuildRetries
			if maxRetries == 0 {
				maxRetries = 3
			}
			backoffBase := buildConfig.BuildBackoffBase
			if backoffBase == "" {
				backoffBase = "1m"
			}

			failures := pipeline.Status.BuildFailures + 1
			pipeline.Status.BuildFailures = failures

			if failures > maxRetries {
				log.Info("Tier 1.5: Build exceeded max retries, marking BuildFailed",
					"failures", failures, "maxRetries", maxRetries)
				pipeline.Status.Phase = "BuildFailed"
				pipeline.Status.Message = fmt.Sprintf("Build failed %d times (max %d). Manual intervention required.", failures, maxRetries)
				if updateErr := r.Status().Update(ctx, pipeline); updateErr != nil {
					log.Error(updateErr, "Could not update status to BuildFailed")
				}
				return ctrl.Result{}, nil
			}

			// Delete the failed job so the next reconcile creates a fresh one
			if deleteErr := r.Delete(ctx, existingJob, client.PropagationPolicy(metav1.DeletePropagationBackground)); deleteErr != nil && !errors.IsNotFound(deleteErr) {
				log.Error(deleteErr, "Could not delete failed build job")
				return ctrl.Result{}, deleteErr
			}

			// Compute exponential backoff: base * 2^(failures-1), cap at 30m
			baseDur, parseErr := time.ParseDuration(backoffBase)
			if parseErr != nil {
				baseDur = time.Minute
			}
			delay := baseDur * time.Duration(int64(1)<<uint(failures-1))
			const maxDelay = 30 * time.Minute
			if delay > maxDelay {
				delay = maxDelay
			}

			if updateErr := r.Status().Update(ctx, pipeline); updateErr != nil {
				log.Error(updateErr, "Could not increment BuildFailures")
			}

			log.Info("Tier 1.5: Build failed, will retry with backoff",
				"attempt", failures, "maxRetries", maxRetries, "backoff", delay)
			return ctrl.Result{RequeueAfter: delay}, nil
		}
		log.Info("Tier 1.5: External build still running, requeueing...")
		return ctrl.Result{RequeueAfter: time.Second * 30}, nil
	}

	if !errors.IsNotFound(err) {
		return ctrl.Result{}, err
	}

	// Job doesn't exist, we must trigger it
	// Generate unique image tag
	tag := fmt.Sprintf("%s-%d", sanitizeImageTagPart(pipeline.Spec.Reference), time.Now().Unix())
	registry := buildConfig.Registry
	if registry == "" {
		registry = "gcr.io"
	}
	targetImage := fmt.Sprintf("%s/%s:%s", registry, pipeline.Name, tag)

	switch buildConfig.Builder {
	case "cloudbuild":
		log.Info("Tier 1.5: Triggering Cloud Build", "image", targetImage)
		if err := r.triggerCloudBuild(ctx, pipeline, targetImage); err != nil {
			log.Error(err, "Failed to trigger Cloud Build")
			return ctrl.Result{}, err
		}
	default:
		log.Info("Tier 1.5: External builder type not yet implemented", "builder", buildConfig.Builder)
		return r.reconcileBuildTier1(ctx, pipeline)
	}

	if pipeline.Annotations == nil {
		pipeline.Annotations = make(map[string]string)
	}
	pipeline.Annotations["clotho.run/target-image"] = targetImage
	pipeline.Annotations["clotho.run/builder"] = buildConfig.Builder

	return ctrl.Result{RequeueAfter: time.Second * 30}, r.Update(ctx, pipeline)
}

// triggerCloudBuild triggers a Cloud Build job via the GCP API
func (r *PipelineReconciler) triggerCloudBuild(ctx context.Context, pipeline *clothov1alpha1.Pipeline, targetImage string) error {
	log := log.FromContext(ctx)
	buildConfig := pipeline.Spec.Build
	if buildConfig == nil {
		return fmt.Errorf("build config is nil")
	}

	jobName := fmt.Sprintf("cloudbuild-trigger-%s", pipeline.Name)
	buildArgs := []string{
		"builds", "submit",
		"--config", "cloudbuild.yaml",
		"--substitutions", fmt.Sprintf("_REGISTRY=%s,_IMAGE_NAME=%s", buildConfig.Registry, pipeline.Name),
		"--tag", targetImage,
		pipeline.Spec.GitRepository,
	}
	for key, value := range buildConfig.BuildArgs {
		buildArgs = append(buildArgs, "--substitutions", fmt.Sprintf("%s=%s", key, value))
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: pipeline.Namespace,
			Annotations: map[string]string{
				"clotho.run/pipeline":     pipeline.Name,
				"clotho.run/target-image": targetImage,
			},
		},
		Spec: batchv1.JobSpec{
			TTLSecondsAfterFinished: int32Ptr(3600),
			ActiveDeadlineSeconds:   int64Ptr(7200),
			BackoffLimit:            int32Ptr(0),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name:  "cloudbuild-trigger",
						Image: "gcr.io/cloud-builders/gcloud:latest",
						Command: []string{"sh", "-c", fmt.Sprintf(`
							set -e
							if [ -f /etc/cloudbuild/key.json ]; then
								gcloud auth activate-service-account --key-file=/etc/cloudbuild/key.json
							elif [ -f /etc/cloudbuild/token ]; then
								gcloud auth activate-service-account --key-file=/etc/cloudbuild/token
							fi
							REPO_URL="%s"
							if [ -n "$GIT_TOKEN" ]; then
								REPO_URL=$(echo "$REPO_URL" | sed "s|https://|https://${GIT_TOKEN}@|")
							fi
							git clone --depth 1 --branch %s "$REPO_URL" /tmp/repo
							cd /tmp/repo/%s
							# Bundle clotho dependency for Cloud Build
							if [ -f Cargo.toml ] && grep -q "clotho" Cargo.toml; then
								git clone --depth 1 --branch main "https://${GIT_TOKEN}@github.com/brettnesbitt/clotho.git" vendor-clotho
								sed -i "s|\.\./\.\./\.\./clotho|vendor-clotho|g" Cargo.toml
							fi
							gcloud builds submit . \
								--config=cloudbuild.yaml \
								--substitutions=_TARGET_IMAGE=%s
						`, pipeline.Spec.GitRepository, pipeline.Spec.Reference, pipeline.Spec.Path, targetImage)},
						Env: []corev1.EnvVar{{
							Name: "GIT_TOKEN",
							ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
								LocalObjectReference: corev1.LocalObjectReference{Name: "clotho-git-credentials"},
								Key:                  "token",
								Optional:             boolPtr(true),
							}},
						}},
						VolumeMounts: []corev1.VolumeMount{{
							Name:      "cloudbuild-key",
							MountPath: "/etc/cloudbuild",
							ReadOnly:  true,
						}},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceMemory: resource.MustParse("256Mi"),
								corev1.ResourceCPU:    resource.MustParse("100m"),
							},
							Limits: corev1.ResourceList{
								corev1.ResourceMemory: resource.MustParse("512Mi"),
								corev1.ResourceCPU:    resource.MustParse("500m"),
							},
						},
					}},
					Volumes: []corev1.Volume{{
						Name: "cloudbuild-key",
						VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
							SecretName: buildConfig.ServiceAccountSecret,
							Optional:   boolPtr(false),
						}},
					}},
				},
			},
		},
	}

	if err := controllerutil.SetControllerReference(pipeline, job, r.Scheme); err != nil {
		return err
	}

	existingJob := &batchv1.Job{}
	if err := r.Get(ctx, types.NamespacedName{Name: jobName, Namespace: pipeline.Namespace}, existingJob); err == nil {
		log.Info("Cloud Build trigger job already exists", "job", jobName)
		return nil
	} else if !errors.IsNotFound(err) {
		return err
	}

	log.Info("Creating Cloud Build trigger job", "job", jobName, "image", targetImage)
	return r.Create(ctx, job)
}

// reconcileBuildTier1 is the original Tier 1 in-cluster builder logic
func (r *PipelineReconciler) reconcileBuildTier1(ctx context.Context, pipeline *clothov1alpha1.Pipeline) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	if pipeline.Spec.Image != "" {
		return ctrl.Result{}, nil
	}

	jobName := fmt.Sprintf("builder-%s", pipeline.Name)
	existingJob := &batchv1.Job{}
	err := r.Get(ctx, types.NamespacedName{Name: jobName, Namespace: pipeline.Namespace}, existingJob)
	if err == nil {
		if existingJob.Status.Succeeded > 0 {
			targetImage := existingJob.Annotations["clotho.run/target-image"]
			if targetImage == "" {
				targetImage = fmt.Sprintf("%s/%s:%s", internalRegistry, pipeline.Name, pipeline.Spec.Reference)
			}
			log.Info("Build job completed, updating image", "image", targetImage)
			pipeline.Spec.Image = targetImage
			return ctrl.Result{}, r.Update(ctx, pipeline)
		}
		if existingJob.Status.Failed > 0 {
			log.Info("Build job failed", "job", jobName)
			return ctrl.Result{}, nil
		}
		log.Info("Build job still running", "job", jobName)
		return ctrl.Result{RequeueAfter: time.Second * 10}, nil
	}
	if !errors.IsNotFound(err) {
		return ctrl.Result{}, err
	}

	log.Info("Creating in-cluster build job (Tier 1)", "pipeline", pipeline.Name)
	return r.createBuilderJob(ctx, pipeline)
}

func (r *PipelineReconciler) createBuilderJob(ctx context.Context, pipeline *clothov1alpha1.Pipeline) (ctrl.Result, error) {
	jobName := fmt.Sprintf("builder-%s", pipeline.Name)
	targetImage := fmt.Sprintf("%s/%s:%s", internalRegistry, pipeline.Name, pipeline.Spec.Reference)

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: pipeline.Namespace,
			Annotations: map[string]string{
				"clotho.run/pipeline":     pipeline.Name,
				"clotho.run/target-image": targetImage,
			},
		},
		Spec: batchv1.JobSpec{
			TTLSecondsAfterFinished: int32Ptr(3600),
			BackoffLimit:            int32Ptr(0),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name:  "builder",
						Image: "ghcr.io/brettnesbitt/clotho-builder:latest",
						Env:   r.buildEnvVars(pipeline),
					}},
				},
			},
		},
	}

	if err := controllerutil.SetControllerReference(pipeline, job, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}

	existing := &batchv1.Job{}
	if err := r.Get(ctx, types.NamespacedName{Name: jobName, Namespace: pipeline.Namespace}, existing); err == nil {
		return ctrl.Result{RequeueAfter: time.Second * 10}, nil
	} else if !errors.IsNotFound(err) {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, r.Create(ctx, job)
}

func int32Ptr(i int32) *int32 { return &i }
func int64Ptr(i int64) *int64 { return &i }
func boolPtr(b bool) *bool    { return &b }

// buildEnvVars creates environment variables for the builder job.
// Only git credentials are needed; registry auth is not required for the internal registry.
func (r *PipelineReconciler) buildEnvVars(pipeline *clothov1alpha1.Pipeline) []corev1.EnvVar {
	var envVars []corev1.EnvVar

	gitSecretName := pipeline.Spec.GitCredentialsSecret
	if gitSecretName == "" {
		gitSecretName = "clotho-git-credentials"
	}

	envVars = append(envVars, corev1.EnvVar{
		Name: "GIT_TOKEN",
		ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: gitSecretName,
				},
				Key:      "token",
				Optional: boolPtr(true),
			},
		},
	})

	// For native pipelines, provide the Clotho SDK repo URL so the builder
	// can clone it alongside the source (path dependency resolution).
	if pipeline.Spec.Runtime == clothov1alpha1.PipelineRuntimeNative {
		envVars = append(envVars, corev1.EnvVar{
			Name:  "CLOTHO_SDK_REPO",
			Value: "https://github.com/brettnesbitt/clotho.git",
		})
		envVars = append(envVars, corev1.EnvVar{
			Name:  "CLOTHO_SDK_REF",
			Value: "clotho-ide",
		})
	}

	return envVars
}

// reconcileSchedule handles scheduled invocations of pipelines.
// For "interval" mode, it sends an HTTP request to the pipeline service at the configured interval.
// For "cron" mode, it calculates the next run time from a cron expression.
// For "trigger" mode (default), it does nothing.
func (r *PipelineReconciler) reconcileSchedule(ctx context.Context, pipeline *clothov1alpha1.Pipeline) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// No schedule configured = trigger mode (on-demand only)
	if pipeline.Spec.Schedule == nil || pipeline.Spec.Schedule.Mode == "trigger" {
		return ctrl.Result{}, nil
	}

	switch pipeline.Spec.Schedule.Mode {
	case "interval":
		return r.reconcileIntervalSchedule(ctx, pipeline)
	case "cron":
		log.Info("Cron scheduling not yet implemented, treating as trigger mode", "pipeline", pipeline.Name)
		return ctrl.Result{}, nil
	default:
		log.Info("Unknown schedule mode, ignoring", "mode", pipeline.Spec.Schedule.Mode)
		return ctrl.Result{}, nil
	}
}

// reconcileIntervalSchedule handles interval-based pipeline invocation.
func (r *PipelineReconciler) reconcileIntervalSchedule(ctx context.Context, pipeline *clothov1alpha1.Pipeline) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	interval, err := time.ParseDuration(pipeline.Spec.Schedule.Interval)
	if err != nil {
		log.Error(err, "Failed to parse schedule interval", "interval", pipeline.Spec.Schedule.Interval)
		return ctrl.Result{}, nil
	}

	now := time.Now()

	// Check if it's time to invoke
	if pipeline.Status.LastInvocation != nil {
		elapsed := now.Sub(pipeline.Status.LastInvocation.Time)
		if elapsed < interval {
			// Not yet time, requeue for the remaining duration
			remaining := interval - elapsed
			log.Info("Waiting for next invocation", "pipeline", pipeline.Name, "remaining", remaining.String())
			return ctrl.Result{RequeueAfter: remaining}, nil
		}
	}

	// Time to invoke the pipeline
	log.Info("Invoking pipeline on schedule", "pipeline", pipeline.Name, "interval", interval.String())

	// Set phase to Running during invocation
	pipeline.Status.Phase = "Running"
	if err := r.Status().Update(ctx, pipeline); err != nil {
		log.Error(err, "Failed to set Running phase")
		return ctrl.Result{}, err
	}

	invokeErr := r.invokePipeline(ctx, pipeline)

	// Refetch before updating status again to avoid conflict
	if err := r.Get(ctx, types.NamespacedName{Name: pipeline.Name, Namespace: pipeline.Namespace}, pipeline); err != nil {
		return ctrl.Result{}, err
	}

	// Restore phase to Enabled and update LastInvocation
	pipeline.Status.Phase = "Enabled"
	nowMeta := metav1.NewTime(now)
	pipeline.Status.LastInvocation = &nowMeta
	if err := r.Status().Update(ctx, pipeline); err != nil {
		log.Error(err, "Failed to update status after invocation")
		return ctrl.Result{}, err
	}

	if invokeErr != nil {
		log.Error(invokeErr, "Failed to invoke pipeline", "pipeline", pipeline.Name)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	log.Info("Pipeline invoked successfully", "pipeline", pipeline.Name, "nextIn", interval.String())
	return ctrl.Result{RequeueAfter: interval}, nil
}

// reconcileDAGPipeline handles DAG-based pipelines with multiple stages.
// Each stage becomes a separate Deployment/SpinApp, and the operator manages
// the inter-stage communication via the message bus.
func (r *PipelineReconciler) reconcileDAGPipeline(ctx context.Context, pipeline *clothov1alpha1.Pipeline) error {
	log := log.FromContext(ctx)

	log.Info("Reconciling DAG pipeline", "pipeline", pipeline.Name, "stages", len(pipeline.Spec.Stages))

	// Validate DAG structure
	if err := r.validateDAG(pipeline); err != nil {
		return fmt.Errorf("invalid DAG structure: %w", err)
	}

	// Create workloads for each stage
	for _, stage := range pipeline.Spec.Stages {
		if err := r.reconcileStage(ctx, pipeline, &stage); err != nil {
			return fmt.Errorf("failed to reconcile stage %s: %w", stage.Name, err)
		}
	}

	return nil
}

// validateDAG validates the DAG structure of a pipeline
func (r *PipelineReconciler) validateDAG(pipeline *clothov1alpha1.Pipeline) error {
	stageNames := make(map[string]bool)

	// Collect all stage names
	for _, stage := range pipeline.Spec.Stages {
		if stage.Name == "" {
			return fmt.Errorf("stage name cannot be empty")
		}
		if stageNames[stage.Name] {
			return fmt.Errorf("duplicate stage name: %s", stage.Name)
		}
		stageNames[stage.Name] = true
	}

	// Validate dependencies
	for _, stage := range pipeline.Spec.Stages {
		for _, dep := range stage.DependsOn {
			if !stageNames[dep] {
				return fmt.Errorf("stage %s depends on non-existent stage %s", stage.Name, dep)
			}
		}
	}

	// Check for cycles (simple DFS-based cycle detection)
	visited := make(map[string]bool)
	recStack := make(map[string]bool)

	var hasCycle func(string) bool
	hasCycle = func(stageName string) bool {
		visited[stageName] = true
		recStack[stageName] = true

		// Find the stage
		var stage *clothov1alpha1.PipelineStage
		for i := range pipeline.Spec.Stages {
			if pipeline.Spec.Stages[i].Name == stageName {
				stage = &pipeline.Spec.Stages[i]
				break
			}
		}

		if stage != nil {
			for _, dep := range stage.DependsOn {
				if !visited[dep] {
					if hasCycle(dep) {
						return true
					}
				} else if recStack[dep] {
					return true
				}
			}
		}

		recStack[stageName] = false
		return false
	}

	for _, stage := range pipeline.Spec.Stages {
		if !visited[stage.Name] {
			if hasCycle(stage.Name) {
				return fmt.Errorf("cycle detected in DAG")
			}
		}
	}

	return nil
}

// reconcileStage creates or updates a workload for a single stage.
// Unlike single-stage pipelines, stage workloads are owned by the real parent
// pipeline (which has a UID) rather than an ephemeral intermediate object.
func (r *PipelineReconciler) reconcileStage(ctx context.Context, pipeline *clothov1alpha1.Pipeline, stage *clothov1alpha1.PipelineStage) error {
	log := log.FromContext(ctx)

	stageName := fmt.Sprintf("%s-%s", pipeline.Name, stage.Name)
	log.Info("Reconciling stage", "stage", stageName, "entrypoint", stage.Entrypoint)

	// Merge stage config with pipeline config
	config := append([]clothov1alpha1.ConfigVar{}, pipeline.Spec.Config...)
	config = append(config, stage.Config...)

	// Inject entrypoint so the container knows which stage code to run
	config = append(config, clothov1alpha1.ConfigVar{
		Name:  "CLOTHO_STAGE_ENTRYPOINT",
		Value: stage.Entrypoint,
	})

	// Build a set of config var names already defined by the user in stage config
	userDefinedVars := make(map[string]bool)
	for _, cfg := range stage.Config {
		userDefinedVars[cfg.Name] = true
	}

	// Add bus configuration for inter-stage communication
	if len(stage.DependsOn) > 0 {
		// This stage reads from upstream stages (only inject if not user-defined)
		for _, dep := range stage.DependsOn {
			envName := fmt.Sprintf("CLOTHO_BUS_%s", strings.ToUpper(dep))
			if !userDefinedVars[envName] {
				busName := fmt.Sprintf("%s-%s-out", pipeline.Name, dep)
				config = append(config, clothov1alpha1.ConfigVar{
					Name:  envName,
					Value: busName,
				})
			}
		}
	}

	// Check if this stage produces output for downstream stages
	hasDownstream := false
	for _, otherStage := range pipeline.Spec.Stages {
		for _, dep := range otherStage.DependsOn {
			if dep == stage.Name {
				hasDownstream = true
				break
			}
		}
		if hasDownstream {
			break
		}
	}

	if hasDownstream && !userDefinedVars["CLOTHO_BUS_OUT"] {
		// This stage writes to a bus for downstream stages (only inject if not user-defined)
		busName := fmt.Sprintf("%s-%s-out", pipeline.Name, stage.Name)
		config = append(config, clothov1alpha1.ConfigVar{
			Name:  "CLOTHO_BUS_OUT",
			Value: busName,
		})
	}

	// Inject NATS URL from messageBus.clusterRef for inter-stage communication
	if pipeline.Spec.MessageBus != nil && pipeline.Spec.MessageBus.ClusterRef != "" && !userDefinedVars["CLOTHO_NATS_URL"] {
		natsURL := fmt.Sprintf("nats://%s.%s.svc.cluster.local:4222",
			pipeline.Spec.MessageBus.ClusterRef, "clotho-system")
		config = append(config, clothov1alpha1.ConfigVar{
			Name:  "CLOTHO_NATS_URL",
			Value: natsURL,
		})
	}

	// Determine resources for this stage
	resources := pipeline.Spec.Resources
	if stage.Resources != nil {
		resources = *stage.Resources
	}

	// Build a spec struct for the stage (used only for constructing the workload,
	// NOT as a K8s object — avoids the ephemeral-UID owner reference bug).
	stageSpec := clothov1alpha1.PipelineSpec{
		Runtime:   pipeline.Spec.Runtime,
		Mode:      pipeline.Spec.Mode,
		Image:     pipeline.Spec.Image,
		Config:    config,
		Resources: resources,
		Replicas:  stage.Replicas,
		Schedule:  stage.Schedule,
	}

	// Deploy based on runtime — owner reference is always the real parent pipeline
	switch pipeline.Spec.Runtime {
	case clothov1alpha1.PipelineRuntimeNative:
		deploy := r.constructDeployment(&clothov1alpha1.Pipeline{
			ObjectMeta: metav1.ObjectMeta{
				Name:      stageName,
				Namespace: pipeline.Namespace,
			},
			Spec: stageSpec,
		})
		// Override the container command to run the specific stage binary.
		// The crane-built image places all binaries under /app/.
		if stage.Entrypoint != "" && len(deploy.Spec.Template.Spec.Containers) > 0 {
			deploy.Spec.Template.Spec.Containers[0].Command = []string{fmt.Sprintf("/app/%s", stage.Entrypoint)}
		}
		// Inject CLOTHO_STAGE_NAME so the SDK can tag step metrics with the stage.
		// CLOTHO_PIPELINE_ID stays as the stage workload name (e.g. bluesky-sieve-ingestor)
		// to keep per-stage telemetry_state docs separate and avoid write races.
		// The API aggregates stage docs under the parent pipeline at query time.
		if len(deploy.Spec.Template.Spec.Containers) > 0 {
			deploy.Spec.Template.Spec.Containers[0].Env = append(
				deploy.Spec.Template.Spec.Containers[0].Env,
				corev1.EnvVar{Name: "CLOTHO_STAGE_NAME", Value: stage.Name},
				corev1.EnvVar{Name: "CLOTHO_PARENT_PIPELINE", Value: pipeline.Name},
			)
		}
		// Owner reference to the REAL parent pipeline (has a UID)
		if err := ctrl.SetControllerReference(pipeline, deploy, r.Scheme); err != nil {
			return err
		}
		found := &appsv1.Deployment{}
		err := r.Get(ctx, types.NamespacedName{Name: deploy.Name, Namespace: deploy.Namespace}, found)
		if err != nil && errors.IsNotFound(err) {
			log.Info("Creating stage Deployment", "stage", stageName)
			return r.Create(ctx, deploy)
		} else if err != nil {
			return err
		}
		found.Spec.Replicas = deploy.Spec.Replicas
		found.Spec.Template = deploy.Spec.Template
		return r.Update(ctx, found)

	default:
		spinApp := r.constructSpinApp(&clothov1alpha1.Pipeline{
			ObjectMeta: metav1.ObjectMeta{
				Name:      stageName,
				Namespace: pipeline.Namespace,
			},
			Spec: stageSpec,
		})
		// Inject CLOTHO_STAGE_NAME so the SDK can tag step metrics with the stage.
		// CLOTHO_PIPELINE_ID stays as the stage workload name to avoid telemetry_state races.
		spinApp.Spec.Variables = append(spinApp.Spec.Variables,
			spinva1.SpinVar{Name: "CLOTHO_STAGE_NAME", Value: stage.Name},
			spinva1.SpinVar{Name: "CLOTHO_PARENT_PIPELINE", Value: pipeline.Name},
		)
		// Owner reference to the REAL parent pipeline (has a UID)
		if err := ctrl.SetControllerReference(pipeline, spinApp, r.Scheme); err != nil {
			return err
		}
		return retry.RetryOnConflict(retry.DefaultRetry, func() error {
			found := &spinva1.SpinApp{}
			err := r.Get(ctx, types.NamespacedName{Name: spinApp.Name, Namespace: spinApp.Namespace}, found)
			if err != nil && errors.IsNotFound(err) {
				log.Info("Creating stage SpinApp", "stage", stageName)
				return r.Create(ctx, spinApp)
			} else if err != nil {
				return err
			}
			found.Spec = spinApp.Spec
			return r.Update(ctx, found)
		})
	}
}

// invokePipeline sends an HTTP request to the pipeline's Kubernetes service.
// The SpinApp exposes an HTTP endpoint; the operator triggers it.
// If the response contains an execution report (x-clotho-execution header),
// the operator forwards it to the Control Plane API.
func (r *PipelineReconciler) invokePipeline(ctx context.Context, pipeline *clothov1alpha1.Pipeline) error {
	log := log.FromContext(ctx)

	// Build the in-cluster service URL
	// SpinApp services are created by SpinKube with the same name as the SpinApp
	serviceURL := fmt.Sprintf("http://%s.%s.svc.cluster.local", pipeline.Name, pipeline.Namespace)

	httpClient := &http.Client{Timeout: 30 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, serviceURL, nil)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("X-Clotho-Trigger", "schedule")
	req.Header.Set("X-Clotho-Pipeline", pipeline.Name)

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("invoking pipeline: %w", err)
	}
	defer resp.Body.Close()

	// Read response body
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode >= 400 {
		log.Info("Pipeline returned error", "status", resp.StatusCode, "body", string(body))
		return fmt.Errorf("pipeline returned HTTP %d", resp.StatusCode)
	}

	// Forward execution report to Control Plane API if present
	if resp.Header.Get("X-Clotho-Execution") == "true" && len(body) > 0 {
		apiURL := r.ControlPlaneURL
		if apiURL == "" {
			apiURL = os.Getenv("CLOTHO_CONTROL_PLANE_URL")
		}
		if apiURL != "" {
			// Inject pipeline mode from the CR spec into the report JSON.
			// This ensures the API knows how to route storage even if the SDK omitted it.
			mode := string(pipeline.Spec.Mode)
			if mode == "" {
				mode = "stream"
			}
			var report map[string]interface{}
			if err := json.Unmarshal(body, &report); err == nil {
				report["mode"] = mode
				if enriched, err := json.Marshal(report); err == nil {
					body = enriched
				}
			}

			execURL := apiURL + "/v1/executions"
			execReq, err := http.NewRequestWithContext(ctx, http.MethodPost, execURL, bytes.NewReader(body))
			if err == nil {
				execReq.Header.Set("Content-Type", "application/json")
				execResp, err := httpClient.Do(execReq)
				if err != nil {
					log.Error(err, "Failed to forward execution report to API")
				} else {
					execResp.Body.Close()
					log.Info("Forwarded execution report to API", "pipeline", pipeline.Name, "mode", mode, "status", execResp.StatusCode)
				}
			}
		} else {
			log.Info("Execution report available but no Control Plane URL configured", "pipeline", pipeline.Name)
		}
	}

	return nil
}
