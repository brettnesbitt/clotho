package phonehome

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	pollInterval     = 10 * time.Second
	handshakeRetry   = 5 * time.Second
	httpTimeout      = 10 * time.Second
)

// Client implements manager.Runnable so it can be added to the controller-manager.
// It handles the outbound "Phone Home" connection to the Clotho SaaS Control Plane.
type Client struct {
	K8sClient    client.Client
	Log          logr.Logger
	ControlPlane string // e.g. "https://api.clotho.io" or "http://clotho-api.clotho-control.svc.cluster.local:3000"
	APIKey       string // Raw API key from CLOTHO_API_KEY secret
	ClusterName  string
}

// NewFromEnv creates a Phone Home client from environment variables.
// Returns nil if CLOTHO_API_KEY is not set (Phone Home disabled).
func NewFromEnv(k8sClient client.Client, log logr.Logger) *Client {
	apiKey := os.Getenv("CLOTHO_API_KEY")
	if apiKey == "" {
		return nil
	}

	controlPlane := os.Getenv("CLOTHO_CONTROL_PLANE_URL")
	if controlPlane == "" {
		controlPlane = "https://api.clotho.io"
	}

	clusterName := os.Getenv("CLOTHO_CLUSTER_NAME")
	if clusterName == "" {
		clusterName = "default"
	}

	return &Client{
		K8sClient:    k8sClient,
		Log:          log.WithName("phone-home"),
		ControlPlane: controlPlane,
		APIKey:       apiKey,
		ClusterName:  clusterName,
	}
}

// Start implements manager.Runnable. Called by the controller-manager after leader election.
func (c *Client) Start(ctx context.Context) error {
	log := c.Log
	log.Info("Starting Phone Home tunnel", "controlPlane", c.ControlPlane, "cluster", c.ClusterName)

	// 1. Handshake with retry
	for {
		if err := c.handshake(ctx); err != nil {
			log.Error(err, "Handshake failed, retrying", "retryIn", handshakeRetry)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(handshakeRetry):
				continue
			}
		}
		log.Info("Handshake successful")
		break
	}

	// 2. Poll loop
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info("Phone Home tunnel shutting down")
			return nil
		case <-ticker.C:
			if err := c.pollAndApply(ctx); err != nil {
				log.Error(err, "Poll cycle failed")
			}
		}
	}
}

// --- Handshake ---

type handshakeRequest struct {
	ClusterName  string `json:"cluster_name"`
	AgentVersion string `json:"agent_version"`
}

type handshakeResponse struct {
	Status         string `json:"status"`
	TenantID       string `json:"tenant_id"`
	PendingCommands int   `json:"pending_commands"`
}

func (c *Client) handshake(ctx context.Context) error {
	body, _ := json.Marshal(handshakeRequest{
		ClusterName:  c.ClusterName,
		AgentVersion: "operator-v0.1",
	})

	resp, err := c.doRequest(ctx, "POST", "/agent/handshake", body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 401 {
		return fmt.Errorf("authentication failed: invalid API key")
	}
	if resp.StatusCode != 200 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("handshake returned %d: %s", resp.StatusCode, string(respBody))
	}

	var result handshakeResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decoding handshake response: %w", err)
	}

	c.Log.Info("Connected to Control Plane",
		"tenant", result.TenantID,
		"pending", result.PendingCommands,
	)

	return nil
}

// --- Poll + Apply ---

type commandsResponse struct {
	Commands []command `json:"commands"`
	Count    int       `json:"count"`
}

type command struct {
	ID           int64  `json:"id"`
	CommandType  string `json:"command_type"`
	ResourceName string `json:"resource_name"`
	Namespace    string `json:"namespace"`
	Payload      string `json:"payload"`
}

type ackRequest struct {
	Status   string `json:"status"`
	ErrorMsg string `json:"error_msg,omitempty"`
}

func (c *Client) pollAndApply(ctx context.Context) error {
	// GET /agent/commands
	resp, err := c.doRequest(ctx, "GET", "/agent/commands", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 401 {
		return fmt.Errorf("authentication failed on poll")
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("poll returned %d", resp.StatusCode)
	}

	var result commandsResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decoding commands: %w", err)
	}

	if result.Count == 0 {
		return nil
	}

	c.Log.Info("Received commands from Control Plane", "count", result.Count)

	for _, cmd := range result.Commands {
		applyErr := c.applyCommand(ctx, cmd)

		ack := ackRequest{Status: "applied"}
		if applyErr != nil {
			ack.Status = "failed"
			ack.ErrorMsg = applyErr.Error()
			c.Log.Error(applyErr, "Failed to apply command", "id", cmd.ID, "type", cmd.CommandType, "resource", cmd.ResourceName)
		} else {
			c.Log.Info("Applied command", "id", cmd.ID, "type", cmd.CommandType, "resource", cmd.ResourceName)
		}

		// ACK the command
		ackBody, _ := json.Marshal(ack)
		ackResp, err := c.doRequest(ctx, "POST", fmt.Sprintf("/agent/commands/%d/ack", cmd.ID), ackBody)
		if err != nil {
			c.Log.Error(err, "Failed to ack command", "id", cmd.ID)
		} else {
			ackResp.Body.Close()
		}
	}

	return nil
}

// applyCommand applies a single command to the local K8s cluster.
func (c *Client) applyCommand(ctx context.Context, cmd command) error {
	switch cmd.CommandType {
	case "apply":
		return c.applyYAML(ctx, cmd)
	case "delete":
		return c.deleteResource(ctx, cmd)
	default:
		return fmt.Errorf("unknown command type: %s", cmd.CommandType)
	}
}

// applyYAML takes a JSON-encoded Pipeline CR and applies it to the cluster.
func (c *Client) applyYAML(ctx context.Context, cmd command) error {
	// Parse the payload as unstructured K8s object
	obj := &unstructured.Unstructured{}
	if err := json.Unmarshal([]byte(cmd.Payload), &obj.Object); err != nil {
		return fmt.Errorf("parsing payload: %w", err)
	}

	// Set namespace from command if not already set
	if obj.GetNamespace() == "" {
		obj.SetNamespace(cmd.Namespace)
	}

	// Try to get existing resource
	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(obj.GroupVersionKind())
	key := types.NamespacedName{Name: obj.GetName(), Namespace: obj.GetNamespace()}

	err := c.K8sClient.Get(ctx, key, existing)
	if err != nil {
		// Resource doesn't exist — create it
		if err := c.K8sClient.Create(ctx, obj); err != nil {
			return fmt.Errorf("creating %s/%s: %w", obj.GetKind(), obj.GetName(), err)
		}
		c.Log.Info("Created resource", "kind", obj.GetKind(), "name", obj.GetName(), "namespace", obj.GetNamespace())
		return nil
	}

	// Resource exists — update spec
	obj.SetResourceVersion(existing.GetResourceVersion())
	if err := c.K8sClient.Update(ctx, obj); err != nil {
		return fmt.Errorf("updating %s/%s: %w", obj.GetKind(), obj.GetName(), err)
	}
	c.Log.Info("Updated resource", "kind", obj.GetKind(), "name", obj.GetName(), "namespace", obj.GetNamespace())
	return nil
}

// deleteResource removes a resource from the cluster.
func (c *Client) deleteResource(ctx context.Context, cmd command) error {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "core.clotho.run",
		Version: "v1alpha1",
		Kind:    "Pipeline",
	})
	obj.SetName(cmd.ResourceName)
	obj.SetNamespace(cmd.Namespace)

	if err := c.K8sClient.Delete(ctx, obj); err != nil {
		return fmt.Errorf("deleting %s/%s: %w", cmd.ResourceName, cmd.Namespace, err)
	}
	c.Log.Info("Deleted resource", "name", cmd.ResourceName, "namespace", cmd.Namespace)
	return nil
}

// --- HTTP Helper ---

func (c *Client) doRequest(ctx context.Context, method, path string, body []byte) (*http.Response, error) {
	url := c.ControlPlane + path

	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("building request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.APIKey)
	req.Header.Set("Content-Type", "application/json")

	httpClient := &http.Client{Timeout: httpTimeout}
	return httpClient.Do(req)
}
