package main

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/logger"
)

// ═══════════════════════════════════════════════════════════════════════════════
// Clotho API v2.0 — MongoDB via Data Proxy (replaces SQLite)
// ═══════════════════════════════════════════════════════════════════════════════

// ── Data Proxy Client ─────────────────────────────────────────────────────────

// DataProxyClient wraps HTTP calls to the clotho-data-proxy service.
type DataProxyClient struct {
	BaseURL  string
	Database string
	HTTP     *http.Client
}

func NewDataProxyClient() (*DataProxyClient, error) {
	baseURL := os.Getenv("DATA_PROXY_URL")
	if baseURL == "" {
		baseURL = "http://clotho-data-proxy.clotho-system.svc.cluster.local:9090"
	}
	db := os.Getenv("MONGO_DB")
	if db == "" {
		db = "clotho"
	}
	c := &DataProxyClient{
		BaseURL:  baseURL,
		Database: db,
		HTTP:     &http.Client{Timeout: 10 * time.Second},
	}
	// Verify connectivity
	if err := c.Ping(); err != nil {
		return nil, fmt.Errorf("data proxy unreachable: %w", err)
	}
	log.Printf("[mongo] Connected to data proxy at %s (db=%s)", baseURL, db)
	return c, nil
}

func (dp *DataProxyClient) Ping() error {
	resp, err := dp.HTTP.Get(dp.BaseURL + "/healthz")
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("healthz returned %d", resp.StatusCode)
	}
	return nil
}

// ── Generic helpers ───────────────────────────────────────────────────────────

func (dp *DataProxyClient) buildURL(collection, path string, queryParams url.Values) string {
	u := fmt.Sprintf("%s/v1/data/%s", dp.BaseURL, collection)
	if path != "" {
		u += "/" + path
	}
	if len(queryParams) > 0 {
		u += "?" + queryParams.Encode()
	}
	return u
}

func (dp *DataProxyClient) get(collection string, params string, out interface{}) error {
	q := url.Values{}
	if params != "" {
		// Parse raw query string and add to values
		parsed, _ := url.ParseQuery(params)
		for k, vs := range parsed {
			for _, v := range vs {
				q.Add(k, v)
			}
		}
	}
	resp, err := dp.HTTP.Get(dp.buildURL(collection, "", q))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(out)
}

func (dp *DataProxyClient) post(collection string, body interface{}, out interface{}) error {
	data, _ := json.Marshal(body)
	resp, err := dp.HTTP.Post(dp.buildURL(collection, "", nil), "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(out)
}

func (dp *DataProxyClient) postPath(collection, path string, body interface{}, out interface{}) error {
	data, _ := json.Marshal(body)
	resp, err := dp.HTTP.Post(dp.buildURL(collection, path, nil), "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(out)
}

func (dp *DataProxyClient) deletePath(collection, path string) error {
	req, _ := http.NewRequest("DELETE", dp.buildURL(collection, path, nil), nil)
	resp, err := dp.HTTP.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// DataResponse matches the data proxy's JSON shape
type DataResponse struct {
	Ok    bool            `json:"ok"`
	Data  json.RawMessage `json:"data,omitempty"`
	Count *int64          `json:"count,omitempty"`
	Error string          `json:"error,omitempty"`
}

// ── Collection-level helpers ──────────────────────────────────────────────────

// findDocs queries a collection with optional filter/sort/limit.
func (dp *DataProxyClient) findDocs(collection string, filter map[string]interface{}, sort map[string]interface{}, limit, skip int64) ([]map[string]interface{}, error) {
	q := url.Values{}
	if filter != nil {
		b, _ := json.Marshal(filter)
		q.Set("filter", string(b))
	}
	if sort != nil {
		b, _ := json.Marshal(sort)
		q.Set("sort", string(b))
	}
	if limit > 0 {
		q.Set("limit", fmt.Sprintf("%d", limit))
	}
	if skip > 0 {
		q.Set("skip", fmt.Sprintf("%d", skip))
	}
	httpResp, err := dp.HTTP.Get(dp.buildURL(collection, "", q))
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()
	var dataResp DataResponse
	if err := json.NewDecoder(httpResp.Body).Decode(&dataResp); err != nil {
		return nil, err
	}
	if dataResp.Error != "" {
		return nil, fmt.Errorf("data proxy error: %s", dataResp.Error)
	}
	if dataResp.Data == nil {
		return nil, nil
	}
	var docs []map[string]interface{}
	if err := json.Unmarshal(dataResp.Data, &docs); err != nil {
		return nil, err
	}
	return docs, nil
}

// findOneDoc fetches a single document by _id.
func (dp *DataProxyClient) findOneDoc(collection, id string) (map[string]interface{}, error) {
	filter := map[string]interface{}{"_id": id}
	docs, err := dp.findDocs(collection, filter, nil, 1, 0)
	if err != nil {
		return nil, err
	}
	if len(docs) == 0 {
		return nil, nil
	}
	return docs[0], nil
}

// insertOne inserts a single document.
func (dp *DataProxyClient) insertOne(collection string, doc map[string]interface{}) error {
	var resp DataResponse
	if err := dp.post(collection, doc, &resp); err != nil {
		return err
	}
	if resp.Error != "" {
		return fmt.Errorf("insert error: %s", resp.Error)
	}
	return nil
}

// upsertByID upserts a document by _id using the data proxy's upsert endpoint.
func (dp *DataProxyClient) upsertByID(collection, id string, doc map[string]interface{}) error {
	var resp DataResponse
	if err := dp.postPath(collection, id+"/upsert", doc, &resp); err != nil {
		return err
	}
	if resp.Error != "" {
		return fmt.Errorf("upsert error: %s", resp.Error)
	}
	return nil
}

// updateByID updates a document by _id with $set.
func (dp *DataProxyClient) updateByID(collection, id string, update map[string]interface{}) error {
	body := map[string]interface{}{"$set": update}
	var resp DataResponse
	if err := dp.postPath(collection, id, body, &resp); err != nil {
		return err
	}
	if resp.Error != "" {
		return fmt.Errorf("update error: %s", resp.Error)
	}
	return nil
}

// deleteByID deletes a document by _id.
func (dp *DataProxyClient) deleteByID(collection, id string) error {
	return dp.deletePath(collection, id)
}

// countDocs returns the count of documents matching an optional filter.
func (dp *DataProxyClient) countDocs(collection string, filter map[string]interface{}) (int64, error) {
	q := url.Values{}
	if filter != nil {
		b, _ := json.Marshal(filter)
		q.Set("filter", string(b))
	}
	httpResp, err := dp.HTTP.Get(dp.buildURL(collection+"/count", "", q))
	if err != nil {
		// Fallback: query and count manually
		docs, err := dp.findDocs(collection, filter, nil, 1, 0)
		if err != nil {
			return 0, err
		}
		return int64(len(docs)), nil
	}
	defer httpResp.Body.Close()
	var countResp DataResponse
	if err := json.NewDecoder(httpResp.Body).Decode(&countResp); err != nil {
		return 0, err
	}
	if countResp.Count != nil {
		return *countResp.Count, nil
	}
	return 0, nil
}

// aggregate runs a MongoDB aggregation pipeline.
func (dp *DataProxyClient) aggregate(collection string, pipeline []map[string]interface{}) ([]map[string]interface{}, error) {
	body := map[string]interface{}{"pipeline": pipeline}
	var resp DataResponse
	if err := dp.postPath(collection, "aggregate", body, &resp); err != nil {
		return nil, err
	}
	if resp.Error != "" {
		return nil, fmt.Errorf("aggregate error: %s", resp.Error)
	}
	if resp.Data == nil {
		return nil, nil
	}
	var results []map[string]interface{}
	if err := json.Unmarshal(resp.Data, &results); err != nil {
		return nil, err
	}
	return results, nil
}

// updateMany updates multiple documents.
func (dp *DataProxyClient) updateMany(collection string, filter, update map[string]interface{}) error {
	body := map[string]interface{}{"filter": filter, "update": update}
	var resp DataResponse
	if err := dp.postPath(collection, "update-many", body, &resp); err != nil {
		return err
	}
	if resp.Error != "" {
		return fmt.Errorf("updateMany error: %s", resp.Error)
	}
	return nil
}

// deleteMany deletes multiple documents.
func (dp *DataProxyClient) deleteMany(collection string, filter map[string]interface{}) error {
	body := map[string]interface{}{"filter": filter}
	var resp DataResponse
	if err := dp.postPath(collection, "delete-many", body, &resp); err != nil {
		return err
	}
	if resp.Error != "" {
		return fmt.Errorf("deleteMany error: %s", resp.Error)
	}
	return nil
}

// ── Collection constants ──────────────────────────────────────────────────────

const (
	colTelemetryState  = "telemetry_state"
	colEvents          = "events"
	colDlqRecords      = "dlq_records"
	colExecutions      = "executions"
	colMetricsBuckets  = "metrics_buckets"
	colLifecycleEvents = "lifecycle_events"
	colBuildHistory    = "build_history"
	colPipelines       = "pipelines"
	colApiKeys         = "api_keys"
	colClusters        = "clusters"
	colCommandQueue    = "command_queue"
	colStepMetrics     = "step_metrics"
)

// ── AgentPayload is the JSON payload from the Rust Agent ──────────────────────
type AgentPayload struct {
	PipelineID string           `json:"pipeline_id"`
	Events     []TelemetryEvent `json:"events"`
	Stats      *ResourceStats   `json:"stats,omitempty"`
}

// TelemetryEvent represents a single telemetry event
type TelemetryEvent struct {
	Type      string                 `json:"type"`
	Timestamp int64                  `json:"timestamp"`
	Payload   map[string]interface{} `json:"payload"`
}

// ResourceStats contains resource utilization metrics
type ResourceStats struct {
	CpuNano  int64 `json:"cpu_nano"`
	MemBytes int64 `json:"mem_bytes"`
}

// PipelineStage represents a single stage in a pipeline
type PipelineStage struct {
	Name       string   `json:"name"`
	Entrypoint string   `json:"entrypoint"`
	Replicas   int      `json:"replicas"`
	DependsOn  []string `json:"dependsOn,omitempty"`
}

// Pipeline represents a pipeline configuration and state (merged K8s + telemetry)
type Pipeline struct {
	ID               string          `json:"id"`
	Environment      string          `json:"environment"`
	Mode             string          `json:"mode"`
	Status           string          `json:"status"`
	Phase            string          `json:"phase"`
	Image            string          `json:"image"`
	GitRepository    string          `json:"git_repository,omitempty"`
	GitRef           string          `json:"git_ref,omitempty"`
	Path             string          `json:"path,omitempty"`
	DesiredReplicas  int             `json:"desired_replicas"`
	CreatedAt        string          `json:"created_at"`
	UptimeMs         int64           `json:"uptime_ms,omitempty"`
	ProgressCurrent  int64           `json:"progress_current,omitempty"`
	ProgressTotal    int64           `json:"progress_total,omitempty"`
	ProgressPercent  float64         `json:"progress_percent,omitempty"`
	CPUMillicores    int64           `json:"cpu_millicores"`
	MemoryBytes      int64           `json:"memory_bytes"`
	LastSeen         string          `json:"last_seen,omitempty"`
	LastInvocation   string          `json:"last_invocation,omitempty"`
	RecordsIn        int64           `json:"records_in"`
	RecordsOut       int64           `json:"records_out"`
	RecordsFailed    int64           `json:"records_failed"`
	RecordsFiltered  int64           `json:"records_filtered"`
	BytesProcessed   int64           `json:"bytes_processed"`
	RecordsPerSec    float64         `json:"records_per_sec,omitempty"`
	PodSummary       *PodSummary     `json:"pod_summary,omitempty"`
	LastBuild        *BuildSummary   `json:"last_build,omitempty"`
	ExecStats        *ExecutionStats `json:"exec_stats,omitempty"`
	Stages           []PipelineStage `json:"stages,omitempty"`
	MessageBusType   string          `json:"message_bus_type,omitempty"`
	ErrorMessage     string          `json:"error_message,omitempty"`
	SdkVersion       string          `json:"sdk_version,omitempty"`
	LatestSdkVersion string          `json:"latest_sdk_version,omitempty"`
	HasBuildConfig   bool            `json:"has_build_config"`
}

// ExecutionStats contains aggregated execution metrics for a pipeline
type ExecutionStats struct {
	TotalRuns  int64   `json:"total_runs"`
	AvgRuntime int64   `json:"avg_runtime_ms"`
	MaxRuntime int64   `json:"max_runtime_ms"`
	P50Runtime int64   `json:"p50_runtime_ms"`
	P99Runtime int64   `json:"p99_runtime_ms"`
	Failures   int64   `json:"failures"`
	FailRate   float64 `json:"fail_rate"`
	LastRun    string  `json:"last_run,omitempty"`
}

// PodSummary gives a quick rollup of pod health
type PodSummary struct {
	Total    int `json:"total"`
	Ready    int `json:"ready"`
	Crashing int `json:"crashing"`
}

// BuildSummary gives the last build job status
type BuildSummary struct {
	Status         string `json:"status"`
	StartTime      string `json:"start_time,omitempty"`
	CompletionTime string `json:"completion_time,omitempty"`
	DurationSec    int64  `json:"duration_sec,omitempty"`
}

// DlqRecord is a dead-letter queue entry for failed pipeline payloads
type DlqRecord struct {
	ID         string `json:"id"`
	PipelineID string `json:"pipeline_id"`
	TraceID    string `json:"trace_id"`
	Error      string `json:"error"`
	Step       string `json:"step"`
	Payload    string `json:"payload"`
	Status     string `json:"status"` // pending, replayed, dismissed
	CreatedAt  string `json:"created_at"`
	ReplayedAt string `json:"replayed_at,omitempty"`
}

// ── In-Memory Storage for Step Telemetry (Live Debugging) ───────
type StepMetrics struct {
	PipelineID      string `json:"pipeline_id"`
	StageName       string `json:"stage_name"`
	StepName        string `json:"step_name"`
	StepType        string `json:"step_type"`
	RecordsIn       int64  `json:"records_in"`
	RecordsOut      int64  `json:"records_out"`
	RecordsFailed   int64  `json:"records_failed"`
	RecordsFiltered int64  `json:"records_filtered"`
	RecordsBranched int64  `json:"records_branched"`
	DurationMs      int64  `json:"duration_ms"`
	Timestamp       int64  `json:"timestamp"`
}

type DataSample struct {
	PipelineID string `json:"pipeline_id"`
	StageName  string `json:"stage_name"`
	StepName   string `json:"step_name"`
	PayloadIn  string `json:"payload_in"`
	PayloadOut string `json:"payload_out"`
	Timestamp  int64  `json:"timestamp"`
}

var (
	stepMetricsMu sync.RWMutex
	// map[pipeline_id]map[step_name]*StepMetrics
	stepMetricsStore = make(map[string]map[string]*StepMetrics)

	dataSampleMu sync.RWMutex
	// map[pipeline_id]map[step_name]*DataSample (latest sample per step)
	dataSampleStore = make(map[string]map[string]*DataSample)
)

func main() {
	// 1. Init Data Proxy Client
	dp, err := NewDataProxyClient()
	if err != nil {
		log.Fatalf("Failed to connect to data proxy: %v", err)
	}

	// 1b. Seed in-memory step metrics store from MongoDB (survives API restarts)
	seedStepMetricsFromMongo(dp)

	// 2. Init K8s Client (optional - graceful fallback if not in-cluster)
	k8s, err := NewK8sClient()
	if err != nil {
		log.Printf("K8s client not available (local mode): %v", err)
	}

	// ── Environment → Namespace mapping ─────────────────────────────────
	envNamespaceMap := map[string]string{
		"production": envOrDefault("CLOTHO_NS_PRODUCTION", "clotho-prod"),
		"preview":    envOrDefault("CLOTHO_NS_PREVIEW", "clotho-preview"),
	}
	nsEnvMap := make(map[string]string)
	for env, ns := range envNamespaceMap {
		nsEnvMap[ns] = env
	}

	resolveNS := func(env string) string {
		if ns, ok := envNamespaceMap[env]; ok {
			return ns
		}
		return envNamespaceMap["production"]
	}
	resolveEnv := func(ns string) string {
		if env, ok := nsEnvMap[ns]; ok {
			return env
		}
		return "production"
	}
	_ = resolveEnv // used in pipeline response mapping

	defaultEnv := "production"

	// 3. Init Server
	app := fiber.New(fiber.Config{
		AppName: "Clotho API v2.0 (MongoDB)",
	})
	app.Use(logger.New())
	app.Use(cors.New())

	// --- INGESTION ENDPOINT (Called by Agent) ---
	app.Post("/v1/telemetry", func(c *fiber.Ctx) error {
		var payload AgentPayload
		if err := c.BodyParser(&payload); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": err.Error()})
		}

		// A. Compute THROUGHPUT deltas BEFORE updateHeartbeat overwrites telemetry_state.
		for _, event := range payload.Events {
			if event.Type != "THROUGHPUT" {
				continue
			}
			cumIn, _ := event.Payload["records_in"].(float64)
			cumOut, _ := event.Payload["records_out"].(float64)
			cumFailed, _ := event.Payload["records_failed"].(float64)
			cumFiltered, _ := event.Payload["records_filtered"].(float64)
			cumBytes, _ := event.Payload["bytes_processed"].(float64)

			// Read previous cumulative values from telemetry_state
			prevIn, prevOut, prevFailed, prevFiltered, prevBytes := getTelemetryCounters(dp, payload.PipelineID)

			// Counter-reset detection
			if int64(cumIn) < prevIn {
				log.Printf("Counter reset detected for %s (cum=%d < prev=%d), skipping delta", payload.PipelineID, int64(cumIn), prevIn)
				continue
			}

			dIn := max64(int64(cumIn)-prevIn, 0)
			dOut := max64(int64(cumOut)-prevOut, 0)
			dFailed := max64(int64(cumFailed)-prevFailed, 0)
			dFiltered := max64(int64(cumFiltered)-prevFiltered, 0)
			dBytes := max64(int64(cumBytes)-prevBytes, 0)

			bucketTs := truncateToMinute(time.Unix(event.Timestamp, 0).UTC().Format(time.RFC3339))
			upsertMetricsBucket(dp, payload.PipelineID, bucketTs, dIn, dOut, dFailed, dFiltered, dBytes)
		}

		// B. Update "Liveness" State
		if err := updateHeartbeatMongo(dp, payload); err != nil {
			log.Printf("Failed to update heartbeat: %v", err)
		}

		// C. Log Events + Route to specialized tables
		for _, event := range payload.Events {
			if err := logEventMongo(dp, payload.PipelineID, event); err != nil {
				log.Printf("Failed to log event: %v", err)
			}

			switch event.Type {
			case "STEP_METRICS":
				// Parse payload to StepMetrics and store in RAM + MongoDB
				b, err := json.Marshal(event.Payload)
				if err == nil {
					var sm StepMetrics
					if err := json.Unmarshal(b, &sm); err == nil {
						stepMetricsMu.Lock()
						if _, ok := stepMetricsStore[payload.PipelineID]; !ok {
							stepMetricsStore[payload.PipelineID] = make(map[string]*StepMetrics)
						}
						// Merge cumulative metrics
						if existing, ok := stepMetricsStore[payload.PipelineID][sm.StepName]; ok {
							existing.RecordsIn += sm.RecordsIn
							existing.RecordsOut += sm.RecordsOut
							existing.RecordsFailed += sm.RecordsFailed
							existing.RecordsFiltered += sm.RecordsFiltered
							existing.RecordsBranched += sm.RecordsBranched
							existing.DurationMs = sm.DurationMs // latest duration
							existing.Timestamp = sm.Timestamp
							upsertStepMetricsMongo(dp, payload.PipelineID, existing)
						} else {
							stepMetricsStore[payload.PipelineID][sm.StepName] = &sm
							upsertStepMetricsMongo(dp, payload.PipelineID, &sm)
						}
						stepMetricsMu.Unlock()
					}
				}

			case "DATA_SAMPLE":
				// Parse payload to DataSample and store latest in RAM
				b, err := json.Marshal(event.Payload)
				if err == nil {
					var ds DataSample
					if err := json.Unmarshal(b, &ds); err == nil {
						dataSampleMu.Lock()
						if _, ok := dataSampleStore[payload.PipelineID]; !ok {
							dataSampleStore[payload.PipelineID] = make(map[string]*DataSample)
						}
						dataSampleStore[payload.PipelineID][ds.StepName] = &ds
						dataSampleMu.Unlock()
					}
				}

			case "DLQ":
				traceID, _ := event.Payload["trace_id"].(string)
				errMsg, _ := event.Payload["error"].(string)
				step, _ := event.Payload["step"].(string)
				dlqPayload, _ := event.Payload["payload"].(string)
				if err := insertDlqRecordMongo(dp, payload.PipelineID, traceID, errMsg, step, dlqPayload); err != nil {
					log.Printf("Failed to insert DLQ record: %v", err)
				}

			case "LIFECYCLE":
				eventName, _ := event.Payload["event"].(string)
				version, _ := event.Payload["version"].(string)
				message := ""
				if runtimeMs, ok := event.Payload["runtime_ms"].(float64); ok && runtimeMs > 0 {
					message = fmt.Sprintf("runtime: %dms", int64(runtimeMs))
				}
				if bootMs, ok := event.Payload["boot_latency_ms"].(float64); ok && bootMs > 0 {
					if message != "" {
						message += ", "
					}
					message += fmt.Sprintf("boot: %dms", int64(bootMs))
				}
				insertLifecycleEvent(dp, payload.PipelineID, eventName, version, message)
			}
		}

		return c.JSON(fiber.Map{"status": "ok"})
	})

	// --- EXECUTION INGESTION (Called by SDK directly or Agent forwarding) ---
	app.Post("/v1/executions", func(c *fiber.Ctx) error {
		var record struct {
			PipelineID     string   `json:"pipeline_id"`
			Mode           string   `json:"mode"`
			StartedAt      string   `json:"started_at"`
			DurationMs     int64    `json:"duration_ms"`
			Status         string   `json:"status"`
			RecordsIn      int64    `json:"records_in"`
			RecordsOut     int64    `json:"records_out"`
			RecordsFailed  int64    `json:"records_failed"`
			BytesProcessed int64    `json:"bytes_processed"`
			LogLines       []string `json:"log_lines"`
		}
		if err := c.BodyParser(&record); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": err.Error()})
		}
		if record.PipelineID == "" {
			return c.Status(400).JSON(fiber.Map{"error": "pipeline_id required"})
		}

		if record.StartedAt == "" {
			record.StartedAt = time.Now().UTC().Format(time.RFC3339)
		}
		if record.Status == "" {
			record.Status = "completed"
		}
		record.Mode = strings.ToLower(strings.TrimSpace(record.Mode))
		if record.Mode == "" {
			record.Mode = "once"
		}
		if record.Mode != "stream" && record.Mode != "once" && record.Mode != "batch" {
			record.Mode = "once"
		}

		log.Printf("[exec] SDK report: pipeline=%s mode=%s duration=%dms in=%d out=%d failed=%d bytes=%d status=%s",
			record.PipelineID, record.Mode, record.DurationMs, record.RecordsIn, record.RecordsOut,
			record.RecordsFailed, record.BytesProcessed, record.Status)

		switch record.Mode {
		case "stream":
			bucketTs := truncateToMinute(record.StartedAt)
			upsertMetricsBucket(dp, record.PipelineID, bucketTs, record.RecordsIn, record.RecordsOut, record.RecordsFailed, 0, record.BytesProcessed)

		default:
			logSnapshot := strings.Join(record.LogLines, "\n")
			recordExecutionMongo(dp, record.PipelineID, record.StartedAt, record.DurationMs,
				record.Status, "", logSnapshot,
				record.RecordsIn, record.RecordsOut, record.RecordsFailed, record.BytesProcessed)
		}

		// Update cumulative telemetry_state
		updateTelemetryCounters(dp, record.PipelineID, record.RecordsIn, record.RecordsOut, record.RecordsFailed, record.BytesProcessed)

		return c.JSON(fiber.Map{"status": "ok"})
	})

	// --- UI ENDPOINTS ---

	// GET /v1/pipelines/:id/steps/metrics -> Live step metrics list
	// For DAG pipelines, stage telemetry is stored under "{pipeline}-{stage}" keys.
	// We collect metrics from the exact pipeline ID AND any stage-prefixed IDs.
	app.Get("/v1/pipelines/:id/steps/metrics", func(c *fiber.Ctx) error {
		pipelineID := c.Params("id")
		stepMetricsMu.RLock()
		defer stepMetricsMu.RUnlock()

		var list []StepMetrics
		prefix := pipelineID + "-"

		// Exact match (simple pipelines or direct stage queries)
		if metricsMap, ok := stepMetricsStore[pipelineID]; ok {
			for _, v := range metricsMap {
				list = append(list, *v)
			}
		}

		// Stage-prefixed matches (DAG pipelines: e.g. "bluesky-sieve-ingestor")
		for storeKey, metricsMap := range stepMetricsStore {
			if storeKey != pipelineID && len(storeKey) > len(prefix) && storeKey[:len(prefix)] == prefix {
				stageName := storeKey[len(prefix):]
				for _, v := range metricsMap {
					sm := *v
					// Tag with the stage name if not already set
					if sm.StageName == "" {
						sm.StageName = stageName
					}
					list = append(list, sm)
				}
			}
		}

		if len(list) == 0 {
			return c.JSON([]StepMetrics{})
		}
		return c.JSON(list)
	})

	// GET /v1/pipelines/:id/steps/samples -> Latest samples map
	// For DAG pipelines, also collects samples from stage-prefixed IDs.
	app.Get("/v1/pipelines/:id/steps/samples", func(c *fiber.Ctx) error {
		pipelineID := c.Params("id")
		dataSampleMu.RLock()
		defer dataSampleMu.RUnlock()

		result := make(map[string]DataSample)
		prefix := pipelineID + "-"

		// Exact match
		if samplesMap, ok := dataSampleStore[pipelineID]; ok {
			for k, v := range samplesMap {
				result[k] = *v
			}
		}

		// Stage-prefixed matches
		for storeKey, samplesMap := range dataSampleStore {
			if storeKey != pipelineID && len(storeKey) > len(prefix) && storeKey[:len(prefix)] == prefix {
				stageName := storeKey[len(prefix):]
				for k, v := range samplesMap {
					ds := *v
					if ds.StageName == "" {
						ds.StageName = stageName
					}
					result[stageName+"/"+k] = ds
				}
			}
		}

		return c.JSON(result)
	})

	// GET /v1/environments -> Available environments
	app.Get("/v1/environments", func(c *fiber.Ctx) error {
		type EnvInfo struct {
			Name  string `json:"name"`
			Label string `json:"label"`
			Color string `json:"color"`
		}
		return c.JSON([]EnvInfo{
			{Name: "production", Label: "Production", Color: "green"},
			{Name: "preview", Label: "Preview", Color: "yellow"},
		})
	})

	// GET /v1/secrets -> List secret names in the environment's namespace (no values)
	app.Get("/v1/secrets", func(c *fiber.Ctx) error {
		if k8s == nil {
			return c.Status(503).JSON(fiber.Map{"error": "K8s client not available"})
		}
		env := c.Query("environment", defaultEnv)
		ns := resolveNS(env)

		secrets, err := k8s.ListSecrets(ns)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}

		type SecretInfo struct {
			Name string `json:"name"`
			Type string `json:"type"`
		}
		result := make([]SecretInfo, 0, len(secrets))
		for _, s := range secrets {
			result = append(result, SecretInfo{Name: s.Metadata.Name, Type: s.Type})
		}
		return c.JSON(result)
	})

	// GET /v1/secrets/:name/keys -> List key names in a secret (no values)
	app.Get("/v1/secrets/:name/keys", func(c *fiber.Ctx) error {
		if k8s == nil {
			return c.Status(503).JSON(fiber.Map{"error": "K8s client not available"})
		}
		env := c.Query("environment", defaultEnv)
		ns := resolveNS(env)
		name := c.Params("name")

		keys, err := k8s.GetSecretKeys(ns, name)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(fiber.Map{"secret": name, "keys": keys})
	})

	// GET /v1/pipelines -> Merged K8s + telemetry state
	app.Get("/v1/pipelines", func(c *fiber.Ctx) error {
		env := c.Query("environment", defaultEnv)
		ns := resolveNS(env)

		if k8s != nil {
			pipelines, err := getMergedPipelinesMongo(dp, k8s, ns)
			if err != nil {
				return c.Status(500).JSON(fiber.Map{"error": err.Error()})
			}
			for i := range pipelines {
				pipelines[i].Environment = env
			}
			return c.JSON(pipelines)
		}

		// Fallback: telemetry only
		pipelines, err := getPipelinesTelemetryOnly(dp)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(pipelines)
	})

	// --- TEST BUILD ENDPOINTS ---

	// POST /v1/pipelines/test -> Create an ephemeral builder Job from a draft branch
	app.Post("/v1/pipelines/test", func(c *fiber.Ctx) error {
		if k8s == nil {
			return c.Status(503).JSON(fiber.Map{"error": "K8s client not available"})
		}

		var body struct {
			GitRepository string `json:"git_repository"`
			Reference     string `json:"reference"`
			Path          string `json:"path"`
			PipelineID    string `json:"pipeline_id"`
		}
		if err := c.BodyParser(&body); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "invalid request body"})
		}
		if body.GitRepository == "" || body.Reference == "" {
			return c.Status(400).JSON(fiber.Map{"error": "git_repository and reference are required"})
		}

		ns := resolveNS("preview")

		testID := fmt.Sprintf("test-%s-%d", sanitizeForK8s(body.Reference), time.Now().Unix())
		targetImage := fmt.Sprintf("clotho-registry.clotho-system.svc.cluster.local:5000/%s:%s",
			testID, sanitizeForK8s(body.Reference))

		if err := k8s.CreateTestBuildJob(ns, testID, body.GitRepository, body.Reference, body.Path, targetImage, ""); err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}

		return c.JSON(fiber.Map{
			"test_id":     testID,
			"status":      "pending",
			"environment": "preview",
		})
	})

	// GET /v1/pipelines/test/:id/status -> Poll test build status
	app.Get("/v1/pipelines/test/:id/status", func(c *fiber.Ctx) error {
		testID := c.Params("id")
		ns := resolveNS("preview")

		if k8s == nil {
			return c.Status(503).JSON(fiber.Map{"error": "K8s client not available"})
		}

		job, err := k8s.GetJob(ns, testID)
		if err != nil {
			return c.Status(404).JSON(fiber.Map{"error": fmt.Sprintf("test build not found: %v", err)})
		}

		status := "pending"
		if job.Status.Active > 0 {
			status = "running"
		} else if job.Status.Succeeded > 0 {
			status = "succeeded"
		} else if job.Status.Failed > 0 {
			status = "failed"
		}

		result := fiber.Map{
			"test_id":    testID,
			"status":     status,
			"start_time": job.Status.StartTime,
		}
		if job.Status.CompletionTime != "" {
			result["completion_time"] = job.Status.CompletionTime
		}

		return c.JSON(result)
	})

	// GET /v1/pipelines/test/:id/logs -> SSE stream of builder Job pod logs
	app.Get("/v1/pipelines/test/:id/logs", func(c *fiber.Ctx) error {
		testID := c.Params("id")
		ns := resolveNS("preview")

		if k8s == nil {
			return c.Status(503).JSON(fiber.Map{"error": "K8s client not available"})
		}

		var podName string
		for retries := 0; retries < 30; retries++ {
			pods, err := k8s.GetPodsForJob(ns, testID)
			if err == nil && len(pods) > 0 {
				podName = pods[0].Metadata.Name
				break
			}
			time.Sleep(2 * time.Second)
		}

		if podName == "" {
			return c.Status(404).JSON(fiber.Map{"error": "builder pod not found (timed out)"})
		}

		c.Set("Content-Type", "text/event-stream")
		c.Set("Cache-Control", "no-cache")
		c.Set("Connection", "keep-alive")
		c.Set("X-Accel-Buffering", "no")

		c.Context().SetBodyStreamWriter(func(w *bufio.Writer) {
			logStream, err := k8s.StreamPodLogs(ns, podName, true)
			if err != nil {
				fmt.Fprintf(w, "data: {\"error\": %q}\n\n", err.Error())
				w.Flush()
				return
			}
			defer logStream.Close()

			scanner := bufio.NewScanner(logStream)
			for scanner.Scan() {
				line := scanner.Text()
				fmt.Fprintf(w, "data: %s\n\n", line)
				w.Flush()
			}

			fmt.Fprintf(w, "event: done\ndata: build complete\n\n")
			w.Flush()
		})

		return nil
	})

	// DELETE /v1/pipelines/test/:id -> Cleanup a test build job
	app.Delete("/v1/pipelines/test/:id", func(c *fiber.Ctx) error {
		testID := c.Params("id")
		ns := resolveNS("preview")

		if k8s == nil {
			return c.Status(503).JSON(fiber.Map{"error": "K8s client not available"})
		}

		if err := k8s.DeleteJob(ns, testID); err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(fiber.Map{"status": "deleted", "test_id": testID})
	})

	// GET /v1/pipelines/:id -> Single pipeline detail
	app.Get("/v1/pipelines/:id", func(c *fiber.Ctx) error {
		id := c.Params("id")
		env := c.Query("environment", defaultEnv)
		ns := resolveNS(env)

		if k8s == nil {
			return c.Status(503).JSON(fiber.Map{"error": "K8s client not available"})
		}

		pipeline, err := getMergedPipelineMongo(dp, k8s, ns, id)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		pipeline.Environment = env
		return c.JSON(pipeline)
	})

	// GET /v1/pipelines/:id/pods -> Live pod status
	app.Get("/v1/pipelines/:id/pods", func(c *fiber.Ctx) error {
		id := c.Params("id")
		env := c.Query("environment", defaultEnv)
		ns := resolveNS(env)

		if k8s == nil {
			return c.Status(503).JSON(fiber.Map{"error": "K8s client not available"})
		}

		pods, err := getPodDetails(k8s, ns, id)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(pods)
	})

	// GET /v1/builds -> Global build history across all pipelines
	app.Get("/v1/builds", func(c *fiber.Ctx) error {
		env := c.Query("environment", defaultEnv)
		ns := resolveNS(env)
		limit := c.QueryInt("limit", 200)

		if k8s != nil {
			syncBuildHistoryMongo(dp, k8s, ns)
		}

		docs, err := dp.findDocs(colBuildHistory, nil, map[string]interface{}{"started_at": -1}, int64(limit), 0)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}

		builds := make([]map[string]interface{}, 0, len(docs))
		for _, d := range docs {
			b := map[string]interface{}{
				"id":             d["_id"],
				"pipeline_id":    d["pipeline_id"],
				"pipeline_name":  d["pipeline_name"],
				"job_name":       d["job_name"],
				"git_repository": d["git_repository"],
				"reference":      d["reference"],
				"path":           d["path"],
				"target_image":   d["target_image"],
				"status":         d["status"],
				"started_at":     d["started_at"],
			}
			if v, ok := d["finished_at"]; ok && v != "" {
				b["finished_at"] = v
			}
			if v, ok := d["duration_ms"]; ok {
				b["duration_ms"] = v
			}
			if v, ok := d["error"]; ok && v != "" {
				b["error"] = v
			}
			if v, ok := d["created_at"]; ok {
				b["created_at"] = v
			}
			builds = append(builds, b)
		}

		return c.JSON(builds)
	})

	// GET /v1/pipelines/:id/builds -> Build job history
	app.Get("/v1/pipelines/:id/builds", func(c *fiber.Ctx) error {
		id := c.Params("id")
		env := c.Query("environment", defaultEnv)
		ns := resolveNS(env)

		if k8s == nil {
			return c.Status(503).JSON(fiber.Map{"error": "K8s client not available"})
		}

		builds, err := getBuildDetails(k8s, ns, id)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(builds)
	})

	// GET /v1/pipelines/:id/events -> Telemetry event history
	app.Get("/v1/pipelines/:id/events", func(c *fiber.Ctx) error {
		pipelineID := c.Params("id")
		events, err := getEventsMongo(dp, pipelineID)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(events)
	})

	// GET /v1/pipelines/:id/executions -> Execution history with per-run logs
	app.Get("/v1/pipelines/:id/executions", func(c *fiber.Ctx) error {
		pipelineID := c.Params("id")
		limit := c.QueryInt("limit", 50)

		execs, err := getExecutionHistoryMongo(dp, pipelineID, limit)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(execs)
	})

	// GET /v1/pipelines/:id/metrics -> Time-series throughput buckets
	app.Get("/v1/pipelines/:id/metrics", func(c *fiber.Ctx) error {
		pipelineID := c.Params("id")
		minutes := c.QueryInt("minutes", 60)

		docs, err := dp.findDocs(colMetricsBuckets, map[string]interface{}{
			"pipeline_id": pipelineID,
			"bucket_ts":   map[string]interface{}{"$gte": time.Now().UTC().Add(time.Duration(-minutes) * time.Minute).Format(time.RFC3339)},
		}, map[string]interface{}{"bucket_ts": 1}, 0, 0)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}

		buckets := make([]map[string]interface{}, 0, len(docs))
		for _, d := range docs {
			buckets = append(buckets, map[string]interface{}{
				"bucket_ts":        d["bucket_ts"],
				"records_in":       toInt64(d["records_in"]),
				"records_out":      toInt64(d["records_out"]),
				"records_failed":   toInt64(d["records_failed"]),
				"records_filtered": toInt64(d["records_filtered"]),
				"bytes_processed":  toInt64(d["bytes_processed"]),
				"invocations":      toInt64(d["invocations"]),
				"avg_duration_ms":  toInt64(d["avg_duration_ms"]),
				"max_duration_ms":  toInt64(d["max_duration_ms"]),
			})
		}
		return c.JSON(buckets)
	})

	// GET /v1/pipelines/:id/lifecycle -> Lifecycle events
	app.Get("/v1/pipelines/:id/lifecycle", func(c *fiber.Ctx) error {
		pipelineID := c.Params("id")
		limit := c.QueryInt("limit", 100)

		docs, err := dp.findDocs(colLifecycleEvents, map[string]interface{}{"pipeline_id": pipelineID}, map[string]interface{}{"timestamp": -1}, int64(limit), 0)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}

		events := make([]map[string]interface{}, 0, len(docs))
		for _, d := range docs {
			events = append(events, map[string]interface{}{
				"id":        d["_id"],
				"event":     d["event"],
				"version":   d["version"],
				"message":   d["message"],
				"timestamp": d["timestamp"],
			})
		}
		return c.JSON(events)
	})

	// GET /v1/pipelines/:id/dlq -> Dead Letter Queue records
	app.Get("/v1/pipelines/:id/dlq", func(c *fiber.Ctx) error {
		pipelineID := c.Params("id")
		limit := c.QueryInt("limit", 100)
		status := c.Query("status", "")

		filter := map[string]interface{}{"pipeline_id": pipelineID}
		if status != "" {
			filter["status"] = status
		}

		docs, err := dp.findDocs(colDlqRecords, filter, map[string]interface{}{"created_at": -1}, int64(limit), 0)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}

		records := make([]DlqRecord, 0, len(docs))
		for _, d := range docs {
			records = append(records, DlqRecord{
				ID:         strVal(d["_id"]),
				PipelineID: strVal(d["pipeline_id"]),
				TraceID:    strVal(d["trace_id"]),
				Error:      strVal(d["error"]),
				Step:       strVal(d["step"]),
				Payload:    strVal(d["payload"]),
				Status:     strVal(d["status"]),
				CreatedAt:  strVal(d["created_at"]),
				ReplayedAt: strVal(d["replayed_at"]),
			})
		}

		totalCount, _ := dp.countDocs(colDlqRecords, map[string]interface{}{"pipeline_id": pipelineID})

		return c.JSON(fiber.Map{
			"records":     records,
			"total_count": totalCount,
		})
	})

	// GET /v1/pipelines/:id/dlq/groups -> Pattern-Centric DLQ view
	app.Get("/v1/pipelines/:id/dlq/groups", func(c *fiber.Ctx) error {
		pipelineID := c.Params("id")

		results, err := dp.aggregate(colDlqRecords, []map[string]interface{}{
			{"$match": map[string]interface{}{"pipeline_id": pipelineID, "status": "pending"}},
			{"$group": map[string]interface{}{
				"_id":        map[string]interface{}{"error": "$error", "step": "$step"},
				"count":      map[string]interface{}{"$sum": 1},
				"first_seen": map[string]interface{}{"$min": "$created_at"},
				"last_seen":  map[string]interface{}{"$max": "$created_at"},
			}},
			{"$sort": map[string]interface{}{"count": -1}},
		})
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}

		groups := make([]map[string]interface{}, 0, len(results))
		for _, r := range results {
			idMap, _ := r["_id"].(map[string]interface{})
			groups = append(groups, map[string]interface{}{
				"error":      idMap["error"],
				"step":       idMap["step"],
				"count":      r["count"],
				"first_seen": r["first_seen"],
				"last_seen":  r["last_seen"],
			})
		}

		totalPending, _ := dp.countDocs(colDlqRecords, map[string]interface{}{"pipeline_id": pipelineID, "status": "pending"})

		return c.JSON(fiber.Map{
			"groups":        groups,
			"total_pending": totalPending,
		})
	})

	// POST /v1/pipelines/:id/dlq/replay-group -> Bulk replay all records matching an error pattern
	app.Post("/v1/pipelines/:id/dlq/replay-group", func(c *fiber.Ctx) error {
		pipelineID := c.Params("id")
		var body struct {
			Error string `json:"error"`
			Step  string `json:"step"`
		}
		if err := c.BodyParser(&body); err != nil || body.Error == "" {
			return c.Status(400).JSON(fiber.Map{"error": "error field required"})
		}

		filter := map[string]interface{}{
			"pipeline_id": pipelineID,
			"error":       body.Error,
			"step":        body.Step,
			"status":      "pending",
		}
		update := map[string]interface{}{
			"$set": map[string]interface{}{
				"status":      "replayed",
				"replayed_at": time.Now().Format(time.RFC3339),
			},
		}
		if err := dp.updateMany(colDlqRecords, filter, update); err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}

		count, _ := dp.countDocs(colDlqRecords, filter)
		log.Printf("[dlq] Bulk replayed %d records for %s (error=%q step=%s)", count, pipelineID, body.Error, body.Step)
		return c.JSON(fiber.Map{"status": "replayed", "count": count, "pipeline_id": pipelineID})
	})

	// POST /v1/pipelines/:id/dlq/dismiss-group -> Bulk dismiss
	app.Post("/v1/pipelines/:id/dlq/dismiss-group", func(c *fiber.Ctx) error {
		pipelineID := c.Params("id")
		var body struct {
			Error string `json:"error"`
			Step  string `json:"step"`
		}
		if err := c.BodyParser(&body); err != nil || body.Error == "" {
			return c.Status(400).JSON(fiber.Map{"error": "error field required"})
		}

		filter := map[string]interface{}{
			"pipeline_id": pipelineID,
			"error":       body.Error,
			"step":        body.Step,
			"status":      "pending",
		}
		update := map[string]interface{}{
			"$set": map[string]interface{}{"status": "dismissed"},
		}
		if err := dp.updateMany(colDlqRecords, filter, update); err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}

		count, _ := dp.countDocs(colDlqRecords, filter)
		log.Printf("[dlq] Bulk dismissed %d records for %s (error=%q step=%s)", count, pipelineID, body.Error, body.Step)
		return c.JSON(fiber.Map{"status": "dismissed", "count": count, "pipeline_id": pipelineID})
	})

	// POST /v1/pipelines/:id/dlq/canary -> Canary Replay
	app.Post("/v1/pipelines/:id/dlq/canary", func(c *fiber.Ctx) error {
		pipelineID := c.Params("id")
		env := c.Query("environment", defaultEnv)
		ns := resolveNS(env)

		var body struct {
			Error string `json:"error"`
			Step  string `json:"step"`
		}
		if err := c.BodyParser(&body); err != nil || body.Error == "" {
			return c.Status(400).JSON(fiber.Map{"error": "error field required"})
		}

		if k8s == nil {
			return c.Status(503).JSON(fiber.Map{"error": "K8s client not available"})
		}

		// Fetch oldest pending DLQ record
		docs, err := dp.findDocs(colDlqRecords, map[string]interface{}{
			"pipeline_id": pipelineID,
			"error":       body.Error,
			"step":        body.Step,
			"status":      "pending",
		}, map[string]interface{}{"created_at": 1}, 1, 0)
		if err != nil || len(docs) == 0 {
			return c.Status(404).JSON(fiber.Map{"error": "No pending records for this error group"})
		}

		recordID := strVal(docs[0]["_id"])
		traceID := strVal(docs[0]["trace_id"])
		payload := strVal(docs[0]["payload"])

		// Find a running pod
		pods, err := k8s.GetPodsForPipeline(ns, pipelineID)
		if err != nil || len(pods) == 0 {
			return c.Status(503).JSON(fiber.Map{"error": "No running pods for pipeline"})
		}
		podIP := pods[0].Status.PodIP
		if podIP == "" {
			return c.Status(503).JSON(fiber.Map{"error": "Pod has no IP"})
		}

		replayURL := fmt.Sprintf("http://%s:8127/clotho/replay", podIP)
		replayBody := fmt.Sprintf(`{"pipeline_id":"%s","records":[{"trace_id":"%s","payload":%s}]}`, pipelineID, traceID, payload)

		httpClient := &http.Client{Timeout: 30 * time.Second}
		req, err := http.NewRequest("POST", replayURL, strings.NewReader(replayBody))
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "Failed to build replay request"})
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Clotho-Canary", "true")

		resp, err := httpClient.Do(req)
		if err != nil {
			return c.JSON(fiber.Map{
				"status":    "unavailable",
				"record_id": recordID,
				"trace_id":  traceID,
				"message":   fmt.Sprintf("Pipeline pod does not expose replay endpoint: %v", err),
			})
		}
		defer resp.Body.Close()
		respBody, _ := io.ReadAll(resp.Body)

		if resp.StatusCode == 200 {
			dp.updateByID(colDlqRecords, recordID, map[string]interface{}{
				"status":      "replayed",
				"replayed_at": time.Now().Format(time.RFC3339),
			})
			log.Printf("[canary] Record %s passed canary for %s", recordID, pipelineID)
			return c.JSON(fiber.Map{
				"status":    "success",
				"record_id": recordID,
				"trace_id":  traceID,
				"message":   "Canary record processed successfully. Safe to replay group.",
			})
		}

		log.Printf("[canary] Record %s failed canary for %s: HTTP %d", recordID, pipelineID, resp.StatusCode)
		return c.JSON(fiber.Map{
			"status":    "failed",
			"record_id": recordID,
			"trace_id":  traceID,
			"http_code": resp.StatusCode,
			"error":     string(respBody),
			"message":   "Canary record failed. Fix the issue before replaying the group.",
		})
	})

	// GET /v1/events -> All telemetry events across all pipelines
	app.Get("/v1/events", func(c *fiber.Ctx) error {
		limit := c.QueryInt("limit", 200)
		since := c.Query("since", "")

		events, err := getAllEventsMongo(dp, limit, since)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(events)
	})

	// POST /v1/pipelines/:id/restart -> Restart pipeline pods
	app.Post("/v1/pipelines/:id/restart", func(c *fiber.Ctx) error {
		id := c.Params("id")
		env := c.Query("environment", defaultEnv)
		ns := resolveNS(env)

		if k8s == nil {
			return c.Status(503).JSON(fiber.Map{"error": "K8s client not available"})
		}

		restarted, err := restartPipelinePods(k8s, ns, id)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}

		return c.JSON(fiber.Map{
			"status":      "restarted",
			"pipeline":    id,
			"environment": env,
			"workloads":   restarted,
		})
	})

	// GET /v1/pods/:name/logs -> Pod log output
	app.Get("/v1/pods/:name/logs", func(c *fiber.Ctx) error {
		podName := c.Params("name")
		env := c.Query("environment", defaultEnv)
		ns := resolveNS(env)
		tailLines := c.QueryInt("tail", 100)

		if k8s == nil {
			return c.Status(503).JSON(fiber.Map{"error": "K8s client not available"})
		}

		logs, err := k8s.GetPodLogs(ns, podName, tailLines)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(fiber.Map{"pod": podName, "logs": logs})
	})

	// POST /v1/pipelines/:id/pause -> Scale replicas to 0
	app.Post("/v1/pipelines/:id/pause", func(c *fiber.Ctx) error {
		id := c.Params("id")
		env := c.Query("environment", defaultEnv)
		ns := resolveNS(env)

		if k8s == nil {
			return c.Status(503).JSON(fiber.Map{"error": "K8s client not available"})
		}

		patch := []byte(`{"spec":{"replicas":0}}`)
		if err := k8s.PatchPipelineCR(ns, id, patch); err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(fiber.Map{"status": "paused", "pipeline": id})
	})

	// POST /v1/pipelines/:id/resume -> Scale replicas to 1
	app.Post("/v1/pipelines/:id/resume", func(c *fiber.Ctx) error {
		id := c.Params("id")
		env := c.Query("environment", defaultEnv)
		ns := resolveNS(env)

		if k8s == nil {
			return c.Status(503).JSON(fiber.Map{"error": "K8s client not available"})
		}

		patch := []byte(`{"spec":{"replicas":1}}`)
		if err := k8s.PatchPipelineCR(ns, id, patch); err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(fiber.Map{"status": "resumed", "pipeline": id})
	})

	// --- CONFIG ENDPOINTS ---

	// GET /v1/pipelines/:id/config -> Read pipeline config vars from CRD
	app.Get("/v1/pipelines/:id/config", func(c *fiber.Ctx) error {
		id := c.Params("id")
		env := c.Query("environment", defaultEnv)
		ns := resolveNS(env)

		if k8s == nil {
			return c.Status(503).JSON(fiber.Map{"error": "K8s client not available"})
		}

		cr, err := k8s.GetPipeline(ns, id)
		if err != nil {
			return c.Status(404).JSON(fiber.Map{"error": fmt.Sprintf("pipeline not found: %v", err)})
		}

		type ConfigEntry struct {
			Name       string `json:"name"`
			Value      string `json:"value,omitempty"`
			Source     string `json:"source"`
			SecretName string `json:"secret_name,omitempty"`
			SecretKey  string `json:"secret_key,omitempty"`
		}

		entries := make([]ConfigEntry, 0)
		for _, cv := range cr.Spec.Config {
			entry := ConfigEntry{Name: cv.Name}
			if cv.ValueFrom != nil && cv.ValueFrom.SecretKeyRef != nil {
				entry.Source = "secret"
				entry.SecretName = cv.ValueFrom.SecretKeyRef.Name
				entry.SecretKey = cv.ValueFrom.SecretKeyRef.Key
			} else {
				entry.Source = "literal"
				entry.Value = cv.Value
			}
			entries = append(entries, entry)
		}

		return c.JSON(fiber.Map{
			"pipeline_id": id,
			"config":      entries,
		})
	})

	// PATCH /v1/pipelines/:id/config -> Update pipeline config vars via CRD patch
	app.Patch("/v1/pipelines/:id/config", func(c *fiber.Ctx) error {
		id := c.Params("id")
		env := c.Query("environment", defaultEnv)
		ns := resolveNS(env)

		if k8s == nil {
			return c.Status(503).JSON(fiber.Map{"error": "K8s client not available"})
		}

		var body struct {
			Config []ConfigVar `json:"config"`
		}
		if err := c.BodyParser(&body); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": "invalid request body"})
		}

		patch := map[string]interface{}{
			"spec": map[string]interface{}{
				"config": body.Config,
			},
		}
		patchJSON, err := json.Marshal(patch)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "failed to marshal patch"})
		}

		if err := k8s.PatchPipelineCR(ns, id, patchJSON); err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}

		return c.JSON(fiber.Map{"status": "updated", "pipeline": id})
	})

	// GET /v1/sdk/version -> Return the canonical SDK version from the repo Cargo.toml
	app.Get("/v1/sdk/version", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"version": latestSDKVersion()})
	})

	// POST /v1/pipelines/:id/rebuild -> Trigger a new build for the pipeline
	// Patches the Pipeline CR with a rebuild annotation so the operator picks it up.
	app.Post("/v1/pipelines/:id/rebuild", func(c *fiber.Ctx) error {
		id := c.Params("id")
		env := c.Query("environment", defaultEnv)
		ns := resolveNS(env)

		if k8s == nil {
			return c.Status(503).JSON(fiber.Map{"error": "K8s client not available"})
		}
		if err := k8s.TriggerRebuild(ns, id); err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		log.Printf("[rebuild] triggered rebuild for pipeline %s in %s", id, ns)
		return c.JSON(fiber.Map{"status": "rebuild_triggered", "pipeline": id})
	})

	// DELETE /v1/pipelines/:id -> Delete pipeline CR
	app.Delete("/v1/pipelines/:id", func(c *fiber.Ctx) error {
		id := c.Params("id")
		env := c.Query("environment", defaultEnv)
		ns := resolveNS(env)

		if k8s == nil {
			return c.Status(503).JSON(fiber.Map{"error": "K8s client not available"})
		}

		if err := k8s.DeletePipelineCR(ns, id); err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(fiber.Map{"status": "deleted", "pipeline": id})
	})

	// --- DLQ ENDPOINTS ---

	// GET /v1/dlq -> List all DLQ records
	app.Get("/v1/dlq", func(c *fiber.Ctx) error {
		pipelineID := c.Query("pipeline_id", "")
		status := c.Query("status", "")
		limit := c.QueryInt("limit", 100)

		records, err := getDlqRecordsMongo(dp, pipelineID, status, limit)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(records)
	})

	// GET /v1/dlq/summary -> DLQ counts per pipeline
	app.Get("/v1/dlq/summary", func(c *fiber.Ctx) error {
		summary, err := getDlqSummaryMongo(dp)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(summary)
	})

	// GET /v1/dlq/:id -> Single DLQ record detail
	app.Get("/v1/dlq/:id", func(c *fiber.Ctx) error {
		id := c.Params("id")
		record, err := getDlqRecordMongo(dp, id)
		if err != nil {
			return c.Status(404).JSON(fiber.Map{"error": "DLQ record not found"})
		}
		return c.JSON(record)
	})

	// POST /v1/dlq/:id/replay -> Mark as replayed
	app.Post("/v1/dlq/:id/replay", func(c *fiber.Ctx) error {
		id := c.Params("id")
		if err := updateDlqStatusMongo(dp, id, "replayed"); err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(fiber.Map{"status": "replayed", "id": id})
	})

	// POST /v1/dlq/:id/dismiss -> Mark as dismissed
	app.Post("/v1/dlq/:id/dismiss", func(c *fiber.Ctx) error {
		id := c.Params("id")
		if err := updateDlqStatusMongo(dp, id, "dismissed"); err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(fiber.Map{"status": "dismissed", "id": id})
	})

	// POST /v1/dlq/replay-all -> Replay all pending DLQ records for a pipeline
	app.Post("/v1/dlq/replay-all", func(c *fiber.Ctx) error {
		pipelineID := c.Query("pipeline_id", "")
		if pipelineID == "" {
			return c.Status(400).JSON(fiber.Map{"error": "pipeline_id required"})
		}
		count, err := replayAllDlqMongo(dp, pipelineID)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(fiber.Map{"status": "replayed", "pipeline_id": pipelineID, "count": count})
	})

	// Health check
	app.Get("/health", func(c *fiber.Ctx) error {
		k8sStatus := "disconnected"
		if k8s != nil {
			k8sStatus = "connected"
		}
		dpStatus := "disconnected"
		if dp != nil {
			dpStatus = "connected"
		}
		return c.JSON(fiber.Map{"status": "healthy", "k8s": k8sStatus, "data_proxy": dpStatus})
	})

	// ==========================================================
	// PHONE HOME TUNNEL — Operator <-> Control Plane
	// ==========================================================

	// POST /v1/api-keys — Generate a new API key for a tenant
	app.Post("/v1/api-keys", func(c *fiber.Ctx) error {
		var body struct {
			TenantID string `json:"tenant_id"`
			Label    string `json:"label"`
		}
		if err := c.BodyParser(&body); err != nil || body.TenantID == "" {
			return c.Status(400).JSON(fiber.Map{"error": "tenant_id required"})
		}

		rawKey, keyHash, keyPrefix, err := generateAPIKey()
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": "failed to generate key"})
		}

		doc := map[string]interface{}{
			"key_hash":   keyHash,
			"key_prefix": keyPrefix,
			"tenant_id":  body.TenantID,
			"label":      body.Label,
			"created_at": time.Now().Format(time.RFC3339),
		}
		if err := dp.insertOne(colApiKeys, doc); err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}

		log.Printf("[phone-home] Generated API key for tenant %s (prefix: %s)", body.TenantID, keyPrefix)

		return c.JSON(fiber.Map{
			"api_key":   rawKey,
			"prefix":    keyPrefix,
			"tenant_id": body.TenantID,
			"helm_cmd": fmt.Sprintf(
				"helm install clotho clotho/clotho --namespace clotho-system --create-namespace --set auth.apiKey=%s --set controlPlaneUrl=https://api.clotho.io",
				rawKey,
			),
		})
	})

	// POST /agent/handshake — Operator authenticates + registers cluster
	app.Post("/agent/handshake", func(c *fiber.Ctx) error {
		apiKey := c.Get("Authorization")
		if apiKey == "" {
			return c.Status(401).JSON(fiber.Map{"error": "missing Authorization header"})
		}
		apiKey = strings.TrimPrefix(apiKey, "Bearer ")

		tenantID, keyID, err := validateAPIKeyMongo(dp, apiKey)
		if err != nil {
			return c.Status(401).JSON(fiber.Map{"error": "invalid API key"})
		}

		var meta struct {
			ClusterName  string `json:"cluster_name"`
			AgentVersion string `json:"agent_version"`
			K8sVersion   string `json:"k8s_version"`
			NodeCount    int    `json:"node_count"`
		}
		c.BodyParser(&meta)

		clusterDoc := map[string]interface{}{
			"tenant_id":      tenantID,
			"cluster_name":   meta.ClusterName,
			"api_key_id":     keyID,
			"status":         "online",
			"last_heartbeat": time.Now().Format(time.RFC3339),
			"agent_version":  meta.AgentVersion,
			"k8s_version":    meta.K8sVersion,
			"node_count":     meta.NodeCount,
			"created_at":     time.Now().Format(time.RFC3339),
		}
		// Upsert by tenant_id+cluster_name
		if meta.ClusterName != "" {
			dp.upsertByID(colClusters, tenantID+"_"+meta.ClusterName, clusterDoc)
		}

		dp.updateByID(colApiKeys, keyID, map[string]interface{}{"last_used_at": time.Now().Format(time.RFC3339)})

		// Count pending commands
		docs, _ := dp.findDocs(colCommandQueue, map[string]interface{}{"tenant_id": tenantID, "status": "pending"}, nil, 0, 0)
		pendingCount := len(docs)

		log.Printf("[phone-home] Handshake OK: tenant=%s cluster=%s pending=%d", tenantID, meta.ClusterName, pendingCount)

		return c.JSON(fiber.Map{
			"status":           "connected",
			"tenant_id":        tenantID,
			"pending_commands": pendingCount,
		})
	})

	// GET /agent/commands — Operator polls for pending commands
	app.Get("/agent/commands", func(c *fiber.Ctx) error {
		apiKey := c.Get("Authorization")
		if apiKey == "" {
			return c.Status(401).JSON(fiber.Map{"error": "missing Authorization header"})
		}
		apiKey = strings.TrimPrefix(apiKey, "Bearer ")

		tenantID, keyID, err := validateAPIKeyMongo(dp, apiKey)
		if err != nil {
			return c.Status(401).JSON(fiber.Map{"error": "invalid API key"})
		}

		dp.updateMany(colClusters, map[string]interface{}{"api_key_id": keyID}, map[string]interface{}{
			"$set": map[string]interface{}{
				"last_heartbeat": time.Now().Format(time.RFC3339),
				"status":         "online",
			},
		})

		docs, err := dp.findDocs(colCommandQueue, map[string]interface{}{"tenant_id": tenantID, "status": "pending"}, map[string]interface{}{"created_at": 1}, 10, 0)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}

		commands := make([]map[string]interface{}, 0)
		for _, d := range docs {
			commands = append(commands, map[string]interface{}{
				"id":            d["_id"],
				"command_type":  d["command_type"],
				"resource_name": d["resource_name"],
				"namespace":     d["namespace"],
				"payload":       d["payload"],
				"created_at":    d["created_at"],
			})
		}

		return c.JSON(fiber.Map{
			"commands": commands,
			"count":    len(commands),
		})
	})

	// POST /agent/commands/:id/ack — Operator confirms command was applied
	app.Post("/agent/commands/:id/ack", func(c *fiber.Ctx) error {
		apiKey := c.Get("Authorization")
		if apiKey == "" {
			return c.Status(401).JSON(fiber.Map{"error": "missing Authorization header"})
		}
		apiKey = strings.TrimPrefix(apiKey, "Bearer ")

		tenantID, _, err := validateAPIKeyMongo(dp, apiKey)
		if err != nil {
			return c.Status(401).JSON(fiber.Map{"error": "invalid API key"})
		}

		cmdID := c.Params("id")

		var body struct {
			Status   string `json:"status"`
			ErrorMsg string `json:"error_msg"`
		}
		if err := c.BodyParser(&body); err != nil {
			body.Status = "applied"
		}
		if body.Status == "" {
			body.Status = "applied"
		}

		err = dp.updateByID(colCommandQueue, cmdID, map[string]interface{}{
			"status":    body.Status,
			"acked_at":  time.Now().Format(time.RFC3339),
			"error_msg": body.ErrorMsg,
		})
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}

		log.Printf("[phone-home] Command %s acked: status=%s tenant=%s", cmdID, body.Status, tenantID)

		return c.JSON(fiber.Map{"status": body.Status, "id": cmdID})
	})

	// POST /v1/commands — UI queues a new command for a tenant's operator
	app.Post("/v1/commands", func(c *fiber.Ctx) error {
		var body struct {
			TenantID     string `json:"tenant_id"`
			CommandType  string `json:"command_type"`
			ResourceName string `json:"resource_name"`
			Namespace    string `json:"namespace"`
			Payload      string `json:"payload"`
		}
		if err := c.BodyParser(&body); err != nil {
			return c.Status(400).JSON(fiber.Map{"error": err.Error()})
		}
		if body.TenantID == "" || body.CommandType == "" || body.Payload == "" {
			return c.Status(400).JSON(fiber.Map{"error": "tenant_id, command_type, and payload required"})
		}
		if body.Namespace == "" {
			body.Namespace = "default"
		}

		doc := map[string]interface{}{
			"tenant_id":     body.TenantID,
			"command_type":  body.CommandType,
			"resource_name": body.ResourceName,
			"namespace":     body.Namespace,
			"payload":       body.Payload,
			"status":        "pending",
			"created_at":    time.Now().Format(time.RFC3339),
		}
		if err := dp.insertOne(colCommandQueue, doc); err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}

		log.Printf("[phone-home] Queued command for tenant %s: %s %s", body.TenantID, body.CommandType, body.ResourceName)

		return c.JSON(fiber.Map{"status": "queued"})
	})

	// GET /v1/clusters — UI reads connected cluster status for a tenant
	app.Get("/v1/clusters", func(c *fiber.Ctx) error {
		tenantID := c.Query("tenant_id", "")
		if tenantID == "" {
			return c.Status(400).JSON(fiber.Map{"error": "tenant_id required"})
		}

		docs, err := dp.findDocs(colClusters, map[string]interface{}{"tenant_id": tenantID}, map[string]interface{}{"created_at": -1}, 0, 0)
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error()})
		}

		clusters := make([]map[string]interface{}, 0)
		for _, d := range docs {
			cl := map[string]interface{}{
				"id":             d["_id"],
				"cluster_name":   d["cluster_name"],
				"status":         d["status"],
				"last_heartbeat": d["last_heartbeat"],
				"agent_version":  d["agent_version"],
				"k8s_version":    d["k8s_version"],
				"node_count":     d["node_count"],
				"created_at":     d["created_at"],
			}
			// Mark offline if no heartbeat in 60s
			if lh, ok := d["last_heartbeat"].(string); ok {
				if t, err := time.Parse(time.RFC3339, lh); err == nil && time.Since(t) > 60*time.Second {
					cl["status"] = "offline"
				}
			}
			clusters = append(clusters, cl)
		}

		return c.JSON(clusters)
	})

	log.Println("Clotho API v2.0 (MongoDB) starting on :3000")
	log.Fatal(app.Listen(":3000"))
}

// ═══════════════════════════════════════════════════════════════════════════════
// MongoDB-backed data operations
// ═══════════════════════════════════════════════════════════════════════════════

func getTelemetryCounters(dp *DataProxyClient, pipelineID string) (recordsIn, recordsOut, recordsFailed, recordsFiltered, bytesProcessed int64) {
	doc, err := dp.findOneDoc(colTelemetryState, pipelineID)
	if err != nil || doc == nil {
		return 0, 0, 0, 0, 0
	}
	recordsIn = toInt64(doc["records_in"])
	recordsOut = toInt64(doc["records_out"])
	recordsFailed = toInt64(doc["records_failed"])
	recordsFiltered = toInt64(doc["records_filtered"])
	bytesProcessed = toInt64(doc["bytes_processed"])
	return
}

func updateTelemetryCounters(dp *DataProxyClient, pipelineID string, recordsIn, recordsOut, recordsFailed, bytesProcessed int64) {
	doc := map[string]interface{}{
		"pipeline_id":     pipelineID,
		"records_in":      recordsIn,
		"records_out":     recordsOut,
		"records_failed":  recordsFailed,
		"bytes_processed": bytesProcessed,
		"last_seen":       time.Now().Format(time.RFC3339),
	}
	dp.upsertByID(colTelemetryState, pipelineID, doc)
}

func updateHeartbeatMongo(dp *DataProxyClient, p AgentPayload) error {
	var cpuNano, memBytes int64
	if p.Stats != nil {
		cpuNano = p.Stats.CpuNano
		memBytes = p.Stats.MemBytes
	}

	var progressCurrent, progressTotal int64
	var recordsIn, recordsOut, recordsFailed, recordsFiltered, bytesProcessed int64
	for _, event := range p.Events {
		if event.Type == "PROGRESS" {
			if current, ok := event.Payload["current"].(float64); ok {
				progressCurrent = int64(current)
			}
			if total, ok := event.Payload["total"].(float64); ok {
				progressTotal = int64(total)
			}
		}
		if event.Type == "THROUGHPUT" {
			if v, ok := event.Payload["records_in"].(float64); ok {
				recordsIn = int64(v)
			}
			if v, ok := event.Payload["records_out"].(float64); ok {
				recordsOut = int64(v)
			}
			if v, ok := event.Payload["records_failed"].(float64); ok {
				recordsFailed = int64(v)
			}
			if v, ok := event.Payload["records_filtered"].(float64); ok {
				recordsFiltered = int64(v)
			}
			if v, ok := event.Payload["bytes_processed"].(float64); ok {
				bytesProcessed = int64(v)
			}
		}
	}

	doc := map[string]interface{}{
		"pipeline_id":         p.PipelineID,
		"cpu_usage_nanocores": cpuNano,
		"memory_bytes":        memBytes,
		"progress_current":    progressCurrent,
		"progress_total":      progressTotal,
		"records_in":          recordsIn,
		"records_out":         recordsOut,
		"records_failed":      recordsFailed,
		"records_filtered":    recordsFiltered,
		"bytes_processed":     bytesProcessed,
		"last_seen":           time.Now().Format(time.RFC3339),
	}
	return dp.upsertByID(colTelemetryState, p.PipelineID, doc)
}

func logEventMongo(dp *DataProxyClient, pipelineID string, e TelemetryEvent) error {
	doc := map[string]interface{}{
		"pipeline_id": pipelineID,
		"event_type":  e.Type,
		"payload":     e.Payload,
		"timestamp":   time.Unix(e.Timestamp, 0).Format(time.RFC3339),
	}
	return dp.insertOne(colEvents, doc)
}

func insertDlqRecordMongo(dp *DataProxyClient, pipelineID, traceID, errMsg, step, payload string) error {
	doc := map[string]interface{}{
		"pipeline_id": pipelineID,
		"trace_id":    traceID,
		"error":       errMsg,
		"step":        step,
		"payload":     payload,
		"status":      "pending",
		"created_at":  time.Now().Format(time.RFC3339),
	}
	if err := dp.insertOne(colDlqRecords, doc); err != nil {
		return err
	}
	// Enforce 10k ring buffer per pipeline
	enforceDlqMaxRecordsMongo(dp, pipelineID, 10000)
	return nil
}

func enforceDlqMaxRecordsMongo(dp *DataProxyClient, pipelineID string, maxRecords int) {
	// Get all records sorted by created_at, delete those beyond maxRecords
	docs, err := dp.findDocs(colDlqRecords, map[string]interface{}{"pipeline_id": pipelineID}, map[string]interface{}{"created_at": -1}, 0, int64(maxRecords))
	if err != nil || len(docs) == 0 {
		return
	}
	for _, d := range docs {
		dp.deleteByID(colDlqRecords, strVal(d["_id"]))
	}
}

func insertLifecycleEvent(dp *DataProxyClient, pipelineID, event, version, message string) {
	doc := map[string]interface{}{
		"pipeline_id": pipelineID,
		"event":       event,
		"version":     version,
		"message":     message,
		"timestamp":   time.Now().Format(time.RFC3339),
	}
	dp.insertOne(colLifecycleEvents, doc)
}

func upsertMetricsBucket(dp *DataProxyClient, pipelineID, bucketTs string, recordsIn, recordsOut, recordsFailed, recordsFiltered, bytesProcessed int64) {
	doc := map[string]interface{}{
		"pipeline_id":      pipelineID,
		"bucket_ts":        bucketTs,
		"records_in":       recordsIn,
		"records_out":      recordsOut,
		"records_failed":   recordsFailed,
		"records_filtered": recordsFiltered,
		"bytes_processed":  bytesProcessed,
		"invocations":      1,
		"avg_duration_ms":  0,
		"max_duration_ms":  0,
	}
	dp.upsertByID(colMetricsBuckets, pipelineID+"_"+bucketTs, doc)
}

func recordExecutionMongo(dp *DataProxyClient, pipelineID, startedAt string, durationMs int64, status, errorMsg, logSnapshot string, recordsIn, recordsOut, recordsFailed, bytesProcessed int64) {
	doc := map[string]interface{}{
		"pipeline_id":     pipelineID,
		"started_at":      startedAt,
		"duration_ms":     durationMs,
		"status":          status,
		"error_msg":       errorMsg,
		"log_snapshot":    logSnapshot,
		"records_in":      recordsIn,
		"records_out":     recordsOut,
		"records_failed":  recordsFailed,
		"bytes_processed": bytesProcessed,
	}
	dp.insertOne(colExecutions, doc)
}

func getEventsMongo(dp *DataProxyClient, pipelineID string) ([]map[string]interface{}, error) {
	docs, err := dp.findDocs(colEvents, map[string]interface{}{"pipeline_id": pipelineID}, map[string]interface{}{"timestamp": -1}, 100, 0)
	if err != nil {
		return nil, err
	}
	events := make([]map[string]interface{}, 0, len(docs))
	for _, d := range docs {
		events = append(events, map[string]interface{}{
			"type":      d["event_type"],
			"payload":   d["payload"],
			"timestamp": d["timestamp"],
		})
	}
	return events, nil
}

func getAllEventsMongo(dp *DataProxyClient, limit int, since string) ([]map[string]interface{}, error) {
	filter := map[string]interface{}{}
	if since != "" {
		filter["timestamp"] = map[string]interface{}{"$gt": since}
	}
	docs, err := dp.findDocs(colEvents, filter, map[string]interface{}{"timestamp": -1}, int64(limit), 0)
	if err != nil {
		return nil, err
	}
	events := make([]map[string]interface{}, 0, len(docs))
	for _, d := range docs {
		events = append(events, map[string]interface{}{
			"pipeline_id": d["pipeline_id"],
			"type":        d["event_type"],
			"payload":     d["payload"],
			"timestamp":   d["timestamp"],
		})
	}
	return events, nil
}

func getExecutionHistoryMongo(dp *DataProxyClient, pipelineID string, limit int) ([]map[string]interface{}, error) {
	docs, err := dp.findDocs(colExecutions, map[string]interface{}{"pipeline_id": pipelineID}, map[string]interface{}{"started_at": -1}, int64(limit), 0)
	if err != nil {
		return nil, err
	}
	records := make([]map[string]interface{}, 0, len(docs))
	for _, d := range docs {
		records = append(records, map[string]interface{}{
			"id":              d["_id"],
			"pipeline_id":     d["pipeline_id"],
			"started_at":      d["started_at"],
			"duration_ms":     toInt64(d["duration_ms"]),
			"status":          d["status"],
			"error_msg":       strVal(d["error_msg"]),
			"log_snapshot":    strVal(d["log_snapshot"]),
			"records_in":      toInt64(d["records_in"]),
			"records_out":     toInt64(d["records_out"]),
			"records_failed":  toInt64(d["records_failed"]),
			"bytes_processed": toInt64(d["bytes_processed"]),
		})
	}
	return records, nil
}

func getDlqRecordsMongo(dp *DataProxyClient, pipelineID, status string, limit int) ([]DlqRecord, error) {
	filter := map[string]interface{}{}
	if pipelineID != "" {
		filter["pipeline_id"] = pipelineID
	}
	if status != "" {
		filter["status"] = status
	}
	docs, err := dp.findDocs(colDlqRecords, filter, map[string]interface{}{"created_at": -1}, int64(limit), 0)
	if err != nil {
		return nil, err
	}
	records := make([]DlqRecord, 0, len(docs))
	for _, d := range docs {
		records = append(records, DlqRecord{
			ID:         strVal(d["_id"]),
			PipelineID: strVal(d["pipeline_id"]),
			TraceID:    strVal(d["trace_id"]),
			Error:      strVal(d["error"]),
			Step:       strVal(d["step"]),
			Payload:    strVal(d["payload"]),
			Status:     strVal(d["status"]),
			CreatedAt:  strVal(d["created_at"]),
			ReplayedAt: strVal(d["replayed_at"]),
		})
	}
	return records, nil
}

func getDlqRecordMongo(dp *DataProxyClient, id string) (*DlqRecord, error) {
	doc, err := dp.findOneDoc(colDlqRecords, id)
	if err != nil || doc == nil {
		return nil, fmt.Errorf("DLQ record not found")
	}
	return &DlqRecord{
		ID:         strVal(doc["_id"]),
		PipelineID: strVal(doc["pipeline_id"]),
		TraceID:    strVal(doc["trace_id"]),
		Error:      strVal(doc["error"]),
		Step:       strVal(doc["step"]),
		Payload:    strVal(doc["payload"]),
		Status:     strVal(doc["status"]),
		CreatedAt:  strVal(doc["created_at"]),
		ReplayedAt: strVal(doc["replayed_at"]),
	}, nil
}

func getDlqSummaryMongo(dp *DataProxyClient) ([]map[string]interface{}, error) {
	results, err := dp.aggregate(colDlqRecords, []map[string]interface{}{
		{"$group": map[string]interface{}{
			"_id":   map[string]interface{}{"pipeline_id": "$pipeline_id", "status": "$status"},
			"count": map[string]interface{}{"$sum": 1},
		}},
		{"$sort": map[string]interface{}{"_id.pipeline_id": 1}},
	})
	if err != nil {
		return nil, err
	}
	out := make([]map[string]interface{}, 0, len(results))
	for _, r := range results {
		idMap, _ := r["_id"].(map[string]interface{})
		out = append(out, map[string]interface{}{
			"pipeline_id": idMap["pipeline_id"],
			"status":      idMap["status"],
			"count":       r["count"],
		})
	}
	return out, nil
}

func updateDlqStatusMongo(dp *DataProxyClient, id, status string) error {
	update := map[string]interface{}{"status": status}
	if status == "replayed" {
		update["replayed_at"] = time.Now().Format(time.RFC3339)
	}
	return dp.updateByID(colDlqRecords, id, update)
}

func replayAllDlqMongo(dp *DataProxyClient, pipelineID string) (int64, error) {
	filter := map[string]interface{}{"pipeline_id": pipelineID, "status": "pending"}
	update := map[string]interface{}{
		"$set": map[string]interface{}{
			"status":      "replayed",
			"replayed_at": time.Now().Format(time.RFC3339),
		},
	}
	if err := dp.updateMany(colDlqRecords, filter, update); err != nil {
		return 0, err
	}
	count, _ := dp.countDocs(colDlqRecords, filter)
	return count, nil
}

func validateAPIKeyMongo(dp *DataProxyClient, rawKey string) (tenantID string, keyID string, err error) {
	keyHash := hashAPIKey(rawKey)
	docs, err := dp.findDocs(colApiKeys, map[string]interface{}{"key_hash": keyHash}, nil, 1, 0)
	if err != nil || len(docs) == 0 {
		return "", "", fmt.Errorf("invalid API key")
	}
	tenantID = strVal(docs[0]["tenant_id"])
	keyID = strVal(docs[0]["_id"])
	return tenantID, keyID, nil
}

func getPipelinesTelemetryOnly(dp *DataProxyClient) ([]Pipeline, error) {
	docs, err := dp.findDocs(colTelemetryState, nil, nil, 0, 0)
	if err != nil {
		return nil, err
	}
	pipelines := make([]Pipeline, 0, len(docs))
	for _, d := range docs {
		p := Pipeline{
			ID:              strVal(d["pipeline_id"]),
			Status:          "UNKNOWN",
			UptimeMs:        toInt64(d["uptime_ms"]),
			ProgressCurrent: toInt64(d["progress_current"]),
			ProgressTotal:   toInt64(d["progress_total"]),
			RecordsIn:       toInt64(d["records_in"]),
			RecordsOut:      toInt64(d["records_out"]),
			RecordsFailed:   toInt64(d["records_failed"]),
			BytesProcessed:  toInt64(d["bytes_processed"]),
			LastSeen:        strVal(d["last_seen"]),
		}
		if p.ProgressTotal > 0 {
			p.ProgressPercent = float64(p.ProgressCurrent) / float64(p.ProgressTotal) * 100
		}
		if ls := p.LastSeen; ls != "" {
			if t, err := time.Parse(time.RFC3339, ls); err == nil && time.Since(t) > 30*time.Second {
				p.Status = "ZOMBIE"
			}
		}
		if p.Status == "UNKNOWN" {
			p.Status = "PENDING"
		}
		pipelines = append(pipelines, p)
	}
	return pipelines, nil
}

func getMergedPipelinesMongo(dp *DataProxyClient, k8s *K8sClient, namespace string) ([]Pipeline, error) {
	crs, err := k8s.GetPipelines(namespace)
	if err != nil {
		return nil, fmt.Errorf("fetching pipeline CRs: %w", err)
	}

	pipelines := make([]Pipeline, 0)
	for _, cr := range crs {
		p := crToPipeline(cr)
		mergeTelemetryMongo(dp, &p)

		if pods, err := k8s.GetPodsForPipeline(namespace, cr.Metadata.Name); err == nil {
			summary := PodSummary{Total: len(pods)}
			for _, pod := range pods {
				if len(pod.Status.ContainerStatuses) > 0 && pod.Status.ContainerStatuses[0].Ready {
					summary.Ready++
				}
				if pod.Status.Phase == "Failed" || (len(pod.Status.ContainerStatuses) > 0 && pod.Status.ContainerStatuses[0].RestartCount > 3) {
					summary.Crashing++
				}
			}
			p.PodSummary = &summary
		}

		if metrics, err := k8s.GetPodMetrics(namespace, cr.Metadata.Name); err == nil {
			for _, pm := range metrics {
				for _, c := range pm.Containers {
					p.CPUMillicores += parseK8sQuantity(c.Usage["cpu"], true)
					p.MemoryBytes += parseK8sQuantity(c.Usage["memory"], false)
				}
			}
		}

		if jobs, err := k8s.GetBuildsForPipeline(namespace, cr.Metadata.Name); err == nil && len(jobs) > 0 {
			job := jobs[len(jobs)-1]
			p.LastBuild = jobToBuildSummary(job)
		}

		if p.Mode == "stream" {
			p.ExecStats = getStreamExecStats(p)
		} else {
			p.ExecStats = getExecutionStatsMongo(dp, p.ID)
		}
		p.LatestSdkVersion = latestSDKVersion()

		pipelines = append(pipelines, p)
	}

	return pipelines, nil
}

func getMergedPipelineMongo(dp *DataProxyClient, k8s *K8sClient, namespace, name string) (*Pipeline, error) {
	cr, err := k8s.GetPipeline(namespace, name)
	if err != nil {
		return nil, err
	}

	p := crToPipeline(*cr)
	mergeTelemetryMongo(dp, &p)

	if pods, err := k8s.GetPodsForPipeline(namespace, name); err == nil {
		summary := PodSummary{Total: len(pods)}
		for _, pod := range pods {
			if len(pod.Status.ContainerStatuses) > 0 && pod.Status.ContainerStatuses[0].Ready {
				summary.Ready++
			}
			if pod.Status.Phase == "Failed" || (len(pod.Status.ContainerStatuses) > 0 && pod.Status.ContainerStatuses[0].RestartCount > 3) {
				summary.Crashing++
			}
		}
		p.PodSummary = &summary
	}

	if metrics, err := k8s.GetPodMetrics(namespace, name); err == nil {
		for _, pm := range metrics {
			for _, c := range pm.Containers {
				p.CPUMillicores += parseK8sQuantity(c.Usage["cpu"], true)
				p.MemoryBytes += parseK8sQuantity(c.Usage["memory"], false)
			}
		}
	}

	if jobs, err := k8s.GetBuildsForPipeline(namespace, name); err == nil && len(jobs) > 0 {
		job := jobs[len(jobs)-1]
		p.LastBuild = jobToBuildSummary(job)
	}

	if p.Mode == "stream" {
		p.ExecStats = getStreamExecStats(p)
	} else {
		p.ExecStats = getExecutionStatsMongo(dp, p.ID)
	}
	p.LatestSdkVersion = latestSDKVersion()

	return &p, nil
}

func mergeTelemetryMongo(dp *DataProxyClient, p *Pipeline) {
	// For DAG pipelines with stages, each stage reports telemetry under its own ID
	// (e.g. "bluesky-sieve-ingestor", "bluesky-sieve-ai-workers").
	// We aggregate all stage docs and fall back to the parent pipeline ID doc.
	stageIDs := stageIDsForPipeline(p)

	var teleDocs []map[string]interface{}
	for _, sid := range stageIDs {
		doc, err := dp.findOneDoc(colTelemetryState, sid)
		if err == nil && doc != nil {
			teleDocs = append(teleDocs, doc)
		}
	}
	if len(teleDocs) == 0 {
		return
	}

	// Aggregate metrics across all stage docs
	var maxUptime int64
	for _, doc := range teleDocs {
		p.RecordsIn += toInt64(doc["records_in"])
		p.RecordsOut += toInt64(doc["records_out"])
		p.RecordsFailed += toInt64(doc["records_failed"])
		p.RecordsFiltered += toInt64(doc["records_filtered"])
		p.BytesProcessed += toInt64(doc["bytes_processed"])
		p.ProgressCurrent += toInt64(doc["progress_current"])
		p.ProgressTotal += toInt64(doc["progress_total"])
		if u := toInt64(doc["uptime_ms"]); u > maxUptime {
			maxUptime = u
		}
		if p.CPUMillicores == 0 {
			p.CPUMillicores += toInt64(doc["cpu_usage_nanocores"]) / 1_000_000
		}
		if p.MemoryBytes == 0 {
			p.MemoryBytes += toInt64(doc["memory_bytes"])
		}
	}
	p.UptimeMs = maxUptime
	if p.ProgressTotal > 0 {
		p.ProgressPercent = float64(p.ProgressCurrent) / float64(p.ProgressTotal) * 100
	}

	// Use the most recent last_seen across all stage docs for liveness
	var latestSeen time.Time
	for _, doc := range teleDocs {
		if ls := strVal(doc["last_seen"]); ls != "" {
			if t, err := time.Parse(time.RFC3339, ls); err == nil {
				if t.After(latestSeen) {
					latestSeen = t
					p.LastSeen = ls
				}
			}
		}
	}
	if !latestSeen.IsZero() && p.Mode == "stream" {
		if time.Since(latestSeen) <= 30*time.Second {
			p.Status = "Streaming"
		} else {
			p.Status = "ZOMBIE"
		}
	}

	// Compute records/sec from last 5 minutes across all stage IDs
	var allBucketDocs []map[string]interface{}
	for _, sid := range stageIDs {
		buckets, _ := dp.findDocs(colMetricsBuckets, map[string]interface{}{
			"pipeline_id": sid,
			"bucket_ts":   map[string]interface{}{"$gte": time.Now().UTC().Add(-5 * time.Minute).Format(time.RFC3339)},
		}, nil, 0, 0)
		allBucketDocs = append(allBucketDocs, buckets...)
	}
	if len(allBucketDocs) > 0 {
		var sumIn int64
		for _, d := range allBucketDocs {
			sumIn += toInt64(d["records_in"])
		}
		windowSec := float64(len(allBucketDocs)) * 60.0
		p.RecordsPerSec = math.Round(float64(sumIn)/windowSec*10) / 10
	}
}

// stageIDsForPipeline returns the telemetry IDs to query for a pipeline.
// For DAG pipelines with stages, returns "{pipeline}-{stage}" for each stage
// plus the parent pipeline ID as fallback. For simple pipelines, returns just the ID.
func stageIDsForPipeline(p *Pipeline) []string {
	if len(p.Stages) == 0 {
		return []string{p.ID}
	}
	ids := make([]string, 0, len(p.Stages)+1)
	for _, s := range p.Stages {
		ids = append(ids, fmt.Sprintf("%s-%s", p.ID, s.Name))
	}
	// Also check the parent pipeline ID (legacy telemetry or single-stage fallback)
	ids = append(ids, p.ID)
	return ids
}

func getExecutionStatsMongo(dp *DataProxyClient, pipelineID string) *ExecutionStats {
	docs, err := dp.findDocs(colExecutions, map[string]interface{}{"pipeline_id": pipelineID}, nil, 0, 0)
	if err != nil || len(docs) == 0 {
		return &ExecutionStats{}
	}

	stats := &ExecutionStats{TotalRuns: int64(len(docs))}
	var durations []int64
	var totalDuration int64
	var maxDuration int64
	var failures int64
	var lastRun string

	for _, d := range docs {
		dur := toInt64(d["duration_ms"])
		if dur > 0 {
			durations = append(durations, dur)
			totalDuration += dur
			if dur > maxDuration {
				maxDuration = dur
			}
		}
		if strVal(d["status"]) == "failed" {
			failures++
		}
		if startedAt := strVal(d["started_at"]); startedAt > lastRun {
			lastRun = startedAt
		}
	}

	if stats.TotalRuns > 0 {
		stats.AvgRuntime = totalDuration / stats.TotalRuns
	}
	stats.MaxRuntime = maxDuration
	stats.Failures = failures
	stats.LastRun = lastRun
	if stats.TotalRuns > 0 {
		stats.FailRate = math.Round(float64(stats.Failures)/float64(stats.TotalRuns)*1000) / 10
	}

	if len(durations) > 0 {
		sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
		stats.P50Runtime = percentile(durations, 50)
		stats.P99Runtime = percentile(durations, 99)
	}

	return stats
}

func syncBuildHistoryMongo(dp *DataProxyClient, k8s *K8sClient, namespace string) {
	crs, err := k8s.GetPipelines(namespace)
	if err != nil {
		log.Printf("[build-sync] Failed to get pipelines: %v", err)
		return
	}

	for _, cr := range crs {
		pipelineID := cr.Metadata.Name
		pipelineName := pipelineID
		gitRepo := cr.Spec.GitRepository
		gitRef := cr.Spec.Reference
		path := cr.Spec.Path

		jobs, err := k8s.GetBuildsForPipeline(namespace, pipelineID)
		if err != nil {
			continue
		}

		for _, job := range jobs {
			jobName := job.Metadata.Name
			targetImage := fmt.Sprintf("clotho-registry.clotho-system.svc.cluster.local:5000/%s:%s", pipelineID, gitRef)

			status := "pending"
			if job.Status.Succeeded > 0 {
				status = "completed"
			} else if job.Status.Failed > 0 {
				status = "failed"
			} else if job.Status.Active > 0 {
				status = "running"
			}

			startedAt := job.Status.StartTime
			finishedAt := job.Status.CompletionTime

			var durationMs int64
			if startedAt != "" && finishedAt != "" {
				if start, err := time.Parse(time.RFC3339, startedAt); err == nil {
					if end, err := time.Parse(time.RFC3339, finishedAt); err == nil {
						durationMs = int64(end.Sub(start).Milliseconds())
					}
				}
			}

			doc := map[string]interface{}{
				"pipeline_id":    pipelineID,
				"pipeline_name":  pipelineName,
				"job_name":       jobName,
				"git_repository": gitRepo,
				"reference":      gitRef,
				"path":           path,
				"target_image":   targetImage,
				"status":         status,
				"started_at":     startedAt,
				"finished_at":    finishedAt,
				"duration_ms":    durationMs,
				"created_at":     time.Now().Format(time.RFC3339),
			}
			dp.upsertByID(colBuildHistory, jobName, doc)

			if status == "completed" {
				dp.upsertByID(colPipelines, pipelineID, map[string]interface{}{
					"id":    pipelineID,
					"image": targetImage,
				})
			}
		}
	}

	log.Printf("[build-sync] Synced build history for %d pipelines in %s", len(crs), namespace)
}

// ═══════════════════════════════════════════════════════════════════════════════
// Utility functions (carried over from SQLite version)
// ═══════════════════════════════════════════════════════════════════════════════

func toInt64(v interface{}) int64 {
	switch val := v.(type) {
	case float64:
		return int64(val)
	case int64:
		return val
	case int:
		return int64(val)
	case json.Number:
		if i, err := val.Int64(); err == nil {
			return i
		}
	case nil:
		return 0
	}
	return 0
}

func strVal(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func truncateToMinute(ts string) string {
	t, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		t = time.Now().UTC()
	}
	return t.Truncate(time.Minute).UTC().Format(time.RFC3339)
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func percentile(sorted []int64, p int) int64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(float64(p)/100*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func getStreamExecStats(p Pipeline) *ExecutionStats {
	stats := &ExecutionStats{}
	if p.Status == "Streaming" {
		stats.TotalRuns = 1
	}
	stats.Failures = p.RecordsFailed
	if p.RecordsIn > 0 {
		stats.FailRate = math.Round(float64(p.RecordsFailed)/float64(p.RecordsIn)*1000) / 10
	}
	stats.LastRun = p.LastSeen
	return stats
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// latestSDKVersion returns the canonical clotho-sdk version that new builds should use.
// Set via CLOTHO_SDK_LATEST_VERSION env var (injected at deploy time from clotho-sdk/Cargo.toml).
// Falls back to a hardcoded value so the API is always functional even without the env var.
func latestSDKVersion() string {
	if v := os.Getenv("CLOTHO_SDK_LATEST_VERSION"); v != "" {
		return v
	}
	return "0.0.1-alpha.1"
}

func sanitizeForK8s(input string) string {
	replacer := strings.NewReplacer("/", "-", ":", "-", "@", "-", " ", "-", "_", "-")
	s := replacer.Replace(strings.ToLower(input))
	s = strings.Trim(s, "-.")
	if len(s) > 50 {
		s = s[:50]
	}
	if s == "" {
		return "default"
	}
	return s
}

func generateAPIKey() (rawKey, keyHash, keyPrefix string, err error) {
	b := make([]byte, 32)
	if _, err = rand.Read(b); err != nil {
		return "", "", "", err
	}
	rawKey = "cl_opt_" + hex.EncodeToString(b)
	keyPrefix = rawKey[:14] + "..."
	keyHash = hashAPIKey(rawKey)
	return rawKey, keyHash, keyPrefix, nil
}

func hashAPIKey(rawKey string) string {
	h := sha256.Sum256([]byte(rawKey))
	return hex.EncodeToString(h[:])
}

// upsertStepMetricsMongo persists a step metrics document to MongoDB.
// Doc ID: "<pipeline_id>__<step_name>" to allow per-step upserts.
func upsertStepMetricsMongo(dp *DataProxyClient, pipelineID string, sm *StepMetrics) {
	docID := pipelineID + "__" + sm.StepName
	doc := map[string]interface{}{
		"pipeline_id":      pipelineID,
		"stage_name":       sm.StageName,
		"step_name":        sm.StepName,
		"step_type":        sm.StepType,
		"records_in":       sm.RecordsIn,
		"records_out":      sm.RecordsOut,
		"records_failed":   sm.RecordsFailed,
		"records_filtered": sm.RecordsFiltered,
		"records_branched": sm.RecordsBranched,
		"duration_ms":      sm.DurationMs,
		"timestamp":        sm.Timestamp,
	}
	if err := dp.upsertByID(colStepMetrics, docID, doc); err != nil {
		log.Printf("[step_metrics] upsert failed for %s: %v", docID, err)
	}
}

// seedStepMetricsFromMongo loads all persisted step metrics into the in-memory store.
// Called once at startup so the store survives API restarts.
func seedStepMetricsFromMongo(dp *DataProxyClient) {
	docs, err := dp.findDocs(colStepMetrics, nil, nil, 0, 0)
	if err != nil {
		log.Printf("[step_metrics] seed failed: %v", err)
		return
	}

	stepMetricsMu.Lock()
	defer stepMetricsMu.Unlock()

	for _, doc := range docs {
		pid, _ := doc["pipeline_id"].(string)
		stepName, _ := doc["step_name"].(string)
		if pid == "" || stepName == "" {
			continue
		}

		sm := &StepMetrics{
			PipelineID:      pid,
			StageName:       getString(doc, "stage_name"),
			StepName:        stepName,
			StepType:        getString(doc, "step_type"),
			RecordsIn:       getInt64(doc, "records_in"),
			RecordsOut:      getInt64(doc, "records_out"),
			RecordsFailed:   getInt64(doc, "records_failed"),
			RecordsFiltered: getInt64(doc, "records_filtered"),
			RecordsBranched: getInt64(doc, "records_branched"),
			DurationMs:      getInt64(doc, "duration_ms"),
			Timestamp:       getInt64(doc, "timestamp"),
		}

		if _, ok := stepMetricsStore[pid]; !ok {
			stepMetricsStore[pid] = make(map[string]*StepMetrics)
		}
		stepMetricsStore[pid][stepName] = sm
	}
	log.Printf("[step_metrics] seeded %d step metrics docs from MongoDB", len(docs))
}

func getString(doc map[string]interface{}, key string) string {
	v, _ := doc[key].(string)
	return v
}

func getInt64(doc map[string]interface{}, key string) int64 {
	switch v := doc[key].(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	case int:
		return int64(v)
	}
	return 0
}
