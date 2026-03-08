package telemetry

import (
	"context"
	"time"

	"github.com/brettnesbitt/clotho/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type FleetReporter struct {
	Client client.Client
	APIURL string // Your Clotho Backend URL
}

// Start runs in a goroutine alongside the controller
func (r *FleetReporter) Start(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)

	for {
		select {
		case <-ticker.C:
			r.report(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (r *FleetReporter) report(ctx context.Context) {
	// 1. List all Pipelines
	pipelines := &v1alpha1.PipelineList{}
	if err := r.Client.List(ctx, pipelines); err != nil {
		return
	}

	// 2. Aggregate Stats
	stats := map[string]int{
		"Total":   0,
		"Running": 0,
		"Failed":  0,
		"Pending": 0,
	}

	for _, p := range pipelines.Items {
		stats["Total"]++
		stats[p.Status.Phase]++ // Assuming Phase is a string
	}

	// 3. Push to Clotho API
	// POST https://api.clotho.run/v1/cluster/stats
	// Body: { "cluster_id": "gke-prod-1", "stats": stats }
}
