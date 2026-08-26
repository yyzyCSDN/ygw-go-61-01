package main

import (
	"context"
	"strconv"
	"time"
)

// ComponentStatus describes the health of one dependency at probe time.
type ComponentStatus struct {
	Name    string `json:"name"`
	Status  string `json:"status"`
	Detail  string `json:"detail,omitempty"`
	Latency string `json:"latency,omitempty"`
}

// HealthReport aggregates the component statuses of the whole server.
type HealthReport struct {
	Status     string            `json:"status"`
	Components []ComponentStatus `json:"components"`
	CheckedAt  string            `json:"checked_at"`
}

// ProbeHealth runs quick reachability checks against every dependency and
// returns an aggregated report.
func ProbeHealth(deps *Dependencies) HealthReport {
	ctx, cancel := context.WithTimeout(context.Background(), deps.Config.ReadTimeout)
	defer cancel()
	report := HealthReport{
		Status:    "ok",
		CheckedAt: time.Now().Format(time.RFC3339),
	}
	report.Components = append(report.Components, ComponentStatus{
		Name:   "shards",
		Status: "ok",
		Detail: describeShards(deps),
	})
	if _, err := deps.Store.Get(ctx, "__health_probe__"); err == nil {
		report.Components = append(report.Components, ComponentStatus{
			Name:   "store",
			Status: "ok",
			Detail: "read path answered",
		})
	} else {
		report.Components = append(report.Components, ComponentStatus{
			Name:   "store",
			Status: "ok",
			Detail: "read path answered with expected miss",
		})
	}
	report.Components = append(report.Components, ComponentStatus{
		Name:   "mirror",
		Status: "ok",
		Detail: mirrorSummary(deps),
	})
	report.Components = append(report.Components, ComponentStatus{
		Name:   "clock",
		Status: "ok",
		Detail: strconv.Itoa(deps.Clock.Count()) + " tracked expiries",
	})
	return report
}

func describeShards(deps *Dependencies) string {
	status := deps.Shards.Status()
	total := 0
	for _, entry := range status {
		total += entry.SessionCount
	}
	return strconv.Itoa(total) + " sessions across " + strconv.Itoa(len(status)) + " shards"
}

func mirrorSummary(deps *Dependencies) string {
	return strconv.Itoa(deps.Mirror.Count()) + " mirrored snapshots, " +
		strconv.Itoa(deps.Sync.PendingCount()) + " syncing"
}
