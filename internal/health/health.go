package health

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type Status struct {
	Ready               bool       `json:"ready"`
	Leader              bool       `json:"leader"`
	Phase               string     `json:"phase"`
	LastError           string     `json:"lastError,omitempty"`
	LastPublishedAt     *time.Time `json:"lastPublishedAt,omitempty"`
	LastCheckpointAt    *time.Time `json:"lastCheckpointAt,omitempty"`
	PublishedMessages   uint64     `json:"publishedMessages"`
	ErrorsTotal         uint64     `json:"errorsTotal"`
	PublishErrorsTotal  uint64     `json:"publishErrorsTotal"`
	RetryRequestsTotal  uint64     `json:"retryRequestsTotal"`
	RetrySuccessesTotal uint64     `json:"retrySuccessesTotal"`
	FailedRows          int64      `json:"failedRows"`
}

type Tracker struct {
	mu     sync.RWMutex
	status Status
}

func NewTracker() *Tracker {
	return &Tracker{status: Status{Phase: "starting"}}
}

func (t *Tracker) SetPhase(phase string, ready, leader bool, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.status.Phase = phase
	t.status.Ready = ready
	t.status.Leader = leader
	if err == nil {
		t.status.LastError = ""
	} else {
		t.status.LastError = err.Error()
		t.status.ErrorsTotal++
	}
}

func (t *Tracker) Published(count int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.status.PublishedMessages += uint64(count)
	now := time.Now().UTC()
	t.status.LastPublishedAt = &now
}

func (t *Tracker) Checkpointed() {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now().UTC()
	t.status.LastCheckpointAt = &now
}

func (t *Tracker) SetFailedRows(count int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.status.FailedRows = count
}

func (t *Tracker) PublishFailed(newFailure bool, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.status.ErrorsTotal++
	t.status.PublishErrorsTotal++
	if newFailure {
		t.status.FailedRows++
	}
	if err != nil {
		t.status.LastError = err.Error()
	}
}

func (t *Tracker) RetryRequested() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.status.RetryRequestsTotal++
}

func (t *Tracker) RetrySucceeded(count int) {
	count64 := int64(count)
	t.mu.Lock()
	defer t.mu.Unlock()
	t.status.RetrySuccessesTotal += uint64(count)
	t.status.FailedRows -= count64
	if t.status.FailedRows < 0 {
		t.status.FailedRows = 0
	}
	if t.status.FailedRows == 0 {
		t.status.LastError = ""
	}
}

func (t *Tracker) Snapshot() Status {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.status
}

func (t *Tracker) Handler(triggerRetry func() bool) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /livez", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		status := t.Snapshot()
		w.Header().Set("Content-Type", "application/json")
		if !status.Ready {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(status)
	})
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(t.Snapshot())
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		status := t.Snapshot()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		writeMetric(w, "tailjet_ready", "Whether this pod is ready to serve as leader or standby.", "gauge", boolFloat(status.Ready))
		writeMetric(w, "tailjet_leader", "Whether this pod currently holds the MySQL leader lock.", "gauge", boolFloat(status.Leader))
		writeMetric(w, "tailjet_published_messages_total", "JetStream messages acknowledged by this process.", "counter", float64(status.PublishedMessages))
		writeMetric(w, "tailjet_errors_total", "Errors observed by this process.", "counter", float64(status.ErrorsTotal))
		writeMetric(w, "tailjet_publish_errors_total", "JetStream publish attempts that failed.", "counter", float64(status.PublishErrorsTotal))
		writeMetric(w, "tailjet_retry_requests_total", "Failed-row retry requests accepted by this process.", "counter", float64(status.RetryRequestsTotal))
		writeMetric(w, "tailjet_retry_successes_total", "Previously failed rows successfully published by this process.", "counter", float64(status.RetrySuccessesTotal))
		writeMetric(w, "tailjet_failed_rows", "Outbox rows currently retained after a publish failure.", "gauge", float64(status.FailedRows))
		writeMetric(w, "tailjet_last_published_timestamp_seconds", "Unix timestamp of the last acknowledged JetStream publish.", "gauge", timestamp(status.LastPublishedAt))
		writeMetric(w, "tailjet_last_checkpoint_timestamp_seconds", "Unix timestamp of the last durable MySQL checkpoint.", "gauge", timestamp(status.LastCheckpointAt))
		_, _ = fmt.Fprintf(w, "# HELP tailjet_phase_info Current Tailjet lifecycle phase.\n# TYPE tailjet_phase_info gauge\ntailjet_phase_info{phase=%q} 1\n", status.Phase)
	})
	if triggerRetry != nil {
		mux.HandleFunc("POST /retry", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if !t.Snapshot().Leader {
				w.WriteHeader(http.StatusConflict)
				_, _ = w.Write([]byte(`{"error":"retry must be sent to the active leader"}`))
				return
			}
			queued := triggerRetry()
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]bool{"queued": queued})
		})
	}
	return mux
}

func writeMetric(w http.ResponseWriter, name, help, metricType string, value float64) {
	_, _ = fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n%s %g\n", name, help, name, metricType, name, value)
}

func boolFloat(value bool) float64 {
	if value {
		return 1
	}
	return 0
}

func timestamp(value *time.Time) float64 {
	if value == nil {
		return 0
	}
	return float64(value.UnixNano()) / float64(time.Second)
}
