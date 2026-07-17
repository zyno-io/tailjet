package health

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetrics(t *testing.T) {
	tracker := NewTracker()
	tracker.SetPhase("streaming", true, true, nil)
	tracker.Published(2)
	tracker.Checkpointed()
	tracker.SetFailedRows(1)
	tracker.PublishFailed(false, errors.New("example error"))
	tracker.RetryRequested()

	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()
	tracker.Handler(nil).ServeHTTP(response, request)
	body, err := io.ReadAll(response.Result().Body)
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		"tailjet_ready 1",
		"tailjet_leader 1",
		"tailjet_published_messages_total 2",
		"tailjet_publish_errors_total 1",
		"tailjet_retry_requests_total 1",
		"tailjet_failed_rows 1",
		"tailjet_phase_info{phase=\"streaming\"} 1",
	} {
		if !strings.Contains(string(body), expected) {
			t.Fatalf("metrics do not contain %q:\n%s", expected, body)
		}
	}
}

func TestRetryRequiresLeader(t *testing.T) {
	tracker := NewTracker()
	called := false
	request := httptest.NewRequest(http.MethodPost, "/retry", nil)
	response := httptest.NewRecorder()
	tracker.Handler(func() bool {
		called = true
		return true
	}).ServeHTTP(response, request)

	if response.Code != http.StatusConflict {
		t.Fatalf("status = %d", response.Code)
	}
	if called {
		t.Fatal("standby invoked retry callback")
	}
}

func TestRetryAcceptedByLeader(t *testing.T) {
	tracker := NewTracker()
	tracker.SetPhase("streaming", true, true, nil)
	called := false
	request := httptest.NewRequest(http.MethodPost, "/retry", nil)
	response := httptest.NewRecorder()
	tracker.Handler(func() bool {
		called = true
		return true
	}).ServeHTTP(response, request)

	if response.Code != http.StatusAccepted {
		t.Fatalf("status = %d", response.Code)
	}
	if !called {
		t.Fatal("leader did not invoke retry callback")
	}
}

func TestSuccessfulRetryClearsResolvedError(t *testing.T) {
	tracker := NewTracker()
	tracker.PublishFailed(true, errors.New("temporary failure"))
	tracker.RetrySucceeded(1)
	status := tracker.Snapshot()
	if status.FailedRows != 0 || status.LastError != "" || status.RetrySuccessesTotal != 1 {
		t.Fatalf("status = %#v", status)
	}
}
