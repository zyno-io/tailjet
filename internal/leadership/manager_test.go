package leadership

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"github.com/zyno-io/tailjet/internal/health"
)

func TestLeaseElectionAllowsOneLeaderAndFailsOverWithoutEarlyRelease(t *testing.T) {
	client := fake.NewSimpleClientset()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	elected := make(chan string, 2)
	var active atomic.Int32
	var maximum atomic.Int32

	worker := func(identity string) func(context.Context) error {
		return func(ctx context.Context) error {
			current := active.Add(1)
			for {
				observed := maximum.Load()
				if current <= observed || maximum.CompareAndSwap(observed, current) {
					break
				}
			}
			elected <- identity
			<-ctx.Done()
			active.Add(-1)
			return context.Cause(ctx)
		}
	}

	type candidate struct {
		identity string
		cancel   context.CancelFunc
		result   chan error
	}
	candidates := map[string]*candidate{}
	for _, identity := range []string{"tailjet-1", "tailjet-2"} {
		settings := Settings{
			Namespace:     "tailjet-system",
			Name:          "tailjet",
			Identity:      identity,
			LeaseDuration: 2 * time.Second,
			RenewDeadline: 1200 * time.Millisecond,
			RetryPeriod:   200 * time.Millisecond,
		}
		lock := &resourcelock.LeaseLock{
			LeaseMeta:  metav1.ObjectMeta{Namespace: settings.Namespace, Name: settings.Name},
			Client:     client.CoordinationV1(),
			LockConfig: resourcelock.ResourceLockConfig{Identity: identity},
		}
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		manager := newManager(settings, lock, health.NewTracker(), logger)
		go func() { result <- manager.Run(ctx, worker(identity)) }()
		candidates[identity] = &candidate{identity: identity, cancel: cancel, result: result}
	}
	t.Cleanup(func() {
		for _, candidate := range candidates {
			candidate.cancel()
		}
	})

	first := receiveIdentity(t, elected, 5*time.Second)
	firstCandidate := candidates[first]
	firstCandidate.cancel()
	select {
	case <-firstCandidate.result:
	case <-time.After(3 * time.Second):
		t.Fatal("first leader did not stop")
	}

	lease, err := client.CoordinationV1().Leases("tailjet-system").Get(context.Background(), "tailjet", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != first {
		t.Fatalf("lease was released early: %#v", lease.Spec.HolderIdentity)
	}

	second := receiveIdentity(t, elected, 6*time.Second)
	if second == first {
		t.Fatalf("same candidate was elected twice: %s", first)
	}
	if maximum.Load() != 1 {
		t.Fatalf("maximum simultaneous leaders = %d", maximum.Load())
	}

	candidates[second].cancel()
	select {
	case <-candidates[second].result:
	case <-time.After(3 * time.Second):
		t.Fatal("second leader did not stop")
	}
}

func receiveIdentity(t *testing.T, identities <-chan string, timeout time.Duration) string {
	t.Helper()
	select {
	case identity := <-identities:
		return identity
	case <-time.After(timeout):
		t.Fatal("timed out waiting for leader election")
		return ""
	}
}
