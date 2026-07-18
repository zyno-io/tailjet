package leadership

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coordinationv1client "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"github.com/zyno-io/tailjet/internal/health"
)

type Settings struct {
	Namespace     string
	Name          string
	Identity      string
	LeaseDuration time.Duration
	RenewDeadline time.Duration
	RetryPeriod   time.Duration
}

type Manager struct {
	settings Settings
	lock     resourcelock.Interface
	health   *health.Tracker
	logger   *slog.Logger
}

func NewInCluster(settings Settings, tracker *health.Tracker, logger *slog.Logger, userAgent string) (*Manager, error) {
	kubeConfig, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("load in-cluster Kubernetes configuration: %w", err)
	}
	kubeConfig.UserAgent = userAgent
	kubeConfig.Timeout = settings.RenewDeadline / 2
	if kubeConfig.Timeout < time.Second {
		kubeConfig.Timeout = time.Second
	}
	client, err := coordinationv1client.NewForConfig(kubeConfig)
	if err != nil {
		return nil, fmt.Errorf("create Kubernetes coordination client: %w", err)
	}
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Namespace: settings.Namespace,
			Name:      settings.Name,
		},
		Client: client,
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: settings.Identity,
		},
		Labels: map[string]string{
			"app.kubernetes.io/name":       "tailjet",
			"app.kubernetes.io/managed-by": "tailjet",
		},
	}
	return newManager(settings, lock, tracker, logger), nil
}

func newManager(settings Settings, lock resourcelock.Interface, tracker *health.Tracker, logger *slog.Logger) *Manager {
	return &Manager{settings: settings, lock: lock, health: tracker, logger: logger}
}

func (m *Manager) Run(ctx context.Context, runLeader func(context.Context) error) error {
	electionCtx, cancelElection := context.WithCancel(ctx)
	defer cancelElection()

	var leading atomic.Bool
	workerDone := make(chan error, 1)
	elector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:            m.lock,
		LeaseDuration:   m.settings.LeaseDuration,
		RenewDeadline:   m.settings.RenewDeadline,
		RetryPeriod:     m.settings.RetryPeriod,
		ReleaseOnCancel: false,
		Name:            m.settings.Name,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(leaderCtx context.Context) {
				leading.Store(true)
				m.health.SetPhase("starting-leader", false, true, nil)
				m.logger.Info("acquired Kubernetes leader lease", "namespace", m.settings.Namespace, "lease", m.settings.Name, "identity", m.settings.Identity)
				workerDone <- runLeader(leaderCtx)
				cancelElection()
			},
			OnStoppedLeading: func() {
				if leading.Load() && ctx.Err() == nil {
					lostErr := errors.New("kubernetes leader lease renewal stopped")
					m.health.SetPhase("leadership-lost", false, false, lostErr)
					m.logger.Error("lost Kubernetes leader lease", "namespace", m.settings.Namespace, "lease", m.settings.Name, "identity", m.settings.Identity)
				}
			},
			OnNewLeader: func(identity string) {
				if identity == m.settings.Identity || leading.Load() {
					return
				}
				m.health.SetPhase("standby", true, false, nil)
				m.logger.Info("observed Kubernetes lease leader", "namespace", m.settings.Namespace, "lease", m.settings.Name, "identity", identity)
			},
		},
	})
	if err != nil {
		return fmt.Errorf("configure Kubernetes leader election: %w", err)
	}

	m.health.SetPhase("waiting-for-leader-election", false, false, nil)
	m.logger.Info("starting Kubernetes leader election", "namespace", m.settings.Namespace, "lease", m.settings.Name, "identity", m.settings.Identity)
	elector.Run(electionCtx)
	cancelElection()

	var workerErr error
	if leading.Load() {
		workerErr = <-workerDone
		leading.Store(false)
	}
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	if workerErr != nil && !errors.Is(workerErr, context.Canceled) {
		return fmt.Errorf("leader worker stopped: %w", workerErr)
	}
	if workerErr == nil && elector.IsLeader() {
		return errors.New("leader worker stopped unexpectedly")
	}
	return errors.New("lost Kubernetes leader lease")
}
