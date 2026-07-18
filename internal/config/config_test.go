package config

import (
	"strings"
	"testing"
	"time"
)

func TestValidateAcceptsProductionConfiguration(t *testing.T) {
	if err := validConfig().Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestValidateRejectsEmbeddedNATSCredentials(t *testing.T) {
	cfg := validConfig()
	cfg.NATSURL = "nats://user:secret@nats.example.com:4222"
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "must not contain credentials") {
		t.Fatalf("error = %v", err)
	}
}

func TestValidateRejectsUnsafeNames(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{"database identifier", func(cfg *Config) { cfg.MySQLDatabase = "app-name" }},
		{"consumer name", func(cfg *Config) { cfg.ConsumerName = "non-ascii-é" }},
		{"stream name", func(cfg *Config) { cfg.NATSExpectedStream = "events.stream" }},
		{"lease name", func(cfg *Config) { cfg.LeaderLeaseName = "Tailjet_Invalid" }},
		{"lease namespace", func(cfg *Config) { cfg.LeaderLeaseNamespace = "invalid.namespace" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := validConfig()
			test.mutate(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestValidateRejectsUnsafeLeaderElectionTiming(t *testing.T) {
	cfg := validConfig()
	cfg.LeaderLeaseDuration = 500 * time.Millisecond
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "at least 1s") {
		t.Fatalf("error = %v", err)
	}

	cfg = validConfig()
	cfg.LeaderRenewDeadline = cfg.LeaderLeaseDuration
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "LEASE_DURATION") {
		t.Fatalf("error = %v", err)
	}

	cfg = validConfig()
	cfg.LeaderRetryPeriod = cfg.LeaderRenewDeadline
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "RENEW_DEADLINE") {
		t.Fatalf("error = %v", err)
	}
}

func validConfig() Config {
	return Config{
		MySQLHost:             "mysql.example.com",
		MySQLPort:             3306,
		MySQLUser:             "tailjet",
		MySQLDatabase:         "app",
		MySQLFlavor:           "mysql",
		MySQLServerID:         240024,
		MySQLTLSMode:          "verify",
		OutboxTable:           "tailjet_outbox",
		StateTable:            "tailjet_state",
		ConsumerName:          "default",
		LeaderElectionMode:    "kubernetes",
		LeaderLeaseName:       "tailjet",
		LeaderLeaseNamespace:  "tailjet-system",
		LeaderIdentity:        "tailjet-example-1",
		NATSURL:               "tls://nats.example.com:4222",
		NATSExpectedStream:    "EVENTS",
		HTTPAddress:           ":8080",
		PublishTimeout:        10 * time.Second,
		CheckpointInterval:    5 * time.Second,
		RecoveryRetryInterval: 5 * time.Second,
		LeaderLeaseDuration:   15 * time.Second,
		LeaderRenewDeadline:   10 * time.Second,
		LeaderRetryPeriod:     2 * time.Second,
		SnapshotBatchSize:     500,
		MaxTransactionRows:    10000,
		MaxTransactionBytes:   64 << 20,
	}
}
