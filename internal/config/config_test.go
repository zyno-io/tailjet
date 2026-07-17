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

func validConfig() Config {
	return Config{
		MySQLHost:           "mysql.example.com",
		MySQLPort:           3306,
		MySQLUser:           "tailjet",
		MySQLDatabase:       "app",
		MySQLFlavor:         "mysql",
		MySQLServerID:       240024,
		MySQLTLSMode:        "verify",
		OutboxTable:         "tailjet_outbox",
		StateTable:          "tailjet_state",
		ConsumerName:        "default",
		LeaderLockName:      "tailjet:app:tailjet_outbox",
		NATSURL:             "tls://nats.example.com:4222",
		NATSExpectedStream:  "EVENTS",
		HTTPAddress:         ":8080",
		PublishTimeout:      10 * time.Second,
		CheckpointInterval:  5 * time.Second,
		LeaderRetryInterval: 5 * time.Second,
		SnapshotBatchSize:   500,
		MaxTransactionRows:  10000,
		MaxTransactionBytes: 64 << 20,
	}
}
