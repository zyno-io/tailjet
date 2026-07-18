package config

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
var consumerNamePattern = regexp.MustCompile(`^[A-Za-z0-9_.:-]+$`)
var dnsLabelPattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

type Config struct {
	MySQLHost             string
	MySQLPort             uint16
	MySQLUser             string
	MySQLPassword         string
	MySQLDatabase         string
	MySQLFlavor           string
	MySQLServerID         uint32
	MySQLTLSMode          string
	MySQLTLSCA            string
	MySQLTLSCert          string
	MySQLTLSKey           string
	OutboxTable           string
	StateTable            string
	ConsumerName          string
	LeaderElectionMode    string
	LeaderLeaseName       string
	LeaderLeaseNamespace  string
	LeaderIdentity        string
	NATSURL               string
	NATSUser              string
	NATSPassword          string
	NATSToken             string
	NATSCreds             string
	NATSTLSCA             string
	NATSTLSCert           string
	NATSTLSKey            string
	NATSExpectedStream    string
	HTTPAddress           string
	PublishTimeout        time.Duration
	CheckpointInterval    time.Duration
	RecoveryRetryInterval time.Duration
	LeaderLeaseDuration   time.Duration
	LeaderRenewDeadline   time.Duration
	LeaderRetryPeriod     time.Duration
	SnapshotBatchSize     int
	MaxTransactionRows    int
	MaxTransactionBytes   int64
}

func Load() (Config, error) {
	leaderIdentity, err := os.Hostname()
	if err != nil {
		return Config{}, fmt.Errorf("determine leader identity: %w", err)
	}
	cfg := Config{
		MySQLHost:            env("TAILJET_MYSQL_HOST", "127.0.0.1"),
		MySQLUser:            env("TAILJET_MYSQL_USER", "tailjet"),
		MySQLPassword:        os.Getenv("TAILJET_MYSQL_PASSWORD"),
		MySQLDatabase:        os.Getenv("TAILJET_MYSQL_DATABASE"),
		MySQLFlavor:          strings.ToLower(env("TAILJET_MYSQL_FLAVOR", "mysql")),
		MySQLTLSMode:         strings.ToLower(env("TAILJET_MYSQL_TLS_MODE", "disable")),
		MySQLTLSCA:           os.Getenv("TAILJET_MYSQL_TLS_CA"),
		MySQLTLSCert:         os.Getenv("TAILJET_MYSQL_TLS_CERT"),
		MySQLTLSKey:          os.Getenv("TAILJET_MYSQL_TLS_KEY"),
		OutboxTable:          env("TAILJET_OUTBOX_TABLE", "tailjet_outbox"),
		StateTable:           env("TAILJET_STATE_TABLE", "tailjet_state"),
		ConsumerName:         env("TAILJET_CONSUMER_NAME", "default"),
		LeaderElectionMode:   strings.ToLower(env("TAILJET_LEADER_ELECTION_MODE", "kubernetes")),
		LeaderLeaseNamespace: env("TAILJET_LEADER_LEASE_NAMESPACE", "default"),
		LeaderIdentity:       env("TAILJET_LEADER_IDENTITY", leaderIdentity),
		NATSURL:              env("TAILJET_NATS_URL", "nats://127.0.0.1:4222"),
		NATSUser:             os.Getenv("TAILJET_NATS_USER"),
		NATSPassword:         os.Getenv("TAILJET_NATS_PASSWORD"),
		NATSToken:            os.Getenv("TAILJET_NATS_TOKEN"),
		NATSCreds:            os.Getenv("TAILJET_NATS_CREDS"),
		NATSTLSCA:            os.Getenv("TAILJET_NATS_TLS_CA"),
		NATSTLSCert:          os.Getenv("TAILJET_NATS_TLS_CERT"),
		NATSTLSKey:           os.Getenv("TAILJET_NATS_TLS_KEY"),
		NATSExpectedStream:   os.Getenv("TAILJET_NATS_EXPECTED_STREAM"),
		HTTPAddress:          env("TAILJET_HTTP_ADDRESS", ":8080"),
	}

	if cfg.MySQLPort, err = uint16Env("TAILJET_MYSQL_PORT", 3306); err != nil {
		return Config{}, err
	}
	if cfg.MySQLServerID, err = uint32Env("TAILJET_MYSQL_SERVER_ID", 240024); err != nil {
		return Config{}, err
	}
	if cfg.PublishTimeout, err = durationEnv("TAILJET_PUBLISH_TIMEOUT", 10*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.CheckpointInterval, err = durationEnv("TAILJET_CHECKPOINT_INTERVAL", 5*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.RecoveryRetryInterval, err = recoveryRetryInterval(); err != nil {
		return Config{}, err
	}
	if cfg.LeaderLeaseDuration, err = durationEnv("TAILJET_LEADER_LEASE_DURATION", 15*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.LeaderRenewDeadline, err = durationEnv("TAILJET_LEADER_RENEW_DEADLINE", 10*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.LeaderRetryPeriod, err = durationEnv("TAILJET_LEADER_RETRY_PERIOD", 2*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.SnapshotBatchSize, err = intEnv("TAILJET_SNAPSHOT_BATCH_SIZE", 500); err != nil {
		return Config{}, err
	}
	if cfg.MaxTransactionRows, err = intEnv("TAILJET_MAX_TRANSACTION_ROWS", 10000); err != nil {
		return Config{}, err
	}
	if cfg.MaxTransactionBytes, err = int64Env("TAILJET_MAX_TRANSACTION_BYTES", 64<<20); err != nil {
		return Config{}, err
	}

	cfg.LeaderLeaseName = os.Getenv("TAILJET_LEADER_LEASE_NAME")
	if cfg.LeaderLeaseName == "" {
		sum := sha256.Sum256([]byte(fmt.Sprintf("%s:%s:%s", cfg.MySQLDatabase, cfg.OutboxTable, cfg.ConsumerName)))
		cfg.LeaderLeaseName = fmt.Sprintf("tailjet-%x", sum[:8])
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	var problems []string
	if c.MySQLHost == "" {
		problems = append(problems, "TAILJET_MYSQL_HOST is required")
	}
	if c.MySQLUser == "" {
		problems = append(problems, "TAILJET_MYSQL_USER is required")
	}
	if c.MySQLDatabase == "" {
		problems = append(problems, "TAILJET_MYSQL_DATABASE is required")
	}
	if c.MySQLServerID == 0 {
		problems = append(problems, "TAILJET_MYSQL_SERVER_ID must be non-zero")
	}
	if c.MySQLFlavor != "mysql" {
		problems = append(problems, "TAILJET_MYSQL_FLAVOR must be mysql")
	}
	if c.MySQLTLSMode != "disable" && c.MySQLTLSMode != "verify" && c.MySQLTLSMode != "skip-verify" {
		problems = append(problems, "TAILJET_MYSQL_TLS_MODE must be disable, verify, or skip-verify")
	}
	if (c.MySQLTLSCert == "") != (c.MySQLTLSKey == "") {
		problems = append(problems, "TAILJET_MYSQL_TLS_CERT and TAILJET_MYSQL_TLS_KEY must be set together")
	}
	if c.MySQLTLSMode == "disable" && (c.MySQLTLSCA != "" || c.MySQLTLSCert != "" || c.MySQLTLSKey != "") {
		problems = append(problems, "MySQL TLS files require TAILJET_MYSQL_TLS_MODE=verify or skip-verify")
	}
	if !identifierPattern.MatchString(c.MySQLDatabase) {
		problems = append(problems, "TAILJET_MYSQL_DATABASE must be a MySQL identifier")
	} else if len(c.MySQLDatabase) > 64 {
		problems = append(problems, "TAILJET_MYSQL_DATABASE must not exceed 64 characters")
	}
	if !identifierPattern.MatchString(c.OutboxTable) {
		problems = append(problems, "TAILJET_OUTBOX_TABLE must be a MySQL identifier")
	} else if len(c.OutboxTable) > 64 {
		problems = append(problems, "TAILJET_OUTBOX_TABLE must not exceed 64 characters")
	}
	if !identifierPattern.MatchString(c.StateTable) {
		problems = append(problems, "TAILJET_STATE_TABLE must be a MySQL identifier")
	} else if len(c.StateTable) > 64 {
		problems = append(problems, "TAILJET_STATE_TABLE must not exceed 64 characters")
	}
	if c.OutboxTable == c.StateTable {
		problems = append(problems, "TAILJET_OUTBOX_TABLE and TAILJET_STATE_TABLE must differ")
	}
	if !consumerNamePattern.MatchString(c.ConsumerName) || len(c.ConsumerName) > 128 {
		problems = append(problems, "TAILJET_CONSUMER_NAME must contain 1 to 128 characters")
	}
	if c.LeaderElectionMode != "kubernetes" && c.LeaderElectionMode != "disabled" {
		problems = append(problems, "TAILJET_LEADER_ELECTION_MODE must be kubernetes or disabled")
	}
	if !dnsLabelPattern.MatchString(c.LeaderLeaseName) || len(c.LeaderLeaseName) > 63 {
		problems = append(problems, "TAILJET_LEADER_LEASE_NAME must be a valid DNS label of at most 63 characters")
	}
	if !dnsLabelPattern.MatchString(c.LeaderLeaseNamespace) || len(c.LeaderLeaseNamespace) > 63 {
		problems = append(problems, "TAILJET_LEADER_LEASE_NAMESPACE must be a valid DNS label of at most 63 characters")
	}
	if c.LeaderIdentity == "" || len(c.LeaderIdentity) > 128 || strings.ContainsAny(c.LeaderIdentity, "\x00\r\n") {
		problems = append(problems, "TAILJET_LEADER_IDENTITY must contain 1 to 128 characters without control line breaks")
	}
	if c.NATSURL == "" {
		problems = append(problems, "TAILJET_NATS_URL is required")
	} else if err := validateNATSURLs(c.NATSURL); err != nil {
		problems = append(problems, err.Error())
	}
	authMethods := 0
	if c.NATSUser != "" || c.NATSPassword != "" {
		authMethods++
		if c.NATSUser == "" {
			problems = append(problems, "TAILJET_NATS_USER is required when TAILJET_NATS_PASSWORD is set")
		}
	}
	if c.NATSToken != "" {
		authMethods++
	}
	if c.NATSCreds != "" {
		authMethods++
	}
	if authMethods > 1 {
		problems = append(problems, "configure only one NATS authentication method")
	}
	if (c.NATSTLSCert == "") != (c.NATSTLSKey == "") {
		problems = append(problems, "TAILJET_NATS_TLS_CERT and TAILJET_NATS_TLS_KEY must be set together")
	}
	if c.NATSExpectedStream != "" && (len(c.NATSExpectedStream) > 255 || strings.ContainsAny(c.NATSExpectedStream, " \t\r\n.*>/\\")) {
		problems = append(problems, "TAILJET_NATS_EXPECTED_STREAM contains an invalid character")
	}
	if c.PublishTimeout <= 0 || c.CheckpointInterval <= 0 || c.RecoveryRetryInterval <= 0 || c.LeaderLeaseDuration <= 0 || c.LeaderRenewDeadline <= 0 || c.LeaderRetryPeriod <= 0 {
		problems = append(problems, "timeouts and intervals must be positive")
	}
	if c.LeaderLeaseDuration < time.Second {
		problems = append(problems, "TAILJET_LEADER_LEASE_DURATION must be at least 1s")
	}
	if c.LeaderLeaseDuration <= c.LeaderRenewDeadline {
		problems = append(problems, "TAILJET_LEADER_LEASE_DURATION must be greater than TAILJET_LEADER_RENEW_DEADLINE")
	}
	if c.LeaderRenewDeadline <= time.Duration(1.2*float64(c.LeaderRetryPeriod)) {
		problems = append(problems, "TAILJET_LEADER_RENEW_DEADLINE must be greater than TAILJET_LEADER_RETRY_PERIOD multiplied by 1.2")
	}
	if c.SnapshotBatchSize <= 0 || c.MaxTransactionRows <= 0 || c.MaxTransactionBytes <= 0 {
		problems = append(problems, "batch and transaction limits must be positive")
	}
	return errors.Join(stringErrors(problems)...)
}

func validateNATSURLs(raw string) error {
	for _, value := range strings.Split(raw, ",") {
		value = strings.TrimSpace(value)
		parsed, err := url.Parse(value)
		if err != nil || parsed.Scheme == "" || parsed.Host == "" {
			return errors.New("TAILJET_NATS_URL contains an invalid URL")
		}
		switch strings.ToLower(parsed.Scheme) {
		case "nats", "tls", "ws", "wss":
		default:
			return errors.New("TAILJET_NATS_URL must use nats, tls, ws, or wss")
		}
		if parsed.User != nil {
			return errors.New("TAILJET_NATS_URL must not contain credentials; use the dedicated NATS authentication variables")
		}
	}
	return nil
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func uint16Env(name string, fallback uint16) (uint16, error) {
	value, err := strconv.ParseUint(env(name, strconv.FormatUint(uint64(fallback), 10)), 10, 16)
	return uint16(value), wrapEnvError(name, err)
}

func uint32Env(name string, fallback uint32) (uint32, error) {
	value, err := strconv.ParseUint(env(name, strconv.FormatUint(uint64(fallback), 10)), 10, 32)
	return uint32(value), wrapEnvError(name, err)
}

func intEnv(name string, fallback int) (int, error) {
	value, err := strconv.Atoi(env(name, strconv.Itoa(fallback)))
	return value, wrapEnvError(name, err)
}

func int64Env(name string, fallback int64) (int64, error) {
	value, err := strconv.ParseInt(env(name, strconv.FormatInt(fallback, 10)), 10, 64)
	return value, wrapEnvError(name, err)
}

func durationEnv(name string, fallback time.Duration) (time.Duration, error) {
	value, err := time.ParseDuration(env(name, fallback.String()))
	return value, wrapEnvError(name, err)
}

func recoveryRetryInterval() (time.Duration, error) {
	if os.Getenv("TAILJET_RECOVERY_RETRY_INTERVAL") == "" && os.Getenv("TAILJET_LEADER_RETRY_INTERVAL") != "" {
		value, err := time.ParseDuration(os.Getenv("TAILJET_LEADER_RETRY_INTERVAL"))
		return value, wrapEnvError("TAILJET_LEADER_RETRY_INTERVAL", err)
	}
	return durationEnv("TAILJET_RECOVERY_RETRY_INTERVAL", 5*time.Second)
}

func wrapEnvError(name string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("invalid %s: %w", name, err)
}

func stringErrors(values []string) []error {
	errs := make([]error, 0, len(values))
	for _, value := range values {
		errs = append(errs, errors.New(value))
	}
	return errs
}
