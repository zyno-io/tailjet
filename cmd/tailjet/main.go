package main

import (
	"context"
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/zyno-io/tailjet/internal/cdc"
	"github.com/zyno-io/tailjet/internal/config"
	"github.com/zyno-io/tailjet/internal/health"
	"github.com/zyno-io/tailjet/internal/leadership"
	"github.com/zyno-io/tailjet/internal/outbox"
	"github.com/zyno-io/tailjet/internal/publisher"
)

var version = "dev"

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{}))
	if err := run(logger); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("Tailjet stopped", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}
	mysqlTLS, err := cfg.MySQLTLSConfig()
	if err != nil {
		return fmt.Errorf("configure MySQL TLS: %w", err)
	}
	db, err := openMySQL(cfg, mysqlTLS)
	if err != nil {
		return err
	}
	defer db.Close()

	natsReady := make(chan struct{}, 1)
	natsConn, err := openNATS(cfg, logger, natsReady)
	if err != nil {
		return err
	}
	defer natsConn.Close()
	js, err := jetstream.New(natsConn)
	if err != nil {
		return fmt.Errorf("create JetStream client: %w", err)
	}

	tracker := health.NewTracker()
	store := outbox.NewStore(db, cfg.MySQLDatabase, cfg.OutboxTable, cfg.StateTable, cfg.ConsumerName)
	messagePublisher := publisher.New(js, natsConn, cfg.MySQLDatabase, cfg.OutboxTable, cfg.NATSExpectedStream, cfg.PublishTimeout)
	runner := cdc.NewRunner(cfg, mysqlTLS, store, messagePublisher, tracker, logger)
	var leaderManager *leadership.Manager
	if cfg.LeaderElectionMode == "kubernetes" {
		leaderManager, err = leadership.NewInCluster(leadership.Settings{
			Namespace:     cfg.LeaderLeaseNamespace,
			Name:          cfg.LeaderLeaseName,
			Identity:      cfg.LeaderIdentity,
			LeaseDuration: cfg.LeaderLeaseDuration,
			RenewDeadline: cfg.LeaderRenewDeadline,
			RetryPeriod:   cfg.LeaderRetryPeriod,
		}, tracker, logger, "tailjet/"+version)
		if err != nil {
			return err
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	retrySignals := make(chan os.Signal, 1)
	signal.Notify(retrySignals, syscall.SIGHUP)
	defer signal.Stop(retrySignals)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-retrySignals:
				queued := runner.TriggerRetry()
				logger.Info("received failed-row retry signal", "queued", queued)
			case <-natsReady:
				queued := runner.TriggerRetry()
				logger.Info("NATS connection is ready; queued failed-row retry", "queued", queued)
			}
		}
	}()
	server := &http.Server{
		Addr:              cfg.HTTPAddress,
		Handler:           tracker.Handler(runner.TriggerRetry),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
	errorsCh := make(chan error, 2)
	go func() {
		logger.Info("health server listening", "address", cfg.HTTPAddress, "version", version)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errorsCh <- fmt.Errorf("health server: %w", err)
		}
	}()
	go func() {
		if leaderManager == nil {
			logger.Warn("Kubernetes leader election is disabled; only one Tailjet replica may run safely")
			tracker.SetPhase("starting-leader", false, true, nil)
			errorsCh <- runner.RunLeader(ctx)
			return
		}
		errorsCh <- leaderManager.Run(ctx, runner.RunLeader)
	}()

	select {
	case <-ctx.Done():
		err = context.Cause(ctx)
	case err = <-errorsCh:
		stop()
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if shutdownErr := server.Shutdown(shutdownCtx); shutdownErr != nil {
		err = errors.Join(err, fmt.Errorf("shutdown health server: %w", shutdownErr))
	}
	if drainErr := natsConn.Drain(); drainErr != nil {
		err = errors.Join(err, fmt.Errorf("drain NATS connection: %w", drainErr))
	}
	return err
}

func openMySQL(cfg config.Config, tlsConfig *tls.Config) (*sql.DB, error) {
	driverConfig := mysqldriver.NewConfig()
	driverConfig.User = cfg.MySQLUser
	driverConfig.Passwd = cfg.MySQLPassword
	driverConfig.Net = "tcp"
	driverConfig.Addr = net.JoinHostPort(cfg.MySQLHost, strconv.Itoa(int(cfg.MySQLPort)))
	driverConfig.DBName = cfg.MySQLDatabase
	driverConfig.ParseTime = true
	driverConfig.Timeout = 10 * time.Second
	driverConfig.ReadTimeout = 30 * time.Second
	driverConfig.WriteTimeout = 30 * time.Second
	if tlsConfig != nil {
		const tlsName = "tailjet"
		if err := mysqldriver.RegisterTLSConfig(tlsName, tlsConfig); err != nil {
			return nil, fmt.Errorf("register MySQL TLS configuration: %w", err)
		}
		driverConfig.TLSConfig = tlsName
	}

	db, err := sql.Open("mysql", driverConfig.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("open MySQL: %w", err)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)
	return db, nil
}

func openNATS(cfg config.Config, logger *slog.Logger, ready chan<- struct{}) (*nats.Conn, error) {
	notifyReady := func() {
		select {
		case ready <- struct{}{}:
		default:
		}
	}
	options := []nats.Option{
		nats.Name("tailjet"),
		nats.MaxReconnects(-1),
		nats.RetryOnFailedConnect(true),
		nats.ReconnectWait(time.Second),
		nats.ReconnectBufSize(-1),
		nats.Timeout(10 * time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			logger.Warn("NATS disconnected", "error", err)
		}),
		nats.ConnectHandler(func(connection *nats.Conn) {
			logger.Info("NATS connected", "server", connection.ConnectedUrlRedacted())
			notifyReady()
		}),
		nats.ReconnectHandler(func(connection *nats.Conn) {
			logger.Info("NATS reconnected", "server", connection.ConnectedUrlRedacted())
			notifyReady()
		}),
		nats.ClosedHandler(func(connection *nats.Conn) {
			logger.Warn("NATS connection closed", "error", connection.LastError())
		}),
	}
	switch {
	case cfg.NATSCreds != "":
		options = append(options, nats.UserCredentials(cfg.NATSCreds))
	case cfg.NATSToken != "":
		options = append(options, nats.Token(cfg.NATSToken))
	case cfg.NATSUser != "":
		options = append(options, nats.UserInfo(cfg.NATSUser, cfg.NATSPassword))
	}
	if cfg.NATSTLSCA != "" {
		options = append(options, nats.RootCAs(cfg.NATSTLSCA))
	}
	if cfg.NATSTLSCert != "" {
		options = append(options, nats.ClientCert(cfg.NATSTLSCert, cfg.NATSTLSKey))
	}

	connection, err := nats.Connect(cfg.NATSURL, options...)
	if err != nil {
		return nil, fmt.Errorf("connect to NATS: %w", err)
	}
	return connection, nil
}
