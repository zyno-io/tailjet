package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

func (c Config) MySQLTLSConfig() (*tls.Config, error) {
	if c.MySQLTLSMode == "disable" {
		return nil, nil
	}
	return loadTLSConfig(c.MySQLHost, c.MySQLTLSMode == "skip-verify", c.MySQLTLSCA, c.MySQLTLSCert, c.MySQLTLSKey)
}

func loadTLSConfig(serverName string, insecureSkipVerify bool, caFile, certFile, keyFile string) (*tls.Config, error) {
	config := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         serverName,
		InsecureSkipVerify: insecureSkipVerify, // Explicitly selected by TAILJET_MYSQL_TLS_MODE.
	}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("read TLS CA: %w", err)
		}
		pool, err := x509.SystemCertPool()
		if err != nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("TLS CA file %q contains no certificates", caFile)
		}
		config.RootCAs = pool
	}
	if certFile != "" {
		certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("load TLS client certificate: %w", err)
		}
		config.Certificates = []tls.Certificate{certificate}
	}
	return config, nil
}
