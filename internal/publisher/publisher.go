package publisher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/zyno-io/tailjet/internal/outbox"
)

type Publisher struct {
	js             jetstream.JetStream
	connection     connectionStatus
	database       string
	table          string
	expectedStream string
	timeout        time.Duration
}

type connectionStatus interface {
	IsConnected() bool
}

func New(js jetstream.JetStream, connection connectionStatus, database, table, expectedStream string, timeout time.Duration) *Publisher {
	return &Publisher{
		js:             js,
		connection:     connection,
		database:       database,
		table:          table,
		expectedStream: expectedStream,
		timeout:        timeout,
	}
}

func (p *Publisher) Publish(ctx context.Context, message outbox.Message) error {
	if err := validateSubject(message.Subject); err != nil {
		return fmt.Errorf("outbox row %d: %w", message.ID, err)
	}
	headers, err := parseHeaders(message.Headers)
	if err != nil {
		return fmt.Errorf("outbox row %d: %w", message.ID, err)
	}

	natsMessage := &nats.Msg{
		Subject: message.Subject,
		Header:  headers,
		Data:    message.Payload,
	}
	if p.connection != nil && !p.connection.IsConnected() {
		return fmt.Errorf("publish outbox row %d to %q: NATS is disconnected", message.ID, message.Subject)
	}
	options := []jetstream.PublishOpt{
		jetstream.WithMsgID(message.StableMessageID(p.database, p.table)),
	}
	if p.expectedStream != "" {
		options = append(options, jetstream.WithExpectStream(p.expectedStream))
	}

	publishContext, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	if _, err := p.js.PublishMsg(publishContext, natsMessage, options...); err != nil {
		return fmt.Errorf("publish outbox row %d to %q: %w", message.ID, message.Subject, err)
	}
	return nil
}

func parseHeaders(raw []byte) (nats.Header, error) {
	headers := nats.Header{}
	if len(raw) == 0 || string(raw) == "null" {
		return headers, nil
	}

	var values map[string]json.RawMessage
	if err := json.Unmarshal(raw, &values); err != nil {
		return nil, fmt.Errorf("headers must be a JSON object: %w", err)
	}
	for name, value := range values {
		if err := validateHeaderName(name); err != nil {
			return nil, err
		}
		if strings.HasPrefix(strings.ToLower(name), "nats-") {
			return nil, fmt.Errorf("header %q uses the NATS-reserved namespace; use message_id or Tailjet configuration", name)
		}

		var single string
		if err := json.Unmarshal(value, &single); err == nil {
			if err := validateHeaderValue(single); err != nil {
				return nil, fmt.Errorf("header %q: %w", name, err)
			}
			headers.Set(name, single)
			continue
		}

		var multiple []string
		if err := json.Unmarshal(value, &multiple); err != nil {
			return nil, fmt.Errorf("header %q must be a string or an array of strings", name)
		}
		for _, item := range multiple {
			if err := validateHeaderValue(item); err != nil {
				return nil, fmt.Errorf("header %q: %w", name, err)
			}
			headers.Add(name, item)
		}
	}
	return headers, nil
}

func validateSubject(subject string) error {
	if subject == "" {
		return errors.New("subject is empty")
	}
	if strings.ContainsAny(subject, " \t\r\n*>") {
		return fmt.Errorf("subject %q is not a concrete NATS subject", subject)
	}
	for _, token := range strings.Split(subject, ".") {
		if token == "" {
			return fmt.Errorf("subject %q contains an empty token", subject)
		}
	}
	return nil
}

func validateHeaderName(name string) error {
	if name == "" {
		return errors.New("header name is empty")
	}
	if strings.ContainsAny(name, ":\r\n") {
		return fmt.Errorf("header name %q contains an invalid character", name)
	}
	return nil
}

func validateHeaderValue(value string) error {
	if strings.ContainsAny(value, "\r\n") {
		return errors.New("value contains a line break")
	}
	return nil
}
