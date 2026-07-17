.PHONY: build integration lint test test-race vet vuln

build:
	go build ./cmd/tailjet

test:
	go test ./...

test-race:
	go test -race ./...

vet:
	go vet ./...

lint:
	go run honnef.co/go/tools/cmd/staticcheck@v0.7.0 ./...

vuln:
	go run golang.org/x/vuln/cmd/govulncheck@v1.6.0 ./...

integration:
	./scripts/integration.sh
