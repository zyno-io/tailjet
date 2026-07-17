# syntax=docker/dockerfile:1
FROM golang:1.26.5-alpine3.24@sha256:0178a641fbb4858c5f1b48e34bdaabe0350a330a1b1149aabd498d0699ff5fb2 AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
ARG VERSION=dev
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build \
      -trimpath \
      -ldflags="-s -w -X main.version=${VERSION}" \
      -o /out/tailjet ./cmd/tailjet

FROM gcr.io/distroless/static-debian12:nonroot@sha256:aef9602f8710ec12bde19d593fed1f76c708531bb7aba205110f1029786ead7b
ARG VERSION=dev
ARG REVISION=unknown
LABEL org.opencontainers.image.title="Tailjet" \
      org.opencontainers.image.description="MySQL transactional outbox relay for NATS JetStream" \
      org.opencontainers.image.source="https://github.com/zyno-io/tailjet" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}"
COPY --from=build /out/tailjet /tailjet
EXPOSE 8080
ENTRYPOINT ["/tailjet"]
