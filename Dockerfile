# Build stage
FROM golang:1.26-alpine AS build

#RUN apk add --no-cache ca-certificates

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /connector .

# Final stage — minimal Alpine image with CA certs for TLS to the Bifrost API.
FROM alpine:latest

RUN apk --no-cache add ca-certificates && \
    addgroup -S -g 65532 connector && \
    adduser -S -u 65532 -G connector connector && \
    mkdir -p /etc/ingressive && \
    chown 65532:65532 /etc/ingressive
COPY --from=build --chown=65532:65532 /connector /connector

LABEL org.opencontainers.image.description="Ingressive Cloud Connector and Proxy" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.source="https://github.com/ingressive-cloud/connector"

USER 65532:65532
ENTRYPOINT ["/connector"]
