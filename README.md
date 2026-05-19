# Ingressive Connector

Ingressive is your Software Defined Edge Network. It allows you to send traffic to our edge network, scan it for malicious traffic, then send it anywhere you want. Ingressive protects your resources with our Access Control, while giving you flexibility to stop thinking about the edge at all. 

## The Connector

The Ingressive Connector is a small reverse proxy that sits inside your infrastructure. Once installed, all configuration is managed by Ingressive.

This allows you to send traffic to the Ingressive Edge Network and receive it wherever you want. Your laptop, the cloud, your datacenter. 

The connector is open source under the Apache 2.0 licence. 

## Installation

Sign in to Ingressive, navigate to Connectors, and click Add Connector. Ingressive will give you instructions on how to run the Connector using Podman Compose. If you're using Kubernetes, check out our [Controller repo](https://github.com/ingressive-cloud/controller). 

## Build-time version

The connector binary reports its version on every connect via the WebSocket Hello
message; that version is then displayed in the Ingressive console next to each
replica. The version string is set at build time:

```bash
VERSION=0.2.0
go build -ldflags "-X main.Version=$VERSION" .
```

If `-ldflags` is omitted the binary reports `dev`.

