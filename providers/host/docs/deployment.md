# Host provider deployment

One Host Provider represents a fleet of homogeneous Linux hosts as one Kubling
data source. Kubling connects to the provider service, while every host agent
opens its own outbound connection to the Agent Gateway.

```text
Kubling ---- gRPC ----> Host Provider <---- outbound gRPC ---- host agents
             50051          50052
```

Expose the provider port only to the Kubling instances that use it. Expose the
Agent Gateway only to enrolled hosts. Persist
`/var/lib/kubling/host-provider`; it contains durable host identities and
session history.

## Run the provider

The Agent Gateway requires TLS and one bearer-token file per fleet namespace.
Tokens must contain at least 32 non-whitespace characters. Mounted certificate,
key and token files must be readable by the container's non-root user.

```sh
docker run --rm \
  --publish 50051:50051 \
  --publish 50052:50052 \
  --volume kubling-host-provider-state:/var/lib/kubling/host-provider \
  --mount type=bind,src="$PWD/gateway.crt",dst=/run/secrets/gateway.crt,readonly \
  --mount type=bind,src="$PWD/gateway.key",dst=/run/secrets/gateway.key,readonly \
  --mount type=bind,src="$PWD/production.token",dst=/run/secrets/production.token,readonly \
  --env KUBLING_HOST_AGENT_TLS_CERTIFICATE=/run/secrets/gateway.crt \
  --env KUBLING_HOST_AGENT_TLS_PRIVATE_KEY=/run/secrets/gateway.key \
  --env KUBLING_HOST_NAMESPACE_TOKEN_FILES=production=/run/secrets/production.token \
  docker.io/kubling/host-provider:latest
```

Pin an exact image tag in production. Register port `50051` as a Kubling gRPC
provider endpoint using the public [provider configuration
documentation](https://docs.kubling.com/providers).

The main provider controls are:

| Environment variable | Purpose |
| --- | --- |
| `KUBLING_HOST_PROVIDER_LISTEN` | Northbound Kubling listener |
| `KUBLING_HOST_AGENT_LISTEN` | Southbound Agent Gateway listener |
| `KUBLING_HOST_STATE_DIRECTORY` | Durable provider state |
| `KUBLING_HOST_AGENT_TLS_CERTIFICATE` | Agent Gateway server certificate |
| `KUBLING_HOST_AGENT_TLS_PRIVATE_KEY` | Agent Gateway private key |
| `KUBLING_HOST_NAMESPACE_TOKEN_FILES` | Comma-separated `namespace=path` token mappings |
| `KUBLING_HOST_ALLOW_PARTIAL_RESULTS` | Permit explicitly requested partial scans |
| `KUBLING_HOST_ALLOW_UNBOUNDED_FANOUT` | Permit scans without an access-field constraint |
| `KUBLING_HOST_SCAN_TIMEOUT` | Deadline for one host scan |
| `KUBLING_HOST_MAX_CONCURRENT_TARGETS` | Concurrent targets per query |
| `KUBLING_HOST_MAX_ROWS_PER_TARGET` | Per-host buffered row limit |
| `KUBLING_HOST_MAX_BYTES_PER_TARGET` | Per-host buffered byte limit |

Equivalent command-line flags are available through `--help`. Plaintext Agent
Gateway transport is intended only for an explicit development environment.

## Routing and completeness

Predicates on `namespace`, `host_id` and `hostname` select target agents. A
query without any of those constraints is rejected unless unbounded fan-out is
enabled. Enabling fan-out does not remove concurrency, deadline, row or byte
limits.

Partial results are used only when the Kubling request explicitly allows them
and the provider has enabled them. Otherwise, failure of any selected host
fails the scan.

## Install an agent

Configure the public `kubling/bin` APT repository, then install the package:

```sh
curl -sLf \
  'https://dl.cloudsmith.io/public/kubling/bin/cfg/setup/bash.deb.sh' \
  | sudo bash
sudo apt install kubling-host-agent
```

The package installs the binary, a systemd unit and
`/etc/kubling/host-agent.yaml.example`. It does not enable or start the service.
Prepare the configuration first:

```sh
sudo cp /etc/kubling/host-agent.yaml.example /etc/kubling/host-agent.yaml
sudo chmod 600 /etc/kubling/host-agent.yaml
```

Set the gateway address, namespace, credential file, CA file and expected TLS
server name. Store the namespace token in a root-readable file outside version
control and update `credentialFile` to that path. The token must match the file
configured for the same namespace on the provider.

Enable the configured agent with:

```sh
sudo systemctl enable --now kubling-host-agent
sudo systemctl status kubling-host-agent
```

Inspect runtime logs with `journalctl -u kubling-host-agent`. The agent creates
and preserves its durable identifier under `/var/lib/kubling/host-agent`.
Package upgrades restart the service only when it is already active.

## Agent configuration

The YAML file supports:

- `gatewayAddress`, `namespace` and `stateDirectory`;
- `credentialFile` and the `tls.caFile` and `tls.serverName` settings;
- an optional reported `hostname` and non-secret `attributes`;
- scan concurrency, reconnect backoff and connection timeout controls;
- optional inclusion of pseudo filesystems.

Environment variables and command-line flags may override file values. Use
`kubling-host-agent --help` for the complete list.
