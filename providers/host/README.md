# Host provider

The Host Provider presents a dynamic fleet of Kubling host agents as one data
source. Agents collect local state with gopsutil and initiate outbound
connections to the provider's Agent Gateway.

This module builds two processes: `kubling-host-provider`, which exposes the
fleet to Kubling, and `kubling-host-agent`, which runs on every observed host.
The provider exposes durable registration through `HOST` and live host facts,
CPU, memory, filesystem, network and process tables through distributed scans.

The agent accepts YAML, environment variables and flags. See
[`agent.example.yaml`](agent.example.yaml) for the small file-based form.

The provider routes equality constraints on `namespace`, `host_id` and
`hostname`. Distributed scans without any routing constraint are rejected by
default and can be enabled explicitly when full-fleet reads are intended.
Fan-out concurrency, scan deadlines and per-host result budgets are bounded
and configurable by the provider process.

When partial results are enabled by both Kubling and the provider, rows from
successful hosts are retained and failed targets are returned as structured
diagnostics. Strict requests fail if any selected host cannot complete.

The Agent Gateway uses TLS and a bearer credential scoped to one namespace.
Credentials are read from files and are never accepted as command-line values.
Plaintext transport exists only as an explicit development mode.

Host identity, session generations and connection history are persisted in the
provider state directory. Live streams are re-established by the agents after
a provider restart.

## Deploy

The provider is distributed as `docker.io/kubling/host-provider`. Host agents
are distributed as Linux binaries and Debian packages because they normally run
on the observed host itself, outside a container, with access to its real
process and mount namespaces.

See the [deployment guide](docs/deployment.md) for network, TLS, namespace,
container and systemd configuration. Compatibility with Kubling releases is
recorded in each provider release rather than fixed in this README.

## Local end-to-end

Run the provider and one real host agent:

```sh
./providers/host/local/run.sh
```

Start the shared Kubling test runtime in a second terminal and execute the host
smoke queries from a third:

```sh
# terminal 2
./testing/kubling/run-kubling.sh

# terminal 3
./providers/host/local/smoke.sh
```
