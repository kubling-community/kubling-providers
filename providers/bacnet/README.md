# BACnet provider

Kubling BACnet Provider exposes building-management data from BACnet/IP
networks through the Kubling provider API.

The project is under active development. It compiles a relational catalog,
maintains BACnet/IP attachments, discovers and identifies devices, selects
their profiles, and executes bounded queries and explicitly enabled updates
against individual devices.

## Configuration

The process reads a YAML configuration selected with `-config` or the
`KUBLING_BACNET_CONFIG` environment variable. See
[`config.example.yaml`](config.example.yaml).

```yaml
server:
  listen: "0.0.0.0:50051"
  shutdownGracePeriod: 10s

logging:
  level: info

profiles:
  files: []

attachments: []
scopes: []
```

The built-in profile covers devices and the common analog, binary and
multi-state point families. Additional profiles can extend it with
manufacturer or device-specific properties without changing provider code.
Profile paths are resolved relative to the provider configuration file and are
compiled before any network listener is opened. See the [profile
guide](profiles/README.md).

The catalog exposes `SCOPES`, `DEVICES`, `DEVICE_OBJECTS`, `OBJECTS`, and
common analog, binary and multi-state point projections. `SCOPES` and
`DEVICES` are entry tables backed by the local discovery registry; querying
them does not contact a device. `DEVICES` supplies the `namespace`, `scope_id`
and `device_instance` keys required by the physical object tables.

`DEVICE_OBJECTS`, `OBJECTS` and the point projections require equality
criteria for those three keys, either in `WHERE` or through equality joins.
Multi-device physical fan-out is not enabled yet.

Object properties are grouped with `ReadPropertyMultiple`. Devices that reject
that service are read with individual `ReadProperty` requests. Optional
properties that are unavailable become `NULL`; a missing property declared as
required by the selected profile fails the query. Object enumeration is
bounded to 4096 entries per device.

## Semantic fragment

The provider publishes a source-local semantic fragment generated from the
same compiled catalog as its relational schema. No separate semantic file or
configuration is required. It describes scopes, devices, BACnet objects and
the specialized point families, including their stable identities, typed
properties and executable relationships.

Profile-defined columns and tables extend the fragment automatically. A new
table becomes a semantic entity related to its underlying BACnet object, while
new columns become typed properties of their table entity. This provides the
structural semantics of the exposed catalog; profiles cannot currently declare
arbitrary domain relationships or ontology terms.

## Updates

BACnet writes are disabled by default. Enable them for a logical scope and
choose the BACnet command priority explicitly:

```yaml
scopes:
  - namespace: madrid.dc1
    id: cooling
    attachment: building-lan
    mutations:
      enabled: true
      priority: 16
```

An update must identify exactly one object with equality predicates for
`namespace`, `scope_id`, `device_instance`, `object_type` and
`object_instance`. Profiles decide which properties are writable for each
object type. The built-in profile allows `present_value` updates on analog,
binary and multi-state output and value objects; input objects remain
read-only.

Commandable properties are written at the scope priority. Assigning `NULL`
relinquishes that priority. A write whose remote outcome is unknown is reported
as such and is not retried automatically. `INSERT` and `DELETE` are not
supported.

Inspect the catalog without opening gRPC or BACnet sockets:

```sh
kubling-provider-bacnet -config provider.yaml -check
kubling-provider-bacnet -config provider.yaml -print-metadata
```

## BACnet networks

An attachment owns one long-lived BACnet/IP UDP socket. Logical scopes share
an attachment when they use the same interface, bind address and BBMD settings:

```yaml
attachments:
  - id: building-lan
    interface: eth0
    bind: 0.0.0.0:47808
    transaction:
      apduTimeout: 3s
      retries: 3
      segmentTimeout: 2s
    registry:
      observationTTL: 30m

scopes:
  - namespace: madrid.dc1
    id: cooling
    attachment: building-lan
    mutations:
      enabled: true
      priority: 16
    discovery:
      broadcast: true
      interval: 1m
      window: 3s
```

Local broadcast discovery requires the wildcard BACnet port shown above.
`discovery.targets` can add directed peers or BACnet routers; set
`remoteNetwork` on a target to send a routed remote-broadcast Who-Is. Devices
that must not depend on discovery can be declared under `staticDevices`.

An attachment may register as a foreign device with one BBMD:

```yaml
foreignDevice:
  bbmd: 192.0.2.10:47808
  ttl: 60s
```

Discovery changes device rows, not the schema. Device identity is qualified by
`namespace`, scope and Device Instance, so independent BACnet networks may use
the same instance number safely. Conflicting routes for one instance inside a
scope are rejected instead of choosing one silently.

Run the provider with an empty local topology using:

```sh
make run
```

For a queryable local BACnet/IP device, use the reproducible
[`local` fixture](local/README.md):

```sh
./local/run.sh
```

The container expects its configuration at `/etc/kubling/provider.yaml` by
default.

## Installation

The provider is distributed as `docker.io/kubling/bacnet-provider` and as a
Debian package. Configure the Kubling APT repository using the [Debian
installation guide](https://docs.kubling.com/install/debian), then install the
native service with:

```sh
sudo apt install kubling-bacnet-provider
sudo install -m 0644 \
  /etc/kubling/bacnet-provider.yaml.example \
  /etc/kubling/bacnet-provider.yaml
sudoedit /etc/kubling/bacnet-provider.yaml
sudo systemctl enable --now kubling-bacnet-provider
```

The package does not enable or start the service automatically. The systemd
unit runs as a restricted dynamic user; keep its configuration and any
referenced profile files under `/etc/kubling` and readable by the service.
Inspect runtime logs with `journalctl -u kubling-bacnet-provider`.

## Security

BACnet/IP does not provide transport encryption or peer authentication. Keep
the provider and BACnet network on trusted, controlled network segments. The
provider gRPC listener is also plaintext and should not be exposed directly to
an untrusted network.

See the repository [`SECURITY.md`](../../SECURITY.md) for vulnerability
reporting.

## Development

Run the complete local check with:

```sh
make check
```

Licensed under Apache-2.0.
