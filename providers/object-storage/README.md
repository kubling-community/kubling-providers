# Object storage provider

Kubling Object Storage Provider exposes configured object buckets through one
relational model. Backends are available for the S3 API, Azure Blob Storage and
Google Cloud Storage, as well as a local filesystem.

Each entry under `buckets` becomes a logical provider namespace containing an
`OBJECTS` table. Physical bucket names, prefixes, endpoints and credentials do
not enter the Kubling model. This lets one provider expose several buckets
without making storage topology part of application queries.

`OBJECTS` uses `(bucket, key)` as its stable identity and includes content,
common HTTP metadata, object-store version data and user metadata. Content is
returned as a provider-owned BLOB reference and read in bounded ranges instead
of being materialized by the provider query path.

## Configuration

Start with [`config.example.yaml`](config.example.yaml). Backends describe
reusable connections; logical buckets select one backend, one physical bucket
and an optional root prefix.

One provider can mix backend types. Credentials are never read from YAML. The
S3 backend uses the standard AWS SDK credential chain. Azure Blob uses
`DefaultAzureCredential`, while GCS uses Application Default Credentials. The
cloud backends support workload identity and environment-based credentials
without placing secrets in this file.

```yaml
backends:
  primary:
    type: s3
    region: eu-west-1
  azure:
    type: azure-blob
    accountUrl: https://example.blob.core.windows.net
  google:
    type: gcs
  local:
    type: filesystem
    root: /var/lib/kubling/object-storage
    createDirectories: true

buckets:
  media:
    backend: primary
    name: company-media
    prefix: production
    allowUnboundedList: false
  scratch:
    backend: local
    name: scratch
    allowUnboundedList: false
```

Each Azure physical bucket is a blob container. For local emulators or legacy
accounts that require a shared key, set `authentication: shared-key`, provide
`accountName`, and name the environment variable containing the key with
`accountKeyEnvironment`. The key itself never belongs in YAML.

GCS maps object generations to `version_id` and uses them for conditional reads
and mutations. A custom endpoint is accepted only with
`authentication: emulator`; production connections always use the standard
Google endpoint and credential chain.

For a filesystem backend, `root` is the confined storage root and each physical
bucket name is a directory below it. `createDirectories` may create the root,
bucket and configured prefix when they are absent. It is disabled by default.
Nested directories created by object keys are handled automatically.

Filesystem writes use a synchronized temporary file and an atomic replacement.
Portable HTTP and user metadata is kept below the provider-owned
`.kubling-object-storage` directory. The root should be managed by this provider
when conditional updates matter; unrelated processes can read the stored files,
but concurrent external writers do not participate in provider conditions.
Keys map to ordinary paths, so a file key cannot coexist with another key below
it (for example, `report` and `report/page-1`).

Validate a configuration or inspect its metadata without opening a listener:

```sh
kubling-provider-object-storage -config provider.yaml -check
kubling-provider-object-storage -config provider.yaml -print-metadata
```

## Query and mutation behavior

Exact `key` equality is the normal access path. A bucket can explicitly allow
listing without a key, but the provider rejects it by default even if an
upstream planner sends such a request.

`INSERT` creates one object and rejects an existing key, while `DELETE` removes
one exact key. `UPDATE` replaces the body when `content` is assigned; updates
limited to HTTP properties or user metadata preserve the existing body. Writes
use object-store conditions to avoid silently overwriting a concurrent change.
The provider has no native transaction surface; Kubling may apply soft
transaction behavior above it.

The current provider protocol carries mutation BLOB values inline. Large
object reads already stream through LOB references, while streamed mutation
input requires a future additive protocol capability.

## Semantic model

The provider publishes a source-local semantic fragment for the common stored
object model. It defines the logical bucket and key as the object identity and
describes content, media metadata, version information and user metadata. The
same model applies to every configured bucket namespace.

## Container image

Official releases are published as
`docker.io/kubling/object-storage-provider`. Mount the provider configuration
and, for filesystem backends, mount the configured storage root. Supply cloud
credentials through the runtime environment or workload identity. Pin an exact
image tag in production. All backend types use the same image. A writable
filesystem mount must grant access to the image's non-root user; read-only
mounts can be used when the logical buckets are queried but never mutated.

## Security

Custom S3 endpoints receive credentials selected by the AWS SDK chain. Use a
trusted HTTPS endpoint and credentials scoped only to the configured buckets.
S3 attribute updates use a conditional server-side self-copy and preserve
storage, encryption and object ACL settings. The identity therefore also needs
permission to read object ACLs; multipart attribute copies need permission to
read object tags.
Azure account URLs use HTTPS with default credentials; plain HTTP is accepted
only with explicit shared-key authentication for local emulators.
GCS emulator endpoints are explicitly unauthenticated and must remain local or
on a trusted development network.
Filesystem object paths are confined below the configured root; path traversal
and object symlinks are rejected. Do not use `/` as a storage root.
The provider gRPC listener is plaintext and should remain on a trusted network
or behind an authenticated transport boundary.

## Development

Run the complete local check with:

```sh
make check
```

The local fixture builds MinIO from its upstream source tag, creates two
buckets, seeds representative objects and starts the provider on `:50053`:

```sh
./local/run.sh
```

Lifecycle commands are `store`, `provider`, `status`, `down`, and `reset`.

For a test without an external object store, run the filesystem fixture. It
uses `/tmp/kubling-object-storage-local` and the same logical namespaces:

```sh
./local/run.sh filesystem
```

The Azure fixture uses Azurite with the same buckets and data:

```sh
./local/run.sh azure
```

The GCS fixture uses a local emulator:

```sh
./local/run.sh gcs
```

Licensed under Apache-2.0.
