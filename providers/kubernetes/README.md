# Kubernetes provider

The Kubernetes provider exposes exactly one Kubernetes cluster as one Kubling
data source. Run another provider instance when Kubling needs to connect to
another cluster so Kubling can aggregate equivalent cluster models and
distinguish them with data-source constants.

The provider dynamically discovers listable resources from the preferred
Kubernetes API versions. Optional schema include and exclude patterns can
filter that catalog. API group/version values become provider namespaces,
while resources become relational tables. Kubernetes object namespaces remain
row data in `metadata__namespace`; they are not provider routing namespaces.

In compact mode, resource tables expose stable identity columns and preserve
`metadata`, `spec`, and `status` as JSON. By default they also retain the
complete `object`; `schema.includeObject` can omit that column. When field
expansion is enabled, the provider uses the cluster's OpenAPI v3 schema to
expose nested scalar fields as typed relational columns. Maps, arrays, and
objects at the configured depth boundary remain JSON. If OpenAPI metadata is
unavailable for a resource, the provider falls back to the compact model.

Kubling Core may define synthetic tables over JSON columns that remain in the
discovered schema. The provider remains unaware of synthetic table definitions.

Queries use the dynamic Kubernetes API with native continuation-token
pagination. Equality filters on `metadata__name` and `metadata__namespace` are
pushed down exactly; Kubling retains responsibility for every other
expression. Ordering and offset are not advertised.

Resource discovery verbs determine each table's mutation support. OpenAPI
read-only declarations and Kubernetes field semantics determine mutability at
column level. Inserts create canonical Kubernetes objects, updates use merge
patch when available and fall back to resource update, and deletes require
exact resource identity. Server-managed fields and the status subresource
remain read-only.

## Configuration

The provider uses standard `client-go` kubeconfig loading when `kubeconfig` is
omitted. Set `inCluster: true` when running inside Kubernetes; it cannot be
combined with `kubeconfig` or `context`.

```bash
go run ./cmd/kubernetes -config ./config.example.yaml
```

The empty namespace strategy controls namespaced operations without
introducing multi-cluster routing:

- `DEFAULT` uses the kubeconfig context's default namespace.
- `ALL` queries all namespaces; mutations still require an explicit namespace.
- `FAIL` rejects operations that omit a namespace.

The `schema` section in [`config.example.yaml`](config.example.yaml) documents
catalog filters, global and resource-specific expansion depth, and retention of
the canonical `object` JSON column.

## Local environment

The local fixture uses the k3s image selected in
[`local/compose.yaml`](local/compose.yaml). It starts one privileged k3s server
on `127.0.0.1:6443`, applies deterministic sample resources, generates a
host-safe kubeconfig, and runs this provider on `:50054`:

```sh
./local/run.sh
```

The generated `local/kubeconfig.local.yaml` contains cluster credentials, has
mode `0600`, and is ignored by the repository-wide `*.local.yaml` rule. The
tracked `local/provider.yaml` references it and uses
`blankNamespaceStrategy: ALL`.

To start only k3s and leave it available for Kubling Core:

```sh
./local/run.sh k3s
```

The fixture creates `kubling-sample` and `kubling-secondary`, equivalent
ConfigMaps in both namespaces, a Secret, and a zero-replica Deployment. The
Deployment never pulls its declared image.

Useful lifecycle and inspection commands are:

```sh
./local/run.sh status
./local/run.sh kubectl get namespaces
./local/run.sh kubectl get configmaps --all-namespaces
./local/run.sh kubeconfig
./local/run.sh logs
./local/run.sh down
./local/run.sh reset
```

Set `K3S_IMAGE`, `K3S_API_PORT`, or
`KUBLING_KUBERNETES_PROVIDER_LISTEN` to override the values defined by the
local environment. When overriding `K3S_API_PORT`, the generated kubeconfig is
rewritten to the same host port automatically.

With only k3s running, execute the provider integration test with:

```sh
./local/run.sh k3s
KUBLING_KUBERNETES_INTEGRATION=1 go test -run TestKubernetesIntegrationMetadataAndQuery -v .
```
