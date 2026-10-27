# OpenAPI provider

The OpenAPI provider turns operations from an OpenAPI 3.x document into
relational tables for Kubling. The API remains the source of truth:
the provider reads its schemas, calls its HTTP operations and returns structured
metadata and tuples through the standard provider gRPC contract.

An OpenAPI document describes HTTP operations and JSON shapes, but it does not
always say which arrays should become relational tables or which fields form a
primary key. Conservative discovery handles unambiguous operations; a small
external YAML mapping supplies only the exceptions. No Kubling DDL is required.

## How it works

At startup, the provider:

1. Loads a strict YAML configuration and a local or remote OpenAPI document.
2. Discovers eligible `GET` operations and applies explicit entity mappings.
3. Selects a successful JSON response and follows `responsePath` to an array.
4. Converts the array item schema into structured Kubling table metadata.
5. Resolves the selected OpenAPI `securityScheme`, when authentication is
   configured.
6. Validates explicit mutation mappings against their paths, parameters and
   JSON request schemas.

When Kubling queries a table, the provider calls the corresponding HTTP
operation. Responses are decoded page by page and converted into streamed
`TupleBatch` values. Source pagination and gRPC batch size are independent: a
large API page may produce several batches, while one batch may combine several
small API pages.

## Configuration

The following example exposes the array at `/data/items` from the
`listInvoices` operation as the `INVOICE` table:

```yaml
specFile: ./billing.openapi.yaml
baseUrl: https://billing.example.com/api
namespace: billing
requestTimeout: 30s
maxResponseBytes: 33554432
allowInsecureHttp: false

headers:
  X-Tenant: tenant-a

authentication:
  securityScheme: BearerAuth
  credential: replace-with-a-token

entities:
  - name: INVOICE
    listOperation: listInvoices
    responsePath: /data/items
    primaryKey:
      - id
    queryParameters:
      - name: status
        value: open
    equalityFilters:
      - field: customerId
        parameter: customer_id
    pagination:
      mode: CURSOR
      pageSize: 100
      pageSizeParameter: limit
      cursorParameter: after
      nextCursorPath: /data/nextCursor
      maxPages: 10000
    mutations:
      insert:
        operation: createInvoice
        bodyPath: /data
      update:
        operation: updateInvoice
        pathParameters:
          - parameter: invoiceId
            field: id
        bodyPath: /data
      delete:
        operation: deleteInvoice
        pathParameters:
          - parameter: invoiceId
            field: id
```

`specFile` accepts either a local path or an absolute HTTP(S) URL. Relative
paths are resolved from the provider configuration file. A URL is downloaded
once during startup, uses `requestTimeout`, and is limited to 32 MiB. Query
strings are preserved, which allows immutable Git commit or artifact URLs.

Datasource headers and authentication are deliberately not sent while fetching
the specification. Local file references are supported for local documents.
Documents loaded over HTTP must currently be self-contained because external
remote references are not fetched.

Private specification endpoints may use separate headers that are never sent to
the datasource:

```yaml
specHeaders:
  Authorization: Bearer ${OPENAPI_REPOSITORY_TOKEN}
```

`specHeaders` require HTTPS. Redirects are accepted only when they preserve the
scheme and host, preventing repository credentials from crossing origins.

Configuration strings support required environment placeholders such as
`${API_TOKEN}` and defaults such as `${REQUEST_TIMEOUT:-30s}`. An unset required
variable fails startup. Use `$${NAME}` when the literal text `${NAME}` is
needed. This lets one committed GitOps configuration describe the provider
while credentials remain in runtime secrets.

`baseUrl` is optional when the first root-level OpenAPI `servers` entry is an
absolute URL without variables. An explicit value overrides the document. It
is combined with the operation path. For example, the base URL
`https://billing.example.com/api` and the OpenAPI path `/invoices` produce an
HTTP request to `https://billing.example.com/api/invoices`.

`namespace` is an opaque logical grouping returned unchanged to Kubling. It
does not expose the API endpoint or physical topology.

An API-specific semantic fragment can be exposed with optional configuration:

```yaml
semantic:
  fragmentFile: ./billing.semantic.yaml
  mediaType: application/yaml
  version: <model-version>
```

The fragment file must be local and is resolved relative to the provider
configuration. Its bytes are returned unchanged. The provider does not infer
domain relationships, identities or cardinalities from the OpenAPI document;
when `semantic` is absent, no fragment is returned.

`maxResponseBytes` limits each successful JSON response page before decoding.
It defaults to 32 MiB and cannot exceed 1 GiB. This prevents a broken or
malicious endpoint from growing the provider process without bound; increase
it only when the API legitimately returns larger pages.

Datasource redirects are followed only when scheme, host and port remain
unchanged. Cross-origin redirects and HTTPS downgrades are rejected before
configured headers or authentication can be sent to the redirected endpoint.
Operation paths from the OpenAPI document are subject to the same-origin rule.

When `baseUrl` uses plain HTTP, configurations containing authentication or
static headers are rejected by default. `allowInsecureHttp: true` is an
explicit opt-in intended only for trusted development networks. Credentials
embedded in `baseUrl` user information are never accepted.

`responsePath` uses RFC 6901 JSON Pointer syntax. It may be empty when the
response body itself is an array. The selected value must be an array of JSON
objects.

`primaryKey` is optional. Every listed field must exist in the exposed table
schema. The provider marks key fields as non-nullable in the returned metadata.

An explicit entity may instead define an engine-generated stable key composed
from ordered response fields:

```yaml
stableKey:
  name: global_id
  columns:
    - tenant_id
    - id
```

`name` defaults to `identifier`. The generated column becomes the table primary
key and is never projected to or mutated through the API. Kubling generates its
value and unfolds supported predicates back to the component fields.
`primaryKey` and `stableKey` are mutually exclusive. Every component must exist
in the exposed table schema, which may include request-binding fields that the
provider adds to the response shape.

The configuration may contain static request headers. Do not commit secrets in
example files; mount or generate the real provider configuration through the
deployment's secret-management mechanism.

`queryParameters` defines fixed query-string values for an entity. Each name
must be declared as an `in: query` parameter by the selected OpenAPI operation.

`equalityFilters` explicitly maps a Kubling field to an OpenAPI query
parameter. This is intentionally not inferred: the mapping asserts that
`field = literal` and the remote parameter have exactly the same semantics.
Mapped columns are exposed as equality-searchable metadata. Logical `AND` is
supported when every operand can be translated through one of these bindings.

### Parameterized list operations

Entity-level `pathParameters` makes collection operations below a resource
path queryable without creating one provider instance per resource. For an
operation such as `GET /repos/{owner}/{repo}/releases`:

```yaml
entities:
  - name: RELEASE
    listOperation: listReleases
    pathParameters:
      - parameter: owner
        field: repository_owner
      - parameter: repo
        field: repository_name
```

Kubling can then bind one remote collection with ordinary equality predicates:

```sql
SELECT id, name
FROM github.RELEASE
WHERE repository_owner = 'example'
  AND repository_name = 'service-api';
```

Each `field` becomes an equality-searchable column. Required dynamic path and
query bindings form a table access pattern, allowing the planner to reject an
unbounded scan and to supply values through dependent joins. The provider also
validates the complete binding before starting HTTP.

When the response does not repeat a binding field, its request value is
materialized as a virtual column in every returned row and remains stable
across pagination. The same field may therefore participate in a primary or
stable key. A path parameter may instead use `value` for a fixed scope:

```yaml
pathParameters:
  - parameter: owner
    value: example
  - parameter: repo
    field: repository_name
```

Bound values are encoded as one URL path segment. Slashes and reserved
characters cannot introduce another path, query, fragment or datasource
origin.

## Mutations

Write operations are always explicit. OpenAPI describes HTTP syntax but cannot
reliably state that a `POST`, `PATCH` or `DELETE` has relational insert, update
or delete semantics, so discovery never enables mutations automatically.

An entity may map any subset of:

- `insert` to a `POST` operation.
- `update` to a `PATCH` or `PUT` operation.
- `delete` to a `DELETE` operation.

Within a mutation mapping, `pathParameters` binds an OpenAPI path parameter
either to a table field or to a fixed value. Update and delete require at least
one field-bound path parameter. Their Kubling filter must contain exact
equality values for every bound field and no unrelated predicates;
collection-wide writes are rejected.

```yaml
mutations:
  update:
    operation: updateInvoice
    pathParameters:
      - parameter: tenantId
        value: tenant-a
      - parameter: invoiceId
        field: id
    queryParameters:
      - name: notify
        value: "false"
    bodyPath: /payload
```

`queryParameters` supplies fixed values declared by the write operation.
`bodyPath` is an optional JSON Pointer used when an API wraps its writable
object, for example `/payload` produces `{"payload": {...}}`.

Insert performs one HTTP request per input tuple. Update assignments must be
literals and are encoded into one JSON object. Delete sends no request body.
Successful requests report one affected row each. Generated values are not yet
supported.

Writable table and column metadata is derived only from these mappings and the
selected request body schemas. Required insert-body properties become
non-nullable; response-only fields do not become mandatory insert values.
Scalar values, JSON, null and base64-encoded binary values are supported.
LOB, XML and geospatial transport values remain unsupported.

## Entity discovery

Discovery removes the need to repeat straightforward list operations from the
OpenAPI document:

```yaml
specFile: https://raw.githubusercontent.com/example/api/main/openapi.yaml
baseUrl: https://api.example.com
namespace: example

discovery:
  enabled: true
  includeTags:
    - Public API
  excludeOperations:
    - listInternalEvents
```

`includeTags`, `includeOperations` and `excludeOperations` are optional and
case-insensitive. When tags and operations are both included, an operation must
match both selectors.

An operation is discovered only when it:

- Is a `GET` operation with an `operationId`.
- Uses only required scalar path parameters with standard simple
  serialization; each one becomes a same-named equality access column.
- Has no path parameter name that collides with a response property; those
  semantics require an explicit field mapping.
- Has no required query, header or cookie parameters.
- Has a successful JSON response containing exactly one array of objects.

The table name is derived, in order, from the array item schema reference, its
schema title, the response property name or the operation ID. Names are
normalized to upper snake case. The detected JSON Pointer becomes the
`responsePath`.

Explicit `entities` and discovery can be combined. An explicit entity with the
same `listOperation` replaces its discovered form, allowing names, primary
keys, query bindings and pagination to be specified only where needed:

```yaml
discovery:
  enabled: true

entities:
  - name: EVENT
    listOperation: listEvents
    responsePath: /data/events
    pagination:
      mode: CURSOR
      pageSize: 100
      cursorParameter: after
      nextCursorPath: /data/next
```

Discovery deliberately does not guess authentication semantics, primary or
stable keys, filter equivalence, pagination conventions or write semantics.
Operations with multiple object arrays, required non-path parameters or special
request semantics remain explicit. Use an explicit entity to rename a path
binding field or replace it with a fixed value. An `includeOperations` entry
that cannot be safely discovered fails startup with the reason instead of
being silently ignored.

## Configuration template generation

The provider can inspect the OpenAPI document and generate a reviewable YAML
configuration containing every supported `GET` entity candidate:

```bash
kubling-provider-openapi \
  -config provider.seed.yaml \
  -generate-config-template > provider.yaml
```

The seed file only needs the document location and any shared provider settings:

```yaml
specFile: https://api.example.com/openapi.yaml
baseUrl: https://api.example.com
namespace: example
```

Unlike conservative runtime discovery, the generator includes every object
array found in a successful JSON response, including multiple arrays from the
same operation. It also creates placeholders for required query parameters and
recognizes common cursor and page pagination shapes. Parameterized collection
paths receive field-bound `pathParameters`; review those field names before
use. Candidates with ambiguous path/response field names are skipped rather
than merged. Skipped operations and inferred values are recorded as YAML
comments.

The generated file is a starting point, not an executable interpretation of
API semantics. Review entity names, choose primary keys, verify pagination and
remove unwanted entities. Mutation mappings are never generated because an
OpenAPI document alone cannot safely establish relational write semantics.
Environment placeholders already present in the seed file remain unchanged in
the output.

## Authentication

Authentication refers to a named entry under the OpenAPI document's
`components.securitySchemes`. The provider derives the wire representation
from that definition rather than duplicating header or query parameter names in
its own configuration.

For example, this OpenAPI definition:

```yaml
components:
  securitySchemes:
    BearerAuth:
      type: http
      scheme: bearer
      bearerFormat: JWT
```

is selected by setting `authentication.securityScheme` to `BearerAuth`.

Bearer authentication and API keys use `credential`:

```yaml
authentication:
  securityScheme: BearerAuth
  credential: replace-with-a-token
```

HTTP Basic authentication uses `username` and `password`:

```yaml
authentication:
  securityScheme: BasicAuth
  username: api-user
  password: replace-with-a-password
```

Supported security schemes are:

- HTTP Bearer authentication.
- HTTP Basic authentication.
- API keys in headers, query parameters or cookies.

One security scheme may be selected for a provider instance. OAuth2, OpenID
Connect and mutual TLS are recognized but not implemented yet. Conflicting
static headers or pagination parameters are rejected during startup.

## Pagination

Pagination is configured per entity because different operations in the same
API may follow different conventions. When `pagination` is omitted, the
provider performs exactly one HTTP request.

Offset pagination sends an offset and optionally a requested page size:

```yaml
pagination:
  mode: OFFSET
  pageSize: 100
  pageSizeParameter: limit
  offsetParameter: offset
```

Page-number pagination defaults to page `1`. Set `startPage: 0` for zero-based
APIs:

```yaml
pagination:
  mode: PAGE
  pageSize: 100
  pageSizeParameter: per_page
  pageParameter: page
  startPage: 1
```

Cursor pagination reads the next cursor from the response body:

```yaml
pagination:
  mode: CURSOR
  pageSize: 100
  pageSizeParameter: limit
  cursorParameter: after
  nextCursorPath: /data/nextCursor
  hasMorePath: /data/hasMore
```

Offset and page modes stop when the API returns fewer rows than `pageSize`.
Cursor mode stops when the next cursor is omitted, an empty string or `null`.
When `hasMorePath` is configured, it must resolve to a boolean and `false`
stops pagination even if the response still contains a cursor. A true value
requires a non-empty next cursor. Repeated cursors are rejected. `maxPages`
defaults to `10000` and prevents a broken API from creating an unbounded
request loop. Reaching the guardrail fails an attempt to fetch another page;
it never turns an incomplete result into a successful, silently truncated
query.

This pagination is an HTTP transport concern. SQL limit and offset are enforced
exactly by the provider. Offset pagination starts directly at the requested
source offset; other pagination modes discard preceding rows locally.

## Schema mapping

Columns are ordered deterministically by name. Required OpenAPI properties are
non-nullable; other properties are nullable unless they form a configured
primary key. `allOf` object composition is supported.

OpenAPI values map to Kubling logical types as follows:

| OpenAPI schema | Kubling type |
| --- | --- |
| `boolean` | `BOOLEAN` |
| `integer`, format `int32` | `INTEGER` |
| `integer`, format `int64` | `LONG` |
| Other `integer` | `BIGINTEGER` |
| `number`, format `float` | `FLOAT` |
| `number`, format `double` | `DOUBLE` |
| Other `number` | `BIGDECIMAL` |
| `string`, format `date` | `DATE` |
| `string`, format `time` | `TIME` |
| `string`, format `date-time` | `TIMESTAMP` |
| `string`, format `byte` or `binary` | `VARBINARY` |
| Other `string` | `STRING` |
| Arrays, objects or ambiguous unions | `JSON` |

Missing nullable properties become Kubling null values. Missing required
properties and incompatible runtime JSON values fail the query instead of
silently coercing data.

## Query behavior

The query implementation supports:

- Complete table reads through configured `GET` operations.
- Field projections, including aliases.
- Literal projections.
- Lazy HTTP pagination and streamed gRPC batches.
- Static query parameters declared by the operation.
- Fixed and equality-bound path parameters declared by the operation.
- Explicit field-to-query-parameter equality bindings and logical `AND`.
- Exact SQL limit and offset handling.
- Exact conversion according to the discovered column metadata.

Ordering is not advertised or accepted as provider pushdown. Filters without
an explicit exact binding are rejected rather than evaluated approximately.
Native transactions are unsupported. Entities without explicit mutation
mappings remain read-only.

The health response currently confirms that configuration, OpenAPI parsing and
entity discovery succeeded during startup. It does not perform an HTTP probe
against the remote API.

## Running without Go code

The official container only needs a mounted YAML configuration:

```sh
docker run --rm \
  -p 50051:50051 \
  -v "$PWD/provider.yaml:/etc/kubling/provider.yaml:ro" \
  -e OPENAPI_SPEC_URL=https://api.example.com/openapi.json \
  -e OPENAPI_BASE_URL=https://api.example.com \
  kubling/openapi-provider:latest
```

The same binary can validate a configuration or print the relational metadata
without starting gRPC:

```sh
kubling-provider-openapi -config provider.yaml -check
kubling-provider-openapi -config provider.yaml -print-metadata
kubling-provider-openapi -config provider.yaml -generate-config-template
```

The config path defaults from `KUBLING_OPENAPI_CONFIG`; the container sets it
to `/etc/kubling/provider.yaml`. `config.example.yaml` is the minimum generic
starting point. API-specific examples live under `examples/` and are examples,
not special behavior built into the provider.

[`examples/github-releases.yaml`](examples/github-releases.yaml) demonstrates
repository-scoped release and release-asset tables selected at query time.
`RELEASE` requires equality values for `repository_owner` and
`repository_name`; `RELEASE_ASSET` additionally requires `release_id`. The
provider materializes those bound values as columns in every returned row and
rejects a query before HTTP when any required binding is missing. No provider
restart or per-repository entity is needed.

The example deliberately pins both the official API description revision and
the requested API version so metadata and request behavior remain
reproducible. Its page limit is a safety guardrail, not a completeness promise:
choose a realistic bound for the expected collection size and avoid values so
large that they effectively permit an unbounded request loop.

`examples/openai-management.yaml` demonstrates a broader management surface:
organization projects, users, admin API keys, audit logs, completions usage and
cost buckets are exposed from OpenAI's official OpenAPI document using only
declarative mappings. It requires `OPENAI_ADMIN_KEY` and
`OPENAI_USAGE_START_TIME`, expressed as Unix seconds. Usage and cost bucket
`results` remain JSON so Kubling can derive synthetic detail tables without
adding API-specific logic to this provider.

## Local environment

The local environment runs the provider against the OpenAI Management API
example. It builds the same container image used by releases, mounts
[`examples/openai-management.yaml`](examples/openai-management.yaml), and
exposes only the provider gRPC port on `127.0.0.1:50055`.

Create the ignored local environment file and set an organization Admin API
key plus the beginning of the usage window:

```sh
cp local/.env.example local/.env
```

For example, a rolling 30-day window can be generated on GNU/Linux with:

```sh
sed -i "s/replace-with-unix-seconds/$(date -d '30 days ago' +%s)/" local/.env
```

After adding the real `OPENAI_ADMIN_KEY`, validate the remote OpenAPI document
and all mappings without starting gRPC:

```sh
./local/run.sh check
```

Start the provider and follow its logs with:

```sh
./local/run.sh
```

The provider remains available on `localhost:50055` after detaching from the
logs. Useful lifecycle and inspection commands are:

```sh
./local/run.sh provider
./local/run.sh metadata
./local/run.sh template > provider.generated.yaml
./local/run.sh logs
./local/run.sh status
./local/run.sh down
./local/run.sh reset
```

Set `KUBLING_OPENAPI_PROVIDER_PORT` in `local/.env` to change the host gRPC
port. `OPENAPI_PROVIDER_IMAGE` may override the local image name. This fixture
uses the real OpenAI API and may expose organization metadata and usage data;
never commit `local/.env` or its Admin API key.

If `50055` is already occupied, override it without editing the file:

```sh
KUBLING_OPENAPI_PROVIDER_PORT=50056 ./local/run.sh
```

The helper checks the selected port before starting Compose and reports this
override instead of returning Docker's lower-level bind error.

## Embedding the Go package

Applications that need custom process integration can still embed the package:

```go
config, err := openapi.LoadConfig("provider.yaml")
if err != nil {
    return err
}

implementation, err := openapi.New(config)
if err != nil {
    return err
}

service := providersdk.NewServer(implementation)
grpcServer := grpc.NewServer()
providerv1.RegisterProviderServiceServer(grpcServer, service)
```

The SDK owns gRPC connection identifiers and transport lifecycle. The OpenAPI
implementation only handles API metadata, HTTP requests and value conversion.

Validate the module with:

```sh
go test ./...
```

## Current boundaries

- Only OpenAPI 3.x documents and successful JSON `GET` responses are supported.
- List path parameters must be required scalars using standard simple,
  non-exploded serialization.
- The row schema must describe an object; top-level `oneOf` and `anyOf` row
  schemas are rejected.
- Remote OpenAPI references are disabled. Local file references are supported.
- Required query, header and cookie parameters are not inferred by runtime
  discovery and need an explicit entity mapping.
- OAuth2 token acquisition, OpenID Connect and mutual TLS remain future work.
