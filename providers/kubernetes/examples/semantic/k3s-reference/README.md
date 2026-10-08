# k3s semantic reference

This directory contains an opt-in semantic profile for a broad k3s catalog.
It complements the small workload fragment bundled with the provider; the
provider does not return this reference automatically from
`GetSemanticFragment`.

The profile consists of:

- `fragment.yaml`, the generated source-local semantic model;
- `provider.yaml`, the provider settings required by its field bindings;
- `provenance.yaml`, the pinned source image, artifact digest and model counts;
- `generate.js`, the source of truth for the generated files.

The fragment uses unqualified relation and field names. Relationships with
exact joins are emitted first; selector-, array- and convention-based links are
kept semantic-only.

## Regenerate

Run the generator from the repository root:

```sh
node providers/kubernetes/examples/semantic/k3s-reference/generate.js
node providers/kubernetes/examples/semantic/k3s-reference/generate.js --check
```

Change the profile version when the generated semantic document changes.

## Validate against k3s

Use the image and digest recorded in `provenance.yaml` as `K3S_IMAGE`, then
start the Kubernetes provider's local fixture. From `providers/kubernetes`:

```sh
profile=examples/semantic/k3s-reference/provenance.yaml
image="$(sed -n 's/^  image: //p' "${profile}")"
digest="$(sed -n 's/^  imageDigest: //p' "${profile}")"
K3S_IMAGE="${image}@${digest}" ./local/run.sh k3s

KUBLING_KUBERNETES_INTEGRATION=1 \
  go test -run TestKubernetesIntegrationK3sReferenceFragmentMatchesMetadata -v .
```

The reference is intentionally not a universal Kubernetes model. It describes
the fixed resource set in `generate.js` for the pinned k3s fixture. RBAC,
disabled or optional APIs, different Kubernetes releases and additional CRDs
can change the catalog and invalidate bindings. Review and regenerate the
profile before using it with another cluster shape.
