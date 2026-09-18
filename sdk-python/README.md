# Kubling provider gRPC Python bindings

Generated messages and gRPC stubs for the Kubling provider protocol. This
package contains provider bindings only.

Distribution name: `kubling-provider-grpc`.

```sh
pip install kubling-provider-grpc
```

```python
from kubling.provider.v1 import provider_pb2_grpc

provider = provider_pb2_grpc.ProviderServiceStub(channel)
```

Shared `kubling.v1` types come from `kubling-grpc`; they are not repackaged
here.

Build from the repository root:

```sh
python -m pip install -r sdk-python/requirements-build.txt
python -m build sdk-python
python -m twine check --strict sdk-python/dist/*
```
