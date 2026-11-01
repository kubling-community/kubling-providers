# Debian packages

This directory builds two packages:

- `kubling-host-agent`, with the agent binary, example configuration and
  systemd unit;
- `kubling-host-provider`, with the provider binary and deployment guide.

The scripts require Go and [nFPM](https://nfpm.goreleaser.com/). Set `VERSION`
to the semantic version being packaged, then run from the repository root:

```sh
for architecture in amd64 arm64; do
  providers/host/packaging/build-deb.sh "$VERSION" "$architecture"
  providers/host/packaging/build-provider-deb.sh "$VERSION" "$architecture"
done

for package in providers/host/dist/*.deb; do
  providers/host/packaging/verify-deb.sh "$package"
done
```

Packages are written to `providers/host/dist/`. Pass a third argument to either
build script to use another output directory. `GO_BINARY` and `NFPM_BINARY` can
select executables that are not on `PATH`.

Installation and service configuration are covered by the
[deployment guide](../docs/deployment.md).
