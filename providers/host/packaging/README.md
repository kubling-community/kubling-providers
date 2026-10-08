# Maintainer packaging notes

Build a Debian package locally with `build-deb.sh`. The script expects Go and
nFPM and writes the package under `dist/` unless another directory is supplied.

The host-provider workflow validates packages on branches and pull requests.
Release tags additionally publish them to the configured public Cloudsmith
repository and attach them to the GitHub release. The workflow is the source of
truth for repository variables and credentials.

End-user installation and service configuration are documented in the public
[deployment guide](../docs/deployment.md).
