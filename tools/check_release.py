#!/usr/bin/env python3
"""Validate provider contract, SDK and release-tag identities."""

import argparse
from pathlib import Path
import re
import subprocess
import xml.etree.ElementTree as ET

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

ROOT = Path(__file__).resolve().parents[1]
STABLE_VERSION = re.compile(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)")
TAG_PREFIXES = {
    "proto": "proto/v",
    "go": "sdk-go/v",
    "java": "sdk-java/v",
    "python": "sdk-python/v",
}
PROVIDER_MODULES = (
    "cassandra",
    "host",
    "inmemory",
    "kubernetes",
    "openapi",
    "redis",
)


def stable_version(path, label):
    version = path.read_text(encoding="utf-8").strip()
    if not STABLE_VERSION.fullmatch(version):
        raise ValueError(f"{label} must contain one stable MAJOR.MINOR.PATCH version")
    return version


def release_version(root=ROOT):
    return stable_version(root / "VERSION", "VERSION")


def go_sdk_version(root=ROOT):
    return stable_version(root / "sdk-go/VERSION", "sdk-go/VERSION")


def version_numbers(version):
    match = STABLE_VERSION.fullmatch(version)
    if not match:
        raise ValueError(f"Invalid stable release version: {version}")
    return tuple(int(part) for part in match.groups())


def package_versions(root=ROOT):
    namespace = {"m": "http://maven.apache.org/POM/4.0.0"}
    pom = ET.parse(root / "sdk-java/pom.xml")
    java = pom.findtext("m:version", namespaces=namespace)
    shared_java = pom.findtext("m:properties/m:kubling.grpc.version", namespaces=namespace)
    with (root / "sdk-python/pyproject.toml").open("rb") as manifest:
        python_manifest = tomllib.load(manifest)["project"]
    shared_python = [
        dependency
        for dependency in python_manifest["dependencies"]
        if dependency.startswith("kubling-grpc==")
    ]
    return {
        "java": java,
        "python": python_manifest["version"],
        "shared_java": shared_java,
        "shared_python": shared_python,
    }


def validate_manifests(root=ROOT):
    version = release_version(root)
    sdk_version = go_sdk_version(root)
    packages = package_versions(root)
    mismatches = {
        language: packages[language]
        for language in ("java", "python")
        if packages[language] != version
    }
    if mismatches:
        details = ", ".join(f"{name}={value}" for name, value in mismatches.items())
        raise ValueError(f"Package versions must equal VERSION {version}: {details}")
    if packages["shared_java"] != "1.1.1" or packages["shared_python"] != [
        "kubling-grpc==1.1.1"
    ]:
        raise ValueError(
            "Java and Python packages must use the shared kubling-grpc 1.1.1 contract"
        )

    expected_sdk = f"github.com/kubling-community/kubling-providers/sdk-go v{sdk_version}"
    stale_modules = [
        name
        for name in PROVIDER_MODULES
        if expected_sdk
        not in (root / f"providers/{name}/go.mod").read_text(encoding="utf-8")
    ]
    if stale_modules:
        raise ValueError(
            f"Provider modules do not use Go SDK v{sdk_version}: "
            + ", ".join(stale_modules)
        )

    return version


def release_tags(version):
    if not STABLE_VERSION.fullmatch(version):
        raise ValueError(f"Invalid stable release version: {version}")
    return {language: prefix + version for language, prefix in TAG_PREFIXES.items()}


def validate_tag(language, tag, root=ROOT):
    contract_version = validate_manifests(root)
    version = go_sdk_version(root) if language == "go" else contract_version
    expected = TAG_PREFIXES[language] + version
    if tag != expected:
        raise ValueError(f"Expected tag {expected}, got {tag}")
    return expected


def git(*arguments, root=ROOT, check=True):
    result = subprocess.run(
        ["git", *arguments],
        cwd=root,
        check=check,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return result.stdout.strip()


def tag_commit(tag, root=ROOT):
    result = git("rev-parse", "--verify", f"refs/tags/{tag}^{{commit}}", root=root, check=False)
    return result or None


def validate_coordinated_version(root=ROOT):
    version = validate_manifests(root)
    sdk_version = go_sdk_version(root)
    if sdk_version != version:
        raise ValueError(
            "Coordinated contract releases require sdk-go/VERSION to equal "
            f"VERSION {version}; found {sdk_version}"
        )
    return version


def validate_go_maintenance(root=ROOT):
    contract_version = validate_manifests(root)
    sdk_version = go_sdk_version(root)
    if sdk_version == contract_version:
        return contract_version, sdk_version, None

    contract_numbers = version_numbers(contract_version)
    sdk_numbers = version_numbers(sdk_version)
    if sdk_numbers[:2] != contract_numbers[:2] or sdk_numbers[2] <= contract_numbers[2]:
        raise ValueError(
            "An independent Go SDK release must be a later patch of the current "
            f"contract version {contract_version}; found {sdk_version}"
        )

    tags = release_tags(contract_version)
    resolved = {tag: tag_commit(tag, root) for tag in tags.values()}
    missing = [tag for tag, commit in resolved.items() if commit is None]
    if missing:
        raise ValueError(
            "Cannot validate the Go SDK maintenance base; missing coordinated tags: "
            + ", ".join(missing)
        )

    base_commit = resolved[tags["proto"]]
    mismatches = {tag: commit for tag, commit in resolved.items() if commit != base_commit}
    if mismatches:
        details = ", ".join(f"{tag}={commit}" for tag, commit in mismatches.items())
        raise ValueError(
            f"Contract {contract_version} tags do not identify one release: {details}"
        )

    protocol_paths = (":(glob)proto/**/*.proto", "buf.yaml", "buf.lock")
    changed = git("diff", "--name-only", tags["proto"], "--", *protocol_paths, root=root)
    if changed:
        details = ", ".join(changed.splitlines())
        raise ValueError(
            "Independent Go SDK releases cannot change the provider protocol; "
            f"changed paths: {details}"
        )

    return contract_version, sdk_version, base_commit


def validate_tags_available(root=ROOT):
    version = validate_coordinated_version(root)
    existing = [tag for tag in release_tags(version).values() if tag_commit(tag, root)]
    if existing:
        raise ValueError("Release tags already exist: " + ", ".join(existing))
    return version


def validate_release_train(expected_commit="HEAD", root=ROOT):
    version = validate_coordinated_version(root)
    expected = git("rev-parse", f"{expected_commit}^{{commit}}", root=root)
    resolved = {tag: tag_commit(tag, root) for tag in release_tags(version).values()}
    missing = [tag for tag, commit in resolved.items() if commit is None]
    if missing:
        raise ValueError("Missing release tags: " + ", ".join(missing))
    mismatches = {tag: commit for tag, commit in resolved.items() if commit != expected}
    if mismatches:
        details = ", ".join(f"{tag}={commit}" for tag, commit in mismatches.items())
        raise ValueError(f"Release tags must resolve to {expected}: {details}")
    return version, expected


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "target",
        choices=("all", "available", "train", "go-maintenance", *TAG_PREFIXES),
    )
    parser.add_argument("value", nargs="?", help="Tag or expected commit")
    args = parser.parse_args()
    try:
        if args.target == "all":
            if args.value:
                parser.error("all does not accept a value")
            contract_version = validate_manifests()
            print(
                f"Validated contract {contract_version} and Go SDK {go_sdk_version()}"
            )
        elif args.target == "available":
            if args.value:
                parser.error("available does not accept a value")
            print(f"Validated release tag availability for {validate_tags_available()}")
        elif args.target == "train":
            version, commit = validate_release_train(args.value or "HEAD")
            print(f"Validated release train {version} at {commit}")
        elif args.target == "go-maintenance":
            if args.value:
                parser.error("go-maintenance does not accept a value")
            contract_version, sdk_version, base_commit = validate_go_maintenance()
            if base_commit is None:
                print(
                    f"Go SDK {sdk_version} is coordinated with contract "
                    f"{contract_version}"
                )
            else:
                print(
                    f"Validated Go SDK {sdk_version} maintenance base "
                    f"for contract {contract_version} at {base_commit}"
                )
        else:
            if not args.value:
                parser.error(f"{args.target} requires a tag")
            print(f"Validated {validate_tag(args.target, args.value)}")
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
