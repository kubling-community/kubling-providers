#!/usr/bin/env python3
"""Validate the canonical provider contract release version and tags."""

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
PROVIDER_MODULES = ("cassandra", "inmemory", "kubernetes", "openapi", "redis")


def release_version(root=ROOT):
    version = (root / "VERSION").read_text(encoding="utf-8").strip()
    if not STABLE_VERSION.fullmatch(version):
        raise ValueError("VERSION must contain one stable MAJOR.MINOR.PATCH version")
    return version


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
        raise ValueError("Java and Python packages must use the shared kubling-grpc 1.1.1 contract")

    expected_sdk = f"github.com/kubling-community/kubling-providers/sdk-go v{version}"
    stale_modules = [
        name
        for name in PROVIDER_MODULES
        if expected_sdk
        not in (root / f"providers/{name}/go.mod").read_text(encoding="utf-8")
    ]
    if stale_modules:
        raise ValueError(
            "Provider modules do not use the coordinated Go SDK version: "
            + ", ".join(stale_modules)
        )

    return version


def release_tags(version):
    if not STABLE_VERSION.fullmatch(version):
        raise ValueError(f"Invalid stable release version: {version}")
    return {language: prefix + version for language, prefix in TAG_PREFIXES.items()}


def validate_tag(language, tag, root=ROOT):
    expected = release_tags(validate_manifests(root))[language]
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


def validate_tags_available(root=ROOT):
    version = validate_manifests(root)
    existing = [tag for tag in release_tags(version).values() if tag_commit(tag, root)]
    if existing:
        raise ValueError("Release tags already exist: " + ", ".join(existing))
    return version


def validate_release_train(expected_commit="HEAD", root=ROOT):
    version = validate_manifests(root)
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
    parser.add_argument("target", choices=("all", "available", "train", *TAG_PREFIXES))
    parser.add_argument("value", nargs="?", help="Tag or expected commit")
    args = parser.parse_args()
    try:
        if args.target == "all":
            if args.value:
                parser.error("all does not accept a value")
            print(f"Validated coordinated version {validate_manifests()}")
        elif args.target == "available":
            if args.value:
                parser.error("available does not accept a value")
            print(f"Validated release tag availability for {validate_tags_available()}")
        elif args.target == "train":
            version, commit = validate_release_train(args.value or "HEAD")
            print(f"Validated release train {version} at {commit}")
        else:
            if not args.value:
                parser.error(f"{args.target} requires a tag")
            print(f"Validated {validate_tag(args.target, args.value)}")
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
