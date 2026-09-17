#!/usr/bin/env python3
"""Inspect distributable Java and Python provider binding packages."""

import argparse
from pathlib import Path
import tarfile
import zipfile

from check_release import release_version

ROOT = Path(__file__).resolve().parents[1]


def require_members(actual, required):
    missing = set(required) - set(actual)
    if missing:
        raise ValueError(f"Missing package members: {sorted(missing)}")
    if any(".local-notes" in name or "__pycache__" in name for name in actual):
        raise ValueError("Local notes or bytecode caches leaked into a distribution")


def java():
    version = release_version()
    jars = list((ROOT / "sdk-java/target").glob("*.jar"))
    binaries = [path for path in jars if not path.name.endswith(("-sources.jar", "-javadoc.jar"))]
    if len(binaries) != 1:
        raise ValueError("Expected exactly one Java binary JAR; run clean verify")
    binary = binaries[0]
    if binary.name != f"kubling-provider-grpc-{version}.jar":
        raise ValueError(f"Java artifact does not match VERSION {version}: {binary.name}")
    package = "com/kubling/provider/grpc/"
    with zipfile.ZipFile(binary) as archive:
        names = archive.namelist()
        require_members(
            names,
            [package + name + ".class" for name in (
                "ProviderServiceGrpc",
                "Field",
                "ValueCapabilities",
                "ReadLobRequest",
            )]
            + [
                "META-INF/LICENSE",
                "META-INF/proto/kubling/provider/v1/provider.proto",
            ],
        )
        duplicated = [
            name
            for name in names
            if name.startswith("com/kubling/transport/grpc/") and name.endswith(".class")
        ]
        if duplicated:
            raise ValueError("Shared kubling-grpc classes leaked into provider JAR")
        for name in names:
            if name.endswith(".class") and int.from_bytes(archive.read(name)[6:8], "big") != 65:
                raise ValueError("The Java artifact must contain Java 21 bytecode")
    with zipfile.ZipFile(binary.with_name(binary.stem + "-sources.jar")) as archive:
        require_members(
            archive.namelist(),
            [package + "ProviderServiceGrpc.java", package + "Field.java"],
        )
    with zipfile.ZipFile(binary.with_name(binary.stem + "-javadoc.jar")) as archive:
        require_members(
            archive.namelist(),
            ["index.html", package + "ProviderServiceGrpc.html"],
        )


def python():
    version = release_version()
    dist = ROOT / "sdk-python/dist"
    wheels, sources = list(dist.glob("*.whl")), list(dist.glob("*.tar.gz"))
    if len(wheels) != 1 or len(sources) != 1:
        raise ValueError("Expected one wheel and one sdist; clear stale dist artifacts")
    prefix = f"kubling_provider_grpc-{version}"
    if not wheels[0].name.startswith(prefix + "-") or sources[0].name != prefix + ".tar.gz":
        raise ValueError(f"Python artifacts do not match VERSION {version}")
    modules = (
        "capabilities",
        "connection",
        "expression",
        "lob",
        "metadata",
        "mutation",
        "provider",
        "query",
        "semantic",
        "tuple",
    )
    required = [f"kubling/provider/v1/{name}_pb2.py" for name in modules]
    required += [
        "kubling/provider/v1/provider_pb2_grpc.py",
        "kubling/provider/proto/kubling/provider/v1/provider.proto",
    ]
    with zipfile.ZipFile(wheels[0]) as archive:
        names = archive.namelist()
        require_members(names, required)
        duplicated = [
            name
            for name in names
            if name.startswith("kubling/v1/") and name.endswith(("_pb2.py", "_pb2_grpc.py"))
        ]
        if duplicated:
            raise ValueError("Shared kubling-grpc modules leaked into provider wheel")
        if not any(name.endswith(".dist-info/licenses/LICENSE") for name in names):
            raise ValueError("Wheel license missing")
        metadata_files = [name for name in names if name.endswith(".dist-info/METADATA")]
        if len(metadata_files) != 1:
            raise ValueError("Expected one wheel METADATA file")
        metadata = archive.read(metadata_files[0]).decode()
        if f"\nVersion: {version}\n" not in metadata:
            raise ValueError("Wheel metadata version does not match VERSION")
        if "Requires-Dist: kubling-grpc==1.1.1" not in metadata:
            raise ValueError("Wheel must depend on kubling-grpc==1.1.1")
    with tarfile.open(sources[0]) as archive:
        members = archive.getnames()
        if not members or any(
            name != prefix and not name.startswith(prefix + "/") for name in members
        ):
            raise ValueError("Source distribution root does not match VERSION")
        names = [name.partition("/")[2] for name in members]
        require_members(
            names,
            ["generated/" + name for name in required]
            + ["LICENSE", "pyproject.toml", "_build_backend.py"],
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("language", choices=("java", "python"))
    args = parser.parse_args()
    {"java": java, "python": python}[args.language]()
    print(f"Validated {args.language} distribution contents")
