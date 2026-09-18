"""Generate from the canonical checkout; build an sdist without external generators."""

from pathlib import Path
import shutil
import subprocess

from setuptools import build_meta as _backend

SDK = Path(__file__).resolve().parent
ROOT = SDK.parent


def _prepare():
    # Setuptools reuses build/lib; remove stale modules from earlier generations.
    if (SDK / "build").is_dir():
        shutil.rmtree(SDK / "build")
    if (ROOT / "buf.yaml").is_file() and (
        ROOT / "proto/kubling/provider/v1/provider.proto"
    ).is_file():
        # Always regenerate in a checkout, so stale local output cannot be published.
        subprocess.run(
            ["bash", str(ROOT / "generate.sh"), "python"], cwd=ROOT, check=True
        )
        package = SDK / "generated/kubling/provider"
        shutil.copytree(
            ROOT / "proto/kubling/provider",
            package / "proto/kubling/provider",
            dirs_exist_ok=True,
        )
        shutil.copyfile(ROOT / "LICENSE", SDK / "LICENSE")

    required = ["LICENSE"]
    required += [
        f"generated/kubling/provider/v1/{name}_pb2.py"
        for name in (
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
    ]
    required += ["generated/kubling/provider/v1/provider_pb2_grpc.py"]
    for relative in required:
        if not (SDK / relative).is_file():
            raise RuntimeError(f"Incomplete source distribution: missing {relative}")


def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):
    _prepare()
    return _backend.build_wheel(wheel_directory, config_settings, metadata_directory)


def build_sdist(sdist_directory, config_settings=None):
    _prepare()
    return _backend.build_sdist(sdist_directory, config_settings)


def build_editable(wheel_directory, config_settings=None, metadata_directory=None):
    _prepare()
    return _backend.build_editable(wheel_directory, config_settings, metadata_directory)


def prepare_metadata_for_build_wheel(metadata_directory, config_settings=None):
    _prepare()
    return _backend.prepare_metadata_for_build_wheel(metadata_directory, config_settings)


def get_requires_for_build_wheel(config_settings=None):
    _prepare()
    return _backend.get_requires_for_build_wheel(config_settings)


def get_requires_for_build_sdist(config_settings=None):
    _prepare()
    return _backend.get_requires_for_build_sdist(config_settings)


def get_requires_for_build_editable(config_settings=None):
    _prepare()
    return _backend.get_requires_for_build_editable(config_settings)
