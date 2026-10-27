import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import check_release


class ReleaseValidationTest(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary_directory.name)
        self.write("VERSION", "0.6.0\n")
        self.write("sdk-go/VERSION", "0.6.0\n")
        self.write("sdk-go/go.mod", "module example.test/sdk-go\n")
        self.write(
            "sdk-java/pom.xml",
            """<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <groupId>example.test</groupId>
  <artifactId>provider</artifactId>
  <version>0.6.0</version>
  <properties>
    <kubling.grpc.version>1.1.1</kubling.grpc.version>
  </properties>
</project>
""",
        )
        self.write(
            "sdk-python/pyproject.toml",
            """[project]
name = "example-provider"
version = "0.6.0"
dependencies = ["kubling-grpc==1.1.1"]
""",
        )
        for provider in check_release.PROVIDER_MODULES:
            self.write_provider_version(provider, "0.6.0")
        self.write("proto/provider.proto", 'syntax = "proto3";\n')
        self.write("buf.yaml", "version: v2\n")
        self.write("buf.lock", "# lock\n")

        self.git("init", "--quiet")
        self.git("config", "user.name", "Release Test")
        self.git("config", "user.email", "release-test@example.invalid")
        self.commit("coordinated release")
        for tag in check_release.release_tags("0.6.0").values():
            self.git("tag", tag)

    def tearDown(self):
        self.temporary_directory.cleanup()

    def write(self, relative_path, content):
        path = self.root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

    def write_provider_version(self, provider, version):
        self.write(
            f"providers/{provider}/go.mod",
            "\n".join(
                (
                    f"module example.test/{provider}",
                    "",
                    "go 1.26.0",
                    "",
                    "require github.com/kubling-community/kubling-providers/sdk-go "
                    f"v{version}",
                    "",
                )
            ),
        )

    def git(self, *arguments):
        return subprocess.run(
            ["git", *arguments],
            cwd=self.root,
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        ).stdout.strip()

    def commit(self, message):
        self.git("add", ".")
        self.git("commit", "--quiet", "-m", message)

    def advance_go_sdk(self, version="0.6.1"):
        self.write("sdk-go/VERSION", f"{version}\n")
        self.write("sdk-go/go.mod", "module example.test/sdk-go\n\n// maintenance\n")
        for provider in check_release.PROVIDER_MODULES:
            self.write_provider_version(provider, version)
        self.commit(f"Go SDK {version}")

    def test_independent_go_patch_uses_its_own_version(self):
        self.advance_go_sdk()

        self.assertEqual("0.6.0", check_release.validate_manifests(self.root))
        self.assertEqual(
            "sdk-go/v0.6.1",
            check_release.validate_tag("go", "sdk-go/v0.6.1", self.root),
        )
        self.assertEqual(
            "sdk-java/v0.6.0",
            check_release.validate_tag("java", "sdk-java/v0.6.0", self.root),
        )
        contract, sdk, base = check_release.validate_go_maintenance(self.root)
        self.assertEqual(("0.6.0", "0.6.1"), (contract, sdk))
        self.assertEqual(self.git("rev-parse", "proto/v0.6.0^{commit}"), base)

    def test_independent_go_patch_rejects_protocol_changes(self):
        self.advance_go_sdk()
        self.write("proto/provider.proto", 'syntax = "proto3";\nmessage Changed {}\n')
        self.commit("change protocol")

        with self.assertRaisesRegex(ValueError, "cannot change the provider protocol"):
            check_release.validate_go_maintenance(self.root)

    def test_protocol_adjacent_documentation_does_not_block_go_patch(self):
        self.advance_go_sdk()
        self.write("proto/README.md", "Updated protocol documentation.\n")
        self.commit("document protocol")

        contract, sdk, _ = check_release.validate_go_maintenance(self.root)
        self.assertEqual(("0.6.0", "0.6.1"), (contract, sdk))

    def test_independent_go_release_must_be_a_later_patch(self):
        self.advance_go_sdk("0.7.0")

        with self.assertRaisesRegex(ValueError, "must be a later patch"):
            check_release.validate_go_maintenance(self.root)

    def test_provider_pins_must_follow_the_go_sdk_version(self):
        self.advance_go_sdk()
        self.write_provider_version("host", "0.6.0")

        with self.assertRaisesRegex(ValueError, "host"):
            check_release.validate_manifests(self.root)

    def test_contract_train_still_requires_coordinated_versions(self):
        self.advance_go_sdk()

        with self.assertRaisesRegex(ValueError, "sdk-go/VERSION to equal VERSION"):
            check_release.validate_release_train("HEAD", self.root)

    def test_coordinated_release_train_remains_valid(self):
        contract, sdk, base = check_release.validate_go_maintenance(self.root)
        self.assertEqual(("0.6.0", "0.6.0", None), (contract, sdk, base))
        version, commit = check_release.validate_release_train("HEAD", self.root)
        self.assertEqual("0.6.0", version)
        self.assertEqual(self.git("rev-parse", "HEAD"), commit)


if __name__ == "__main__":
    unittest.main()
