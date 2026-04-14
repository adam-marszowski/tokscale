#!/usr/bin/env bash
set -euo pipefail

EXPECTED_VERSION="${1:-}"
if [[ "${EXPECTED_VERSION}" == "--expect-version" ]]; then
  if [[ -z "${2:-}" ]]; then
    echo "--expect-version requires a value" >&2
    exit 2
  fi
  EXPECTED_VERSION="${2}"
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

python3 - <<'PY' "${EXPECTED_VERSION}"
import json
import pathlib
import sys

expected_version = sys.argv[1] or None
root = pathlib.Path(".")

try:
    import tomllib
except ModuleNotFoundError:
    tomllib = None

if tomllib is None:
    raise SystemExit("Python tomllib is required (Python 3.11+)")

with (root / "Cargo.toml").open("rb") as cargo_file:
    cargo_data = tomllib.load(cargo_file)

workspace_section = cargo_data.get("workspace", {}).get("package", {})
workspace_version = workspace_section.get("version")
if not workspace_version:
    raise SystemExit("Could not find [workspace.package] version in Cargo.toml")
if expected_version and workspace_version != expected_version:
    raise SystemExit(
        f"Cargo workspace version mismatch: expected {expected_version}, found {workspace_version}"
    )

def load_json(path: str) -> dict:
    return json.loads((root / path).read_text())

cli_package = load_json("packages/cli/package.json")
cli_package_name = cli_package.get("name")
if not cli_package_name:
    raise SystemExit("packages/cli/package.json is missing its package name")

errors: list[str] = []

if cli_package_name != "@softiq/tokscale-om":
    errors.append(
        f"packages/cli/package.json name: expected @softiq/tokscale-om, found {cli_package_name}"
    )
if set(cli_package.get("bin", {}).keys()) != {"tokscale-om"}:
    errors.append("packages/cli/package.json should expose only the tokscale-om bin")
if cli_package.get("publishConfig", {}).get("registry") != "https://registry.npmjs.org/":
    errors.append("packages/cli/package.json publishConfig.registry should be https://registry.npmjs.org/")
if cli_package.get("publishConfig", {}).get("access") != "public":
    errors.append("packages/cli/package.json publishConfig.access should be public")

platform_packages = sorted((root / "packages").glob("cli-*/package.json"))
if not platform_packages:
    raise SystemExit("No platform package manifests found under packages/cli-*")

def expect_equal(label: str, actual: str, expected: str) -> None:
    if actual != expected:
        errors.append(f"{label}: expected {expected}, found {actual}")

expect_equal("packages/cli/package.json version", cli_package["version"], workspace_version)

platform_names = set()
for path in platform_packages:
    manifest = json.loads(path.read_text())
    name = manifest.get("name")
    if not name:
        errors.append(f"{path} missing package name")
        continue
    if not name.startswith("@softiq/tokscale-om-"):
        errors.append(f"{path} package name should use @softiq/tokscale-om-* scope, found {name}")
    platform_names.add(name)
    expect_equal(f"{path} version", manifest["version"], workspace_version)
    if manifest.get("publishConfig", {}).get("registry") != "https://registry.npmjs.org/":
        errors.append(f"{path} publishConfig.registry should be https://registry.npmjs.org/")
    if manifest.get("publishConfig", {}).get("access") != "public":
        errors.append(f"{path} publishConfig.access should be public")

expected_optional = platform_names
actual_optional = set(cli_package["optionalDependencies"].keys())
if actual_optional != expected_optional:
    errors.append(
        "packages/cli optionalDependencies keys mismatch: "
        f"expected {sorted(expected_optional)}, found {sorted(actual_optional)}"
    )

for name, version in cli_package["optionalDependencies"].items():
    expect_equal(f"packages/cli optional dependency {name}", version, workspace_version)

missing_manifests = actual_optional - platform_names
extra_manifests = platform_names - actual_optional
if missing_manifests:
    errors.append(
        "Missing platform manifests for optional dependencies: "
        f"{sorted(missing_manifests)}"
    )
if extra_manifests:
    errors.append(
        "Platform manifests not listed in optionalDependencies: "
        f"{sorted(extra_manifests)}"
    )

if expected_version and cli_package["version"] != expected_version:
    errors.append(
        f"packages/cli/package.json version mismatch: expected {expected_version}, found {cli_package['version']}"
    )

if errors:
    raise SystemExit("Version coherence check failed:\n- " + "\n- ".join(errors))

print(f"Version coherence OK: {workspace_version}")
PY
