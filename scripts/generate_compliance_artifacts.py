"""Generate SBOM and legal notice artifacts for the final release.

The script intentionally relies on uv for dependency resolution because both
application services are pyproject-based and uv-managed. License labels are
derived from Python package metadata, the declared Python runtime, and frontend
CDN assets.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
PROJECTS = {
    "Backend": ROOT / "Backend",
    "Frontend": ROOT / "Frontend",
}
LOCAL_PROJECT_NAMES = {"backend", "frontend"}

# The production Dockerfiles use python:3.11-slim. A local developer may run
# this script on Windows or another Python version, so missing package metadata
# is resolved against a Linux/Python 3.11 target to match the release runtime.
TARGET_PYTHON_VERSION = "3.11"
TARGET_PYTHON_PLATFORM = "linux"

SBOM_PATH = ROOT / "sbom.cdx.json"
LEGAL_MD_PATH = ROOT / "LEGAL_NOTICES.md"
LEGAL_JSON_PATH = ROOT / "Frontend" / "web" / "static" / "legal_notices.json"

METADATA_SNIPPET = """
import importlib.metadata as m
import json

records = []
for dist in m.distributions():
    metadata = dist.metadata
    records.append({
        "name": metadata.get("Name"),
        "version": dist.version,
        "license_expression": metadata.get("License-Expression"),
        "license": metadata.get("License"),
        "classifiers": metadata.get_all("Classifier") or [],
        "summary": metadata.get("Summary"),
        "home_page": metadata.get("Home-page"),
        "project_url": metadata.get_all("Project-URL") or [],
    })
print(json.dumps(records, ensure_ascii=True))
"""

# Frontend libraries loaded from a CDN are not visible in Python dependency
# metadata, so they are declared explicitly here.
CDN_COMPONENTS: list[dict[str, Any]] = [
    {
        "type": "library",
        "bom-ref": "cdn:bootstrap@5.3.3",
        "name": "bootstrap",
        "version": "5.3.3",
        "purl": "pkg:npm/bootstrap@5.3.3",
        "licenses": [{"license": {"id": "MIT"}}],
        "externalReferences": [
            {
                "type": "distribution",
                "url": "https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/",
            }
        ],
        "properties": [
            {"name": "ata:project", "value": "Frontend"},
            {"name": "ata:source", "value": "CDN asset referenced by base.html"},
        ],
    },
    {
        "type": "library",
        "bom-ref": "cdn:bootstrap-icons@1.11.3",
        "name": "bootstrap-icons",
        "version": "1.11.3",
        "purl": "pkg:npm/bootstrap-icons@1.11.3",
        "licenses": [{"license": {"id": "MIT"}}],
        "externalReferences": [
            {
                "type": "distribution",
                "url": "https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/",
            }
        ],
        "properties": [
            {"name": "ata:project", "value": "Frontend"},
            {"name": "ata:source", "value": "CDN asset referenced by base.html"},
        ],
    },
]

# The Python interpreter is part of the deployed runtime even though it is not a
# package resolved by uv. Keep this aligned with Backend/Dockerfile and
# Frontend/Dockerfile.
RUNTIME_COMPONENTS: list[dict[str, Any]] = [
    {
        "type": "platform",
        "bom-ref": "runtime:python@3.11",
        "name": "Python",
        "version": "3.11",
        "purl": "pkg:generic/python@3.11",
        "licenses": [{"license": {"id": "Python-2.0"}}],
        "externalReferences": [
            {
                "type": "website",
                "url": "https://www.python.org/",
            }
        ],
        "properties": [
            {"name": "ata:project", "value": "Backend"},
            {"name": "ata:project", "value": "Frontend"},
            {"name": "ata:source", "value": "Docker runtime image python:3.11-slim"},
        ],
    },
]


def main() -> None:
    timestamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    uv_version = run(["uv", "--version"], ROOT).strip()
    uv_version_number = uv_version.split()[1] if len(uv_version.split()) > 1 else uv_version

    project_boms = {name: uv_export_cyclonedx(path) for name, path in PROJECTS.items()}
    metadata_by_project = {
        name: read_python_metadata(path) for name, path in PROJECTS.items()
    }
    fill_missing_python_metadata(project_boms, metadata_by_project)

    combined = build_combined_sbom(
        project_boms, metadata_by_project, timestamp, uv_version_number
    )
    SBOM_PATH.write_text(json.dumps(combined, indent=2) + "\n", encoding="utf-8")

    legal_data = build_legal_notice_data(combined, timestamp, uv_version)
    LEGAL_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
    LEGAL_JSON_PATH.write_text(
        json.dumps(legal_data, indent=2, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )
    LEGAL_MD_PATH.write_text(render_legal_markdown(legal_data), encoding="utf-8")

    print(f"Wrote {SBOM_PATH.relative_to(ROOT)}")
    print(f"Wrote {LEGAL_MD_PATH.relative_to(ROOT)}")
    print(f"Wrote {LEGAL_JSON_PATH.relative_to(ROOT)}")


def run(cmd: list[str], cwd: Path) -> str:
    result = subprocess.run(
        cmd,
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


def uv_export_cyclonedx(project_dir: Path) -> dict[str, Any]:
    """Export one uv project as CycloneDX before merging service-level SBOMs."""
    with tempfile.TemporaryDirectory() as temp_dir:
        output_file = Path(temp_dir) / "sbom.cdx.json"
        subprocess.run(
            [
                "uv",
                "export",
                "--format",
                "cyclonedx1.5",
                "--all-extras",
                "--all-groups",
                "--output-file",
                str(output_file),
            ],
            cwd=project_dir,
            check=True,
            capture_output=True,
            text=True,
        )
        return json.loads(output_file.read_text(encoding="utf-8"))


def read_python_metadata(project_dir: Path) -> dict[tuple[str, str], dict[str, Any]]:
    """Read license/source metadata from packages installed by uv for a service."""
    output = run(["uv", "run", "python", "-c", METADATA_SNIPPET], project_dir)
    metadata_rows = json.loads(output)
    return metadata_rows_by_name_version(metadata_rows)


def metadata_rows_by_name_version(
    metadata_rows: list[dict[str, Any]],
) -> dict[tuple[str, str], dict[str, Any]]:
    by_name_version: dict[tuple[str, str], dict[str, Any]] = {}
    for row in metadata_rows:
        name = row.get("name")
        version = row.get("version")
        if not name or not version:
            continue
        by_name_version[(canonical_name(name), version)] = row
    return by_name_version


def fill_missing_python_metadata(
    project_boms: dict[str, dict[str, Any]],
    metadata_by_project: dict[str, dict[tuple[str, str], dict[str, Any]]],
) -> None:
    """Resolve metadata for platform-conditional packages absent on this host.

    Example: uvloop is part of the Linux runtime dependency graph, but it is not
    installed when this script is executed on Windows. Installing missing
    packages into a temporary Linux-target directory lets importlib.metadata read
    their wheel metadata without modifying the project environment.
    """
    for project_name, bom in project_boms.items():
        metadata_map = metadata_by_project[project_name]
        missing_specs: list[str] = []
        for component in bom.get("components", []):
            name = component.get("name", "")
            version = component.get("version", "")
            key = (canonical_name(name), version)
            if not name or not version or canonical_name(name) in LOCAL_PROJECT_NAMES:
                continue
            if key not in metadata_map:
                missing_specs.append(f"{name}=={version}")

        if not missing_specs:
            continue
        metadata_map.update(read_target_python_metadata(sorted(set(missing_specs))))


def read_target_python_metadata(
    package_specs: list[str],
) -> dict[tuple[str, str], dict[str, Any]]:
    with tempfile.TemporaryDirectory() as temp_dir:
        target = Path(temp_dir) / "site-packages"
        subprocess.run(
            [
                "uv",
                "pip",
                "install",
                "--target",
                str(target),
                "--python-version",
                TARGET_PYTHON_VERSION,
                "--python-platform",
                TARGET_PYTHON_PLATFORM,
                "--quiet",
                *package_specs,
            ],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        snippet = (
            "import importlib.metadata as m, json; "
            f"path = {json.dumps(str(target))}; "
            "records = []; "
            "records.extend({"
            "'name': d.metadata.get('Name'), "
            "'version': d.version, "
            "'license_expression': d.metadata.get('License-Expression'), "
            "'license': d.metadata.get('License'), "
            "'classifiers': d.metadata.get_all('Classifier') or [], "
            "'summary': d.metadata.get('Summary'), "
            "'home_page': d.metadata.get('Home-page'), "
            "'project_url': d.metadata.get_all('Project-URL') or []"
            "} for d in m.distributions(path=[path])); "
            "print(json.dumps(records, ensure_ascii=True))"
        )
        output = run([sys.executable, "-c", snippet], ROOT)
    return metadata_rows_by_name_version(json.loads(output))


def build_combined_sbom(
    project_boms: dict[str, dict[str, Any]],
    metadata_by_project: dict[str, dict[tuple[str, str], dict[str, Any]]],
    timestamp: str,
    uv_version: str,
) -> dict[str, Any]:
    """Merge backend, frontend, runtime, and CDN components into one SBOM."""
    root_ref = "automated-thematic-analysis@0.1.0"
    components: list[dict[str, Any]] = []
    dependencies: list[dict[str, Any]] = []
    root_depends_on: list[str] = []

    for project_name, bom in project_boms.items():
        prefix = project_name.lower()
        metadata_map = metadata_by_project[project_name]
        project_component = dict(bom["metadata"]["component"])
        project_component["bom-ref"] = prefixed_ref(prefix, project_component["bom-ref"])
        project_component.setdefault("properties", []).append(
            {"name": "ata:project", "value": project_name}
        )
        components.append(project_component)
        root_depends_on.append(project_component["bom-ref"])

        for component in bom.get("components", []):
            component = json.loads(json.dumps(component))
            original_ref = component["bom-ref"]
            component["bom-ref"] = prefixed_ref(prefix, original_ref)
            component.setdefault("properties", []).append(
                {"name": "ata:project", "value": project_name}
            )
            metadata = metadata_map.get(
                (canonical_name(component["name"]), component.get("version", ""))
            )
            license_id = extract_license(metadata) if metadata else "NOASSERTION"
            component["licenses"] = [cyclonedx_license_entry(license_id)]
            source_url = source_from_metadata(metadata)
            if source_url:
                component.setdefault("externalReferences", []).append(
                    {"type": "website", "url": source_url}
                )
            components.append(component)

        for dependency in bom.get("dependencies", []):
            dependencies.append(
                {
                    "ref": prefixed_ref(prefix, dependency["ref"]),
                    "dependsOn": [
                        prefixed_ref(prefix, ref)
                        for ref in dependency.get("dependsOn", [])
                    ],
                }
            )

    for component in CDN_COMPONENTS:
        components.append(json.loads(json.dumps(component)))
        root_depends_on.append(component["bom-ref"])
        dependencies.append({"ref": component["bom-ref"], "dependsOn": []})

    for component in RUNTIME_COMPONENTS:
        components.append(json.loads(json.dumps(component)))
        root_depends_on.append(component["bom-ref"])
        dependencies.append({"ref": component["bom-ref"], "dependsOn": []})

    dependencies.append({"ref": root_ref, "dependsOn": sorted(root_depends_on)})

    return {
        "bomFormat": "CycloneDX",
        "specVersion": "1.5",
        "version": 1,
        "serialNumber": f"urn:uuid:{uuid.uuid4()}",
        "metadata": {
            "timestamp": timestamp,
            "tools": [
                {
                    "vendor": "Astral Software Inc.",
                    "name": "uv",
                    "version": uv_version,
                },
                {
                    "vendor": "AMOS SS 2026 Project",
                    "name": "scripts/generate_compliance_artifacts.py",
                    "version": "1",
                },
            ],
            "component": {
                "type": "application",
                "bom-ref": root_ref,
                "name": "automated-thematic-analysis",
                "version": "0.1.0",
            },
        },
        "components": components,
        "dependencies": dependencies,
    }


def build_legal_notice_data(
    sbom: dict[str, Any], timestamp: str, uv_version: str
) -> dict[str, Any]:
    """Project the SBOM into the compact data shape consumed by the UI."""
    entries_by_key: dict[tuple[str, str, str, str], dict[str, Any]] = {}

    for component in sbom.get("components", []):
        name = component.get("name", "")
        version = component.get("version", "")
        if canonical_name(name) in LOCAL_PROJECT_NAMES:
            continue

        purl = component.get("purl", "")
        ecosystem = ecosystem_from_purl(purl)
        license_id = component_license(component)
        source = component_source(component) or purl
        scopes = sorted(
            {
                prop.get("value", "")
                for prop in component.get("properties", [])
                if prop.get("name") == "ata:project" and prop.get("value")
            }
        )

        key = (ecosystem, canonical_name(name), version, license_id)
        entry = entries_by_key.setdefault(
            key,
            {
                "name": name,
                "version": version,
                "ecosystem": ecosystem,
                "license": license_id,
                "used_in": [],
                "source": source,
                "purl": purl,
            },
        )
        entry["used_in"] = sorted(set(entry["used_in"]) | set(scopes))

    return {
        "generated_at": timestamp,
        "generated_by": (
            f"{uv_version}; scripts/generate_compliance_artifacts.py; "
            "license labels from Python package metadata and declared CDN assets"
        ),
        "scope": (
            "Includes backend and frontend Python dependencies resolved with uv "
            "including development groups, the Python runtime declared by the "
            "Dockerfiles, plus Bootstrap CDN assets referenced by the frontend "
            "layout."
        ),
        "review_note": (
            "License information is automatically generated from dependency "
            "metadata. No manual legal clearing was performed."
        ),
        "sbom_path": "sbom.cdx.json",
        "entries": sorted(
            entries_by_key.values(),
            key=lambda item: (
                item["ecosystem"].lower(),
                item["name"].lower(),
                item["version"],
            ),
        ),
    }


def render_legal_markdown(data: dict[str, Any]) -> str:
    lines = [
        "# Legal Notices",
        "",
        f"Generated: {data['generated_at']}",
        "",
        data["scope"],
        "",
        data["review_note"],
        "",
        f"Complete SBOM: `{data['sbom_path']}`",
        "",
        "| Component | Version | Ecosystem | Used in | License | Source |",
        "|---|---:|---|---|---|---|",
    ]

    for entry in data["entries"]:
        lines.append(
            "| {name} | {version} | {ecosystem} | {used_in} | {license} | {source} |".format(
                name=escape_markdown(entry["name"]),
                version=escape_markdown(entry["version"]),
                ecosystem=escape_markdown(entry["ecosystem"]),
                used_in=escape_markdown(", ".join(entry["used_in"])),
                license=escape_markdown(entry["license"]),
                source=escape_markdown(entry["source"]),
            )
        )

    return "\n".join(lines) + "\n"


def prefixed_ref(prefix: str, bom_ref: str) -> str:
    return f"{prefix}:{bom_ref}"


def canonical_name(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def extract_license(metadata: dict[str, Any] | None) -> str:
    if not metadata:
        return "NOASSERTION"

    expression = clean_license_value(metadata.get("license_expression"))
    if expression:
        return expression

    raw_license_text = metadata.get("license") or ""
    normalised_raw = normalise_license_text(raw_license_text)
    if normalised_raw and normalised_raw != raw_license_text:
        return normalised_raw

    raw_license = clean_license_value(raw_license_text)
    if raw_license:
        normalised = normalise_license_text(raw_license)
        if normalised:
            return normalised

    for classifier in metadata.get("classifiers", []):
        if not classifier.startswith("License ::"):
            continue
        normalised = normalise_license_text(classifier)
        if normalised:
            return normalised

    return "NOASSERTION"


def clean_license_value(value: str | None) -> str:
    if not value:
        return ""
    stripped = re.sub(r"\s+", " ", value.strip())
    if len(stripped) > 120:
        return ""
    return stripped


def normalise_license_text(value: str) -> str:
    """Map common metadata/classifier strings to SPDX-style identifiers."""
    lower = value.lower()
    if "mit" in lower:
        return "MIT"
    if "apache" in lower and ("2.0" in lower or "software license" in lower):
        return "Apache-2.0"
    if "mozilla public license 2.0" in lower or "mpl-2.0" in lower:
        return "MPL-2.0"
    if "bsd" in lower:
        return "BSD-3-Clause"
    if "python software foundation" in lower or "psf" in lower:
        return "PSF-2.0"
    if "isc" in lower:
        return "ISC"
    if "lgpl" in lower:
        return "LGPL"
    if "gpl" in lower:
        return "GPL"
    return value


def cyclonedx_license_entry(license_id: str) -> dict[str, dict[str, str] | str]:
    if license_id == "NOASSERTION":
        return {"license": {"name": license_id}}
    if re.search(r"\s(AND|OR|WITH)\s|[()]", license_id):
        return {"expression": license_id}
    if not re.match(r"^[A-Za-z0-9.+-]+$", license_id):
        return {"license": {"name": license_id}}
    return {"license": {"id": license_id}}


def component_license(component: dict[str, Any]) -> str:
    licenses = component.get("licenses") or []
    if not licenses:
        return "NOASSERTION"
    if licenses[0].get("expression"):
        return licenses[0]["expression"]
    license_entry = licenses[0].get("license", {})
    if license_entry.get("expression"):
        return license_entry["expression"]
    return license_entry.get("id") or license_entry.get("name") or "NOASSERTION"


def source_from_metadata(metadata: dict[str, Any] | None) -> str:
    if not metadata:
        return ""
    home_page = metadata.get("home_page") or ""
    project_urls = metadata.get("project_url") or []
    preferred_labels = (
        "source",
        "repository",
        "github",
        "homepage",
        "documentation",
        "code",
    )
    parsed: list[tuple[str, str]] = []
    for raw in project_urls:
        label, separator, url = raw.partition(",")
        if separator:
            parsed.append((label.strip().lower(), url.strip()))
    for preferred in preferred_labels:
        for label, url in parsed:
            if preferred in label:
                return url
    return home_page


def component_source(component: dict[str, Any]) -> str:
    for reference in component.get("externalReferences", []):
        if reference.get("type") in {"website", "distribution"} and reference.get("url"):
            return reference["url"]
    return ""


def ecosystem_from_purl(purl: str) -> str:
    if purl.startswith("pkg:pypi/"):
        return "Python"
    if purl.startswith("pkg:npm/"):
        return "JavaScript/CSS"
    if purl.startswith("pkg:generic/python@"):
        return "Runtime"
    return "Other"


def escape_markdown(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")


if __name__ == "__main__":
    main()
