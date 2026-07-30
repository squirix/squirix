#!/usr/bin/env python3
"""Self-test: squirix ND1412 DAG override accepts allowed edges and rejects forbidden ones."""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path


def run(cmd: list[str], *, cwd: Path | None = None) -> None:
    print("+", " ".join(cmd), flush=True)
    completed = subprocess.run(cmd, cwd=str(cwd) if cwd else None, check=False)
    if completed.returncode != 0:
        raise RuntimeError(f"Command failed ({completed.returncode}): {' '.join(cmd)}")


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8", newline="\n")


def create_classlib(root: Path, name: str, namespace: str, refs: list[Path] | None = None) -> Path:
    proj_dir = root / name
    if proj_dir.exists():
        shutil.rmtree(proj_dir)
    run(["dotnet", "new", "classlib", "-n", name, "-o", str(proj_dir), "-f", "net10.0", "--force"])
    csproj = proj_dir / f"{name}.csproj"
    # Disable implicit usings / nullable noise for tiny fixtures.
    text = csproj.read_text(encoding="utf-8")
    text = text.replace(
        "</PropertyGroup>",
        "  <ImplicitUsings>disable</ImplicitUsings>\n"
        "    <Nullable>enable</Nullable>\n"
        "    <GenerateDocumentationFile>false</GenerateDocumentationFile>\n"
        "    <TreatWarningsAsErrors>false</TreatWarningsAsErrors>\n"
        "    <RunAnalyzersDuringBuild>false</RunAnalyzersDuringBuild>\n"
        "    <EnableNETAnalyzers>false</EnableNETAnalyzers>\n"
        "  </PropertyGroup>",
        1,
    )
    if refs:
        items = "\n".join(
            f'    <ProjectReference Include="{ref.as_posix()}" />' for ref in refs
        )
        text = text.replace("</Project>", f"  <ItemGroup>\n{items}\n  </ItemGroup>\n</Project>")
    csproj.write_text(text, encoding="utf-8", newline="\n")
    # Replace Class1.cs
    for old in proj_dir.glob("*.cs"):
        old.unlink()
    write_text(
        proj_dir / "Type1.cs",
        f"namespace {namespace};\n\npublic static class Type1\n{{\n    public static int Value => 1;\n}}\n",
    )
    return csproj


def build_project(csproj: Path) -> Path:
    run(["dotnet", "build", str(csproj), "-c", "Release", "--verbosity", "quiet"])
    dlls = list(csproj.parent.glob("bin/Release/net10.0/*.dll"))
    dlls = [d for d in dlls if d.name == csproj.stem + ".dll"]
    if not dlls:
        raise RuntimeError(f"No output dll for {csproj}")
    return dlls[0]


def extract_nd1412_cdata(ndrules: Path) -> str:
    text = ndrules.read_text(encoding="utf-8")
    match = re.search(
        r'RuleToken="ND1412"[^>]*>\s*<!\[CDATA\[(.*?)\]\]>\s*</Query>',
        text,
        flags=re.DOTALL,
    )
    if not match:
        raise RuntimeError("ND1412 CDATA not found in squirix.ndrules")
    return match.group(1)


def resolve_console_exe(console: Path) -> Path:
    if console.suffix.lower() == ".exe":
        cmd_wrapper = console.with_suffix(".cmd")
        if cmd_wrapper.is_file():
            return cmd_wrapper
    return console


def create_ndproj(console: Path, path: Path, dlls: list[Path], rule_body: str) -> None:
    """Create a valid .ndproj via NDepend.Console, then inject the ND1412 rule body."""
    if path.exists():
        path.unlink()
    exe = resolve_console_exe(console)
    cmd = [str(exe), "/CreateProject", str(path), *[str(d.resolve()) for d in dlls]]
    run(cmd)
    text = path.read_text(encoding="utf-8")
    queries = f"""  <Queries>
    <Group Name="Architecture" Active="True" ShownInReport="True">
      <Query Active="True" DisplayList="True" DisplayStat="False" DisplaySelectionView="False" IsCriticalRule="True" RuleToken="ND1412"><![CDATA[{rule_body}]]></Query>
    </Group>
  </Queries>
"""
    if "<Queries>" in text:
        text = re.sub(r"<Queries>.*?</Queries>", queries.strip(), text, count=1, flags=re.DOTALL)
    else:
        text = text.replace("</NDepend>", queries + "</NDepend>")
    write_text(path, text)


def run_ndepend(console: Path, ndproj: Path, out_dir: Path) -> Path:
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    # Prefer the .cmd wrapper so ILRewriter license hooks are applied.
    exe = resolve_console_exe(console)
    cmd = [
        str(exe),
        str(ndproj),
        "/Silent",
        "/OutDir",
        str(out_dir),
        "/KeepXmlFilesUsedToBuildReport",
        "/ForceReturnZeroExitCode",
    ]
    run(cmd)
    results = list(out_dir.glob("XmlFilesUsedToBuildReport/CodeRuleResult.xml"))
    if not results:
        results = list(out_dir.rglob("CodeRuleResult.xml"))
    if not results:
        raise RuntimeError(f"CodeRuleResult.xml missing under {out_dir}")
    return results[0]


def matched_count(code_rule_result: Path) -> int:
    root = ET.parse(code_rule_result).getroot()
    total = 0
    for query in root.iter("Query"):
        name = query.get("Name") or ""
        if "Enforcing Clean Architecture" in name or query.get("RuleId") == "ND1412":
            total += int(query.get("NbNodeMatched") or "0")
    # Fixture projects have a single query; if Name didn't round-trip, sum all matches.
    if total == 0:
        for query in root.iter("Query"):
            total += int(query.get("NbNodeMatched") or "0")
    return total


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ndepend-console", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--repo", type=Path, default=None)
    args = parser.parse_args()

    repo = args.repo.resolve() if args.repo else Path(__file__).resolve().parents[2]
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=True)
    # Keep fixture projects outside the repo so Directory.Build.props analyzers do not apply.
    fixtures = Path(tempfile.mkdtemp(prefix="squirix-nd1412-"))
    evidence_fixtures_link = out / "fixtures-path.txt"
    evidence_fixtures_link.write_text(str(fixtures) + "\n", encoding="utf-8")
    print(f"fixtures={fixtures}", flush=True)

    rule_body = extract_nd1412_cdata(repo / "build" / "ndepend" / "squirix.ndrules")
    console = args.ndepend_console.resolve()

    evidence: dict = {"allowed": {}, "forbidden": {}}

    # Allowed: Cluster.Replication -> Storage.Replication
    allowed_root = fixtures / "allowed"
    storage_csproj = create_classlib(
        allowed_root, "StorageReplication", "Squirix.Server.Storage.Replication"
    )
    cluster_csproj = create_classlib(
        allowed_root,
        "ClusterReplication",
        "Squirix.Server.Cluster.Replication",
        refs=[storage_csproj],
    )
    # Reference usage so NDepend sees the namespace dependency.
    write_text(
        cluster_csproj.parent / "UsesStorage.cs",
        "namespace Squirix.Server.Cluster.Replication;\n\n"
        "public static class UsesStorage\n{\n"
        "    public static int Create() => Squirix.Server.Storage.Replication.Type1.Value;\n"
        "}\n",
    )
    storage_dll = build_project(storage_csproj)
    cluster_dll = build_project(cluster_csproj)
    allowed_proj = allowed_root / "allowed.ndproj"
    create_ndproj(console, allowed_proj, [cluster_dll, storage_dll], rule_body)
    allowed_result = run_ndepend(console, allowed_proj, allowed_root / "ndout")
    allowed_matches = matched_count(allowed_result)
    evidence["allowed"] = {"matches": allowed_matches, "result": str(allowed_result)}
    if allowed_matches != 0:
        raise RuntimeError(f"Allowed DAG edge unexpectedly matched {allowed_matches} issue(s).")

    # Forbidden: Storage.Replication -> Cluster.Replication
    forbidden_root = fixtures / "forbidden"
    cluster2 = create_classlib(
        forbidden_root, "ClusterReplication2", "Squirix.Server.Cluster.Replication"
    )
    storage2 = create_classlib(
        forbidden_root,
        "StorageReplication2",
        "Squirix.Server.Storage.Replication",
        refs=[cluster2],
    )
    write_text(
        storage2.parent / "UsesCluster.cs",
        "namespace Squirix.Server.Storage.Replication;\n\n"
        "public static class UsesCluster\n{\n"
        "    public static int Create() => Squirix.Server.Cluster.Replication.Type1.Value;\n"
        "}\n",
    )
    cluster2_dll = build_project(cluster2)
    storage2_dll = build_project(storage2)
    forbidden_proj = forbidden_root / "forbidden.ndproj"
    create_ndproj(console, forbidden_proj, [storage2_dll, cluster2_dll], rule_body)
    forbidden_result = run_ndepend(console, forbidden_proj, forbidden_root / "ndout")
    forbidden_matches = matched_count(forbidden_result)
    evidence["forbidden"] = {"matches": forbidden_matches, "result": str(forbidden_result)}
    if forbidden_matches < 1:
        raise RuntimeError("Forbidden reverse edge produced no ND1412 matches.")

    # Forbidden: Client -> Server
    client_root = fixtures / "client_server"
    server = create_classlib(client_root, "ServerCore", "Squirix.Server.Core")
    client = create_classlib(
        client_root, "ClientLib", "Squirix.Client", refs=[server]
    )
    write_text(
        client.parent / "UsesServer.cs",
        "namespace Squirix.Client;\n\n"
        "public static class UsesServer\n{\n"
        "    public static int Create() => Squirix.Server.Core.Type1.Value;\n"
        "}\n",
    )
    server_dll = build_project(server)
    client_dll = build_project(client)
    client_proj = client_root / "client.ndproj"
    create_ndproj(console, client_proj, [client_dll, server_dll], rule_body)
    client_result = run_ndepend(console, client_proj, client_root / "ndout")
    client_matches = matched_count(client_result)
    evidence["client_server"] = {"matches": client_matches, "result": str(client_result)}
    if client_matches < 1:
        raise RuntimeError("Client->Server forbidden edge produced no ND1412 matches.")

    evidence_path = out / "evidence.json"
    evidence_path.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(evidence, indent=2))
    print(f"Wrote {evidence_path}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001
        print(exc, file=sys.stderr)
        raise SystemExit(1)
