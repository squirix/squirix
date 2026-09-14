#!/usr/bin/env python3
"""Synchronize the checked-in NDepend rules into the NDepend project."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from xml.dom import Node, minidom


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=None)
    args = parser.parse_args()
    script_dir = Path(__file__).resolve().parent
    repo = args.repo.resolve() if args.repo else script_dir.parents[1]
    ndproj_path = repo / "squirix.ndproj"
    ndrules_path = script_dir / "squirix.ndrules"
    ndproj = minidom.parse(str(ndproj_path))
    ndrules = minidom.parse(str(ndrules_path))
    root = ndproj.documentElement
    rules_root = ndrules.documentElement

    def child(parent: Node, name: str) -> Node | None:
        return next((n for n in parent.childNodes if n.nodeType == Node.ELEMENT_NODE and n.tagName == name), None)

    def children(parent: Node, name: str) -> list[Node]:
        return [n for n in parent.childNodes if n.nodeType == Node.ELEMENT_NODE and n.tagName == name]

    runtime = child(root, "RuntimeProfileDesc")
    queries = child(root, "Queries")
    if runtime is None or queries is None:
        raise RuntimeError("Unexpected squirix.ndproj shape: RuntimeProfileDesc or Queries not found.")
    ide = child(root, "IDEFiles")
    if ide is not None:
        root.removeChild(ide)
    ide = ndproj.createElement("IDEFiles")
    file_node = ndproj.createElement("IDEFile")
    file_node.setAttribute("FilePath", ".\\squirix.slnx")
    file_node.setAttribute("Filters", "")
    file_node.setAttribute("Configuration", "DEBUG|AnyCPU")
    info = ndproj.createElement("RootDirResolvingInfo")
    info.setAttribute("Enabled", "False")
    info.setAttribute("Hints", "Debug|bin|.bin|b|AnyCPU|x64|x86|v*.*|net*")
    info.setAttribute("TimeOut", "10")
    root_dir = ndproj.createElement("RootDir")
    root_dir.appendChild(ndproj.createTextNode("."))
    info.appendChild(root_dir)
    file_node.appendChild(info)
    ide.appendChild(file_node)
    root.insertBefore(ide, runtime)

    rule_queries = child(child(rules_root, "Queries"), "CustomJustMyCodeQueries")
    if rule_queries is None:
        raise RuntimeError("Unexpected squirix.ndrules shape: CustomJustMyCodeQueries not found.")
    legacy = next((g for g in children(queries, "Group") if g.getAttribute("Name") == "squirix JustMyCode"), None)
    if legacy is not None:
        queries.removeChild(legacy)
    just = next((g for g in children(queries, "Group") if g.getAttribute("Name") == "Defining JustMyCode"), None)
    if just is None:
        raise RuntimeError("Unexpected squirix.ndproj shape: JustMyCode group not found.")
    for q in children(just, "Query"):
        if "// <Name>" in q.toxml():
            just.removeChild(q)
    for q in children(rule_queries, "Query"):
        just.appendChild(ndproj.importNode(q, deep=True))

    overrides = child(child(rules_root, "Queries"), "CustomRuleOverrides")
    for override in children(overrides, "Query") if overrides is not None else []:
        token, group_name = override.getAttribute("RuleToken"), override.getAttribute("Group")
        if not token or not group_name:
            raise RuntimeError("CustomRuleOverrides Query must specify RuleToken and Group attributes.")
        text = override.toxml()
        if f"// <Id>{token}:" not in text:
            raise RuntimeError(f"Rule override '{token}' must include its explicit stock-rule Id.")
        group = next((g for g in root.getElementsByTagName("Group") if g.getAttribute("Name") == group_name), None)
        if group is None:
            raise RuntimeError(f"NDepend group '{group_name}' not found.")
        name = next((line[9:-7] for line in text.splitlines() if line.startswith("// <Name>") and line.endswith("</Name>")), None)
        candidates = [q for q in group.getElementsByTagName("Query") if f"${token}$" in q.toxml() or f"// {token} squirix override" in q.toxml() or (name and f"// <Name>{name}</Name>" in q.toxml())]
        if not candidates:
            raise RuntimeError(f"Rule override '{token}' target was not found.")
        target = candidates[0]
        for duplicate in candidates[1:]:
            duplicate.parentNode.removeChild(duplicate)
        replacement = ndproj.importNode(override, deep=True)
        replacement.removeAttribute("RuleToken")
        replacement.removeAttribute("Group")
        target.parentNode.replaceChild(replacement, target)
    rule_files = child(root, "RuleFiles")
    if rule_files is not None:
        root.removeChild(rule_files)
    for token in ("ND1500", "ND1501", "ND1502", "ND1503", "ND1504", "ND1505", "ND2201"):
        for q in root.getElementsByTagName("Query"):
            if f"${token}$" in q.toxml() or f"// <Id>{token}:" in q.toxml():
                q.setAttribute("Active", "False")
    rendered = ndproj.toxml(encoding="utf-8").decode("utf-8")
    rendered = "\n".join(line.rstrip() for line in rendered.splitlines()) + "\n"
    ndproj_path.write_text(rendered, encoding="utf-8", newline="\n")
    print(f"Updated {ndproj_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
