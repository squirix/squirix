"""Sync build/rider/csharp-file-layout.xml into Rider DotSettings layers."""

from __future__ import annotations

import html
import os
import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
LAYOUT = REPO / "build" / "rider" / "csharp-file-layout.xml"
SOLUTION_DOTSETTINGS = REPO / "squirix.slnx.DotSettings"
KEY = "/Default/CodeStyle/CSharpFileLayoutPatterns/Pattern/@EntryValue"
ENTRY_RE = re.compile(
    rf'<s:String x:Key="{re.escape(KEY)}">.*?</s:String>',
    re.S,
)
EMPTY_DICTIONARY = (
    '<wpf:ResourceDictionary xmlns:x="http://schemas.microsoft.com/winfx/2006/xaml" '
    'xmlns:s="clr-namespace:System;assembly=mscorlib" '
    'xmlns:wpf="http://schemas.microsoft.com/winfx/2006/xaml/presentation" '
    'xml:space="preserve">\n</wpf:ResourceDictionary>\n'
)


def encode_pattern(xml: str) -> str:
    encoded = html.escape(xml.strip() + "\n", quote=False)
    return f'<s:String x:Key="{KEY}">{encoded}</s:String>'


def _jetbrains_roaming_root() -> Path | None:
    appdata = os.environ.get("APPDATA")
    if not appdata:
        return None
    root = Path(appdata) / "JetBrains"
    try:
        return root.resolve(strict=False)
    except OSError:
        return None


def find_global_dotsettings() -> Path | None:
    """Locate Rider GlobalSettingsStorage.DotSettings under %APPDATA%\\JetBrains only."""
    root = _jetbrains_roaming_root()
    if root is None or not root.is_dir():
        return None

    matches: list[Path] = []
    for path in root.glob("Rider*/resharper-host/GlobalSettingsStorage.DotSettings"):
        try:
            resolved = path.resolve(strict=False)
        except OSError:
            continue
        if resolved.is_relative_to(root):
            matches.append(resolved)

    if not matches:
        return None

    matches.sort(key=lambda p: p.as_posix(), reverse=True)
    return matches[0]


def _assert_allowed_write_target(path: Path) -> Path:
    """Reject writes outside the repo DotSettings file or JetBrains roaming settings."""
    resolved = path.resolve(strict=False)
    allowed = {SOLUTION_DOTSETTINGS.resolve(strict=False)}
    jetbrains = _jetbrains_roaming_root()
    if jetbrains is not None and resolved.is_relative_to(jetbrains):
        return resolved
    if resolved in allowed:
        return resolved
    raise ValueError(f"refusing to write unexpected DotSettings path: {resolved}")


def upsert_dotsettings(path: Path, entry: str) -> None:
    target = _assert_allowed_write_target(path)
    text = target.read_text(encoding="utf-8") if target.exists() else EMPTY_DICTIONARY
    if ENTRY_RE.search(text):
        text = ENTRY_RE.sub(entry, text, count=1)
    else:
        text = text.replace("</wpf:ResourceDictionary>", f"\t{entry}\n</wpf:ResourceDictionary>")
    target.write_text(text, encoding="utf-8")
    print(f"updated {target}")


def main() -> None:
    xml = LAYOUT.read_text(encoding="utf-8")
    entry = encode_pattern(xml)
    upsert_dotsettings(SOLUTION_DOTSETTINGS, entry)

    global_settings = find_global_dotsettings()
    if global_settings is None:
        print("skip missing Rider GlobalSettingsStorage.DotSettings")
        return

    upsert_dotsettings(global_settings, entry)


if __name__ == "__main__":
    main()
