import importlib.util
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


SCRIPT = Path(__file__).with_name("sync_rider_file_layout.py")
SPEC = importlib.util.spec_from_file_location("sync_rider_file_layout", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class SyncRiderFileLayoutTests(unittest.TestCase):
    def test_find_global_dotsettings_uses_highest_numeric_rider_version(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory) / "JetBrains"
            for version in ("Rider2026.1", "Rider2026.10", "Rider2026.2"):
                path = root / version / "resharper-host" / "GlobalSettingsStorage.DotSettings"
                path.parent.mkdir(parents=True)
                path.touch()

            with patch.dict(os.environ, {"APPDATA": temporary_directory}):
                self.assertEqual(
                    root / "Rider2026.10" / "resharper-host" / "GlobalSettingsStorage.DotSettings",
                    MODULE.find_global_dotsettings(),
                )

    def test_find_global_dotsettings_ignores_non_versioned_rider_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory) / "JetBrains"
            path = root / "RiderBackup" / "resharper-host" / "GlobalSettingsStorage.DotSettings"
            path.parent.mkdir(parents=True)
            path.touch()

            with patch.dict(os.environ, {"APPDATA": temporary_directory}):
                self.assertIsNone(MODULE.find_global_dotsettings())

    def test_upsert_preserves_utf8_bom(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "squirix.slnx.DotSettings"
            path.write_bytes(b"\xef\xbb\xbf" + MODULE.EMPTY_DICTIONARY.encode("utf-8"))

            with patch.object(MODULE, "SOLUTION_DOTSETTINGS", path):
                MODULE.upsert_dotsettings(path, '<s:String x:Key="test">value</s:String>')

            self.assertTrue(path.read_bytes().startswith(b"\xef\xbb\xbf"))

    def test_upsert_rejects_unrelated_jetbrains_file(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory) / "JetBrains"
            path = root / "unrelated.DotSettings"
            path.parent.mkdir(parents=True)
            with patch.dict(os.environ, {"APPDATA": temporary_directory}):
                with self.assertRaises(ValueError):
                    MODULE.upsert_dotsettings(path, "entry")


if __name__ == "__main__":
    unittest.main()