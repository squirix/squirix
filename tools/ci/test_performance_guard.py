import contextlib
import copy
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


def load_script(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CHECK = load_script("performance_check_tests", "check-performance.py")
RUNNER = load_script("performance_runner_tests", "run-performance-guard.py")


class PerformanceGuardTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.baseline = self.root / "baseline"
        self.current = self.root / "current"
        self.manifest = {
            "baselineCommit": "a" * 40,
            "timeTolerance": 0.2, "allocationTolerance": 0.05,
            "maxRelativeStandardError": 0.05, "minSamples": 10,
            "suites": [{
                "name": "server", "project": "benchmarks/server/Server.csproj",
                "filters": ["*Fixture.*"], "sources": ["Fixture.cs"],
                "cases": [{"name": "Fixture.Read", "checkTime": True},
                          {"name": "Fixture.Empty", "checkTime": False}],
            }],
        }
        host = {field: "same" for field in CHECK.HOST_FIELDS}
        host["LogicalCoreCount"] = 4
        self.report = {
            "HostEnvironmentInfo": host,
            "Benchmarks": [
                self.benchmark("Read", 100, 100),
                self.benchmark("Empty", 0, 0),
            ],
        }
        self.write(self.baseline, self.report)
        self.write(self.current, self.report)
        self.manifest_path = self.root / "manifest.json"
        self.manifest_path.write_text(json.dumps(self.manifest), encoding="utf-8")

    @staticmethod
    def benchmark(method, time, allocated):
        return {
            "Type": "Fixture", "Method": method, "Parameters": "",
            "Statistics": {"N": 10, "OriginalValues": [time] * 10, "Mean": time, "StandardError": 0},
            "Memory": {"BytesAllocatedPerOperation": allocated},
        }

    def write(self, directory, report, filename="report.json"):
        path = directory / "server" / "results" / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(report), encoding="utf-8")
        return path

    def change(self, callback):
        report = copy.deepcopy(self.report)
        callback(report)
        self.write(self.current, report)

    def compare(self):
        return CHECK.compare(self.manifest, self.baseline, self.current)

    def testAcceptsReleaseAndExactBudgetBoundaries(self):
        report = copy.deepcopy(self.report)
        report["Benchmarks"][0] = self.benchmark("Read", 120, 105)
        self.write(self.current, report)
        summary, failures = self.compare()
        self.assertEqual([], failures)
        self.assertIn("+20.0%", summary)
        self.assertIn("| 100 | 105 | ok |", summary)

    def testRejectsTimeJustBeyondBudget(self):
        self.change(lambda report: report["Benchmarks"].__setitem__(0, self.benchmark("Read", 120.01, 100)))
        _, failures = self.compare()
        self.assertEqual(["Fixture.Read: time regression."], failures)

    def testRejectsAllocationsJustBeyondBudget(self):
        self.change(lambda report: report["Benchmarks"][0]["Memory"].update(BytesAllocatedPerOperation=105.01))
        _, failures = self.compare()
        self.assertEqual(["Fixture.Read: allocation regression."], failures)

    def testRejectsAnyAllocationOnZeroAllocationPath(self):
        self.change(lambda report: report["Benchmarks"][1]["Memory"].update(BytesAllocatedPerOperation=1))
        _, failures = self.compare()
        self.assertEqual(["Fixture.Empty: allocation regression."], failures)

    def testTinyResponseStillGuardsAllocationsWithoutTiming(self):
        self.change(lambda report: report["Benchmarks"].__setitem__(1, self.benchmark("Empty", 500, 0)))
        summary, failures = self.compare()
        self.assertEqual([], failures)
        self.assertIn("allocation only", summary)

    def testRejectsMissingAndUnexpectedCases(self):
        for method, expected_message in (("Other", "unexpected"), ("Read", "missing")):
            with self.subTest(case=method):
                report = copy.deepcopy(self.report)
                report["Benchmarks"] = [self.benchmark(method, 100, 100)]
                self.write(self.current, report)
                with self.assertRaisesRegex(ValueError, expected_message):
                    self.compare()

    def testRejectsDuplicateReports(self):
        self.write(self.current, self.report, "duplicate.json")
        with self.assertRaisesRegex(ValueError, "Duplicate result"):
            self.compare()

    def testRejectsSmokeAndInconsistentSampleCounts(self):
        for sample_count, declared in ((1, 1), (10, 9)):
            with self.subTest(case=f"samples{sample_count}declared{declared}"):
                report = copy.deepcopy(self.report)
                report["Benchmarks"][0]["Statistics"].update(OriginalValues=[100] * sample_count, N=declared)
                self.write(self.current, report)
                with self.assertRaisesRegex(ValueError, "insufficient"):
                    self.compare()

    def testRejectsInProcessEvenWithTenSamples(self):
        self.change(lambda report: report["Benchmarks"][0].update(DisplayInfo="Read: Toolchain=InProcessEmitToolchain"))
        with self.assertRaisesRegex(ValueError, "native child processes"):
            self.compare()

    def testRejectsInvalidNumbersAndMissingMeasurements(self):
        for field, invalid in (("Mean", float("nan")), ("Mean", -1), ("StandardError", float("inf")), ("Mean", None)):
            with self.subTest(case=field, partition=str(type(invalid))):
                report = copy.deepcopy(self.report)
                report["Benchmarks"][0]["Statistics"][field] = invalid
                self.write(self.current, report)
                with self.assertRaisesRegex(ValueError, "finite number"):
                    self.compare()
        self.change(lambda report: report["Benchmarks"][0].pop("Memory"))
        with self.assertRaisesRegex(ValueError, "allocated bytes"):
            self.compare()

    def testRejectsNoisySamples(self):
        self.change(lambda report: report["Benchmarks"][0]["Statistics"].update(StandardError=6))
        _, failures = self.compare()
        self.assertEqual(["Fixture.Read: noisy current measurement; rerun."], failures)

    def testRejectsDifferentRuntimeAndProcessor(self):
        for field in ("RuntimeVersion", "ProcessorName"):
            with self.subTest(case=field):
                self.change(lambda report: report["HostEnvironmentInfo"].update({field: "different"}))
                with self.assertRaisesRegex(ValueError, "same host"):
                    self.compare()

    def testRejectsEmptyArtifactDirectory(self):
        with self.assertRaisesRegex(ValueError, "missing"):
            CHECK.read_results(self.root / "absent", self.manifest)

    def testCliFailsOnRegressionAndWritesReport(self):
        self.change(lambda report: report["Benchmarks"].__setitem__(0, self.benchmark("Read", 150, 100)))
        output = self.root / "summary.md"
        with patch.dict(os.environ, {}, clear=True), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            code = CHECK.main([str(self.manifest_path), str(self.baseline), str(self.current), "--report", str(output)])
        self.assertEqual(1, code)
        self.assertIn("time regression", output.read_text(encoding="utf-8"))

    def testManifestRequiresImmutableCommitAndNonemptyCases(self):
        for change in (
            lambda manifest: manifest.update(baselineCommit="v0.1.0-preview.10"),
            lambda manifest: manifest["suites"][0].update(cases=[]),
            lambda manifest: manifest["suites"][0]["cases"].append({"name": "Fixture.Read", "checkTime": True}),
            lambda manifest: manifest.update(minSamples=1),
            lambda manifest: manifest.update(timeTolerance=float("nan")),
        ):
            manifest = copy.deepcopy(self.manifest)
            change(manifest)
            self.manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaises(ValueError):
                CHECK.load_manifest(self.manifest_path)

    def testRunnerUsesNativeProcessesAndTenMeasurements(self):
        filters = ["*Fixture.Read", "*Fixture.Empty"]
        arguments = RUNNER.benchmark_arguments(Path("Server.dll"), filters, Path("reports"))
        self.assertEqual(["dotnet", "Server.dll", "--filter", *filters], arguments[:5])
        self.assertNotIn("--inProcess", arguments)
        self.assertEqual("10", arguments[arguments.index("--iterationCount") + 1])
        self.assertEqual("10", arguments[arguments.index("--warmupCount") + 1])
        self.assertEqual("500", arguments[arguments.index("--iterationTime") + 1])
        self.assertEqual("DontRemove", arguments[arguments.index("--outliers") + 1])

    def testRunnerRemovesSmokeAndArtifactOverrides(self):
        overrides = {"SQUIRIX_BENCHMARK_SMOKE": "1", "SQUIRIX_E2E_BENCHMARK_IN_PROCESS": "1", "BDN_ARTIFACTS": "old", "PATH": "preserved"}
        with patch.dict(os.environ, overrides, clear=True):
            self.assertEqual({"PATH": "preserved"}, RUNNER.benchmark_environment())

    def testRunnerRejectsEscapingFixturePath(self):
        with self.assertRaisesRegex(ValueError, "escapes"):
            RUNNER.repo_path(self.root, "../outside.cs")
        self.assertEqual(self.root / "Fixture.cs", RUNNER.repo_path(self.root, "Fixture.cs"))

    def testRunnerRejectsStaleResultsBeforeExecutingCommands(self):
        artifacts = self.root / "artifacts"
        (artifacts / "reports").mkdir(parents=True)
        with patch.object(RUNNER, "execute") as execute:
            with self.assertRaisesRegex(ValueError, "fresh artifact"):
                RUNNER.run(self.manifest_path, artifacts)
            execute.assert_not_called()


if __name__ == "__main__":
    unittest.main()
