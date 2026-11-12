"""Regression checks for bounded Docker commands and cleanup after timeouts."""

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import failover


class CommandTimeoutTests(unittest.TestCase):
    def testCommandTimeoutTerminatesWait(self):
        started = time.monotonic()
        with self.assertRaisesRegex(TimeoutError, "0.1s limit") as raised:
            failover.run([sys.executable, "-c", "import time; time.sleep(30)"], timeout=0.1)
        self.assertIsInstance(raised.exception.__cause__, subprocess.TimeoutExpired)
        self.assertLess(time.monotonic() - started, 5)

    def checkCleanupAfterBuildTimeout(self, logTimeout):
        config = {"services": {"node-" + node: {"networks": {"squirix-net": {}}} for node in failover.NODES}}
        buildError = TimeoutError("Build stalled")

        def command(arguments, capture=False, timeout=failover.COMMAND_TIMEOUT):
            if "config" in arguments:
                return json.dumps(config)
            if "build" in arguments:
                self.assertEqual(timeout, failover.BUILD_TIMEOUT)
                raise buildError
            if "down" in arguments:
                self.assertEqual(timeout, failover.CLEANUP_TIMEOUT)
                return ""
            self.fail("Unexpected command: " + repr(arguments))

        with tempfile.TemporaryDirectory() as directory:
            logResult = subprocess.CompletedProcess([], 0, stdout="server logs")
            logEffect = subprocess.TimeoutExpired("logs", failover.LOG_TIMEOUT) if logTimeout else None
            with patch.object(failover, "OUTPUT", Path(directory)), \
                    patch.object(failover, "run", side_effect=command) as commands, \
                    patch.object(failover.subprocess, "run", side_effect=logEffect, return_value=logResult) as logs:
                with self.assertRaises(TimeoutError) as raised:
                    failover.main()
                self.assertIs(raised.exception, buildError)
                self.assertEqual(commands.call_args_list[-1].args[0],
                                 failover.COMPOSE + ["down", "--volumes", "--remove-orphans"])
                self.assertEqual(logs.call_args.kwargs["timeout"], failover.LOG_TIMEOUT)
                self.assertEqual(sum("down" in call.args[0] for call in commands.call_args_list), 1)
                saved = (Path(directory) / "server.log").read_text(encoding="utf-8")
                self.assertIn("proceeding to cleanup" if logTimeout else "server logs", saved)

    def testBuildTimeoutStillCleansUp(self):
        self.checkCleanupAfterBuildTimeout(logTimeout=False)

    def testLogTimeoutStillCleansUp(self):
        self.checkCleanupAfterBuildTimeout(logTimeout=True)


if __name__ == "__main__":
    unittest.main()
