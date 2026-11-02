"""Deterministic controls for the process nemesis schedule and network topology."""

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from run import Cluster, CommandFailed, FAULTS, command, parse_leader, schedule


class ScheduleTests(unittest.TestCase):
    def testCutResolvesAddressAgainAfterRestart(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = Cluster(SimpleNamespace(output=Path(directory) / "run", replicas=3, seed=1))
            cluster.ips = ["10.0.0.1", "10.0.0.2", "10.0.0.3"]
            with patch("run.command", side_effect=["10.0.0.2\n", "10.0.0.9\n"]) as inspect, \
                    patch.object(cluster, "execute") as execute:
                cluster.cut(0, 1)
                cluster.cut(0, 1)
            self.assertEqual(inspect.call_count, 2)
            self.assertTrue(all(call.args[-1] == cluster.nodes[1] for call in inspect.call_args_list))
            self.assertEqual([call.args for call in execute.call_args_list], [
                (0, "iptables", "-A", "OUTPUT", "-d", address, "-p", "tcp", "--dport", "5001", "-j", "DROP")
                for address in ("10.0.0.2", "10.0.0.9")])

    def testLocalLinuxStorageReachesClusterSetup(self):
        for filesystem in ("ext2/ext3", "xfs", "overlayfs"):
            with self.subTest(filesystem=filesystem), tempfile.TemporaryDirectory() as directory:
                cluster = Cluster(SimpleNamespace(output=Path(directory) / "run", replicas=5, seed=1478, image="test"))
                with patch("run.command", side_effect=[filesystem, "image-id"]), patch.object(cluster, "record"), \
                        patch.object(cluster, "certificates", side_effect=RuntimeError("setup reached")) as certificates, \
                        self.assertRaisesRegex(RuntimeError, "setup reached"):
                    cluster.start()
                certificates.assert_called_once_with()

    def testHostSharedStorageIsRejectedBeforeAllocatingCluster(self):
        for filesystem in ("9p", "drvfs", "cifs", "smb2", "virtiofs"):
            with self.subTest(filesystem=filesystem), tempfile.TemporaryDirectory() as directory:
                cluster = Cluster(SimpleNamespace(output=Path(directory) / "run", replicas=5, seed=1478))
                with patch("run.command", return_value=filesystem) as execute, \
                        self.assertRaisesRegex(RuntimeError, "requires local Linux storage"):
                    cluster.start()
                execute.assert_called_once_with("stat", "-f", "-c", "%T", cluster.root)
                self.assertEqual(cluster.created, [])
                self.assertEqual(cluster.mounted, [])

    def testRecoveryRequiresEveryEndpoint(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = Cluster(SimpleNamespace(output=Path(directory) / "run", replicas=3, seed=1))
            def poll(predicate, seconds, description):
                self.assertEqual(seconds, 90)
                self.assertFalse(predicate())
                self.assertTrue(predicate())
            with patch("run.wait_until", side_effect=poll), patch.object(cluster, "client_alive"), \
                    patch.object(cluster, "http", side_effect=["Healthy", RuntimeError("starting"), "Healthy", "Healthy"]) as http, \
                    patch.object(cluster, "record") as record:
                cluster.await_recovery()
            self.assertEqual([call.args[0] for call in http.call_args_list], [0, 1, 2, 1])
            record.assert_called_once_with("endpoints-recovered")

    def testUnavailableFollowerDoesNotHideLeader(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = Cluster(SimpleNamespace(output=Path(directory) / "run", replicas=3, seed=1))
            metrics = ('squirix_replication_leader_authority{group="n0"} 1\n'
                       'squirix_replication_term{group="n0"} 7\n')
            with patch.object(cluster, "http", side_effect=[RuntimeError("recovering"), "Healthy", metrics]), \
                    patch.object(cluster, "record") as record:
                self.assertEqual(cluster.leader(excluded=1), {"node": 2, "term": 7, "group": "n0"})
            record.assert_called_once_with("diagnostic-unavailable", node=0, message="recovering")

    def testAbortLetsClientFlushBeforeCleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = Cluster(SimpleNamespace(output=Path(directory) / "run", replicas=3, seed=1))
            (cluster.root / "control").mkdir()
            cluster.created.append(cluster.client)
            def poll(predicate, seconds, description):
                self.assertEqual((cluster.root / "control/abort").read_text(), "abort")
                self.assertFalse(predicate())
                self.assertTrue(predicate())
            with patch("run.wait_until", side_effect=poll), patch("run.command", side_effect=["true", "false"]):
                cluster.abort_client()

    def testLeaderScrapeRefreshesReadiness(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = Cluster(SimpleNamespace(output=Path(directory) / "run", replicas=3, seed=1))
            metrics = ('squirix_replication_leader_authority{group="n0"} 1\n'
                       'squirix_replication_term{group="n0"} 7\n')
            with patch.object(cluster, "http", side_effect=["Unhealthy", metrics, "Healthy", ""]) as http:
                self.assertEqual(cluster.leader(excluded=1), {"node": 0, "term": 7, "group": "n0"})
            self.assertEqual([call.args for call in http.call_args_list], [
                (0, "/health/ready", (200, 503)), (0, "/metrics", (200,)),
                (2, "/health/ready", (200, 503)), (2, "/metrics", (200,))])

    def testDiagnosticFailureCannotBecomeObservation(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster = Cluster(SimpleNamespace(output=Path(directory) / "run", replicas=3, seed=1))
            for response in ("certificate verify failed", "HTTP/1.1 401 Unauthorized\r\n", ""):
                with patch("run.command", return_value=response), self.assertRaises(RuntimeError):
                    cluster.http(0, "/metrics", (200,))

    def testPauseTargetsObservedLeaderWithoutChangingPlan(self):
        with tempfile.TemporaryDirectory() as directory:
            args = SimpleNamespace(output=Path(directory) / "run", replicas=3, seed=1)
            cluster = Cluster(args)
            planned = {"kind": "pause", "nodes": [0, 2, 1]}
            with patch.object(cluster, "leader", return_value={"node": 1, "term": 5, "group": "n0"}):
                actual = cluster.target_leader(planned)
            self.assertEqual(actual["nodes"], [1, 0, 2])
            self.assertEqual(planned["nodes"], [0, 2, 1])
            self.assertEqual(actual["leader_before"]["term"], 5)

    def testReplacementMustAdvanceTermBeforeResume(self):
        with tempfile.TemporaryDirectory() as directory:
            args = SimpleNamespace(output=Path(directory) / "run", replicas=3, seed=1)
            cluster = Cluster(args)
            before = {"node": 1, "term": 5, "group": "n0"}
            after = {"node": 2, "term": 6, "group": "n0"}
            def poll(predicate, seconds, description):
                self.assertFalse(predicate())
                self.assertTrue(predicate())
            with patch.object(cluster, "leader", side_effect=[{**after, "term": 5}, after]) as leader, \
                    patch("run.wait_until", side_effect=poll), patch.object(cluster, "record") as record:
                cluster.await_replacement({"kind": "pause", "nodes": [1, 0, 2], "leader_before": before})
            self.assertEqual([call.kwargs for call in leader.call_args_list], [{"excluded": 1}, {"excluded": 1}])
            record.assert_called_once_with("leader-replaced-before-resume", before=before, after=after)

    def testLeaderObservationRequiresAuthorityAndTerm(self):
        metrics = ('squirix_replication_leader_authority{node="n2",group="n0"} 1\n'
                   'squirix_replication_term{group="n0",node="n2"} 7\n')
        self.assertEqual(parse_leader(metrics, "n0"), 7)
        self.assertIsNone(parse_leader(metrics, "n1"))
        self.assertIsNone(parse_leader(metrics.replace('} 1', '} 0'), "n0"))
        self.assertIsNone(parse_leader(metrics.splitlines()[0], "n0"))

    def testFailureDoesNotExposeSecretArguments(self):
        with patch("run.subprocess.run", return_value=SimpleNamespace(returncode=125, stdout="cannot start")):
            with self.assertRaises(CommandFailed) as caught:
                command("docker", "run", "-e", "SIGNING_KEY=private-value")
        self.assertEqual(str(caught.exception), "docker failed with exit code 125: cannot start")
        self.assertNotIn("private-value", str(caught.exception))

    def testSeedReplaysEveryFaultAndVictim(self):
        for replicas in (3, 5):
            first = schedule(1478, replicas)
            self.assertEqual(first, schedule(1478, replicas))
            self.assertNotEqual(first, schedule(1479, replicas))
            self.assertCountEqual([step["kind"] for step in first], FAULTS)
            for step in first:
                self.assertCountEqual(step["nodes"], range(replicas))

    def testPartitionCutsBothDirectionsAndBridgeRemains(self):
        for replicas in (3, 5):
            for kind in ("partition", "bridge", "one-way"):
                with tempfile.TemporaryDirectory() as directory:
                    args = SimpleNamespace(output=Path(directory) / "run", replicas=replicas, seed=1)
                    cluster = Cluster(args)
                    with patch.object(cluster, "cut") as cut, patch.object(cluster, "record"):
                        cluster.fault({"kind": kind, "nodes": list(range(replicas))})
                    minority = set(range(replicas // 2))
                    majority = set(range(replicas // 2, replicas))
                    if kind == "bridge":
                        majority.remove(replicas // 2)
                    expected = {(a, b) for a in minority for b in majority}
                    if kind != "one-way":
                        expected |= {(b, a) for a in minority for b in majority}
                    self.assertEqual({call.args for call in cut.call_args_list}, expected)
                    self.assertEqual(cut.call_count, len(expected))


if __name__ == "__main__":
    unittest.main()
