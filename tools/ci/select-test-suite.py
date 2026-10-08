#!/usr/bin/env python3
"""Map a CI suite id to the test host runs one job performs.

Prints two outputs on stdout, suitable for appending to $GITHUB_OUTPUT:

- `https_cert=<true|false>`: whether any of the runs needs the ASP.NET Core HTTPS
  development certificate;
- `runs`: one line per test host run, `<name>\\t<csproj>\\t<treenode-filter or empty>`.

A suite id is either a single suite (one run) or a group of suites that one job
runs back to back. Exits 1 on unknown ids.

The mapping lives here (not inline in the composite action) so job logs show
only the selected suite: inline case comments used to echo every suite's
filter into every job log, which once led to a misdiagnosed filter-mismatch report.
"""

import sys

SERVER_UNIT_PROJECT = "tests/squirix.server/squirix.server.unit-tests/Squirix.Server.UnitTests.csproj"

# Suite id -> (project, treenode-filter or "", needs the ASP.NET Core HTTPS development certificate).
# The certificate flag is False only for projects that cannot host a server: the client
# tests reference the client library alone and the protocol-model tests reference the
# model alone, so nothing in them listens on HTTPS. Every suite that references
# Squirix.Server starts real hosts and needs the certificate.
# e2e-single-node excludes the scheduled/manual stress suite and mirrors the prior
# VSTest filter
#   (FullyQualifiedName~Cache.SingleNode|FullyQualifiedName~Client|FullyQualifiedName~Persistence)&Suite!=Stress
# translated to one TUnit treenode-filter. The legacy Client/Persistence substring
# matched 3 MultiNode-owned methods still covered by e2e-multi-node; no legacy
# Persistence-only test exists outside Cache.SingleNode.
# e2e-multi-node is every end-to-end test outside Cache.SingleNode and Cache.MultiNode.Failover
# except the stress suite (Property("Suite","Stress"), run by its own job), selected by exclusion
# so a new class runs without being listed here.
# e2e-failover is the failover end-to-end tests (Cache.MultiNode.Failover) except the stress suite:
# they run whole clusters through elections and fault bounds, so they get a job of their own.
SUITES = {
    "unit": ("tests/squirix/squirix.unit-tests/Squirix.UnitTests.csproj", "", False),
    "client-integration": ("tests/squirix/squirix.integration-tests/Squirix.IntegrationTests.csproj", "", False),
    "server-unit": (SERVER_UNIT_PROJECT, "", True),
    "server-integration": ("tests/squirix.server/squirix.server.integration-tests/Squirix.Server.IntegrationTests.csproj", "", True),
    "smoke": ("tests/squirix.server/squirix.server.smoke-tests/Squirix.Server.SmokeTests.csproj", "", True),
    "e2e-single-node": (
        "tests/squirix.e2e.tests/Squirix.E2ETests.csproj",
        "/*/Squirix.E2ETests.Cache.SingleNode*/*/*",
        True,
    ),
    "e2e-multi-node": (
        "tests/squirix.e2e.tests/Squirix.E2ETests.csproj",
        "/*/(*)&(!Squirix.E2ETests.Cache.SingleNode*)&(!Squirix.E2ETests.Cache.MultiNode.Failover*)/*/*[Suite!=Stress]",
        True,
    ),
    "e2e-failover": (
        "tests/squirix.e2e.tests/Squirix.E2ETests.csproj",
        "/*/Squirix.E2ETests.Cache.MultiNode.Failover*/*/*[Suite!=Stress]",
        True,
    ),
    "protocol-model": ("tests/squirix.protocol-model/squirix.protocol-model.tests/Squirix.ProtocolModel.Tests.csproj", "", False),
}

# Group id -> suites one job runs back to back. Starting a job (runner, checkout, build outputs, certificate) takes about
# as long as a suite runs, and every job holds one of the 20 slots of the organization, so suites share jobs. The groups
# are balanced by run time: none is much longer than the longest suite, the server integration tests.
#  - `light`: the suites that finish in a few seconds each (ubuntu, pull requests, which run the multi-node and the failover
#    end-to-end tests in jobs of their own).
#  - `light-e2e`: `light` plus the multi-node end-to-end tests (ubuntu, outside pull requests).
#  - `server-unit-failover`: the server unit tests and the failover end-to-end tests (ubuntu, outside pull requests), so
#    its three jobs (with the server integration tests) end at about the same time.
#  - `server-unit-e2e`: the server unit tests and the multi-node and failover end-to-end tests (Windows; macOS runs the
#    server unit tests as a separate job).
#  - `e2e-cluster`: the multi-node and the failover end-to-end tests (macOS).
#  - `desktop-integration`: the short suites Windows and macOS run, plus the server integration tests.
#  - `arm-client`, `arm-server`: every suite ARM covers; nothing waits for ARM, so two jobs are enough.
GROUPS = {
    "light": ("unit", "client-integration", "protocol-model", "smoke", "e2e-single-node"),
    "light-e2e": ("unit", "client-integration", "protocol-model", "smoke", "e2e-single-node", "e2e-multi-node"),
    "server-unit-failover": ("server-unit", "e2e-failover"),
    "server-unit-e2e": ("server-unit", "e2e-multi-node", "e2e-failover"),
    "e2e-cluster": ("e2e-multi-node", "e2e-failover"),
    "desktop-integration": ("unit", "client-integration", "e2e-single-node", "server-integration"),
    "arm-client": ("unit", "client-integration", "protocol-model", "e2e-single-node", "e2e-multi-node"),
    "arm-server": ("server-unit", "server-integration"),
}


def main(argv) -> int:
    suite = argv[1] if len(argv) > 1 else ""
    names = GROUPS.get(suite)
    if names is None:
        names = (suite,)
    if any(name not in SUITES for name in names):
        print(f"Unsupported suite: {suite}", file=sys.stderr)
        return 1

    # $GITHUB_OUTPUT keeps the line ending inside a multi-line value, so write LF on every platform.
    sys.stdout.reconfigure(newline="\n")
    https_cert = any(SUITES[name][2] for name in names)
    print(f"https_cert={'true' if https_cert else 'false'}")
    print("runs<<RUNS")
    for name in names:
        project, suite_filter, _ = SUITES[name]
        print(f"{name}\t{project}\t{suite_filter}")
    print("RUNS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
