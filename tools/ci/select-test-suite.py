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
# e2e-multi-node excludes stress and mirrors
#   (FullyQualifiedName~Cache.MultiNode|FullyQualifiedName~Security)&Suite!=Stress
# plus the HA release classes, as one treenode-filter with class-name alternation.
# The stress class (MixedMutationStressTests, Property("Suite","Stress")) is absent
# from the alternation. HA release classes (M8 replica sets) stay visible here.
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
        "/*/*/(CrossNodeCrudTests)|(CrossNodeExpirationTests)|(CrossNodeTypedValueTests)|(InterNodeMtlsTests)|(ReplicaSetsReleaseE2ETests)|(FailoverE2ETests)|(ClusterPackageVersionE2ETests)|(TopologyActivationE2ETests)/*",
        True,
    ),
    "protocol-model": ("tests/squirix.protocol-model/squirix.protocol-model.tests/Squirix.ProtocolModel.Tests.csproj", "", False),
}

# The server unit tests are the longest suite, so jobs may run them as two parallel halves.
# `server-unit-a` takes the namespaces listed here (about half of the run time) and
# `server-unit-b` takes every other namespace. Both filters are derived from this one list,
# so together they always cover the whole project: a namespace that is added or renamed
# lands in `server-unit-b` on its own.
SERVER_UNIT_FIRST_HALF = ("Squirix.Server.UnitTests.Node*", "Squirix.Server.UnitTests.Adapters*")

SUITES["server-unit-a"] = (
    SERVER_UNIT_PROJECT,
    "/*/" + "|".join(f"({namespace})" for namespace in SERVER_UNIT_FIRST_HALF) + "/*/*",
    True,
)
SUITES["server-unit-b"] = (
    SERVER_UNIT_PROJECT,
    "/*/" + "&".join(f"(!{namespace})" for namespace in SERVER_UNIT_FIRST_HALF) + "/*/*",
    True,
)

# Group id -> suites one job runs back to back. `light` holds the suites that finish in a
# few seconds each, where a runner per suite would mostly pay for its own start-up.
GROUPS = {
    "light": ("unit", "client-integration", "protocol-model", "smoke", "e2e-single-node"),
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
