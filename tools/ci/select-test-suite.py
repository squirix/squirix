#!/usr/bin/env python3
"""Map a CI suite id to its dotnet test project and optional --treenode-filter.

Prints `project=<csproj>`, `filter=<treenode-filter or empty>` and
`https_cert=<true|false>` on stdout, suitable for appending to $GITHUB_OUTPUT.
Exits 1 on unknown suites.

The mapping lives here (not inline in the composite action) so job logs show
only the selected suite: inline case comments used to echo every suite's
filter into every job log, which once led to a misdiagnosed filter-mismatch report.
"""

import sys

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
    "server-unit": ("tests/squirix.server/squirix.server.unit-tests/Squirix.Server.UnitTests.csproj", "", True),
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


def main(argv) -> int:
    suite = argv[1] if len(argv) > 1 else ""
    mapping = SUITES.get(suite)
    if mapping is None:
        print(f"Unsupported suite: {suite}", file=sys.stderr)
        return 1
    project, suite_filter, https_cert = mapping
    print(f"project={project}")
    print(f"filter={suite_filter}")
    print(f"https_cert={'true' if https_cert else 'false'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
