#!/usr/bin/env dotnet
#:property PublishAot=false
#:property IsAotCompatible=true
#:property NoWarn=SA1649;S3903
#:property ImplicitUsings=disable
#:include shared/SqrTool.cs
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using static Squirix.Tools.SqrTool;

var runs = new[]
{
    new ResiliencyRun("tests/squirix.server/squirix.server.unit-tests/Squirix.Server.UnitTests.csproj", "/*/*/NodeCallPolicyTests/*", "CallPolicy unit tests"),
    new ResiliencyRun("tests/squirix.server/squirix.server.integration-tests/Squirix.Server.IntegrationTests.csproj", "/*/*/RoutingDeadlineFlowTests/*", "Routing deadline flow integration tests"),
    new ResiliencyRun("tests/squirix.server/squirix.server.integration-tests/Squirix.Server.IntegrationTests.csproj", "/*/*/RpcMutationIdempotencyIntegrationTests/*", "Mutation idempotency integration tests"),
    new ResiliencyRun("tests/squirix.server/squirix.server.integration-tests/Squirix.Server.IntegrationTests.csproj", "/*/*/CrossNodeOpIdIdempotencyTests/*", "Cross-node operation idempotency integration tests"),
    new ResiliencyRun("tests/squirix.server/squirix.server.integration-tests/Squirix.Server.IntegrationTests.csproj", "/*/*/RpcIdempotencyRestartTests/*", "Idempotency restart integration tests"),
    new ResiliencyRun("tests/squirix.e2e.tests/Squirix.E2ETests.csproj", "/*/Squirix.E2ETests.Cache.MultiNode.Failover*/*/*[Suite!=Stress]", "Failover end-to-end tests"),
};

var output = Console.Out;
var argv = Environment.GetCommandLineArgs()[1..];
if (argv.Length == 1 && (string.Equals(argv[0], "--help", StringComparison.OrdinalIgnoreCase)
    || string.Equals(argv[0], "-h", StringComparison.OrdinalIgnoreCase)
    || string.Equals(argv[0], "-?", StringComparison.OrdinalIgnoreCase)))
{
    await output.WriteLineAsync("sqr-resiliency-repeat — run selected resiliency tests repeatedly.", CancellationToken.None).ConfigureAwait(false);
    await output.WriteLineAsync(CancellationToken.None).ConfigureAwait(false);
    await output.WriteLineAsync("Usage:", CancellationToken.None).ConfigureAwait(false);
    await output.WriteLineAsync("  dotnet run --file tools/internal/sqr-resiliency-repeat.cs -- [-Iterations N] [-Configuration Release] [-NoBuild]", CancellationToken.None).ConfigureAwait(false);
    return 0;
}

var iterations = 5;
var configuration = "Release";
var noBuild = false;
var argIndex = 0;
while (argIndex < argv.Length)
{
    var a = argv[argIndex];
    if (string.Equals(a, "-Iterations", StringComparison.OrdinalIgnoreCase)
        || string.Equals(a, "--iterations", StringComparison.OrdinalIgnoreCase))
    {
        if (argIndex + 1 >= argv.Length || !int.TryParse(argv[argIndex + 1], NumberStyles.Integer, CultureInfo.InvariantCulture, out iterations) || iterations < 1)
            return await FailAsync("Iterations must be positive.").ConfigureAwait(false);

        argIndex += 2;
        continue;
    }

    if (string.Equals(a, "-Configuration", StringComparison.OrdinalIgnoreCase)
        || string.Equals(a, "--configuration", StringComparison.OrdinalIgnoreCase))
    {
        if (argIndex + 1 >= argv.Length)
            return await FailAsync("missing value for -Configuration").ConfigureAwait(false);

        configuration = argv[argIndex + 1];
        argIndex += 2;
        continue;
    }

    if (string.Equals(a, "-NoBuild", StringComparison.OrdinalIgnoreCase)
        || string.Equals(a, "--no-build", StringComparison.OrdinalIgnoreCase))
    {
        noBuild = true;
        argIndex++;
        continue;
    }

    return await FailAsync($"unknown argument '{a}'").ConfigureAwait(false);
}

var repoRoot = ResolveRepoRoot();
var dotnetPath = ResolveDotnetPath();
if (dotnetPath == null)
    return await FailAsync("dotnet executable path is unavailable.").ConfigureAwait(false);

for (var iteration = 1; iteration <= iterations; iteration++)
{
    await output.WriteLineAsync(
        $"Iteration {iteration.ToString(CultureInfo.InvariantCulture)}/{iterations.ToString(CultureInfo.InvariantCulture)}", CancellationToken.None).ConfigureAwait(false);
    foreach (var run in runs)
    {
        await output.WriteLineAsync($"Running {run.Label}", CancellationToken.None).ConfigureAwait(false);

        // No --nologo: under Microsoft.Testing.Platform, dotnet test --nologo discovers zero tests (exit code 5).
        var list = new List<string>
        {
            "test",
            "--project",
            run.Project,
            "--configuration",
            configuration,
            "--treenode-filter",
            run.Filter,
        };
        if (noBuild)
            list.Add("--no-build");

        var code = await RunDotnetAsync(dotnetPath, repoRoot, list, CancellationToken.None).ConfigureAwait(false);
        if (code != 0)
        {
            return await FailAsync(
                $"Failed: {run.Label} on iteration {iteration.ToString(CultureInfo.InvariantCulture)}.",
                code).ConfigureAwait(false);
        }
    }
}

await output.WriteLineAsync("All resiliency repeat runs passed.", CancellationToken.None).ConfigureAwait(false);
return 0;

static async Task<int> FailAsync(string message, int code = 1)
{
    await Console.Error.WriteLineAsync(message, CancellationToken.None).ConfigureAwait(false);
    return code;
}

internal sealed record ResiliencyRun(string Project, string Filter, string Label);
