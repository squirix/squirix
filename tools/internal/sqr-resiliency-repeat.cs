#:property PublishAot=false
#:property NoWarn=SA1649;S3903
using System.Diagnostics;

var runs = new[]
{
    new ResiliencyRun("tests/squirix.server/squirix.server.unit-tests/Squirix.Server.UnitTests.csproj", "FullyQualifiedName~Squirix.Server.UnitTests.Cluster.CallPolicyTests", "CallPolicy unit tests"),
    new ResiliencyRun("tests/squirix.server/squirix.server.integration-tests/Squirix.Server.IntegrationTests.csproj", "FullyQualifiedName~Squirix.Server.IntegrationTests.Reliability.TimeoutBehaviorIntegrationTests", "Timeout behavior integration tests"),
    new ResiliencyRun("tests/squirix.server/squirix.server.integration-tests/Squirix.Server.IntegrationTests.csproj", "FullyQualifiedName~Squirix.Server.IntegrationTests.Reliability.DrainAndShutdownIntegrationTests", "Drain and shutdown integration tests"),
    new ResiliencyRun("tests/squirix.server/squirix.server.integration-tests/Squirix.Server.IntegrationTests.csproj", "FullyQualifiedName~Squirix.Server.IntegrationTests.Metrics.CallPolicyContentionMetricsIntegrationTests", "Call-policy metrics integration tests"),
    new ResiliencyRun("tests/squirix.server/squirix.server.integration-tests/Squirix.Server.IntegrationTests.csproj", "FullyQualifiedName~Squirix.Server.IntegrationTests.Reliability.ClientPoolLifecycleIntegrationTests", "Client-pool lifecycle integration tests"),
};

var output = Console.Out;
var argv = Environment.GetCommandLineArgs().Skip(1).ToArray();
if (argv.Length is 1 && (string.Equals(argv[0], "--help", StringComparison.OrdinalIgnoreCase)
    || string.Equals(argv[0], "-h", StringComparison.OrdinalIgnoreCase)
    || string.Equals(argv[0], "-?", StringComparison.OrdinalIgnoreCase)))
{
    await output.WriteLineAsync("sqr-resiliency-repeat — run selected resiliency tests repeatedly.").ConfigureAwait(false);
    await output.WriteLineAsync().ConfigureAwait(false);
    await output.WriteLineAsync("Usage:").ConfigureAwait(false);
    await output.WriteLineAsync("  dotnet run --file tools/internal/sqr-resiliency-repeat.cs -- [-Iterations N] [-Configuration Release] [-NoBuild]").ConfigureAwait(false);
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
        if (argIndex + 1 >= argv.Length || !int.TryParse(argv[argIndex + 1], out iterations) || iterations < 1)
            return await Fail("Iterations must be positive.").ConfigureAwait(false);

        argIndex += 2;
        continue;
    }

    if (string.Equals(a, "-Configuration", StringComparison.OrdinalIgnoreCase)
        || string.Equals(a, "--configuration", StringComparison.OrdinalIgnoreCase))
    {
        if (argIndex + 1 >= argv.Length)
            return await Fail("missing value for -Configuration").ConfigureAwait(false);

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

    return await Fail($"unknown argument '{a}'").ConfigureAwait(false);
}

var repoRoot = ResolveRepoRoot();
for (var iteration = 1; iteration <= iterations; iteration++)
{
    await output.WriteLineAsync($"Iteration {iteration}/{iterations}").ConfigureAwait(false);
    foreach (var run in runs)
    {
        await output.WriteLineAsync($"Running {run.Label}").ConfigureAwait(false);
        var list = new List<string>
        {
            "test",
            run.Project,
            "--configuration",
            configuration,
            "--filter",
            run.Filter,
            "--nologo",
        };
        if (noBuild)
            list.Add("--no-build");

        var code = await RunDotnet(repoRoot, list).ConfigureAwait(false);
        if (code != 0)
            return await Fail($"Failed: {run.Label} on iteration {iteration}.", code).ConfigureAwait(false);
    }
}

await output.WriteLineAsync("All resiliency repeat runs passed.").ConfigureAwait(false);
return 0;

static string ResolveRepoRoot()
{
    var entryDir = AppContext.GetData("EntryPointFileDirectoryPath") as string;
    var startDir = !string.IsNullOrWhiteSpace(entryDir) ? entryDir : Environment.CurrentDirectory;
    var current = new DirectoryInfo(startDir);

    while (current is not null)
    {
        var hasSolution = File.Exists(Path.Combine(current.FullName, "squirix.slnx"));
        var hasCoreProject = File.Exists(Path.Combine(current.FullName, "src", "squirix", "Squirix.csproj"));
        if (hasSolution || hasCoreProject)
            return current.FullName;

        current = current.Parent;
    }

    return Environment.CurrentDirectory;
}

static async Task<int> RunDotnet(string repoRoot, IReadOnlyList<string> args)
{
    using var proc = Process.Start(new ProcessStartInfo
    {
        FileName = "dotnet",
        WorkingDirectory = repoRoot,
        UseShellExecute = false,
        Arguments = string.Join(' ', args.Select(QuoteIfNeeded)),
    });
    if (proc is not null)
        await proc.WaitForExitAsync().ConfigureAwait(false);

    return proc?.ExitCode ?? 1;
}

static string QuoteIfNeeded(string value)
{
    return value.Contains(' ', StringComparison.Ordinal) ? $"\"{value}\"" : value;
}

static async Task<int> Fail(string message, int code = 1)
{
    await Console.Error.WriteLineAsync(message).ConfigureAwait(false);
    return code;
}

internal sealed record ResiliencyRun(string Project, string Filter, string Label);
