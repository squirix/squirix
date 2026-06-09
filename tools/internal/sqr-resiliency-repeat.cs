#:property PublishAot=false
#:property NoWarn=SA1649
using System.Diagnostics;

var runs = new[]
{
    new ResiliencyRun("tests/squirix.server/squirix.server.unit-tests/Squirix.Server.UnitTests.csproj", "FullyQualifiedName~Squirix.Server.UnitTests.Cluster.CallPolicyTests", "CallPolicy unit tests"),
    new ResiliencyRun("tests/squirix.server/squirix.server.integration-tests/Squirix.Server.IntegrationTests.csproj", "FullyQualifiedName~Squirix.Server.IntegrationTests.Reliability.TimeoutBehaviorIntegrationTests", "Timeout behavior integration tests"),
    new ResiliencyRun("tests/squirix.server/squirix.server.integration-tests/Squirix.Server.IntegrationTests.csproj", "FullyQualifiedName~Squirix.Server.IntegrationTests.Reliability.DrainAndShutdownIntegrationTests", "Drain and shutdown integration tests"),
    new ResiliencyRun("tests/squirix.server/squirix.server.integration-tests/Squirix.Server.IntegrationTests.csproj", "FullyQualifiedName~Squirix.Server.IntegrationTests.Metrics.CallPolicyContentionMetricsIntegrationTests", "Call-policy metrics integration tests"),
    new ResiliencyRun("tests/squirix.server/squirix.server.integration-tests/Squirix.Server.IntegrationTests.csproj", "FullyQualifiedName~Squirix.Server.IntegrationTests.Reliability.ClientPoolLifecycleIntegrationTests", "Client-pool lifecycle integration tests"),
};

var argv = Environment.GetCommandLineArgs().Skip(1).ToArray();
if (argv.Length is 1 && (string.Equals(argv[0], "--help", StringComparison.OrdinalIgnoreCase)
    || string.Equals(argv[0], "-h", StringComparison.OrdinalIgnoreCase)
    || string.Equals(argv[0], "-?", StringComparison.OrdinalIgnoreCase)))
{
    Console.WriteLine("sqr-resiliency-repeat — run selected resiliency tests repeatedly.");
    Console.WriteLine();
    Console.WriteLine("Usage:");
    Console.WriteLine("  dotnet run --file tools/internal/sqr-resiliency-repeat.cs -- [-Iterations N] [-Configuration Release] [-NoBuild]");
    return 0;
}

var iterations = 5;
var configuration = "Release";
var noBuild = false;
for (var i = 0; i < argv.Length; i++)
{
    var a = argv[i];
    if (string.Equals(a, "-Iterations", StringComparison.OrdinalIgnoreCase)
        || string.Equals(a, "--iterations", StringComparison.OrdinalIgnoreCase))
    {
        if (i + 1 >= argv.Length || !int.TryParse(argv[++i], out iterations) || iterations < 1)
            return Fail("Iterations must be positive.");
        continue;
    }

    if (string.Equals(a, "-Configuration", StringComparison.OrdinalIgnoreCase)
        || string.Equals(a, "--configuration", StringComparison.OrdinalIgnoreCase))
    {
        if (i + 1 >= argv.Length)
            return Fail("missing value for -Configuration");
        configuration = argv[++i];
        continue;
    }

    if (string.Equals(a, "-NoBuild", StringComparison.OrdinalIgnoreCase)
        || string.Equals(a, "--no-build", StringComparison.OrdinalIgnoreCase))
    {
        noBuild = true;
        continue;
    }

    return Fail($"unknown argument '{a}'");
}

var repoRoot = ResolveRepoRoot();
for (var iteration = 1; iteration <= iterations; iteration++)
{
    Console.WriteLine($"Iteration {iteration}/{iterations}");
    foreach (var run in runs)
    {
        Console.WriteLine($"Running {run.Label}");
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

        var code = RunDotnet(repoRoot, list);
        if (code != 0)
            return Fail($"Failed: {run.Label} on iteration {iteration}.", code);
    }
}

Console.WriteLine("All resiliency repeat runs passed.");
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

static int RunDotnet(string repoRoot, IReadOnlyList<string> args)
{
    using var proc = Process.Start(new ProcessStartInfo
    {
        FileName = "dotnet",
        WorkingDirectory = repoRoot,
        UseShellExecute = false,
        Arguments = string.Join(' ', args.Select(QuoteIfNeeded)),
    });
    proc?.WaitForExit();
    return proc?.ExitCode ?? 1;
}

static string QuoteIfNeeded(string value)
{
    return value.Contains(' ') ? $"\"{value}\"" : value;
}

static int Fail(string message, int code = 1)
{
    Console.Error.WriteLine(message);
    return code;
}

internal sealed record ResiliencyRun(string Project, string Filter, string Label);
