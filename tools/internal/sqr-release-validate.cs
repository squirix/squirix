#:property PublishAot=false
#:property NoWarn=SA1649

// The file-based release app keeps its options DTO inline so the validator remains directly runnable.
using System.Diagnostics;
using System.IO.Compression;
using System.Xml.Linq;

var requiredDocs = new[]
{
    "README.md",
    "docs/architecture.md",
    "docs/server-mode.md",
    "docs/operational-runbook.md",
};

var packageProjects = new[]
{
    "src/squirix/Squirix.csproj",
    "src/squirix.server/Squirix.Server.csproj",
};

var output = Console.Out;
var argv = Environment.GetCommandLineArgs().Skip(1).ToArray();
if (argv.Length is 1 && (string.Equals(argv[0], "--help", StringComparison.OrdinalIgnoreCase)
    || string.Equals(argv[0], "-h", StringComparison.OrdinalIgnoreCase)
    || string.Equals(argv[0], "-?", StringComparison.OrdinalIgnoreCase)))
{
    output.WriteLine("sqr-release-validate — validate release readiness and package artifacts.");
    output.WriteLine();
    output.WriteLine("Usage:");
    output.WriteLine("  dotnet run --file tools/internal/sqr-release-validate.cs -- [-SkipTests] [-SkipFormat] [-IncludeIntegrationTests] [-IncludePropertyTests] [-IncludeStressChecks] [-IncludeBenchmarks] [-Configuration Release] [-ArtifactsDirectory artifacts/release-validation] [-PackageVersion <ver>]");
    return 0;
}

var options = ParseOptions(argv);
if (!options.IsValid)
    return 1;

var repoRoot = ResolveRepoRoot();
var repoRootResolved = Path.GetFullPath(repoRoot);
var artifactsPath = Path.GetFullPath(Path.Combine(repoRootResolved, options.ArtifactsDirectory));
var packageOutputPath = Path.Combine(artifactsPath, "packages");
var smokePackageOutputPath = Path.Combine(repoRootResolved, "artifacts", "packages");

try
{
    Step("Prepare artifacts directory");
    if (!artifactsPath.StartsWith(repoRootResolved, StringComparison.OrdinalIgnoreCase) || string.Equals(artifactsPath, repoRootResolved, StringComparison.OrdinalIgnoreCase))
        throw new InvalidOperationException($"ArtifactsDirectory must resolve to a child directory inside the repository: {options.ArtifactsDirectory}");

    if (Directory.Exists(artifactsPath))
        Directory.Delete(artifactsPath, true);
    _ = Directory.CreateDirectory(packageOutputPath);

    Step("Validate required docs");
    foreach (var relativePath in requiredDocs)
    {
        var path = Path.Combine(repoRootResolved, relativePath);
        if (!File.Exists(path))
            throw new FileNotFoundException($"Required file is missing: {path}");
    }

    Step("Restore");
    RunDotnetOrThrow(repoRootResolved, ["restore", "squirix.slnx"]);

    if (!options.SkipFormat)
    {
        Step("Verify formatting");
        RunDotnetOrThrow(repoRootResolved, ["format", "squirix.slnx", "--verify-no-changes", "--no-restore"]);
    }

    Step("Build solution");
    RunDotnetOrThrow(repoRootResolved, ["build", "squirix.slnx", "--configuration", options.Configuration, "--no-restore"]);

    Step("Validate file-based tools");
    RunDotnetOrThrow(repoRootResolved, ["run", "--file", "tools/internal/sqr-tools-verify.cs", "--"]);

    Step("Validate file-based examples");
    RunDotnetOrThrow(repoRootResolved, ["run", "--file", "tools/internal/sqr-examples-verify.cs", "--"]);

    if (!options.SkipTests)
    {
        Step("Run unit tests");
        RunDotnetOrThrow(repoRootResolved, ["test", "tests/squirix/squirix.unit-tests/Squirix.UnitTests.csproj", "--configuration", options.Configuration, "--no-build", "--verbosity", "normal"]);
        RunDotnetOrThrow(repoRootResolved, ["test", "tests/squirix.server/squirix.server.unit-tests/Squirix.Server.UnitTests.csproj", "--configuration", options.Configuration, "--no-build", "--verbosity", "normal"]);

        Step("Run smoke tests");
        RunDotnetOrThrow(repoRootResolved, ["test", "tests/squirix.server/squirix.server.smoke-tests/Squirix.Server.SmokeTests.csproj", "--configuration", options.Configuration, "--no-build", "--verbosity", "normal"]);

        if (options.IncludePropertyTests)
        {
            Step("Run property tests");
            RunDotnetOrThrow(repoRootResolved, ["test", "tests/squirix.server/squirix.server.property-tests/Squirix.Server.PropertyTests.csproj", "--configuration", options.Configuration, "--no-build", "--verbosity", "normal"]);
        }

        if (options.IncludeIntegrationTests)
        {
            Step("Run integration tests");
            RunDotnetOrThrow(repoRootResolved, ["test", "tests/squirix.server/squirix.server.integration-tests/Squirix.Server.IntegrationTests.csproj", "--configuration", options.Configuration, "--no-build", "--verbosity", "normal"]);
        }

        if (options.IncludeStressChecks)
        {
            Step("Run selected resiliency stress checks");
            var code = RunDotnet(
                repoRootResolved,
                [
                    "run",
                    "--file",
                    "tools/internal/sqr-resiliency-repeat.cs",
                    "--",
                    "-Iterations",
                    "2",
                    "-Configuration",
                    options.Configuration,
                    "-NoBuild",
                ]);
            if (code != 0)
                throw new InvalidOperationException($"Selected resiliency stress checks failed with exit code {code}.");
        }
    }

    Step("Pack packages");
    foreach (var project in packageProjects)
        RunDotnetOrThrow(repoRootResolved, NewPackArguments(project, options.Configuration, packageOutputPath, options.PackageVersion));

    Step("Validate package artifacts");
    var packages = Directory.EnumerateFiles(packageOutputPath, "*.nupkg", SearchOption.TopDirectoryOnly)
        .OrderBy(static path => path, StringComparer.OrdinalIgnoreCase)
        .ToArray();
    if (packages.Length < packageProjects.Length)
        throw new InvalidOperationException($"Expected at least {packageProjects.Length} .nupkg files in {packageOutputPath}.");

    foreach (var package in packages)
        ValidatePackageMetadata(package);

    Step("Prepare external package smoke source");
    if (Directory.Exists(smokePackageOutputPath))
        Directory.Delete(smokePackageOutputPath, true);
    _ = Directory.CreateDirectory(smokePackageOutputPath);
    foreach (var package in Directory.EnumerateFiles(packageOutputPath, "*.*nupkg", SearchOption.TopDirectoryOnly))
        File.Copy(package, Path.Combine(smokePackageOutputPath, Path.GetFileName(package)));

    Step("Build external package smoke against packed artifacts");
    RunDotnetOrThrow(
        repoRootResolved,
        [
            "build",
            "samples/external-package-smoke/ExternalPackageSmoke.csproj",
            "--configuration",
            options.Configuration,
            "/p:SmokeUsePackages=true",
        ]);

    Step("Run external package smoke against packed artifacts");
    var smokeRunCode = RunDotnet(
        Path.Combine(repoRootResolved, "samples", "external-package-smoke"),
        ["run", "--configuration", options.Configuration, "--no-build", "/p:SmokeUsePackages=true"]);
    if (smokeRunCode != 0)
        throw new InvalidOperationException($"External package smoke failed with exit code {smokeRunCode}.");

    if (options.IncludeBenchmarks)
    {
        Step("Build benchmarks");
        RunDotnetOrThrow(repoRootResolved, ["build", "benchmarks/squirix.benchmarks/Squirix.Benchmarks.csproj", "--configuration", options.Configuration, "--no-restore"]);
    }

    output.WriteLine($"Release validation completed. Artifacts: {packageOutputPath}");
    return 0;
}
catch (InvalidOperationException ex)
{
    Console.Error.WriteLine(ex.Message);
    return 1;
}
catch (IOException ex)
{
    Console.Error.WriteLine(ex.Message);
    return 1;
}
catch (UnauthorizedAccessException ex)
{
    Console.Error.WriteLine(ex.Message);
    return 1;
}

ReleaseOptions ParseOptions(string[] args)
{
    var parsed = new ReleaseOptions();
    for (var i = 0; i < args.Length; i++)
    {
        var a = args[i];
        if (string.Equals(a, "-SkipTests", StringComparison.OrdinalIgnoreCase) || string.Equals(a, "--skip-tests", StringComparison.OrdinalIgnoreCase))
        {
            parsed.SkipTests = true;
            continue;
        }

        if (string.Equals(a, "-SkipFormat", StringComparison.OrdinalIgnoreCase) || string.Equals(a, "--skip-format", StringComparison.OrdinalIgnoreCase))
        {
            parsed.SkipFormat = true;
            continue;
        }

        if (string.Equals(a, "-IncludeIntegrationTests", StringComparison.OrdinalIgnoreCase) || string.Equals(a, "--include-integration-tests", StringComparison.OrdinalIgnoreCase))
        {
            parsed.IncludeIntegrationTests = true;
            continue;
        }

        if (string.Equals(a, "-IncludePropertyTests", StringComparison.OrdinalIgnoreCase) || string.Equals(a, "--include-property-tests", StringComparison.OrdinalIgnoreCase))
        {
            parsed.IncludePropertyTests = true;
            continue;
        }

        if (string.Equals(a, "-IncludeStressChecks", StringComparison.OrdinalIgnoreCase) || string.Equals(a, "--include-stress-checks", StringComparison.OrdinalIgnoreCase))
        {
            parsed.IncludeStressChecks = true;
            continue;
        }

        if (string.Equals(a, "-IncludeBenchmarks", StringComparison.OrdinalIgnoreCase) || string.Equals(a, "--include-benchmarks", StringComparison.OrdinalIgnoreCase))
        {
            parsed.IncludeBenchmarks = true;
            continue;
        }

        if (string.Equals(a, "-Configuration", StringComparison.OrdinalIgnoreCase) || string.Equals(a, "--configuration", StringComparison.OrdinalIgnoreCase))
        {
            if (i + 1 >= args.Length)
                return ReleaseOptions.Invalid("missing value for -Configuration");
            parsed.Configuration = args[++i];
            continue;
        }

        if (string.Equals(a, "-ArtifactsDirectory", StringComparison.OrdinalIgnoreCase) || string.Equals(a, "--artifacts-directory", StringComparison.OrdinalIgnoreCase))
        {
            if (i + 1 >= args.Length)
                return ReleaseOptions.Invalid("missing value for -ArtifactsDirectory");
            parsed.ArtifactsDirectory = args[++i];
            continue;
        }

        if (string.Equals(a, "-PackageVersion", StringComparison.OrdinalIgnoreCase) || string.Equals(a, "--package-version", StringComparison.OrdinalIgnoreCase))
        {
            if (i + 1 >= args.Length)
                return ReleaseOptions.Invalid("missing value for -PackageVersion");
            parsed.PackageVersion = args[++i];
            continue;
        }

        return ReleaseOptions.Invalid($"unknown argument '{a}'");
    }

    return parsed;
}

string ResolveRepoRoot()
{
    var entryDir = AppContext.GetData("EntryPointFileDirectoryPath") as string;
    if (!string.IsNullOrWhiteSpace(entryDir))
    {
        var dir = new DirectoryInfo(entryDir);
        while (dir is not null)
        {
            if (File.Exists(Path.Combine(dir.FullName, "squirix.slnx")))
                return dir.FullName;

            dir = dir.Parent;
        }
    }

    return Environment.CurrentDirectory;
}

static void Step(string name)
{
    Console.Out.WriteLine($"==> {name}");
}

void RunDotnetOrThrow(string workingDirectory, IReadOnlyList<string> args)
{
    var code = RunDotnet(workingDirectory, args);
    if (code != 0)
        throw new InvalidOperationException($"dotnet {string.Join(' ', args)} failed with exit code {code}.");
}

static int RunDotnet(string workingDirectory, IReadOnlyList<string> args)
{
    using var proc = Process.Start(new ProcessStartInfo
    {
        FileName = "dotnet",
        WorkingDirectory = workingDirectory,
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

static IReadOnlyList<string> NewPackArguments(string projectPath, string configuration, string packageOutputPath, string packageVersion)
{
    var args = new List<string>
    {
        "pack",
        projectPath,
        "--configuration",
        configuration,
        "--no-build",
        "-o",
        packageOutputPath,
        "/p:ContinuousIntegrationBuild=true",
    };
    if (!string.IsNullOrWhiteSpace(packageVersion))
    {
        args.Add($"/p:SquirixPackageVersion={packageVersion}");
        args.Add($"/p:PackageVersion={packageVersion}");
    }

    return args;
}

static void ValidatePackageMetadata(string packagePath)
{
    using var archive = ZipFile.OpenRead(packagePath);
    var names = archive.Entries.Select(static e => e.FullName).ToArray();
    var nuspecName = names.FirstOrDefault(static n => n.EndsWith(".nuspec", StringComparison.OrdinalIgnoreCase));
    if (string.IsNullOrWhiteSpace(nuspecName))
        throw new InvalidOperationException($"Package has no nuspec: {packagePath}");
    if (!names.Contains("README.md", StringComparer.Ordinal))
        throw new InvalidOperationException($"Package has no README.md: {packagePath}");

    var nuspecEntry = archive.GetEntry(nuspecName) ?? throw new InvalidOperationException($"Package nuspec entry is missing: {packagePath}");
    using var stream = nuspecEntry.Open();
    var document = XDocument.Load(stream);
    var metadata = document.Root?.Elements().FirstOrDefault(static e => string.Equals(e.Name.LocalName, "metadata", StringComparison.Ordinal));
    if (metadata is null)
        throw new InvalidOperationException($"Package metadata is missing in {packagePath}.");

    foreach (var name in new[] { "id", "version", "authors", "description", "tags" })
    {
        var value = metadata.Elements().FirstOrDefault(e => string.Equals(e.Name.LocalName, name, StringComparison.Ordinal))?.Value.Trim();
        if (string.IsNullOrWhiteSpace(value))
            throw new InvalidOperationException($"Package metadata '{name}' is missing in {packagePath}.");
    }

    var repository = metadata.Elements().FirstOrDefault(static e => string.Equals(e.Name.LocalName, "repository", StringComparison.Ordinal));
    var repositoryUrl = repository?.Attribute("url")?.Value.Trim();
    if (string.IsNullOrWhiteSpace(repositoryUrl))
        throw new InvalidOperationException($"Package metadata 'repository.url' is missing in {packagePath}.");

    var licenseElement = metadata.Elements().FirstOrDefault(static e => string.Equals(e.Name.LocalName, "license", StringComparison.Ordinal));
    if (string.IsNullOrWhiteSpace(licenseElement?.Value.Trim()))
        throw new InvalidOperationException($"Package metadata 'license' is missing in {packagePath}.");
}

internal sealed class ReleaseOptions
{
    public string ArtifactsDirectory { get; set; } = "artifacts/release-validation";

    public string Configuration { get; set; } = "Release";

    public bool IncludeBenchmarks { get; set; }

    public bool IncludeIntegrationTests { get; set; }

    public bool IncludePropertyTests { get; set; }

    public bool IncludeStressChecks { get; set; }

    public bool IsValid { get; private set; } = true;

    public string PackageVersion { get; set; } = string.Empty;

    public bool SkipFormat { get; set; }

    public bool SkipTests { get; set; }

    public static ReleaseOptions Invalid(string message)
    {
        Console.Error.WriteLine($"ERROR: {message}");
        return new ReleaseOptions { IsValid = false };
    }
}
