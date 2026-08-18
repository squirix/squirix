#:property PublishAot=false
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;

var output = Console.Out;
var argv = Environment.GetCommandLineArgs()[1..];
if (argv.Length == 1 && (string.Equals(argv[0], "--help", StringComparison.OrdinalIgnoreCase)
    || string.Equals(argv[0], "-h", StringComparison.OrdinalIgnoreCase)
    || string.Equals(argv[0], "-?", StringComparison.OrdinalIgnoreCase)))
{
    await output.WriteLineAsync("sqr-package-smoke — build and run external package smoke sample.").ConfigureAwait(false);
    await output.WriteLineAsync().ConfigureAwait(false);
    await output.WriteLineAsync("Usage:").ConfigureAwait(false);
    await output.WriteLineAsync("  dotnet run --file tools/internal/sqr-package-smoke.cs --").ConfigureAwait(false);
    return 0;
}

if (argv.Length > 0)
{
    await Console.Error.WriteLineAsync($"ERROR: unknown argument '{argv[0]}'").ConfigureAwait(false);
    return 1;
}

var repoRoot = ResolveRepoRoot();
var dotnetPath = ResolveDotnetPath();
if (dotnetPath == null)
{
    await Console.Error.WriteLineAsync("ERROR: dotnet executable path is unavailable.").ConfigureAwait(false);
    return 1;
}

var packageDir = Path.Join(repoRoot, "artifacts", "packages");
var packageCacheRoot = Path.Join(repoRoot, "artifacts", "package-smoke-nuget");
var packageCacheDir = Path.Join(packageCacheRoot, Guid.NewGuid().ToString("N"));
_ = Directory.CreateDirectory(packageDir);
_ = Directory.CreateDirectory(packageCacheRoot);
_ = Directory.CreateDirectory(packageCacheDir);

Environment.SetEnvironmentVariable("NUGET_PACKAGES", packageCacheDir);
foreach (var packagePath in Directory.EnumerateFiles(packageDir, "squirix.*.nupkg", SearchOption.TopDirectoryOnly))
    File.Delete(packagePath);
foreach (var packagePath in Directory.EnumerateFiles(packageDir, "squirix.*.snupkg", SearchOption.TopDirectoryOnly))
    File.Delete(packagePath);

var coreProject = Path.Join(repoRoot, "src", "squirix", "Squirix.csproj");
var serverProject = Path.Join(repoRoot, "src", "squirix.server", "Squirix.Server.csproj");
var corePackCode = await RunDotnetAsync(dotnetPath, repoRoot, ["pack", coreProject, "-c", "Release", "-o", packageDir], CancellationToken.None).ConfigureAwait(false);
if (corePackCode != 0)
    return corePackCode;

var serverPackCode = await RunDotnetAsync(dotnetPath, repoRoot, ["pack", serverProject, "-c", "Release", "-o", packageDir], CancellationToken.None).ConfigureAwait(false);
if (serverPackCode != 0)
    return serverPackCode;

if (!HasClientPackage(packageDir))
{
    await Console.Error.WriteLineAsync("ERROR: squirix client package was not produced.").ConfigureAwait(false);
    return 1;
}

if (!HasServerPackage(packageDir))
{
    await Console.Error.WriteLineAsync("ERROR: squirix.server package was not produced.").ConfigureAwait(false);
    return 1;
}

var sampleDir = Path.Join(repoRoot, "samples", "external-package-smoke");
var settingsPath = Path.Join(sampleDir, "Squirix.settings.json");
var hadSettings = File.Exists(settingsPath);
var settingsBackup = hadSettings ? await File.ReadAllBytesAsync(settingsPath, CancellationToken.None).ConfigureAwait(false) : null;

try
{
    const int maxAttempts = 5;
    for (var attempt = 1; attempt <= maxAttempts; attempt++)
    {
        var port = GetFreeTcpPort();
        var url = $"https://127.0.0.1:{port.ToString(CultureInfo.InvariantCulture)}";
        var json = BuildSettingsJson(url);
        await File.WriteAllTextAsync(settingsPath, json, CancellationToken.None).ConfigureAwait(false);

        var exitCode = await RunDotnetAsync(dotnetPath, sampleDir, ["run", "-c", "Release", "-p:SmokeUsePackages=true"], CancellationToken.None).ConfigureAwait(false);
        if (exitCode == 0 || attempt == maxAttempts)
            return exitCode;
    }

    return 1;
}
finally
{
    if (settingsBackup != null)
        await File.WriteAllBytesAsync(settingsPath, settingsBackup, CancellationToken.None).ConfigureAwait(false);
    else if (File.Exists(settingsPath))
        File.Delete(settingsPath);
}

static string BuildSettingsJson(string uri)
{
    var settings = new
    {
        Squirix = new
        {
            Cluster = new
            {
                NodeId = "external-smoke",
                Uri = uri,
                VirtualNodes = 128,
                Peers = new[]
                {
                    new
                    {
                        NodeId = "external-smoke",
                        Uri = uri,
                    },
                },
            },
        },
    };

#pragma warning disable ZA1001 // Ad-hoc smoke settings DTO; source generation is not worth the ceremony here.
    return JsonSerializer.Serialize(settings, new JsonSerializerOptions { WriteIndented = true });
#pragma warning restore ZA1001
}

static int GetFreeTcpPort()
{
    using var listener = new TcpListener(IPAddress.Loopback, 0);
    listener.Start();
    if (listener.LocalEndpoint is not IPEndPoint endpoint)
        throw new InvalidOperationException("TcpListener did not expose a local IPEndPoint.");

    return endpoint.Port;
}

static string ResolveRepoRoot()
{
    var entryDir = AppContext.GetData("EntryPointFileDirectoryPath") as string;
    var startDir = !string.IsNullOrWhiteSpace(entryDir) ? entryDir : Environment.CurrentDirectory;
    var current = new DirectoryInfo(startDir);

    while (current != null)
    {
        var hasSolution = File.Exists(Path.Join(current.FullName, "squirix.slnx"));
        var hasCoreProject = File.Exists(Path.Join(current.FullName, "src", "squirix", "Squirix.csproj"));
        if (hasSolution || hasCoreProject)
            return current.FullName;

        current = current.Parent;
    }

    return Environment.CurrentDirectory;
}

static bool HasClientPackage(string directory)
{
    foreach (var path in Directory.EnumerateFiles(directory, "squirix*.nupkg", SearchOption.TopDirectoryOnly))
    {
        var name = Path.GetFileName(path);
        if (name.StartsWith("squirix.", StringComparison.Ordinal)
            && !name.StartsWith("squirix.server.", StringComparison.Ordinal))
            return true;
    }

    return false;
}

static bool HasServerPackage(string directory)
{
    using var enumerator = Directory.EnumerateFiles(directory, "squirix.server*.nupkg", SearchOption.TopDirectoryOnly).GetEnumerator();
    return enumerator.MoveNext();
}

static string? ResolveDotnetPath()
{
    var dotnetRoot = Environment.GetEnvironmentVariable("DOTNET_ROOT");
    if (!string.IsNullOrWhiteSpace(dotnetRoot))
    {
        var dotnetRootCandidate = Path.Join(dotnetRoot, OperatingSystem.IsWindows() ? "dotnet.exe" : "dotnet");
        if (File.Exists(dotnetRootCandidate))
            return Path.GetFullPath(dotnetRootCandidate);
    }

    var processPath = Environment.ProcessPath;
    if (!string.IsNullOrWhiteSpace(processPath))
    {
        var processFileName = Path.GetFileName(processPath);
        if (string.Equals(processFileName, "dotnet", StringComparison.OrdinalIgnoreCase)
            || string.Equals(processFileName, "dotnet.exe", StringComparison.OrdinalIgnoreCase))
            return Path.GetFullPath(processPath);
    }

    var pathValue = Environment.GetEnvironmentVariable("PATH");
    if (string.IsNullOrWhiteSpace(pathValue))
        return null;

    var executableName = OperatingSystem.IsWindows() ? "dotnet.exe" : "dotnet";
    foreach (var segment in pathValue.Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
    {
        var pathCandidate = Path.Join(segment, executableName);
        if (File.Exists(pathCandidate))
            return Path.GetFullPath(pathCandidate);
    }

    return null;
}

static async Task<int> RunDotnetAsync(string dotnetPath, string workingDirectory, IReadOnlyList<string> args, CancellationToken cancellationToken)
{
    var startInfo = new ProcessStartInfo
    {
        FileName = dotnetPath,
        WorkingDirectory = workingDirectory,
        UseShellExecute = false,
    };

    foreach (var arg in args)
        startInfo.ArgumentList.Add(arg);

    using var proc = Process.Start(startInfo);
    if (proc == null)
    {
        await Console.Error.WriteLineAsync($"Failed to start process: {startInfo.FileName} {string.Join(' ', args)}").ConfigureAwait(false);
        await Console.Error.WriteLineAsync($"Working directory: {startInfo.WorkingDirectory}").ConfigureAwait(false);
        return 1;
    }

    await proc.WaitForExitAsync(cancellationToken).ConfigureAwait(false);
    return proc.ExitCode;
}
