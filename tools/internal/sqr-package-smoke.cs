#:property PublishAot=false
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;

var output = Console.Out;
var argv = Environment.GetCommandLineArgs().Skip(1).ToArray();
if (argv.Length is 1 && (string.Equals(argv[0], "--help", StringComparison.OrdinalIgnoreCase)
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
var packageDir = Path.Combine(repoRoot, "artifacts", "packages");
var packageCacheRoot = Path.Combine(repoRoot, "artifacts", "package-smoke-nuget");
var packageCacheDir = Path.Combine(packageCacheRoot, Guid.NewGuid().ToString("N"));
_ = Directory.CreateDirectory(packageDir);
_ = Directory.CreateDirectory(packageCacheRoot);
_ = Directory.CreateDirectory(packageCacheDir);

Environment.SetEnvironmentVariable("NUGET_PACKAGES", packageCacheDir);
foreach (var packagePath in Directory.EnumerateFiles(packageDir, "squirix.*.nupkg", SearchOption.TopDirectoryOnly))
    File.Delete(packagePath);
foreach (var packagePath in Directory.EnumerateFiles(packageDir, "squirix.*.snupkg", SearchOption.TopDirectoryOnly))
    File.Delete(packagePath);

var coreProject = Path.Combine(repoRoot, "src", "squirix", "Squirix.csproj");
var serverProject = Path.Combine(repoRoot, "src", "squirix.server", "Squirix.Server.csproj");
var corePackCode = await RunDotnet(repoRoot, ["pack", coreProject, "-c", "Release", "-o", packageDir]).ConfigureAwait(false);
if (corePackCode != 0)
    return corePackCode;

var serverPackCode = await RunDotnet(repoRoot, ["pack", serverProject, "-c", "Release", "-o", packageDir]).ConfigureAwait(false);
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

var sampleDir = Path.Combine(repoRoot, "samples", "external-package-smoke");
var settingsPath = Path.Combine(sampleDir, "Squirix.settings.json");
var hadSettings = File.Exists(settingsPath);
var settingsBackup = hadSettings ? await File.ReadAllBytesAsync(settingsPath).ConfigureAwait(false) : null;

try
{
    const int maxAttempts = 5;
    for (var attempt = 1; attempt <= maxAttempts; attempt++)
    {
        var port = GetFreeTcpPort();
        var url = $"https://127.0.0.1:{port}";
        var json = BuildSettingsJson(url);
        await File.WriteAllTextAsync(settingsPath, json).ConfigureAwait(false);

        var exitCode = await RunDotnet(sampleDir, ["run", "-c", "Release", "-p:SmokeUsePackages=true"]).ConfigureAwait(false);
        if (exitCode == 0 || attempt == maxAttempts)
            return exitCode;
    }

    return 1;
}
finally
{
    if (settingsBackup is not null)
    {
        await File.WriteAllBytesAsync(settingsPath, settingsBackup).ConfigureAwait(false);
    }
    else if (File.Exists(settingsPath))
    {
        File.Delete(settingsPath);
    }
}

static string BuildSettingsJson(string url)
{
    var settings = new
    {
        Squirix = new
        {
            Cluster = new
            {
                NodeId = "external-smoke",
                Url = url,
                VirtualNodes = 128,
                Peers = new[]
                {
                    new
                    {
                        NodeId = "external-smoke",
                        Url = url,
                    },
                },
            },
        },
    };

    return JsonSerializer.Serialize(settings, new JsonSerializerOptions { WriteIndented = true });
}

static int GetFreeTcpPort()
{
    using var listener = new TcpListener(IPAddress.Loopback, 0);
    listener.Start();
    return ((IPEndPoint)listener.LocalEndpoint).Port;
}

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

static bool HasClientPackage(string directory)
{
    return Directory.EnumerateFiles(directory, "squirix*.nupkg", SearchOption.TopDirectoryOnly)
                  .Any(static path =>
                  {
                      var name = Path.GetFileName(path);
                      return name.StartsWith("squirix.", StringComparison.Ordinal)
                          && !name.StartsWith("squirix.server.", StringComparison.Ordinal);
                  });
}

static bool HasServerPackage(string directory)
{
    return Directory.EnumerateFiles(directory, "squirix.server*.nupkg", SearchOption.TopDirectoryOnly).Any();
}

static async Task<int> RunDotnet(string workingDirectory, IReadOnlyList<string> args)
{
    var startInfo = new ProcessStartInfo
    {
        FileName = "dotnet",
        WorkingDirectory = workingDirectory,
        UseShellExecute = false,
    };

    foreach (var arg in args)
        startInfo.ArgumentList.Add(arg);

    using var proc = Process.Start(startInfo);
    if (proc is null)
    {
        await Console.Error.WriteLineAsync($"Failed to start process: {startInfo.FileName} {string.Join(' ', args)}").ConfigureAwait(false);
        await Console.Error.WriteLineAsync($"Working directory: {startInfo.WorkingDirectory}").ConfigureAwait(false);
        return 1;
    }

    await proc.WaitForExitAsync().ConfigureAwait(false);
    return proc.ExitCode;
}
