#:property PublishAot=false
using System.Diagnostics;

var output = Console.Out;
var argv = Environment.GetCommandLineArgs()[1..];
if (argv.Length is 1 && (string.Equals(argv[0], "--help", StringComparison.OrdinalIgnoreCase) || string.Equals(argv[0], "-h", StringComparison.OrdinalIgnoreCase) ||
                         string.Equals(argv[0], "-?", StringComparison.OrdinalIgnoreCase)))
{
    await output.WriteLineAsync("sqr-tools-verify — runs --help for every tools/sqr-*.cs file.").ConfigureAwait(false);
    await output.WriteLineAsync().ConfigureAwait(false);
    await output.WriteLineAsync("Usage:").ConfigureAwait(false);
    await output.WriteLineAsync("  dotnet run --file tools/internal/sqr-tools-verify.cs --").ConfigureAwait(false);
    await output.WriteLineAsync().ConfigureAwait(false);
    await output.WriteLineAsync("Exit codes: 0 ok, 1 failed tool execution").ConfigureAwait(false);
    return 0;
}

var entryDir = AppContext.GetData("EntryPointFileDirectoryPath") as string;
var toolsDir = !string.IsNullOrWhiteSpace(entryDir) ? Directory.GetParent(entryDir)?.FullName : Path.Join(Environment.CurrentDirectory, "tools");
if (string.IsNullOrWhiteSpace(toolsDir) || !Directory.Exists(toolsDir))
{
    await Console.Error.WriteLineAsync("ERROR: tools directory not found.").ConfigureAwait(false);
    return 1;
}

var files = new List<string>();
foreach (var file in Directory.EnumerateFiles(toolsDir, "sqr-*.cs", SearchOption.TopDirectoryOnly))
    files.Add(Path.GetFullPath(file));

files.Sort(StringComparer.OrdinalIgnoreCase);

if (files.Count is 0)
{
    await Console.Error.WriteLineAsync("ERROR: no tools/sqr-*.cs files found.").ConfigureAwait(false);
    return 1;
}

var dotnetPath = ResolveDotnetPath();
if (dotnetPath is null)
{
    await Console.Error.WriteLineAsync("ERROR: dotnet executable path is unavailable.").ConfigureAwait(false);
    return 1;
}

var repoRoot = Directory.GetParent(toolsDir)?.FullName;
if (string.IsNullOrWhiteSpace(repoRoot))
{
    await Console.Error.WriteLineAsync("ERROR: repository root not found.").ConfigureAwait(false);
    return 1;
}

repoRoot = Path.GetFullPath(repoRoot);

foreach (var file in files)
{
    var name = Path.GetFileName(file);
    await output.WriteLineAsync($"---- {name} --help ----").ConfigureAwait(false);
    var processStartInfo = new ProcessStartInfo
    {
        FileName = dotnetPath,
        Arguments = $"run --file \"{file}\" -- --help",
        WorkingDirectory = repoRoot,
        UseShellExecute = false,
    };
    using var proc = Process.Start(processStartInfo);

    if (proc is not null)
        await proc.WaitForExitAsync(CancellationToken.None).ConfigureAwait(false);

    if (proc?.ExitCode is not 0)
        return proc?.ExitCode ?? 1;
}

await output.WriteLineAsync("OK: all file-based tools responded to --help.").ConfigureAwait(false);
return 0;

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
        var dotnet = string.Equals(processFileName, "dotnet", StringComparison.OrdinalIgnoreCase);
        var dotnetExe = string.Equals(processFileName, "dotnet.exe", StringComparison.OrdinalIgnoreCase);
        if (dotnet || dotnetExe)
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
