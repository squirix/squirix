#:property PublishAot=false
using System.Diagnostics;

var output = Console.Out;
var argv = Environment.GetCommandLineArgs()[1..];
if (argv.Length is 1 && (string.Equals(argv[0], "--help", StringComparison.OrdinalIgnoreCase) || string.Equals(argv[0], "-h", StringComparison.OrdinalIgnoreCase) ||
                         string.Equals(argv[0], "-?", StringComparison.OrdinalIgnoreCase)))
{
    await output.WriteLineAsync("sqr-examples-verify — compile and smoke-run file-based examples.").ConfigureAwait(false);
    await output.WriteLineAsync().ConfigureAwait(false);
    await output.WriteLineAsync("Usage:").ConfigureAwait(false);
    await output.WriteLineAsync("  dotnet run --file tools/internal/sqr-examples-verify.cs --").ConfigureAwait(false);
    await output.WriteLineAsync().ConfigureAwait(false);
    await output.WriteLineAsync("Exit codes: 0 ok, 1 failed example execution").ConfigureAwait(false);
    return 0;
}

var dotnetPath = ResolveDotnetPath();
if (dotnetPath is null)
{
    await Console.Error.WriteLineAsync("ERROR: dotnet executable path is unavailable.").ConfigureAwait(false);
    return 1;
}

var entryDir = AppContext.GetData("EntryPointFileDirectoryPath") as string;
var repoRoot = !string.IsNullOrWhiteSpace(entryDir) ? Directory.GetParent(entryDir)?.Parent?.FullName : Environment.CurrentDirectory;
if (string.IsNullOrWhiteSpace(repoRoot) || !Directory.Exists(repoRoot))
{
    await Console.Error.WriteLineAsync("ERROR: repository root not found.").ConfigureAwait(false);
    return 1;
}

repoRoot = Path.GetFullPath(repoRoot);

var examplesDir = Path.Combine(repoRoot, "examples");
if (!Directory.Exists(examplesDir))
{
    await Console.Error.WriteLineAsync("ERROR: examples directory not found.").ConfigureAwait(false);
    return 1;
}

var files = new List<string>();
foreach (var file in Directory.EnumerateFiles(examplesDir, "*.cs", SearchOption.TopDirectoryOnly))
    files.Add(Path.GetFullPath(file));

files.Sort(StringComparer.OrdinalIgnoreCase);

if (files.Count is 0)
{
    await Console.Error.WriteLineAsync("ERROR: no examples/*.cs files found.").ConfigureAwait(false);
    return 1;
}

foreach (var file in files)
{
    var name = Path.GetFileName(file);
    var relativePath = Path.GetRelativePath(repoRoot, file).Replace('\\', '/');

    await output.WriteLineAsync($"---- {relativePath} --help ----").ConfigureAwait(false);
    if (await RunDotnetAsync(dotnetPath, repoRoot, ["run", "--file", relativePath, "--", "--help"], CancellationToken.None).ConfigureAwait(false) is not 0)
        return 1;

    foreach (var smokeArgs in GetSmokeArgs(name))
    {
        var smokeCommand = FormatSmokeCommand(smokeArgs);
        await output.WriteLineAsync($"---- {relativePath} {smokeCommand} ----").ConfigureAwait(false);
        if (await RunDotnetAsync(dotnetPath, repoRoot, ["run", "--file", relativePath, "--", .. smokeArgs], CancellationToken.None).ConfigureAwait(false) is not 0)
            return 1;
    }
}

await output.WriteLineAsync("OK: all file-based examples compiled and smoke-run successfully.").ConfigureAwait(false);
return 0;

static IEnumerable<string[]> GetSmokeArgs(string fileName)
{
    return fileName switch
    {
        "squirix-runner.cs" => [["--skip-load"]],
        _ => [],
    };
}

static string FormatSmokeCommand(string[] args)
{
    if (args.Length is 0)
        return string.Empty;

    if (args.Length is 1)
        return args[0];

    var builder = new System.Text.StringBuilder();
    for (var i = 0; i < args.Length; i++)
    {
        if (i > 0)
            builder.Append(' ');

        builder.Append(args[i]);
    }

    return builder.ToString();
}

static async Task<int> RunDotnetAsync(string dotnetPath, string workingDirectory, string[] args, CancellationToken cancellationToken)
{
    var quotedArgs = new string[args.Length];
    for (var i = 0; i < args.Length; i++)
    {
        var arg = args[i];
        quotedArgs[i] = arg.Contains(' ', StringComparison.Ordinal) ? $"\"{arg}\"" : arg;
    }

    var arguments = string.Join(' ', quotedArgs);
    using var proc = Process.Start(
        new ProcessStartInfo
        {
            FileName = dotnetPath,
            Arguments = arguments,
            WorkingDirectory = workingDirectory,
            UseShellExecute = false,
        });

    if (proc is not null)
        await proc.WaitForExitAsync(cancellationToken).ConfigureAwait(false);

    return proc?.ExitCode ?? 1;
}

static string? ResolveDotnetPath()
{
    var dotnetRoot = Environment.GetEnvironmentVariable("DOTNET_ROOT");
    if (!string.IsNullOrWhiteSpace(dotnetRoot))
    {
        var dotnetRootCandidate = Path.Combine(dotnetRoot, OperatingSystem.IsWindows() ? "dotnet.exe" : "dotnet");
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
        var pathCandidate = Path.Combine(segment, executableName);
        if (File.Exists(pathCandidate))
            return Path.GetFullPath(pathCandidate);
    }

    return null;
}
