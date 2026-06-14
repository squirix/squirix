#:property PublishAot=false
using System.Diagnostics;

var output = Console.Out;
var argv = Environment.GetCommandLineArgs().Skip(1).ToArray();
if (argv.Length is 1 && (string.Equals(argv[0], "--help", StringComparison.OrdinalIgnoreCase)
    || string.Equals(argv[0], "-h", StringComparison.OrdinalIgnoreCase)
    || string.Equals(argv[0], "-?", StringComparison.OrdinalIgnoreCase)))
{
    await output.WriteLineAsync("sqr-examples-verify — compile and smoke-run file-based examples.").ConfigureAwait(false);
    await output.WriteLineAsync().ConfigureAwait(false);
    await output.WriteLineAsync("Usage:").ConfigureAwait(false);
    await output.WriteLineAsync("  dotnet run --file tools/internal/sqr-examples-verify.cs --").ConfigureAwait(false);
    await output.WriteLineAsync().ConfigureAwait(false);
    await output.WriteLineAsync("Exit codes: 0 ok, 1 failed example execution").ConfigureAwait(false);
    return 0;
}

var entryDir = AppContext.GetData("EntryPointFileDirectoryPath") as string;
var repoRoot = !string.IsNullOrWhiteSpace(entryDir)
    ? Directory.GetParent(entryDir)?.Parent?.FullName
    : Environment.CurrentDirectory;
if (string.IsNullOrWhiteSpace(repoRoot) || !Directory.Exists(repoRoot))
{
    await Console.Error.WriteLineAsync("ERROR: repository root not found.").ConfigureAwait(false);
    return 1;
}

var examplesDir = Path.Combine(repoRoot, "examples");
if (!Directory.Exists(examplesDir))
{
    await Console.Error.WriteLineAsync("ERROR: examples directory not found.").ConfigureAwait(false);
    return 1;
}

var files = Directory.EnumerateFiles(examplesDir, "*.cs", SearchOption.TopDirectoryOnly)
    .Select(Path.GetFullPath)
    .OrderBy(static path => path, StringComparer.OrdinalIgnoreCase)
    .ToArray();

if (files.Length == 0)
{
    await Console.Error.WriteLineAsync("ERROR: no examples/*.cs files found.").ConfigureAwait(false);
    return 1;
}

foreach (var file in files)
{
    var name = Path.GetFileName(file);
    var relativePath = Path.GetRelativePath(repoRoot, file).Replace('\\', '/');

    await output.WriteLineAsync($"---- {relativePath} --help ----").ConfigureAwait(false);
    if (await RunDotnet(repoRoot, ["run", "--file", relativePath, "--", "--help"]).ConfigureAwait(false) != 0)
        return 1;

    foreach (var smokeArgs in GetSmokeArgs(name))
    {
        await output.WriteLineAsync($"---- {relativePath} {string.Join(' ', smokeArgs)} ----").ConfigureAwait(false);
        if (await RunDotnet(repoRoot, ["run", "--file", relativePath, "--", .. smokeArgs]).ConfigureAwait(false) != 0)
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

static async Task<int> RunDotnet(string workingDirectory, string[] args)
{
    var arguments = string.Join(' ', args.Select(static arg => arg.Contains(' ', StringComparison.Ordinal) ? $"\"{arg}\"" : arg));
    using var proc = Process.Start(new ProcessStartInfo
    {
        FileName = "dotnet",
        Arguments = arguments,
        WorkingDirectory = workingDirectory,
        UseShellExecute = false,
    });

    if (proc is not null)
        await proc.WaitForExitAsync().ConfigureAwait(false);

    return proc?.ExitCode ?? 1;
}
