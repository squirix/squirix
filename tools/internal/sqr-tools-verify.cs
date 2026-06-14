#:property PublishAot=false
using System.Diagnostics;

var output = Console.Out;
var argv = Environment.GetCommandLineArgs().Skip(1).ToArray();
if (argv.Length is 1 && (string.Equals(argv[0], "--help", StringComparison.OrdinalIgnoreCase)
    || string.Equals(argv[0], "-h", StringComparison.OrdinalIgnoreCase)
    || string.Equals(argv[0], "-?", StringComparison.OrdinalIgnoreCase)))
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
var toolsDir = !string.IsNullOrWhiteSpace(entryDir)
    ? Directory.GetParent(entryDir)?.FullName
    : Path.Combine(Environment.CurrentDirectory, "tools");
if (string.IsNullOrWhiteSpace(toolsDir) || !Directory.Exists(toolsDir))
{
    await Console.Error.WriteLineAsync("ERROR: tools directory not found.").ConfigureAwait(false);
    return 1;
}

var files = Directory.EnumerateFiles(toolsDir, "sqr-*.cs", SearchOption.TopDirectoryOnly)
    .Select(Path.GetFullPath)
    .OrderBy(static path => path, StringComparer.OrdinalIgnoreCase)
    .ToArray();

if (files.Length == 0)
{
    await Console.Error.WriteLineAsync("ERROR: no tools/sqr-*.cs files found.").ConfigureAwait(false);
    return 1;
}

foreach (var file in files)
{
    var name = Path.GetFileName(file);
    await output.WriteLineAsync($"---- {name} --help ----").ConfigureAwait(false);
    using var proc = Process.Start(new ProcessStartInfo
    {
        FileName = "dotnet",
        Arguments = $"run --file \"{file}\" -- --help",
        WorkingDirectory = Directory.GetParent(toolsDir)?.FullName ?? Environment.CurrentDirectory,
        UseShellExecute = false,
    });

    if (proc is not null)
        await proc.WaitForExitAsync().ConfigureAwait(false);

    if (proc is null || proc.ExitCode != 0)
        return proc?.ExitCode ?? 1;
}

await output.WriteLineAsync("OK: all file-based tools responded to --help.").ConfigureAwait(false);
return 0;
