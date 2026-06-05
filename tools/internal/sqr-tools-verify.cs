#:property PublishAot=false
using System.Diagnostics;

var argv = Environment.GetCommandLineArgs().Skip(1).ToArray();
if (argv.Length is 1 && (string.Equals(argv[0], "--help", StringComparison.OrdinalIgnoreCase)
    || string.Equals(argv[0], "-h", StringComparison.OrdinalIgnoreCase)
    || string.Equals(argv[0], "-?", StringComparison.OrdinalIgnoreCase)))
{
    Console.WriteLine("sqr-tools-verify — runs --help for every tools/sqr-*.cs file.");
    Console.WriteLine();
    Console.WriteLine("Usage:");
    Console.WriteLine("  dotnet run --file tools/internal/sqr-tools-verify.cs --");
    Console.WriteLine();
    Console.WriteLine("Exit codes: 0 ok, 1 failed tool execution");
    return 0;
}

var entryDir = AppContext.GetData("EntryPointFileDirectoryPath") as string;
var toolsDir = !string.IsNullOrWhiteSpace(entryDir)
    ? Directory.GetParent(entryDir)?.FullName
    : Path.Combine(Environment.CurrentDirectory, "tools");
if (string.IsNullOrWhiteSpace(toolsDir) || !Directory.Exists(toolsDir))
{
    Console.Error.WriteLine("ERROR: tools directory not found.");
    return 1;
}

var files = Directory.EnumerateFiles(toolsDir, "sqr-*.cs", SearchOption.TopDirectoryOnly)
    .Select(Path.GetFullPath)
    .OrderBy(static path => path, StringComparer.OrdinalIgnoreCase)
    .ToArray();

if (files.Length == 0)
{
    Console.Error.WriteLine("ERROR: no tools/sqr-*.cs files found.");
    return 1;
}

foreach (var file in files)
{
    var name = Path.GetFileName(file);
    Console.WriteLine($"---- {name} --help ----");
    using var proc = Process.Start(new ProcessStartInfo
    {
        FileName = "dotnet",
        Arguments = $"run --file \"{file}\" -- --help",
        WorkingDirectory = Directory.GetParent(toolsDir)?.FullName ?? Environment.CurrentDirectory,
        UseShellExecute = false,
    });

    proc?.WaitForExit();
    if (proc is null || proc.ExitCode != 0)
        return proc?.ExitCode ?? 1;
}

Console.WriteLine("OK: all file-based tools responded to --help.");
return 0;
