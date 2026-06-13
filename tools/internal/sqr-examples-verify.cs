#:property PublishAot=false
using System.Diagnostics;

var output = Console.Out;
var argv = Environment.GetCommandLineArgs().Skip(1).ToArray();
if (argv.Length is 1 && (string.Equals(argv[0], "--help", StringComparison.OrdinalIgnoreCase)
    || string.Equals(argv[0], "-h", StringComparison.OrdinalIgnoreCase)
    || string.Equals(argv[0], "-?", StringComparison.OrdinalIgnoreCase)))
{
    output.WriteLine("sqr-examples-verify — compile and smoke-run file-based examples.");
    output.WriteLine();
    output.WriteLine("Usage:");
    output.WriteLine("  dotnet run --file tools/internal/sqr-examples-verify.cs --");
    output.WriteLine();
    output.WriteLine("Exit codes: 0 ok, 1 failed example execution");
    return 0;
}

var entryDir = AppContext.GetData("EntryPointFileDirectoryPath") as string;
var repoRoot = !string.IsNullOrWhiteSpace(entryDir)
    ? Directory.GetParent(entryDir)?.Parent?.FullName
    : Environment.CurrentDirectory;
if (string.IsNullOrWhiteSpace(repoRoot) || !Directory.Exists(repoRoot))
{
    Console.Error.WriteLine("ERROR: repository root not found.");
    return 1;
}

var examplesDir = Path.Combine(repoRoot, "examples");
if (!Directory.Exists(examplesDir))
{
    Console.Error.WriteLine("ERROR: examples directory not found.");
    return 1;
}

var files = Directory.EnumerateFiles(examplesDir, "*.cs", SearchOption.TopDirectoryOnly)
    .Select(Path.GetFullPath)
    .OrderBy(static path => path, StringComparer.OrdinalIgnoreCase)
    .ToArray();

if (files.Length == 0)
{
    Console.Error.WriteLine("ERROR: no examples/*.cs files found.");
    return 1;
}

foreach (var file in files)
{
    var name = Path.GetFileName(file);
    var relativePath = Path.GetRelativePath(repoRoot, file).Replace('\\', '/');

    output.WriteLine($"---- {relativePath} --help ----");
    if (RunDotnet(repoRoot, ["run", "--file", relativePath, "--", "--help"]) != 0)
        return 1;

    foreach (var smokeArgs in GetSmokeArgs(name))
    {
        output.WriteLine($"---- {relativePath} {string.Join(' ', smokeArgs)} ----");
        if (RunDotnet(repoRoot, ["run", "--file", relativePath, "--", .. smokeArgs]) != 0)
            return 1;
    }
}

output.WriteLine("OK: all file-based examples compiled and smoke-run successfully.");
return 0;

static IEnumerable<string[]> GetSmokeArgs(string fileName)
{
    return fileName switch
    {
        "squirix-runner.cs" => [["--skip-load"]],
        _ => [],
    };
}

static int RunDotnet(string workingDirectory, string[] args)
{
    var arguments = string.Join(' ', args.Select(static arg => arg.Contains(' ', StringComparison.Ordinal) ? $"\"{arg}\"" : arg));
    using var proc = Process.Start(new ProcessStartInfo
    {
        FileName = "dotnet",
        Arguments = arguments,
        WorkingDirectory = workingDirectory,
        UseShellExecute = false,
    });

    proc?.WaitForExit();
    return proc?.ExitCode ?? 1;
}
