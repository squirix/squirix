#:property PublishAot=false
#:property IsAotCompatible=true
#:property ImplicitUsings=disable
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;

const string targetFrameworkProperty = "$(SquirixTargetFramework)";
const string packageTargetFrameworks = "net10.0;net11.0";
var projectExtensions = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { ".csproj", ".props", ".targets" };
var skippedDirectories = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "bin", "obj", ".git" };
var scopedTopLevelDirectories = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "src", "tests", "benchmarks", "samples" };

var argv = Environment.GetCommandLineArgs()[1..];
if (argv.Length == 1 && (string.Equals(argv[0], "--help", StringComparison.OrdinalIgnoreCase) || string.Equals(argv[0], "-h", StringComparison.OrdinalIgnoreCase) ||
                         string.Equals(argv[0], "-?", StringComparison.OrdinalIgnoreCase)))
{
    var output = Console.Out;
    await output.WriteLineAsync("sqr-net10-verify — verify the repository build and package target frameworks.", CancellationToken.None).ConfigureAwait(false);
    await output.WriteLineAsync(CancellationToken.None).ConfigureAwait(false);
    await output.WriteLineAsync("Usage:", CancellationToken.None).ConfigureAwait(false);
    await output.WriteLineAsync("  dotnet run --file tools/internal/sqr-net10-verify.cs -- [--root <path>]", CancellationToken.None).ConfigureAwait(false);
    return 0;
}

var root = ResolveDefaultRepoRoot();
var argIndex = 0;
while (argIndex < argv.Length)
{
    var a = argv[argIndex];
    if (string.Equals(a, "--root", StringComparison.OrdinalIgnoreCase))
    {
        if (argIndex + 1 >= argv.Length)
            return await FailUsageAsync("missing value for --root").ConfigureAwait(false);

        root = argv[argIndex + 1];
        argIndex += 2;
    }
    else
    {
        return await FailUsageAsync($"unknown argument '{a}'").ConfigureAwait(false);
    }
}

var resolvedRoot = Path.GetFullPath(root);
if (!Directory.Exists(resolvedRoot))
    return await FailUsageAsync($"root path does not exist: {resolvedRoot}").ConfigureAwait(false);

var failures = new List<string>();
foreach (var file in EnumerateProjectFiles(resolvedRoot))
    ValidateFile(resolvedRoot, file, failures);

if (failures.Count > 0)
{
    foreach (var failure in failures)
        await Console.Error.WriteLineAsync(failure, CancellationToken.None).ConfigureAwait(false);
    return 1;
}

await Console.Out.WriteLineAsync("squirix .NET baseline verified: builds use the shared target framework and release packages support net10.0 and net11.0.", CancellationToken.None).ConfigureAwait(false);
return 0;

string ResolveDefaultRepoRoot()
{
    var entryDir = AppContext.GetData("EntryPointFileDirectoryPath") as string;
    if (string.IsNullOrWhiteSpace(entryDir))
        return Environment.CurrentDirectory;
    var parent = Directory.GetParent(entryDir);
    var info = parent?.Parent;
    var dir = info?.Parent;
    return dir != null ? dir.FullName : Environment.CurrentDirectory;
}

IEnumerable<string> EnumerateProjectFiles(string repoRoot)
{
    foreach (var file in Directory.EnumerateFiles(repoRoot, "*.props", SearchOption.TopDirectoryOnly))
        yield return file;
    foreach (var file in Directory.EnumerateFiles(repoRoot, "*.targets", SearchOption.TopDirectoryOnly))
        yield return file;

    foreach (var file in Directory.EnumerateFiles(repoRoot, "*.*", SearchOption.AllDirectories))
    {
        var extension = Path.GetExtension(file);
        if (!projectExtensions.Contains(extension))
            continue;

        var relative = Path.GetRelativePath(repoRoot, file);
        var parts = relative.Split([Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar], StringSplitOptions.None);
        if (parts.Length == 0 || !scopedTopLevelDirectories.Contains(parts[0]))
            continue;

        if (ContainsSkippedSegment(parts, skippedDirectories))
            continue;

        yield return file;
    }
}

static bool ContainsSkippedSegment(string[] parts, HashSet<string> skipped)
{
    foreach (var part in parts)
    {
        if (skipped.Contains(part))
            return true;
    }

    return false;
}

void ValidateFile(string repoRoot, string path, List<string> outFailures)
{
    XmlDocument document;
    try
    {
        document = new XmlDocument();
        document.Load(path);
    }
    catch (InvalidOperationException ex)
    {
        outFailures.Add($"{Path.GetRelativePath(repoRoot, path)}: invalid XML: {ex.Message}");
        return;
    }
    catch (XmlException ex)
    {
        outFailures.Add($"{Path.GetRelativePath(repoRoot, path)}: invalid XML: {ex.Message}");
        return;
    }
    catch (IOException ex)
    {
        outFailures.Add($"{Path.GetRelativePath(repoRoot, path)}: invalid XML: {ex.Message}");
        return;
    }
    catch (UnauthorizedAccessException ex)
    {
        outFailures.Add($"{Path.GetRelativePath(repoRoot, path)}: invalid XML: {ex.Message}");
        return;
    }

    foreach (XmlNode node in document.GetElementsByTagName("*"))
    {
        if (node is not XmlElement element)
            continue;

        var localName = element.LocalName;
        var expected = localName switch
        {
            "TargetFramework" => targetFrameworkProperty,
            "TargetFrameworks" => packageTargetFrameworks,
            _ => null,
        };
        if (expected != null && !string.Equals(element.InnerText.Trim(), expected, StringComparison.Ordinal))
            outFailures.Add($"{Path.GetRelativePath(repoRoot, path)}: {localName} must be '{expected}'.");
    }
}

static async Task<int> FailUsageAsync(string message)
{
    await Console.Error.WriteLineAsync($"ERROR: {message}", CancellationToken.None).ConfigureAwait(false);
    return 1;
}
