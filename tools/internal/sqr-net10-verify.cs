#:property PublishAot=false
using System.Xml.Linq;

const string supportedTargetFramework = "net10.0";
var projectExtensions = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { ".csproj", ".props", ".targets" };
var skippedDirectories = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "bin", "obj", ".git" };
var scopedTopLevelDirectories = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "src", "tests", "benchmarks", "samples" };

var argv = Environment.GetCommandLineArgs().Skip(1).ToArray();
if (argv.Length is 1 && (string.Equals(argv[0], "--help", StringComparison.OrdinalIgnoreCase)
    || string.Equals(argv[0], "-h", StringComparison.OrdinalIgnoreCase)
    || string.Equals(argv[0], "-?", StringComparison.OrdinalIgnoreCase)))
{
    var output = Console.Out;
    await output.WriteLineAsync("sqr-net10-verify — verify that all project TFMs are net10.0.").ConfigureAwait(false);
    await output.WriteLineAsync().ConfigureAwait(false);
    await output.WriteLineAsync("Usage:").ConfigureAwait(false);
    await output.WriteLineAsync("  dotnet run --file tools/internal/sqr-net10-verify.cs -- [--root <path>]").ConfigureAwait(false);
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
        await Console.Error.WriteLineAsync(failure).ConfigureAwait(false);
    return 1;
}

await Console.Out.WriteLineAsync("squirix .NET baseline verified: all project TargetFramework entries are net10.0.").ConfigureAwait(false);
return 0;

string ResolveDefaultRepoRoot()
{
    var entryDir = AppContext.GetData("EntryPointFileDirectoryPath") as string;
    if (!string.IsNullOrWhiteSpace(entryDir))
    {
        var internalDir = Directory.GetParent(entryDir);
        var toolsDir = internalDir?.Parent;
        var repoDir = toolsDir?.Parent;
        if (repoDir is not null)
            return repoDir.FullName;
    }

    return Environment.CurrentDirectory;
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

        if (parts.Any(skippedDirectories.Contains))
            continue;

        yield return file;
    }
}

void ValidateFile(string repoRoot, string path, List<string> outFailures)
{
    XDocument document;
    try
    {
        document = XDocument.Load(path, LoadOptions.None);
    }
    catch (InvalidOperationException ex)
    {
        outFailures.Add($"{Path.GetRelativePath(repoRoot, path)}: invalid XML: {ex.Message}");
        return;
    }
    catch (System.Xml.XmlException ex)
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

    foreach (var element in document.Descendants())
    {
        var localName = element.Name.LocalName;
        if (!string.Equals(localName, "TargetFramework", StringComparison.Ordinal)
            && !string.Equals(localName, "TargetFrameworks", StringComparison.Ordinal))
        {
            continue;
        }

        var frameworks = element.Value.Split(';');
        foreach (var framework in frameworks)
        {
            var value = framework.Trim();
            if (value.Length == 0 || string.Equals(value, supportedTargetFramework, StringComparison.Ordinal))
            {
                continue;
            }

            outFailures.Add(
                $"{Path.GetRelativePath(repoRoot, path)}: unsupported target framework '{value}'. squirix projects must target net10.0 only.");
        }
    }
}

static async Task<int> FailUsageAsync(string message)
{
    await Console.Error.WriteLineAsync($"ERROR: {message}").ConfigureAwait(false);
    return 1;
}
