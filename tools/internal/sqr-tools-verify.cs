#!/usr/bin/env dotnet
#:property PublishAot=false
#:property IsAotCompatible=true
#:property ImplicitUsings=disable
#:include shared/SqrTool.cs
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using static Squirix.Tools.SqrTool;

var output = Console.Out;
var argv = Environment.GetCommandLineArgs()[1..];
if (argv.Length == 1 && (string.Equals(argv[0], "--help", StringComparison.OrdinalIgnoreCase) || string.Equals(argv[0], "-h", StringComparison.OrdinalIgnoreCase) ||
                         string.Equals(argv[0], "-?", StringComparison.OrdinalIgnoreCase)))
{
    await output.WriteLineAsync("sqr-tools-verify — runs --help for every other sqr-*.cs file under tools/.", CancellationToken.None).ConfigureAwait(false);
    await output.WriteLineAsync(CancellationToken.None).ConfigureAwait(false);
    await output.WriteLineAsync("Usage:", CancellationToken.None).ConfigureAwait(false);
    await output.WriteLineAsync("  dotnet run --file tools/internal/sqr-tools-verify.cs --", CancellationToken.None).ConfigureAwait(false);
    await output.WriteLineAsync(CancellationToken.None).ConfigureAwait(false);
    await output.WriteLineAsync("Exit codes: 0 ok, 1 failed tool execution", CancellationToken.None).ConfigureAwait(false);
    return 0;
}

var entryDir = AppContext.GetData("EntryPointFileDirectoryPath") as string;
var toolsDir = !string.IsNullOrWhiteSpace(entryDir) ? Directory.GetParent(entryDir)?.FullName : Path.Join(Environment.CurrentDirectory, "tools");
if (string.IsNullOrWhiteSpace(toolsDir) || !Directory.Exists(toolsDir))
{
    await Console.Error.WriteLineAsync("ERROR: tools directory not found.", CancellationToken.None).ConfigureAwait(false);
    return 1;
}

// The tool itself is skipped: running it with --help would only print this usage again.
var selfPath = AppContext.GetData("EntryPointFilePath") as string;
var files = new List<string>();
foreach (var file in Directory.EnumerateFiles(toolsDir, "sqr-*.cs", SearchOption.AllDirectories))
{
    var fullPath = Path.GetFullPath(file);
    if (selfPath == null || !string.Equals(fullPath, Path.GetFullPath(selfPath), StringComparison.OrdinalIgnoreCase))
        files.Add(fullPath);
}

files.Sort(StringComparer.OrdinalIgnoreCase);

if (files.Count == 0)
{
    await Console.Error.WriteLineAsync("ERROR: no sqr-*.cs files found under tools/.", CancellationToken.None).ConfigureAwait(false);
    return 1;
}

var dotnetPath = ResolveDotnetPath();
if (dotnetPath == null)
{
    await Console.Error.WriteLineAsync("ERROR: dotnet executable path is unavailable.", CancellationToken.None).ConfigureAwait(false);
    return 1;
}

var repoRoot = Directory.GetParent(toolsDir)?.FullName;
if (string.IsNullOrWhiteSpace(repoRoot))
{
    await Console.Error.WriteLineAsync("ERROR: repository root not found.", CancellationToken.None).ConfigureAwait(false);
    return 1;
}

repoRoot = Path.GetFullPath(repoRoot);

foreach (var file in files)
{
    var name = Path.GetRelativePath(toolsDir, file);
    await output.WriteLineAsync($"---- {name} --help ----", CancellationToken.None).ConfigureAwait(false);
    var processStartInfo = new ProcessStartInfo
    {
        FileName = dotnetPath,
        Arguments = $"run --file \"{file}\" -- --help",
        WorkingDirectory = repoRoot,
        UseShellExecute = false,
    };
    using var proc = Process.Start(processStartInfo);

    if (proc != null)
        await proc.WaitForExitAsync(CancellationToken.None).ConfigureAwait(false);

    if (proc?.ExitCode != 0)
        return proc?.ExitCode ?? 1;
}

await output.WriteLineAsync("OK: all file-based tools responded to --help.", CancellationToken.None).ConfigureAwait(false);
return 0;
