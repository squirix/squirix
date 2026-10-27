using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Tools;

/// <summary>Helpers shared by the file-based tools in tools/internal; each tool pulls them in with #:include.</summary>
internal static class SqrTool
{
    /// <summary>Finds the nearest directory above the tool file that holds squirix.slnx or the client project.</summary>
    /// <returns>The repository root, or the current directory when no root is found.</returns>
    public static string ResolveRepoRoot()
    {
        var entryDir = AppContext.GetData("EntryPointFileDirectoryPath") as string;
        var startDir = !string.IsNullOrWhiteSpace(entryDir) ? entryDir : Environment.CurrentDirectory;
        var current = new DirectoryInfo(startDir);

        while (current != null)
        {
            var hasSolution = File.Exists(Path.Join(current.FullName, "squirix.slnx"));
            var hasCoreProject = File.Exists(Path.Join(current.FullName, "src", "squirix", "Squirix.csproj"));
            if (hasSolution || hasCoreProject)
                return current.FullName;

            current = current.Parent;
        }

        return Environment.CurrentDirectory;
    }

    /// <summary>Finds the dotnet executable: DOTNET_ROOT first, then the host running the tool, then PATH.</summary>
    /// <returns>The full path of the dotnet executable, or null when none is found.</returns>
    public static string? ResolveDotnetPath()
    {
        var executableName = OperatingSystem.IsWindows() ? "dotnet.exe" : "dotnet";
        var dotnetRoot = Environment.GetEnvironmentVariable("DOTNET_ROOT");
        if (!string.IsNullOrWhiteSpace(dotnetRoot))
        {
            var dotnetRootCandidate = Path.Join(dotnetRoot, executableName);
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

        foreach (var segment in pathValue.Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var pathCandidate = Path.Join(segment, executableName);
            if (File.Exists(pathCandidate))
                return Path.GetFullPath(pathCandidate);
        }

        return null;
    }

    /// <summary>Runs dotnet with the given arguments and waits for it to exit.</summary>
    /// <param name="dotnetPath">The dotnet executable.</param>
    /// <param name="workingDirectory">The working directory of the process.</param>
    /// <param name="args">The arguments, passed one by one without shell quoting.</param>
    /// <param name="cancellationToken">Cancels the wait for the process to exit.</param>
    /// <returns>The exit code of the process, or 1 when it does not start.</returns>
    public static async Task<int> RunDotnetAsync(string dotnetPath, string workingDirectory, IReadOnlyList<string> args, CancellationToken cancellationToken)
    {
        var startInfo = new ProcessStartInfo
        {
            FileName = dotnetPath,
            WorkingDirectory = workingDirectory,
            UseShellExecute = false,
        };

        foreach (var arg in args)
            startInfo.ArgumentList.Add(arg);

        using var proc = Process.Start(startInfo);
        if (proc == null)
        {
            await Console.Error.WriteLineAsync($"Failed to start process: {startInfo.FileName} {string.Join(' ', args)}", CancellationToken.None).ConfigureAwait(false);
            await Console.Error.WriteLineAsync($"Working directory: {startInfo.WorkingDirectory}", CancellationToken.None).ConfigureAwait(false);
            return 1;
        }

        await proc.WaitForExitAsync(cancellationToken).ConfigureAwait(false);
        return proc.ExitCode;
    }
}
