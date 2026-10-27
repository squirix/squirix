using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using Squirix.Server.TestKit.Diagnostics;
using Squirix.Testing;

namespace Squirix.Server.TestKit.IO;

/// <summary>Owns the per-process temp root of a test process: removes the roots of exited test processes and its own root on exit.</summary>
public static class ProcTempRoot
{
    private const string ProcessIdPrefix = "pid";
    private const string StartTicksPrefix = "-start";

    private static readonly Lazy<string> Root = new(Create);

    /// <summary>Gets the per-process temp root, sweeping roots of exited test processes on first use.</summary>
    /// <returns>The <see cref="PathKit.GetProcTempPath" /> root of this process under the test temp root.</returns>
    public static string Claim() => Root.Value;

    private static string Create()
    {
        var root = PathKit.GetProcTempPath(PathKit.TestTempRootName);
        if (Path.GetDirectoryName(root) is { } parent && Directory.Exists(parent))
            SweepExited(parent, Path.GetFileName(root));

        AppDomain.CurrentDomain.ProcessExit += static (_, _) => DeleteQuietly(Root.Value);
        return root;
    }

    private static void SweepExited(string parent, string own)
    {
        foreach (var dir in Directory.EnumerateDirectories(parent))
        {
            var name = Path.GetFileName(dir);
            if (!string.Equals(name, own, StringComparison.Ordinal) && TryParse(name, out var processId, out var startTicks) && !IsRunning(processId, startTicks))
                DeleteQuietly(dir);
        }
    }

    private static bool TryParse(string name, out int processId, out long startTicks)
    {
        processId = 0;
        startTicks = 0;
        if (!name.StartsWith(ProcessIdPrefix, StringComparison.Ordinal))
            return false;

        var span = name.AsSpan(ProcessIdPrefix.Length);
        var split = span.IndexOf(StartTicksPrefix, StringComparison.Ordinal);
        return split > 0
            && int.TryParse(span[..split], NumberStyles.None, CultureInfo.InvariantCulture, out processId)
            && long.TryParse(span[(split + StartTicksPrefix.Length)..], NumberStyles.None, CultureInfo.InvariantCulture, out startTicks);
    }

    /// <summary>Tells whether the process that made a root still runs; a reused process identifier has another start time.</summary>
    /// <param name="processId">The identifier of the process that made the root.</param>
    /// <param name="startTicks">The start time of that process, in UTC ticks.</param>
    /// <returns><see langword="true" /> when that process still runs or start times are not supported.</returns>
    private static bool IsRunning(int processId, long startTicks)
    {
        try
        {
            using var process = Process.GetProcessById(processId);
            return process.StartTime.ToUniversalTime().Ticks == startTicks;
        }
        catch (Exception ex) when (ex is ArgumentException or InvalidOperationException or Win32Exception)
        {
            // Gone, or a reused identifier: the roots live in the user's temp, so their test process would have a readable start time.
            return false;
        }
        catch (NotSupportedException)
        {
            // Start times cannot be compared here, so the root may belong to a running process.
            return true;
        }
    }

    private static void DeleteQuietly(string dir)
    {
        try
        {
            if (Directory.Exists(dir))
                Directory.Delete(dir, true);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            TestLog.Suppressed($"Could not delete the test temp root '{dir}'.", ex);
        }
    }
}
