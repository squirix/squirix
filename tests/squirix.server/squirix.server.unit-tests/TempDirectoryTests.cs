using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit.IO;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Temporary test data follows both scope and process lifetimes.</summary>
public sealed class TempDirectoryTests
{
    private const string ChildMode = "SQUIRIX_TEMP_CLEANUP_CHILD";
    private const string PathPrefix = "SQUIRIX_TEMP_PATH=";

    /// <summary>Disposal removes the unique directory and leaves another scope intact.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeRemovesOwnedDirectoryOnly(CancellationToken cancellationToken)
    {
        using var sibling = new TempDirectory("squirix-temp-cleanup");
        using var directory = new TempDirectory("squirix-temp-cleanup", "nested");
        string path = directory;
        var root = Path.GetDirectoryName(path)!;
        await File.WriteAllTextAsync(Path.Join(path, "payload"), "test", cancellationToken);

        _ = await Assert.That(Path.GetRelativePath(ProcTempRoot.Claim(), root).StartsWith("..", StringComparison.Ordinal)).IsFalse();
        directory.Dispose();
        directory.Dispose();

        _ = await Assert.That(Directory.Exists(root)).IsFalse();
        _ = await Assert.That(Directory.Exists(sibling)).IsTrue();
    }

    /// <summary>Normal exit removes abandoned data; the next process sweeps data left by a killed process and preserves live data.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ProcessCleanupRemovesAbandonedData(CancellationToken cancellationToken)
    {
        var mode = Environment.GetEnvironmentVariable(ChildMode);
        if (mode != null)
        {
            // Deliberately leave this scope undisposed to exercise the process fallback.
            var directory = new TempDirectory("squirix-temp-cleanup-child");
            await File.WriteAllTextAsync(Path.Join(directory, "payload"), "test", cancellationToken);
            await using var output = new StreamWriter(Console.OpenStandardOutput()) { AutoFlush = true };
            await output.WriteLineAsync(PathPrefix + directory);
            if (string.Equals(mode, "wait", StringComparison.Ordinal))
                await Task.Delay(Timeout.InfiniteTimeSpan, TimeProvider.System, cancellationToken);
            return;
        }

        using var budget = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        budget.CancelAfter(TimeSpan.FromMinutes(1));
        using var live = new TempDirectory("squirix-temp-cleanup-live");
        var exited = await RunChildAsync(false, budget.Token);
        _ = await Assert.That(Directory.Exists(exited)).IsFalse();

        var killed = await RunChildAsync(true, budget.Token);
        _ = await Assert.That(File.Exists(Path.Join(killed, "payload"))).IsTrue();
        _ = await RunChildAsync(false, budget.Token);
        _ = await Assert.That(Directory.Exists(killed)).IsFalse();
        _ = await Assert.That(Directory.Exists(live)).IsTrue();
    }

    private static async Task<string> RunChildAsync(bool kill, CancellationToken cancellationToken)
    {
        var info = new ProcessStartInfo("dotnet")
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        info.ArgumentList.Add("exec");
        info.ArgumentList.Add(Path.Join(AppContext.BaseDirectory, "Squirix.Server.UnitTests.dll"));
        info.ArgumentList.Add("--treenode-filter");
        info.ArgumentList.Add("/*/*/TempDirectoryTests/ProcessCleanupRemovesAbandonedData");
        info.Environment[ChildMode] = kill ? "wait" : "exit";
        using var child = ThrowHelper.Required(Process.Start(info), "Could not start cleanup test child.");
        var errors = child.StandardError.ReadToEndAsync(cancellationToken);
        try
        {
            string? path = null;
            while (await child.StandardOutput.ReadLineAsync(cancellationToken) is { } line)
            {
                if (!line.StartsWith(PathPrefix, StringComparison.Ordinal))
                    continue;
                path = line[PathPrefix.Length..];
                break;
            }

            if (path == null)
                throw new InvalidOperationException("Child did not create test data: " + await errors);
            if (kill)
                child.Kill(true);
            var output = child.StandardOutput.ReadToEndAsync(cancellationToken);
            await child.WaitForExitAsync(cancellationToken);
            _ = await output;
            var error = await errors;
            return !kill && child.ExitCode != 0
                ? ThrowHelper.Throw<string>(new InvalidOperationException("Cleanup child failed: " + error))
                : path;
        }
        finally
        {
            if (!child.HasExited)
            {
                child.Kill(true);
                await child.WaitForExitAsync(CancellationToken.None);
            }
        }
    }
}
