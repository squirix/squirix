using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;
using Squirix.E2ETests.Fixtures.ProcessHosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Smoke;

/// <summary>Checks actual cross-process ownership of the shared smoke-test port slices.</summary>
[Property("Suite", "Smoke")]
public sealed class ProcessPortIsolationTests
{
    private const string ChildMode = "SQUIRIX_PORT_ISOLATION_CHILD";
    private const string PortPrefix = "SQUIRIX_RESERVED_PORT=";

    /// <summary>Cancellation waits for every started worker to exit before removing its temporary directory.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(60_000)]
    public async Task CancellationWaitsForWorkersAndCleansTemp(CancellationToken cancellationToken)
    {
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        List<Process> observed = [];
        string? root = null;
        var cancelled = false;
        var allRunning = true;
        try
        {
            try
            {
                await RunIsolatedAsync(
                    cancellation.Token,
                    whenReady: async (children, directory, token) =>
                    {
                        root = directory;
                        for (var index = 0; index < children.Count; index++)
                        {
                            var child = Process.GetProcessById(children[index].Id);
                            observed.Add(child);
                            allRunning &= !child.HasExited;
                        }

                        await cancellation.CancelAsync();
                        await Task.Delay(Timeout.InfiniteTimeSpan, TimeProvider.System, token);
                    });
            }
            catch (OperationCanceledException exception) when (exception.CancellationToken == cancellation.Token && !cancellationToken.IsCancellationRequested)
            {
                cancelled = true;
            }

            _ = await Assert.That(cancelled).IsTrue();
            _ = await Assert.That(observed.Count).IsEqualTo(3);
            _ = await Assert.That(allRunning).IsTrue();
            _ = await Assert.That(root).IsNotNull();
            _ = await Assert.That(Directory.Exists(root)).IsFalse();
            for (var index = 0; index < observed.Count; index++)
                _ = await Assert.That(observed[index].HasExited).IsTrue();
        }
        finally
        {
            for (var index = 0; index < observed.Count; index++)
                observed[index].Dispose();
        }
    }

    /// <summary>An exhausted outer coordination directory cannot prevent an isolated group from claiming its own slices.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(60_000)]
    public async Task OccupiedSharedSlicesDoNotBlockIsolation(CancellationToken cancellationToken)
    {
        var directory = Directory.CreateTempSubdirectory("squirix-port-isolation-busy-");
        List<SafeFileHandle> claims = [];
        try
        {
            var locks = Path.Join(directory.FullName, "squirix-testkit-port-slices");
            _ = Directory.CreateDirectory(locks);
            for (var index = 0; index < 8; index++)
                claims.Add(File.OpenHandle(Path.Join(locks, $"squirix-test-port-slice-{index}.lock"), FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None));

            await RunIsolatedAsync(cancellationToken, directory.FullName);
            _ = await Assert.That(Directory.GetFiles(locks).Length).IsEqualTo(8);
            _ = await Assert.That(Directory.GetDirectories(directory.FullName).Length).IsEqualTo(1);
        }
        finally
        {
            for (var index = 0; index < claims.Count; index++)
                claims[index].Dispose();
            directory.Delete(true);
        }
    }

    /// <summary>Three live processes reserve disjoint slices even after releasing their listening sockets.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(60_000)]
    public async Task ProcessesReserveDisjointSmokeSlices(CancellationToken cancellationToken)
    {
        if (Environment.GetEnvironmentVariable(ChildMode) != null)
        {
            using var reservation = ProcessPortReservation.Reserve();
            reservation.ReleaseForStart();
            await using var output = new StreamWriter(Console.OpenStandardOutput()) { AutoFlush = true };
            await output.WriteLineAsync((PortPrefix + reservation.Uri.Port.ToString(CultureInfo.InvariantCulture) + "|" + Path.GetTempPath()).AsMemory(), cancellationToken);
            _ = await Console.In.ReadLineAsync(cancellationToken);
            return;
        }

        await RunIsolatedAsync(cancellationToken);
    }

    private static async Task<int> ReadPortAsync(Process child, string tempRoot, Task<string> errors, CancellationToken cancellationToken)
    {
        var output = new StringBuilder();
        while (await child.StandardOutput.ReadLineAsync(cancellationToken) is { } line)
        {
            if (!line.StartsWith(PortPrefix, StringComparison.Ordinal))
            {
                _ = output.AppendLine(line);
                continue;
            }

            var separator = line.IndexOf('|', StringComparison.Ordinal);
            var actualRoot = Path.TrimEndingDirectorySeparator(line[(separator + 1)..]);
            _ = await Assert.That(actualRoot).IsEqualTo(Path.TrimEndingDirectorySeparator(tempRoot));
            return int.Parse(line.AsSpan(PortPrefix.Length, separator - PortPrefix.Length), CultureInfo.InvariantCulture);
        }

        throw new InvalidOperationException("Child did not reserve a port: " + output + await errors.WaitAsync(cancellationToken));
    }

    private static async Task RunIsolatedAsync(
        CancellationToken cancellationToken,
        string? tempParent = null,
        Func<IReadOnlyList<Process>, string, CancellationToken, Task>? whenReady = null)
    {
        var directory = Directory.CreateDirectory(Path.Join(tempParent ?? Path.GetTempPath(), "squirix-port-isolation-" + Guid.NewGuid().ToString("N")));
        DirectoryInfo? pipes = null;
        List<Process> children = [];
        List<Task<string>> errors = [];
        try
        {
            // Keep MTP socket paths short even when the test data root is deeply nested.
            pipes = Directory.CreateTempSubdirectory("sqi-");
            var slices = new int[3];
            for (var index = 0; index < slices.Length; index++)
            {
                var child = StartChild(directory.FullName, pipes.FullName);
                children.Add(child);
                errors.Add(child.StandardError.ReadToEndAsync(CancellationToken.None));
                var port = await ReadPortAsync(child, directory.FullName, errors[index], cancellationToken);
                _ = await Assert.That(port is >= 14_000 and < 17_000).IsTrue();
                slices[index] = (port - 14_000) / 375;
                for (var previous = 0; previous < index; previous++)
                    _ = await Assert.That(slices[index]).IsNotEqualTo(slices[previous]);
            }

            for (var index = 0; index < children.Count; index++)
                _ = await Assert.That(children[index].HasExited).IsFalse();
            if (whenReady != null)
                await whenReady(children, directory.FullName, cancellationToken);
            cancellationToken.ThrowIfCancellationRequested();
        }
        finally
        {
            // Every process is a direct child and starts no descendants, so each handle proves its worker has exited.
            try
            {
                await StopChildrenAsync(children);
                _ = await Task.WhenAll(errors);
            }
            finally
            {
                for (var index = 0; index < children.Count; index++)
                    children[index].Dispose();
            }

            try
            {
                directory.Delete(true);
            }
            finally
            {
                pipes?.Delete(true);
            }
        }

        _ = await Assert.That(Directory.Exists(directory.FullName)).IsFalse();
        _ = await Assert.That(Directory.Exists(pipes.FullName)).IsFalse();
    }

    private static Process StartChild(string tempRoot, string pipeRoot)
    {
        var info = new ProcessStartInfo(Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") ?? "dotnet")
        {
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        info.ArgumentList.Add("exec");
        info.ArgumentList.Add(Path.Join(AppContext.BaseDirectory, "Squirix.E2ETests.dll"));
        info.ArgumentList.Add("--treenode-filter");
        info.ArgumentList.Add("/*/*/ProcessPortIsolationTests/ProcessesReserveDisjointSmokeSlices");
        info.ArgumentList.Add("--results-directory");
        info.ArgumentList.Add(Path.Join(tempRoot, "results"));
        info.Environment["TESTINGPLATFORM_PIPE_DIRECTORY"] = pipeRoot;
        info.Environment[ChildMode] = "worker";
        info.Environment["TEMP"] = tempRoot;
        info.Environment["TMP"] = tempRoot;
        info.Environment["TMPDIR"] = tempRoot;
        return Process.Start(info) ?? ThrowStartFailed();
    }

    private static async Task StopChildAsync(Process child)
    {
        if (!child.HasExited)
            child.Kill();
        await child.WaitForExitAsync(CancellationToken.None);
    }

    private static Task StopChildrenAsync(List<Process> children)
    {
        var stops = new Task[children.Count];
        for (var index = 0; index < children.Count; index++)
            stops[index] = StopChildAsync(children[index]);
        return Task.WhenAll(stops);
    }

    private static Process ThrowStartFailed() => throw new InvalidOperationException("Could not start port-isolation test child.");
}
