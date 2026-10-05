using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.Versioning;
using System.Security.AccessControl;
using System.Security.Principal;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using TUnit.Core.Exceptions;

namespace Squirix.Server.IntegrationTests;

/// <summary>The journal and the manifest ledger diagnostics must reach the logger the host registers, not a null logger.</summary>
public sealed class JournalHostLoggingTests : NodeIntegrationTestBase
{
    private const int JournalWaitCanceledWhileStalledEventId = 1014;
    private const int ManifestRetentionCleanupFailedEventId = 1009;
    private const int ManifestRetentionDeleteFailedEventId = 1008;

    /// <summary>A stall warning logged by the host journal reaches the host logger under the journal coordinator category.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="InvalidOperationException">The host journal does not expose its stall probe.</exception>
    [Test]
    public async Task JournalEventsReachHostLogger(CancellationToken cancellationToken)
    {
        using var recorder = new RecordingLoggerProvider();
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var options = new IntegrationStartOptions { PersistenceOptions = new PersistenceOptions(), ServicesConfigure = recorder.Register, TimeProvider = clock };
        await using var cluster = await StartClusterAsync("node_journal_logging", options, cancellationToken);
        var node = cluster["node_journal_logging"];
        if (node.GetRequiredService<JournalCoordinatorHost>().Coordinator is not IJournalStallProbeSource journal)
            throw new InvalidOperationException("the host journal does not expose its stall probe.");

        // The node is idle, so its journal thread performs no segment I/O and the test is the only writer of the probe.
        journal.StallProbe.IoStarted(nameof(IJournalSegmentWriter.FlushToDisk));
        try
        {
            // The warning fires only once the I/O has been in progress for the slow-operation threshold, measured on the server clock.
            clock.Advance(TimeSpan.FromMilliseconds(JournalSlowOperationReporter.WarningThresholdMs + 1));

            journal.StallProbe.ReportWaitCanceled("durability commit");
        }
        finally
        {
            journal.StallProbe.IoFinished();
        }

        _ = await Assert.That(recorder.Find(JournalWaitCanceledWhileStalledEventId)?.Category).IsEqualTo(typeof(JournalCoordinator).FullName);
    }

    /// <summary>A manifest retention cleanup failure logged by the host ledger reaches the host logger under the ledger category.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="SkipTestException">Thrown when the platform has no directory ACL to deny listing with, or the account can list despite it.</exception>
    [Test]
    [SupportedOSPlatform("windows")]
    public async Task RetentionFailureReachesHostLogger(CancellationToken cancellationToken)
    {
        if (!OperatingSystem.IsWindows())
            throw new SkipTestException("Denying directory listing through an ACL is Windows-specific.");

        using var recorder = new RecordingLoggerProvider();
        var options = new IntegrationStartOptions { PersistenceOptions = new PersistenceOptions(), ServicesConfigure = recorder.Register };
        await using var cluster = await StartClusterAsync("node_ledger_cleanup_logging", options, cancellationToken);
        var node = cluster["node_ledger_cleanup_logging"];
        var ledger = node.GetRequiredService<Ledger>();
        var dataDir = node.GetRequiredService<PersistenceOptions>().DataDir;
        var current = await ledger.ReadCurrentOrDefaultAsync(cancellationToken);

        // The first write scans the data directory for the next manifest index; do it before listing is denied.
        await ledger.WriteAsync(current, cancellationToken);

        // Retention cleanup lists the data directory; once listing is denied (files stay readable and writable) it fails with an exception.
        var directory = new DirectoryInfo(dataDir);
        var deny = new FileSystemAccessRule(WindowsIdentity.GetCurrent().User!, FileSystemRights.ListDirectory, InheritanceFlags.None, PropagationFlags.None, AccessControlType.Deny);
        var security = directory.GetAccessControl();
        security.AddAccessRule(deny);
        directory.SetAccessControl(security);
        try
        {
            // An elevated administrator (as on hosted CI runners) can still list the directory despite the deny entry; the cleanup
            // failure cannot be provoked there, and waiting for it would only time out.
            if (CanListDirectory(directory))
                throw new SkipTestException("This account can list the directory despite the deny entry, so the cleanup failure cannot be provoked.");

            await ledger.WriteAsync(current, cancellationToken);

            var started = Stopwatch.GetTimestamp();
            while (recorder.Find(ManifestRetentionCleanupFailedEventId)?.Category == null && Stopwatch.GetElapsedTime(started) < TimeSpan.FromSeconds(30))
                await Task.Delay(TimeSpan.FromMilliseconds(50), TimeProvider.System, cancellationToken);
        }
        finally
        {
            security = directory.GetAccessControl();
            _ = security.RemoveAccessRule(deny);
            directory.SetAccessControl(security);
        }

        _ = await Assert.That(recorder.Find(ManifestRetentionCleanupFailedEventId)?.Category).IsEqualTo(typeof(Ledger).FullName);
    }

    /// <summary>A manifest retention warning logged by the host ledger reaches the host logger under the ledger category.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="SkipTestException">Thrown when the platform lets an open file be deleted.</exception>
    [Test]
    public async Task RetentionWarningReachesHostLogger(CancellationToken cancellationToken)
    {
        if (!OperatingSystem.IsWindows())
            throw new SkipTestException("Blocking a delete by holding the file open is Windows-specific.");

        using var recorder = new RecordingLoggerProvider();
        var options = new IntegrationStartOptions
        {
            PersistenceOptions = new PersistenceOptions { ManifestRetentionCount = 1 },
            ServicesConfigure = recorder.Register,
        };
        await using var cluster = await StartClusterAsync("node_ledger_logging", options, cancellationToken);
        var node = cluster["node_ledger_logging"];
        var ledger = node.GetRequiredService<Ledger>();
        var dataDir = node.GetRequiredService<PersistenceOptions>().DataDir;
        var current = await ledger.ReadCurrentOrDefaultAsync(cancellationToken);

        // A manifest the host cannot delete: it is stale once newer manifests are published, so retention cleanup must report the failure.
        await ledger.WriteAsync(current, cancellationToken);
        var stalest = string.Empty;
        foreach (var path in Directory.GetFiles(dataDir, $"{FilePrefixes.Manifest}*{FileExtensions.Manifest}"))
        {
            if (stalest.Length == 0 || string.CompareOrdinal(path, stalest) < 0)
                stalest = path;
        }

        using var held = File.OpenHandle(stalest, FileMode.Open, FileAccess.Read, FileShare.None);
        await ledger.WriteAsync(current, cancellationToken);
        await ledger.WriteAsync(current, cancellationToken);

        var started = Stopwatch.GetTimestamp();
        while (recorder.Find(ManifestRetentionDeleteFailedEventId)?.Category == null && Stopwatch.GetElapsedTime(started) < TimeSpan.FromSeconds(30))
            await Task.Delay(TimeSpan.FromMilliseconds(50), TimeProvider.System, cancellationToken);

        _ = await Assert.That(recorder.Find(ManifestRetentionDeleteFailedEventId)?.Category).IsEqualTo(typeof(Ledger).FullName);
    }

    private static bool CanListDirectory(DirectoryInfo directory)
    {
        try
        {
            _ = directory.GetFileSystemInfos();
            return true;
        }
        catch (UnauthorizedAccessException)
        {
            return false;
        }
    }
}
