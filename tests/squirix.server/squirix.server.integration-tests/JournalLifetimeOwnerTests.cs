using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.Threading;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests;

/// <summary>
/// The journal host is the only owner of the journal lifetime. Every other registration that exposes the journal hands out a
/// decorator the container disposes first; if it passed the dispose through, a journal whose dispose throws (a leaked journal I/O thread)
/// would abort the container and skip the manifest ledger, the replica group registry, and its follower logs.
/// </summary>
public sealed class JournalLifetimeOwnerTests : NodeIntegrationTestBase
{
    private const string Scope = "journal-lifetime-owner";

    /// <summary>The replica groups a node of the RF=2 topology serves: its own group and the follower group of the other node.</summary>
    private static readonly string[] ServedGroups = ["node-a", "node-b"];

    /// <summary>Stopping a production RF=2 node over a throwing journal neither throws nor skips the services disposed after the journal.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NodeStopSurvivesThrowingJournalDispose(CancellationToken cancellationToken)
    {
        var disposals = new ThrowingJournalRegistration();
        var options = new IntegrationStartOptions
        {
            ReplicaCount = 2,
            UsePersistence = true,
            ExtraScope = Scope,
            ServicesConfigure = disposals.Configure,
        };
        await using var cluster = await StartClusterAsync("node-a", "node-b", options, cancellationToken);
        var node = cluster["node-a"];
        var ledger = node.GetRequiredService<Ledger>();
        var dataDir = node.GetRequiredService<PersistenceOptions>().DataDir;

        // Each open follower log holds its log file, so the file does not open exclusively while the node runs.
        _ = await Assert.That(AnyGroupLogReleased(dataDir)).IsFalse();

        // Every registration that exposes the journal is resolved before the node stops, as the hosted services resolve them.
        _ = node.GetRequiredService<IJournalCoordinator>();
        _ = node.GetRequiredService<IJournalMetrics>();
        _ = node.GetRequiredService<IExclusiveMaintenanceExecutor>();

        await cluster.StopNodeAsync("node-a");

        // node-b still runs, so every dispose so far is node-a's: its journal was disposed once, by its owner.
        _ = await Assert.That(disposals.Count).IsEqualTo(1);
        _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(ledger.ReadCurrentOrDefaultAsync(cancellationToken));

        // The replica group registry disposed every follower log it serves, and each log released its file.
        _ = await Assert.That(AllGroupLogsReleased(dataDir)).IsTrue();

        // The released group logs reopen on the same directory.
        var restart = new IntegrationStartOptions { ReplicaCount = 2, UsePersistence = true, ExtraScope = Scope, CleanTestDir = false };
        var registry = (await cluster.StartNodeAsync("node-a", restart, cancellationToken)).GetRequiredService<ReplicaGroupRegistry>();
        _ = await Assert.That(registry.TryGetLog("node-a", out _) && registry.TryGetLog("node-b", out _)).IsTrue();
    }

    private static bool AllGroupLogsReleased(string dataDir)
    {
        foreach (var group in ServedGroups)
        {
            if (!IsReleased(GroupStoragePaths.GetLogPath(dataDir, group)))
                return false;
        }

        return true;
    }

    private static bool AnyGroupLogReleased(string dataDir)
    {
        foreach (var group in ServedGroups)
        {
            if (IsReleased(GroupStoragePaths.GetLogPath(dataDir, group)))
                return true;
        }

        return false;
    }

    /// <summary>Determines whether no open handle holds <paramref name="path" />: only then does the file open exclusively.</summary>
    /// <param name="path">The file path.</param>
    /// <returns><see langword="true" /> when the file opens with no sharing; otherwise <see langword="false" />.</returns>
    private static bool IsReleased(string path)
    {
        try
        {
            using var handle = File.OpenHandle(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            return true;
        }
        catch (IOException)
        {
            return false;
        }
    }

    /// <summary>Swaps the journal the production host owns for one whose dispose fails, and counts those disposals.</summary>
    [ThreadSafe]
    private sealed class ThrowingJournalRegistration
    {
        private int _count;

        /// <summary>Gets the number of throwing journals disposed so far.</summary>
        internal int Count => Volatile.Read(ref _count);

        /// <summary>Wraps the journal host registration so the host owns a throwing journal from its first resolution.</summary>
        /// <param name="services">The node service collection.</param>
        /// <exception cref="InvalidOperationException">The composition registers no journal host factory.</exception>
        internal void Configure(IServiceCollection services)
        {
            for (var i = services.Count - 1; i >= 0; i--)
            {
                if (services[i].ServiceType != typeof(JournalCoordinatorHost))
                    continue;

                if (services[i].ImplementationFactory is not { } factory)
                    throw new InvalidOperationException("the journal host is not registered through a factory.");

                services[i] = ServiceDescriptor.Singleton(typeof(JournalCoordinatorHost), new ThrowingHostFactory(factory, this).Create);
                return;
            }

            throw new InvalidOperationException("the composition registers no journal host.");
        }

        /// <summary>
        /// Creates a journal that forwards to <paramref name="journal" />, and whose dispose disposes it and then fails, as a journal whose
        /// I/O thread leaked on shutdown does.
        /// </summary>
        /// <param name="journal">The production journal.</param>
        /// <returns>The throwing journal.</returns>
        /// <remarks>The node stays idle: it neither mutates nor cuts a snapshot, so the snapshot barriers are not set up and no append is raised.</remarks>
        internal IJournalCoordinator CreateJournal(IJournalCoordinator journal)
        {
            var expectations = new IJournalCoordinatorCreateExpectations();
            var setups = expectations.Setups;
            _ = setups.AppendedBytes.Gets().Callback(() => journal.AppendedBytes);
            _ = setups.AppendedOps.Gets().Callback(() => journal.AppendedOps);
            _ = setups.CurrentSegmentIndex.Gets().Callback(() => journal.CurrentSegmentIndex);
            _ = setups.HighWaterBytes.Gets().Callback(() => journal.HighWaterBytes);
            _ = setups.InFlightApplyGate.Gets().Callback(() => journal.InFlightApplyGate);
            _ = setups.IsJournalGroupCommitEnabled.Gets().Callback(() => journal.IsJournalGroupCommitEnabled);
            _ = setups.MaxBytes.Gets().Callback(() => journal.MaxBytes);
            _ = setups.NextSequence.Gets().Callback(() => journal.NextSequence);
            _ = setups.RecentAppendLatencyMs.Gets().Callback(() => journal.RecentAppendLatencyMs);
            _ = setups.UsedBytes.Gets().Callback(() => journal.UsedBytes);
            _ = setups.AppendIdempotencyOutcomeAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<Action?>(), Arg.Any<CancellationToken>())
               .Callback(journal.AppendIdempotencyOutcomeAsync);
            _ = setups.AppendPutAsync(Arg.Any<AsyncLockOwnership>(), Arg.Any<CacheKey>(), Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<CancellationToken>())
               .Callback(journal.AppendPutAsync);
            _ = setups.AppendRemoveAsync(Arg.Any<AsyncLockOwnership>(), Arg.Any<CacheKey>(), Arg.Any<CancellationToken>())
               .Callback(journal.AppendRemoveAsync);
            _ = setups.AwaitDurabilityCommitAsync(Arg.Any<CancellationToken>()).Callback(journal.AwaitDurabilityCommitAsync);
            _ = setups.ExecuteMaintenanceExclusiveAsync(Arg.Any<Func<CancellationToken, ValueTask>>(), Arg.Any<CancellationToken>())
               .Callback(journal.ExecuteMaintenanceExclusiveAsync);
            _ = setups.FailJournalPipeline(Arg.Any<Exception>()).Callback(journal.FailJournalPipeline);
            _ = setups.GetJournalThreadFailure().Callback(journal.GetJournalThreadFailure);
            _ = setups.WaitForStartupAsync(Arg.Any<CancellationToken>()).Callback(journal.WaitForStartupAsync);
            _ = setups.DisposeAsync().Callback(async () =>
            {
                await journal.DisposeAsync();
                _ = Interlocked.Increment(ref _count);
                throw new TimeoutException("journal I/O thread is still alive after shutdown; writer, ring, and gates are leaked.");
            });

            return expectations.Instance();
        }
    }

    /// <summary>Resolves the production journal host and hands it the throwing journal over the journal it owns.</summary>
    [Immutable]
    private sealed class ThrowingHostFactory
    {
        private readonly Func<IServiceProvider, object> _inner;
        private readonly ThrowingJournalRegistration _registration;

        internal ThrowingHostFactory(Func<IServiceProvider, object> inner, ThrowingJournalRegistration registration)
        {
            _inner = inner;
            _registration = registration;
        }

        /// <summary>Resolves the production journal host and hands it the throwing journal.</summary>
        /// <param name="services">The node service provider.</param>
        /// <returns>The journal host, owning the throwing journal.</returns>
        /// <exception cref="InvalidOperationException">The wrapped factory does not create a journal host.</exception>
        internal JournalCoordinatorHost Create(IServiceProvider services)
        {
            if (_inner(services) is not JournalCoordinatorHost host)
                throw new InvalidOperationException("the journal host factory returned another service.");

            host.Attach(_registration.CreateJournal(host.Coordinator));
            return host;
        }
    }
}
