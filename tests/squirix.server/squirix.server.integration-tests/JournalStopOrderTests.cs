using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests;

/// <summary>The journal stops after every other hosted service has stopped, so their last appends are durable and later appends are refused.</summary>
public sealed class JournalStopOrderTests : NodeIntegrationTestBase
{
    private const string NodeId = "node_journal_stop_order";

    /// <summary>Appends made by a hosted service while the host stops reach the journal, are durable after the stop, and the journal refuses appends once the host stopped.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HostedServiceAppendsPrecedeJournalStop(CancellationToken cancellationToken)
    {
        var probe = new StopProbe();
        await using var cluster = await StartClusterAsync(NodeId, new IntegrationStartOptions { UsePersistence = true, ServicesConfigure = probe.Register }, cancellationToken);
        var node = cluster[NodeId];
        var dataDir = node.GetRequiredService<PersistenceOptions>().DataDir;

        await cluster.StopNodeAsync(NodeId);

        // The observer registered before the persistence services stops last: the host has stopped the journal by then, and is not yet disposed.
        _ = await Assert.That(probe.AppendedWhileStopping).IsEqualTo(2);
        _ = await Assert.That(probe.RefusedAfterJournalStopped).IsTrue();
        _ = await Assert.That(ReadPutKeys(dataDir, cancellationToken)).Contains(StopProbe.StopPhaseKey.ToString());
        _ = await Assert.That(ReadPutKeys(dataDir, cancellationToken)).Contains(StopProbe.StoppedPhaseKey.ToString());
    }

    private static List<string> ReadPutKeys(string dataDir, CancellationToken cancellationToken)
    {
        var keys = new List<string>();
        using var records = JournalReadPath.ReadAll(dataDir, 1, cancellationToken);
        while (records.MoveNext())
        {
            if (records.Current.Operation == JournalOperationKind.Put)
                keys.Add(records.Current.Key.ToString());
        }

        return keys;
    }

    /// <summary>Registers a hosted service that appends while the host stops, and one that observes the journal once the host stopped it.</summary>
    [ThreadSafe]
    private sealed class StopProbe
    {
        internal static readonly CacheKey StopPhaseKey = CacheKey.Default("stop-phase");

        internal static readonly CacheKey StoppedPhaseKey = CacheKey.Default("stopped-phase");

        private static readonly byte[] Payload = JournalEntryPayloadKit.EncodePut("v");

        private int _appended;
        private int _refused;

        /// <summary>Gets how many appends the late hosted service made while the host was stopping.</summary>
        internal int AppendedWhileStopping => Volatile.Read(ref _appended);

        /// <summary>Gets a value indicating whether the journal refused an append at the very end of the host stop.</summary>
        internal bool RefusedAfterJournalStopped => Volatile.Read(ref _refused) == 1;

        /// <summary>Adds the late appender after every registration and the observer before all of them.</summary>
        /// <param name="services">The node service collection.</param>
        internal void Register(IServiceCollection services)
        {
            services.Insert(0, ServiceDescriptor.Singleton<IHostedService>(sp => new Observer(sp.GetRequiredService<IJournalCoordinator>(), this)));
            _ = services.AddSingleton<IHostedService>(sp => new LateAppender(sp.GetRequiredService<IJournalCoordinator>(), this));
        }

        /// <summary>Appends in the stop phase and in the stopped phase, which run before the journal stops.</summary>
        private sealed class LateAppender : IHostedLifecycleService
        {
            private readonly IJournalCoordinator _journal;
            private readonly StopProbe _probe;

            internal LateAppender(IJournalCoordinator journal, StopProbe probe)
            {
                _journal = journal;
                _probe = probe;
            }

            public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

            public Task StartedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

            public Task StartingAsync(CancellationToken cancellationToken) => Task.CompletedTask;

            public async Task StopAsync(CancellationToken cancellationToken)
            {
                await _journal.AppendPutUnderGateAsync(StopPhaseKey, Payload, CancellationToken.None);
                _ = Interlocked.Increment(ref _probe._appended);
            }

            public async Task StoppedAsync(CancellationToken cancellationToken)
            {
                await _journal.AppendPutUnderGateAsync(StoppedPhaseKey, Payload, CancellationToken.None);
                _ = Interlocked.Increment(ref _probe._appended);
            }

            public Task StoppingAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        }

        /// <summary>Tries to append in the last stopped phase of the host, after the journal stopped.</summary>
        private sealed class Observer : IHostedLifecycleService
        {
            private readonly IJournalCoordinator _journal;
            private readonly StopProbe _probe;

            internal Observer(IJournalCoordinator journal, StopProbe probe)
            {
                _journal = journal;
                _probe = probe;
            }

            public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

            public Task StartedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

            public Task StartingAsync(CancellationToken cancellationToken) => Task.CompletedTask;

            public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

            public async Task StoppedAsync(CancellationToken cancellationToken)
            {
                try
                {
                    await _journal.AppendPutUnderGateAsync(CacheKey.Default("after-stop"), Payload, CancellationToken.None);
                }
                catch (Exception ex) when (ex is InvalidOperationException or ObjectDisposedException)
                {
                    Volatile.Write(ref _probe._refused, 1);
                }
            }

            public Task StoppingAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        }
    }
}
