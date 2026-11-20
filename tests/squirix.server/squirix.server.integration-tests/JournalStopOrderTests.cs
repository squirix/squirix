using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Composition;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Networking;
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
        using var directory = new TempDirectory("squirix-journal-stop-order");
        var uri = GetNextHttpUri();
        ListenPortPool.ReleaseHeldPrimary(uri);
        var probe = new StopProbe();
        var options = new NodeHostStartOptions
        {
            PersistenceOptions = new PersistenceOptions { DataDir = directory },
            ServicesConfigure = probe.Register,
        };
        await using var node = await NodeHost.StartAsync(new TopologyOptions(new ServerPeer { NodeId = NodeId, Uri = uri }) { NodeId = NodeId, Uri = uri }, options, cancellationToken);
        var journal = node.Services.GetRequiredService<IJournalCoordinator>();

        await node.StopAsync(cancellationToken);

        _ = await Assert.That(probe.AppendedWhileStopping).IsEqualTo(2);
        var rejected = journal.AppendPutUnderGateAsync(CacheKey.Default("after-stop"), StopProbe.Payload, cancellationToken);
        _ = await NodeAsyncAssert.ThrowsOneOfAsync<InvalidOperationException, ObjectDisposedException>(rejected.AsTask(), TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);
        _ = await Assert.That(ReadPutKeys(directory, cancellationToken)).Contains(StopProbe.StopPhaseKey.ToString());
        _ = await Assert.That(ReadPutKeys(directory, cancellationToken)).Contains(StopProbe.StoppedPhaseKey.ToString());
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

    /// <summary>Registers a hosted service that appends while the host stops, and another at the opposite end of the stop order.</summary>
    [ThreadSafe]
    private sealed class StopProbe
    {
        internal static readonly CacheKey StopPhaseKey = CacheKey.Default("stop-phase");

        internal static readonly CacheKey StoppedPhaseKey = CacheKey.Default("stopped-phase");

        internal static readonly byte[] Payload = JournalEntryPayloadKit.EncodePut("v");

        private int _appended;

        /// <summary>Gets how many appends the late hosted service made while the host was stopping.</summary>
        internal int AppendedWhileStopping => Volatile.Read(ref _appended);

        /// <summary>Adds the late appender after every registration and the other writer before all of them.</summary>
        /// <param name="services">The node service collection.</param>
        internal void Register(IServiceCollection services)
        {
            services.Insert(0, ServiceDescriptor.Singleton<INodeService>(sp => new LateAppender(sp.GetRequiredService<IJournalCoordinator>(), this, StoppedPhaseKey)));
            _ = services.AddSingleton<INodeService>(sp => new LateAppender(sp.GetRequiredService<IJournalCoordinator>(), this, StopPhaseKey));
        }

        /// <summary>Appends while the node stops its services, before the final journal drain.</summary>
        private sealed class LateAppender : INodeService
        {
            private readonly IJournalCoordinator _journal;
            private readonly StopProbe _probe;
            private readonly CacheKey _key;

            internal LateAppender(IJournalCoordinator journal, StopProbe probe, CacheKey key)
            {
                _journal = journal;
                _probe = probe;
                _key = key;
            }

            public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

            public async Task StopAsync(CancellationToken cancellationToken)
            {
                await _journal.AppendPutUnderGateAsync(_key, Payload, CancellationToken.None);
                _ = Interlocked.Increment(ref _probe._appended);
            }
        }
    }
}
