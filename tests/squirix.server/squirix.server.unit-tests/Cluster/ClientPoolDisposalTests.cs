using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Node.Observability;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster;

/// <summary>Pool shutdown disposal: best-effort drain across peers, and disposal of the HTTP handlers the pool owns.</summary>
[Immutable]
public sealed class ClientPoolDisposalTests : DisposableServerUnitTestBase
{
    private const string PoolDisposalsTotalInstrumentName = "squirix_peer_pool_disposals_total";

    /// <summary>An unexpected policy disposal failure must not leak the remaining peers or channels.</summary>
    [Test]
    public async Task DisposeContinuesAfterPolicyFailure()
    {
        using var meter = new Meter("Squirix");
        using var sink = new NodeMeasurementSink(meter);
        var policies = new Dictionary<string, RecordingPolicy>(StringComparer.Ordinal)
        {
            ["n0"] = new(new InvalidOperationException("Unexpected policy disposal failure.")),
            ["n1"] = new(null),
            ["n2"] = new(null),
        };
        var pool = new ServerClientPool(BuildPeers(3), new ServerClientPoolArgs { PolicyFactory = nodeId => policies[nodeId] }, new ServerClientPoolMetrics(meter), NullLogger<ServerClientPool>.Instance);
        await using (pool)
        {
            await pool.DisposeAsync();

            _ = await Assert.That(policies["n0"].Disposed).IsTrue();
            _ = await Assert.That(policies["n1"].Disposed).IsTrue();
            _ = await Assert.That(policies["n2"].Disposed).IsTrue();
            _ = await Assert.That(sink.HasEvent(PoolDisposalsTotalInstrumentName)).IsTrue();
        }
    }

    /// <summary>Disposing the pool must dispose every default handler the pool created for its peers.</summary>
    [Test]
    public async Task DisposeReleasesOwnedHandlers()
    {
        using var meter = new Meter("Squirix");
        var created = new List<TrackingHandler>();
        var args = new ServerClientPoolArgs
        {
            PolicyFactory = static _ => new RecordingPolicy(null),
            OwnedHandlerFactory = (_, _) => Track(created),
        };
        var pool = new ServerClientPool(BuildPeers(3), args, new ServerClientPoolMetrics(meter), NullLogger<ServerClientPool>.Instance);
        await pool.DisposeAsync();

        await AssertAllDisposedAsync(created, 3);
    }

    /// <summary>Disposing the pool must dispose every mTLS handler the pool created from the node certificate.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeReleasesOwnedMtlsHandlers(CancellationToken cancellationToken)
    {
        using var meter = new Meter("Squirix");
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = LoadCertificate(bundle);
        var created = new List<TrackingHandler>();
        var args = MtlsArgs(certificate, null, (_, _) => Track(created));
        var pool = new ServerClientPool(BuildPeers(2), args, new ServerClientPoolMetrics(meter), NullLogger<ServerClientPool>.Instance);
        await pool.DisposeAsync();

        await AssertAllDisposedAsync(created, 2);
    }

    /// <summary>Handlers from the peer handler factory stay owned by the factory's caller.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeKeepsFactoryHandlers(CancellationToken cancellationToken)
    {
        using var meter = new Meter("Squirix");
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = LoadCertificate(bundle);
        var supplied = new List<TrackingHandler>();
        try
        {
            var args = MtlsArgs(certificate, _ => Track(supplied), null);
            var pool = new ServerClientPool(BuildPeers(2), args, new ServerClientPoolMetrics(meter), NullLogger<ServerClientPool>.Instance);
            await pool.DisposeAsync();

            _ = await Assert.That(supplied.Count).IsEqualTo(2);
            for (var i = 0; i < supplied.Count; i++)
                _ = await Assert.That(supplied[i].Disposed).IsFalse();
        }
        finally
        {
            for (var i = 0; i < supplied.Count; i++)
                supplied[i].Dispose();
        }
    }

    private static async Task AssertAllDisposedAsync(List<TrackingHandler> created, int expectedCount)
    {
        _ = await Assert.That(created.Count).IsEqualTo(expectedCount);
        for (var i = 0; i < created.Count; i++)
            _ = await Assert.That(created[i].Disposed).IsTrue();
    }

    private static MtlsCertificate LoadCertificate(MtlsTestCertificateBundle bundle) =>
        MtlsCertificate.Load(new MtlsOptions { CaPath = bundle.CaPath, CertPfxPath = bundle.PfxPath, InternalListenPort = 6601 }, 6001, true, "node-a");

    private static ServerClientPoolArgs MtlsArgs(
        MtlsCertificate certificate,
        Func<string, HttpMessageHandler>? peerHandlerFactory,
        Func<MtlsCertificate?, string, HttpMessageHandler>? ownedHandlerFactory) => new()
        {
            PolicyFactory = static _ => new RecordingPolicy(null),
            PeerHandlerFactory = peerHandlerFactory,
            OwnedHandlerFactory = ownedHandlerFactory,
            MtlsOptions = new MtlsOptions { InternalListenPort = 6601 },
            Certificate = certificate,
            InterNodeMtlsEnabled = true,
        };

    private static TrackingHandler Track(List<TrackingHandler> created)
    {
        var handler = new TrackingHandler();
        created.Add(handler);
        return handler;
    }

    private static ServerPeer[] BuildPeers(int count)
    {
        var peers = new ServerPeer[count];
        for (var i = 0; i < count; i++)
            peers[i] = new ServerPeer { NodeId = $"n{NodeInvariantIndexStrings.Format(i)}", Uri = new Uri(NodeInvariantIndexStrings.FormatHttpsOrigin("localhost", 6500 + i)) };

        return peers;
    }

    /// <summary>Records whether the owner disposed the handler; Rocks cannot observe the protected dispose overload.</summary>
    private sealed class TrackingHandler : DelegatingHandler
    {
        internal bool Disposed { get; private set; }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                Disposed = true;

            base.Dispose(disposing);
        }
    }

    private sealed class RecordingPolicy : IServerCallPolicy
    {
        private readonly Exception? _disposeError;

        internal RecordingPolicy(Exception? disposeError)
        {
            _disposeError = disposeError;
        }

        internal bool Disposed { get; private set; }

        public void BeginDrain()
        {
        }

        public ValueTask DisposeAsync()
        {
            Disposed = true;
            return _disposeError != null ? ValueTask.FromException(_disposeError) : ValueTask.CompletedTask;
        }

        public ValueTask<T> ExecuteAsync<TState, T>(TState state, Func<TState, CancellationToken, ValueTask<T>> action, CancellationToken cancellationToken) =>
            throw new NotSupportedException("RecordingPolicy does not execute calls.");
    }
}
