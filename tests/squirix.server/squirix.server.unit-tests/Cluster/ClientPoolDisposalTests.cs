using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Logging;
using Squirix.Server.Node.Observability;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Logging;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using Squirix.Testing;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire.Codec;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster;

/// <summary>Pool shutdown disposal: best-effort drain across peers, and disposal of the peer clients the pool owns.</summary>
[Immutable]
public sealed class ClientPoolDisposalTests : DisposableServerUnitTestBase
{
    /// <summary>The clients the pool creates per peer: one for the forward channel and one for the lease channel.</summary>
    private const int ClientsPerPeer = 2;

    private const int DrainTimedOutEventId = 5003;

    private const int LeaseCancelFailedEventId = 5006;

    private const int MaterialLeakedEventId = 5005;

    private const int MaterialReleasedLateEventId = 5007;

    private const string PoolDisposalsTotalInstrumentName = "squirix_peer_pool_disposals_total";

    /// <summary>A socket admitted after abort is closed before TLS starts and releases its gate entry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AbortBeforeHandshakeLeavesGate(CancellationToken cancellationToken)
    {
        using var peer = new NativePeerFixture();
        var connections = new TrackedConnections();
        connections.AbortAll();
        var connect = TrackedConnectionStream.ConnectAsync(connections, peer.Endpoint, cancellationToken).AsTask();

        _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(connect, TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);
        await connections.WaitAsync(cancellationToken);
        _ = await Assert.That(connections.Pending).IsEqualTo(0);
    }

    /// <summary>Aborting closes the socket immediately while its owning stream retains the gate entry until disposal.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AbortKeepsSocketGateUntilDisposed(CancellationToken cancellationToken)
    {
        using var peer = new NativePeerFixture();
        var connections = new TrackedConnections();
        await using var stream = await TrackedConnectionStream.ConnectAsync(connections, peer.Endpoint, cancellationToken);
        using var accepted = await peer.AcceptTcpAsync(cancellationToken);
        _ = await Assert.That(connections.Pending).IsEqualTo(1);

        connections.AbortAll();

        var received = await accepted.GetStream().ReadAsync(new byte[1], cancellationToken);
        _ = await Assert.That(received).IsEqualTo(0);
        _ = await Assert.That(connections.Pending).IsEqualTo(1);
        await stream.DisposeAsync();
        _ = await Assert.That(connections.Pending).IsEqualTo(0);
    }

    /// <summary>A call drain that uses up the whole shutdown budget still leaves the aborted connections a short grace to dispose: no leak is reported and the hold is released.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AbortedConnectionsGetGrace(CancellationToken cancellationToken)
    {
        using var meter = new Meter("Squirix");
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var nodeCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-a");
        using var certificate = MtlsCertificate.Create(nodeCertificate, bundle.Ca);
        var log = new EventRecordingLogger();
        var clock = new FakeTimeProvider();
        var created = new List<TrackingClient>();
        TrackedConnections? tracked = null;
        var args = new ServerClientPoolArgs
        {
            PolicyFactory = static _ => new RecordingPolicy(null),
            MtlsOptions = new MtlsOptions { InternalListenPort = 6601 },
            Certificate = certificate,
            InterNodeMtlsEnabled = true,
            ShutdownBudget = TimeSpan.FromSeconds(10),
            TimeProvider = clock,
        };
        args = WithClients(
            args,
            connections =>
            {
                tracked = connections;
                return OpenConnection(connections, created);
            });
        var pool = new ServerClientPool(BuildPeers(1), args, new ServerClientPoolMetrics(meter), log, log);
        var lease = pool.LeaseChannel("n0", cancellationToken);
        Task disposing;
        try
        {
            disposing = pool.DisposeAsync().AsTask();
            await AwaitCancellationAsync(lease.Token, cancellationToken);
            clock.Advance(TimeSpan.FromSeconds(10));
        }
        finally
        {
            lease.Dispose();
        }

        // The released lease lets the call drain finish; the budget is already gone, so only the grace is left for the connection.
        _ = await Assert.That(disposing.IsCompleted).IsFalse();
        for (var i = 0; i < ClientsPerPeer; i++)
            tracked!.Exit();
        await disposing.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);

        _ = await Assert.That(log.Find(MaterialLeakedEventId)).IsNull();
        certificate.ReleaseHold();
        _ = await Assert.That(certificate.IsReleased).IsTrue();
    }

    /// <summary>A default native dial registers its socket and releases it after a failed handshake.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DefaultDialIsTracked(CancellationToken cancellationToken)
    {
        using var peer = new NativePeerFixture();
        await using var pool = peer.CreatePool();
        var ping = NativePeerFixture.PingAsync(pool.ForNode(NativePeerFixture.Node), cancellationToken);
        using var accepted = await peer.AcceptTcpAsync(cancellationToken);
        await WaitForHandshakeStartAsync(accepted, cancellationToken);
        _ = await Assert.That(pool.OpenConnections).IsEqualTo(1);

        accepted.Dispose();
        _ = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(ping, TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await Assert.That(pool.OpenConnections).IsEqualTo(0);
    }

    /// <summary>Disposal refuses new channel leases and cancels the leased calls, then waits for the leases to end before it disposes the channels.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeAwaitsLeasedCalls(CancellationToken cancellationToken)
    {
        using var meter = new Meter("Squirix");
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = LoadCertificate(bundle);
        var created = new List<TrackingClient>();
        var pool = new ServerClientPool(
            BuildPeers(1),
            MtlsArgs(certificate, _ => Track(created)),
            new ServerClientPoolMetrics(meter),
            NullLogger<ServerClientPool>.Instance,
            NullLogger<PeerHandshakeVerifier>.Instance);
        var lease = pool.LeaseChannel("n0", cancellationToken);
        Task disposing;
        try
        {
            disposing = pool.DisposeAsync().AsTask();

            _ = await Assert.That(disposing.IsCompleted).IsFalse();
            await AwaitCancellationAsync(lease.Token, cancellationToken);
            _ = await Assert.That(disposing.IsCompleted).IsFalse();
            _ = await Assert.That(created[0].Disposed).IsFalse();
            _ = NodeExceptionAssert.For<ObjectDisposedException>().Throws(pool, static p => _ = p.LeaseChannel("n0", CancellationToken.None));
        }
        finally
        {
            lease.Dispose();
        }

        await disposing.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);

        await AssertAllDisposedAsync(created, 1);
        _ = await Assert.That(certificate.IsReleased).IsFalse();
        certificate.ReleaseHold();
        _ = await Assert.That(certificate.IsReleased).IsTrue();
    }

    /// <summary>
    /// A peer whose drain never ends does not hold disposal past the shutdown budget: the timeout is logged and every channel is still
    /// disposed.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeBoundedByShutdownBudget(CancellationToken cancellationToken)
    {
        using var meter = new Meter("Squirix");
        var log = new EventRecordingLogger();
        var clock = new FakeTimeProvider();
        var stuck = new StuckPolicy();
        var healthy = new RecordingPolicy(null);
        try
        {
            var created = new List<TrackingClient>();
            var args = new ServerClientPoolArgs
            {
                PolicyFactory = nodeId => string.Equals(nodeId, "n0", StringComparison.Ordinal) ? stuck : healthy,
                ShutdownBudget = TimeSpan.FromSeconds(10),
                TimeProvider = clock,
            };
            args = WithClients(args, _ => Track(created));
            var pool = new ServerClientPool(BuildPeers(2), args, new ServerClientPoolMetrics(meter), log, log);

            var disposing = pool.DisposeAsync().AsTask();
            _ = await Assert.That(disposing.IsCompleted).IsFalse();
            _ = await Assert.That(healthy.Disposed).IsTrue();

            clock.Advance(TimeSpan.FromSeconds(10));
            await disposing.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);

            await AssertAllDisposedAsync(created, 2);
            _ = await Assert.That(log.Find(DrainTimedOutEventId)?.Level).IsEqualTo(ServerLogLevel.Warning);
            _ = await Assert.That(log.FindMessage(DrainTimedOutEventId)).Contains("peers still busy: n0.", StringComparison.Ordinal);
        }
        finally
        {
            stuck.Release();
            await stuck.DisposeAsync();
            await healthy.DisposeAsync();
        }
    }

    /// <summary>An unexpected policy disposal failure must not leak the remaining peers or channels.</summary>
    [Test]
    public async Task DisposeContinuesAfterPolicyFailure()
    {
        using var meter = new Meter("Squirix");
        using var sink = new MeasurementSink(meter);
        var policies = new Dictionary<string, RecordingPolicy>(StringComparer.Ordinal)
        {
            ["n0"] = new(new InvalidOperationException("Unexpected policy disposal failure.")),
            ["n1"] = new(null),
            ["n2"] = new(null),
        };
        var pool = new ServerClientPool(
            BuildPeers(3),
            new ServerClientPoolArgs { TimeProvider = TimeProvider.System, PolicyFactory = nodeId => policies[nodeId] },
            new ServerClientPoolMetrics(meter),
            NullLogger<ServerClientPool>.Instance,
            NullLogger<PeerHandshakeVerifier>.Instance);
        await using (pool)
        {
            await pool.DisposeAsync();

            _ = await Assert.That(policies["n0"].Disposed).IsTrue();
            _ = await Assert.That(policies["n1"].Disposed).IsTrue();
            _ = await Assert.That(policies["n2"].Disposed).IsTrue();
            _ = await Assert.That(sink.HasEvent(PoolDisposalsTotalInstrumentName)).IsTrue();
        }
    }

    /// <summary>A policy whose disposal throws before returning its task must not leak the remaining peers or channels either.</summary>
    [Test]
    public async Task DisposeContinuesAfterPolicyThrow()
    {
        using var meter = new Meter("Squirix");
        using var sink = new MeasurementSink(meter);
        var second = new RecordingPolicy(null);
        var third = new RecordingPolicy(null);
        var policies = new Dictionary<string, IServerCallPolicy>(StringComparer.Ordinal)
        {
            ["n0"] = new ThrowingPolicy(new InvalidOperationException("Policy disposal threw.")),
            ["n1"] = second,
            ["n2"] = third,
        };
        var pool = new ServerClientPool(
            BuildPeers(3),
            new ServerClientPoolArgs { TimeProvider = TimeProvider.System, PolicyFactory = nodeId => policies[nodeId] },
            new ServerClientPoolMetrics(meter),
            NullLogger<ServerClientPool>.Instance,
            NullLogger<PeerHandshakeVerifier>.Instance);

        await pool.DisposeAsync();

        _ = await Assert.That(second.Disposed).IsTrue();
        _ = await Assert.That(third.Disposed).IsTrue();
        _ = await Assert.That(sink.HasEvent(PoolDisposalsTotalInstrumentName)).IsTrue();
    }

    /// <summary>Pool disposal aborts a registered socket during the TLS handshake and releases its certificate hold.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeDrainsRedirectedConnections(CancellationToken cancellationToken)
    {
        using var peer = new NativePeerFixture();
        await using var pool = peer.CreatePool(resolve: _ => ValueTask.FromResult<EndPoint>(peer.Endpoint));
        var ping = NativePeerFixture.PingAsync(pool.ForNode(NativePeerFixture.Node), cancellationToken);
        using var accepted = await peer.AcceptTcpAsync(cancellationToken);
        await WaitForHandshakeStartAsync(accepted, cancellationToken);
        _ = await Assert.That(pool.OpenConnections).IsEqualTo(1);
        peer.Material.ReleaseHold();
        _ = await Assert.That(peer.Material.IsReleased).IsFalse();

        await pool.DisposeAsync();

        _ = await Assert.That(pool.OpenConnections).IsEqualTo(0);
        _ = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(ping, TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await Assert.That(peer.Material.IsReleased).IsTrue();
    }

    /// <summary>Clients supplied to the pool stay owned by the factory's caller.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeKeepsExternallyOwnedClients(CancellationToken cancellationToken)
    {
        using var meter = new Meter("Squirix");
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = LoadCertificate(bundle);
        var supplied = new List<TrackingClient>();
        try
        {
            var args = MtlsArgs(certificate, _ => Track(supplied), false);
            var pool = new ServerClientPool(
                BuildPeers(2),
                args,
                new ServerClientPoolMetrics(meter),
                NullLogger<ServerClientPool>.Instance,
                NullLogger<PeerHandshakeVerifier>.Instance);
            await pool.DisposeAsync();

            _ = await Assert.That(supplied.Count).IsEqualTo(2 * ClientsPerPeer);
            for (var i = 0; i < supplied.Count; i++)
                _ = await Assert.That(supplied[i].Disposed).IsFalse();
        }
        finally
        {
            for (var i = 0; i < supplied.Count; i++)
                supplied[i].Dispose();
        }
    }

    /// <summary>A connection that outlives the shutdown budget keeps the material loaded: disposal returns, logs the leak and never frees the certificates.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeKeepsMaterialPastBudget(CancellationToken cancellationToken)
    {
        using var meter = new Meter("Squirix");
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var nodeCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-a");
        using var certificate = MtlsCertificate.Create(nodeCertificate, bundle.Ca);
        var log = new EventRecordingLogger();
        var clock = new FakeTimeProvider();
        var created = new List<TrackingClient>();
        var args = new ServerClientPoolArgs
        {
            PolicyFactory = static _ => new RecordingPolicy(null),
            MtlsOptions = new MtlsOptions { InternalListenPort = 6601 },
            Certificate = certificate,
            InterNodeMtlsEnabled = true,
            ShutdownBudget = TimeSpan.FromSeconds(10),
            TimeProvider = clock,
        };
        args = WithClients(args, connections => OpenConnection(connections, created));
        var pool = new ServerClientPool(BuildPeers(1), args, new ServerClientPoolMetrics(meter), log, log);

        var disposing = pool.DisposeAsync().AsTask();
        _ = await Assert.That(disposing.IsCompleted).IsFalse();
        await AssertAllDisposedAsync(created, 1);

        await AdvanceUntilCompletedAsync(clock, disposing, cancellationToken);

        _ = await Assert.That(log.Find(MaterialLeakedEventId)?.Level).IsEqualTo(ServerLogLevel.Error);
        _ = await Assert.That(log.FindMessage(MaterialLeakedEventId)).Contains($"{ClientsPerPeer} connections were still open", StringComparison.Ordinal);
        certificate.ReleaseHold();
        _ = await Assert.That(certificate.IsReleased).IsFalse();
        _ = await Assert.That(certificate.NodeCertificate!.Handle).IsNotEqualTo(nint.Zero);
    }

    /// <summary>Disposing the pool must dispose every default client the pool created for its peers.</summary>
    [Test]
    public async Task DisposeReleasesOwnedClients()
    {
        using var meter = new Meter("Squirix");
        var created = new List<TrackingClient>();
        var args = new ServerClientPoolArgs
        {
            TimeProvider = TimeProvider.System,
            PolicyFactory = static _ => new RecordingPolicy(null),
        };
        args = WithClients(args, _ => Track(created));
        var pool = new ServerClientPool(BuildPeers(3), args, new ServerClientPoolMetrics(meter), NullLogger<ServerClientPool>.Instance, NullLogger<PeerHandshakeVerifier>.Instance);
        await pool.DisposeAsync();

        await AssertAllDisposedAsync(created, 3);
    }

    /// <summary>Disposing the pool must dispose every mTLS client the pool created from the node certificate.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeReleasesOwnedMtlsClients(CancellationToken cancellationToken)
    {
        using var meter = new Meter("Squirix");
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = LoadCertificate(bundle);
        var created = new List<TrackingClient>();
        var args = MtlsArgs(certificate, _ => Track(created));
        var pool = new ServerClientPool(BuildPeers(2), args, new ServerClientPoolMetrics(meter), NullLogger<ServerClientPool>.Instance, NullLogger<PeerHandshakeVerifier>.Instance);
        await pool.DisposeAsync();

        await AssertAllDisposedAsync(created, 2);
    }

    /// <summary>A callback registered on a lease token that throws does not stop disposal: the channels are disposed and the material hold is released.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeSurvivesThrowingLeaseCallback(CancellationToken cancellationToken)
    {
        using var meter = new Meter("Squirix");
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = LoadCertificate(bundle);
        var log = new EventRecordingLogger();
        var created = new List<TrackingClient>();
        var pool = new ServerClientPool(BuildPeers(1), MtlsArgs(certificate, _ => Track(created)), new ServerClientPoolMetrics(meter), log, log);
        var lease = pool.LeaseChannel("n0", cancellationToken);
        Task disposing;
        try
        {
            await using var registration = lease.Token.Register(static () => throw new InvalidOperationException("callback failure"));
            disposing = pool.DisposeAsync().AsTask();
            await AwaitCancellationAsync(lease.Token, cancellationToken);
        }
        finally
        {
            lease.Dispose();
        }

        await disposing.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);

        await AssertAllDisposedAsync(created, 1);
        _ = await Assert.That(log.Find(LeaseCancelFailedEventId)?.Level).IsEqualTo(ServerLogLevel.Warning);
        certificate.ReleaseHold();
        _ = await Assert.That(certificate.IsReleased).IsTrue().Because("The pool must have released its hold despite the throwing callback.");
    }

    /// <summary>The resolver sends the dial to its loopback peer, where socket tracking lasts through the TLS handshake.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EndpointResolverRedirectsTrackedDial(CancellationToken cancellationToken)
    {
        using var peer = new NativePeerFixture();
        await using var pool = peer.CreatePool(resolve: _ => ValueTask.FromResult<EndPoint>(peer.Endpoint), target: new Uri("https://127.0.0.1:1"));
        var ping = NativePeerFixture.PingAsync(pool.ForNode(NativePeerFixture.Node), cancellationToken);
        using var accepted = await peer.AcceptTcpAsync(cancellationToken);
        await WaitForHandshakeStartAsync(accepted, cancellationToken);
        _ = await Assert.That(pool.OpenConnections).IsEqualTo(1);

        accepted.Dispose();
        _ = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(ping, TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await Assert.That(pool.OpenConnections).IsEqualTo(0);
    }

    /// <summary>A shutdown budget that is negative but not infinite, or above the longest accepted timeout, is rejected.</summary>
    /// <param name="budgetMilliseconds">The budget in milliseconds.</param>
    [Test]
    [Arguments(-2.0)]
    [Arguments(4_294_967_296.0)]
    public async Task InvalidShutdownBudgetThrows(double budgetMilliseconds)
    {
        using var meter = new Meter("Squirix");
        var args = new ServerClientPoolArgs
        {
            TimeProvider = TimeProvider.System,
            PolicyFactory = static _ => new RecordingPolicy(null),
            ShutdownBudget = TimeSpan.FromMilliseconds(budgetMilliseconds),
        };
        var metrics = new ServerClientPoolMetrics(meter);

        var failure = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(
            (args, metrics),
            static state => _ = new ServerClientPool(BuildPeers(1), state.args, state.metrics, NullLogger<ServerClientPool>.Instance, NullLogger<PeerHandshakeVerifier>.Instance));

        _ = await Assert.That(failure.ParamName).IsEqualTo("args");
    }

    /// <summary>Once the connections that outlived the budget are gone, the pool releases its hold late and the material is freed with the loader's release.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LeakedMaterialReleasedOnDrain(CancellationToken cancellationToken)
    {
        using var meter = new Meter("Squirix");
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var nodeCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-a");
        using var certificate = MtlsCertificate.Create(nodeCertificate, bundle.Ca);
        var log = new EventRecordingLogger();
        var clock = new FakeTimeProvider();
        var created = new List<TrackingClient>();
        TrackedConnections? tracked = null;
        var args = new ServerClientPoolArgs
        {
            PolicyFactory = static _ => new RecordingPolicy(null),
            MtlsOptions = new MtlsOptions { InternalListenPort = 6601 },
            Certificate = certificate,
            InterNodeMtlsEnabled = true,
            ShutdownBudget = TimeSpan.FromSeconds(10),
            TimeProvider = clock,
        };
        args = WithClients(
            args,
            connections =>
            {
                tracked = connections;
                return OpenConnection(connections, created);
            });
        var pool = new ServerClientPool(BuildPeers(1), args, new ServerClientPoolMetrics(meter), log, log);
        var disposing = pool.DisposeAsync().AsTask();
        await AdvanceUntilCompletedAsync(clock, disposing, cancellationToken);
        certificate.ReleaseHold();
        _ = await Assert.That(log.Find(MaterialLeakedEventId)?.Level).IsEqualTo(ServerLogLevel.Error);
        _ = await Assert.That(certificate.IsReleased).IsFalse();
        var connections = ThrowHelper.Required(tracked, "Expected the pool to create its owned client.");
        _ = await Assert.That(connections.LateRelease.IsCompleted).IsFalse();

        for (var i = 0; i < ClientsPerPeer; i++)
            connections.Exit();
        await connections.LateRelease.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);

        _ = await Assert.That(certificate.IsReleased).IsTrue();
        _ = await Assert.That(log.Find(MaterialReleasedLateEventId)?.Level).IsEqualTo(ServerLogLevel.Information);
        _ = await Assert.That(certificate.NodeCertificate!.Handle).IsEqualTo(nint.Zero);
    }

    /// <summary>A lease for a caller token that cannot cancel still cancels when the pool disposes, and the caller token's own cancellation reaches a lease that links it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LeaseTokenFollowsCallerAndDispose(CancellationToken cancellationToken)
    {
        using var meter = new Meter("Squirix");
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = LoadCertificate(bundle);
        var created = new List<TrackingClient>();
        var pool = new ServerClientPool(
            BuildPeers(1),
            MtlsArgs(certificate, _ => Track(created)),
            new ServerClientPoolMetrics(meter),
            NullLogger<ServerClientPool>.Instance,
            NullLogger<PeerHandshakeVerifier>.Instance);
        using var caller = new CancellationTokenSource();
        var plain = pool.LeaseChannel("n0", CancellationToken.None);
        var linked = pool.LeaseChannel("n0", caller.Token);
        Task disposing;
        try
        {
            var plainCancelable = plain.Token.CanBeCanceled;
            var plainCanceledEarly = plain.Token.IsCancellationRequested;
            await caller.CancelAsync();
            var linkedCanceledByCaller = linked.Token.IsCancellationRequested;
            var plainCanceledByCaller = plain.Token.IsCancellationRequested;

            _ = await Assert.That(plainCancelable).IsTrue();
            _ = await Assert.That(plainCanceledEarly).IsFalse();
            _ = await Assert.That(linkedCanceledByCaller).IsTrue();
            _ = await Assert.That(plainCanceledByCaller).IsFalse();

            disposing = pool.DisposeAsync().AsTask();
            await AwaitCancellationAsync(plain.Token, cancellationToken);
        }
        finally
        {
            plain.Dispose();
            linked.Dispose();
        }

        await disposing.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);

        await AssertAllDisposedAsync(created, 1);
        certificate.ReleaseHold();
    }

    /// <summary>Two pools using the same endpoint resolver own separate sockets and drain independently.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PoolsKeepIndependentConnectionGates(CancellationToken cancellationToken)
    {
        using var peer = new NativePeerFixture();
        Func<CancellationToken, ValueTask<EndPoint>> resolve = _ => ValueTask.FromResult<EndPoint>(peer.Endpoint);
        await using var first = peer.CreatePool(resolve: resolve);
        await using var second = peer.CreatePool(resolve: resolve);
        var one = NativePeerFixture.PingAsync(first.ForNode(NativePeerFixture.Node), cancellationToken);
        using var accepted = await peer.AcceptTcpAsync(cancellationToken);
        await WaitForHandshakeStartAsync(accepted, cancellationToken);
        var two = NativePeerFixture.PingAsync(second.ForNode(NativePeerFixture.Node), cancellationToken);
        await using var stream = await peer.AcceptTlsAsync(cancellationToken);
        _ = await NativePeerFixture.ReadAsync(stream, cancellationToken);
        _ = await Assert.That(first.OpenConnections).IsEqualTo(1);
        await first.DisposeAsync();
        _ = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(one, TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await Assert.That(first.OpenConnections).IsEqualTo(0);
        _ = await Assert.That(second.OpenConnections).IsEqualTo(1);
        await NativePeerFixture.ReplyAsync(stream, cancellationToken);
        _ = await two;
    }

    /// <summary>A caller cancellation reaches the configured resolver before a socket is acquired.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RedirectedDialHonorsCallerCancellation(CancellationToken cancellationToken)
    {
        using var peer = new NativePeerFixture();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var endpoint = new TaskCompletionSource<EndPoint>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var caller = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        await using var pool = peer.CreatePool(
            TimeSpan.FromMinutes(1),
            async ct =>
            {
                _ = entered.TrySetResult();
                return await endpoint.Task.WaitAsync(ct);
            });
        var call = NativePeerFixture.PingAsync(pool.ForNode(NativePeerFixture.Node), caller.Token);
        await entered.Task.WaitAsync(cancellationToken);
        await caller.CancelAsync();

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(call, TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);
        _ = await Assert.That(pool.OpenConnections).IsEqualTo(0);
    }

    /// <summary>A registration failure releases the material hold and disposes the channels created before it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RegistrationFailureReleasesHolds(CancellationToken cancellationToken)
    {
        using var meter = new Meter("Squirix");
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = LoadCertificate(bundle);
        var created = new List<TrackingClient>();
        var calls = 0;
        var args = new ServerClientPoolArgs
        {
            TimeProvider = TimeProvider.System,
            PolicyFactory = _ => ++calls == 2 ? throw new InvalidOperationException("policy failure") : new RecordingPolicy(null),
            MtlsOptions = new MtlsOptions { InternalListenPort = 6601 },
            Certificate = certificate,
            InterNodeMtlsEnabled = true,
        };
        args = WithClients(args, _ => Track(created));
        var metrics = new ServerClientPoolMetrics(meter);
        var peers = BuildPeers(2);

        _ = NodeExceptionAssert.For<InvalidOperationException>().Throws(
            (peers, args, metrics),
            static state => _ = new ServerClientPool(state.peers, state.args, state.metrics, NullLogger<ServerClientPool>.Instance, NullLogger<PeerHandshakeVerifier>.Instance));

        await AssertAllDisposedAsync(created, 2);
        _ = await Assert.That(certificate.IsReleased).IsFalse();
        certificate.ReleaseHold();
        _ = await Assert.That(certificate.IsReleased).IsTrue();
    }

    /// <summary>An injected in-memory client acquires no socket entry.</summary>
    [Test]
    public async Task SocketlessPeerNeedsNoConnectionEntry()
    {
        using var meter = new Meter("Squirix");
        var args = new ServerClientPoolArgs { PolicyFactory = static _ => new RecordingPolicy(null), TimeProvider = TimeProvider.System };
        args = WithClients(args, static _ => new TrackingClient());
        await using var pool = new ServerClientPool(BuildPeers(1), args, new ServerClientPoolMetrics(meter), NullLogger<ServerClientPool>.Instance, NullLogger.Instance);

        _ = await Assert.That(pool.ForNode("n0")).IsTypeOf<TrackingClient>();
        _ = await Assert.That(pool.OpenConnections).IsEqualTo(0);
    }

    /// <summary>Advances the fake clock until <paramref name="task" /> completes; the wait timers register at unpredictable moments, so one advance is not enough.</summary>
    /// <param name="clock">The fake clock.</param>
    /// <param name="task">The task to complete.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    private static async Task AdvanceUntilCompletedAsync(FakeTimeProvider clock, Task task, CancellationToken cancellationToken)
    {
        var deadline = TimeProvider.System.GetTimestamp() + (5 * TimeProvider.System.TimestampFrequency);
        while (!task.IsCompleted && TimeProvider.System.GetTimestamp() < deadline)
        {
            clock.Advance(TimeSpan.FromSeconds(10));
            await Task.Delay(1, cancellationToken);
        }

        await task.WaitAsync(TimeSpan.FromSeconds(1), TimeProvider.System, cancellationToken);
    }

    /// <summary>Asserts that the pool created and disposed the clients of both channels of every peer.</summary>
    /// <param name="created">The clients the pool created.</param>
    /// <param name="peers">The number of peers.</param>
    /// <returns>An asynchronous operation.</returns>
    private static async Task AssertAllDisposedAsync(List<TrackingClient> created, int peers)
    {
        _ = await Assert.That(created.Count).IsEqualTo(peers * ClientsPerPeer);
        for (var i = 0; i < created.Count; i++)
            _ = await Assert.That(created[i].Disposed).IsTrue();
    }

    /// <summary>Waits until <paramref name="token" /> is cancelled; the pool cancels leased calls through callbacks that run off the disposing thread.</summary>
    /// <param name="token">The lease token.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    private static async Task AwaitCancellationAsync(CancellationToken token, CancellationToken cancellationToken)
    {
        var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using (token.UnsafeRegister(static state => Complete(state), cancelled))
            await cancelled.Task.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);
    }

    private static ServerPeer[] BuildPeers(int count)
    {
        var peers = new ServerPeer[count];
        for (var i = 0; i < count; i++)
            peers[i] = new ServerPeer { NodeId = $"n{NodeInvariantIndexStrings.Format(i)}", Uri = new Uri(NodeInvariantIndexStrings.FormatHttpsOrigin("localhost", 6500 + i)) };

        return peers;
    }

    private static void Complete(object? state)
    {
        if (state is TaskCompletionSource source)
            _ = source.TrySetResult();
    }

    private static MtlsCertificate LoadCertificate(MtlsTestCertificateBundle bundle) => MtlsCertificate.Load(
        new MtlsOptions { CaPath = bundle.CaPath, CertPfxPath = bundle.PfxPath, InternalListenPort = 6601 },
        6001,
        true,
        "node-a");

    private static ServerClientPoolArgs MtlsArgs(MtlsCertificate certificate, Func<TrackedConnections, TrackingClient> create, bool owned = true)
    {
        var args = new ServerClientPoolArgs
        {
            TimeProvider = TimeProvider.System,
            PolicyFactory = static _ => new RecordingPolicy(null),
            MtlsOptions = new MtlsOptions { InternalListenPort = 6601 },
            Certificate = certificate,
            InterNodeMtlsEnabled = true,
        };
        return WithClients(args, create, owned);
    }

    /// <summary>Creates a tracked client with one connection that never closes, as a handshake stalled past the shutdown budget.</summary>
    /// <param name="connections">The pool's connection gate.</param>
    /// <param name="created">The clients created so far.</param>
    /// <returns>The client.</returns>
    private static TrackingClient OpenConnection(TrackedConnections connections, List<TrackingClient> created)
    {
        connections.Enter();
        return Track(created);
    }

    private static TrackingClient Track(List<TrackingClient> created)
    {
        var client = new TrackingClient();
        created.Add(client);
        return client;
    }

    /// <summary>Waits until the client sent its first TLS flight on <paramref name="accepted" />; the handshake then stalls on the silent peer.</summary>
    /// <param name="accepted">The connection the silent listener accepted.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    private static async Task WaitForHandshakeStartAsync(TcpClient accepted, CancellationToken cancellationToken)
    {
        var rented = ArrayPool<byte>.Shared.Rent(1);
        try
        {
            var read = await accepted.GetStream().ReadAsync(new Memory<byte>(rented, 0, 1), cancellationToken).AsTask()
                                     .WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);
            _ = await Assert.That(read).IsGreaterThan(0);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    private static ServerClientPoolArgs WithClients(ServerClientPoolArgs args, Func<TrackedConnections, TrackingClient> create, bool owned = true) => args with
    {
        ChannelsOverride = (_, connections) =>
        {
            var forward = create(connections);
            var replication = create(connections);
            return new PeerChannels(forward, replication, owned ? forward : null, owned ? replication : null);
        },
    };

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

        public ValueTask<T> ExecuteAsync<TState, T>(TState state, Func<TState, CancellationToken, ValueTask<T>> action, CancellationToken cancellationToken, ServerDeadlineBudget? deadlineBudget = null) =>
            throw new NotSupportedException("RecordingPolicy does not execute calls.");
    }

    /// <summary>A policy whose drain never completes, as a peer with an in-flight call that ignores cancellation.</summary>
    private sealed class StuckPolicy : IServerCallPolicy
    {
        private readonly TaskCompletionSource _drained = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void BeginDrain()
        {
        }

        public ValueTask DisposeAsync() => new(_drained.Task);

        public ValueTask<T> ExecuteAsync<TState, T>(TState state, Func<TState, CancellationToken, ValueTask<T>> action, CancellationToken cancellationToken, ServerDeadlineBudget? deadlineBudget = null) =>
            throw new NotSupportedException("StuckPolicy does not execute calls.");

        internal void Release() => _ = _drained.TrySetResult();
    }

    /// <summary>A policy whose disposal throws before returning its task.</summary>
    private sealed class ThrowingPolicy : IServerCallPolicy
    {
        private readonly Exception _failure;

        internal ThrowingPolicy(Exception failure)
        {
            _failure = failure;
        }

        public void BeginDrain()
        {
        }

        public ValueTask DisposeAsync() => throw _failure;

        public ValueTask<T> ExecuteAsync<TState, T>(TState state, Func<TState, CancellationToken, ValueTask<T>> action, CancellationToken cancellationToken, ServerDeadlineBudget? deadlineBudget = null) =>
            throw new NotSupportedException("ThrowingPolicy does not execute calls.");
    }

    private sealed class TrackingClient : IPeerClient, IDisposable
    {
        internal bool Disposed { get; private set; }

        public void Dispose() => Disposed = true;

        public Task<T> SendAsync<T>(PeerOperation operation, WireMessage request, WireParser<T> parser, CancellationToken cancellationToken)
            where T : WireMessage => throw new NotSupportedException();
    }
}
