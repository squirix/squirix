using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
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
    private const int DrainTimedOutEventId = 5003;

    private const int LeaseCancelFailedEventId = 5006;

    private const int MaterialLeakedEventId = 5005;

    private const int MaterialReleasedLateEventId = 5007;

    private const string PoolDisposalsTotalInstrumentName = "squirix_peer_pool_disposals_total";

    /// <summary>Disposal refuses new channel leases and cancels the leased calls, then waits for the leases to end before it disposes the channels.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeAwaitsLeasedCalls(CancellationToken cancellationToken)
    {
        using var meter = new Meter("Squirix");
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = LoadCertificate(bundle);
        var created = new List<TrackingHandler>();
        var pool = new ServerClientPool(BuildPeers(1), MtlsArgs(certificate, null, (_, _, _) => Track(created)), new ServerClientPoolMetrics(meter), NullLogger<ServerClientPool>.Instance);
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
        DisposeAsLoader(certificate);
        _ = await Assert.That(certificate.IsReleased).IsTrue();
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
        var created = new List<TrackingHandler>();
        var args = new ServerClientPoolArgs
        {
            PolicyFactory = static _ => new RecordingPolicy(null),
            OwnedHandlerFactory = (_, _, connections) => OpenConnection(connections, created),
            MtlsOptions = new MtlsOptions { InternalListenPort = 6601 },
            Certificate = certificate,
            InterNodeMtlsEnabled = true,
            ShutdownBudget = TimeSpan.FromSeconds(10),
            TimeProvider = clock,
        };
        var pool = new ServerClientPool(BuildPeers(1), args, new ServerClientPoolMetrics(meter), log);

        var disposing = pool.DisposeAsync().AsTask();
        _ = await Assert.That(disposing.IsCompleted).IsFalse();
        await AssertAllDisposedAsync(created, 1);

        await AdvanceUntilCompletedAsync(clock, disposing, cancellationToken);

        _ = await Assert.That(log.Find(MaterialLeakedEventId)?.Level).IsEqualTo(LogLevel.Error);
        _ = await Assert.That(log.FindMessage(MaterialLeakedEventId)).Contains("1 connections were still open", StringComparison.Ordinal);
        DisposeAsLoader(certificate);
        _ = await Assert.That(certificate.IsReleased).IsFalse();
        _ = await Assert.That(certificate.NodeCertificate!.Handle).IsNotEqualTo(nint.Zero);
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
        var created = new List<TrackingHandler>();
        TrackedConnections? tracked = null;
        var args = new ServerClientPoolArgs
        {
            PolicyFactory = static _ => new RecordingPolicy(null),
            OwnedHandlerFactory = (_, _, connections) =>
            {
                tracked = connections;
                return OpenConnection(connections, created);
            },
            MtlsOptions = new MtlsOptions { InternalListenPort = 6601 },
            Certificate = certificate,
            InterNodeMtlsEnabled = true,
            ShutdownBudget = TimeSpan.FromSeconds(10),
            TimeProvider = clock,
        };
        var pool = new ServerClientPool(BuildPeers(1), args, new ServerClientPoolMetrics(meter), log);
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
        tracked!.Exit();
        await disposing.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);

        _ = await Assert.That(log.Find(MaterialLeakedEventId)).IsNull();
        DisposeAsLoader(certificate);
        _ = await Assert.That(certificate.IsReleased).IsTrue();
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
        var created = new List<TrackingHandler>();
        TrackedConnections? tracked = null;
        var args = new ServerClientPoolArgs
        {
            PolicyFactory = static _ => new RecordingPolicy(null),
            OwnedHandlerFactory = (_, _, connections) =>
            {
                tracked = connections;
                return OpenConnection(connections, created);
            },
            MtlsOptions = new MtlsOptions { InternalListenPort = 6601 },
            Certificate = certificate,
            InterNodeMtlsEnabled = true,
            ShutdownBudget = TimeSpan.FromSeconds(10),
            TimeProvider = clock,
        };
        var pool = new ServerClientPool(BuildPeers(1), args, new ServerClientPoolMetrics(meter), log);
        var disposing = pool.DisposeAsync().AsTask();
        await AdvanceUntilCompletedAsync(clock, disposing, cancellationToken);
        DisposeAsLoader(certificate);
        _ = await Assert.That(log.Find(MaterialLeakedEventId)?.Level).IsEqualTo(LogLevel.Error);
        _ = await Assert.That(certificate.IsReleased).IsFalse();
        _ = await Assert.That(pool.LateMaterialRelease.IsCompleted).IsFalse();

        tracked!.Exit();
        await pool.LateMaterialRelease.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);

        _ = await Assert.That(certificate.IsReleased).IsTrue();
        _ = await Assert.That(log.Find(MaterialReleasedLateEventId)?.Level).IsEqualTo(LogLevel.Information);
        _ = await Assert.That(certificate.NodeCertificate!.Handle).IsEqualTo(nint.Zero);
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
        var created = new List<TrackingHandler>();
        var pool = new ServerClientPool(BuildPeers(1), MtlsArgs(certificate, null, (_, _, _) => Track(created)), new ServerClientPoolMetrics(meter), log);
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
        _ = await Assert.That(log.Find(LeaseCancelFailedEventId)?.Level).IsEqualTo(LogLevel.Warning);
        DisposeAsLoader(certificate);
        _ = await Assert.That(certificate.IsReleased).IsTrue().Because("The pool must have released its hold despite the throwing callback.");
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
            var created = new List<TrackingHandler>();
            var args = new ServerClientPoolArgs
            {
                PolicyFactory = nodeId => string.Equals(nodeId, "n0", StringComparison.Ordinal) ? stuck : healthy,
                OwnedHandlerFactory = (_, _, _) => Track(created),
                ShutdownBudget = TimeSpan.FromSeconds(10),
                TimeProvider = clock,
            };
            var pool = new ServerClientPool(BuildPeers(2), args, new ServerClientPoolMetrics(meter), log);

            var disposing = pool.DisposeAsync().AsTask();
            _ = await Assert.That(disposing.IsCompleted).IsFalse();
            _ = await Assert.That(healthy.Disposed).IsTrue();

            clock.Advance(TimeSpan.FromSeconds(10));
            await disposing.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);

            await AssertAllDisposedAsync(created, 2);
            _ = await Assert.That(log.Find(DrainTimedOutEventId)?.Level).IsEqualTo(LogLevel.Warning);
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

    /// <summary>A policy whose disposal throws before returning its task must not leak the remaining peers or channels either.</summary>
    [Test]
    public async Task DisposeContinuesAfterPolicyThrow()
    {
        using var meter = new Meter("Squirix");
        using var sink = new NodeMeasurementSink(meter);
        var second = new RecordingPolicy(null);
        var third = new RecordingPolicy(null);
        var policies = new Dictionary<string, IServerCallPolicy>(StringComparer.Ordinal)
        {
            ["n0"] = new ThrowingPolicy(new InvalidOperationException("Policy disposal threw.")),
            ["n1"] = second,
            ["n2"] = third,
        };
        var pool = new ServerClientPool(BuildPeers(3), new ServerClientPoolArgs { PolicyFactory = nodeId => policies[nodeId] }, new ServerClientPoolMetrics(meter), NullLogger<ServerClientPool>.Instance);

        await pool.DisposeAsync();

        _ = await Assert.That(second.Disposed).IsTrue();
        _ = await Assert.That(third.Disposed).IsTrue();
        _ = await Assert.That(sink.HasEvent(PoolDisposalsTotalInstrumentName)).IsTrue();
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
            OwnedHandlerFactory = (_, _, _) => Track(created),
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
        var args = MtlsArgs(certificate, null, (_, _, _) => Track(created));
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

    /// <summary>A connection of a handler the factory supplied counts in the pool's gate, and disposal aborts it and releases the material hold.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeDrainsFactoryHandlerConnections(CancellationToken cancellationToken)
    {
        using var meter = new Meter("Squirix");
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var certificate = LoadCertificate(bundle);
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var connected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var handler = new SocketsHttpHandler { ConnectCallback = (_, token) => ConnectSignallingAsync(listener, connected, token) };
        using var client = new HttpClient(handler, false);
        var args = MtlsArgs(certificate, _ => handler, null);
        var pool = new ServerClientPool(BuildPeers(1), args, new ServerClientPoolMetrics(meter), NullLogger<ServerClientPool>.Instance);

        // The listener never answers, so the TLS handshake stays open until the pool aborts the connection.
        var request = client.GetAsync(new Uri("https://localhost:1/"), cancellationToken);
        await connected.Task.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);
        _ = await Assert.That(pool.OpenConnections).IsEqualTo(1);

        await pool.DisposeAsync();

        _ = await Assert.That(pool.OpenConnections).IsEqualTo(0);
        _ = await NodeAsyncAssert.ThrowsAsync<HttpRequestException>(request);
        DisposeAsLoader(certificate);
        _ = await Assert.That(certificate.IsReleased).IsTrue();
    }

    /// <summary>A connect callback the factory handler already had keeps dialing, and its connection leaves the gate when the connection ends.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ComposedConnectCallbackStillRedirects(CancellationToken cancellationToken)
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var connections = new TrackedConnections();
        var connected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var handler = new SocketsHttpHandler { ConnectCallback = (_, token) => ConnectSignallingAsync(listener, connected, token) };
        ServerClientPool.TrackFactoryConnections(handler, connections);
        using var client = new HttpClient(handler, false);

        var request = client.GetAsync(new Uri("https://localhost:1/"), cancellationToken);
        using var accepted = await listener.AcceptTcpClientAsync(cancellationToken);
        await connected.Task.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);
        _ = await Assert.That(connections.Pending).IsEqualTo(1);

        // Ending the connection from the server side fails the handshake; the handler disposes the stream, which leaves the gate.
        accepted.Dispose();
        _ = await NodeAsyncAssert.ThrowsAsync<HttpRequestException>(request);
        await connections.WaitAsync(cancellationToken).AsTask().WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);

        _ = await Assert.That(connections.Pending).IsEqualTo(0);
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

    private static async Task AssertAllDisposedAsync(List<TrackingHandler> created, int expectedCount)
    {
        _ = await Assert.That(created.Count).IsEqualTo(expectedCount);
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

    private static async ValueTask<Stream> ConnectSignallingAsync(TcpListener listener, TaskCompletionSource connected, CancellationToken cancellationToken)
    {
        var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        try
        {
            await socket.ConnectAsync(listener.LocalEndpoint, cancellationToken);
            var stream = new NetworkStream(socket, true);
            socket = null;
            _ = connected.TrySetResult();
            return stream;
        }
        finally
        {
            socket?.Dispose();
        }
    }

    private static void Complete(object? state)
    {
        if (state is TaskCompletionSource source)
            _ = source.TrySetResult();
    }

    /// <summary>Releases the loader's hold the way the DI container does on host shutdown.</summary>
    /// <param name="material">The material.</param>
    private static void DisposeAsLoader(MtlsCertificate material)
    {
        IDisposable loader = material;
        loader.Dispose();
    }

    private static MtlsCertificate LoadCertificate(MtlsTestCertificateBundle bundle) =>
        MtlsCertificate.Load(new MtlsOptions { CaPath = bundle.CaPath, CertPfxPath = bundle.PfxPath, InternalListenPort = 6601 }, 6001, true, "node-a");

    private static ServerClientPoolArgs MtlsArgs(
        MtlsCertificate certificate,
        Func<string, HttpMessageHandler>? peerHandlerFactory,
        Func<MtlsCertificate?, string, TrackedConnections, HttpMessageHandler>? ownedHandlerFactory) => new()
        {
            PolicyFactory = static _ => new RecordingPolicy(null),
            PeerHandlerFactory = peerHandlerFactory,
            OwnedHandlerFactory = ownedHandlerFactory,
            MtlsOptions = new MtlsOptions { InternalListenPort = 6601 },
            Certificate = certificate,
            InterNodeMtlsEnabled = true,
        };

    /// <summary>Creates a tracked handler with one connection that never closes, as a handshake stalled past the shutdown budget.</summary>
    /// <param name="connections">The pool's connection gate.</param>
    /// <param name="created">The handlers created so far.</param>
    /// <returns>The handler.</returns>
    private static TrackingHandler OpenConnection(TrackedConnections connections, List<TrackingHandler> created)
    {
        connections.Enter();
        return Track(created);
    }

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

    /// <summary>A policy whose drain never completes, as a peer with an in-flight call that ignores cancellation.</summary>
    private sealed class StuckPolicy : IServerCallPolicy
    {
        private readonly TaskCompletionSource _drained = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void BeginDrain()
        {
        }

        public ValueTask DisposeAsync() => new(_drained.Task);

        public ValueTask<T> ExecuteAsync<TState, T>(TState state, Func<TState, CancellationToken, ValueTask<T>> action, CancellationToken cancellationToken) =>
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

        public ValueTask<T> ExecuteAsync<TState, T>(TState state, Func<TState, CancellationToken, ValueTask<T>> action, CancellationToken cancellationToken) =>
            throw new NotSupportedException("ThrowingPolicy does not execute calls.");
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
