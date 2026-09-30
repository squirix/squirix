using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Internal.Cluster.Reliability;
using Squirix.Internal.Cluster.Transport;
using Squirix.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests;

/// <summary><see cref="ClientPool" /> disposal of the HTTP handlers it owns.</summary>
[Immutable]
public sealed class ClientPoolDisposalTests
{
    private const string DisposeFailuresInstrumentName = "squirix_client_pool_dispose_failures_total";

    private static readonly Peer[] Peers =
    [
        new() { NodeId = "node-a", Uri = new Uri("https://127.0.0.1:6510") },
        new() { NodeId = "node-b", Uri = new Uri("https://127.0.0.1:6511") },
    ];

    /// <summary>A channel that fails to dispose is counted and does not stop the pool from releasing the other peers.</summary>
    [Test]
    public async Task ChannelFailureKeepsDisposingPeersAsync()
    {
        using var sink = new MeasurementSink("Squirix");
        var peers = CreatePeers("channel-dispose-failure");
        var created = new List<TrackingHandler>();
        var pool = new ClientPool(peers, static _ => new CallPolicy(), () => created.Count == 0 ? Track(created, new TrackingHandler(true)) : Track(created, new TrackingHandler()));

        await pool.DisposeAsync();

        _ = await Assert.That(created.Count).IsEqualTo(peers.Length);
        _ = await Assert.That(created[1].Disposed).IsTrue();
        _ = await Assert.That(sink.HasEvent(DisposeFailuresInstrumentName, ("node_id", peers[0].NodeId), ("stage", "channel"))).IsTrue();
        _ = await Assert.That(sink.HasEvent(DisposeFailuresInstrumentName, ("node_id", peers[0].NodeId), ("exception_type", nameof(InvalidOperationException)))).IsTrue();
    }

    /// <summary>Disposing the pool must dispose every per-peer handler the pool created.</summary>
    [Test]
    public async Task DisposeReleasesPoolCreatedHandlersAsync()
    {
        var created = new List<TrackingHandler>();
        var pool = new ClientPool(Peers, static _ => new CallPolicy(), () => Track(created));
        await pool.DisposeAsync();

        _ = await Assert.That(created.Count).IsEqualTo(Peers.Length);
        for (var i = 0; i < created.Count; i++)
            _ = await Assert.That(created[i].Disposed).IsTrue();
    }

    /// <summary>A caller-supplied handler is shared by every channel and stays owned by the caller.</summary>
    [Test]
    public async Task DisposeKeepsCallerSharedHandlerAsync()
    {
        using var shared = new TrackingHandler();
        var pool = new ClientPool(Peers, static _ => new CallPolicy(), shared);
        await pool.DisposeAsync();

        _ = await Assert.That(shared.Disposed).IsFalse();
    }

    /// <summary>A policy that fails to dispose is counted and does not stop the pool from disposing the other policies and every channel.</summary>
    [Test]
    public async Task PolicyFailureKeepsDisposingPeersAsync()
    {
        using var sink = new MeasurementSink("Squirix");
        var peers = CreatePeers("policy-dispose-failure");
        var failing = new ICallPolicyCreateExpectations();
        _ = failing.Setups.BeginDrain();
        _ = failing.Setups.DisposeAsync().Callback(static () => ValueTask.FromException(new InvalidOperationException("Policy dispose failed.")));
        var failingPolicy = failing.Instance();
        var secondDisposed = false;
        var second = new ICallPolicyCreateExpectations();
        _ = second.Setups.BeginDrain();
        _ = second.Setups.DisposeAsync().Callback(() =>
        {
            secondDisposed = true;
            return ValueTask.CompletedTask;
        });
        var secondPolicy = second.Instance();
        var created = new List<TrackingHandler>();
        var pool = new ClientPool(peers, nodeId => string.Equals(nodeId, peers[0].NodeId, StringComparison.Ordinal) ? failingPolicy : secondPolicy, () => Track(created));

        await pool.DisposeAsync();

        _ = await Assert.That(secondDisposed).IsTrue();
        _ = await Assert.That(created.Count).IsEqualTo(peers.Length);
        for (var i = 0; i < created.Count; i++)
            _ = await Assert.That(created[i].Disposed).IsTrue();

        _ = await Assert.That(sink.HasEvent(DisposeFailuresInstrumentName, ("node_id", peers[0].NodeId), ("stage", "policy"))).IsTrue();
    }

    /// <summary>A policy that fails to start draining is counted and does not stop the pool from draining and disposing every peer.</summary>
    [Test]
    public async Task DrainFailureKeepsDisposingPeersAsync()
    {
        using var sink = new MeasurementSink("Squirix");
        var peers = CreatePeers("drain-failure");
        var failing = new ICallPolicyCreateExpectations();
        _ = failing.Setups.BeginDrain().Callback(static () => throw new InvalidOperationException("Policy drain failed."));
        _ = failing.Setups.DisposeAsync().ReturnValue(ValueTask.CompletedTask);
        var failingPolicy = failing.Instance();
        var created = new List<TrackingHandler>();
        var pool = new ClientPool(peers, nodeId => string.Equals(nodeId, peers[0].NodeId, StringComparison.Ordinal) ? failingPolicy : new CallPolicy(), () => Track(created));

        await pool.DisposeAsync();

        _ = await Assert.That(created.Count).IsEqualTo(peers.Length);
        for (var i = 0; i < created.Count; i++)
            _ = await Assert.That(created[i].Disposed).IsTrue();

        _ = await Assert.That(sink.HasEvent(DisposeFailuresInstrumentName, ("node_id", peers[0].NodeId), ("stage", "policy"))).IsTrue();
    }

    /// <summary>A repeated dispose disposes each peer policy once.</summary>
    [Test]
    public async Task RepeatedDisposeRunsOnceAsync()
    {
        var disposals = 0;
        var expectations = new ICallPolicyCreateExpectations();
        _ = expectations.Setups.BeginDrain();
        _ = expectations.Setups.DisposeAsync().Callback(() =>
        {
            disposals++;
            return ValueTask.CompletedTask;
        });
        var policy = expectations.Instance();
        var pool = new ClientPool(Peers, _ => policy, static () => new TrackingHandler());

        await pool.DisposeAsync();
        await pool.DisposeAsync();

        _ = await Assert.That(disposals).IsEqualTo(Peers.Length);
    }

    /// <summary>A dispose racing a running one completes only when the running disposal has finished.</summary>
    [Test]
    public async Task SecondDisposeWaitsForFirstAsync()
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var expectations = new ICallPolicyCreateExpectations();
        _ = expectations.Setups.BeginDrain();
        _ = expectations.Setups.DisposeAsync().ReturnValue(new ValueTask(release.Task));
        var policy = expectations.Instance();
        var pool = new ClientPool([Peers[0]], _ => policy, static () => new TrackingHandler());

        var first = StartDisposeAsync(pool);
        var second = StartDisposeAsync(pool);
        _ = await Assert.That(second.IsCompleted).IsFalse();

        release.SetResult();
        await first;
        await second;
    }

    private static Task StartDisposeAsync(ClientPool pool) => pool.DisposeAsync().AsTask();

    private static Peer[] CreatePeers(string prefix) =>
    [
        new() { NodeId = prefix + "-a", Uri = new Uri("https://127.0.0.1:6510") },
        new() { NodeId = prefix + "-b", Uri = new Uri("https://127.0.0.1:6511") },
    ];

    private static TrackingHandler Track(List<TrackingHandler> created, TrackingHandler handler)
    {
        created.Add(handler);
        return handler;
    }

    private static TrackingHandler Track(List<TrackingHandler> created)
    {
        var handler = new TrackingHandler();
        created.Add(handler);
        return handler;
    }

    /// <summary>Records whether the owner disposed the handler; Rocks cannot observe the protected dispose overload.</summary>
    private sealed class TrackingHandler : DelegatingHandler
    {
        private readonly bool _throwOnDispose;

        internal TrackingHandler(bool throwOnDispose = false)
        {
            _throwOnDispose = throwOnDispose;
        }

        internal bool Disposed { get; private set; }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                Disposed = true;

            base.Dispose(disposing);
            if (disposing && _throwOnDispose)
                throw new InvalidOperationException("Handler dispose failed.");
        }
    }
}
