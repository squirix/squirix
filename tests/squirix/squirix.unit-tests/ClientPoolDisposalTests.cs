using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Internal.Cluster.Reliability;
using Squirix.Internal.Cluster.Transport;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests;

/// <summary><see cref="ClientPool" /> disposal of the HTTP handlers it owns.</summary>
[Immutable]
public sealed class ClientPoolDisposalTests
{
    private static readonly Peer[] Peers =
    [
        new() { NodeId = "node-a", Uri = new Uri("https://127.0.0.1:6510") },
        new() { NodeId = "node-b", Uri = new Uri("https://127.0.0.1:6511") },
    ];

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

    private static TrackingHandler Track(List<TrackingHandler> created)
    {
        var handler = new TrackingHandler();
        created.Add(handler);
        return handler;
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
}
