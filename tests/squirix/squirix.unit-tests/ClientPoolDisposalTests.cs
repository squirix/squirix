using System;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Internal.Cluster.Reliability;
using Squirix.Internal.Cluster.Transport;
using Squirix.Testing;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using TUnit.Mocks;
using TUnit.Mocks.Generated;

namespace Squirix.UnitTests;

/// <summary><see cref="ClientPool" /> disposal and per-peer policy lifecycle.</summary>
[Immutable]
public sealed class ClientPoolDisposalTests
{
    private const string DisposeFailuresInstrumentName = "squirix_client_pool_dispose_failures_total";

    private static readonly Peer[] Peers =
    [
        new() { NodeId = "node-a", Uri = new Uri("https://127.0.0.1:6510") },
        new() { NodeId = "node-b", Uri = new Uri("https://127.0.0.1:6511") },
    ];

    /// <summary>TLS settings and the certificate material they reference remain owned by the caller.</summary>
    [Test]
    public async Task DisposeKeepsCallerTlsMaterialAsync()
    {
        using var key = ECDsa.Create();
        var request = new CertificateRequest("CN=ClientPoolTests", key, HashAlgorithmName.SHA256);
        var now = TimeProvider.System.GetUtcNow();
        using var certificate = request.CreateSelfSigned(now.AddMinutes(-1), now.AddDays(1));
        var tlsOptions = new SslClientAuthenticationOptions { ClientCertificates = [certificate] };
        var pool = new ClientPool(Peers, static _ => new CallPolicy(TimeProvider.System), TimeProvider.System, tlsOptions);
        await pool.DisposeAsync();

        _ = await Assert.That(certificate.HasPrivateKey).IsTrue();
        _ = await Assert.That(tlsOptions.ClientCertificates.Count).IsEqualTo(1);
    }

    /// <summary>A policy that fails to dispose is counted and does not stop the pool from disposing the other policies and every channel.</summary>
    [Test]
    public async Task PolicyFailureKeepsDisposingPeersAsync()
    {
        using var sink = new MeasurementSink("Squirix");
        var peers = CreatePeers("policy-dispose-failure");
        var failing = ICallPolicy.Mock(MockBehavior.Strict);
        _ = failing.BeginDrain();
        _ = failing.DisposeAsync().ReturnsAsync(static () => ValueTask.FromException(new InvalidOperationException("Policy dispose failed.")));
        var failingPolicy = failing.Object;
        var secondDisposed = false;
        var second = ICallPolicy.Mock(MockBehavior.Strict);
        _ = second.BeginDrain();
        _ = second.DisposeAsync().ReturnsAsync(() =>
        {
            secondDisposed = true;
            return ValueTask.CompletedTask;
        });
        var secondPolicy = second.Object;
        var pool = new ClientPool(peers, nodeId => string.Equals(nodeId, peers[0].NodeId, StringComparison.Ordinal) ? failingPolicy : secondPolicy, TimeProvider.System);

        await pool.DisposeAsync();

        _ = await Assert.That(secondDisposed).IsTrue();
        _ = await Assert.That(sink.HasEvent(DisposeFailuresInstrumentName, ("node_id", peers[0].NodeId), ("stage", "policy"))).IsTrue();
    }

    /// <summary>A policy that fails to start draining is counted and does not stop the pool from draining and disposing every peer.</summary>
    [Test]
    public async Task DrainFailureKeepsDisposingPeersAsync()
    {
        using var sink = new MeasurementSink("Squirix");
        var peers = CreatePeers("drain-failure");
        var failing = ICallPolicy.Mock(MockBehavior.Strict);
        _ = failing.BeginDrain().Callback(static () => throw new InvalidOperationException("Policy drain failed."));
        _ = failing.DisposeAsync().ReturnsAsync(ValueTask.CompletedTask);
        var failingPolicy = failing.Object;
        var pool = new ClientPool(peers, nodeId => string.Equals(nodeId, peers[0].NodeId, StringComparison.Ordinal) ? failingPolicy : new CallPolicy(TimeProvider.System), TimeProvider.System);

        await pool.DisposeAsync();

        _ = await Assert.That(sink.HasEvent(DisposeFailuresInstrumentName, ("node_id", peers[0].NodeId), ("stage", "policy"))).IsTrue();
    }

    /// <summary>A repeated dispose disposes each peer policy once.</summary>
    [Test]
    public async Task RepeatedDisposeRunsOnceAsync()
    {
        var disposals = 0;
        var expectations = ICallPolicy.Mock(MockBehavior.Strict);
        _ = expectations.BeginDrain();
        _ = expectations.DisposeAsync().ReturnsAsync(() =>
        {
            disposals++;
            return ValueTask.CompletedTask;
        });
        var policy = expectations.Object;
        var pool = new ClientPool(Peers, _ => policy, TimeProvider.System);

        await pool.DisposeAsync();
        await pool.DisposeAsync();

        _ = await Assert.That(disposals).IsEqualTo(Peers.Length);
    }

    /// <summary>A dispose racing a running one completes only when the running disposal has finished.</summary>
    [Test]
    public async Task SecondDisposeWaitsForFirstAsync()
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var expectations = ICallPolicy.Mock(MockBehavior.Strict);
        _ = expectations.BeginDrain();
        _ = expectations.DisposeAsync().ReturnsAsync(new ValueTask(release.Task));
        var policy = expectations.Object;
        var pool = new ClientPool([Peers[0]], _ => policy, TimeProvider.System);

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
}
