using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Node.Hosting;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Logging;
using Squirix.Server.TestKit.Mtls;
using Squirix.Server.TestKit.Networking;
using Squirix.Transport.Binary;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests;

/// <summary>Checks the negotiated peer channel between independently composed native node listeners.</summary>
public sealed class NativePeerTransportTests
{
    /// <summary>Both roles exchange protobuf messages under mTLS and preserve explicit peer operation statuses.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(30_000)]
    public async Task AuthenticatedNodesExchangePeerFrames(CancellationToken cancellationToken)
    {
        using var firstPort = ListenPortPool.IntegrationTests.HoldPort();
        using var secondPort = ListenPortPool.IntegrationTests.HoldPort();
        ClusterIdentity? identity = null;
        var peers = ClusterIdentity.CreatePeers([new ClusterNode("nodeA", firstPort.HttpUri), new ClusterNode("nodeB", secondPort.HttpUri)], ref identity);
        using var owner = identity!;
        using var first = await StartNodeAsync(firstPort, peers, owner, "nodeA", cancellationToken);
        using var second = await StartNodeAsync(secondPort, peers, owner, "nodeB", cancellationToken);
        try
        {
            using var lease = first.Services.GetRequiredService<IServerClientPool>().LeaseChannel("nodeB", cancellationToken);
            var pong = await lease.Client.SendAsync("ping", new Empty(), Empty.Parser, lease.Token);
            _ = await Assert.That(pong.CalculateSize()).IsEqualTo(0);
            var refused = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(
                lease.Client.SendAsync("missing", new Empty(), Empty.Parser, lease.Token),
                TimeSpan.FromSeconds(5),
                TimeProvider.System,
                cancellationToken);
            _ = await Assert.That(refused.StatusCode).IsEqualTo(FrameStatus.Unimplemented);
            var ack = await second.Services.GetRequiredService<IServerClientPool>().ForNode("nodeA").SendAsync("ping", new Empty(), Empty.Parser, cancellationToken);
            _ = await Assert.That(ack.CalculateSize()).IsEqualTo(0);
        }
        finally
        {
            await first.StopAsync(CancellationToken.None);
            await second.StopAsync(CancellationToken.None);
        }
    }

    /// <summary>A transfer waiting for its connection and a second queued transfer leave the peer available for control traffic.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(30_000)]
    public async Task ControlCallsBypassQueuedStateTransfers(CancellationToken cancellationToken)
    {
        using var firstPort = ListenPortPool.IntegrationTests.HoldPort();
        using var secondPort = ListenPortPool.IntegrationTests.HoldPort();
        ClusterIdentity? identity = null;
        var peers = ClusterIdentity.CreatePeers([new ClusterNode("nodeA", firstPort.HttpUri), new ClusterNode("nodeB", secondPort.HttpUri)], ref identity);
        using var owner = identity!;
        using var first = await StartNodeAsync(firstPort, peers, owner, "nodeA", cancellationToken);
        using var second = await StartNodeAsync(secondPort, peers, owner, "nodeB", cancellationToken);
        try
        {
            var address = peers[1].InterNodeUri!;
            var material = first.Services.GetRequiredService<MtlsCertificate>();
            var verifier = new PeerHandshakeVerifier(material.PeerChains!, TimeProvider.System, NullLogger.Instance);
            var tls = NativePeerClient.CreateTlsOptions(address.DnsSafeHost, "nodeB", material, verifier);
            var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var dialCount = 0;
            async ValueTask<EndPoint> ResolveAsync(CancellationToken token)
            {
                if (Interlocked.Increment(ref dialCount) == 1)
                {
                    entered.SetResult();
                    await Task.Delay(Timeout.InfiniteTimeSpan, TimeProvider.System, token);
                }

                return new DnsEndPoint(address.DnsSafeHost, address.Port);
            }

            using var channel = new NativePeerClient((address, "nodeB"), material, verifier, new TrackedConnections(), TimeSpan.FromSeconds(5), null, new PeerSocketOptions(tls, ResolveAsync));
            using var transfer = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var firstTransfer = channel.SendAsync("replication/TransferReplicaState", new Empty(), Empty.Parser, transfer.Token);
            await entered.Task.WaitAsync(cancellationToken);
            var queuedTransfer = channel.SendAsync("replication/TransferReplicaState", new Empty(), Empty.Parser, transfer.Token);
            try
            {
                var ping = await channel.SendAsync("ping", new Empty(), Empty.Parser, cancellationToken);
                _ = await Assert.That(ping.CalculateSize()).IsEqualTo(0);
                _ = await Assert.That(Volatile.Read(ref dialCount)).IsEqualTo(2);
                _ = await Assert.That(queuedTransfer.IsCompleted).IsFalse();
            }
            finally
            {
                await transfer.CancelAsync();
                _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(Task.WhenAll(firstTransfer, queuedTransfer), TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);
                _ = await Assert.That(firstTransfer.IsCanceled).IsTrue();
                _ = await Assert.That(queuedTransfer.IsCanceled).IsTrue();
            }
        }
        finally
        {
            await first.StopAsync(CancellationToken.None);
            await second.StopAsync(CancellationToken.None);
        }
    }

    private static async Task<IHost> StartNodeAsync(HeldPort port, ServerPeer[] peers, ClusterIdentity identity, string name, CancellationToken cancellationToken)
    {
        var options = new TopologyOptions(peers) { NodeId = name, Uri = port.HttpUri };
        var material = await identity.ResolveNodeStartupForBindAsync(options, TestNodeProfile.Normal, null, cancellationToken);
        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder();
        await TestNodeServices.ConfigureBuilderAsync(builder.Services, options, Configure, cancellationToken);
        void Configure(CompositionArgs args)
        {
            args.MtlsOptions = material.Options;
            args.Certificate = material.Certificate;
            args.ManagementUri = new Uri("https://127.0.0.1:0");
        }

        var host = builder.Build();
        try
        {
            port.Dispose();
            await host.StartAsync(cancellationToken);
            return host;
        }
        catch
        {
            host.Dispose();
            throw;
        }
    }
}
