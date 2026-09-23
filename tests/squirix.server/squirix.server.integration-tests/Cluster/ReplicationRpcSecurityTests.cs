using System;
using System.Globalization;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using Squirix.Server.Adapters.Grpc.Replication;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster;

/// <summary>REQ-SEC-001: closed replication RPCs are bound to internal mTLS identity.</summary>
public sealed class ReplicationRpcSecurityTests : NodeIntegrationTestBase
{
    /// <summary>Matching peer certificate and sender_node_id is accepted.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CertificateNodeIdMatchIsAccepted(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node-a", "node-b", new IntegrationStartOptions { FoundationOnly = true }, cancellationToken);
        var options = cluster["node-a"].GetRequiredService<MtlsOptions>();
        var interNodeUri = new UriBuilder(cluster["node-a"].Uri.Scheme, cluster["node-a"].Uri.Host, options.InternalListenPort).Uri;
        using var handler = await CreateTrustedInterNodeClientHandlerAsync("node-b", cluster["node-b"].Uri, "node-a", cluster.Peers, cancellationToken);
        using var channel = GrpcChannel.ForAddress(
            interNodeUri,
            new GrpcChannelOptions
            {
                HttpHandler = handler,
                MaxReceiveMessageSize = EntryLimits.GrpcMaxReceiveMessageSizeBytes,
                MaxSendMessageSize = EntryLimits.GrpcMaxSendMessageSizeBytes,
            });

        var client = new SquirixReplicationService.SquirixReplicationServiceClient(channel);

        // Certificate identity is node-b; claim sender_node_id node-b for matching identity.
        var response = await client.GetReplicaStatusAsync(CreateStatusRequest("node-b"), cancellationToken: cancellationToken);

        _ = await Assert.That(response).IsNotNull();
        _ = await Assert.That(response.RefusalCode).IsEqualTo("not-ready");
    }

    /// <summary>Claimed sender_node_id must match the peer certificate NodeId.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CertificateNodeIdMismatchIsRejected(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node-a", "node-b", new IntegrationStartOptions { FoundationOnly = true }, cancellationToken);
        var nodeA = cluster["node-a"];

        var mtlsOptions = nodeA.GetRequiredService<MtlsOptions>();
        var interNodeUri = new UriBuilder(nodeA.Uri.Scheme, nodeA.Uri.Host, mtlsOptions.InternalListenPort).Uri;
        using var handler = await CreateTrustedInterNodeClientHandlerAsync("node-b", cluster["node-b"].Uri, "node-a", cluster.Peers, cancellationToken);
        using var channel = GrpcChannel.ForAddress(
            interNodeUri,
            new GrpcChannelOptions
            {
                HttpHandler = handler,
                MaxReceiveMessageSize = EntryLimits.GrpcMaxReceiveMessageSizeBytes,
                MaxSendMessageSize = EntryLimits.GrpcMaxSendMessageSizeBytes,
            });

        var client = new SquirixReplicationService.SquirixReplicationServiceClient(channel);

        // Certificate identity is node-b; claim sender_node_id node-a to force mismatch after TLS succeeds.
        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(client.GetReplicaStatusAsync(CreateStatusRequest("node-a"), cancellationToken: cancellationToken).ResponseAsync);

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.Unauthenticated);
    }

    /// <summary>External listener does not expose the closed replication service.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExternalListenerRefusesReplicationRpc(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node-a", "node-b", new IntegrationStartOptions { FoundationOnly = true }, cancellationToken);
        using var channel = CreateGrpcChannel(cluster["node-a"].Uri);
        var client = new SquirixReplicationService.SquirixReplicationServiceClient(channel);
        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(client.GetReplicaStatusAsync(CreateStatusRequest("node-b"), cancellationToken: cancellationToken).ResponseAsync);

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.Unimplemented);
    }

    /// <summary>Leader-authorized RPCs reject a trusted peer claiming a foreign LeaderNodeId.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ForeignLeaderNodeIdIsRejected(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node-a", "node-b", new IntegrationStartOptions { FoundationOnly = true }, cancellationToken);
        var nodeA = cluster["node-a"];

        var mtlsOptions = nodeA.GetRequiredService<MtlsOptions>();
        var interNodeUri = new UriBuilder(nodeA.Uri.Scheme, nodeA.Uri.Host, mtlsOptions.InternalListenPort).Uri;
        using var handler = await CreateTrustedInterNodeClientHandlerAsync("node-b", cluster["node-b"].Uri, "node-a", cluster.Peers, cancellationToken);
        using var channel = GrpcChannel.ForAddress(
            interNodeUri,
            new GrpcChannelOptions
            {
                HttpHandler = handler,
                MaxReceiveMessageSize = EntryLimits.GrpcMaxReceiveMessageSizeBytes,
                MaxSendMessageSize = EntryLimits.GrpcMaxSendMessageSizeBytes,
            });

        var client = new SquirixReplicationService.SquirixReplicationServiceClient(channel);

        // Certificate identity is node-b; claim leader_node_id node-a to force a leader identity mismatch.
        var request = CreateAppendRequest("node-b", "node-a");
        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(client.AppendReplicaEntriesAsync(request, cancellationToken: cancellationToken).ResponseAsync);

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.PermissionDenied);
    }

    /// <summary>Forged Host headers cannot bind closed replication RPCs onto the external listener.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ForgedInternalHostHeaderIsRejected(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node-a", "node-b", new IntegrationStartOptions { FoundationOnly = true }, cancellationToken);
        var nodeA = cluster["node-a"];

        var mtlsOptions = nodeA.GetRequiredService<MtlsOptions>();
        var forgedHost = string.Create(CultureInfo.InvariantCulture, $"127.0.0.1:{mtlsOptions.InternalListenPort}");

        using var inner = LoopbackHttp.CreateHandler();
        using var handler = new ForgedHostHandler(forgedHost, inner);
        using var channel = GrpcChannel.ForAddress(
            nodeA.Uri,
            new GrpcChannelOptions
            {
                HttpHandler = handler,
                DisposeHttpClient = false,
                MaxReceiveMessageSize = EntryLimits.GrpcMaxReceiveMessageSizeBytes,
                MaxSendMessageSize = EntryLimits.GrpcMaxSendMessageSizeBytes,
            });
        var client = new SquirixReplicationService.SquirixReplicationServiceClient(channel);
        var headers = new Metadata { { "squirix-internal-owner-rpc", "true" } };

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            client.GetReplicaStatusAsync(CreateStatusRequest("node-b"), new CallOptions(headers, cancellationToken: cancellationToken)).ResponseAsync);

        _ = await Assert.That(ex.StatusCode == StatusCode.Unimplemented || ex.StatusCode == StatusCode.PermissionDenied || ex.StatusCode == StatusCode.Unauthenticated).IsTrue();
    }

    /// <summary>Leader-authorized RPCs accept a trusted peer whose LeaderNodeId matches its certificate identity.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MatchingLeaderNodeIdIsAccepted(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node-a", "node-b", new IntegrationStartOptions { FoundationOnly = true }, cancellationToken);
        var nodeA = cluster["node-a"];

        var mtlsOptions = nodeA.GetRequiredService<MtlsOptions>();
        var interNodeUri = new UriBuilder(nodeA.Uri.Scheme, nodeA.Uri.Host, mtlsOptions.InternalListenPort).Uri;
        using var handler = await CreateTrustedInterNodeClientHandlerAsync("node-b", cluster["node-b"].Uri, "node-a", cluster.Peers, cancellationToken);
        using var channel = GrpcChannel.ForAddress(
            interNodeUri,
            new GrpcChannelOptions
            {
                HttpHandler = handler,
                MaxReceiveMessageSize = EntryLimits.GrpcMaxReceiveMessageSizeBytes,
                MaxSendMessageSize = EntryLimits.GrpcMaxSendMessageSizeBytes,
            });

        var client = new SquirixReplicationService.SquirixReplicationServiceClient(channel);

        // Certificate identity is node-b; claim leader_node_id node-b for matching identity.
        var response = await client.AppendReplicaEntriesAsync(CreateAppendRequest("node-b", "node-b"), cancellationToken: cancellationToken);

        _ = await Assert.That(response).IsNotNull();
        _ = await Assert.That(response.Success).IsFalse();
        _ = await Assert.That(response.RefusalCode).IsEqualTo("not-ready");
    }

    private static AppendReplicaEntriesRequest CreateAppendRequest(string senderNodeId, string leaderNodeId) => new()
    {
        Header = new ReplicationEnvelopeHeader
        {
            SchemaVersion = EnvelopeCodec.SchemaVersion,
            GroupId = "g1",
            TopologyFingerprint = ByteString.CopyFrom(1, 2, 3, 4),
            ConfigurationGeneration = 1,
            Term = 1,
            LeaderNodeId = leaderNodeId,
            SenderNodeId = senderNodeId,
        },
        PrevLogIndex = 0,
    };

    private static GetReplicaStatusRequest CreateStatusRequest(string senderNodeId) => new()
    {
        Header = new ReplicationEnvelopeHeader
        {
            SchemaVersion = EnvelopeCodec.SchemaVersion,
            GroupId = "g1",
            TopologyFingerprint = ByteString.CopyFrom(1, 2, 3, 4),
            ConfigurationGeneration = 1,
            Term = 1,
            LeaderNodeId = senderNodeId,
            SenderNodeId = senderNodeId,
        },
    };

    private sealed class ForgedHostHandler : DelegatingHandler
    {
        private readonly string _host;

        internal ForgedHostHandler(string host, HttpMessageHandler inner)
            : base(inner)
        {
            _host = host;
        }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            request.Headers.Host = _host;
            return base.SendAsync(request, cancellationToken);
        }
    }
}
