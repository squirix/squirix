using System;
using System.Globalization;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Adapters.Grpc.Replication;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Networking;
using Xunit;

namespace Squirix.Server.IntegrationTests.Cluster;

/// <summary>REQ-SEC-001: closed replication RPCs are bound to internal mTLS identity.</summary>
public sealed class ReplicationRpcSecurityTests : NodeIntegrationTestBase
{
    /// <summary>External listener does not expose the closed replication service.</summary>
    [Fact]
    public async Task ExternalListenerRejectsReplicationService()
    {
        var uriA = GetNextHttpUri();
        var uriB = GetNextHttpUri();
        var peers = BuildClusterPeers([("node-a", uriA), ("node-b", uriB)]);

        await using var nodeA = await StartNodeAsync(uriA, peers, new NodeStartOptions { FoundationOnly = true });
        await using var nodeB = await StartNodeAsync(uriB, peers, new NodeStartOptions { FoundationOnly = true });

        using var channel = CreateGrpcChannel(uriA);
        var client = new SquirixReplicationService.SquirixReplicationServiceClient(channel);
        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            client.GetReplicaStatusAsync(CreateStatusRequest("node-b"), cancellationToken: DefaultCancellationToken).ResponseAsync);

        Assert.Equal(StatusCode.Unimplemented, ex.StatusCode);
    }

    /// <summary>Forged Host headers cannot bind closed replication RPCs onto the external listener.</summary>
    [Fact]
    public async Task ForgedInternalHostHeaderIsRejected()
    {
        var uriA = GetNextHttpUri();
        var uriB = GetNextHttpUri();
        var peers = BuildClusterPeers([("node-a", uriA), ("node-b", uriB)]);

        await using var nodeA = await StartNodeAsync(uriA, peers, new NodeStartOptions { FoundationOnly = true });
        await using var nodeB = await StartNodeAsync(uriB, peers, new NodeStartOptions { FoundationOnly = true });

        var mtlsOptions = nodeA.Services.GetRequiredService<MtlsOptions>();
        var forgedHost = string.Create(CultureInfo.InvariantCulture, $"127.0.0.1:{mtlsOptions.InternalListenPort}");

        using var inner = LoopbackHttp.CreateHandler();
        using var handler = new ForgedHostHandler(forgedHost, inner);
        using var channel = GrpcChannel.ForAddress(
            uriA,
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
            client.GetReplicaStatusAsync(
                CreateStatusRequest("node-b"),
                new CallOptions(headers, cancellationToken: DefaultCancellationToken)).ResponseAsync);

        Assert.True(ex.StatusCode is StatusCode.Unimplemented or StatusCode.PermissionDenied or StatusCode.Unauthenticated);
    }

    /// <summary>Claimed sender_node_id must match the peer certificate NodeId.</summary>
    [Fact]
    public async Task CertificateNodeIdMismatchIsRejected()
    {
        var uriA = GetNextHttpUri();
        var uriB = GetNextHttpUri();
        var peers = BuildClusterPeers([("node-a", uriA), ("node-b", uriB)]);

        await using var nodeA = await StartNodeAsync(uriA, peers, new NodeStartOptions { FoundationOnly = true });
        await using var nodeB = await StartNodeAsync(uriB, peers, new NodeStartOptions { FoundationOnly = true });

        var mtlsOptions = nodeA.Services.GetRequiredService<MtlsOptions>();
        var interNodeUri = new UriBuilder(uriA.Scheme, uriA.Host, mtlsOptions.InternalListenPort).Uri;
        using var handler = await CreateTrustedInterNodeClientHandlerAsync("node-b", uriB, "node-a", peers, DefaultCancellationToken);
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
        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            client.GetReplicaStatusAsync(CreateStatusRequest("node-a"), cancellationToken: DefaultCancellationToken).ResponseAsync);

        Assert.Equal(StatusCode.Unauthenticated, ex.StatusCode);
    }

    /// <summary>Matching peer certificate and sender_node_id is accepted.</summary>
    [Fact]
    public async Task CertificateNodeIdMatchIsAccepted()
    {
        var uriA = GetNextHttpUri();
        var uriB = GetNextHttpUri();
        var peers = BuildClusterPeers([("node-a", uriA), ("node-b", uriB)]);

        await using var nodeA = await StartNodeAsync(uriA, peers, new NodeStartOptions { FoundationOnly = true });
        await using var nodeB = await StartNodeAsync(uriB, peers, new NodeStartOptions { FoundationOnly = true });

        var mtlsOptions = nodeA.Services.GetRequiredService<MtlsOptions>();
        var interNodeUri = new UriBuilder(uriA.Scheme, uriA.Host, mtlsOptions.InternalListenPort).Uri;
        using var handler = await CreateTrustedInterNodeClientHandlerAsync("node-b", uriB, "node-a", peers, DefaultCancellationToken);
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
        var response = await client.GetReplicaStatusAsync(CreateStatusRequest("node-b"), cancellationToken: DefaultCancellationToken);

        Assert.NotNull(response);
        Assert.Equal("not-ready", response.RefusalCode);
    }

    private static GetReplicaStatusRequest CreateStatusRequest(string senderNodeId) => new()
    {
        Header = new ReplicationEnvelopeHeader
        {
            SchemaVersion = 1,
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
