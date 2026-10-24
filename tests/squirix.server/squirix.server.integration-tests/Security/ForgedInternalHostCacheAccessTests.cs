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
using Squirix.Server.LocalCache;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Security;

/// <summary>Verifies a forged Host header on the public listener never reaches the anonymous internal cache mapping.</summary>
public sealed class ForgedInternalHostCacheAccessTests : NodeIntegrationTestBase
{
    /// <summary>A token-less write with the internal port in the Host header is rejected and stores nothing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ForgedHostWriteWithoutTokenIsRejected(CancellationToken cancellationToken)
    {
        var credentials = TestJwtHelper.CreateRandomCredentials("https://integration.squirix.test", "forged-host");
        await using var cluster = await StartClusterAsync(
            "node-a",
            "node-b",
            new IntegrationStartOptions { Security = TestNodeSecurityOptions.FromJwtCredentials(credentials) },
            cancellationToken);
        var key = TestKeyOwnerHelper.TwoNode.FindKeyOwnedBy("default", "node-a", "forged-host-write");

        using var inner = LoopbackHttp.CreateHandler();
        using var handler = new ForgedHostHandler(ForgedHost(cluster["node-a"]), inner);
        using var channel = CreateChannel(cluster["node-a"], handler);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            client.SetEntryAsync(
                new SetEntryAsyncRequest
                {
                    OperationId = RpcOperationIdentity.New(),
                    CacheName = "default",
                    Key = key,
                    Entry = new NodeCacheEntry<object?> { Value = "must-not-store", Version = 1 }.MapToProto(),
                },
                cancellationToken: cancellationToken).ResponseAsync);

        var stored = await cluster["node-a"].GetRequiredService<PhysicalCache<object?>>().GetValueAsync(new CacheKey("default", key), cancellationToken);
        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.Unauthenticated);
        _ = await Assert.That(stored.Found).IsFalse();
    }

    /// <summary>A token-less read with the internal port in the Host header is rejected.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ForgedHostReadWithoutTokenIsRejected(CancellationToken cancellationToken)
    {
        var credentials = TestJwtHelper.CreateRandomCredentials("https://integration.squirix.test", "forged-host");
        await using var cluster = await StartClusterAsync(
            "node-a",
            "node-b",
            new IntegrationStartOptions { Security = TestNodeSecurityOptions.FromJwtCredentials(credentials) },
            cancellationToken);
        var key = TestKeyOwnerHelper.TwoNode.FindKeyOwnedBy("default", "node-a", "forged-host-read");

        using var seedChannel = CreateGrpcChannel(cluster["node-a"].Uri);
        var seedHeaders = new Metadata { { "authorization", $"Bearer {TestJwtHelper.CreateBearerToken(credentials)}" } };
        var seeded = await new SquirixCacheService.SquirixCacheServiceClient(seedChannel).SetEntryAsync(
            new SetEntryAsyncRequest
            {
                OperationId = RpcOperationIdentity.New(),
                CacheName = "default",
                Key = key,
                Entry = new NodeCacheEntry<object?> { Value = "seeded-secret", Version = 1 }.MapToProto(),
            },
            new CallOptions(seedHeaders, cancellationToken: cancellationToken));
        _ = await Assert.That(seeded).IsNotNull();

        using var inner = LoopbackHttp.CreateHandler();
        using var handler = new ForgedHostHandler(ForgedHost(cluster["node-a"]), inner);
        using var channel = CreateChannel(cluster["node-a"], handler);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);

        var value = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            client.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = key }, cancellationToken: cancellationToken).ResponseAsync);
        var entry = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            client.GetEntryAsync(new GetEntryAsyncRequest { CacheName = "default", Key = key }, cancellationToken: cancellationToken).ResponseAsync);

        _ = await Assert.That(value.StatusCode).IsEqualTo(StatusCode.Unauthenticated);
        _ = await Assert.That(entry.StatusCode).IsEqualTo(StatusCode.Unauthenticated);
    }

    /// <summary>A forged Host header on the public listener never reaches the closed replication service.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ForgedHostReplicationCallIsRefused(CancellationToken cancellationToken)
    {
        var credentials = TestJwtHelper.CreateRandomCredentials("https://integration.squirix.test", "forged-host");
        await using var cluster = await StartClusterAsync(
            "node-a",
            "node-b",
            new IntegrationStartOptions { FoundationOnly = true, Security = TestNodeSecurityOptions.FromJwtCredentials(credentials) },
            cancellationToken);

        using var inner = LoopbackHttp.CreateHandler();
        using var handler = new ForgedHostHandler(ForgedHost(cluster["node-a"]), inner);
        using var channel = CreateChannel(cluster["node-a"], handler);
        var client = new SquirixReplicationService.SquirixReplicationServiceClient(channel);
        var request = new GetReplicaStatusRequest
        {
            Header = new ReplicationEnvelopeHeader
            {
                SchemaVersion = EnvelopeSchema.Version,
                GroupId = "g1",
                TopologyFingerprint = ByteString.CopyFrom(1, 2, 3, 4),
                ConfigurationGeneration = 1,
                Term = 1,
                LeaderNodeId = "node-b",
                SenderNodeId = "node-b",
            },
        };

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(client.GetReplicaStatusAsync(request, cancellationToken: cancellationToken).ResponseAsync);

        _ = await Assert.That(ex.StatusCode).IsNotEqualTo(StatusCode.OK);
    }

    /// <summary>A forged Host header does not bypass JWT auth but a valid token still works on the public listener.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ForgedHostWithValidTokenStillServed(CancellationToken cancellationToken)
    {
        var credentials = TestJwtHelper.CreateRandomCredentials("https://integration.squirix.test", "forged-host");
        await using var cluster = await StartClusterAsync(
            "node-a",
            "node-b",
            new IntegrationStartOptions { Security = TestNodeSecurityOptions.FromJwtCredentials(credentials) },
            cancellationToken);
        var key = TestKeyOwnerHelper.TwoNode.FindKeyOwnedBy("default", "node-a", "forged-host-token");

        using var inner = LoopbackHttp.CreateHandler();
        using var handler = new ForgedHostHandler(ForgedHost(cluster["node-a"]), inner);
        using var channel = CreateChannel(cluster["node-a"], handler);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var headers = new Metadata { { "authorization", $"Bearer {TestJwtHelper.CreateBearerToken(credentials)}" } };

        var response = await client.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = key }, new CallOptions(headers, cancellationToken: cancellationToken));

        _ = await Assert.That(response.Found).IsFalse();
    }

    private static string ForgedHost(ITestNodeHost node)
        => string.Create(CultureInfo.InvariantCulture, $"127.0.0.1:{node.GetRequiredService<MtlsOptions>().InternalListenPort}");

    private static GrpcChannel CreateChannel(ITestNodeHost node, HttpMessageHandler handler)
        => GrpcChannel.ForAddress(
            node.Uri,
            new GrpcChannelOptions
            {
                HttpHandler = handler,
                DisposeHttpClient = false,
                MaxReceiveMessageSize = EntryLimits.GrpcMaxReceiveMessageSizeBytes,
                MaxSendMessageSize = EntryLimits.GrpcMaxSendMessageSizeBytes,
            });

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
