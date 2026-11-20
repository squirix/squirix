using System;
using System.Globalization;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Squirix.Server.Adapters.Wire.Replication;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.LocalCache;
using Squirix.Server.Serialization.Protobuf;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using Squirix.Transport.Wire;
using Squirix.Transport.Wire.Cache;
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
        var client = channel;

        var ex = await NodeAsyncAssert.ThrowsAsync<HttpRequestException>(
            client.SetEntryAsync(
                new SetEntryAsyncRequest
                {
                    OperationId = RpcOperationIdentity.New(),
                    CacheName = "default",
                    Key = key,
                    Entry = new NodeCacheEntry<object?> { Value = "must-not-store", Version = 1 }.MapToProto(),
                },
                cancellationToken: cancellationToken),
            TimeSpan.FromSeconds(30),
            TimeProvider.System,
            cancellationToken);

        var stored = cluster["node-a"].GetRequiredService<PhysicalCache<object?>>().GetValue(new CacheKey("default", key), cancellationToken);
        _ = await Assert.That(ex.StatusCode).IsNull();
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

        using var seedChannel = CreateBinaryChannel(cluster["node-a"].Uri, TestJwtHelper.CreateBearerToken(credentials));
        var seeded = await new CacheWireClient(seedChannel).SetEntryAsync(
            new SetEntryAsyncRequest
            {
                OperationId = RpcOperationIdentity.New(),
                CacheName = "default",
                Key = key,
                Entry = new NodeCacheEntry<object?> { Value = "seeded-secret", Version = 1 }.MapToProto(),
            },
            cancellationToken: cancellationToken);
        _ = await Assert.That(seeded).IsNotNull();

        using var inner = LoopbackHttp.CreateHandler();
        using var handler = new ForgedHostHandler(ForgedHost(cluster["node-a"]), inner);
        using var channel = CreateChannel(cluster["node-a"], handler);
        var client = channel;

        var value = await NodeAsyncAssert.ThrowsAsync<HttpRequestException>(client.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = key }, cancellationToken: cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        var entry = await NodeAsyncAssert.ThrowsAsync<HttpRequestException>(client.GetEntryAsync(new GetEntryAsyncRequest { CacheName = "default", Key = key }, cancellationToken: cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(value.StatusCode).IsNull();
        _ = await Assert.That(entry.StatusCode).IsNull();
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
        var client = channel;
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

        var ex = await NodeAsyncAssert.ThrowsAsync<HttpRequestException>(client.GetReplicaStatusAsync(request, cancellationToken: cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(ex.StatusCode).IsNull();
    }

    /// <summary>A forged Host header does not bypass JWT auth but a valid token still works on the public listener.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BinaryReadAfterForgedHostRejection(CancellationToken cancellationToken)
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
        using var legacy = CreateChannel(cluster["node-a"], handler);
        var oldClient = legacy;
        var request = new GetValueAsyncRequest { CacheName = "default", Key = key };
        var rejected = await NodeAsyncAssert.ThrowsAsync<HttpRequestException>(oldClient.GetValueAsync(request, cancellationToken: cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await Assert.That(rejected.StatusCode).IsNull();

        using var channel = CreateBinaryChannel(cluster["node-a"].Uri, TestJwtHelper.CreateBearerToken(credentials));
        var client = new CacheWireClient(channel);
        var response = await client.GetValueAsync(request, cancellationToken: cancellationToken);

        _ = await Assert.That(response.Found).IsFalse();
    }

    private static string ForgedHost(ITestNodeHost node)
        => string.Create(CultureInfo.InvariantCulture, $"127.0.0.1:{node.GetRequiredService<MtlsOptions>().InternalListenPort}");

    private static HttpClient CreateChannel(ITestNodeHost node, HttpMessageHandler handler)
        => new(handler, false) { BaseAddress = node.Uri };

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
