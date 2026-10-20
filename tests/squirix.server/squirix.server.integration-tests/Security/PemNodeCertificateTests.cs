using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Security;

/// <summary>Verifies internode mTLS works when the node certificate is configured as a PEM certificate and key pair.</summary>
public sealed class PemNodeCertificateTests : NodeIntegrationTestBase
{
    /// <summary>Verifies a write forwarded to the owner node completes over internode mTLS between nodes configured with PEM node certificates.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ForwardingWorksWithPemCertificates(CancellationToken cancellationToken)
    {
        var options = new IntegrationStartOptions { PemNodeCertificate = true, TimeProvider = new FakeTimeProvider(DateTimeOffset.UtcNow) };
        await using var cluster = await StartClusterAsync("node-a", "node-b", options, cancellationToken);

        var key = TestKeyOwnerHelper.TwoNode.FindKeyOwnedBy("default", "node-b", "pem-forward");
        using var channelA = CreateGrpcChannel(cluster["node-a"].Uri);
        var clientA = new SquirixCacheService.SquirixCacheServiceClient(channelA);
        var setResponse = await clientA.TryAddEntryAsync(
            new TryAddEntryAsyncRequest
            {
                OperationId = RpcOperationIdentity.New(),
                CacheName = "default",
                Key = key,
                Entry = new NodeCacheEntry<object?> { Value = "pem-forwarded-value", Version = 1 }.MapToProto(),
            },
            cancellationToken: cancellationToken);

        _ = await Assert.That(setResponse.Added).IsTrue();

        using var channelB = CreateGrpcChannel(cluster["node-b"].Uri);
        var clientB = new SquirixCacheService.SquirixCacheServiceClient(channelB);
        var getResponse = await clientB.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = key }, cancellationToken: cancellationToken);

        _ = await Assert.That(getResponse.Found).IsTrue();
    }
}
