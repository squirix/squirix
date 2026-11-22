using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Serialization.Wire;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Networking;
using Squirix.Transport.Wire;
using Squirix.Transport.Wire.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests;

/// <summary>
/// Integration coverage for ordinary cache operations across routing hops.
/// Uses <see cref="TwoNodeFixture" /> to share two servers across all tests.
/// </summary>
public sealed class SingleCopyRoutingTests : NodeIntegrationTestBase
{
    /// <summary>Gets the shared two-node fixture injected once per test class.</summary>
    [ClassDataSource<TwoNodeFixture>(Shared = SharedType.PerClass)]
    public required TwoNodeFixture Fixture { get; init; }

    private Uri UriA => Fixture.UriA;

    private Uri UriB => Fixture.UriB;

    /// <summary>A repeated raw remove evaluates the replacement at its owner.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ForwardedRemoveUsesCurrentState(CancellationToken cancellationToken)
    {
        var key = TestKeyOwnerHelper.TwoNode.FindKeyOwnedBy("default", "node-b", "forwarded-remove");
        using var entryChannel = CreateBinaryChannel(UriA);
        var entry = new CacheWireClient(entryChannel);
        using var ownerChannel = CreateBinaryChannel(UriB);
        var owner = new CacheWireClient(ownerChannel);
        var request = new RemoveAsyncRequest { CacheName = "default", Key = key, OperationId = RpcOperationIdentity.New() };

        _ = await owner.SetEntryAsync(CreateSet(key, "original"), cancellationToken: cancellationToken);
        _ = await Assert.That((await entry.RemoveAsync(request, cancellationToken: cancellationToken)).Removed).IsTrue();
        _ = await owner.SetEntryAsync(CreateSet(key, "replacement"), cancellationToken: cancellationToken);
        _ = await Assert.That((await entry.RemoveAsync(request, cancellationToken: cancellationToken)).Removed).IsTrue();
        var current = await owner.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = key }, cancellationToken: cancellationToken);
        _ = await Assert.That(current.Found).IsFalse();
    }

    /// <summary>Reads sent to a non-owner are forwarded to the key owner and return what the owner holds.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ForwardedReadsReturnOwnerState(CancellationToken cancellationToken)
    {
        var key = TestKeyOwnerHelper.TwoNode.FindKeyOwnedBy("default", "node-b", "forwarded-reads");
        using var channelA = CreateBinaryChannel(UriA);
        var clientA = new CacheWireClient(channelA);
        using var channelB = CreateBinaryChannel(UriB);
        var clientB = new CacheWireClient(channelB);
        _ = await clientB.SetEntryAsync(CreateSet(key, "owner-value", TimeSpan.FromHours(1)), cancellationToken: cancellationToken);

        var value = await clientA.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = key }, cancellationToken: cancellationToken);
        var entry = await clientA.GetEntryAsync(new GetEntryAsyncRequest { CacheName = "default", Key = key }, cancellationToken: cancellationToken);
        var expiration = await clientA.GetExpirationAsync(new GetExpirationAsyncRequest { CacheName = "default", Key = key }, cancellationToken: cancellationToken);

        _ = await Assert.That(value.Found).IsTrue();
        _ = await Assert.That(value.Value!.StringValue).IsEqualTo("owner-value");
        _ = await Assert.That(entry.Found).IsTrue();
        _ = await Assert.That(expiration.HasExpiration).IsTrue();
    }

    private static SetEntryAsyncRequest CreateSet(string key, string value, TimeSpan? expiration = null) => new()
    {
        OperationId = RpcOperationIdentity.New(),
        CacheName = "default",
        Key = key,
        Entry = new NodeCacheEntry<object?> { Value = value, Version = 1, Expiration = expiration }.MapToWire(),
    };
}
