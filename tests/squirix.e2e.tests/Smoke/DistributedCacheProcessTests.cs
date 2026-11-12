using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.DependencyInjection;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Smoke;

/// <summary>Exercises the public distributed cache adapter against a separate HTTPS server process.</summary>
[Property("Suite", "Smoke")]
public sealed class DistributedCacheProcessTests
{
    /// <summary>Gets the running server shared by the non-destructive scenarios.</summary>
    [ClassDataSource<DistributedCacheProcessFixture>(Shared = SharedType.PerClass)]
    public required DistributedCacheProcessFixture Fixture { get; init; }

    /// <summary>A cancelled write leaves an empty stored value distinguishable from a miss.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(60_000)]
    public async Task CancelledWritePreservesEmptyValue(CancellationToken cancellationToken)
    {
        await using var provider = DistributedCacheProcessFixture.CreateProvider(Fixture.Uri);
        var cache = provider.GetRequiredService<IDistributedCache>();
        var options = new DistributedCacheEntryOptions();
        await cache.SetAsync("empty", [], options, cancellationToken);
        var write = cache.SetAsync("empty", [1], options, new CancellationToken(true));
        await write.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        _ = await Assert.That(write.IsCanceled).IsTrue();
        var stored = await cache.GetAsync("empty", cancellationToken);
        _ = await Assert.That(stored).IsNotNull();
        _ = await Assert.That(stored!.Length).IsEqualTo(0);
        await cache.RemoveAsync("empty", cancellationToken);
        _ = await Assert.That(await cache.GetAsync("empty", cancellationToken)).IsNull();
    }

    /// <summary>Verifies that a fresh HybridCache provider uses the previously stored secondary value.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(60_000)]
    public async Task FreshHybridCacheUsesServerL2(CancellationToken cancellationToken)
    {
        await using var first = DistributedCacheProcessFixture.CreateProvider(Fixture.Uri);
        var writer = first.GetRequiredService<HybridCache>();
        var expected = await writer.GetOrCreateAsync("shared", static _ => ValueTask.FromResult("from-server"), cancellationToken: cancellationToken);
        var distributed = first.GetRequiredService<IDistributedCache>();
        await WaitForL2Async(distributed, "shared", true, cancellationToken);

        await using var second = DistributedCacheProcessFixture.CreateProvider(Fixture.Uri);
        var reader = second.GetRequiredService<HybridCache>();
        var actual = await reader.GetOrCreateAsync("shared", static _ => ValueTask.FromResult("unexpected-factory"), cancellationToken: cancellationToken);
        _ = await Assert.That(actual).IsEqualTo(expected);
    }

    /// <summary>Key removal is visible after L1 expires, and a fresh provider reads the stored tag marker.</summary>
    /// <param name="byTag">Whether to invalidate by tag instead of by key.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    [Timeout(60_000)]
    public async Task HybridCacheObservesRemoteInvalidation(bool byTag, CancellationToken cancellationToken)
    {
        await using var first = DistributedCacheProcessFixture.CreateProvider(Fixture.Uri);
        await using var second = DistributedCacheProcessFixture.CreateProvider(Fixture.Uri);
        var writer = first.GetRequiredService<HybridCache>();
        var reader = second.GetRequiredService<HybridCache>();
        var distributed = first.GetRequiredService<IDistributedCache>();
        var key = byTag ? "tagged-by-tag" : "tagged-by-key";
        var tag = byTag ? "tag-invalidation" : "key-invalidation";
        var localExpiration = TimeSpan.FromMilliseconds(250);
        var options = new HybridCacheEntryOptions { Expiration = TimeSpan.FromMinutes(1), LocalCacheExpiration = localExpiration };
        await writer.SetAsync(key, "original", options, [tag], cancellationToken);
        await WaitForL2Async(distributed, key, true, cancellationToken);
        var original = await reader.GetOrCreateAsync(key, static _ => ValueTask.FromResult("unexpected-factory"), options, cancellationToken: cancellationToken);
        _ = await Assert.That(original).IsEqualTo("original");
        if (byTag)
        {
            await writer.RemoveByTagAsync(tag, cancellationToken);
            _ = await Assert.That(await distributed.GetAsync(key, cancellationToken)).IsNotNull();
        }
        else
        {
            await writer.RemoveAsync(key, cancellationToken);
        }

        await Task.Delay(localExpiration, TimeProvider.System, cancellationToken);

        // HybridCache can retain tag metadata independently of the L1 entry.
        await using var fresh = byTag ? DistributedCacheProcessFixture.CreateProvider(Fixture.Uri) : null;
        var observer = fresh?.GetRequiredService<HybridCache>() ?? reader;
        var replacement = await observer.GetOrCreateAsync(key, static _ => ValueTask.FromResult("replacement"), options, cancellationToken: cancellationToken);
        _ = await Assert.That(replacement).IsEqualTo("replacement");
    }

    /// <summary>A miss invokes the factory again after the hosted L2 entry expires.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(60_000)]
    public async Task HybridCacheRecreatesExpiredValue(CancellationToken cancellationToken)
    {
        await using var first = DistributedCacheProcessFixture.CreateProvider(Fixture.Uri);
        await using var second = DistributedCacheProcessFixture.CreateProvider(Fixture.Uri);
        var writer = first.GetRequiredService<HybridCache>();
        var reader = second.GetRequiredService<HybridCache>();
        var distributed = first.GetRequiredService<IDistributedCache>();
        const string key = "expires";
        var options = new HybridCacheEntryOptions { Expiration = TimeSpan.FromSeconds(2), LocalCacheExpiration = TimeSpan.FromMilliseconds(100) };
        var initial = await writer.GetOrCreateAsync(key, static _ => ValueTask.FromResult("initial"), options, cancellationToken: cancellationToken);
        _ = await Assert.That(initial).IsEqualTo("initial");
        await WaitForL2Async(distributed, key, true, cancellationToken);
        await WaitForL2Async(distributed, key, false, cancellationToken);
        var replacement = await reader.GetOrCreateAsync(key, static _ => ValueTask.FromResult("replacement"), cancellationToken: cancellationToken);
        _ = await Assert.That(replacement).IsEqualTo("replacement");
    }

    /// <summary>A warm write takes ownership of its wire bytes before returning to the caller.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(60_000)]
    public async Task WarmSetOwnsItsPayload(CancellationToken cancellationToken)
    {
        await using var provider = DistributedCacheProcessFixture.CreateProvider(Fixture.Uri);
        var cache = provider.GetRequiredService<IDistributedCache>();
        _ = await cache.GetAsync("warm", cancellationToken);
        byte[] bytes = [0, 255, 128];
        var write = cache.SetAsync("owned", bytes, new DistributedCacheEntryOptions(), CancellationToken.None);
        bytes[0] = 42;
        await write;
        var stored = await cache.GetAsync("owned", cancellationToken);
        _ = await Assert.That(stored.AsSpan().SequenceEqual(new byte[] { 0, 255, 128 })).IsTrue();
    }

    private static async Task WaitForL2Async(IDistributedCache cache, string key, bool expectedPresence, CancellationToken cancellationToken)
    {
        using var bound = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        bound.CancelAfter(TimeSpan.FromSeconds(10));
        while (true)
        {
            var value = await cache.GetAsync(key, bound.Token);
            var isPresent = value != null;
            if (isPresent == expectedPresence)
                return;

            await Task.Delay(TimeSpan.FromMilliseconds(25), TimeProvider.System, bound.Token);
        }
    }
}
