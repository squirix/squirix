using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.SingleNode;

/// <summary>Null and missing-value CRUD integration tests on a controllable clock.</summary>
[Immutable]
public sealed class CrudNullValueTests : ClockTestBase
{
    /// <summary>Verifies GetEntryAsync returns entry or null when missing or expired.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GetEntryAsyncReturnsEntryOrNull(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("get-entry-async", cancellationToken);
        _ = await Assert.That((await cache.GetEntryAsync("missing", cancellationToken)).Found).IsFalse();

        // Generous expiration so expiry does not overtake the immediate read on a loaded CI runner.
        var expiration = TimeSpan.FromSeconds(2);
        await cache.SetAsync("k1", "v1", Expiry.In(expiration), cancellationToken);
        var e = await cache.GetEntryAsync("k1", cancellationToken);
        _ = await Assert.That(e.Found).IsTrue();
        _ = await Assert.That(e.Value).IsEqualTo("v1");
        Clock.Advance(expiration + TimeSpan.FromSeconds(2));
        _ = await Assert.That((await cache.GetEntryAsync("k1", cancellationToken)).Found).IsFalse();
    }

    /// <summary>Verifies GetEntry returns entry with metadata or null when missing or expired.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GetEntryReturnsEntryOrNull(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("get-entry", cancellationToken);
        _ = await Assert.That((await cache.GetEntryAsync("missing", cancellationToken)).Found).IsFalse();

        // Generous expiration so the immediate read is not overtaken by expiry on a loaded CI runner.
        var expiration = TimeSpan.FromSeconds(2);
        await cache.SetAsync("k1", "v1", Expiry.In(expiration), cancellationToken);
        var e = await cache.GetEntryAsync("k1", cancellationToken);
        _ = await Assert.That(e.Found).IsTrue();
        _ = await Assert.That(e.Value).IsEqualTo("v1");
        Clock.Advance(expiration + TimeSpan.FromSeconds(2));
        _ = await Assert.That((await cache.GetEntryAsync("k1", cancellationToken)).Found).IsFalse();
    }

    /// <summary>Verifies TryGetValue returns proper flags and value.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GetReturnsFlags(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("try-get", cancellationToken);
        var miss = await cache.GetValueAsync("missing", cancellationToken);
        _ = await Assert.That(miss.Found).IsFalse();
        await cache.SetAsync("k1", "v1", cancellationToken: cancellationToken);
        var found = await cache.GetValueAsync("k1", cancellationToken);
        _ = await Assert.That(found.Found).IsTrue();
    }

    /// <summary>Verifies RemoveAsync returns the removed entry metadata before deleting the key.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveAsyncReturnsRemovedEntryMetadata(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("try-remove-entry-metadata-public-extra", cancellationToken);
        await cache.SetAsync("k", "v", cancellationToken: cancellationToken);
        var before = await cache.GetEntryAsync("k", cancellationToken);
        _ = await Assert.That(before.Found).IsTrue();
        _ = await Assert.That(before.Value).IsEqualTo("v");
        var removed = await cache.RemoveAsync("k", cancellationToken);
        _ = await Assert.That(removed).IsTrue();
        _ = await Assert.That((await cache.GetValueAsync("k", cancellationToken)).Found).IsFalse();
    }

    /// <summary>Verifies RemoveAsync removes a stored null value.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveAsyncStoredNullReportsRemoved(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<object?>("try-remove-null-stored-public-extra", cancellationToken);
        await cache.SetAsync("k", null, cancellationToken: cancellationToken);
        var removed = await cache.RemoveAsync("k", cancellationToken);
        _ = await Assert.That(removed).IsTrue();
        _ = await Assert.That((await cache.GetValueAsync("k", cancellationToken)).Found).IsFalse();
    }

    /// <summary>Verifies TryRemove returns whether a live entry was removed.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveReturnsFlag(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("try-remove", cancellationToken);
        var miss = await cache.RemoveAsync("missing", cancellationToken);
        _ = await Assert.That(miss).IsFalse();
        await cache.SetAsync("k1", "v1", cancellationToken: cancellationToken);
        var removed = await cache.RemoveAsync("k1", cancellationToken);
        _ = await Assert.That(removed).IsTrue();
        _ = await Assert.That((await cache.GetValueAsync("k1", cancellationToken)).Found).IsFalse();
    }

    /// <summary>Verifies RemoveAsync removes a stored null value.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveReturnsRemovedForStoredNullEntry(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<object?>("try-remove-null-entry-public-extra", cancellationToken);
        await cache.SetAsync("k", null, cancellationToken: cancellationToken);
        var result = await cache.RemoveAsync("k", cancellationToken);
        _ = await Assert.That(result).IsTrue();
        _ = await Assert.That((await cache.GetValueAsync("k", cancellationToken)).Found).IsFalse();
    }

    /// <summary>Verifies RemoveAsync removes a stored null value.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveReturnsRemovedForStoredNullValue(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string?>("try-remove-null-value-public-extra", cancellationToken);
        await cache.SetAsync("k", null, cancellationToken: cancellationToken);
        var removed = await cache.RemoveAsync("k", cancellationToken);
        _ = await Assert.That(removed).IsTrue();
        _ = await Assert.That((await cache.GetValueAsync("k", cancellationToken)).Found).IsFalse();
    }
}
