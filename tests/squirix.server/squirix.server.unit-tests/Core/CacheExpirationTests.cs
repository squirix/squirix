using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Core;

/// <summary>
/// Unit tests for <see cref="PhysicalCache{T}" /> expiration and expiration handling.
/// Verifies both relative expiration (<see cref="NodeCacheEntry{T}.Expiration" />) and absolute
/// expiration (<see cref="NodeCacheEntry{T}.ExpiresUtc" />).
/// </summary>
[Immutable]
public sealed class CacheExpirationTests : ServerUnitTestBase
{
    /// <summary>Verifies TryAddAsync stores absolute expiration metadata that GetEntryAsync can read back.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AddPreservesAbsoluteExpiry(CancellationToken cancellationToken)
    {
        var timeProvider = new FakeTimeProvider();
        var cache = new PhysicalCache<string>(timeProvider);

        var expiresUtc = timeProvider.GetUtcNow().UtcDateTime.AddSeconds(5);
        var added = await cache.TryAddAsync(CacheKey.Default("k"), new NodeCacheEntry<string> { Value = "v", ExpiresUtc = expiresUtc }, cancellationToken);

        _ = await Assert.That(added).IsTrue();

        var stored = await cache.GetEntryAsync(CacheKey.Default("k"), cancellationToken);
        _ = await Assert.That(stored).IsNotNull();
        _ = await Assert.That(stored.ExpiresUtc).IsEqualTo(expiresUtc);
        var remaining = stored.ExpiresUtc!.Value - timeProvider.GetUtcNow().UtcDateTime;
        _ = await Assert.That(remaining > TimeSpan.Zero).IsTrue();
        _ = await Assert.That(remaining <= TimeSpan.FromSeconds(5)).IsTrue();
    }

    /// <summary>Verifies TryAddAsync treats an expired existing entry as absent and inserts a new value.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AddReplacesExpiredEntry(CancellationToken cancellationToken)
    {
        var timeProvider = new FakeTimeProvider();
        var cache = new PhysicalCache<string>(timeProvider);

        _ = await Assert.That(
                             await cache.TryAddAsync(
                                 CacheKey.Default("k"),
                                 new NodeCacheEntry<string> { Value = "expired", Expiration = TimeSpan.FromMilliseconds(10) },
                                 cancellationToken))
                        .IsTrue();

        timeProvider.Advance(TimeSpan.FromMilliseconds(25));

        _ = await Assert.That(await cache.TryAddAsync(CacheKey.Default("k"), new NodeCacheEntry<string> { Value = "new" }, cancellationToken)).IsTrue();
        _ = await Assert.That((await cache.GetValueAsync(CacheKey.Default("k"), cancellationToken)).Value).IsEqualTo("new");
    }

    /// <summary>When the absolute deadline is earlier than the relative one, the absolute one wins (issue #445).</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EarliestAbsoluteDeadlineWins(CancellationToken cancellationToken)
    {
        var timeProvider = new FakeTimeProvider();
        var cache = new PhysicalCache<string>(timeProvider);
        var now = timeProvider.GetUtcNow().UtcDateTime;

        await cache.SetAsync(
            CacheKey.Default("k"),
            new NodeCacheEntry<string> { Value = "v", Expiration = TimeSpan.FromMinutes(5), ExpiresUtc = now.AddMinutes(1) },
            cancellationToken);

        var stored = await cache.GetEntryAsync(CacheKey.Default("k"), cancellationToken);
        _ = await Assert.That(stored).IsNotNull();
        _ = await Assert.That(stored.ExpiresUtc).IsEqualTo(now.AddMinutes(1));

        timeProvider.Advance(TimeSpan.FromMinutes(2));
        _ = await Assert.That((await cache.GetValueAsync(CacheKey.Default("k"), cancellationToken)).Found).IsFalse();
    }

    /// <summary>Both relative and absolute expiration are respected; the earlier deadline wins (issue #445).</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EarliestDeadlineWins(CancellationToken cancellationToken)
    {
        var timeProvider = new FakeTimeProvider();
        var cache = new PhysicalCache<string>(timeProvider);
        var now = timeProvider.GetUtcNow().UtcDateTime;

        await cache.SetAsync(
            CacheKey.Default("k"),
            new NodeCacheEntry<string> { Value = "v", Expiration = TimeSpan.FromMinutes(1), ExpiresUtc = now.AddMinutes(5) },
            cancellationToken);

        var stored = await cache.GetEntryAsync(CacheKey.Default("k"), cancellationToken);
        _ = await Assert.That(stored).IsNotNull();
        _ = await Assert.That(stored.ExpiresUtc).IsEqualTo(now.AddMinutes(1));

        timeProvider.Advance(TimeSpan.FromMinutes(2));
        _ = await Assert.That((await cache.GetValueAsync(CacheKey.Default("k"), cancellationToken)).Found).IsFalse();
    }

    /// <summary>Ensures entries expire correctly when inserted with either relative expiration or absolute expiration.</summary>
    /// <param name="expirationMs">expiration in milliseconds when using relative expiration.</param>
    /// <param name="sleepMs">Delay before checking presence in milliseconds.</param>
    /// <param name="useAbsoluteExpires">If <see langword="true" />, uses <see cref="NodeCacheEntry{T}.ExpiresUtc" />; otherwise <see cref="NodeCacheEntry{T}.Expiration" />.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(10, 25, true)]
    [Arguments(10, 25, false)]
    [Arguments(25, 60, true)]
    [Arguments(25, 60, false)]
    public async Task ExpirationSyncTheoryTest(int expirationMs, int sleepMs, bool useAbsoluteExpires, CancellationToken cancellationToken)
    {
        var timeProvider = new FakeTimeProvider();
        var cache = new PhysicalCache<string>(timeProvider);

        var entry = useAbsoluteExpires ? new NodeCacheEntry<string> { Value = "v", ExpiresUtc = timeProvider.GetUtcNow().UtcDateTime.AddMilliseconds(expirationMs) }
            : new NodeCacheEntry<string> { Value = "v", Expiration = TimeSpan.FromMilliseconds(expirationMs) };

        await cache.SetAsync(CacheKey.Default("k"), entry, cancellationToken);
        _ = await Assert.That((await cache.GetValueAsync(CacheKey.Default("k"), cancellationToken)).Found).IsTrue();

        timeProvider.Advance(TimeSpan.FromMilliseconds(sleepMs));
        _ = await Assert.That((await cache.GetValueAsync(CacheKey.Default("k"), cancellationToken)).Found).IsFalse();
    }

    /// <summary>
    /// Verifies whether an entry should exist or be expired after a fixed delay,
    /// based on expiration and/or absolute expiration configuration.
    /// </summary>
    /// <param name="expirationMs">expiration in milliseconds (nullable).</param>
    /// <param name="expiresMs">Absolute expiration in milliseconds relative now (nullable).</param>
    /// <param name="shouldStillExist">Expected presence of the entry after the delay.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(null, null, true)]
    [Arguments(50, null, false)]
    [Arguments(null, 50, false)]
    public async Task PresenceAfterDelaySyncTheoryTest(int? expirationMs, int? expiresMs, bool shouldStillExist, CancellationToken cancellationToken)
    {
        var timeProvider = new FakeTimeProvider();
        var cache = new PhysicalCache<string>(timeProvider);

        var entry = new NodeCacheEntry<string>
        {
            Value = "v",
            Expiration = expirationMs != null ? TimeSpan.FromMilliseconds(expirationMs.Value) : null,
            ExpiresUtc = expiresMs != null ? timeProvider.GetUtcNow().UtcDateTime.AddMilliseconds(expiresMs.Value) : null,
        };

        await cache.SetAsync(CacheKey.Default("k"), entry, cancellationToken);
        timeProvider.Advance(TimeSpan.FromMilliseconds(60));

        var exists = (await cache.GetValueAsync(CacheKey.Default("k"), cancellationToken)).Found;
        _ = await Assert.That(exists).IsEqualTo(shouldStillExist);
    }

    /// <summary>Verifies remove operations treat expired keys as missing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveExpiredKeyReturnsFalse(CancellationToken cancellationToken)
    {
        var timeProvider = new FakeTimeProvider();
        var cache = new PhysicalCache<string>(timeProvider);

        await cache.SetAsync(CacheKey.Default("k"), new NodeCacheEntry<string> { Value = "v", Expiration = TimeSpan.FromMilliseconds(10) }, cancellationToken);

        timeProvider.Advance(TimeSpan.FromMilliseconds(25));

        _ = await Assert.That((await cache.RemoveAsync(CacheKey.Default("k"), cancellationToken)).Removed).IsFalse();
        _ = await Assert.That((await cache.RemoveAsync(CacheKey.Default("k"), cancellationToken)).Removed).IsFalse();
    }

    /// <summary>Relative expiration exceeding the DateTime range saturates instead of throwing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SetAsyncSaturatesHugeRelativeExpiration(CancellationToken cancellationToken)
    {
        var timeProvider = new FakeTimeProvider();
        var cache = new PhysicalCache<string>(timeProvider);

        await cache.SetAsync(CacheKey.Default("k"), new NodeCacheEntry<string> { Value = "v", Expiration = TimeSpan.MaxValue }, cancellationToken);

        var stored = await cache.GetEntryAsync(CacheKey.Default("k"), cancellationToken);
        _ = await Assert.That(stored).IsNotNull();
        _ = await Assert.That(stored.ExpiresUtc).IsEqualTo(DateTime.MaxValue);
    }

    /// <summary>TouchAsync with relative expiration exceeding the DateTime range saturates instead of throwing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TouchAsyncSaturatesHugeExpiration(CancellationToken cancellationToken)
    {
        var timeProvider = new FakeTimeProvider();
        var cache = new PhysicalCache<string>(timeProvider);

        await cache.SetAsync(CacheKey.Default("k"), new NodeCacheEntry<string> { Value = "v", Expiration = TimeSpan.FromMinutes(1) }, cancellationToken);
        _ = await Assert.That(await cache.TouchAsync(CacheKey.Default("k"), TimeSpan.MaxValue, cancellationToken)).IsTrue();

        var stored = await cache.GetEntryAsync(CacheKey.Default("k"), cancellationToken);
        _ = await Assert.That(stored).IsNotNull();
        _ = await Assert.That(stored.ExpiresUtc).IsEqualTo(DateTime.MaxValue);
    }
}
