using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using Xunit;

namespace Squirix.Server.UnitTests.Core;

/// <summary>Unit tests covering expiration operations: TouchAsync and RemoveExpirationAsync.</summary>
[Immutable]
public sealed class ExpirationOperationsTests : ServerUnitTestBase
{
    /// <summary>Verifies RemoveExpirationAsync removes expiration for an existing expiring key and the value remains after the old expiration window.</summary>
    [Fact]
    public async Task RemoveExpiryClearsOnlyTheExpiryAsync()
    {
        var cache = new PhysicalCache<string>();
        await cache.SetAsync(
            CacheKey.Default("k1"),
            new NodeCacheEntry<string> { Value = "v", ExpiresUtc = DateTime.UtcNow.AddMilliseconds(150), Version = 1 },
            DefaultCancellationToken);

        var entryBefore = await cache.GetEntryAsync(CacheKey.Default("k1"), DefaultCancellationToken);
        Assert.NotNull(entryBefore);
        _ = Assert.NotNull(entryBefore.ExpiresUtc);

        var ok = await cache.RemoveExpirationAsync(CacheKey.Default("k1"), DefaultCancellationToken);
        Assert.True(ok);

        var entryAfter = await cache.GetEntryAsync(CacheKey.Default("k1"), DefaultCancellationToken);
        Assert.NotNull(entryAfter);
        Assert.Null(entryAfter.ExpiresUtc);
        await Task.Delay(200, DefaultCancellationToken);
        var found = await cache.GetValueAsync(CacheKey.Default("k1"), DefaultCancellationToken);
        Assert.True(found.Found);
        Assert.Equal("v", found.Value);
    }

    /// <summary>Verifies RemoveExpirationAsync does not resurrect an already expired entry.</summary>
    [Fact]
    public async Task RemoveExpiryWontResurrectExpiredEntry()
    {
        var timeProvider = new FakeTimeProvider();
        var cache = new PhysicalCache<string>(timeProvider);

        await cache.SetAsync(
            CacheKey.Default("k"),
            new NodeCacheEntry<string>
            {
                Value = "v",
                Expiration = TimeSpan.FromMilliseconds(10),
            },
            DefaultCancellationToken);

        timeProvider.Advance(TimeSpan.FromMilliseconds(30));

        Assert.False(await cache.RemoveExpirationAsync(CacheKey.Default("k"), DefaultCancellationToken));
        var result = await cache.GetValueAsync(CacheKey.Default("k"), DefaultCancellationToken);
        Assert.False(result.Found);
    }

    /// <summary>Verifies RemoveExpirationAsync on a non-expiring key returns false and leaves the value and absence of expiration unchanged.</summary>
    [Fact]
    public async Task RemoveExpiryNonExpiringFalseKeepsLive()
    {
        var cache = new PhysicalCache<string>();
        await cache.SetAsync(CacheKey.Default("k"), new NodeCacheEntry<string> { Value = "v", Version = 1 }, DefaultCancellationToken);

        Assert.False(await cache.RemoveExpirationAsync(CacheKey.Default("k"), DefaultCancellationToken));
        var entry = await cache.GetEntryAsync(CacheKey.Default("k"), DefaultCancellationToken);
        Assert.NotNull(entry);
        Assert.Equal("v", entry.Value);
        Assert.Null(entry.ExpiresUtc);
    }

    /// <summary>Verifies concurrent TouchAsync calls from separate workers on a live key all succeed with no spurious failure under contention.</summary>
    [Fact]
    public async Task ConcurrentTouchOnLiveKeyAlwaysSucceeds()
    {
        var cache = new PhysicalCache<string>();
        var key = CacheKey.Default("touch-race");
        await cache.SetAsync(
            key,
            new NodeCacheEntry<string> { Value = "v", Expiration = TimeSpan.FromHours(1), Version = 1 },
            DefaultCancellationToken);

        const int width = 32;
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var tasks = new Task<bool>[width];
        for (var i = 0; i < width; i++)
        {
            var state = new TouchRaceState(cache, key, gate.Task, DefaultCancellationToken);
            tasks[i] = Task.Factory.StartNew(
                RunTouchAfterGateAsync,
                state,
                DefaultCancellationToken,
                TaskCreationOptions.None,
                TaskScheduler.Default).Unwrap();
        }

        _ = gate.TrySetResult();
        var results = await Task.WhenAll(tasks);
        Assert.All(results, Assert.True);
    }

    /// <summary>Verifies RemoveExpirationAsync returns false for a missing key.</summary>
    [Fact]
    public async Task RemoveExpiryReturnsFalseForMissingKey()
    {
        var cache = new PhysicalCache<int>();
        Assert.False(await cache.RemoveExpirationAsync(CacheKey.Default("missing"), DefaultCancellationToken));
    }

    /// <summary>Verifies RemoveExpirationAsync removes expiration once and returns false when the key is already persistent.</summary>
    [Fact]
    public async Task RemoveExpiryReturnsFalseForPersistent()
    {
        var cache = new PhysicalCache<string>();
        await cache.SetAsync(
            CacheKey.Default("k"),
            new NodeCacheEntry<string>
            {
                Value = "v",
                Expiration = TimeSpan.FromMinutes(1),
            },
            DefaultCancellationToken);

        Assert.True(await cache.RemoveExpirationAsync(CacheKey.Default("k"), DefaultCancellationToken));
        Assert.False(await cache.RemoveExpirationAsync(CacheKey.Default("k"), DefaultCancellationToken));
        var entry = await cache.GetEntryAsync(CacheKey.Default("k"), DefaultCancellationToken);
        Assert.NotNull(entry);
        Assert.Equal("v", entry.Value);
        Assert.Null(entry.ExpiresUtc);
    }

    private static async Task<bool> RunTouchAfterGateAsync(object? state)
    {
        var race = state as TouchRaceState ?? ThrowHelper.Throw<TouchRaceState>(new InvalidOperationException());
        await race.Gate.WaitAsync(race.CancellationToken).ConfigureAwait(false);
        return await race.Cache.TouchAsync(race.Key, TimeSpan.FromMinutes(5), race.CancellationToken).ConfigureAwait(false);
    }

    [Immutable]
    private sealed class TouchRaceState
    {
        internal TouchRaceState(PhysicalCache<string> cache, CacheKey key, Task gate, CancellationToken cancellationToken)
        {
            Cache = cache;
            Key = key;
            Gate = gate;
            CancellationToken = cancellationToken;
        }

        internal PhysicalCache<string> Cache { get; }

        internal CancellationToken CancellationToken { get; }

        internal Task Gate { get; }

        internal CacheKey Key { get; }
    }
}
