using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Core;

/// <summary>Unit tests covering expiration operations: TouchAsync and RemoveExpirationAsync.</summary>
[Immutable]
public sealed class ExpirationOperationsTests : ServerUnitTestBase
{
    /// <summary>Verifies concurrent TouchAsync calls from separate workers on a live key all succeed with no spurious failure under contention.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConcurrentTouchOnLiveKeyAlwaysSucceeds(CancellationToken cancellationToken)
    {
        var cache = new PhysicalCache<string>();
        var key = CacheKey.Default("touch-race");
        await cache.SetAsync(key, new NodeCacheEntry<string> { Value = "v", Expiration = TimeSpan.FromHours(1), Version = 1 }, cancellationToken);

        const int width = 32;
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var tasks = new Task<bool>[width];
        for (var i = 0; i < width; i++)
        {
            var state = new TouchRaceState(cache, key, gate.Task, cancellationToken);
            tasks[i] = Task.Factory.StartNew(RunTouchAfterGateAsync, state, cancellationToken, TaskCreationOptions.None, TaskScheduler.Default).Unwrap();
        }

        _ = gate.TrySetResult();
        var results = await Task.WhenAll(tasks);
        _ = await Assert.That(results).All(static result => result);
    }

    /// <summary>Verifies RemoveExpirationAsync removes expiration for an existing expiring key and the value remains after the old expiration window.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveExpiryClearsOnlyTheExpiryAsync(CancellationToken cancellationToken)
    {
        var cache = new PhysicalCache<string>();
        await cache.SetAsync(CacheKey.Default("k1"), new NodeCacheEntry<string> { Value = "v", ExpiresUtc = DateTime.UtcNow.AddMilliseconds(150), Version = 1 }, cancellationToken);

        var entryBefore = await cache.GetEntryAsync(CacheKey.Default("k1"), cancellationToken);
        _ = await Assert.That(entryBefore).IsNotNull();
        _ = await Assert.That(entryBefore.ExpiresUtc).IsNotNull();

        var ok = await cache.RemoveExpirationAsync(CacheKey.Default("k1"), cancellationToken);
        _ = await Assert.That(ok).IsTrue();

        var entryAfter = await cache.GetEntryAsync(CacheKey.Default("k1"), cancellationToken);
        _ = await Assert.That(entryAfter).IsNotNull();
        _ = await Assert.That(entryAfter.ExpiresUtc).IsNull();
        await Task.Delay(200, cancellationToken);
        var found = await cache.GetValueAsync(CacheKey.Default("k1"), cancellationToken);
        _ = await Assert.That(found.Found).IsTrue();
        _ = await Assert.That(found.Value).IsEqualTo("v");
    }

    /// <summary>Verifies RemoveExpirationAsync on a non-expiring key returns false and leaves the value and absence of expiration unchanged.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveExpiryNonExpiringFalseKeepsLive(CancellationToken cancellationToken)
    {
        var cache = new PhysicalCache<string>();
        await cache.SetAsync(CacheKey.Default("k"), new NodeCacheEntry<string> { Value = "v", Version = 1 }, cancellationToken);

        _ = await Assert.That(await cache.RemoveExpirationAsync(CacheKey.Default("k"), cancellationToken)).IsFalse();
        var entry = await cache.GetEntryAsync(CacheKey.Default("k"), cancellationToken);
        _ = await Assert.That(entry).IsNotNull();
        _ = await Assert.That(entry.Value).IsEqualTo("v");
        _ = await Assert.That(entry.ExpiresUtc).IsNull();
    }

    /// <summary>Verifies RemoveExpirationAsync returns false for a missing key.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveExpiryReturnsFalseForMissingKey(CancellationToken cancellationToken)
    {
        var cache = new PhysicalCache<int>();
        _ = await Assert.That(await cache.RemoveExpirationAsync(CacheKey.Default("missing"), cancellationToken)).IsFalse();
    }

    /// <summary>Verifies RemoveExpirationAsync removes expiration once and returns false when the key is already persistent.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveExpiryReturnsFalseForPersistent(CancellationToken cancellationToken)
    {
        var cache = new PhysicalCache<string>();
        await cache.SetAsync(
            CacheKey.Default("k"),
            new NodeCacheEntry<string>
            {
                Value = "v",
                Expiration = TimeSpan.FromMinutes(1),
            },
            cancellationToken);

        _ = await Assert.That(await cache.RemoveExpirationAsync(CacheKey.Default("k"), cancellationToken)).IsTrue();
        _ = await Assert.That(await cache.RemoveExpirationAsync(CacheKey.Default("k"), cancellationToken)).IsFalse();
        var entry = await cache.GetEntryAsync(CacheKey.Default("k"), cancellationToken);
        _ = await Assert.That(entry).IsNotNull();
        _ = await Assert.That(entry.Value).IsEqualTo("v");
        _ = await Assert.That(entry.ExpiresUtc).IsNull();
    }

    /// <summary>Verifies RemoveExpirationAsync does not resurrect an already expired entry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveExpiryWontResurrectExpiredEntry(CancellationToken cancellationToken)
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
            cancellationToken);

        timeProvider.Advance(TimeSpan.FromMilliseconds(30));

        _ = await Assert.That(await cache.RemoveExpirationAsync(CacheKey.Default("k"), cancellationToken)).IsFalse();
        var result = await cache.GetValueAsync(CacheKey.Default("k"), cancellationToken);
        _ = await Assert.That(result.Found).IsFalse();
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
