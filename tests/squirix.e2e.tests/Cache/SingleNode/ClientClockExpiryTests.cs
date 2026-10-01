using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.E2ETests.Fixtures;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.SingleNode;

/// <summary>
/// An absolute expiration is on the client clock. The node runs on a fake clock far from the real time the client reads, so these tests
/// see the client and the server disagree about the time and still observe the lifetime the client asked for.
/// </summary>
[Immutable]
public sealed class ClientClockExpiryTests : ClockTestBase
{
    private static readonly TimeSpan Lifetime = TimeSpan.FromHours(1);

    /// <summary>The same absolute instant passed to <c language="csharp">SetAsync</c> and to <c language="csharp">TouchAsync</c> gives the same lifetime.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SetAndTouchShareAbsoluteLifetime(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("client-clock-set-touch", cancellationToken);
        var expiresAt = DateTimeOffset.UtcNow + Lifetime;
        await cache.SetAsync("set", "v", Expiry.At(expiresAt), cancellationToken);
        await cache.SetAsync("touch", "v", cancellationToken: cancellationToken);
        _ = await Assert.That(await cache.TouchAsync("touch", expiresAt, cancellationToken)).IsTrue();

        // Wide margins: the client measures the lifetime when each call is sent, and real time passes between the calls.
        Clock.Advance(Lifetime - TimeSpan.FromMinutes(5));
        var setLive = (await cache.GetValueAsync("set", cancellationToken)).Found;
        var touchLive = (await cache.GetValueAsync("touch", cancellationToken)).Found;
        Clock.Advance(TimeSpan.FromMinutes(10));
        var setGone = !(await cache.GetValueAsync("set", cancellationToken)).Found;
        var touchGone = !(await cache.GetValueAsync("touch", cancellationToken)).Found;

        _ = await Assert.That(setLive && touchLive).IsTrue();
        _ = await Assert.That(setGone && touchGone).IsTrue();
    }

    /// <summary>An absolute expiration already in the past on the client clock is rejected instead of writing an entry nobody can read.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PastExpiresAtIsRejected(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("client-clock-past", cancellationToken);

        _ = await NodeAsyncAssert.ThrowsAnyAsync<ArgumentOutOfRangeException>(
            cache.SetAsync("k", "v", Expiry.At(DateTimeOffset.UtcNow.AddSeconds(-1)), cancellationToken));

        _ = await Assert.That((await cache.GetValueAsync("k", cancellationToken)).Found).IsFalse();
    }

    /// <summary>An absolute touch already in the past on the client clock is rejected.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PastTouchIsRejected(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("client-clock-past-touch", cancellationToken);
        await cache.SetAsync("k", "v", cancellationToken: cancellationToken);

        // The instant is checked when the call is made, so the refusal is synchronous.
        _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(cache, static c => _ = c.TouchAsync("k", DateTimeOffset.UtcNow.AddSeconds(-1), CancellationToken.None));
    }

    /// <summary>GetOrAdd refuses an absolute expiration already in the past before it runs the value factory.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PastGetOrAddSkipsFactory(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("client-clock-past-get-or-add", cancellationToken);
        var calls = new StrongBox<int>(0);

        // The options are checked when the call is made, like the key, so the refusal is synchronous.
        _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(
            (Cache: cache, Calls: calls),
            static state => _ = state.Cache.GetOrAddAsync(
                "k",
                (_, _) =>
                {
                    _ = Interlocked.Increment(ref state.Calls.Value);
                    return Task.FromResult<string?>("v");
                },
                Expiry.At(DateTimeOffset.UtcNow.AddSeconds(-1)),
                CancellationToken.None));

        _ = await Assert.That(Volatile.Read(ref calls.Value)).IsEqualTo(0);
    }

    /// <summary>An entry that never expires reads back with the largest deadline instead of overflowing on the client clock.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnboundedDeadlineReadsAsMaxValue(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("client-clock-unbounded", cancellationToken);
        await cache.SetAsync("k", "v", Expiry.At(DateTimeOffset.MaxValue), cancellationToken);

        var entry = await cache.GetEntryAsync("k", cancellationToken);

        _ = await Assert.That(entry.ExpiresUtc).IsEqualTo(DateTime.MaxValue);
    }

    /// <summary>A read reports the entry deadline on the client clock, not on the server clock.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EntryDeadlineReadsOnClientClock(CancellationToken cancellationToken)
    {
        var cache = await Client.GetCacheAsync<string>("client-clock-read", cancellationToken);
        await cache.SetAsync("k", "v", Expiry.In(TimeSpan.FromMinutes(10)), cancellationToken);

        var before = DateTime.UtcNow;
        var entry = await cache.GetEntryAsync("k", cancellationToken);
        var after = DateTime.UtcNow;

        _ = await Assert.That(entry.ExpiresUtc).IsNotNull();
        _ = await Assert.That(entry.ExpiresUtc!.Value).IsGreaterThan(before + TimeSpan.FromMinutes(9));
        _ = await Assert.That(entry.ExpiresUtc.Value).IsLessThanOrEqualTo(after + TimeSpan.FromMinutes(10));
    }
}
