using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// A deadline off the whole-millisecond boundary is rounded up once, so the memory deadline, the journaled deadline and the replayed
/// deadline are the same instant.
/// </summary>
/// <remarks>Recovery reads the wall clock, so the write clock starts two hours in the past and the deadlines are measured in hours.</remarks>
[Immutable]
public sealed class JournalDeadlinePinningReplayTests : IsolatedStorageTestBase
{
    private const string CacheName = JournalReplayKit.CacheName;
    private const string Key = JournalReplayKit.Key;

    private static readonly TimeSpan PastStart = TimeSpan.FromHours(-2);
    private static readonly TimeSpan Ttl = TimeSpan.FromHours(3);

    private readonly Meter _testMeter = new("test");

    /// <inheritdoc />
    protected override string TempDirectoryName => "squirix-journal-pinning";

    private JournalReplayKit Kit => new(Dir, _testMeter);

    /// <summary>A set with an absolute deadline one tick past a millisecond boundary keeps the rounded-up deadline in memory, journal and replay.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SetPinsAbsoluteDeadlineEverywhere(CancellationToken cancellationToken)
    {
        var written = await Kit.WriteAsync(
            static (cache, clock, ct) => cache.SetEntryAsync(
                UnitMutationOpIds.Default,
                CacheName,
                Key,
                new NodeCacheEntry<string>("v", expiresUtc: clock.GetUtcNow().UtcDateTime.Add(Ttl)),
                ct),
            TimeSpan.Zero,
            PastStart,
            1,
            cancellationToken);

        await AssertPinnedEverywhereAsync(written, cancellationToken);
    }

    /// <summary>A touch decided one tick past a millisecond boundary keeps the rounded-up deadline in memory, journal and replay.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TouchPinsDeadlineEverywhere(CancellationToken cancellationToken)
    {
        var written = await Kit.WriteAsync(
            static async (cache, ct) =>
            {
                await cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, Key, new NodeCacheEntry<string>("v"), ct);
                _ = await Assert.That(await cache.TouchAsync(UnitMutationOpIds.Default, CacheName, Key, Ttl, ct)).IsTrue();
            },
            TimeSpan.Zero,
            PastStart,
            1,
            cancellationToken);

        await AssertPinnedEverywhereAsync(written, cancellationToken);
    }

    /// <inheritdoc />
    protected override void DisposeManaged()
    {
        _testMeter.Dispose();
        base.DisposeManaged();
    }

    private async Task AssertPinnedEverywhereAsync((DateTime WriteStart, NodeCacheEntry<string>? Memory) written, CancellationToken cancellationToken)
    {
        // The clock starts one tick past a millisecond boundary, so the deadline is one tick past one too and rounds up to the next millisecond.
        var expected = written.WriteStart.AddTicks(-1).Add(Ttl).AddMilliseconds(1);
        var journaled = Kit.ReadLastJournaledPut(cancellationToken);
        var recovered = await Kit.RecoverAsync(TimeProvider.System, false, cancellationToken);
        var replayed = await recovered.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken);

        _ = await Assert.That(written.Memory!.ExpiresUtc).IsEqualTo(expected);
        _ = await Assert.That(journaled.ExpiresUtc).IsEqualTo(expected);
        _ = await Assert.That(replayed!.ExpiresUtc).IsEqualTo(expected);
    }
}
