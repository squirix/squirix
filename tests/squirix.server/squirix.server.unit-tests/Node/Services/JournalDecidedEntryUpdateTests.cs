using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.LocalCache;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// Input validation of a local-owner update, the pinned deadline it journals, and the liveness rules of the raw decision read that
/// touch, expiration removal and update share.
/// </summary>
/// <remarks>Recovery reads the wall clock, so the write clock starts two hours in the past and the deadlines are measured in hours.</remarks>
[Immutable]
public sealed class JournalDecidedEntryUpdateTests : IsolatedStorageTestBase
{
    private const string CacheName = JournalReplayKit.CacheName;
    private const string Key = JournalReplayKit.Key;

    private static readonly TimeSpan PastStart = TimeSpan.FromHours(-2);
    private static readonly TimeSpan Ttl = TimeSpan.FromHours(3);

    private readonly Meter _testMeter = new("test");

    /// <inheritdoc />
    protected override string TempDirectoryName => "squirix-journal-decided-update";

    /// <summary>A touch of an entry that expires exactly now skips without a frame and drops the dead node from memory.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExpiredAtNowIsSkippedAndDropped(CancellationToken cancellationToken)
    {
        var kit = new JournalReplayKit(Dir, _testMeter);
        var clock = JournalReplayKit.CreateWriteClock(PastStart);
        await using var session = await kit.OpenAsync(clock, cancellationToken);
        ILocalCacheStats stats = session.Physical;
        await session.Physical.SetAsync(new CacheKey(CacheName, Key), new NodeCacheEntry<string>("v", expiresUtc: clock.GetUtcNow().UtcDateTime), cancellationToken);
        _ = await Assert.That(stats.EntryCount).IsEqualTo(1);

        _ = await Assert.That(await session.Cache.TouchAsync(UnitMutationOpIds.Default, CacheName, Key, Ttl, cancellationToken)).IsFalse();

        _ = await Assert.That(stats.EntryCount).IsEqualTo(0);
        _ = await Assert.That(kit.CountJournalRecords(cancellationToken)).IsEqualTo(0);
    }

    /// <summary>An entry that expires one tick after now is still live for the decision, so a touch applies.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LiveByOneTickIsTouched(CancellationToken cancellationToken)
    {
        var kit = new JournalReplayKit(Dir, _testMeter);
        var clock = JournalReplayKit.CreateWriteClock(PastStart);
        await using var session = await kit.OpenAsync(clock, cancellationToken);
        await session.Physical.SetAsync(new CacheKey(CacheName, Key), new NodeCacheEntry<string>("v", expiresUtc: clock.GetUtcNow().UtcDateTime.AddTicks(1)), cancellationToken);

        _ = await Assert.That(await session.Cache.TouchAsync(UnitMutationOpIds.Default, CacheName, Key, Ttl, cancellationToken)).IsTrue();
    }

    /// <summary>An update of a missing key with a value over the entry limit is rejected before the existence check.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MissingKeyOversizedUpdateIsRejected(CancellationToken cancellationToken)
    {
        var kit = new JournalReplayKit(Dir, _testMeter);
        await using var session = await kit.OpenAsync(JournalReplayKit.CreateWriteClock(PastStart), cancellationToken);
        var oversized = new string('x', EntryLimits.MaxEntrySizeBytes);

        var failure = await NodeAsyncAssert.ThrowsAsync<SquirixException, bool>(session.Cache.UpdateAsync(UnitMutationOpIds.Default, CacheName, Key, oversized, cancellationToken));

        _ = await Assert.That(failure.Code).IsEqualTo(SquirixErrorCode.PayloadTooLarge);
        _ = await Assert.That(kit.CountJournalRecords(cancellationToken)).IsEqualTo(0);
    }

    /// <summary>A value within the limit whose entry with tags is over it is rejected under the gate: no frame, and the key guard is released.</summary>
    /// <param name="groupCommit">Whether group commit is on.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task OversizedWithTagsUpdateIsRejected(bool groupCommit, CancellationToken cancellationToken)
    {
        var kit = new JournalReplayKit(Dir, _testMeter, groupCommit);
        await using var session = await kit.OpenAsync(JournalReplayKit.CreateWriteClock(PastStart), cancellationToken);
        var tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["team"] = "a" }.ToFrozenDictionary(StringComparer.Ordinal);
        await session.Cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, Key, new NodeCacheEntry<string>("v", tags: tags), cancellationToken);
        var framesBefore = kit.CountJournalRecords(cancellationToken);

        // The encoded value alone is exactly the limit, so only the tags and the entry header push the assembled entry over it.
        var nearLimit = new string('x', EntryLimits.MaxEntrySizeBytes - 5);
        var failure = await NodeAsyncAssert.ThrowsAsync<SquirixException, bool>(session.Cache.UpdateAsync(UnitMutationOpIds.Default, CacheName, Key, nearLimit, cancellationToken));

        _ = await Assert.That(failure.Code).IsEqualTo(SquirixErrorCode.PayloadTooLarge);
        _ = await Assert.That(kit.CountJournalRecords(cancellationToken)).IsEqualTo(framesBefore);
        await session.Cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, Key, new NodeCacheEntry<string>("w"), cancellationToken);
        _ = await Assert.That((await session.Physical.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken))!.Value).IsEqualTo("w");
    }

    /// <summary>An update pins an off-millisecond deadline the entry already holds, so memory, journal and replay agree on the rounded-up instant.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UpdatePinsHeldDeadlineEverywhere(CancellationToken cancellationToken)
    {
        var kit = new JournalReplayKit(Dir, _testMeter);
        var clock = JournalReplayKit.CreateWriteClock(PastStart, 1);
        var start = clock.GetUtcNow().UtcDateTime;
        NodeCacheEntry<string>? memory;
        await using (var session = await kit.OpenAsync(clock, cancellationToken))
        {
            await session.Physical.SetAsync(new CacheKey(CacheName, Key), new NodeCacheEntry<string>("v", expiresUtc: start.Add(Ttl)), cancellationToken);
            _ = await Assert.That(await session.Cache.UpdateAsync(UnitMutationOpIds.Default, CacheName, Key, "w", cancellationToken)).IsTrue();
            memory = await session.Physical.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken);
        }

        var expected = start.AddTicks(-1).Add(Ttl).AddMilliseconds(1);
        var journaled = kit.ReadLastJournaledPut(cancellationToken);
        var recovered = await kit.RecoverAsync(TimeProvider.System, false, cancellationToken);
        var replayed = await recovered.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken);

        _ = await Assert.That(memory!.ExpiresUtc).IsEqualTo(expected);
        _ = await Assert.That(journaled.ExpiresUtc).IsEqualTo(expected);
        _ = await Assert.That(replayed!.ExpiresUtc).IsEqualTo(expected);
    }

    /// <inheritdoc />
    protected override void DisposeManaged()
    {
        _testMeter.Dispose();
        base.DisposeManaged();
    }
}
