using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// A local-owner touch, expiration removal or update journals the entry it decided, so replay, compaction and snapshot plus tail
/// recovery restore the state of the last frame of a key even after the deadline of the entry the frame replaced has passed.
/// </summary>
/// <remarks>Recovery reads the wall clock, so the write clock starts two hours in the past and the deadlines are measured in hours.</remarks>
[Immutable]
public sealed class JournalDecidedEntryReplayTests : IsolatedStorageTestBase
{
    private const string CacheName = JournalReplayKit.CacheName;
    private const string Key = JournalReplayKit.Key;

    private static readonly TimeSpan ExtendedTtl = TimeSpan.FromHours(3);
    private static readonly TimeSpan OriginalTtl = TimeSpan.FromHours(1);
    private static readonly TimeSpan PastStart = TimeSpan.FromHours(-2);

    private readonly Meter _testMeter = new("test");

    /// <inheritdoc />
    protected override string TempDirectoryName => "squirix-journal-decided-replay";

    private JournalReplayKit Kit => new(Dir, _testMeter);

    /// <summary>Removing the expiration before the deadline keeps the key without a deadline across a restart after the original deadline.</summary>
    /// <param name="compact">Whether the journal is compacted before recovery.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task PersistBeforeDeadlineSurvivesRestart(bool compact, CancellationToken cancellationToken)
    {
        _ = await WriteAsync(
            static async (cache, ct) =>
            {
                await cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, Key, new NodeCacheEntry<string>("v", expiration: OriginalTtl), ct);
                _ = await Assert.That(await cache.RemoveExpirationAsync(UnitMutationOpIds.Default, CacheName, Key, ct)).IsTrue();
            },
            cancellationToken);

        var recovered = await Kit.RecoverAsync(JournalReplayKit.CreateRestartClock(), compact, cancellationToken);

        var entry = await recovered.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken);
        _ = await Assert.That(entry).IsNotNull();
        _ = await Assert.That(entry!.Value).IsEqualTo("v");
        _ = await Assert.That(entry.ExpiresUtc).IsNull();
    }

    /// <summary>Removing the expiration after a snapshot cut keeps the key without a deadline although the snapshot entry has since expired.</summary>
    /// <param name="compact">Whether the journal is compacted before recovery.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task SnapshotThenPersistInTailSurvivesRestart(bool compact, CancellationToken cancellationToken)
    {
        _ = await Kit.WriteSnapshotThenTailAsync(
            PastStart,
            OriginalTtl,
            static async (cache, ct) => _ = await cache.RemoveExpirationAsync(UnitMutationOpIds.Default, CacheName, Key, ct),
            cancellationToken);

        var recovered = await Kit.RecoverAsync(JournalReplayKit.CreateRestartClock(), compact, cancellationToken);

        var entry = await recovered.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken);
        _ = await Assert.That(entry).IsNotNull();
        _ = await Assert.That(entry!.ExpiresUtc).IsNull();
    }

    /// <summary>Extending the expiration after a snapshot cut keeps the key until the extended deadline although the snapshot entry has since expired.</summary>
    /// <param name="compact">Whether the journal is compacted before recovery.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task SnapshotThenTouchInTailSurvivesRestart(bool compact, CancellationToken cancellationToken)
    {
        var start = await Kit.WriteSnapshotThenTailAsync(
            PastStart,
            OriginalTtl,
            static async (cache, ct) => _ = await cache.TouchAsync(UnitMutationOpIds.Default, CacheName, Key, ExtendedTtl, ct),
            cancellationToken);

        var recovered = await Kit.RecoverAsync(JournalReplayKit.CreateRestartClock(), compact, cancellationToken);

        var entry = await recovered.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken);
        _ = await Assert.That(entry).IsNotNull();
        _ = await Assert.That(entry!.ExpiresUtc).IsEqualTo(start.Add(ExtendedTtl));
    }

    /// <summary>A touch of an entry whose deadline passed changes nothing and writes no frame, so the key stays absent after a restart.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TouchAfterDeadlineIsNotJournaled(CancellationToken cancellationToken)
    {
        var memory = await Kit.WriteAsync(
            static async (cache, clock, ct) =>
            {
                await cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, Key, new NodeCacheEntry<string>("v", expiration: OriginalTtl), ct);
                clock.Advance(OriginalTtl + TimeSpan.FromMinutes(1));
                _ = await Assert.That(await cache.TouchAsync(UnitMutationOpIds.Default, CacheName, Key, ExtendedTtl, ct)).IsFalse();
            },
            TimeSpan.Zero,
            PastStart,
            0,
            cancellationToken);

        _ = await Assert.That(memory.Memory).IsNull();
        _ = await Assert.That(Kit.CountJournalRecords(cancellationToken)).IsEqualTo(1);
        var recovered = await Kit.RecoverAsync(JournalReplayKit.CreateRestartClock(), false, cancellationToken);
        _ = await Assert.That(await recovered.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken)).IsNull();
    }

    /// <summary>Extending the expiration before the deadline keeps the key until exactly the decided deadline across a restart after the original deadline.</summary>
    /// <param name="compact">Whether the journal is compacted before recovery.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task TouchBeforeDeadlineSurvivesRestart(bool compact, CancellationToken cancellationToken)
    {
        var written = await WriteAsync(
            static async (cache, ct) =>
            {
                await cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, Key, new NodeCacheEntry<string>("v", expiration: OriginalTtl), ct);
                _ = await Assert.That(await cache.TouchAsync(UnitMutationOpIds.Default, CacheName, Key, ExtendedTtl, ct)).IsTrue();
            },
            cancellationToken);

        var recovered = await Kit.RecoverAsync(JournalReplayKit.CreateRestartClock(), compact, cancellationToken);

        var entry = await recovered.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken);
        _ = await Assert.That(entry).IsNotNull();
        _ = await Assert.That(entry!.ExpiresUtc).IsEqualTo(written.Add(ExtendedTtl));
    }

    /// <summary>An update after a touch journals the touched deadline, not the original one.</summary>
    /// <param name="compact">Whether the journal is compacted before recovery.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task UpdateAfterTouchKeepsTouchedDeadline(bool compact, CancellationToken cancellationToken)
    {
        var written = await WriteAsync(
            static async (cache, ct) =>
            {
                await cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, Key, new NodeCacheEntry<string>("v1", expiration: OriginalTtl), ct);
                _ = await Assert.That(await cache.TouchAsync(UnitMutationOpIds.Default, CacheName, Key, ExtendedTtl, ct)).IsTrue();
                _ = await Assert.That(await cache.UpdateAsync(UnitMutationOpIds.Default, CacheName, Key, "v2", ct)).IsTrue();
            },
            cancellationToken);

        var recovered = await Kit.RecoverAsync(JournalReplayKit.CreateRestartClock(), compact, cancellationToken);

        var entry = await recovered.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken);
        _ = await Assert.That(entry).IsNotNull();
        _ = await Assert.That(entry!.Value).IsEqualTo("v2");
        _ = await Assert.That(entry.ExpiresUtc).IsEqualTo(written.Add(ExtendedTtl));
    }

    /// <summary>An update keeps the tags and the version of the entry across a restart.</summary>
    /// <param name="compact">Whether the journal is compacted before recovery.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task UpdateKeepsTagsAcrossRestart(bool compact, CancellationToken cancellationToken)
    {
        _ = await WriteAsync(
            static async (cache, ct) =>
            {
                var tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["team"] = "a" }.ToFrozenDictionary(StringComparer.Ordinal);
                await cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, Key, new NodeCacheEntry<string>("v1", 2, tags: tags), ct);
                _ = await Assert.That(await cache.UpdateAsync(UnitMutationOpIds.Default, CacheName, Key, "v2", ct)).IsTrue();
            },
            cancellationToken);

        var recovered = await Kit.RecoverAsync(JournalReplayKit.CreateRestartClock(), compact, cancellationToken);

        var entry = await recovered.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken);
        _ = await Assert.That(entry).IsNotNull();
        _ = await Assert.That(entry!.Value).IsEqualTo("v2");
        _ = await Assert.That(entry.Version).IsEqualTo(2);
        _ = await Assert.That(entry.Tags).IsNotNull();
        _ = await Assert.That(entry.Tags!["team"]).IsEqualTo("a");
    }

    /// <inheritdoc />
    protected override void DisposeManaged()
    {
        _testMeter.Dispose();
        base.DisposeManaged();
    }

    private async Task<DateTime> WriteAsync(Func<JournalLoggingCacheDecorator<string>, CancellationToken, ValueTask> mutate, CancellationToken cancellationToken)
    {
        var written = await Kit.WriteAsync(mutate, TimeSpan.Zero, PastStart, 0, cancellationToken);
        return written.WriteStart;
    }
}
