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
/// With group commit on, a touch, an expiration removal and an update apply exactly the entry they journal, a skipped mutation releases
/// the same-key guard, and a snapshot cut between the frames replays to the same state.
/// </summary>
/// <remarks>Recovery reads the wall clock, so the write clock starts two hours in the past and the deadlines are measured in hours.</remarks>
[Immutable]
public sealed class JournalDecidedEntryGroupCommitTests : IsolatedStorageTestBase
{
    private const string CacheName = JournalReplayKit.CacheName;
    private const string Key = JournalReplayKit.Key;

    private static readonly TimeSpan ExtendedTtl = TimeSpan.FromHours(3);
    private static readonly TimeSpan OriginalTtl = TimeSpan.FromHours(1);
    private static readonly TimeSpan PastStart = TimeSpan.FromHours(-2);

    private static readonly Func<JournalLoggingCacheDecorator<string>, CancellationToken, ValueTask> PersistTail =
        static async (cache, ct) => _ = await cache.RemoveExpirationAsync(UnitMutationOpIds.Default, CacheName, Key, ct);

    private static readonly Func<JournalLoggingCacheDecorator<string>, CancellationToken, ValueTask> TouchTail =
        static async (cache, ct) => _ = await cache.TouchAsync(UnitMutationOpIds.Default, CacheName, Key, ExtendedTtl, ct);

    private readonly Meter _testMeter = new("test");

    /// <inheritdoc />
    protected override string TempDirectoryName => "squirix-journal-decided-group";

    private JournalReplayKit Kit => new(Dir, _testMeter, true);

    /// <summary>A skipped expiration removal releases the same-key guard, so an immediate set of the key succeeds.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PersistSkipReleasesKeyGuard(CancellationToken cancellationToken)
    {
        await using var session = await Kit.OpenAsync(JournalReplayKit.CreateWriteClock(PastStart), cancellationToken);
        await session.Cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, Key, new NodeCacheEntry<string>("v"), cancellationToken);

        _ = await Assert.That(await session.Cache.RemoveExpirationAsync(UnitMutationOpIds.Default, CacheName, Key, cancellationToken)).IsFalse();
        await session.Cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, Key, new NodeCacheEntry<string>("w"), cancellationToken);

        _ = await Assert.That((await session.Physical.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken))!.Value).IsEqualTo("w");
    }

    /// <summary>The expiration removal applies the journaled entry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PersistAppliesJournaledEntry(CancellationToken cancellationToken)
    {
        var memory = await MutateAndReadMemoryAsync(
            static (cache, ct) => cache.RemoveExpirationAsync(UnitMutationOpIds.Default, CacheName, Key, ct),
            cancellationToken);

        await AssertEqualsLastJournaledPutAsync(memory, cancellationToken);
        _ = await Assert.That(memory.ExpiresUtc).IsNull();
    }

    /// <summary>A snapshot cut before the tail mutation replays to the state the tail journals.</summary>
    /// <param name="persist">Whether the tail removes the expiration; otherwise it extends it.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task SnapshotCutBetweenFramesMatchesReplay(bool persist, CancellationToken cancellationToken)
    {
        var start = await Kit.WriteSnapshotThenTailAsync(
            PastStart,
            OriginalTtl,
            persist ? PersistTail : TouchTail,
            cancellationToken);

        var recovered = await Kit.RecoverAsync(JournalReplayKit.CreateRestartClock(), false, cancellationToken);

        var entry = await recovered.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken);
        _ = await Assert.That(entry).IsNotNull();
        _ = await Assert.That(entry!.ExpiresUtc).IsEqualTo(persist ? null : start.Add(ExtendedTtl));
    }

    /// <summary>A skipped touch releases the same-key guard, so an immediate set of the key succeeds.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TouchSkipReleasesKeyGuard(CancellationToken cancellationToken)
    {
        await using var session = await Kit.OpenAsync(JournalReplayKit.CreateWriteClock(PastStart), cancellationToken);

        _ = await Assert.That(await session.Cache.TouchAsync(UnitMutationOpIds.Default, CacheName, Key, ExtendedTtl, cancellationToken)).IsFalse();
        await session.Cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, Key, new NodeCacheEntry<string>("w"), cancellationToken);

        _ = await Assert.That((await session.Physical.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken))!.Value).IsEqualTo("w");
    }

    /// <summary>The touch applies the journaled entry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TouchAppliesJournaledEntry(CancellationToken cancellationToken)
    {
        var memory = await MutateAndReadMemoryAsync(
            static (cache, ct) => cache.TouchAsync(UnitMutationOpIds.Default, CacheName, Key, ExtendedTtl, ct),
            cancellationToken);

        await AssertEqualsLastJournaledPutAsync(memory, cancellationToken);
    }

    /// <summary>The update applies the journaled entry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UpdateAppliesJournaledEntry(CancellationToken cancellationToken)
    {
        var memory = await MutateAndReadMemoryAsync(
            static (cache, ct) => cache.UpdateAsync(UnitMutationOpIds.Default, CacheName, Key, "v2", ct),
            cancellationToken);

        await AssertEqualsLastJournaledPutAsync(memory, cancellationToken);
        _ = await Assert.That(memory.Value).IsEqualTo("v2");
    }

    /// <inheritdoc />
    protected override void DisposeManaged()
    {
        _testMeter.Dispose();
        base.DisposeManaged();
    }

    private async Task AssertEqualsLastJournaledPutAsync(NodeCacheEntry<string> memory, CancellationToken cancellationToken)
    {
        var journaled = Kit.ReadLastJournaledPut(cancellationToken);

        _ = await Assert.That(journaled.Value).IsEqualTo(memory.Value);
        _ = await Assert.That(journaled.Version).IsEqualTo(memory.Version);
        _ = await Assert.That(journaled.ExpiresUtc).IsEqualTo(memory.ExpiresUtc);
        _ = await Assert.That(journaled.Tags).IsNotNull();
        _ = await Assert.That(journaled.Tags!["team"]).IsEqualTo(memory.Tags!["team"]);
    }

    private async Task<NodeCacheEntry<string>> MutateAndReadMemoryAsync(
        Func<JournalLoggingCacheDecorator<string>, CancellationToken, ValueTask<bool>> mutate,
        CancellationToken cancellationToken)
    {
        var clock = JournalReplayKit.CreateWriteClock(PastStart);
        NodeCacheEntry<string>? memory;
        await using (var session = await Kit.OpenAsync(clock, cancellationToken))
        {
            var tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["team"] = "a" }.ToFrozenDictionary(StringComparer.Ordinal);
            var seeded = new NodeCacheEntry<string>("v1", 3, clock.GetUtcNow().UtcDateTime.Add(OriginalTtl), tags: tags);
            await session.Cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, Key, seeded, cancellationToken);
            _ = await Assert.That(await mutate(session.Cache, cancellationToken)).IsTrue();
            memory = await session.Physical.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken);
        }

        _ = await Assert.That(memory).IsNotNull();
        return memory!;
    }
}
