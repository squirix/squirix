using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// Only the leader decides expiry, on its own clock, and a key becomes absent for readers only through a committed tombstone: leaders
/// whose clocks are apart agree on what a key holds, a follower keeps the entry until the tombstone, and a restart keeps the decision.
/// </summary>
public sealed class ReplicaExpiryClockTests : ServerUnitTestBase
{
    private const string CacheName = "cache";
    private const string Key = "k";

    private static readonly DateTimeOffset Start = new(2030, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private static readonly TimeSpan Skew = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan Ttl = TimeSpan.FromSeconds(60);

    /// <summary>A leader whose clock is behind reads a key the earlier leader expired as absent: the tombstone, not its clock, decides.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BehindLeaderKeepsCommittedExpiry(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-expiry-clock-behind");
        var ahead = new FakeTimeProvider(Start + Skew);
        await using (var aRegistry = await OpenRegistryAsync(dir, cancellationToken))
        await using (var a = new Leader(aRegistry, ahead))
        {
            await a.Committer.CommitSetAsync(NewOperationId(), CacheName, Key, new NodeCacheEntry<object?>("v", 1, null, Ttl), cancellationToken);
            ahead.Advance(Ttl);

            _ = await Assert.That(await a.Cache.GetEntryAsync(CacheName, Key, cancellationToken)).IsNull();
            _ = await Assert.That(await LastKindAsync(a.Registry, cancellationToken)).IsEqualTo(ReplicaMutationKinds.Expire);
        }

        // The behind leader's clock still has the entry live; it starts from the same log and catches up.
        var behind = new FakeTimeProvider(Start + Ttl);
        await using var bRegistry = await OpenRegistryAsync(dir, cancellationToken);
        await using var b = new Leader(bRegistry, behind);
        _ = await b.Committer.VerifyReplicasAsync(cancellationToken);

        _ = await Assert.That(await b.RawAsync(cancellationToken)).IsNull();
        _ = await Assert.That(await b.Cache.GetEntryAsync(CacheName, Key, cancellationToken)).IsNull();
    }

    /// <summary>A write through memory admission onto an expired key commits one record that folds the expiry, and is never reported failed.</summary>
    /// <param name="kind">The write kind.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes when the test has run.</returns>
    [Test]
    [Arguments(ReplicaMutationKinds.Set)]
    [Arguments(ReplicaMutationKinds.TryAdd)]
    [Arguments(ReplicaMutationKinds.Touch)]
    public async Task AdmissionFoldsExpiryIntoWrite(string kind, CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-expiry-clock-admission");
        using var meter = new Meter("squirix-expiry-clock-admission");
        var clock = new FakeTimeProvider(Start);
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var leader = new Leader(registry, clock);
        var accounting = new MemoryUsageAccounting();
        var gate = new PressureGate(new StateEvaluator(Options.Create(new PressureOptions { MaxEstimatedCacheBytes = 10_000_000_000 })), accounting, "n1", meter);
        var admission = new MemoryAdmissionCacheDecorator<object?>(
            leader.Cache,
            gate,
            new CacheEntrySizeEstimator<object?>(),
            accounting,
            (cacheName, operationId) => registry.HasRecordedOutcome("n1", cacheName, operationId),
            leader.Cache.PeekEntryAsync);
        await admission.SetEntryAsync(NewOperationId(), CacheName, Key, new NodeCacheEntry<object?>("v", 1, null, Ttl), cancellationToken);
        clock.Advance(Ttl);
        var before = (await StatusAsync(registry, cancellationToken)).LastLogIndex;

        var applied = kind switch
        {
            ReplicaMutationKinds.Set => await SetAsync(admission, cancellationToken),
            ReplicaMutationKinds.TryAdd => await admission.TryAddEntryAsync(NewOperationId(), CacheName, Key, new NodeCacheEntry<object?>("w", 1), cancellationToken),
            _ => await admission.TouchAsync(NewOperationId(), CacheName, Key, Ttl, cancellationToken),
        };

        _ = await Assert.That((await StatusAsync(registry, cancellationToken)).LastLogIndex).IsEqualTo(before + 1);
        _ = await Assert.That(await LastKindAsync(registry, cancellationToken)).IsEqualTo(kind);
        _ = await Assert.That(applied).IsEqualTo(!string.Equals(kind, ReplicaMutationKinds.Touch, StringComparison.Ordinal));
    }

    /// <summary>A leader whose clock is ahead takes over a key the earlier leader still read live and expires it through a tombstone.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AheadLeaderCommitsExpiryOnTakeover(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-expiry-clock-ahead");
        var behind = new FakeTimeProvider(Start);
        await using (var bRegistry = await OpenRegistryAsync(dir, cancellationToken))
        await using (var b = new Leader(bRegistry, behind))
        {
            await b.Committer.CommitSetAsync(NewOperationId(), CacheName, Key, new NodeCacheEntry<object?>("v", 1, null, Ttl), cancellationToken);
            behind.Advance(Ttl - Skew);

            _ = await Assert.That((await b.Cache.GetEntryAsync(CacheName, Key, cancellationToken))?.Value).IsEqualTo("v");
        }

        var ahead = new FakeTimeProvider(Start + Ttl);
        await using (var aRegistry = await OpenRegistryAsync(dir, cancellationToken))
        await using (var a = new Leader(aRegistry, ahead))
        {
            _ = await a.Committer.VerifyReplicasAsync(cancellationToken);
            _ = await Assert.That((await a.RawAsync(cancellationToken))?.Value).IsEqualTo("v");

            _ = await Assert.That(await a.Cache.GetEntryAsync(CacheName, Key, cancellationToken)).IsNull();
            _ = await Assert.That(await LastKindAsync(a.Registry, cancellationToken)).IsEqualTo(ReplicaMutationKinds.Expire);
        }

        // Back on the behind clock, the committed tombstone still decides.
        await using var againRegistry = await OpenRegistryAsync(dir, cancellationToken);
        await using var again = new Leader(againRegistry, new FakeTimeProvider(Start + Ttl - Skew));
        _ = await again.Committer.VerifyReplicasAsync(cancellationToken);

        _ = await Assert.That(await again.RawAsync(cancellationToken)).IsNull();
        _ = await Assert.That(await again.Cache.GetEntryAsync(CacheName, Key, cancellationToken)).IsNull();
    }

    /// <summary>The snapshot capture of the leader drops a key only once its tombstone is applied; an expired key without one is captured.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CommittedTombstoneSurvivesSnapshot(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-expiry-clock-snapshot");
        var clock = new FakeTimeProvider(Start);
        await using var leaderRegistry = await OpenRegistryAsync(dir, cancellationToken);
        await using var leader = new Leader(leaderRegistry, clock);
        await leader.Committer.CommitSetAsync(NewOperationId(), CacheName, Key, new NodeCacheEntry<object?>("v", 1, null, Ttl), cancellationToken);
        await leader.Committer.CommitSetAsync(NewOperationId(), CacheName, "other", new NodeCacheEntry<object?>("w", 1, null, Ttl), cancellationToken);
        clock.Advance(Ttl);
        _ = await leader.Cache.GetEntryAsync(CacheName, Key, cancellationToken);

        var captured = new List<(CacheKey Key, NodeCacheEntry<object?> Entry)>();
        await new LocalCacheSnapshotCapture<object?>(leader.Physical, CacheExpiryAuthority.CommittedRecords).CaptureEntriesAsync(captured, clock.GetUtcNow().UtcDateTime, cancellationToken);

        var (capturedKey, _) = await Assert.That(captured).HasSingleItem();
        _ = await Assert.That(capturedKey.Key).IsEqualTo("other");
    }

    /// <summary>A touch and an expired read of one key run in one order: the touch of an expired key deletes it and reports false, and the read misses.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConcurrentTouchFollowsExpiration(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-expiry-clock-touch");
        var clock = new FakeTimeProvider(Start);
        await using var leaderRegistry = await OpenRegistryAsync(dir, cancellationToken);
        await using var leader = new Leader(leaderRegistry, clock);
        await leader.Committer.CommitSetAsync(NewOperationId(), CacheName, Key, new NodeCacheEntry<object?>("v", 1, null, Ttl), cancellationToken);
        clock.Advance(Ttl);

        var touch = leader.Committer.CommitTouchAsync(NewOperationId(), CacheName, Key, Ttl, cancellationToken);
        var read = leader.Cache.GetEntryAsync(CacheName, Key, cancellationToken).AsTask();
        await Task.WhenAll(touch, read);

        _ = await Assert.That(await touch).IsFalse();
        _ = await Assert.That(await read).IsNull();
        _ = await Assert.That(await leader.RawAsync(cancellationToken)).IsNull();
    }

    /// <summary>A touch of a live key wins over the expiry: a later read past the old deadline finds the entry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TouchBeforeDeadlineWins(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-expiry-clock-touch-live");
        var clock = new FakeTimeProvider(Start);
        await using var leaderRegistry = await OpenRegistryAsync(dir, cancellationToken);
        await using var leader = new Leader(leaderRegistry, clock);
        await leader.Committer.CommitSetAsync(NewOperationId(), CacheName, Key, new NodeCacheEntry<object?>("v", 1, null, Ttl), cancellationToken);
        clock.Advance(Ttl - Skew);

        var touched = await leader.Committer.CommitTouchAsync(NewOperationId(), CacheName, Key, Ttl, cancellationToken);
        clock.Advance(Skew);

        _ = await Assert.That(touched).IsTrue();
        _ = await Assert.That((await leader.Cache.GetEntryAsync(CacheName, Key, cancellationToken))?.Value).IsEqualTo("v");
    }

    /// <summary>Dispose with an expiry queued behind a commit that never ends completes within one shutdown budget and reports the leak once.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeBoundsExpiryOnGate(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-expiry-clock-dispose");
        var clock = new FakeTimeProvider(Start);
        var shutdownClock = new FakeTimeProvider(Start);
        var budget = TimeSpan.FromMilliseconds(50);
        var log = new EventRecordingLogger();
        var gateway = new ReplicaCommitterDoubles.ParkingGateway { HeldNode = "n2" };
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        var physical = new PhysicalCache<object?>(clock, expiry: CacheExpiryAuthority.CommittedRecords);
        var committer = CreateCommitter(registry, gateway, new ClientCache<object?>(physical, physical), clock, log, (shutdownClock, budget));
        try
        {
            await committer.CommitSetAsync(NewOperationId(), CacheName, Key, new NodeCacheEntry<object?>("v", 1, null, Ttl), cancellationToken);
            clock.Advance(Ttl);

            // Both followers park the next write, which holds the commit gate; the expiry queues behind it.
            gateway.Arm();
            var stuck = committer.CommitSetAsync(NewOperationId(), CacheName, "other", new NodeCacheEntry<object?>("w", 1), cancellationToken);
            await gateway.Entered.WaitAsync(TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
            var expiry = committer.ExpireAsync(CacheName, Key, cancellationToken);

            var disposal = committer.DisposeAsync().AsTask();
            shutdownClock.Advance(budget);
            await disposal.WaitAsync(TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

            // The gate drain reports its leak once; the expiry it faults may end before the expiration coordinator needs to report one.
            _ = await Assert.That(log.Count(4005)).IsEqualTo(1);
            _ = await Assert.That(log.Count(4032)).IsLessThanOrEqualTo(1);
            _ = await Assert.That(stuck.IsCompleted).IsFalse();
            _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception, NodeCacheEntry<object?>?>(new ValueTask<NodeCacheEntry<object?>?>(expiry.WaitAsync(TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken)));
        }
        finally
        {
            gateway.Release();
            gateway.ReleaseHeld();
            await committer.DisposeAsync();
        }
    }

    /// <summary>A replica applying the committed records keeps the entry past its deadline on its own clock until the tombstone is applied.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FollowerKeepsEntryUntilTombstoneCommit(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-expiry-clock-follower");
        var clock = new FakeTimeProvider(Start);
        await using var leaderRegistry = await OpenRegistryAsync(dir, cancellationToken);
        await using var leader = new Leader(leaderRegistry, clock);
        await leader.Committer.CommitSetAsync(NewOperationId(), CacheName, Key, new NodeCacheEntry<object?>("v", 1, null, Ttl), cancellationToken);
        clock.Advance(Ttl);
        _ = await leader.Cache.GetEntryAsync(CacheName, Key, cancellationToken);
        var records = await RecordsAsync(leader.Registry, cancellationToken);
        var followerPhysical = new PhysicalCache<object?>(new FakeTimeProvider(Start + (Ttl * 10)), expiry: CacheExpiryAuthority.CommittedRecords);
        var follower = new ClientCache<object?>(followerPhysical, followerPhysical);

        await ReplicaCacheApplier.ApplyAsync(follower, records[0], cancellationToken);
        var beforeTombstone = await follower.GetEntryAsync(CacheName, Key, cancellationToken);
        await ReplicaCacheApplier.ApplyAsync(follower, records[1], cancellationToken);

        _ = await Assert.That(records.Count).IsEqualTo(2);
        _ = await Assert.That(beforeTombstone?.Value).IsEqualTo("v");
        _ = await Assert.That(await follower.GetEntryAsync(CacheName, Key, cancellationToken)).IsNull();
    }

    /// <summary>An expired read whose tombstone cannot reach a majority is refused retryably, and the entry stays in memory.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NoMajorityExpiredReadIsRefusedRetryably(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-expiry-clock-nomajority");
        var clock = new FakeTimeProvider(Start);
        await using var leaderRegistry = await OpenRegistryAsync(dir, cancellationToken);
        await using var leader = new Leader(leaderRegistry, clock);
        await leader.Committer.CommitSetAsync(NewOperationId(), CacheName, Key, new NodeCacheEntry<object?>("v", 1, null, Ttl), cancellationToken);
        leader.Gateway.Set("n2", FollowerMode.Down);
        leader.Gateway.Set("n3", FollowerMode.Down);
        clock.Advance(Ttl);

        var refused = await NodeAsyncAssert.ThrowsAsync<RpcException, NodeCacheEntry<object?>?>(leader.Cache.GetEntryAsync(CacheName, Key, cancellationToken));

        _ = await Assert.That(refused.StatusCode).IsEqualTo(StatusCode.Unavailable);
        _ = await Assert.That(refused.Status.Detail).IsEqualTo(ServerOpContract.ExpirationPendingDetail);
        _ = await Assert.That((await leader.RawAsync(cancellationToken))?.Value).IsEqualTo("v");
    }

    /// <summary>A restart replays the committed tombstone: the expired value does not come back.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestartDoesNotRestoreExpiredValue(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-expiry-clock-restart");
        var clock = new FakeTimeProvider(Start);
        await using (var leaderRegistry = await OpenRegistryAsync(dir, cancellationToken))
        await using (var leader = new Leader(leaderRegistry, clock))
        {
            await leader.Committer.CommitSetAsync(NewOperationId(), CacheName, Key, new NodeCacheEntry<object?>("v", 1, null, Ttl), cancellationToken);
            clock.Advance(Ttl);
            _ = await Assert.That(await leader.Cache.GetEntryAsync(CacheName, Key, cancellationToken)).IsNull();
        }

        await using var restartedRegistry = await OpenRegistryAsync(dir, cancellationToken);

        await using var restarted = new Leader(restartedRegistry, new FakeTimeProvider(Start));
        _ = await restarted.Committer.VerifyReplicasAsync(cancellationToken);

        _ = await Assert.That(await restarted.RawAsync(cancellationToken)).IsNull();
    }

    /// <summary>A restart rebuilds a client outcome that more expiration tombstones than the idempotency store holds followed: a retry replays it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestartReplaysOutcomeBehindTombstones(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-expiry-clock-rebuild");
        var options = new FollowerLogOptions { IdempotencyCapacity = 4 };
        var clock = new FakeTimeProvider(Start);
        var operationId = NewOperationId();
        var entry = new NodeCacheEntry<object?>("v", 1);
        ulong written;
        await using (var registry = await OpenRegistryAsync(dir, options, cancellationToken))
        await using (var leader = new Leader(registry, clock))
        {
            await leader.Committer.CommitSetAsync(operationId, CacheName, Key, entry, cancellationToken);
            for (var i = 0; i < 5; i++)
            {
                var expired = "expired-" + i.ToString(System.Globalization.CultureInfo.InvariantCulture);
                await leader.Physical.SetAsync(new CacheKey(CacheName, expired), new NodeCacheEntry<object?>("x", 1, Start.UtcDateTime), cancellationToken);
                _ = await leader.Committer.ExpireAsync(CacheName, expired, cancellationToken);
            }

            written = (await StatusAsync(registry, cancellationToken)).LastLogIndex;
            var tombstones = 0;
            var records = await RecordsAsync(registry, cancellationToken);
            for (var i = 0; i < records.Count; i++)
                tombstones += string.Equals(records[i].MutationKind, ReplicaMutationKinds.Expire, StringComparison.Ordinal) ? 1 : 0;

            _ = await Assert.That(tombstones).IsEqualTo(5);
        }

        await using var restartedRegistry = await OpenRegistryAsync(dir, options, cancellationToken);
        await using var restarted = new Leader(restartedRegistry, clock);
        await restarted.Committer.CommitSetAsync(operationId, CacheName, Key, entry, cancellationToken);

        _ = await Assert.That((await StatusAsync(restartedRegistry, cancellationToken)).LastLogIndex).IsEqualTo(written);
    }

    private static async Task<bool> SetAsync(MemoryAdmissionCacheDecorator<object?> admission, CancellationToken cancellationToken)
    {
        await admission.SetEntryAsync(NewOperationId(), CacheName, Key, new NodeCacheEntry<object?>("w", 1), cancellationToken);
        return true;
    }

    private static async Task<List<ReplicaLogRecord>> RecordsAsync(ReplicaGroupRegistry registry, CancellationToken cancellationToken)
    {
        if (!registry.TryGetLog("n1", out var log))
            throw new InvalidOperationException("The owned group log is not open.");

        var read = await log.ReadEntriesAsync(1, 64, cancellationToken);
        var records = new List<ReplicaLogRecord>(read.Entries.Count);
        foreach (var entry in read.Entries)
            records.Add(ReplicaLogCodec.Decode(entry.Payload) ?? ThrowHelper.Throw<ReplicaLogRecord>(new InvalidOperationException("The log entry must decode.")));

        return records;
    }

    private static async Task<string> LastKindAsync(ReplicaGroupRegistry registry, CancellationToken cancellationToken) => (await RecordsAsync(registry, cancellationToken))[^1].MutationKind;

    /// <summary>The owner of group n1 over an open registry: a committed-records cache and a committer on one fake clock.</summary>
    private sealed class Leader : IAsyncDisposable
    {
        internal Leader(ReplicaGroupRegistry registry, FakeTimeProvider clock)
        {
            Registry = registry;
            Physical = new PhysicalCache<object?>(clock, expiry: CacheExpiryAuthority.CommittedRecords);
            var local = new ClientCache<object?>(Physical, Physical);
            Committer = CreateCommitter(registry, Gateway, local, clock);
            Cache = new ReplicatedCache(local, Committer);
        }

        internal ReplicatedCache Cache { get; }

        internal ReplicaGroupCommitter Committer { get; }

        internal ScriptedGateway Gateway { get; } = new();

        internal PhysicalCache<object?> Physical { get; }

        internal ReplicaGroupRegistry Registry { get; }

        public ValueTask DisposeAsync() => Committer.DisposeAsync();

        internal ValueTask<NodeCacheEntry<object?>?> RawAsync(CancellationToken cancellationToken) => Physical.RawReader.GetEntryRawAsync(new CacheKey(CacheName, Key), cancellationToken);
    }
}
