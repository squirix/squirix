using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaExpiryKit;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>How a failure of the run of tombstones of a batch expiry is mapped for its keys, while the other keys keep their decided result.</summary>
public sealed class ReplicaBatchExpiryFailureTests : ServerUnitTestBase
{
    private const long LogIndexOfPin = 900;

    /// <summary>The default commit budget of a committer, which the budget timers of the committer are armed with.</summary>
    private static readonly TimeSpan CommitBudget = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan StallTimeout = TimeSpan.FromSeconds(10);

    /// <summary>
    /// An append whose frames reached the disk before its metadata write failed holds the first tombstone, so the outcome is unknown for
    /// every tombstoned key; the live and absent keys keep their result.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes when the test has run.</returns>
    [Test]
    public async Task TornAppendIsUnknownForTombstonedKeys(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-batch-expiry-torn");
        using var hooks = new StallableFollowerLogFaultHooks();
        var clock = new FakeTimeProvider(Start);
        var options = new FollowerLogOptions { ShutdownTimeProvider = TimeProvider.System, TimeProvider = TimeProvider.System, FaultHooks = hooks };
        await using var registry = await OpenRegistryAsync(dir, options, cancellationToken);
        await using var leader = new Leader(registry, new ScriptedGateway(), clock);
        await SeedExpiredAsync(leader, clock, ["a", "b"], cancellationToken);
        await leader.Committer.CommitSetAsync(NewOperationId(), CacheName, "live", new NodeCacheEntry<object?>("v", 1, null, Ttl), cancellationToken);
        var before = (await StatusAsync(registry, cancellationToken)).LastLogIndex;

        hooks.StallNextMetaWrite();
        var batch = leader.Committer.CommitExpiriesAsync([(CacheName, "a"), (CacheName, "live"), (CacheName, "gone"), (CacheName, "b")]);
        await hooks.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        hooks.ReleaseWithFailure(new IOException("Injected metadata write failure."));
        var results = await batch.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        _ = await Assert.That((results[0].Failure as SquirixException)?.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);
        _ = await Assert.That((results[3].Failure as SquirixException)?.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);
        _ = await Assert.That(results[0].Status).IsEqualTo(ReplicaExpiryStatus.Failed);
        _ = await Assert.That(results[1].Status).IsEqualTo(ReplicaExpiryStatus.Live);
        _ = await Assert.That(results[2].Status).IsEqualTo(ReplicaExpiryStatus.Absent);
        _ = await Assert.That((await StatusAsync(registry, cancellationToken)).LastLogIndex).IsEqualTo(before + 2);
    }

    /// <summary>
    /// A commit budget that expires while the run waits for the log, before its append, is a definite retryable refusal of its keys; no log
    /// index is consumed and the other keys keep their result.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes when the test has run.</returns>
    [Test]
    public async Task BudgetBeforeAppendRefusesTombstonedKeys(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-batch-expiry-budget");
        using var hooks = new StallableFollowerLogFaultHooks();
        var budgetClock = new DueTimerClock(CommitBudget);
        budgetClock.SetUtcNow(Start);
        var options = new FollowerLogOptions { ShutdownTimeProvider = TimeProvider.System, TimeProvider = TimeProvider.System, FaultHooks = hooks };
        await using var registry = await OpenRegistryAsync(dir, options, cancellationToken);
        await using var leader = new Leader(registry, new ScriptedGateway(), budgetClock, true);
        await SeedExpiredAsync(leader, budgetClock, ["a", "b"], cancellationToken);
        await leader.Committer.CommitSetAsync(NewOperationId(), CacheName, "live", new NodeCacheEntry<object?>("v", 1, null, Ttl), cancellationToken);
        var log = Log(registry);
        var before = (await log.GetStatusAsync(cancellationToken)).LastLogIndex;

        // A metadata write parked under the log gate keeps the append of the run waiting for it well past the commit budget.
        hooks.StallNextMetaWrite();
        var holder = log.AdvanceAppliedAsync(before, cancellationToken);
        await hooks.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        budgetClock.ForgetCreated();
        var batch = leader.Committer.CommitExpiriesAsync([(CacheName, "a"), (CacheName, "live"), (CacheName, "b")]);

        // One budget timer covers the decisions under the gate, the next one the commit of the run.
        _ = await Assert.That(await budgetClock.TimerCreated.WaitAsync(StallTimeout, cancellationToken)).IsTrue();
        _ = await Assert.That(await budgetClock.TimerCreated.WaitAsync(StallTimeout, cancellationToken)).IsTrue();
        budgetClock.Advance(CommitBudget * 5);
        hooks.Release();
        _ = await holder.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        var results = await batch.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        _ = await Assert.That((results[0].Failure as SquirixException)?.Code).IsEqualTo(SquirixErrorCode.TooManyRequests);
        _ = await Assert.That((results[2].Failure as SquirixException)?.Code).IsEqualTo(SquirixErrorCode.TooManyRequests);
        _ = await Assert.That(results[1].Status).IsEqualTo(ReplicaExpiryStatus.Live);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(before);
    }

    /// <summary>A tombstone whose identity the group retains unresolved is left out of the run and fails alone; the rest of the run commits.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes when the test has run.</returns>
    [Test]
    public async Task UnresolvedIdentityFailsAloneInRun(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-batch-expiry-pinned-run");
        var clock = new FakeTimeProvider(Start);
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var leader = new Leader(registry, new ScriptedGateway(), clock);
        await SeedExpiredAsync(leader, clock, ["a", "b", "c"], cancellationToken);
        await PinExpiryAsync(leader, registry, "a", cancellationToken);
        var before = (await StatusAsync(registry, cancellationToken)).LastLogIndex;

        var results = await leader.Committer.CommitExpiriesAsync([(CacheName, "a"), (CacheName, "b"), (CacheName, "c")]);

        _ = await Assert.That(results[0].Status).IsEqualTo(ReplicaExpiryStatus.Failed);
        _ = await Assert.That((results[0].Failure as SquirixException)?.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);
        _ = await Assert.That(results[1].Status).IsEqualTo(ReplicaExpiryStatus.Tombstoned);
        _ = await Assert.That(results[2].Status).IsEqualTo(ReplicaExpiryStatus.Tombstoned);
        _ = await Assert.That((await StatusAsync(registry, cancellationToken)).LastLogIndex).IsEqualTo(before + 2);
        _ = await Assert.That(await leader.RawAsync("a", cancellationToken)).IsNotNull();
        _ = await Assert.That(await leader.RawAsync("b", cancellationToken)).IsNull();
        _ = await Assert.That(await leader.RawAsync("c", cancellationToken)).IsNull();
    }

    /// <summary>A pinned tombstone before the only other one is mapped like it is in a longer run: it fails alone, and the other key commits alone.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes when the test has run.</returns>
    [Test]
    public async Task UnresolvedIdentityBesideOneTombstone(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-batch-expiry-pinned-single");
        var clock = new FakeTimeProvider(Start);
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var leader = new Leader(registry, new ScriptedGateway(), clock);
        await SeedExpiredAsync(leader, clock, ["a", "b"], cancellationToken);
        await PinExpiryAsync(leader, registry, "a", cancellationToken);
        var before = (await StatusAsync(registry, cancellationToken)).LastLogIndex;

        var results = await leader.Committer.CommitExpiriesAsync([(CacheName, "a"), (CacheName, "b")]);

        _ = await Assert.That(results[0].Status).IsEqualTo(ReplicaExpiryStatus.Failed);
        _ = await Assert.That((results[0].Failure as SquirixException)?.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);
        _ = await Assert.That(results[1].Status).IsEqualTo(ReplicaExpiryStatus.Tombstoned);
        _ = await Assert.That((await StatusAsync(registry, cancellationToken)).LastLogIndex).IsEqualTo(before + 1);
        _ = await Assert.That(await leader.RawAsync("b", cancellationToken)).IsNull();
    }

    /// <summary>A key whose decision throws fails alone: the other keys of the batch still commit their tombstones.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes when the test has run.</returns>
    [Test]
    public async Task FaultingDecisionFailsOnlyItsKey(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-batch-expiry-faulting");
        var clock = new FakeTimeProvider(Start);
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        var physical = new PhysicalCache<object?>(clock, new CacheEntrySizeEstimator<object?>(), expiry: CacheExpiryAuthority.CommittedRecords);
        var failure = new InvalidOperationException("The read of the key failed.");
        await using var committer = CreateCommitter(registry, new ScriptedGateway(), new FaultingReadCache(new ClientCache<object?>(physical, physical), "bad", failure), clock);
        foreach (var key in new[] { "a", "b" })
            await committer.CommitSetAsync(NewOperationId(), CacheName, key, new NodeCacheEntry<object?>(key, 1, null, Ttl), cancellationToken);

        clock.Advance(Ttl);
        var before = (await StatusAsync(registry, cancellationToken)).LastLogIndex;

        var results = await committer.CommitExpiriesAsync([(CacheName, "a"), (CacheName, "bad"), (CacheName, "b")]);

        _ = await Assert.That((results[0].Status, results[2].Status)).IsEqualTo((ReplicaExpiryStatus.Tombstoned, ReplicaExpiryStatus.Tombstoned));
        _ = await Assert.That(results[1].Status).IsEqualTo(ReplicaExpiryStatus.Failed);
        _ = await Assert.That(results[1].Failure).IsSameReferenceAs(failure);
        _ = await Assert.That((await StatusAsync(registry, cancellationToken)).LastLogIndex).IsEqualTo(before + 2);
        _ = await Assert.That(await physical.RawReader.GetEntryRawAsync(new CacheKey(CacheName, "a"), cancellationToken)).IsNull();
        _ = await Assert.That(await physical.RawReader.GetEntryRawAsync(new CacheKey(CacheName, "b"), cancellationToken)).IsNull();
    }

    private static IFollowerLog Log(ReplicaGroupRegistry registry) =>
        registry.TryGetLog("n1", out var log) ? log : throw new InvalidOperationException("The owned group log is not open.");

    /// <summary>Reserves the identity of the tombstone of an expired key in the group log without resolving it, as an entry appended but not yet committed.</summary>
    /// <param name="leader">The leader.</param>
    /// <param name="registry">The open registry of the owner.</param>
    /// <param name="key">The expired key.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">The key holds no entry.</exception>
    private static async Task PinExpiryAsync(Leader leader, ReplicaGroupRegistry registry, string key, CancellationToken cancellationToken)
    {
        var entry = await leader.RawAsync(key, cancellationToken) ?? ThrowHelper.Throw<NodeCacheEntry<object?>>(new InvalidOperationException("The key must hold an entry."));
        var ticks = Math.Max(1L, entry.ExpiresUtc.GetValueOrDefault().Ticks);
        var operationId = ReplicaExpirationOperationId.Create(leader.Committer.GroupId, CacheName, key, entry.Version, new DateTime(ticks, DateTimeKind.Utc));
        var reserved = Log(registry).Idempotency.Reserve(
            ReplicaExpirationOperationId.OperationScope,
            operationId,
            ReplicaOperationFingerprints.Expire(CacheName, key, entry.Version, ticks),
            GroupRecordKind.Expiration,
            LogIndexOfPin,
            1,
            new GroupReserveOptions(false, 0));
        _ = await Assert.That(reserved).IsEqualTo(GroupIdempotencyReserveResult.Success);
    }

    /// <summary>Local cache that delegates to another and throws a fixed failure when one key is read.</summary>
    private sealed class FaultingReadCache : ILogicalNamespacedCache<object?>
    {
        private readonly Exception _failure;
        private readonly ILogicalNamespacedCache<object?> _inner;
        private readonly string _key;

        internal FaultingReadCache(ILogicalNamespacedCache<object?> inner, string key, Exception failure)
        {
            _inner = inner;
            _key = key;
            _failure = failure;
        }

        public ValueTask<NodeCacheEntry<object?>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) =>
            string.Equals(key, _key, StringComparison.Ordinal) ? throw _failure : _inner.GetEntryAsync(cacheName, key, cancellationToken);

        public ValueTask<NodeCacheValueResult<object?>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
            _inner.GetValueAsync(cacheName, key, cancellationToken);

        public ValueTask<CacheRemoveResult<object?>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
            _inner.RemoveAsync(operationId, cacheName, key, cancellationToken);

        public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
            _inner.RemoveExpirationAsync(operationId, cacheName, key, cancellationToken);

        public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken) =>
            _inner.SetEntryAsync(operationId, cacheName, key, entry, cancellationToken);

        public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) =>
            _inner.TouchAsync(operationId, cacheName, key, expiration, cancellationToken);

        public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken) =>
            _inner.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken);

        public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, object? value, CancellationToken cancellationToken) =>
            _inner.UpdateAsync(operationId, cacheName, key, value, cancellationToken);
    }
}
