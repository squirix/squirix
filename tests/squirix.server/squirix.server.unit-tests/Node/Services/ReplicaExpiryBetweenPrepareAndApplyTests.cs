using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Rocks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// A key that expires between the prepare of a replicated mutation and its apply changes nothing about what the client was told:
/// the returned outcome, the retried outcome and memory all agree, and a restart applies the committed entries to the same result.
/// </summary>
public sealed class ReplicaExpiryBetweenPrepareAndApplyTests : ServerUnitTestBase
{
    private const string CacheName = "cache";
    private const string Key = "k";

    private static readonly TimeSpan Ttl = TimeSpan.FromMinutes(1);
    private static readonly TimeSpan Wait = TimeSpan.FromMinutes(5);

    /// <summary>A restart after the deadline re-applies a committed rejected try-add without inserting the rejected entry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestartAfterDeadlineKeepsCommittedFalse(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-owner-expiry-restart");
        var clock = new FakeTimeProvider();
        await using (var firstRegistry = await OpenRegistryAsync(dir, cancellationToken))
        await using (var first = new Owner(firstRegistry, clock))
        {
            await first.Committer.CommitSetAsync(NewOperationId(), CacheName, Key, new NodeCacheEntry<object?>("a", 1, clock.GetUtcNow().UtcDateTime.Add(Ttl)), cancellationToken);
            var added = await first.Committer.CommitTryAddAsync(NewOperationId(), CacheName, Key, new NodeCacheEntry<object?>("b"), cancellationToken);
            _ = await Assert.That(added).IsFalse();
        }

        clock.Advance(Wait);
        await using var restartedRegistry = await OpenRegistryAsync(dir, cancellationToken);
        await using var restarted = new Owner(restartedRegistry, clock);
        _ = await restarted.Committer.VerifyReplicasAsync(cancellationToken);

        _ = await Assert.That((await restarted.RawAsync(cancellationToken))?.Value).IsEqualTo("a");
        await restarted.Committer.FlushAppliedAsync(Durable(), cancellationToken);
        var status = await StatusAsync(restartedRegistry, cancellationToken);
        _ = await Assert.That((status.CommitIndex, status.LastAppliedIndex)).IsEqualTo((2UL, 2UL));
    }

    /// <summary>A remove-expiration that expires before the apply reports true, clears the deadline in memory, and reports true again on a retry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveExpirationBeforeApplyAgrees(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-owner-expiry-removeexpiration");
        var clock = new FakeTimeProvider();
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var owner = new Owner(registry, clock);
        await owner.SeedAsync(new NodeCacheEntry<object?>("a", 1, clock.GetUtcNow().UtcDateTime.Add(Ttl)), cancellationToken);
        owner.ExpireOnNextAppend(Wait);
        var id = NewOperationId();

        var cleared = await owner.Committer.CommitRemoveExpirationAsync(id, CacheName, Key, cancellationToken);
        var raw = await owner.RawAsync(cancellationToken);
        var retried = await owner.Committer.CommitRemoveExpirationAsync(id, CacheName, Key, cancellationToken);

        _ = await Assert.That((cleared, retried)).IsEqualTo((true, true));
        _ = await Assert.That(raw?.Value).IsEqualTo("a");
        _ = await Assert.That(raw?.ExpiresUtc).IsNull();
    }

    /// <summary>A touch that finds its key expired before the apply reports true, writes the decided deadline, and reports true again on a retry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TouchExpiringBeforeApplyAgrees(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-owner-expiry-touch");
        var clock = new FakeTimeProvider();
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var owner = new Owner(registry, clock);
        await owner.SeedAsync(new NodeCacheEntry<object?>("a", 1, clock.GetUtcNow().UtcDateTime.Add(Ttl)), cancellationToken);
        var decided = clock.GetUtcNow().UtcDateTime.Add(Ttl * 10);
        owner.ExpireOnNextAppend(Wait);
        var id = NewOperationId();

        var touched = await owner.Committer.CommitTouchAsync(id, CacheName, Key, Ttl * 10, cancellationToken);
        var raw = await owner.RawAsync(cancellationToken);
        var retried = await owner.Committer.CommitTouchAsync(id, CacheName, Key, Ttl * 10, cancellationToken);

        _ = await Assert.That((touched, retried)).IsEqualTo((true, true));
        _ = await Assert.That(raw?.Value).IsEqualTo("a");
        _ = await Assert.That(raw?.ExpiresUtc).IsEqualTo(decided);
    }

    /// <summary>A try-add rejected while the key was live reports false, keeps the earlier entry in memory, and reports false again on a retry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AddExpiringBeforeApplyAgrees(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-owner-expiry-tryadd");
        var clock = new FakeTimeProvider();
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var owner = new Owner(registry, clock);
        await owner.SeedAsync(new NodeCacheEntry<object?>("a", 1, clock.GetUtcNow().UtcDateTime.Add(Ttl)), cancellationToken);
        owner.ExpireOnNextAppend(Wait);
        var id = NewOperationId();

        var added = await owner.Committer.CommitTryAddAsync(id, CacheName, Key, new NodeCacheEntry<object?>("b"), cancellationToken);
        var raw = await owner.RawAsync(cancellationToken);
        var retried = await owner.Committer.CommitTryAddAsync(id, CacheName, Key, new NodeCacheEntry<object?>("b"), cancellationToken);

        _ = await Assert.That((added, retried)).IsEqualTo((false, false));
        _ = await Assert.That(raw?.Value).IsEqualTo("a");
    }

    /// <summary>An update that finds its key expired before the apply reports true, writes the new value under the decided deadline, and reports true again on a retry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UpdateExpiringBeforeApplyAgrees(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-owner-expiry-update");
        var clock = new FakeTimeProvider();
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var owner = new Owner(registry, clock);
        var deadline = clock.GetUtcNow().UtcDateTime.Add(Ttl);
        await owner.SeedAsync(new NodeCacheEntry<object?>("a", 1, deadline), cancellationToken);
        owner.ExpireOnNextAppend(Wait);
        var id = NewOperationId();

        var updated = await owner.Committer.CommitUpdateAsync(id, CacheName, Key, "b", cancellationToken);
        var raw = await owner.RawAsync(cancellationToken);
        var retried = await owner.Committer.CommitUpdateAsync(id, CacheName, Key, "b", cancellationToken);

        _ = await Assert.That((updated, retried)).IsEqualTo((true, true));
        _ = await Assert.That(raw?.Value).IsEqualTo("b");
        _ = await Assert.That(raw?.ExpiresUtc).IsEqualTo(deadline);
    }

    private static IJournalDurabilityCoordinator Durable()
    {
        var durability = new IJournalDurabilityCoordinatorCreateExpectations();
        _ = durability.Setups.AwaitDurabilityCommitAsync(Arg.Any<CancellationToken>()).ReturnValue(ValueTask.CompletedTask);
        return durability.Instance();
    }

    /// <summary>An RF=3 owner whose memory and committer share one fake clock.</summary>
    private sealed class Owner : IAsyncDisposable
    {
        private readonly FakeTimeProvider _clock;
        private readonly ScriptedGateway _gateway = new();
        private readonly PhysicalCache<object?> _physical;

        internal Owner(ReplicaGroupRegistry registry, FakeTimeProvider clock)
        {
            _clock = clock;
            _physical = new PhysicalCache<object?>(clock);
            Committer = CreateCommitter(registry, _gateway, new ClientCache<object?>(_physical, _physical), clock);
        }

        internal ReplicaGroupCommitter Committer { get; }

        public ValueTask DisposeAsync() => Committer.DisposeAsync();

        /// <summary>Advances the clock once, while the next mutation is appended on the followers: after its local append, before its majority.</summary>
        /// <param name="span">How far the clock moves.</param>
        internal void ExpireOnNextAppend(TimeSpan span)
        {
            var advanced = 0;
            _gateway.OnAppend = () =>
            {
                if (Interlocked.Exchange(ref advanced, 1) == 0)
                    _clock.Advance(span);
            };
        }

        internal ValueTask<NodeCacheEntry<object?>?> RawAsync(CancellationToken cancellationToken) => _physical.RawReader.GetEntryRawAsync(new CacheKey(CacheName, Key), cancellationToken);

        internal Task SeedAsync(NodeCacheEntry<object?> entry, CancellationToken cancellationToken) => Committer.CommitSetAsync(NewOperationId(), CacheName, Key, entry, cancellationToken);
    }
}
