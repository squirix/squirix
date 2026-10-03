using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>After a restart the idempotency store keeps the outcomes with the newest log indexes, whether a snapshot or the log carries them.</summary>
[Immutable]
public sealed class ReplicaOutcomeRecoveryTests : ServerUnitTestBase
{
    private const string GroupId = "grp-outcome-recovery";
    private const string Scope = "client";

    private static readonly byte[] Fingerprint = [1, 2, 3];
    private static readonly byte[] Key = [107];

    /// <summary>
    /// Snapshot outcomes restore with their capture age and fill the store; the newest log outcomes take their places, so every retained
    /// log outcome replays and the stale snapshot outcomes are gone.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NewestLogOutcomesReplaceStale(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-outcome-recovery-newest");
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        await using (var log = await OpenAsync(dir, clock, cancellationToken))
        {
            await CommitAsync(log, clock, ["a", "b", "c", "d"], 1UL, cancellationToken);
            _ = await log.CompactThroughAsync(4UL, cancellationToken);
            clock.Advance(TimeSpan.FromMinutes(16));
            log.Idempotency.Expire();
            await CommitAsync(log, clock, ["e", "f", "g", "h"], 5UL, cancellationToken);
        }

        clock.Advance(TimeSpan.FromSeconds(1));
        await using var reopened = await OpenAsync(dir, clock, cancellationToken);
        var restored = await ReplicaOutcomeRecovery.RestoreAsync(reopened, clock, cancellationToken);

        _ = await Assert.That(restored).IsEqualTo(4);
        foreach (var id in new[] { "e", "f", "g", "h" })
            _ = await Assert.That(reopened.Idempotency.Lookup(Scope, id, Fingerprint, out _)).IsEqualTo(GroupIdempotencyLookup.Found);

        foreach (var id in new[] { "a", "b", "c", "d" })
            _ = await Assert.That(reopened.Idempotency.Lookup(Scope, id, Fingerprint, out _)).IsEqualTo(GroupIdempotencyLookup.Miss);
    }

    /// <summary>An identity that ran again after its first outcome expired replays the newest outcome, not the stale one a snapshot carries.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReusedIdentityReplaysNewestOutcome(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-outcome-recovery-reuse");
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        await using (var log = await OpenAsync(dir, clock, cancellationToken))
        {
            await CommitAsync(log, clock, ["x"], 1UL, cancellationToken);
            _ = await log.CompactThroughAsync(1UL, cancellationToken);
            clock.Advance(TimeSpan.FromMinutes(16));
            log.Idempotency.Expire();
            await CommitAsync(log, clock, ["x"], 2UL, cancellationToken);
        }

        clock.Advance(TimeSpan.FromSeconds(1));
        await using var reopened = await OpenAsync(dir, clock, cancellationToken);
        var restored = await ReplicaOutcomeRecovery.RestoreAsync(reopened, clock, cancellationToken);

        _ = await Assert.That(restored).IsEqualTo(1);
        _ = await Assert.That(reopened.Idempotency.Lookup(Scope, "x", Fingerprint, out var record)).IsEqualTo(GroupIdempotencyLookup.Found);
        _ = await Assert.That(record.LogIndex).IsEqualTo(2UL);
        await SequenceAssert.EqualAsync<byte>([2], record.OutcomePayload.ToArray());
    }

    /// <summary>
    /// A pinned tail entry keeps its place when the store is full: the newest log outcomes displace the oldest snapshot outcomes, and the
    /// store is trimmed to its capacity once the rebuild is done.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PinnedTailKeepsPlaceOverSnapshot(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-outcome-recovery-pin");
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        await using (var log = await OpenAsync(dir, clock, cancellationToken))
        {
            await CommitAsync(log, clock, ["a", "b", "c", "d"], 1UL, cancellationToken);
            _ = await log.CompactThroughAsync(4UL, cancellationToken);
            await CommitAsync(log, clock, ["e", "f"], 5UL, cancellationToken);
        }

        await using var reopened = await OpenAsync(dir, clock, cancellationToken);
        _ = reopened.Idempotency.Reserve(Scope, "tail", Fingerprint, GroupRecordKind.UserMutation, 7UL, 1UL, true);
        var restored = await ReplicaOutcomeRecovery.RestoreAsync(reopened, clock, cancellationToken);

        _ = await Assert.That(restored).IsEqualTo(2);
        _ = await Assert.That(reopened.Idempotency.Lookup(Scope, "tail", Fingerprint, out _)).IsEqualTo(GroupIdempotencyLookup.Unresolved);
        foreach (var id in new[] { "d", "e", "f" })
            _ = await Assert.That(reopened.Idempotency.Lookup(Scope, id, Fingerprint, out _)).IsEqualTo(GroupIdempotencyLookup.Found);

        foreach (var id in new[] { "a", "b", "c" })
            _ = await Assert.That(reopened.Idempotency.Lookup(Scope, id, Fingerprint, out _)).IsEqualTo(GroupIdempotencyLookup.Miss);
    }

    /// <summary>Appends and commits one entry per identity, resolving each outcome with its log index as the payload.</summary>
    /// <param name="log">The log.</param>
    /// <param name="clock">The clock the decision times are stamped from.</param>
    /// <param name="ids">The operation identifiers, in log order.</param>
    /// <param name="firstIndex">The log index of the first entry.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    private static async Task CommitAsync(FollowerLog log, FakeTimeProvider clock, string[] ids, ulong firstIndex, CancellationToken cancellationToken)
    {
        var index = firstIndex;
        foreach (var id in ids)
        {
            var outcome = new[] { Convert.ToByte(index) };
            var record = new ReplicaLogRecord(index, 1UL, id, Scope, Fingerprint, "UserMutation", "cache", Key, "Set", Fingerprint, outcome, 0, clock.GetUtcNow().UtcDateTime.Ticks, 0, 0);
            var entries = new FollowerLogEntry[] { new(index, 1UL, ReplicaLogCodec.Encode(in record)) };
            var request = new FollowerLogAppendRequest("leader", 1UL, index - 1UL, index == 1UL ? 0UL : 1UL, 0UL, entries);
            _ = await log.AppendAsync(request, cancellationToken);
            _ = log.Idempotency.Reserve(Scope, id, Fingerprint, GroupRecordKind.UserMutation, index, 1UL);
            _ = log.Idempotency.TryResolve(Scope, id, outcome, index, 1UL);
            index++;
        }

        _ = await log.AdvanceCommitAsync(index - 1UL, cancellationToken);
        _ = await log.AdvanceAppliedAsync(index - 1UL, cancellationToken);
    }

    private static async Task<FollowerLog> OpenAsync(string dir, FakeTimeProvider clock, CancellationToken cancellationToken)
    {
        var options = new FollowerLogOptions { IdempotencyCapacity = 4, IdempotencyRetention = TimeSpan.FromMinutes(15), TimeProvider = clock };
        var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, options);
        try
        {
            await log.OpenAsync(cancellationToken);
            return log;
        }
        catch
        {
            await log.DisposeAsync();
            throw;
        }
    }
}
