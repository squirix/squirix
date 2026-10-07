using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>A group applier catches memory up from a group log in batches, and raises its applied index only upward.</summary>
public sealed class ReplicaGroupApplierTests : ServerUnitTestBase
{
    private const string GroupId = "grp-applier";
    private const int ReseededEventId = 4027;

    /// <summary>A durable applied index above the in-memory one raises the applied index and logs the gap, without applying anything.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CatchUpReseedsUpwardWhenDurableMoves(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-applier-reseed");
        await using var log = await SeedAsync(dir, 3, cancellationToken);
        var cache = new StubCache();
        var events = new EventRecordingLogger();
        var applier = new ReplicaGroupApplier(cache, events, GroupId, "n1");

        await applier.CatchUpAsync(log, 0UL, 2UL, cancellationToken);
        _ = await Assert.That(applier.AppliedIndex).IsEqualTo(2UL);
        _ = await Assert.That(events.Count(ReseededEventId)).IsEqualTo(0);

        await applier.CatchUpAsync(log, 3UL, 3UL, cancellationToken);

        _ = await Assert.That(applier.AppliedIndex).IsEqualTo(3UL);
        _ = await Assert.That(events.Count(ReseededEventId)).IsEqualTo(1);
        _ = await Assert.That(events.Find(ReseededEventId)?.Level).IsEqualTo(LogLevel.Warning);
        await SequenceAssert.EqualAsync(["k1", "k2"], cache.Applied.ToArray(), StringComparer.Ordinal);
    }

    /// <summary>A durable applied index below the in-memory one never lowers it, and nothing is applied twice.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CatchUpNeverSeedsDownward(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-applier-downward");
        await using var log = await SeedAsync(dir, 3, cancellationToken);
        var cache = new StubCache();
        var events = new EventRecordingLogger();
        var applier = new ReplicaGroupApplier(cache, events, GroupId, "n1");
        await applier.CatchUpAsync(log, 0UL, 3UL, cancellationToken);

        await applier.CatchUpAsync(log, 1UL, 3UL, cancellationToken);

        _ = await Assert.That(applier.AppliedIndex).IsEqualTo(3UL);
        _ = await Assert.That(events.Count(ReseededEventId)).IsEqualTo(0);
        await SequenceAssert.EqualAsync(["k1", "k2", "k3"], cache.Applied.ToArray(), StringComparer.Ordinal);
    }

    /// <summary>A backlog larger than one batch is applied in log order, starting right above the seeded applied index.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CatchUpReadsInBatchesAboveAppliedIndex(CancellationToken cancellationToken)
    {
        const int count = 1030;
        const int seeded = 5;
        using var dir = new TempDirectory("squirix-applier-batches");
        await using var log = await SeedAsync(dir, count, cancellationToken);
        var cache = new StubCache();
        var applier = new ReplicaGroupApplier(cache, NullLogger.Instance, GroupId, "n1");

        await applier.CatchUpAsync(log, ulong.CreateChecked(seeded), ulong.CreateChecked(count), cancellationToken);

        var expected = new string[count - seeded];
        for (var index = 0; index < expected.Length; index++)
            expected[index] = Key(ulong.CreateChecked(index + seeded + 1));

        _ = await Assert.That(applier.AppliedIndex).IsEqualTo(ulong.CreateChecked(count));
        await SequenceAssert.EqualAsync(expected, cache.Applied.ToArray(), StringComparer.Ordinal);
    }

    /// <summary>A commit index the log does not reach densely is refused after applying what is retained, and the applied index stays there.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CatchUpRefusesGapAndKeepsAppliedIndex(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-applier-gap");
        await using var log = await SeedAsync(dir, 3, cancellationToken);
        var cache = new StubCache();
        var applier = new ReplicaGroupApplier(cache, NullLogger.Instance, GroupId, "n1");

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(applier.CatchUpAsync(log, 0UL, 5UL, cancellationToken));

        _ = await Assert.That(applier.AppliedIndex).IsEqualTo(3UL);
        await SequenceAssert.EqualAsync(["k1", "k2", "k3"], cache.Applied.ToArray(), StringComparer.Ordinal);
    }

    private static string Key(ulong logIndex) => $"k{logIndex.ToString(CultureInfo.InvariantCulture)}";

    private static async Task<FollowerLog> SeedAsync(string dir, int count, CancellationToken cancellationToken)
    {
        var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance);
        try
        {
            await log.OpenAsync(cancellationToken);
            var factory = new ReplicaMutationFactory(new StubCache(), "n1", 1UL, TimeProvider.System, NullLogger.Instance);
            var entries = new FollowerLogEntry[count];
            for (var index = 0; index < entries.Length; index++)
            {
                var logIndex = ulong.CreateChecked(index + 1);
                var key = Key(logIndex);
                entries[index] = new FollowerLogEntry(logIndex, 1UL, factory.PrepareSet(NewOperationId(), "cache", key, Entry(key), logIndex).CanonicalPayload);
            }

            var appended = await log.AppendAsync(new FollowerLogAppendRequest("n1", 1UL, 0UL, 0UL, 0UL, entries), cancellationToken);
            if (!appended.Success)
                throw new InvalidOperationException($"The group log refused the entries: {appended.RefusalCode}.");

            _ = await log.AdvanceCommitAsync(ulong.CreateChecked(count), cancellationToken);
            return log;
        }
        catch
        {
            await log.DisposeAsync();
            throw;
        }
    }
}
