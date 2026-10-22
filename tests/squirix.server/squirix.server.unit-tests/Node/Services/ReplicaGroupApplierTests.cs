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
using Squirix.Server.TestKit.Replication;
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
    private const ulong WaiterCount = 200;
    private const ulong RegisteringThreads = 4;

    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

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

    /// <summary>A reader waiting for an unapplied index completes as soon as the applied index reaches it, and not on a smaller advance.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WaitAppliedWakesOnAdvance(CancellationToken cancellationToken)
    {
        var applier = new ReplicaGroupApplier(new StubCache(), NullLogger.Instance, GroupId, "n1");
        applier.ResetTo(2UL);
        var wait = applier.WaitAppliedAsync(5UL, cancellationToken);

        applier.ResetTo(4UL);
        var belowIndex = wait.IsCompleted;
        applier.ResetTo(5UL);
        await wait.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

        _ = await Assert.That(belowIndex).IsFalse();
        _ = await Assert.That(applier.WaitAppliedAsync(5UL, cancellationToken).IsCompletedSuccessfully).IsTrue();
    }

    /// <summary>A canceled wait ends with the cancellation and leaves the applier usable for later waiters.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WaitAppliedEndsOnCancellation(CancellationToken cancellationToken)
    {
        var applier = new ReplicaGroupApplier(new StubCache(), NullLogger.Instance, GroupId, "n1");
        using var source = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var canceled = applier.WaitAppliedAsync(1UL, source.Token);
        await source.CancelAsync();
        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(canceled);

        var later = applier.WaitAppliedAsync(1UL, cancellationToken);
        applier.ResetTo(1UL);
        await later.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
    }

    /// <summary>Readers registering from several threads while the applied index advances concurrently all complete, none lost.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConcurrentWaitersAllWake(CancellationToken cancellationToken)
    {
        var race = new WaitRace(new ReplicaGroupApplier(new StubCache(), NullLogger.Instance, GroupId, "n1"), cancellationToken);
        race.RunAll();

        await Task.WhenAll(race.Waiters).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

        _ = await Assert.That(race.Applier.AppliedIndex).IsEqualTo(WaiterCount);
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
        var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, TimeProvider.System);
        try
        {
            await log.OpenSeededAsync(cancellationToken);
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

    /// <summary>Registers waiters for indexes 1..WaiterCount from several threads while one thread advances the applied index through them.</summary>
    private sealed class WaitRace
    {
        private readonly Barrier _start = new(int.CreateChecked(RegisteringThreads) + 1);
        private readonly CancellationToken _cancellationToken;
        private ulong _slots;

        internal WaitRace(ReplicaGroupApplier applier, CancellationToken cancellationToken)
        {
            Applier = applier;
            _cancellationToken = cancellationToken;
        }

        internal ReplicaGroupApplier Applier { get; }

        internal Task[] Waiters { get; } = new Task[WaiterCount];

        internal void RunAll()
        {
            var threads = new Thread[RegisteringThreads + 1];
            for (var i = 0; i < threads.Length; i++)
            {
                threads[i] = new Thread(static state =>
                {
                    if (state is WaitRace running)
                        running.Run();
                });
                threads[i].Start(this);
            }

            for (var i = 0; i < threads.Length; i++)
                threads[i].Join();
        }

        private void Run()
        {
            var slot = Interlocked.Increment(ref _slots);
            _start.SignalAndWait(_cancellationToken);
            if (slot == RegisteringThreads + 1)
            {
                for (var index = 1UL; index <= WaiterCount; index++)
                {
                    Applier.ResetTo(index);
                    if (index % 7UL == 0UL)
                        _ = Thread.Yield();
                }

                return;
            }

            for (var index = slot; index <= WaiterCount; index += RegisteringThreads)
                Waiters[index - 1UL] = Applier.WaitAppliedAsync(index, _cancellationToken);
        }
    }
}
