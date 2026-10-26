using System;
using System.Buffers.Binary;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaStateHarness;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>The sink of a state transfer wipes a group in bounded steps, checksums what it writes, and awaits the journal barrier before it reports.</summary>
[Immutable]
public sealed class ReplicaStateSinkTests : ServerUnitTestBase
{
    /// <summary>A wipe step examines no more keys than it is allowed, reports whether more may remain, and leaves the keys of other groups alone.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WipeStepRespectsBound(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-sink-wipe");
        await using var harness = await OpenAsync(dir, cancellationToken);
        await harness.SeedAsync(cancellationToken, "g-1", "g-2", "g-3", "g-4", "g-5", "other-1", "other-2", "other-3");
        _ = await Assert.That(await harness.Sink.EnterAsync(GroupId, cancellationToken)).IsTrue();

        var first = await harness.Sink.WipeStepAsync(GroupId, 2, cancellationToken);
        var afterFirst = await GroupKeysAsync(harness, cancellationToken);
        var second = await harness.Sink.WipeStepAsync(GroupId, 2, cancellationToken);
        var afterSecond = await GroupKeysAsync(harness, cancellationToken);
        var third = await harness.Sink.WipeStepAsync(GroupId, 2, cancellationToken);
        var afterThird = await GroupKeysAsync(harness, cancellationToken);

        _ = await Assert.That((first, second, third)).IsEqualTo((ReplicaWipeStep.More, ReplicaWipeStep.More, ReplicaWipeStep.Done));
        _ = await Assert.That((afterFirst, afterSecond, afterThird)).IsEqualTo((3, 1, 0));
        _ = await Assert.That(await OtherKeysAsync(harness, cancellationToken)).IsEqualTo(3);
        harness.Sink.Leave(GroupId);
    }

    /// <summary>The checksum over the written entries equals one computed independently from the documented layout, and counts the entries.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ChecksumMatchesIndependentComputation(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-sink-crc");
        await using var harness = await OpenAsync(dir, cancellationToken);
        _ = await Assert.That(await harness.Sink.EnterAsync(GroupId, cancellationToken)).IsTrue();
        var entries = new[] { Entry("k-1", "a", 0L), Entry("k-2", "bb", 636_000_000_000_000_000L), Entry("k-3", "ccc", 1L) };

        _ = await harness.Sink.WriteAsync(GroupId, entries.AsMemory(0, 2), cancellationToken);
        _ = await harness.Sink.WriteAsync(GroupId, entries.AsMemory(2, 1), cancellationToken);
        var totals = await harness.Sink.SealAsync(GroupId, cancellationToken);

        var crc = Crc32C.InitialValue;
        for (var i = 0; i < entries.Length; i++)
        {
            crc = AppendField(crc, Encoding.UTF8.GetBytes(entries[i].CacheName));
            crc = AppendField(crc, entries[i].Key.Span);
            crc = AppendField(crc, entries[i].Payload.Span);
            crc = AppendTicks(crc, entries[i].ExpiresUtcTicks);
        }

        _ = await Assert.That((totals.EntryCount, totals.Crc)).IsEqualTo((3UL, Crc32C.Finalize(crc)));
        harness.Sink.Leave(GroupId);
    }

    /// <summary>The seal awaits the journal durability barrier before it reports, and entering again starts the count over.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SealAwaitsBarrier(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-sink-barrier");
        await using var harness = await OpenAsync(dir, cancellationToken);
        _ = await Assert.That(await harness.Sink.EnterAsync(GroupId, cancellationToken)).IsTrue();
        _ = await harness.Sink.WriteAsync(GroupId, new[] { Entry("k-1", "a", 0L) }, cancellationToken);
        _ = await Assert.That(harness.BarrierStates.Count).IsEqualTo(0);

        var totals = await harness.Sink.SealAsync(GroupId, cancellationToken);

        _ = await Assert.That((harness.BarrierStates.Count, totals.EntryCount)).IsEqualTo((1, 1UL));
        harness.Sink.Leave(GroupId);
        _ = await Assert.That(await harness.Sink.EnterAsync(GroupId, cancellationToken)).IsTrue();
        _ = await Assert.That((await harness.Sink.SealAsync(GroupId, cancellationToken)).EntryCount).IsEqualTo(0UL);
        harness.Sink.Leave(GroupId);
    }

    /// <summary>A group whose apply driver is held is not entered, and leaving releases the driver.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EnterRefusesHeldDriverAndLeaveReleasesIt(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-sink-enter");
        await using var harness = await OpenAsync(dir, cancellationToken);
        var lease = harness.Appliers.For(GroupId).DriverLease;
        await lease.LeadAsync(cancellationToken);
        _ = await Assert.That(await harness.Sink.EnterAsync(GroupId, cancellationToken)).IsFalse();
        lease.EndLeading();

        _ = await Assert.That(await harness.Sink.EnterAsync(GroupId, cancellationToken)).IsTrue();
        _ = await Assert.That(lease.TryEnterPass()).IsFalse();
        harness.Sink.Leave(GroupId);

        _ = await Assert.That(lease.TryEnterPass()).IsTrue();
        lease.ExitPass();
    }

    /// <summary>An entry whose payload does not decode is refused as invalid data, and the applied index can be reseeded without a gap report.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InvalidPayloadIsRefusedAndResetSeeds(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-sink-invalid");
        await using var harness = await OpenAsync(dir, cancellationToken);
        _ = await Assert.That(await harness.Sink.EnterAsync(GroupId, cancellationToken)).IsTrue();
        var broken = new ReplicaStateEntry(CacheName, Encoding.UTF8.GetBytes("k"), new byte[] { 255, 255, 255 }, 0L);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(harness.Sink.WriteAsync(GroupId, new[] { broken }, cancellationToken).AsTask());
        harness.Sink.ResetAppliedTo(GroupId, 7UL);

        _ = await Assert.That(harness.Appliers.For(GroupId).AppliedIndex).IsEqualTo(7UL);
        _ = await Assert.That(harness.Events.Count(4027)).IsEqualTo(0);
        harness.Sink.Leave(GroupId);
    }

    /// <summary>At the high-water mark a wipe step stops with the examined key kept, and the next step removes that key first, so no key of the group is left behind.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WipeAtHighWaterKeepsTheExaminedKey(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-sink-wipe-busy");
        await using var harness = await OpenAsync(dir, cancellationToken);
        await harness.SeedAsync(cancellationToken, "g-1", "g-2", "g-3");
        _ = await Assert.That(await harness.Sink.EnterAsync(GroupId, cancellationToken)).IsTrue();
        harness.Pressure.AtHighWater = true;

        var busy = await harness.Sink.WipeStepAsync(GroupId, 10, cancellationToken);
        var kept = await GroupKeysAsync(harness, cancellationToken);
        harness.Pressure.AtHighWater = false;
        var done = await harness.Sink.WipeStepAsync(GroupId, 10, cancellationToken);

        _ = await Assert.That((busy, kept, done)).IsEqualTo((ReplicaWipeStep.JournalBusy, 3, ReplicaWipeStep.Done));
        _ = await Assert.That(await GroupKeysAsync(harness, cancellationToken)).IsEqualTo(0);
        harness.Sink.Leave(GroupId);
    }

    /// <summary>A removal the journal quota refuses leaves the key in place and pending: the next step removes it and nothing is skipped.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RefusedRemovalIsRetriedFirst(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-sink-wipe-refused");
        var refusals = new RefusalScript(0);
        await using var harness = await OpenAsync(dir, cancellationToken, decorateSinkCache: refusals.Wrap);
        await harness.SeedAsync(cancellationToken, "g-1", "g-2", "g-3");
        _ = await Assert.That(await harness.Sink.EnterAsync(GroupId, cancellationToken)).IsTrue();

        var busy = await harness.Sink.WipeStepAsync(GroupId, 10, cancellationToken);
        var kept = await GroupKeysAsync(harness, cancellationToken);
        var done = await harness.Sink.WipeStepAsync(GroupId, 10, cancellationToken);

        _ = await Assert.That((busy, kept, done)).IsEqualTo((ReplicaWipeStep.JournalBusy, 3, ReplicaWipeStep.Done));
        _ = await Assert.That(await GroupKeysAsync(harness, cancellationToken)).IsEqualTo(0);
        harness.Sink.Leave(GroupId);
    }

    /// <summary>A write stops at the first entry the journal quota refuses, reports how many were written, and the rest written later give the checksum of one pass.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RefusedWriteStopsAtEntryAndResumes(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-sink-write-refused");
        var refusals = new RefusalScript(1);
        await using var harness = await OpenAsync(dir, cancellationToken, decorateSinkCache: refusals.Wrap);
        _ = await Assert.That(await harness.Sink.EnterAsync(GroupId, cancellationToken)).IsTrue();
        var entries = new[] { Entry("k-1", "a", 0L), Entry("k-2", "bb", 0L), Entry("k-3", "ccc", 0L) };

        var first = await harness.Sink.WriteAsync(GroupId, entries, cancellationToken);
        var second = await harness.Sink.WriteAsync(GroupId, entries.AsMemory(first), cancellationToken);
        var totals = await harness.Sink.SealAsync(GroupId, cancellationToken);

        var onePass = TotalsOf(entries);
        _ = await Assert.That((first, second)).IsEqualTo((1, 2));
        _ = await Assert.That((totals.EntryCount, totals.Crc)).IsEqualTo((onePass.EntryCount, onePass.Crc));
        _ = await Assert.That(await ReadAsync(harness, "k-3", cancellationToken)).IsNotNull();
        harness.Sink.Leave(GroupId);
    }

    /// <summary>At the high-water mark a write writes nothing, and memory and the checksum stay unchanged.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WriteAtHighWaterWritesNothing(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-sink-write-busy");
        await using var harness = await OpenAsync(dir, cancellationToken);
        _ = await Assert.That(await harness.Sink.EnterAsync(GroupId, cancellationToken)).IsTrue();
        var entries = new[] { Entry("k-1", "a", 0L), Entry("k-2", "bb", 0L) };
        harness.Pressure.AtHighWater = true;

        var written = await harness.Sink.WriteAsync(GroupId, entries, cancellationToken);
        var totals = await harness.Sink.SealAsync(GroupId, cancellationToken);

        _ = await Assert.That((written, totals.EntryCount)).IsEqualTo((0, 0UL));
        _ = await Assert.That(await ReadAsync(harness, "k-1", cancellationToken)).IsNull();
        harness.Sink.Leave(GroupId);
    }

    private static uint AppendTicks(uint crc, long ticks)
    {
        Span<byte> bytes = stackalloc byte[sizeof(long)];
        BinaryPrimitives.WriteInt64LittleEndian(bytes, ticks);
        return Crc32C.Append(crc, bytes);
    }

    private static uint AppendField(uint crc, ReadOnlySpan<byte> bytes)
    {
        Span<byte> length = stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32LittleEndian(length, bytes.Length);
        return Crc32C.Append(Crc32C.Append(crc, length), bytes);
    }

    private static async Task<int> CountAsync(ReplicaStateHarness harness, bool group, CancellationToken cancellationToken)
    {
        var count = 0;
        await foreach (var (key, _) in harness.Cache.EnumerateLiveAsync(cancellationToken))
        {
            if (key.Key.StartsWith("other", StringComparison.Ordinal) != group)
                count++;
        }

        return count;
    }

    private static Task<int> GroupKeysAsync(ReplicaStateHarness harness, CancellationToken cancellationToken) => CountAsync(harness, true, cancellationToken);

    private static Task<int> OtherKeysAsync(ReplicaStateHarness harness, CancellationToken cancellationToken) => CountAsync(harness, false, cancellationToken);

    private static Task<NodeCacheEntry<object?>?> ReadAsync(ReplicaStateHarness harness, string key, CancellationToken cancellationToken) =>
        harness.Cache.GetEntryAsync(new CacheKey(CacheName, key), cancellationToken).AsTask();

    /// <summary>Refuses the writes at the given positions, counted over all removals and stores, as the journal quota does: the call throws and changes nothing.</summary>
    [ThreadSafe]
    private sealed class RefusalScript
    {
        private readonly int[] _refuseAt;
        private int _calls;

        internal RefusalScript(params int[] refuseAt)
        {
            _refuseAt = refuseAt;
        }

        internal ILogicalNamespacedCache<object?> Wrap(ILogicalNamespacedCache<object?> inner)
        {
            var rock = new ILogicalNamespacedCacheCreateExpectations<object?>();
            _ = rock.Setups.RemoveAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
                    .Callback((operationId, cacheName, key, token) => Next() ? throw Refusal() : inner.RemoveAsync(operationId, cacheName, key, token));
            _ = rock.Setups.SetEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<NodeCacheEntry<object?>>(), Arg.Any<int?>(), Arg.Any<CancellationToken>())
                    .Callback((operationId, cacheName, key, entry, length, token) => Next() ? throw Refusal() : inner.SetEntryAsync(operationId, cacheName, key, entry, length, token));
            return rock.Instance();
        }

        private static JournalCapacityExceededException Refusal() => new("journal total bytes exceed configured limit.");

        private bool Next() => Array.IndexOf(_refuseAt, Interlocked.Increment(ref _calls) - 1) >= 0;
    }
}
