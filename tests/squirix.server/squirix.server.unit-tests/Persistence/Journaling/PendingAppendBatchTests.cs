using System;
using System.Buffers;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Verifies batch completion and failure drains assign each buffer, counter and ack to exactly one owner.</summary>
public sealed class PendingAppendBatchTests
{
    /// <summary>Missing and duplicate items cannot claim another entry or decrement pending counters twice.</summary>
    [Test]
    public async Task BatchClaimsOnlyRemainingUniqueEntries()
    {
        var registry = new PendingAppendRegistry();
        var counter = new MutableInt32();
        var first = Track(registry, counter, 16);
        var second = Track(registry, counter, 32);
        var third = Track(registry, counter, 48);
        _ = await Assert.That(registry.Untrack(first, out var removed)).IsTrue();
        _ = await Assert.That(removed).IsNotNull();
        var completed = new List<PendingAppendEntry> { removed };
        ReturnOwned(removed, counter);

        registry.UntrackBatch([first, second, second, third], completed);
        _ = await Assert.That(completed.Count).IsEqualTo(2);
        _ = await Assert.That(completed[0].Item).IsSameReferenceAs(second);
        _ = await Assert.That(completed[1].Item).IsSameReferenceAs(third);
        _ = await Assert.That(registry.PendingBytes).IsEqualTo(0L);
        _ = await Assert.That(registry.PendingCount).IsEqualTo(0);
        for (var i = 0; i < completed.Count; i++)
            ReturnOwned(completed[i], counter);

        registry.UntrackBatch([second, third], completed);
        _ = await Assert.That(completed.Count).IsEqualTo(0);
        _ = await Assert.That(counter.Value).IsEqualTo(0);
    }

    /// <summary>A value-equal work item that was not tracked cannot take the tracked object's buffer.</summary>
    [Test]
    public async Task BatchUsesReferenceIdentity()
    {
        var registry = new PendingAppendRegistry();
        var counter = new MutableInt32();
        var original = Track(registry, counter, 32);
        var copy = JournalWorkItem.Append(original.FrameBytes ?? [], original.FrameLength, original.Ack);
        var completed = new List<PendingAppendEntry>();

        _ = await Assert.That(copy.Equals(original)).IsTrue();
        registry.UntrackBatch([copy], completed);
        _ = await Assert.That(completed.Count).IsEqualTo(0);
        _ = await Assert.That(registry.PendingCount).IsEqualTo(1);
        registry.UntrackBatch([original], completed);
        _ = await Assert.That(completed.Count).IsEqualTo(1);
        ReturnOwned(completed[0], counter);
        _ = await Assert.That(counter.Value).IsEqualTo(0);
    }

    /// <summary>A prior drain owns all resources and failures; a late batch claims nothing.</summary>
    [Test]
    public async Task DrainBeforeBatchOwnsEveryItem()
    {
        var registry = new PendingAppendRegistry();
        var counter = new MutableInt32();
        var first = Track(registry, counter, 32);
        var second = Track(registry, counter, 48);
        var reason = new InvalidOperationException("pipeline failed");
        _ = await Assert.That(registry.FailAll(reason, NullLogger.Instance, counter)).IsEqualTo(2);
        var completed = new List<PendingAppendEntry>();

        registry.UntrackBatch([first, second], completed);
        _ = await Assert.That(completed.Count).IsEqualTo(0);
        _ = await Assert.That(counter.Value).IsEqualTo(0);
        _ = await Assert.That(registry.PendingBytes).IsEqualTo(0L);
        _ = await Assert.That(first.Ack?.Task.Exception?.InnerException).IsSameReferenceAs(reason);
        _ = await Assert.That(second.Ack?.Task.Exception?.InnerException).IsSameReferenceAs(reason);
        _ = await Assert.That(registry.ReturnQuarantinedBuffers()).IsEqualTo(2);
        _ = await Assert.That(registry.ReturnQuarantinedBuffers()).IsEqualTo(0);
    }

    /// <summary>A completed batch keeps its ownership when a later drain takes unrelated appends.</summary>
    [Test]
    public async Task BatchBeforeDrainKeepsItsCompletions()
    {
        var registry = new PendingAppendRegistry();
        var counter = new MutableInt32();
        var first = Track(registry, counter, 32);
        var second = Track(registry, counter, 48);
        var unrelated = Track(registry, counter, 64);
        var completed = new List<PendingAppendEntry>();

        registry.UntrackBatch([first, second], completed);
        _ = await Assert.That(registry.PendingBytes).IsEqualTo(64L);
        _ = await Assert.That(registry.PendingCount).IsEqualTo(1);
        var reason = new InvalidOperationException("pipeline failed");
        _ = await Assert.That(registry.FailAll(reason, NullLogger.Instance, counter)).IsEqualTo(1);
        _ = await Assert.That(first.Ack?.Task.IsCompleted).IsFalse();
        _ = await Assert.That(second.Ack?.Task.IsCompleted).IsFalse();
        _ = await Assert.That(unrelated.Ack?.Task.Exception?.InnerException).IsSameReferenceAs(reason);
        for (var i = 0; i < completed.Count; i++)
            ReturnOwned(completed[i], counter);
        _ = await Assert.That(counter.Value).IsEqualTo(0);
        _ = await Assert.That(registry.ReturnQuarantinedBuffers()).IsEqualTo(1);
    }

    /// <summary>A simultaneous total drain and batch completion never share ownership or leak a counter slot.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RacingBatchAndDrainConserveEveryResource(CancellationToken cancellationToken)
    {
        for (var round = 0; round < 128; round++)
        {
            var registry = new PendingAppendRegistry();
            var counter = new MutableInt32();
            var items = new JournalWorkItem[8];
            for (var i = 0; i < items.Length; i++)
                items[i] = Track(registry, counter, 32);
            var completed = new List<PendingAppendEntry>();
            var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var reason = new InvalidOperationException("pipeline failed");
            var claimed = ClaimAsync();
            var drained = DrainAsync();
            _ = start.TrySetResult();
            await Task.WhenAll(claimed, drained).WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);

            _ = await Assert.That(completed.Count is 0 or 8).IsTrue();
            var owned = new HashSet<JournalWorkItem>(ReferenceEqualityComparer.Instance);
            for (var i = 0; i < completed.Count; i++)
            {
                _ = await Assert.That(owned.Add(completed[i].Item)).IsTrue();
                ReturnOwned(completed[i], counter);
            }

            for (var i = 0; i < items.Length; i++)
            {
                if (owned.Contains(items[i]))
                    _ = await Assert.That(items[i].Ack?.Task.IsCompletedSuccessfully).IsTrue();
                else
                    _ = await Assert.That(items[i].Ack?.Task.Exception?.InnerException).IsSameReferenceAs(reason);
            }

            _ = await Assert.That(counter.Value).IsEqualTo(0);
            _ = await Assert.That(registry.PendingCount).IsEqualTo(0);
            _ = await Assert.That(registry.PendingBytes).IsEqualTo(0L);
            _ = await Assert.That(registry.ReturnQuarantinedBuffers()).IsEqualTo(items.Length - completed.Count);
            _ = await Assert.That(registry.ReturnQuarantinedBuffers()).IsEqualTo(0);

            async Task ClaimAsync()
            {
                await start.Task;
                registry.UntrackBatch(items, completed);
            }

            async Task DrainAsync()
            {
                await start.Task;
                _ = registry.FailAll(reason, NullLogger.Instance, counter);
            }
        }
    }

    private static void ReturnOwned(PendingAppendEntry entry, MutableInt32 counter)
    {
        _ = Interlocked.Decrement(ref counter.Value);
        ArrayPool<byte>.Shared.ReturnCleared(entry.FrameBytes);
        _ = entry.Ack?.TrySetResult();
    }

    private static JournalWorkItem Track(PendingAppendRegistry registry, MutableInt32 counter, int length)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(length);
        var ack = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var item = JournalWorkItem.Append(buffer, length, ack);
        registry.Track(item, buffer, length, ack);
        _ = Interlocked.Increment(ref counter.Value);
        return item;
    }
}
