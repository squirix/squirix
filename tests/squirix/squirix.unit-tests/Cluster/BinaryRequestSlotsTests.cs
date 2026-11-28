using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Squirix.Internal.Cluster.Transport;
using Squirix.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests.Cluster;

/// <summary>Checks correlation slot ownership when queued requests cancel or the transport closes.</summary>
public sealed class BinaryRequestSlotsTests
{
    /// <summary>Returns freed IDs in FIFO order, skipping a canceled waiter, and wakes the rest on close.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CancellationPreservesSlotOrder(CancellationToken cancellationToken)
    {
        var slots = new BinaryRequestSlots();
        var ids = new HashSet<int>();
        for (var i = 0; i < BinaryRequestSlots.Capacity; i++)
        {
            var acquired = slots.TryTake(out var id);
            _ = await Assert.That(acquired && ids.Add(id)).IsTrue();
        }

        _ = await Assert.That(slots.TryTake(out _)).IsFalse();
        using var cancel = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var first = QueueAsync(slots, cancellationToken);
        var middle = QueueAsync(slots, cancel.Token);
        var third = QueueAsync(slots, cancellationToken);
        var last = QueueAsync(slots, cancellationToken);
        await cancel.CancelAsync();
        var error = await AsyncAssert.ThrowsAnyAsync<OperationCanceledException, int>(new ValueTask<int>(middle), TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);
        _ = await Assert.That(error.CancellationToken).IsEqualTo(cancel.Token);
        slots.Release(99);
        _ = await Assert.That(await first.WaitAsync(cancellationToken)).IsEqualTo(99);
        _ = await Assert.That(third.IsCompleted).IsFalse();
        slots.Release(25);
        _ = await Assert.That(await third.WaitAsync(cancellationToken)).IsEqualTo(25);
        slots.Close();
        _ = await AsyncAssert.ThrowsAsync<ChannelClosedException, int>(new ValueTask<int>(last), TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);
        slots.Release(25);
        _ = await Assert.That(slots.TryTake(out _)).IsFalse();
    }

    private static Task<int> QueueAsync(BinaryRequestSlots slots, CancellationToken token) => slots.WaitAsync(token).AsTask();
}
