using System;
using System.Diagnostics.CodeAnalysis;
using System.Numerics;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Bounded ring queue for journal work items (multi-producer, single consumer).</summary>
internal sealed class BoundedJournalRing : IDisposable
{
    private readonly SemaphoreSlim _availableSlots;
    private readonly int _mask;
    private readonly int[] _published;
    private readonly JournalWorkItem[] _slots;
    private readonly AutoResetEvent _workSignal = new(false);
    private long _head;
    private long _tail;

    internal BoundedJournalRing(int capacity)
    {
        if (capacity <= 0 || !BitOperations.IsPow2(capacity))
            throw new ArgumentOutOfRangeException(nameof(capacity), "capacity must be a power of two.");

        _slots = new JournalWorkItem[capacity];
        _published = new int[capacity];
        _mask = capacity - 1;
        _availableSlots = new SemaphoreSlim(capacity, capacity);
    }

    public void Dispose()
    {
        _workSignal.Dispose();
        _availableSlots.Dispose();
    }

    /// <summary>Enqueues a work item, blocking until a slot is available.</summary>
    /// <param name="item">The work item to publish into the ring.</param>
    /// <param name="cancellationToken">Token that cancels the wait for a free slot.</param>
    /// <returns>A task that completes when the item has been published to the ring.</returns>
    /// <remarks>
    /// <para>
    /// Acceptance contract: when this method throws <see cref="OperationCanceledException" />, the item was
    /// <b>not</b> admitted to the ring. Slot acquisition (<c>_availableSlots.WaitAsync</c>) is the only await
    /// before publication, and it runs before <see cref="TryEnqueueCore" />; therefore a cancellation that
    /// surfaces there leaves the ring untouched. The synchronous publication spin-loop contains no await, so
    /// once the slot is acquired the item is published and no cancellation can interrupt it.
    /// </para>
    /// <para>Callers rely on this boundary to decide ack ownership: an un-admitted item means the caller still owns its ack.</para>
    /// </remarks>
    internal async ValueTask EnqueueAsync(JournalWorkItem item, CancellationToken cancellationToken)
    {
        await _availableSlots.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            while (!TryEnqueueCore(in item))
                Thread.SpinWait(32);
        }
        catch
        {
            _ = _availableSlots.Release();
            throw;
        }

        // The item is admitted once the publication loop exits. Signal after admission, outside the slot-release
        // guard: the published item has consumed a slot, so a signal failure during teardown must not release it
        // again (double-release corrupts the slot semaphore) nor be mistaken for a not-admitted item.
        NotifyWorkAvailable();
    }

    internal void NotifyWorkAvailable() => _ = _workSignal.Set();

    internal bool TryDequeue([NotNullWhen(true)] out JournalWorkItem? item)
    {
        if (!TryDequeueCore(out item))
            return false;

        _ = _availableSlots.Release();
        return true;
    }

    internal void WaitForWork(int timeoutMs, CancellationToken cancellationToken)
    {
        if (HasQueuedWork() || cancellationToken.IsCancellationRequested || timeoutMs == 0)
            return;

        var waitMs = timeoutMs;
        if (timeoutMs != Timeout.Infinite)
        {
            waitMs = ComputeRemainingWaitMs(Environment.TickCount64 + timeoutMs);
            if (waitMs == 0)
                return;
        }

        // A fired signal means "re-evaluate": ring work, a group-commit deadline, or manifest roll
        // completion. Returning here lets the journal loop re-drain and recompute its next wait so a
        // group-commit notify (which adds no ring item) can never be lost.
        _ = _workSignal.WaitOne(waitMs);
    }

    private static int ComputeRemainingWaitMs(long deadline)
    {
        var remaining = deadline - Environment.TickCount64;
        if (remaining <= 0)
            return 0;

        return remaining > int.MaxValue ? int.MaxValue : Convert.ToInt32(remaining);
    }

    private bool HasQueuedWork() => Volatile.Read(ref _tail) > Volatile.Read(ref _head);

    private bool TryDequeueCore([NotNullWhen(true)] out JournalWorkItem? item)
    {
        var head = Volatile.Read(ref _head);
        var tail = Volatile.Read(ref _tail);
        if (head >= tail)
        {
            item = null;
            return false;
        }

        var index = Convert.ToInt32(head & _mask);
        if (Volatile.Read(ref _published[index]) == 0)
        {
            item = null;
            return false;
        }

        item = _slots[index];
        Volatile.Write(ref _published[index], 0);
        Volatile.Write(ref _head, head + 1);
        return true;
    }

    private bool TryEnqueueCore(ref readonly JournalWorkItem item)
    {
        while (true)
        {
            var tail = Interlocked.Read(ref _tail);
            var head = Volatile.Read(ref _head);
            if (tail - head >= _slots.Length)
                return false;

            if (Interlocked.CompareExchange(ref _tail, tail + 1, tail) != tail)
                continue;

            var index = Convert.ToInt32(tail & _mask);
            _slots[index] = item;
            Volatile.Write(ref _published[index], 1);
            return true;
        }
    }
}
