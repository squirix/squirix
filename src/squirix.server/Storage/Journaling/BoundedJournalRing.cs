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
    private int _disposed;
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
        Volatile.Write(ref _disposed, 1);
        _workSignal.Dispose();
        _availableSlots.Dispose();
    }

    internal async ValueTask EnqueueAsync(JournalWorkItem item, CancellationToken cancellationToken)
    {
        await _availableSlots.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            while (!TryEnqueueCore(in item))
                Thread.SpinWait(32);

            NotifyWorkAvailable();
        }
        catch
        {
            _ = _availableSlots.Release();
            throw;
        }
    }

    internal void NotifyWorkAvailable()
    {
        if (Volatile.Read(ref _disposed) == 1)
            return;

        // Best-effort: during teardown the signal may already be disposed; a missed wake is
        // harmless because the journal loop re-evaluates on its next spin or timeout. Letting the
        // exception escape here would wrongly release the slot for an item that is already published
        // and corrupt the ring accounting.
        try
        {
            _ = _workSignal.Set();
        }
        catch (ObjectDisposedException)
        {
            Volatile.Write(ref _disposed, 1);
        }
    }

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
