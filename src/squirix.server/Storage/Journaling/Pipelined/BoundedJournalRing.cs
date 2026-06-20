using System;
using System.Numerics;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Storage.Journaling.Pipelined;

/// <summary>Bounded ring queue for journal work items (multi-producer, single consumer).</summary>
#pragma warning disable CA1001 // SemaphoreSlim lifetime matches the journal coordinator.
internal sealed class BoundedJournalRing
{
    private readonly JournalWorkItem[] _slots;
    private readonly int[] _published;
    private readonly SemaphoreSlim _availableSlots;
    private readonly int _mask;
    private long _head;
    private long _tail;

    public BoundedJournalRing(int capacity)
    {
        if (capacity <= 0 || !BitOperations.IsPow2(capacity))
            throw new ArgumentOutOfRangeException(nameof(capacity), "capacity must be a power of two.");

        _slots = new JournalWorkItem[capacity];
        _published = new int[capacity];
        _mask = capacity - 1;
        _availableSlots = new SemaphoreSlim(capacity, capacity);
    }

#pragma warning disable MA0045, MA0032 // Shutdown enqueue is synchronous from journal dispose.
    public void EnqueueBlocking(JournalWorkItem item)
    {
        _availableSlots.Wait(CancellationToken.None);
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
    }
#pragma warning restore MA0045, MA0032

    public async ValueTask EnqueueAsync(JournalWorkItem item, CancellationToken cancellationToken)
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
    }

    public bool TryDequeue(out JournalWorkItem item)
    {
        if (!TryDequeueCore(out item))
            return false;

        _ = _availableSlots.Release();
        return true;
    }

    public void SpinWaitForWork(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            if (Volatile.Read(ref _tail) > Volatile.Read(ref _head))
                return;

            Thread.SpinWait(64);
        }
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

    private bool TryDequeueCore(out JournalWorkItem item)
    {
        var head = Volatile.Read(ref _head);
        var tail = Volatile.Read(ref _tail);
        if (head >= tail)
        {
            item = default;
            return false;
        }

        var index = Convert.ToInt32(head & _mask);
        if (Volatile.Read(ref _published[index]) is 0)
        {
            item = default;
            return false;
        }

        item = _slots[index];
        Volatile.Write(ref _published[index], 0);
        Volatile.Write(ref _head, head + 1);
        return true;
    }
}
#pragma warning restore CA1001
