using System;
using System.Numerics;
using System.Threading;

namespace Squirix.Server.Storage.Journaling.PipelinedWal.Backends.Pipelined;

/// <summary>Bounded multi-producer single-consumer ring for WAL work items.</summary>
internal sealed class BoundedMpscRing
{
    private readonly WalWorkItem[] _slots;
    private readonly int _mask;
    private long _head;
    private long _tail;

    public BoundedMpscRing(int capacityPow2)
    {
        if (capacityPow2 <= 0 || !BitOperations.IsPow2(capacityPow2))
            throw new ArgumentOutOfRangeException(nameof(capacityPow2), "capacity must be a power of two.");

        _slots = new WalWorkItem[capacityPow2];
        _mask = capacityPow2 - 1;
    }

    public bool TryEnqueue(ref readonly WalWorkItem item)
    {
        while (true)
        {
            var tail = Interlocked.Read(ref _tail);
            var head = Volatile.Read(ref _head);
            if (tail - head >= _slots.Length)
                return false;

            if (Interlocked.CompareExchange(ref _tail, tail + 1, tail) != tail)
                continue;

            _slots[Convert.ToInt32(tail & _mask)] = item;
            return true;
        }
    }

    public bool TryDequeue(out WalWorkItem item)
    {
        var head = _head;
        var tail = Volatile.Read(ref _tail);
        if (head >= tail)
        {
            item = default;
            return false;
        }

        item = _slots[Convert.ToInt32(head & _mask)];
        _head = head + 1;
        return true;
    }

    public void SpinWaitForWork(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            if (Volatile.Read(ref _tail) > _head)
                return;

            Thread.SpinWait(64);
        }
    }
}
