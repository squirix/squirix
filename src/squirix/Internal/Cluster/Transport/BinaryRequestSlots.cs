using System.Collections.Generic;
using System.Numerics;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Squirix.Internal.Cluster.Transport;

/// <summary>Reserves correlation IDs for a connection and admits overflow in arrival order.</summary>
internal sealed class BinaryRequestSlots
{
    internal const int Capacity = 128;
    private readonly Lock _gate = new();
    private readonly LinkedList<Waiter> _waiters = new();
    private ulong _low = ulong.MaxValue;
    private ulong _high = ulong.MaxValue;
    private volatile bool _queued;
    private volatile bool _closed;

    internal bool TryTake(out int id)
    {
        id = -1;
        return !_closed && !_queued && TakeAvailable(out id);
    }

    internal ValueTask<int> WaitAsync(CancellationToken token)
    {
        lock (_gate)
        {
            if (_closed)
                return ValueTask.FromException<int>(new ChannelClosedException());
            if (token.IsCancellationRequested)
                return ValueTask.FromCanceled<int>(token);
            if (!_queued && TakeAvailable(out var id))
                return ValueTask.FromResult(id);
            var waiter = new Waiter(this, token);
            _queued = true;
            _waiters.AddLast(waiter.Node);
            waiter.Watch();
            Drain();
            return new ValueTask<int>(waiter.Task);
        }
    }

    internal void Release(int id)
    {
        if (id < 64)
            _ = Interlocked.Or(ref _low, 1UL << id);
        else
            _ = Interlocked.Or(ref _high, 1UL << (id - 64));
        if (_queued)
        {
            lock (_gate)
                Drain();
        }
    }

    internal void Close()
    {
        lock (_gate)
        {
            _closed = true;
            while (_waiters.First is { } first)
            {
                _waiters.RemoveFirst();
                first.Value.Finish(-1);
            }

            _queued = false;
        }
    }

    private static bool Take(ref ulong bits, out int id)
    {
        var available = Volatile.Read(ref bits);
        while (available != 0)
        {
            var seen = Interlocked.CompareExchange(ref bits, available & (available - 1), available);
            if (seen == available)
            {
                id = BitOperations.TrailingZeroCount(available);
                return true;
            }

            available = seen;
        }

        id = -1;
        return false;
    }

    private bool TakeAvailable(out int id)
    {
        if (Take(ref _low, out id))
            return true;
        if (!Take(ref _high, out id))
            return false;
        id += 64;
        return true;
    }

    private void Drain()
    {
        while (_waiters.First is { } first && TakeAvailable(out var id))
        {
            _waiters.RemoveFirst();
            first.Value.Finish(id);
        }

        _queued = _waiters.Count != 0;
    }

    private void Cancel(Waiter waiter)
    {
        lock (_gate)
        {
            if (waiter.Node.List == null)
                return;
            _waiters.Remove(waiter.Node);
            _queued = _waiters.Count != 0;
            _ = waiter.TrySetCanceled(waiter.Token);
        }
    }

    private sealed class Waiter : TaskCompletionSource<int>
    {
        private readonly BinaryRequestSlots _owner;
        private CancellationTokenRegistration _registration;

        internal Waiter(BinaryRequestSlots owner, CancellationToken token)
            : base(TaskCreationOptions.RunContinuationsAsynchronously)
        {
            _owner = owner;
            Token = token;
            Node = new LinkedListNode<Waiter>(this);
        }

        internal CancellationToken Token { get; }
        internal LinkedListNode<Waiter> Node { get; }

        internal void Watch() => _registration = Token.UnsafeRegister(
            static state =>
            {
                if (state is Waiter waiter)
                    waiter._owner.Cancel(waiter);
            },
            this);

        internal void Finish(int id)
        {
            _ = _registration.Unregister();
            if (id >= 0)
                _ = TrySetResult(id);
            else
                _ = TrySetException(new ChannelClosedException());
        }
    }
}
