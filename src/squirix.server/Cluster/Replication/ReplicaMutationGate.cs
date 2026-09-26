using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Threading;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Bounds in-flight group mutations and serializes conflicts through a fixed number of key stripes.</summary>
/// <remarks>Disposing the gate faults every entry still queued on the capacity or on a stripe with <see cref="ObjectDisposedException" />.</remarks>
[ThreadSafe]
internal sealed class ReplicaMutationGate : IDisposable
{
    private readonly AsyncSemaphore _capacity;
    private readonly AsyncLock[] _stripes;
    private int _activeCount;

    internal ReplicaMutationGate(int maxInFlight, int stripeCount = 64)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxInFlight);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(stripeCount);

        MaxInFlight = maxInFlight;
        _capacity = new AsyncSemaphore(maxInFlight);
        _stripes = new AsyncLock[stripeCount];
        for (var i = 0; i < _stripes.Length; i++)
            _stripes[i] = new AsyncLock();
    }

    internal int ActiveCount => Volatile.Read(ref _activeCount);

    internal int MaxInFlight { get; }

    internal int StripeCount => _stripes.Length;

    public void Dispose()
    {
        _capacity.Dispose();
        for (var i = 0; i < _stripes.Length; i++)
            _stripes[i].Dispose();
    }

    /// <summary>Waits for a capacity slot and the key's stripe.</summary>
    /// <param name="keyHash">Hash of the key whose mutations are serialized.</param>
    /// <param name="cancellationToken">Cancels the wait while it is still queued.</param>
    /// <returns>The lease that returns the stripe and the slot when disposed.</returns>
    /// <exception cref="ObjectDisposedException">The gate was disposed before or while the entry was queued.</exception>
    internal async ValueTask<ReplicaMutationLease> EnterAsync(int keyHash, CancellationToken cancellationToken)
    {
        await _capacity.WaitAsync(cancellationToken).ConfigureAwait(false);
        AsyncLockHolder stripe;
        try
        {
            stripe = await _stripes[(keyHash & int.MaxValue) % _stripes.Length].LockAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            _capacity.Release();
            throw;
        }

        _ = Interlocked.Increment(ref _activeCount);
        return new ReplicaMutationLease(this, stripe);
    }

    /// <summary>Returns a lease's stripe and capacity slot.</summary>
    /// <param name="stripe">The stripe holder the lease owns.</param>
    /// <remarks>Never throws: a lease still out when the gate was disposed keeps its stripe and slot until it returns them here.</remarks>
    internal void Exit(AsyncLockHolder stripe)
    {
        _ = Interlocked.Decrement(ref _activeCount);
        stripe.Dispose();
        _capacity.Release();
    }
}
