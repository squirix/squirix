using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Bounds in-flight group mutations and serializes conflicts through a fixed number of key stripes.</summary>
[ThreadSafe]
internal sealed class ReplicaMutationGate : IDisposable
{
    private readonly SemaphoreSlim _capacity;
    private readonly SemaphoreSlim[] _stripes;
    private int _activeCount;

    internal ReplicaMutationGate(int maxInFlight, int stripeCount = 64)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxInFlight);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(stripeCount);

        MaxInFlight = maxInFlight;
        _capacity = new SemaphoreSlim(maxInFlight, maxInFlight);
        _stripes = new SemaphoreSlim[stripeCount];
        for (var i = 0; i < _stripes.Length; i++)
            _stripes[i] = new SemaphoreSlim(1, 1);
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

    internal async ValueTask<ReplicaMutationLease> EnterAsync(int keyHash, CancellationToken cancellationToken)
    {
        await _capacity.WaitAsync(cancellationToken).ConfigureAwait(false);
        var stripe = _stripes[(keyHash & int.MaxValue) % _stripes.Length];
        try
        {
            await stripe.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            _ = _capacity.Release();
            throw;
        }

        _ = Interlocked.Increment(ref _activeCount);
        return new ReplicaMutationLease(this, stripe);
    }

    internal void Exit(SemaphoreSlim stripe)
    {
        _ = Interlocked.Decrement(ref _activeCount);
        _ = stripe.Release();
        _ = _capacity.Release();
    }
}
