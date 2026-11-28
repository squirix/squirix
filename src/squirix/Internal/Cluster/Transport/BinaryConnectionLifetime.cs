using System.Threading;

namespace Squirix.Internal.Cluster.Transport;

/// <summary>Arbitrates admission on a connection against closing it after its last live call times out.</summary>
internal struct BinaryConnectionLifetime
{
    /// <summary>The sign bit closes admission permanently. The remaining bits count calls that can still complete.</summary>
    private int _state;

    internal readonly bool IsOpen => Volatile.Read(in _state) >= 0;

    internal bool TryEnter()
    {
        var state = Volatile.Read(ref _state);
        while (state >= 0)
        {
            var observed = Interlocked.CompareExchange(ref _state, state + 1, state);
            if (state == observed)
                return true;
            state = observed;
        }

        return false;
    }

    internal void Leave() => Interlocked.Decrement(ref _state);

    internal bool TryCloseIfIdle() => Interlocked.CompareExchange(ref _state, int.MinValue, 0) == 0;

    internal bool TryClose() => Interlocked.Or(ref _state, int.MinValue) >= 0;
}
