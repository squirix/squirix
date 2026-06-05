using System;
using System.Buffers;
using System.Threading;

namespace Squirix.Server.LocalCache;

internal sealed class KeyedLockStriper
{
    private const int DefaultStripeCount = 64;
    private readonly object[] _locks;

    public KeyedLockStriper(int stripeCount = DefaultStripeCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(stripeCount);

        _locks = new object[stripeCount];
        for (var i = 0; i < _locks.Length; i++)
            _locks[i] = new object();
    }

    public Releaser AcquireAll(ReadOnlySpan<string> keys)
    {
        Span<int> stackStripes = stackalloc int[16];
        var pooledStripes = keys.Length > stackStripes.Length ? new int[keys.Length] : null;
        var stripes = pooledStripes is null ? stackStripes[..keys.Length] : pooledStripes.AsSpan(0, keys.Length);

        var count = 0;
        foreach (var key in keys)
        {
            var stripe = GetStripe(key);
            var seen = false;
            for (var j = 0; j < count; j++)
            {
                if (stripes[j] != stripe)
                    continue;

                seen = true;
                break;
            }

            if (seen)
                continue;

            stripes[count++] = stripe;
        }

        stripes = stripes[..count];
        stripes.Sort();

        var acquired = 0;
        try
        {
            for (; acquired < stripes.Length; acquired++)
                Monitor.Enter(_locks[stripes[acquired]]);

            var lockedStripes = ArrayPool<int>.Shared.Rent(count);
            stripes[..count].CopyTo(lockedStripes);
            return new Releaser(_locks, lockedStripes, count, true);
        }
        catch
        {
            for (var i = acquired - 1; i >= 0; i--)
                Monitor.Exit(_locks[stripes[i]]);

            throw;
        }
    }

    private int GetStripe(string key) => (StringComparer.Ordinal.GetHashCode(key) & int.MaxValue) % _locks.Length;

    internal readonly struct Releaser : IDisposable
    {
        private readonly int _count;
        private readonly object[] _locks;
        private readonly bool _ownsStripes;
        private readonly int[] _stripes;

        public Releaser(object[] locks, int[] stripes, int count, bool ownsStripes)
        {
            _locks = locks;
            _stripes = stripes;
            _count = count;
            _ownsStripes = ownsStripes;
        }

        public void Dispose()
        {
            for (var i = _count - 1; i >= 0; i--)
                Monitor.Exit(_locks[_stripes[i]]);

            if (!_ownsStripes)
                return;
            Array.Clear(_stripes, 0, _count);
            ArrayPool<int>.Shared.Return(_stripes);
        }
    }
}
