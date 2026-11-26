using System;
using System.Threading;

namespace Squirix.Server.LocalCache;

/// <summary>Index and accounting state shared by the slots in a page. Identities and charges use columns only when their uniform representation no longer suffices.</summary>
internal abstract class SlotPageBase
{
    internal const int IndexBits = 8;
    internal const int Capacity = 1 << IndexBits;

    private readonly int[] _hashes;
    private readonly long _identityBase;
    private long _sequence;
    private volatile long[]? _identities;
    private volatile int _published;
    private long[]? _charges;
    private long _commonCharge;
    private bool _hasCharge;

    protected SlotPageBase(int length, long identityBase)
    {
        _identityBase = identityBase;
        _hashes = new int[length];
    }

    /// <summary>Gets the page sequence. Equal even reads bracket a consistent observation of a slot.</summary>
    internal long Sequence => Volatile.Read(ref _sequence);

    internal int Length => _hashes.Length;

    internal ref int this[int offset] => ref _hashes[offset];

    /// <summary>Maps a one-based slot number to an offset in a 2⁸-slot page.</summary>
    /// <param name="slot">The one-based slot number.</param>
    /// <returns>The zero-based page offset.</returns>
    internal static int OffsetOf(int slot) => (slot - 1) & (Capacity - 1);

    internal long ReadIdentity(int offset)
    {
        var identities = _identities;
        return identities != null ? Volatile.Read(ref identities[offset]) : InitialIdentity(offset);
    }

    internal long NextIdentity(int offset, ref long lastIdentity) =>
        _identities == null ? _identityBase + offset + 1 : checked(++lastIdentity);

    internal void PublishIdentity(int offset, long identity)
    {
        if (_identities is { } identities)
            Volatile.Write(ref identities[offset], identity);
        else
            _published = offset + 1;
    }

    internal void RetireIdentity(int offset) => Interlocked.Exchange(ref _identities![offset], 0);

    internal long Charge(int offset) => _charges is { } charges ? charges[offset] : _commonCharge;

    internal void SetCharge(int offset, long bytes)
    {
        if (!_hasCharge)
        {
            _commonCharge = bytes;
            _hasCharge = true;
        }

        if (_charges == null)
        {
            if (bytes == _commonCharge)
                return;

            _charges = new long[Length];
            Array.Fill(_charges, _commonCharge);
        }

        _charges[offset] = bytes;
    }

    protected void PrepareReuse()
    {
        if (_identities != null)
            return;

        var identities = new long[Length];
        for (var i = 0; i < _published; i++)
            identities[i] = _identityBase + i + 1;
        _identities = identities;
    }

    /// <summary>Makes the sequence odd.</summary>
    /// <returns>The odd sequence.</returns>
    /// <remarks>
    /// A reader that sees a field write must then see the odd sequence. Every field write that follows is a release write, which keeps this write
    /// before it; a value-type value is written plainly, so a full fence keeps it after this write.
    /// </remarks>
    protected long BeginWrite()
    {
        var odd = _sequence + 1;
        Volatile.Write(ref _sequence, odd);
        Interlocked.MemoryBarrier();

        return odd;
    }

    /// <summary>Makes the sequence even; the release write keeps every field write before it.</summary>
    /// <param name="odd">The odd sequence <see cref="BeginWrite" /> returned.</param>
    protected void EndWrite(long odd) => Volatile.Write(ref _sequence, odd + 1);

    private long InitialIdentity(int offset) => offset < _published ? _identityBase + offset + 1 : 0;
}
