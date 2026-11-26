using System;
using System.Threading;

namespace Squirix.Server.LocalCache;

/// <summary>Index and accounting state shared by the slots in a page. Identities and charges use columns only when their uniform representation no longer suffices.</summary>
/// <remarks>
/// Each page reserves two disjoint ranges of occupant identities. In the compact representation
/// a published slot is either original (0), vacant (1), or reused (2). Before a reused occupant
/// is removed, the writer publishes a full identity column so an older handle can never match again.
/// </remarks>
internal abstract class SlotPageBase
{
    internal const int IndexBits = 8;
    internal const int Capacity = 1 << IndexBits;

    private readonly int[] _hashesAndReuse;
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
        Length = length;

        // Keep the two-bit lifetime states in the tail of the existing hash allocation.
        _hashesAndReuse = new int[length + ((length + 15) / 16)];
    }

    /// <summary>Gets the page sequence. Equal even reads bracket a consistent observation of a slot.</summary>
    internal long Sequence => Volatile.Read(ref _sequence);

    internal int Length { get; }

    internal ref int this[int offset] => ref _hashesAndReuse[offset];

    /// <summary>Maps a one-based slot number to an offset in a 2⁸-slot page.</summary>
    /// <param name="slot">The one-based slot number.</param>
    /// <returns>The zero-based page offset.</returns>
    internal static int OffsetOf(int slot) => (slot - 1) & (Capacity - 1);

    internal long ReadIdentity(int offset)
    {
        var identities = _identities;
        return identities != null ? Volatile.Read(ref identities[offset]) : CompactIdentity(offset);
    }

    internal long NextIdentity(int offset, ref long lastIdentity)
    {
        var start = offset < _published ? _identityBase + Length : _identityBase;
        return _identities == null ? start + offset + 1 : checked(++lastIdentity);
    }

    internal void PublishIdentity(int offset, long identity)
    {
        if (_identities is { } identities)
            Volatile.Write(ref identities[offset], identity);
        else if (offset < _published)
            PublishReplacement(offset);
        else
            _published = offset + 1;
    }

    internal void RetireIdentity(int offset)
    {
        if (_identities is { } identities)
            _ = Interlocked.Exchange(ref identities[offset], 0);
        else
            _ = Interlocked.Or(ref ReuseWord(offset), 1 << ((offset % 16) * 2));
    }

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

    protected void PrepareReuse(int offset)
    {
        if (_identities != null || ReuseState(offset) == 0)
            return;

        var identities = new long[Length];
        for (var i = 0; i < _published; i++)
            identities[i] = CompactIdentity(i);
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

    private long CompactIdentity(int offset)
    {
        if (offset >= _published)
            return 0;

        return ReuseState(offset) switch
        {
            0 => _identityBase + offset + 1,
            2 => _identityBase + Length + offset + 1,
            _ => 0,
        };
    }

    private int ReuseState(int offset) => (Volatile.Read(ref ReuseWord(offset)) >>> ((offset % 16) * 2)) & 3;

    private ref int ReuseWord(int offset) => ref _hashesAndReuse[Length + (offset / 16)];

    private void PublishReplacement(int offset)
    {
        ref var word = ref ReuseWord(offset);
        var shift = (offset % 16) * 2;
        Volatile.Write(ref word, (word & ~(3 << shift)) | (2 << shift));
    }
}
