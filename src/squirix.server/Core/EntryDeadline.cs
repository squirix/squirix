using System;
using System.Runtime.CompilerServices;

namespace Squirix.Server.Core;

/// <summary>Packs an optional deadline into one word while preserving ticks and DateTimeKind.</summary>
internal static class EntryDeadline
{
    /// <summary>No DateTime has all bits set because its tick count would exceed DateTime.MaxValue.</summary>
    internal const long None = -1;

    internal static long Encode(DateTime? value) => value is { } date ? Unsafe.BitCast<DateTime, long>(date) : None;

    internal static DateTime? Decode(long value) => value == None ? null : Unsafe.BitCast<long, DateTime>(value);
}
