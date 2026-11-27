using System;
using System.Runtime.InteropServices;
using Squirix.Server.Attributes;
using Squirix.Server.Threading;

namespace Squirix.Server.Storage.Journaling.Abstractions;

/// <summary>Orders a cache frame under its acquisition and optionally releases admission once the ring owns the frame.</summary>
/// <param name="Ownership">The acquisition authorizing sequence allocation and ring publication.</param>
/// <param name="ReleaseAfterEnqueue">The release reserved for the single-frame durable mutation prepare phase.</param>
[Immutable]
[StructLayout(LayoutKind.Auto)]
internal readonly record struct JournalAppendContext(AsyncLockOwnership Ownership, Action? ReleaseAfterEnqueue)
{
    public static implicit operator JournalAppendContext(AsyncLockOwnership ownership)
    {
        return new JournalAppendContext(ownership, null);
    }
}
