using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>An ordered entry in a replica group log.</summary>
/// <param name="LogIndex">The one-based log index of the entry.</param>
/// <param name="Term">The term in which the leader created the entry.</param>
/// <param name="Payload">The canonical entry bytes.</param>
[Immutable]
internal readonly record struct FollowerLogEntry(ulong LogIndex, ulong Term, ReadOnlyMemory<byte> Payload)
{
    /// <summary>Gets the logical bytes of this entry as a span.</summary>
    internal ReadOnlySpan<byte> PayloadSpan => Payload.Span;
}
