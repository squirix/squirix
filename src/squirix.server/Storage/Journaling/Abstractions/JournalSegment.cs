using Squirix.Attributes;

namespace Squirix.Server.Storage.Journaling.Abstractions;

[Immutable]
internal sealed record JournalSegment
{
    internal int Index { get; init; }

    internal string Path { get; init; } = string.Empty;
}
