namespace Squirix.Server.Storage.Journaling.Abstractions;

internal sealed record JournalSegment
{
    internal int Index { get; init; }

    internal string Path { get; init; } = string.Empty;
}
