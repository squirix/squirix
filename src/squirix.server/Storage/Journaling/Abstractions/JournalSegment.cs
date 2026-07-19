namespace Squirix.Server.Storage.Journaling.Abstractions;

internal sealed record JournalSegment
{
    internal string Path { get; init; } = string.Empty;

    internal int Index { get; init; }
}
