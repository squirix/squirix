namespace Squirix.Server.Storage.Journaling;

internal sealed record JournalSegment
{
    internal string Path { get; init; } = string.Empty;

    internal int Index { get; init; }
}
