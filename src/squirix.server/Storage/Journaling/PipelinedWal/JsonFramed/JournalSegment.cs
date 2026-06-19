namespace Squirix.Server.Storage.Journaling;

internal readonly struct JournalSegment
{
    public int Index { get; init; }

    public string Path { get; init; }
}
