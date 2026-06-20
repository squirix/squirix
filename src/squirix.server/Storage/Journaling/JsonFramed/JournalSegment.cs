namespace Squirix.Server.Storage.Journaling.JsonFramed;

internal readonly struct JournalSegment
{
    public int Index { get; init; }

    public string Path { get; init; }
}
