namespace Squirix.Server.Storage.Journaling;

/// <summary>The kind of change startup recovery made to a journal segment file.</summary>
internal enum JournalRepairKind
{
    /// <summary>A damaged file header in front of intact frames was restored without discarding data.</summary>
    HeaderRestored = 0,

    /// <summary>A segment that held no frames had its torn header rewritten.</summary>
    TornCreationHeaderRewritten = 1,

    /// <summary>A torn last frame was cut from the end of the segment.</summary>
    TornTailTruncated = 2,
}
