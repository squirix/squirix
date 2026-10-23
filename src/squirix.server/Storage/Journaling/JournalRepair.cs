using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Describes one change startup recovery made to a journal segment file.</summary>
/// <param name="Path">The repaired segment path.</param>
/// <param name="Kind">What kind of repair was applied.</param>
/// <param name="OriginalLength">The segment length in bytes before the repair.</param>
/// <param name="DiscardedBytes">The number of bytes the repair discarded; zero for a header restore.</param>
[Immutable]
internal readonly record struct JournalRepair(string Path, JournalRepairKind Kind, long OriginalLength, long DiscardedBytes);
